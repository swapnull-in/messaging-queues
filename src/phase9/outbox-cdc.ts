/**
 * Phase 9 — THE DUAL-WRITE PROBLEM, the TRANSACTIONAL OUTBOX, and CDC.
 *
 * Run with:  node "src/phase9/outbox-cdc.ts"
 *
 * If you learn ONE pattern from this whole repo, learn this one.
 *
 * ─── The setup ────────────────────────────────────────────────────────────
 * Almost every service does two things when something happens:
 *   1. change its OWN database   ("mark order #42 PAID")
 *   2. tell the rest of the world ("publish an OrderPlaced event to the broker")
 *
 * These are TWO different systems — your DB and your broker (Kafka, SQS,
 * RabbitMQ). There is no shared transaction across them. So the naive code —
 * write the DB, then publish — has a gap in the middle where the process can
 * die. That gap is the bug.
 *
 * ─── The dual-write bug ───────────────────────────────────────────────────
 *   db.commit(orderPaid)      // ✅ persisted
 *   <<< CRASH >>>             // power loss, OOM, deploy, network blip
 *   broker.publish(event)     // ❌ never runs
 *
 *   Result: the order is PAID in your DB forever, but the "OrderPlaced" event
 *   was never sent. Downstream (shipping, email, analytics) never hears about
 *   it. The two systems now DISAGREE, permanently. Swap the order and you get
 *   the mirror-image bug: publish first, crash before commit → a PHANTOM event
 *   for an order that doesn't exist.
 *
 * You cannot fix this by being careful, adding retries, or reordering the two
 * writes. The gap is fundamental: two systems, no shared commit.
 *
 * ─── The fix: make it ONE write ───────────────────────────────────────────
 * Stop writing to two systems in the request path. Write to ONE — your DB —
 * atomically, and let a separate process move the event to the broker later.
 *
 *   OUTBOX: in the SAME DB transaction that changes state, insert the event
 *   into an "outbox" table. One commit, both rows, all-or-nothing. A separate
 *   RELAY polls the outbox and publishes (at-least-once), marking rows sent.
 *   Crash anywhere and the truth is still in the DB: on restart the relay
 *   replays unsent rows. State and event can never disagree.
 *
 *   CDC (Change Data Capture): don't even write an outbox table — a reader
 *   tails the DB's own commit log (Postgres WAL, MySQL binlog) and emits an
 *   event per committed row change. Same guarantee, no app-level table, fully
 *   decoupled from request timing. This is what Debezium does.
 *
 * Both give AT-LEAST-ONCE delivery, so a duplicate is always possible. The
 * consumer dedupes by event id → EFFECTIVELY-ONCE. There is no exactly-once
 * on the wire; there never was.
 *
 * This file simulates all of it in-process: a tiny transactional DB (a Map +
 * an atomic transaction()), a broker (append-only log + a deduping consumer),
 * and a controllable crash point. Read the timestamped log top to bottom.
 *
 * ─── TAKEAWAY ─────────────────────────────────────────────────────────────
 * Never dual-write to a DB and a broker in the request path. Write the event
 * to an OUTBOX in the same transaction (or tail the DB's change log via CDC),
 * relay at-least-once, and dedupe by id for effectively-once. There is no
 * exactly-once on the wire.
 */

/** Timestamped logging so you can watch the flow happen in real time. */
function log(...args: unknown[]): void {
  const t = new Date().toISOString().slice(11, 23); // HH:MM:SS.mmm
  console.log(`[${t}]`, ...args);
}

// A crash we can throw at a precise point to simulate a process dying mid-flow.
// It's a normal Error we catch in the demo — in production this is a real
// kill -9, so NOTHING after the throw runs (no finally, no cleanup, no publish).
class Crash extends Error {
  constructor(where: string) {
    super(`💥 process crashed ${where}`);
    this.name = "Crash";
  }
}

// ─── The DATABASE ───────────────────────────────────────────────────────────
// An in-process stand-in for Postgres/MySQL. Two things matter:
//   1. transaction() is ATOMIC — every write inside the callback commits
//      together, or (if the callback throws) NOTHING commits.
//   2. every commit appends to a CHANGE LOG (the "WAL"/binlog) — the ordered,
//      durable record of what actually happened. CDC reads from here.
interface Row {
  [key: string]: unknown;
}

interface ChangeLogEntry {
  lsn: number; // log sequence number — a monotonic position in the commit log
  table: string;
  key: string;
  after: Row; // the row's committed value
}

class Database {
  // table name → (primary key → row)
  private tables = new Map<string, Map<string, Row>>();
  // The commit log / WAL. Append-only, ordered, the source of truth for CDC.
  private changeLog: ChangeLogEntry[] = [];
  private nextLsn = 1;

  private table(name: string): Map<string, Row> {
    let t = this.tables.get(name);
    if (!t) {
      t = new Map();
      this.tables.set(name, t);
    }
    return t;
  }

  /**
   * Run `work` as a single atomic transaction. `work` records intended writes
   * via the passed-in tx handle; they are applied to the tables AND appended to
   * the change log only if `work` returns without throwing. If it throws, the
   * staged writes are discarded — as if the transaction never happened.
   */
  transaction<T>(work: (tx: Tx) => T): T {
    const staged: Array<{ table: string; key: string; row: Row }> = [];
    const tx: Tx = {
      put(table, key, row) {
        staged.push({ table, key, row });
      },
    };

    const result = work(tx); // may throw → we never reach the commit below

    // COMMIT: apply every staged write and append it to the change log, all at
    // once. Because this runs synchronously after `work` succeeded, there is no
    // gap where half of it is visible.
    for (const w of staged) {
      this.table(w.table).set(w.key, w.row);
      this.changeLog.push({
        lsn: this.nextLsn++,
        table: w.table,
        key: w.key,
        after: w.row,
      });
    }
    return result;
  }

  get(table: string, key: string): Row | undefined {
    return this.table(table).get(key);
  }

  /** Rows in a table (for inspection in the demo). */
  rows(table: string): Row[] {
    return [...this.table(table).values()];
  }

  /** Read committed changes with lsn strictly greater than `afterLsn`. CDC. */
  changesSince(afterLsn: number): ChangeLogEntry[] {
    return this.changeLog.filter((c) => c.lsn > afterLsn);
  }
}

interface Tx {
  /** Stage a row write inside the current transaction. */
  put(table: string, key: string, row: Row): void;
}

// ─── The BROKER ─────────────────────────────────────────────────────────────
// An append-only event log (like a Kafka topic) plus a consumer that dedupes
// by event id. publish() is at-least-once: the same event id may arrive twice.
interface Event {
  id: string; // stable, unique per logical event — the dedup key
  type: string;
  payload: Row;
}

class Broker {
  private topic: Event[] = [];

  /** Producers append events here. Duplicates are allowed on the wire. */
  publish(event: Event): void {
    this.topic.push(event);
    log(`   📤 broker received  ${event.id}  (${event.type})`);
  }

  events(): Event[] {
    return this.topic;
  }
}

// The downstream CONSUMER. At-least-once delivery means it must assume every
// event might be a duplicate, so it tracks processed ids and skips repeats.
// This is what turns "at-least-once on the wire" into "effectively-once".
class Consumer {
  private processed = new Set<string>();
  public handled: string[] = []; // ids it actually acted on (side effect ran)

  handle(event: Event): void {
    if (this.processed.has(event.id)) {
      log(`   🔁 consumer saw ${event.id} again → DEDUPED (no double side effect)`);
      return;
    }
    this.processed.add(event.id);
    this.handled.push(event.id);
    log(`   ✅ consumer handled ${event.id}  (${event.type}) — side effect done`);
  }
}

// ─── Shared demo helpers ────────────────────────────────────────────────────
const ORDERS = "orders";
const OUTBOX = "outbox";

function paidOrderRow(orderId: string): Row {
  return { id: orderId, status: "PAID", amountCents: 4999 };
}

function orderPlacedEvent(orderId: string): Event {
  return {
    id: `evt_OrderPlaced_${orderId}`, // deterministic id → natural dedup key
    type: "OrderPlaced",
    payload: { orderId, amountCents: 4999 },
  };
}

// ════════════════════════════════════════════════════════════════════════════
// A) DUAL-WRITE — the anti-pattern. Two separate writes with a fatal gap.
// ════════════════════════════════════════════════════════════════════════════
function demoDualWrite(): void {
  log("═══ A) DUAL-WRITE (anti-pattern): DB and broker as two steps ═══");

  // ── A1: crash BETWEEN the commit and the publish ──────────────────────────
  log("A1) commit order, then publish — crash in the gap between them:");
  {
    const db = new Database();
    const broker = new Broker();
    try {
      db.transaction((tx) => tx.put(ORDERS, "42", paidOrderRow("42")));
      log("   💾 order 42 committed as PAID");
      throw new Crash("AFTER db commit, BEFORE broker.publish");
      // Unreachable — exactly like a real kill -9. The publish never happens.
      broker.publish(orderPlacedEvent("42"));
    } catch (err) {
      log(`   ${(err as Error).message}`);
    }
    const order = db.get(ORDERS, "42");
    log(`   → DB says order 42 = ${order?.status}, broker has ${broker.events().length} event(s)`);
    log("   ❌ INCONSISTENT: order is PAID but OrderPlaced was never published.\n");
  }

  // ── A2: reverse the order → phantom event ─────────────────────────────────
  log("A2) publish first, then commit — crash before the commit:");
  {
    const db = new Database();
    const broker = new Broker();
    try {
      broker.publish(orderPlacedEvent("77"));
      throw new Crash("AFTER broker.publish, BEFORE db commit");
      db.transaction((tx) => tx.put(ORDERS, "77", paidOrderRow("77")));
    } catch (err) {
      log(`   ${(err as Error).message}`);
    }
    const order = db.get(ORDERS, "77");
    log(`   → DB says order 77 = ${order ? order.status : "does not exist"}, broker has ${broker.events().length} event(s)`);
    log("   ❌ PHANTOM EVENT: downstream hears OrderPlaced for an order the DB never saved.\n");
  }
}

// ════════════════════════════════════════════════════════════════════════════
// B) TRANSACTIONAL OUTBOX — one atomic write, a relay publishes later.
// ════════════════════════════════════════════════════════════════════════════
function demoOutbox(): void {
  log("═══ B) TRANSACTIONAL OUTBOX: state + event in ONE transaction ═══");

  const db = new Database();
  const broker = new Broker();
  const consumer = new Consumer();

  // The RELAY: polls the outbox for unsent rows and publishes them, then marks
  // them sent. In production this is a background worker / Debezium-outbox job.
  // It is at-least-once: it may publish, then crash before marking sent, and
  // re-publish on restart — which is exactly why the consumer dedupes.
  function relay(): void {
    for (const row of db.rows(OUTBOX)) {
      if (row.sent) continue;
      broker.publish(row.event as Event);
      consumer.handle(row.event as Event);
      // Mark sent in its own tiny transaction. (Belt-and-suspenders: if this
      // fails, the row stays unsent and gets re-published — a safe duplicate.)
      db.transaction((tx) => tx.put(OUTBOX, row.id as string, { ...row, sent: true }));
      log(`   🏷  outbox row ${row.id} marked sent`);
    }
  }

  // The request path: ONE atomic transaction writes BOTH the state change and
  // the event into the outbox. There is no gap — they commit together or not
  // at all. Then... the process crashes before the relay ever runs.
  log("B1) service handles payment: atomic {order PAID + outbox event}, then crashes:");
  try {
    db.transaction((tx) => {
      tx.put(ORDERS, "42", paidOrderRow("42"));
      const evt = orderPlacedEvent("42");
      tx.put(OUTBOX, evt.id, { id: evt.id, event: evt, sent: false });
    });
    log("   💾 committed: order 42 = PAID  AND  outbox row (unsent) — one commit");
    throw new Crash("AFTER commit, BEFORE the relay could publish");
    relay();
  } catch (err) {
    log(`   ${(err as Error).message}`);
  }
  log(`   → broker has ${broker.events().length} event(s) so far — the crash lost nothing that matters.`);
  log("     The truth is safe in the DB outbox, waiting.\n");

  // Restart: the relay runs and REPLAYS the outbox. The event gets delivered.
  log("B2) process restarts → relay drains the outbox:");
  relay();
  const order = db.get(ORDERS, "42");
  log(`   → DB order 42 = ${order?.status}, broker has ${broker.events().length} event(s), consumer handled ${consumer.handled.length}`);
  log("   ✅ CONSISTENT: state and event agree. The event survived the crash.\n");
}

// ════════════════════════════════════════════════════════════════════════════
// C) CDC — tail the DB's own commit log; no app-level outbox table.
// ════════════════════════════════════════════════════════════════════════════
function demoCdc(): void {
  log("═══ C) CDC (Change Data Capture): tail the commit log, emit per change ═══");

  const db = new Database();
  const broker = new Broker();
  const consumer = new Consumer();

  // The service just writes its state. No outbox table, no publish in the
  // request path — it doesn't even know CDC exists. This is the big win:
  // publishing is fully decoupled from request timing.
  log("C1) service writes state normally (NO outbox, NO publish):");
  db.transaction((tx) => tx.put(ORDERS, "88", paidOrderRow("88")));
  db.transaction((tx) => tx.put(ORDERS, "99", paidOrderRow("99")));
  log("   💾 orders 88 and 99 committed as PAID\n");

  // The CDC reader tails the WAL/binlog from its last position and emits one
  // event per committed row change. Debezium works exactly like this.
  log("C2) CDC reader tails the commit log and emits an event per committed change:");
  let cursorLsn = 0;
  for (const change of db.changesSince(cursorLsn)) {
    if (change.table !== ORDERS) continue;
    const event: Event = {
      id: `evt_cdc_${change.table}_${change.key}_lsn${change.lsn}`,
      type: "OrderPlaced",
      payload: change.after,
    };
    broker.publish(event);
    consumer.handle(event);
    cursorLsn = change.lsn; // advance the cursor so we never re-emit on restart
  }
  log(`   → broker has ${broker.events().length} event(s) derived straight from committed writes.`);
  log("   ✅ No dual write ever existed — the event IS the committed change.\n");
}

// ════════════════════════════════════════════════════════════════════════════
// D) IDEMPOTENCY — at-least-once means duplicates; dedupe by id.
// ════════════════════════════════════════════════════════════════════════════
function demoIdempotency(): void {
  log("═══ D) IDEMPOTENCY: at-least-once → dedupe by id → effectively-once ═══");

  const broker = new Broker();
  const consumer = new Consumer();
  const evt = orderPlacedEvent("42");

  // The relay published, crashed before marking the row sent, restarted, and
  // published the SAME event id again. On the wire that's a duplicate. This is
  // normal and expected — every at-least-once system does this sometimes.
  log("D1) relay publishes an event, then re-publishes it after a crash-restart:");
  broker.publish(evt);
  consumer.handle(evt); // first time: side effect runs
  broker.publish(evt); // at-least-once duplicate
  consumer.handle(evt); // second time: DEDUPED by id

  log(`   → broker log has ${broker.events().length} deliveries, consumer ran the side effect ${consumer.handled.length} time`);
  log("   ✅ EFFECTIVELY-ONCE: the duplicate was ignored because the id was already seen.\n");
}

async function main(): Promise<void> {
  demoDualWrite();
  demoOutbox();
  demoCdc();
  demoIdempotency();

  log("──────────────────────────────────────────────────────────────────");
  log("TAKEAWAY: never dual-write to a DB and a broker in the request path.");
  log("Write the event to an OUTBOX in the same transaction (or tail the DB's");
  log("change log via CDC), relay at-least-once, and dedupe by id for");
  log("effectively-once. There is no exactly-once on the wire.");

  // Match phase1: end the process explicitly once the demo has run.
  await new Promise((r) => setTimeout(r, 0));
  process.exit(0);
}

main();
