/**
 * Phase 13 — EXACTLY-ONCE PURCHASE: the capstone.
 *
 * Run with:  node "src/phase13/exactly-once-purchase.ts"
 *
 * This phase COMPOSES two ideas you already have into one end-to-end flow:
 *   - phase3  (idempotency: claim a key before the side effect, skip repeats)
 *   - phase9  (transactional outbox: never dual-write DB + broker)
 * Read those first — this file doesn't re-teach them, it wires them together
 * around real money: user clicks Buy exactly once → money moves exactly once.
 *
 * ─── The problem: at-least-once is a law, exactly-once is a design ────────
 * Every layer of the stack retries: the user (double-tap, refresh), the HTTP
 * client, proxies, your service mesh, any queue in the path. So duplicate
 * DELIVERY of the purchase request is guaranteed by construction. The design
 * question is making duplicate EXECUTION impossible:
 *
 *   at-least-once delivery + idempotent processing = effectively-exactly-once
 *
 * — the only honest meaning of "exactly-once" across a network.
 *
 * ─── The contract (client ⇄ server) ───────────────────────────────────────
 * The client mints an IDEMPOTENCY KEY per purchase *intent* (a UUID, persisted
 * client-side before the first send) and sends it on every retry of that
 * intent. The server promises four things:
 *
 *   1. FIRST WINS          — first request with key K executes; later Ks don't.
 *                            Mechanism: atomic claim via UNIQUE constraint.
 *   2. REPLAY THE RECEIPT  — later Ks get the STORED RESPONSE of the first
 *                            (status code + body). Mechanism: response snapshot
 *                            saved with the key.
 *   3. CONCURRENT-SAFE     — two Ks in flight → one executes, the other gets
 *                            409-in-progress. Mechanism: the claim row is the lock.
 *   4. LOUD ON MISUSE      — same K, different payload → 422, never silent
 *                            execution. Mechanism: request hash stored with the key.
 *
 * The subtle bug the claim kills: check-then-insert ("SELECT key; if absent,
 * process; INSERT") is a textbook TOCTOU race — two concurrent duplicates both
 * pass the check. The claim must be ONE atomic insert BEFORE processing starts.
 *
 * ─── The engine: claim → execute → snapshot (a state machine) ─────────────
 * Because the payment provider (PSP) call is an EXTERNAL side effect, we can't
 * hold a DB transaction open across a multi-second network call — we're forced
 * into a TWO-PHASE shape:
 *
 *   txn 1: INSERT key K, status=RUNNING          (visible; duplicates get 409)
 *   ...    call the PSP with a DERIVED idempotency key   (no txn held open)
 *   txn 2: order + ledger + outbox event + key→FINISHED + response snapshot
 *          — ONE atomic commit (phase9's lesson: no dual-write, ever)
 *
 * Two dedup layers: OUR key stops duplicate orders; the PSP's key stops
 * duplicate charges — so even our own internal retries can't double-charge.
 *
 * A crash between the phases leaves K stuck RUNNING. That state needs an
 * OWNER: a sweeper inspects stale RUNNING keys and DISAMBIGUATES by asking the
 * PSP "did a charge with this derived key happen?" — yes → roll forward to
 * FINISHED; no → release the key so a clean retry can re-claim. Never let
 * stale RUNNING mean "blocked forever" or "assume failed".
 *
 * Downstream (fulfillment) hangs off the OUTBOX event: relayed at-least-once,
 * deduped by event id → exactly-once-by-effect. And a RECONCILIATION job is
 * the backstop that PROVES the invariant holds (every PSP charge ↔ exactly one
 * FINISHED key) — every anomaly it finds is a sev on the prevention layer,
 * not something to quietly mop up.
 *
 * This file walks every failure named above, deterministically (controlled
 * crash/duplicate/timeout points — no randomness), then prints the invariants.
 *
 * ─── TAKEAWAY (the deep dive's bottom line, verbatim) ─────────────────────
 * "Exactly-once across a network is a myth at the transport layer and a design
 * at the application layer: at-least-once delivery plus idempotent processing.
 * The key is claimed atomically in the same transaction that creates the order
 * — after that, duplicates don't fail, they replay the receipt."
 */

import { log } from "../phase1/queue.ts";

// ─── Controlled failure points (same trick as phase9) ───────────────────────
// A Crash is a stand-in for kill -9: NOTHING after the throw runs.
class Crash extends Error {
  constructor(where: string) {
    super(`💥 process crashed ${where}`);
    this.name = "Crash";
  }
}

// The network ate the PSP's response. The charge may or may not have landed —
// from the caller's side this is fundamentally AMBIGUOUS.
class TimeoutError extends Error {
  constructor(msg: string) {
    super(msg);
    this.name = "TimeoutError";
  }
}

class UniqueConflict extends Error {
  constructor(table: string, key: string) {
    super(`unique constraint violated: ${table}[${key}]`);
    this.name = "UniqueConflict";
  }
}

// ─── A tiny transactional DB (see phase9 for the full version with a WAL) ───
// One property matters here: transaction() is ATOMIC — every staged write
// commits together, or (if the callback throws) NOTHING commits. `insert`
// enforces a UNIQUE constraint: the database's uniqueness machinery is the one
// component already solving distributed races — don't rebuild it in app code.
interface Row {
  [key: string]: unknown;
}

interface Tx {
  /** INSERT — throws UniqueConflict if the key already exists. The atomic claim. */
  insert(table: string, key: string, row: Row): void;
  /** UPSERT — plain write, used for updates inside the same engine. */
  put(table: string, key: string, row: Row): void;
  /** DELETE — used by the sweeper to release a dead claim. */
  del(table: string, key: string): void;
}

class Database {
  private tables = new Map<string, Map<string, Row>>();

  private table(name: string): Map<string, Row> {
    let t = this.tables.get(name);
    if (!t) {
      t = new Map();
      this.tables.set(name, t);
    }
    return t;
  }

  transaction<T>(work: (tx: Tx) => T): T {
    const staged: Array<{ op: "put" | "del"; table: string; key: string; row?: Row }> = [];
    const stagedKeys = new Set<string>();
    const self = this;
    const tx: Tx = {
      insert(table, key, row) {
        // Uniqueness is checked against committed rows AND writes staged in
        // this same transaction — exactly what a UNIQUE index gives you.
        if (self.table(table).has(key) || stagedKeys.has(`${table}/${key}`)) {
          throw new UniqueConflict(table, key);
        }
        stagedKeys.add(`${table}/${key}`);
        staged.push({ op: "put", table, key, row });
      },
      put(table, key, row) {
        staged.push({ op: "put", table, key, row });
      },
      del(table, key) {
        staged.push({ op: "del", table, key });
      },
    };

    const result = work(tx); // may throw → nothing below runs → nothing commits

    for (const w of staged) {
      if (w.op === "put") this.table(w.table).set(w.key, w.row!);
      else this.table(w.table).delete(w.key);
    }
    return result;
  }

  get(table: string, key: string): Row | undefined {
    return this.table(table).get(key);
  }

  rows(table: string): Row[] {
    return [...this.table(table).values()];
  }
}

// ─── The PAYMENT PROVIDER (PSP — think Stripe) ──────────────────────────────
// The PSP runs the SAME claim-and-replay engine on its side (Stripe accepts an
// idempotency key per charge). This is the second dedup layer: our internal
// retries — including retries of an AMBIGUOUS timeout — cannot double-charge.
interface ChargeRow {
  chargeId: string;
  idempotencyKey: string; // the DERIVED key we sent
  userId: string;
  amountCents: number;
}

class PaymentProvider {
  private byKey = new Map<string, ChargeRow>();
  private nextChargeNo = 1;
  /** Every call that reached the PSP over the wire, duplicates included. */
  public wireCalls = 0;

  charge(
    idempotencyKey: string,
    userId: string,
    amountCents: number,
    opts: { timeoutAfterCharge?: boolean } = {},
  ): ChargeRow {
    this.wireCalls += 1;
    const existing = this.byKey.get(idempotencyKey);
    if (existing) {
      log(`      🏦 PSP: key ${idempotencyKey} already charged → replaying ${existing.chargeId} (no new charge)`);
      return existing;
    }
    const row: ChargeRow = {
      chargeId: `ch_${this.nextChargeNo++}`,
      idempotencyKey,
      userId,
      amountCents,
    };
    this.byKey.set(idempotencyKey, row);
    log(`      🏦 PSP: CHARGED ${row.chargeId} — $${(amountCents / 100).toFixed(2)} (key ${idempotencyKey})`);
    if (opts.timeoutAfterCharge) {
      // The money moved, but the response is lost on the wire. The caller has
      // no way to distinguish this from "the request never arrived".
      throw new TimeoutError("PSP response lost after the charge landed");
    }
    return row;
  }

  /** The sweeper's disambiguation query: "did a charge with this key happen?" */
  findByKey(idempotencyKey: string): ChargeRow | undefined {
    return this.byKey.get(idempotencyKey);
  }

  charges(): ChargeRow[] {
    return [...this.byKey.values()];
  }
}

// ─── Downstream FULFILLMENT (phase9's deduping consumer, condensed) ─────────
// At-least-once delivery from the relay means duplicates on the wire are
// normal; the consumer dedupes by event id → exactly-once-BY-EFFECT.
interface FulfillmentEvent {
  id: string; // stable per logical event — the dedup key
  orderId: string;
  amountCents: number;
}

class FulfillmentConsumer {
  private processed = new Set<string>();
  public shipped: string[] = []; // order ids actually fulfilled

  handle(event: FulfillmentEvent): void {
    if (this.processed.has(event.id)) {
      log(`      🔁 fulfillment saw ${event.id} again → DEDUPED (nothing ships twice)`);
      return;
    }
    this.processed.add(event.id);
    this.shipped.push(event.orderId);
    log(`      📦 fulfillment shipped order ${event.orderId} (event ${event.id})`);
  }
}

// ─── The CHECKOUT SERVICE — claim → execute → snapshot ──────────────────────
const KEYS = "idempotency_keys";
const ORDERS = "orders";
const LEDGER = "ledger";
const OUTBOX = "outbox";

interface Cart {
  userId: string;
  sku: string;
  amountCents: number;
}

interface ApiResponse {
  httpStatus: number;
  body: Row;
}

interface KeyRow extends Row {
  key: string;
  requestHash: string;
  payload: Cart; // kept so the sweeper can roll the step forward
  status: "RUNNING" | "FINISHED";
  response?: ApiResponse;
}

/** Deterministic stand-in for a request-body hash (promise 4: loud on misuse). */
function hashRequest(cart: Cart): string {
  return JSON.stringify([cart.userId, cart.sku, cart.amountCents]);
}

/**
 * The DERIVED idempotency key we hand the PSP. Deterministic function of OUR
 * key, so every retry of intent K — client retry, in-process retry, sweeper
 * roll-forward — lands on the same PSP-side dedup slot.
 */
function derivedPspKey(key: string): string {
  return `psp_${key}`;
}

/** Deterministic chaos injection — every failure in this demo is chosen, not rolled. */
interface ChaosPlan {
  /** Fired between the RUNNING claim and the PSP call — a concurrent duplicate. */
  whileRunning?: () => void;
  /** First PSP call: the charge lands but the response is lost (ambiguous). */
  pspTimesOut?: boolean;
  /** kill -9 after the RUNNING claim, BEFORE the PSP is ever called. */
  crashBeforeCharge?: boolean;
  /** kill -9 after the PSP charged, BEFORE the FINISHED commit. */
  crashBeforeFinish?: boolean;
}

class CheckoutService {
  private db: Database;
  private psp: PaymentProvider;
  private nextOrderNo = 1;
  /** Every HTTP request the server received, duplicates and retries included. */
  public requestsReceived = 0;

  constructor(db: Database, psp: PaymentProvider) {
    this.db = db;
    this.psp = psp;
  }

  purchase(key: string, cart: Cart, chaos: ChaosPlan = {}): ApiResponse {
    this.requestsReceived += 1;
    const requestHash = hashRequest(cart);

    // ── PHASE 1: the ATOMIC CLAIM ───────────────────────────────────────────
    // One INSERT, not check-then-insert (that's the TOCTOU race). On conflict,
    // the existing row tells us exactly which promise to honor.
    try {
      this.db.transaction((tx) =>
        tx.insert(KEYS, key, {
          key,
          requestHash,
          payload: cart,
          status: "RUNNING",
        } satisfies KeyRow),
      );
      log(`   🔑 claimed key ${key} (status=RUNNING) — first request wins`);
    } catch (err) {
      if (!(err instanceof UniqueConflict)) throw err;
      const row = this.db.get(KEYS, key) as KeyRow;
      if (row.requestHash !== requestHash) {
        log(`   🚨 key ${key} reused with a DIFFERENT payload → 422 (loud on misuse, never silent execution)`);
        return { httpStatus: 422, body: { error: "idempotency key reused with different request" } };
      }
      if (row.status === "RUNNING") {
        log(`   ⏳ key ${key} is RUNNING → 409-in-progress (the claim row is the lock)`);
        return { httpStatus: 409, body: { error: "request in progress, retry shortly" } };
      }
      log(`   📋 key ${key} is FINISHED → replaying the stored receipt (no execution)`);
      return row.response!;
    }

    if (chaos.whileRunning) chaos.whileRunning(); // a concurrent duplicate arrives NOW
    if (chaos.crashBeforeCharge) throw new Crash("after the claim, BEFORE calling the PSP");

    // ── EXTERNAL SIDE EFFECT: charge via the PSP, with a DERIVED key ────────
    // We cannot hold a DB transaction open across this network call — that is
    // exactly why the claim was committed (visible) first.
    const pspKey = derivedPspKey(key);
    let charge: ChargeRow;
    try {
      charge = this.psp.charge(pspKey, cart.userId, cart.amountCents, {
        timeoutAfterCharge: chaos.pspTimesOut === true,
      });
    } catch (err) {
      if (!(err instanceof TimeoutError)) throw err;
      // AMBIGUOUS: did the money move? Unknowable from here. But the retry is
      // safe BECAUSE the derived key is the same — the PSP's dedup layer turns
      // "maybe charged" into "charged exactly once" no matter how we retry.
      log(`   ⚠️  PSP timed out — ambiguous! Retrying with the SAME derived key ${pspKey}…`);
      charge = this.psp.charge(pspKey, cart.userId, cart.amountCents);
    }

    if (chaos.crashBeforeFinish) throw new Crash("after the PSP charged, BEFORE the FINISHED commit");

    // ── PHASE 2: ONE atomic commit — order + ledger + outbox + snapshot ────
    return this.finish(key, cart, charge);
  }

  /**
   * The FINISHED transaction. Order row, ledger entry, fulfillment event in
   * the OUTBOX (phase9: never dual-write to the DB and a broker — the event
   * commits WITH the state), and the key flipped to FINISHED with the response
   * snapshot. Crash before this → nothing visible; crash after → the retry
   * replays the receipt. "Order exists but key doesn't" is unrepresentable —
   * and that is exactly the state that produces double charges.
   */
  private finish(key: string, cart: Cart, charge: ChargeRow): ApiResponse {
    const orderId = `order_${this.nextOrderNo++}`;
    const response: ApiResponse = {
      httpStatus: 201,
      body: { orderId, chargeId: charge.chargeId, amountCents: cart.amountCents },
    };
    this.db.transaction((tx) => {
      tx.put(ORDERS, orderId, { orderId, userId: cart.userId, sku: cart.sku, status: "PAID" });
      tx.put(LEDGER, `led_${orderId}`, {
        orderId,
        debit: cart.userId,
        credit: "merchant",
        amountCents: cart.amountCents,
      });
      const evt: FulfillmentEvent = { id: `evt_fulfill_${orderId}`, orderId, amountCents: cart.amountCents };
      tx.put(OUTBOX, evt.id, { id: evt.id, event: evt, sent: false });
      const claim = this.db.get(KEYS, key) as KeyRow;
      tx.put(KEYS, key, { ...claim, status: "FINISHED", response });
    });
    log(`   💾 ONE commit: ${orderId} + ledger + outbox event + key ${key} → FINISHED (snapshot stored)`);
    return response;
  }

  /**
   * The SWEEPER — the owner of the RUNNING state. A crash between the two
   * phases strands a key at RUNNING; the sweeper disambiguates by asking the
   * PSP about the derived key: charge exists → ROLL FORWARD to FINISHED;
   * no charge → RELEASE the key so a clean retry can re-claim. (In production
   * "stale" means a lease/timestamp expired; here every RUNNING key at sweep
   * time is stale, for determinism.)
   */
  sweepStaleRunning(): void {
    for (const row of this.db.rows(KEYS) as KeyRow[]) {
      if (row.status !== "RUNNING") continue;
      const charge = this.psp.findByKey(derivedPspKey(row.key));
      if (charge) {
        log(`   🧹 sweeper: key ${row.key} stale RUNNING, PSP HAS ${charge.chargeId} → roll FORWARD`);
        this.finish(row.key, row.payload, charge);
      } else {
        log(`   🧹 sweeper: key ${row.key} stale RUNNING, PSP has NO charge → RELEASE key (clean retry allowed)`);
        this.db.transaction((tx) => tx.del(KEYS, row.key));
      }
    }
  }
}

// ─── The OUTBOX RELAY (phase9, condensed) — at-least-once on purpose ────────
// Publishes unsent outbox rows to fulfillment. Deterministic fault: it crashes
// once after delivering a row but BEFORE marking it sent, so the restart
// re-delivers that row — the duplicate the consumer must (and does) absorb.
class OutboxRelay {
  private db: Database;
  private consumer: FulfillmentConsumer;
  public deliveries = 0; // wire deliveries, duplicates included
  private crashAfterFirstDelivery: boolean;

  constructor(db: Database, consumer: FulfillmentConsumer) {
    this.db = db;
    this.consumer = consumer;
    this.crashAfterFirstDelivery = true; // fault fires exactly once, then never again
  }

  drain(): void {
    for (const row of this.db.rows(OUTBOX)) {
      if (row.sent) continue;
      this.deliveries += 1;
      this.consumer.handle(row.event as FulfillmentEvent);
      if (this.crashAfterFirstDelivery) {
        this.crashAfterFirstDelivery = false;
        throw new Crash("after delivering an outbox row, BEFORE marking it sent");
      }
      this.db.transaction((tx) => tx.put(OUTBOX, row.id as string, { ...row, sent: true }));
    }
  }
}

// ─── RECONCILIATION — the backstop that PROVES the invariant ────────────────
// Every PSP charge must map to exactly one FINISHED idempotency key (and vice
// versa). Count = 0 is the goal; every firing is a sev on the prevention
// layer, not something to quietly mop up.
function reconcile(db: Database, psp: PaymentProvider): number {
  let anomalies = 0;
  const finishedByPspKey = new Map<string, KeyRow>();
  for (const row of db.rows(KEYS) as KeyRow[]) {
    if (row.status === "FINISHED") finishedByPspKey.set(derivedPspKey(row.key), row);
  }
  for (const charge of psp.charges()) {
    if (!finishedByPspKey.has(charge.idempotencyKey)) {
      anomalies += 1;
      log(`   🚨 reconciliation: charge ${charge.chargeId} has NO matching FINISHED key — page someone`);
    }
  }
  for (const [pspKey, row] of finishedByPspKey) {
    if (!psp.findByKey(pspKey)) {
      anomalies += 1;
      log(`   🚨 reconciliation: key ${row.key} FINISHED but PSP has no charge — page someone`);
    }
  }
  return anomalies;
}

// ═════════════════════════════════════════════════════════════════════════════
// THE DEMO — one user, five purchase intents, every failure the design names.
// ═════════════════════════════════════════════════════════════════════════════
function main(): void {
  const db = new Database();
  const psp = new PaymentProvider();
  const server = new CheckoutService(db, psp);
  const cart: Cart = { userId: "user_1", sku: "sku_headphones", amountCents: 4999 };

  // ── A) DOUBLE-CLICK / RETRY WITH THE SAME KEY ─────────────────────────────
  log("═══ A) double-click: same intent, same client-minted key, three arrivals ═══");
  // The client minted idem_1 for THIS intent and persisted it before sending —
  // every retry of this Buy click carries the same key.
  const first = server.purchase("idem_1", cart, {
    whileRunning: () => {
      // The second click races the first — it arrives while idem_1 is RUNNING.
      log("   👆 duplicate click lands MID-FLIGHT:");
      const dup = server.purchase("idem_1", cart);
      log(`   ← duplicate got HTTP ${dup.httpStatus} — no second execution started`);
    },
  });
  log(`   ← first click got HTTP ${first.httpStatus}: ${JSON.stringify(first.body)}`);
  log("   👆 a LATE retry of the same click (key idem_1) arrives after completion:");
  const late = server.purchase("idem_1", cart);
  log(`   ← late retry got HTTP ${late.httpStatus}: ${JSON.stringify(late.body)} — the SAME receipt, byte for byte`);
  log("   👆 and a buggy client reuses idem_1 for a DIFFERENT cart:");
  const misuse = server.purchase("idem_1", { ...cart, amountCents: 9999 });
  log(`   ← got HTTP ${misuse.httpStatus} — surfaced as a bug, not honored\n`);

  // ── B) RESPONSE LOST AFTER THE CHARGE SUCCEEDED ───────────────────────────
  log("═══ B) success, but the RESPONSE is lost on the way back to the client ═══");
  server.purchase("idem_2", cart); // ...the 201 never reaches the client
  log("   🕳  the 201 receipt vanished in the network — client saw an error and retries idem_2:");
  const replayed = server.purchase("idem_2", cart);
  log(`   ← retry got HTTP ${replayed.httpStatus}: ${JSON.stringify(replayed.body)}`);
  log("   ✅ the ORIGINAL stored response — a hiccup for the user, not a second charge\n");

  // ── C) THE PSP CALL ITSELF IS AMBIGUOUS ───────────────────────────────────
  log("═══ C1) PSP times out AFTER charging — retry with the same derived key ═══");
  const c1 = server.purchase("idem_3", cart, { pspTimesOut: true });
  log(`   ← got HTTP ${c1.httpStatus}: ${JSON.stringify(c1.body)} — one charge despite the ambiguity\n`);

  log("═══ C2) crash AFTER the charge, BEFORE the FINISHED commit → sweeper rolls forward ═══");
  try {
    server.purchase("idem_4", cart, { crashBeforeFinish: true });
  } catch (err) {
    log(`   ${(err as Error).message}`);
  }
  log("   → key idem_4 is stranded at RUNNING; the money moved but our DB doesn't know yet");
  server.sweepStaleRunning(); // asks the PSP by derived key → charge exists → roll forward
  log("   👆 the client's retry of idem_4 now simply replays the receipt:");
  const c2 = server.purchase("idem_4", cart);
  log(`   ← got HTTP ${c2.httpStatus}: ${JSON.stringify(c2.body)}\n`);

  log("═══ C3) crash BEFORE the PSP was ever called → sweeper releases the key ═══");
  try {
    server.purchase("idem_5", cart, { crashBeforeCharge: true });
  } catch (err) {
    log(`   ${(err as Error).message}`);
  }
  server.sweepStaleRunning(); // PSP has no charge for idem_5 → release, retry is clean
  log("   👆 the client's retry of idem_5 re-claims and executes cleanly:");
  const c3 = server.purchase("idem_5", cart);
  log(`   ← got HTTP ${c3.httpStatus}: ${JSON.stringify(c3.body)}\n`);

  // ── D) DOWNSTREAM FULFILLMENT VIA THE OUTBOX ──────────────────────────────
  log("═══ D) outbox relay → fulfillment: at-least-once wire, exactly-once effect ═══");
  // Every order above committed its fulfillment event WITH the order (phase9:
  // no dual-write). The relay now drains — and crashes mid-way on purpose.
  const consumer = new FulfillmentConsumer();
  const relay = new OutboxRelay(db, consumer);
  try {
    relay.drain();
  } catch (err) {
    log(`   ${(err as Error).message}`);
  }
  log("   → relay restarts and drains again — the unmarked row is RE-delivered:");
  relay.drain();
  log(`   → ${relay.deliveries} deliveries on the wire, ${consumer.shipped.length} orders actually shipped\n`);

  // ── E) THE INVARIANTS — after all the chaos ───────────────────────────────
  log("═══ E) invariant checks ═══");
  let allOk = true;
  function check(name: string, actual: number, expected: number): void {
    const ok = actual === expected;
    if (!ok) allOk = false;
    log(`   ${ok ? "✅" : "❌"} ${name}: ${actual}  (expected ${expected})`);
  }
  const intents = 5; // idem_1 … idem_5 — five Buy clicks the user MEANT to make
  log(`   ${server.requestsReceived} requests hit the server, ${psp.wireCalls} calls hit the PSP — for ${intents} intents:`);
  check("charge rows at the PSP        ", psp.charges().length, intents);
  check("order rows in our DB          ", db.rows(ORDERS).length, intents);
  check("ledger entries (money moved)  ", db.rows(LEDGER).length, intents);
  check("fulfillments shipped          ", consumer.shipped.length, intents);
  check("reconciliation anomalies      ", reconcile(db, psp), 0);

  log("");
  log("──────────────────────────────────────────────────────────────────────");
  log("TAKEAWAY: exactly-once across a network is a myth at the transport layer");
  log("and a design at the application layer: at-least-once delivery plus");
  log("idempotent processing. The key is claimed atomically in the same");
  log("transaction that creates the order — after that, duplicates don't fail,");
  log("they replay the receipt. Derived key to the PSP, outbox to downstream,");
  log("sweeper for stuck RUNNING, reconciliation to prove it all held.");

  process.exit(allOk ? 0 : 1);
}

main();
