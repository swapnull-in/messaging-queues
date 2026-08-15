/**
 * Phase 12 — BACKPRESSURE: what a fast producer + a slow consumer do to a queue,
 * and the four ways to respond.
 *
 * Run with:  node "src/phase12/backpressure.ts"
 *
 * A queue is a shock absorber: it smooths out bursts so a brief spike in
 * producers doesn't overwhelm the consumers. That works only while producers
 * and consumers balance OVER TIME. The moment the arrival rate P stays above
 * the drain rate C, the queue is no longer absorbing a burst — it's absorbing
 * a permanent deficit, and a buffer with no ceiling will absorb it right up to
 * the point the process runs out of memory and dies.
 *
 * We model discrete ticks. Every tick:
 *   - the producer wants to enqueue P messages,
 *   - the consumer drains up to C messages   (with C < P, a sustained overload).
 *
 * CONSUMER LAG = how many messages are sitting in the buffer, unprocessed.
 * Lag is THE backpressure signal. Not CPU, not queue "health" — LAG, and
 * especially its time-derivative (d(lag)/dt): if lag rises every tick, the
 * system is falling behind and will never catch up on its own. End-to-end
 * latency tracks lag directly (a message waits ~ lag / C ticks before it's
 * even looked at), so rising lag = rising latency.
 *
 * There are exactly four responses to sustained overload:
 *
 *   A) DO NOTHING (unbounded buffer)  — the bug. Lag and latency climb forever
 *      until OOM. An unbounded queue converts an overload into a crash.
 *   B) BLOCK the producer (bounded buffer, flow control) — cap the buffer; when
 *      it's full, make the producer WAIT. The "slow down" signal propagates
 *      backward to the source. Throughput settles at C; lag is bounded. This is
 *      how TCP and reactive-streams backpressure work.
 *   C) SHED load (bounded buffer, drop / 429) — cap the buffer; when it's full,
 *      DROP messages or reject them (HTTP 429). Lag and latency stay bounded at
 *      the cost of lost work. Right choice when you CAN'T slow the source
 *      (inbound HTTP, market data) and stale work is worthless.
 *   D) SCALE consumers — add workers to raise C above P. Works... until every
 *      worker hammers the same downstream (the DB). Then the DB becomes the
 *      bottleneck and more consumers just move the queue from your process into
 *      the database. Cap concurrency at what the downstream can actually take.
 *
 * TAKEAWAY: an unbounded queue converts an overload into an out-of-memory
 * crash. Consumer lag is the signal; respond by BLOCKING the producer (flow
 * control), SHEDDING load (drop / 429), or scaling consumers up to the point
 * the downstream DB becomes the real bottleneck. Never let the buffer grow
 * without a bound.
 */

/** Timestamped logging so you can watch the flow happen in real time. */
function log(...args: unknown[]): void {
  const t = new Date().toISOString().slice(11, 23); // HH:MM:SS.mmm
  console.log(`[${t}]`, ...args);
}

const TICKS = 12;
const P = 10; // producer wants to enqueue this many messages per tick
const C = 6; //  consumer can drain at most this many per tick  (C < P → overload)

/** Draw a little bar so the buffer growth is visual, not just numbers. */
function bar(n: number): string {
  const capped = Math.min(n, 60);
  return "█".repeat(capped) + (n > 60 ? "…" : "");
}

/**
 * One row of the per-tick table. `latency` is the estimated end-to-end wait a
 * message enqueued this tick will see: everything ahead of it must drain first,
 * so wait ≈ buffer / C ticks.
 */
function report(tick: number, buffer: number, dropped: number, blocked: number): void {
  const latency = (buffer / C).toFixed(1);
  const extra =
    dropped > 0 ? `  dropped=${dropped}` : blocked > 0 ? `  blocked=${blocked}` : "";
  log(
    `t${String(tick).padStart(2, "0")}  lag=${String(buffer).padStart(3)}  ` +
      `wait≈${latency.padStart(4)}t${extra}  ${bar(buffer)}`,
  );
}

// ─── A) UNBOUNDED — the problem ───────────────────────────────────────────────
// No ceiling. Each tick we add P and remove C. The P−C surplus (=4/tick here)
// has nowhere to go, so it accumulates. Lag and latency rise linearly and never
// stop. In a real process this array is your heap; eventually the process OOMs.
function unbounded(): void {
  log("═══ A) UNBOUNDED buffer — do nothing, hope it's fine ═══");
  log(`   producer P=${P}/tick, consumer C=${C}/tick, surplus=${P - C}/tick`);
  let buffer = 0;
  for (let t = 1; t <= TICKS; t++) {
    buffer += P; //         producer enqueues, never told to stop
    const drained = Math.min(C, buffer);
    buffer -= drained; //   consumer drains what it can
    report(t, buffer, 0, 0);
  }
  const dLag = P - C;
  log(`   → lag rises by ${dLag} EVERY tick (d(lag)/dt=+${dLag}); it never converges.`);
  log(`   → this is the signal: a positive, non-decreasing lag derivative = overload.`);
  log("");
}

// ─── B) BOUNDED + BLOCK — flow control ────────────────────────────────────────
// Cap the buffer at CAP. The consumer drains C; that frees C slots. The producer
// may only enqueue what fits — the rest is BLOCKED (the producer's enqueue call
// waits). That "wait" is the backpressure signal travelling BACKWARD to the
// source. Steady state: producer is throttled to exactly C, buffer pinned near
// CAP, lag and latency bounded. Nothing is lost; the source just runs slower.
function boundedBlock(): void {
  const CAP = 12;
  log(`═══ B) BOUNDED + BLOCK (cap=${CAP}) — throttle the producer (flow control) ═══`);
  let buffer = 0;
  for (let t = 1; t <= TICKS; t++) {
    const drained = Math.min(C, buffer);
    buffer -= drained; //           consumer drains first, freeing space
    const space = CAP - buffer;
    const admitted = Math.min(P, space); // producer only enqueues what fits...
    const blocked = P - admitted; //        ...the rest is held back (blocked)
    buffer += admitted;
    report(t, buffer, 0, blocked);
  }
  log(`   → throughput settles at the consumer's rate C=${C}; lag capped at ${CAP}.`);
  log(`   → the producer is FORCED to slow to C. TCP / reactive-streams do exactly this.`);
  log("");
}

// ─── C) SHED / DROP — load shedding (429) ─────────────────────────────────────
// Same cap, but we CAN'T slow the source (it's inbound HTTP). When the buffer is
// full we DROP the overflow — or reply 429 "Too Many Requests". Lag and latency
// stay bounded exactly like the BLOCK case; the cost is lost work instead of a
// slowed producer. Correct when stale work is worthless (live prices, sensor
// readings) and the source won't take "wait" for an answer.
function shed(): void {
  const CAP = 12;
  log(`═══ C) SHED / DROP (cap=${CAP}) — drop / reject with 429 ═══`);
  let buffer = 0;
  let totalDropped = 0;
  for (let t = 1; t <= TICKS; t++) {
    const drained = Math.min(C, buffer);
    buffer -= drained;
    const space = CAP - buffer;
    const admitted = Math.min(P, space); // accept what fits...
    const dropped = P - admitted; //        ...drop (or 429) the overflow
    totalDropped += dropped;
    buffer += admitted;
    report(t, buffer, dropped, 0);
  }
  log(`   → lag capped at ${CAP}, latency bounded — but ${totalDropped} messages were shed.`);
  log(`   → use when the source can't be slowed and stale work is worthless.`);
  log("");
}

// ─── D) THE AUTOSCALE TRAP ────────────────────────────────────────────────────
// "Just add consumers." Scaling C above P does drain the backlog — until every
// worker calls the SAME downstream (one DB). The DB has its own ceiling, D. Past
// that, extra consumers don't add throughput; they just pile requests onto the
// DB's connection pool. You haven't removed the queue — you've MOVED it from
// your process into the database, where it's harder to see and hurts everything.
function autoscaleTrap(): void {
  const DB_LIMIT = 8; // the shared downstream can absorb at most this many/tick
  log(`═══ D) THE AUTOSCALE TRAP — downstream DB caps out at ${DB_LIMIT}/tick ═══`);
  log("   worker count → effective throughput (bounded by the shared DB):");
  for (const workers of [1, 2, 4, 8, 16]) {
    const wantedThroughput = workers * 3; // each worker would like to do 3/tick
    const effective = Math.min(wantedThroughput, DB_LIMIT);
    const wastedAtDb = wantedThroughput - effective;
    const note =
      effective < DB_LIMIT
        ? "under DB limit — scaling still helps"
        : wastedAtDb === 0
          ? "exactly at DB limit — this is the ceiling"
          : `DB SATURATED — ${wastedAtDb}/tick just queues inside the DB`;
    log(
      `   ${String(workers).padStart(2)} workers → want ${String(wantedThroughput).padStart(
        2,
      )}/tick → get ${String(effective).padStart(2)}/tick   (${note})`,
    );
  }
  log(`   → past the DB's limit, more consumers move the queue INTO the database.`);
  log(`   → cap consumer concurrency at what the downstream can actually take.`);
  log("");
}

async function main(): Promise<void> {
  log("BACKPRESSURE: one fast producer, one slow consumer, four responses.");
  log("Watch column 'lag' — that is consumer lag, THE backpressure signal.");
  log("");

  // A tiny delay between sections so the output reads as distinct scenes.
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

  unbounded();
  await sleep(50);
  boundedBlock();
  await sleep(50);
  shed();
  await sleep(50);
  autoscaleTrap();

  log("──────────────────────────────────────────────────────────────────────");
  log("Takeaway: an unbounded queue converts an overload into an OOM crash.");
  log("Consumer lag is the signal; respond by BLOCKING the producer (flow");
  log("control), SHEDDING load (drop / 429), or scaling consumers up to the");
  log("point the downstream DB becomes the real bottleneck. Never let the");
  log("buffer grow without a bound.");

  process.exit(0);
}

main();
