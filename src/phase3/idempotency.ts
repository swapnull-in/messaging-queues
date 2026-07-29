/**
 * Phase 3 — IDEMPOTENCY: the most important queue skill.
 *
 * Run with:  npm run phase3
 *
 * ─── The problem ─────────────────────────────────────────────────────────
 * Every real queue gives you AT-LEAST-ONCE delivery. That means a message can
 * be delivered MORE THAN ONCE. The classic way it happens:
 *
 *   1. worker pulls "charge card for order #42"
 *   2. worker charges the card  ✅  (the side effect happened!)
 *   3. worker CRASHES before it can ack
 *   4. the queue never got the ack, so it redelivers the message
 *   5. a new worker charges the card AGAIN  ❌  customer billed twice
 *
 * You cannot make delivery exactly-once at the transport layer (it's provably
 * impossible in a distributed system). So the fix is at the HANDLER layer:
 * make the work IDEMPOTENT — running it twice has the same effect as once.
 *
 * ─── The fix ─────────────────────────────────────────────────────────────
 * Give each unit of work a stable IDEMPOTENCY KEY (e.g. the order id). Before
 * doing the side effect, atomically check "have I already done this key?".
 * If yes, skip. If no, do it and record the key. Same idea whether the store
 * is Redis, Postgres, or a unique DB constraint.
 *
 * This file shows the SAME job processed twice, first WITHOUT protection
 * (double charge) then WITH it (charged once), so you can see the difference.
 */

import { log } from "../phase1/queue.ts";

// A fake "payments" system so we can see the damage. In real life this is
// Stripe, a bank API, an email send — any side effect you don't want doubled.
class PaymentGateway {
  public totalCharged = 0;
  private calls = 0;

  async charge(orderId: string, amountCents: number): Promise<void> {
    this.calls += 1;
    this.totalCharged += amountCents;
    log(`   💳 CHARGED order ${orderId}: $${(amountCents / 100).toFixed(2)}  (gateway call #${this.calls})`);
  }
}

// A tiny "already done" store. Real version: a Redis SET with SETNX, or a
// UNIQUE column in your database. The key property is ATOMIC check-and-set.
class ProcessedStore {
  private done = new Set<string>();

  /** Returns true if this key was NOT seen before (i.e. it's safe to proceed). */
  claim(key: string): boolean {
    if (this.done.has(key)) return false; // already processed — skip
    this.done.add(key);
    return true;
  }
}

interface ChargeJob {
  orderId: string;
  amountCents: number;
}

// ─── Handler WITHOUT idempotency — the bug ────────────────────────────────
async function unsafeHandler(gateway: PaymentGateway, job: ChargeJob) {
  await gateway.charge(job.orderId, job.amountCents); // no guard: runs every time
}

// ─── Handler WITH idempotency — the fix ───────────────────────────────────
async function safeHandler(
  gateway: PaymentGateway,
  store: ProcessedStore,
  job: ChargeJob,
) {
  // The idempotency key. For "charge this order" the natural key is the order
  // id — charging order #42 is the same operation no matter how many times the
  // message is delivered.
  const key = `charge:${job.orderId}`;

  if (!store.claim(key)) {
    log(`   ⏭  already processed ${key} — skipping (no double charge)`);
    return;
  }
  await gateway.charge(job.orderId, job.amountCents);
}

async function main() {
  const job: ChargeJob = { orderId: "42", amountCents: 4999 };

  log("═══ WITHOUT idempotency: the same message delivered twice ═══");
  const g1 = new PaymentGateway();
  await unsafeHandler(g1, job);
  await unsafeHandler(g1, job); // redelivery!
  log(`   → total charged: $${(g1.totalCharged / 100).toFixed(2)}  ❌ customer billed twice\n`);

  log("═══ WITH idempotency: the same message delivered twice ═══");
  const g2 = new PaymentGateway();
  const store = new ProcessedStore();
  await safeHandler(g2, store, job);
  await safeHandler(g2, store, job); // redelivery — but guarded
  log(`   → total charged: $${(g2.totalCharged / 100).toFixed(2)}  ✅ charged exactly once\n`);

  log("Lesson: you can't stop redelivery — you make the handler safe against it.");
  log("In BullMQ, a stable jobId is a second layer: queue.add(name, data, { jobId: 'charge-42' })");
  log("means adding the same job twice is a no-op at the QUEUE level, before a worker even runs.");
}

main();
