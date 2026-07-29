/**
 * The WORKER process — run with:  npm run phase2:worker
 *
 * This is a long-running process. Start it, leave it running, then run the
 * producer in another terminal. The worker will pick up jobs the moment they
 * land in Redis — even jobs produced before it started, because they persist.
 *
 * Compare to Phase 1's `queue.consume(handler)`. BullMQ gives you the same
 * loop (receive → run → ack/retry) but you didn't have to write it.
 */

import { Worker, Queue, type Job } from "bullmq";
import { connection, QUEUE_NAME, type EmailJob } from "./connection.ts";
import { log } from "../phase1/queue.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * A real dead-letter queue. BullMQ keeps exhausted jobs in a "failed" set, but
 * a common production pattern is to also push them onto a separate queue you
 * monitor/alert on. We do that here so the DLQ concept from Phase 1 carries over.
 */
const deadLetterQueue = new Queue(`${QUEUE_NAME}-dead-letter`, { connection });

// ─── The processor: same logic as Phase 1's handler ──────────────────────
const worker = new Worker<EmailJob>(
  QUEUE_NAME,
  async (job: Job<EmailJob>) => {
    log(`  [worker] processing ${job.id} (attempt ${job.attemptsMade + 1})`, job.data);
    await sleep(300); // simulate network latency

    if (job.data.to === "broken@example.com") {
      throw new Error("SMTP 550: mailbox unavailable"); // fails every attempt
    }

    log(`      ✉  sent "${job.data.subject}" to ${job.data.to}`);
    return { sentAt: Date.now() }; // return value is stored on the completed job
  },
  {
    connection,
    concurrency: 2, // same as Phase 1: two jobs in flight at once
  },
);

// ─── Lifecycle events (BullMQ's version of Phase 1's log lines) ───────────
worker.on("completed", (job) => {
  log(`✓ completed ${job.id}`); // the "ack"
});

worker.on("failed", async (job, err) => {
  if (!job) return;
  const willRetry = job.attemptsMade < (job.opts.attempts ?? 1);
  if (willRetry) {
    log(`↩ ${job.id} failed ("${err.message}") — will retry (backoff)`);
  } else {
    log(`☠ ${job.id} exhausted retries — moving to dead-letter queue`);
    await deadLetterQueue.add("dead", { original: job.data, error: err.message });
  }
});

log("worker online — waiting for jobs on queue:", QUEUE_NAME);
log("(leave this running; produce jobs in another terminal with: npm run phase2:produce)");
