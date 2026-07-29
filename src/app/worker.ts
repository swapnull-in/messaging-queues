/**
 * The delivery WORKER (the CONSUMER) — run with:  npm run app:worker
 *
 * Pulls delivery jobs off the queue and actually makes the HTTP call to the
 * receiver. This is where all the reliability you learned lives:
 *
 *   - retries + exponential backoff  → configured on the job (see queue.ts)
 *   - dead-letter after 5 attempts   → we push exhausted jobs to the DLQ
 *   - concurrency                    → deliver several webhooks at once
 *
 * Run several of these processes and they share the load automatically — that's
 * the "distributed" property. The queue lives in Redis, not in any one process.
 */

import { Worker, type Job } from "bullmq";
import { connection, DELIVERY_QUEUE, deadLetterQueue, type DeliveryJob } from "./queue.ts";
import { log } from "../phase1/queue.ts";

const worker = new Worker<DeliveryJob>(
  DELIVERY_QUEUE,
  async (job: Job<DeliveryJob>) => {
    const { url, event, payload } = job.data;
    log(`  [worker] delivering ${job.id} → ${url} (attempt ${job.attemptsMade + 1})`);

    // The actual side effect: POST the event to the receiver's URL.
    // We add a timeout so a hung receiver doesn't block a worker forever.
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json", "x-webhook-event": event },
      body: JSON.stringify({ event, payload, deliveryId: job.id, attempt: job.attemptsMade + 1 }),
      signal: AbortSignal.timeout(5_000),
    });

    // Webhook convention: any 2xx means the receiver accepted it. Anything else
    // (or a thrown network error) counts as failure → BullMQ retries with backoff.
    if (!res.ok) {
      throw new Error(`receiver returned HTTP ${res.status}`);
    }

    log(`      ✓ delivered ${job.id} (HTTP ${res.status})`);
  },
  { connection, concurrency: 5 },
);

worker.on("completed", (job) => log(`✓ ${job.id} completed`));

worker.on("failed", async (job, err) => {
  if (!job) return;
  const attemptsAllowed = job.opts.attempts ?? 1;
  if (job.attemptsMade < attemptsAllowed) {
    log(`↩ ${job.id} failed ("${err.message}") — retry ${job.attemptsMade}/${attemptsAllowed} with backoff`);
  } else {
    log(`☠ ${job.id} exhausted ${attemptsAllowed} attempts — dead-lettering`);
    await deadLetterQueue.add("dead", {
      original: job.data,
      error: err.message,
      failedAt: job.finishedOn,
    });
  }
});

log("delivery worker online — waiting for webhook jobs");
log("(leave running; start the API with `npm run app:server` and a target with `npm run app:target`)");
