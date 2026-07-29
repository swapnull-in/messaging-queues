/**
 * The PRODUCER process — run with:  npm run phase2:produce
 *
 * This adds a batch of jobs to Redis and exits. It does NOT process anything.
 * That's the whole point: the producer's job is to hand work off and move on.
 * If no worker is running yet, the jobs simply wait in Redis until one is.
 *
 * Compare to Phase 1's `queue.produce(body)` — same idea, but `queue.add()`
 * writes to Redis, so the job outlives this process.
 */

import { Queue } from "bullmq";
import { connection, QUEUE_NAME, type EmailJob } from "./connection.ts";
import { log } from "../phase1/queue.ts";

const queue = new Queue<EmailJob>(QUEUE_NAME, { connection });

const jobs: EmailJob[] = [
  { to: "alice@example.com", subject: "Welcome, Alice!" },
  { to: "bob@example.com", subject: "Welcome, Bob!" },
  { to: "broken@example.com", subject: "Welcome!" }, // will fail every attempt
  { to: "carol@example.com", subject: "Welcome, Carol!" },
];

for (const data of jobs) {
  const job = await queue.add("send-email", data, {
    attempts: 3, // retry up to 3 times (Phase 1's maxAttempts)
    backoff: {
      type: "exponential", // wait longer between each retry: 1s, 2s, 4s...
      delay: 1_000,
    },
    removeOnComplete: true, // tidy: drop successful jobs from Redis
    removeOnFail: false, // keep failed jobs so you can inspect them
  });
  log("→ produced", job.id, data);
}

log("all jobs queued. producer exiting — the worker will process them.");
await queue.close();
process.exit(0);
