/**
 * Phase 2 self-contained demo — run with:  npm run phase2:demo
 *
 * The "real" way to run BullMQ is separate processes (see worker.ts /
 * producer.ts). But for a quick, Phase-1-style narrated run, this file starts a
 * worker, produces the same 4 jobs, watches them drain, prints stats, and exits.
 *
 * Run `npm run phase1` and `npm run phase2:demo` back to back — the output is
 * deliberately similar so you can see it's the SAME queue, different engine.
 */

import { Queue, Worker, QueueEvents, type Job } from "bullmq";
import { connection, QUEUE_NAME, type EmailJob } from "./connection.ts";
import { log } from "../phase1/queue.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

const queue = new Queue<EmailJob>(QUEUE_NAME, { connection });
const events = new QueueEvents(QUEUE_NAME, { connection });
const deadLetterQueue = new Queue(`${QUEUE_NAME}-dead-letter`, { connection });

const worker = new Worker<EmailJob>(
  QUEUE_NAME,
  async (job: Job<EmailJob>) => {
    log(`  [worker] processing ${job.id} (attempt ${job.attemptsMade + 1})`);
    await sleep(300);
    if (job.data.to === "broken@example.com") {
      throw new Error("SMTP 550: mailbox unavailable");
    }
    log(`      ✉  sent "${job.data.subject}" to ${job.data.to}`);
  },
  { connection, concurrency: 2 },
);

worker.on("completed", (job) => log(`✓ completed ${job.id}`));
worker.on("failed", async (job, err) => {
  if (!job) return;
  if (job.attemptsMade < (job.opts.attempts ?? 1)) {
    log(`↩ ${job.id} failed — will retry`);
  } else {
    log(`☠ ${job.id} exhausted retries — dead-lettering`);
    await deadLetterQueue.add("dead", { original: job.data, error: err.message });
  }
});

async function main() {
  await events.waitUntilReady();

  const jobs: EmailJob[] = [
    { to: "alice@example.com", subject: "Welcome, Alice!" },
    { to: "bob@example.com", subject: "Welcome, Bob!" },
    { to: "broken@example.com", subject: "Welcome!" },
    { to: "carol@example.com", subject: "Welcome, Carol!" },
  ];
  for (const data of jobs) {
    const job = await queue.add("send-email", data, {
      attempts: 3,
      backoff: { type: "exponential", delay: 500 },
      removeOnComplete: true,
      removeOnFail: false,
    });
    log("→ produced", job.id, data);
  }

  // Let jobs drain, including exponential-backoff retries for the broken one.
  await sleep(6_000);

  const counts = await queue.getJobCounts();
  const dlqCount = await deadLetterQueue.count();
  log("──────────────────────────────────────────");
  log("final job counts:", counts);
  log(`dead-letter queue has ${dlqCount} message(s)`);

  await worker.close();
  await queue.close();
  await events.close();
  await deadLetterQueue.close();
  process.exit(0);
}

main();
