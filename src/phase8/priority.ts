/**
 * Phase 8 — PRIORITY QUEUES: let urgent work jump the line.
 *
 * Run with:  npm run phase8   (needs Redis)
 *
 * A plain queue is FIFO — first in, first out. But sometimes a job is more
 * urgent than others already waiting (a password-reset email shouldn't sit
 * behind 10,000 marketing emails). A PRIORITY queue reorders the waiting jobs
 * so higher-priority ones are handed out first, regardless of insertion order.
 *
 * In BullMQ: pass `priority` when adding a job. LOWER number = HIGHER priority
 * (1 beats 10). Jobs with no priority are treated as lowest.
 *
 * The demo adds a mixed batch, THEN starts the worker, so you can watch the
 * queue hand them out in priority order — not the order they were added.
 */

import { Queue, Worker, type Job } from "bullmq";
import { log } from "../phase1/queue.ts";

const connection = { host: "127.0.0.1", port: 6379, maxRetriesPerRequest: null };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface EmailJob {
  label: string;
}

async function main() {
  const queue = new Queue<EmailJob>("priority-emails", { connection });
  await queue.obliterate({ force: true }); // clean slate

  // Add a batch in a deliberately "wrong" order. Priorities: 1 = urgent,
  // 5 = normal, 10 = bulk. Note the urgent one is added LAST.
  const batch: Array<{ label: string; priority: number }> = [
    { label: "marketing blast #1", priority: 10 },
    { label: "marketing blast #2", priority: 10 },
    { label: "order receipt", priority: 5 },
    { label: "marketing blast #3", priority: 10 },
    { label: "order receipt #2", priority: 5 },
    { label: "PASSWORD RESET (urgent!)", priority: 1 }, // added last, but should run FIRST
  ];

  for (const { label, priority } of batch) {
    await queue.add("send", { label }, { priority });
    log(`→ added "${label}" (priority ${priority})`);
  }

  log("");
  log("Now starting the worker — watch the ORDER it processes them:");
  log("");

  const done: string[] = [];
  const worker = new Worker<EmailJob>(
    "priority-emails",
    async (job: Job<EmailJob>) => {
      await sleep(150);
      log(`   ▶ processed "${job.data.label}"  (priority ${job.opts.priority ?? "none"})`);
      done.push(job.data.label);
    },
    { connection, concurrency: 1 }, // one at a time so ordering is obvious
  );

  await sleep(2_000);

  log("");
  log("Processing order:");
  done.forEach((label, i) => log(`   ${i + 1}. ${label}`));
  log("");
  log("The password reset was added LAST but ran FIRST. The two receipts (p5)");
  log("beat the marketing blasts (p10). Priority reorders the waiting set —");
  log("insertion order only breaks ties within the same priority.");

  await worker.close();
  await queue.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Phase 8 error:", err.message, "\nIs Redis running? redis-cli ping");
  process.exit(1);
});
