/**
 * Phase 4 — SCHEDULING: delayed jobs, repeatable (cron) jobs, rate limiting.
 *
 * Run with:  npm run phase4   (needs Redis)
 *
 * So far every job ran ASAP. But real systems need time control:
 *
 *   - DELAYED   — "send this reminder in 24h", "retry this in 5 min".
 *   - REPEATABLE— "generate a report every night", "poll this API every 30s".
 *   - RATE LIMIT— "call this 3rd-party API at most 2×/second or we get banned".
 *
 * BullMQ + Redis give you all three for free. In Phase 1 you'd have had to
 * hand-write timers and token buckets; here they're config.
 *
 * This runs two short demos back-to-back so each idea is easy to watch.
 */

import { Queue, Worker, type Job } from "bullmq";
import { log } from "../phase1/queue.ts";

const connection = { host: "127.0.0.1", port: 6379, maxRetriesPerRequest: null };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ─── Demo A: DELAYED + REPEATABLE jobs ────────────────────────────────────
async function demoDelayedAndRepeatable() {
  log("═══ Demo A: delayed & repeatable jobs ═══");
  const queue = new Queue("sched-timed", { connection });

  const worker = new Worker(
    "sched-timed",
    async (job: Job) => {
      const lateBy = Date.now() - job.data.scheduledFor;
      log(`   ▶ ran "${job.name}" ${job.data.label}` +
          (job.data.scheduledFor ? `  (fired ~${lateBy}ms after its target)` : ""));
    },
    { connection },
  );

  // 1) A DELAYED job — enqueued now, but must not run for 2 seconds.
  const target = Date.now() + 2_000;
  await queue.add("reminder", { label: "(delayed 2s)", scheduledFor: target }, { delay: 2_000 });
  log("→ scheduled a DELAYED job to run in 2s (watch the gap below)");

  // 2) A REPEATABLE job — fires every 1s on a schedule Redis manages for you.
  //    You could also pass a cron string, e.g. pattern: '0 9 * * *' for 9am daily.
  await queue.add(
    "heartbeat",
    { label: "(repeats every 1s)" },
    { repeat: { every: 1_000 }, removeOnComplete: true },
  );
  log("→ registered a REPEATABLE job firing every 1s");

  await sleep(4_500); // watch: heartbeat ~4×, delayed reminder once at ~2s

  // Clean up the repeat schedule so it doesn't keep firing forever in Redis.
  for (const s of await queue.getJobSchedulers()) {
    await queue.removeJobScheduler(s.key);
  }
  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
  log("");
}

// ─── Demo B: RATE LIMITING ────────────────────────────────────────────────
async function demoRateLimiting() {
  log("═══ Demo B: rate limiting (max 2 jobs / second) ═══");
  const queue = new Queue("sched-rate", { connection });

  const worker = new Worker(
    "sched-rate",
    async (job: Job) => {
      log(`   ▶ processed ${job.name} #${job.data.n}`);
    },
    {
      connection,
      // The limiter: at most `max` jobs are started per `duration` ms, no
      // matter how many are waiting or how high concurrency is. Perfect for
      // staying under a 3rd-party API's rate cap.
      limiter: { max: 2, duration: 1_000 },
    },
  );

  // Produce a burst of 6 jobs all at once...
  for (let n = 1; n <= 6; n++) {
    await queue.add("api-call", { n });
  }
  log("→ produced 6 jobs at once — but the limiter releases only 2 per second");
  log("  (watch the timestamps: ~2 jobs, pause, ~2 more, pause, ~2 more)");

  await sleep(4_000); // 6 jobs at 2/sec ≈ 3 seconds

  await worker.close();
  await queue.obliterate({ force: true });
  await queue.close();
}

async function main() {
  await demoDelayedAndRepeatable();
  await demoRateLimiting();
  log("");
  log("Takeaway: time control (delay / repeat / rate) is config on a real broker,");
  log("not code you maintain. Redis persists the schedule, so it survives restarts.");
  process.exit(0);
}

main();
