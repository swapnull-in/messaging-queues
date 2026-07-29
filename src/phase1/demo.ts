/**
 * Phase 1 demo — run with:  npm run phase1
 *
 * We model a realistic "send welcome email" job. Some emails succeed on the
 * first try; one address always fails so you can watch:
 *   retry → retry → dead-letter.
 *
 * Read the timestamped log from top to bottom to see the queue's life cycle.
 */

import { MessageQueue, log } from "./queue.ts";

interface EmailJob {
  to: string;
  subject: string;
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const queue = new MessageQueue<EmailJob>({
    maxAttempts: 3,          // try a message 3 times before dead-lettering
    visibilityTimeoutMs: 2_000,
  });

  // ─── The worker: pretends to send an email ────────────────────────────
  queue.consume(
    async (msg) => {
      await sleep(300); // simulate network latency

      // Pretend this one address is broken (bounces every time).
      if (msg.body.to === "broken@example.com") {
        throw new Error("SMTP 550: mailbox unavailable");
      }

      log(`      ✉  sent "${msg.body.subject}" to ${msg.body.to}`);
    },
    { concurrency: 2 }, // two emails in flight at once
  );

  // ─── The producer: enqueue a batch of jobs ────────────────────────────
  queue.produce({ to: "alice@example.com", subject: "Welcome, Alice!" });
  queue.produce({ to: "bob@example.com", subject: "Welcome, Bob!" });
  queue.produce({ to: "broken@example.com", subject: "Welcome!" }); // will fail
  queue.produce({ to: "carol@example.com", subject: "Welcome, Carol!" });

  // Let everything drain: successes ack, the broken one retries then DLQs.
  await sleep(4_000);

  log("──────────────────────────────────────────");
  log("final stats:", queue.stats());
  const dlq = queue.getDeadLetter();
  log(`dead-letter queue has ${dlq.length} message(s):`);
  for (const m of dlq) {
    log(`   ${m.id} → ${m.body.to} (failed ${m.attempts} times)`);
  }

  process.exit(0);
}

main();
