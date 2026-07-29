/**
 * Phase 7 — PUB/SUB: fire-and-forget broadcast (the opposite of a queue).
 *
 * Run with:  npm run phase7   (needs Redis)
 *
 * A queue stores a message until *someone* processes it exactly once. Pub/Sub
 * does the opposite: a publisher broadcasts to a CHANNEL, and EVERY currently
 * connected subscriber gets a copy — instantly, with:
 *
 *   - no persistence  → if no one is listening, the message is gone forever
 *   - no acks         → the publisher never knows or cares who received it
 *   - no retries      → delivery is best-effort, fire-and-forget
 *
 *   publisher → (channel "news") → subscriber A   (gets it)
 *                                → subscriber B   (gets it)
 *                                → subscriber C   (offline → misses it)
 *
 * Use it for live, ephemeral signals: "user typing…", live dashboards, cache
 * invalidation, chat fan-out. NOT for work that must not be lost — use a queue.
 *
 * Note: a Redis connection in "subscriber mode" can't run normal commands, so
 * each subscriber needs its own connection. That's why we make several.
 */

import Redis from "ioredis";
import { log } from "../phase1/queue.ts";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const CHANNEL = "news";

async function main() {
  const publisher = new Redis();

  // Two subscribers online from the start.
  const subA = new Redis();
  const subB = new Redis();
  await subA.subscribe(CHANNEL);
  await subB.subscribe(CHANNEL);
  subA.on("message", (_ch, msg) => log(`   📥 [A] received: ${msg}`));
  subB.on("message", (_ch, msg) => log(`   📥 [B] received: ${msg}`));
  log("subscribers A and B are listening on channel:", CHANNEL);

  // Publish two messages — both live subscribers get each one.
  await sleep(100);
  log("→ publishing 'Breaking: queues are fun'");
  await publisher.publish(CHANNEL, "Breaking: queues are fun");
  await sleep(200);
  log("→ publishing 'Update: pub/sub has no memory'");
  await publisher.publish(CHANNEL, "Update: pub/sub has no memory");
  await sleep(300);

  // Now a THIRD subscriber joins late...
  log("");
  log("--- subscriber C connects now (after the first two messages) ---");
  const subC = new Redis();
  await subC.subscribe(CHANNEL);
  subC.on("message", (_ch, msg) => log(`   📥 [C] received: ${msg}`));
  await sleep(100);

  log("→ publishing 'Late news: C is here'");
  await publisher.publish(CHANNEL, "Late news: C is here");
  await sleep(300);

  log("");
  log("Notice: C got ONLY the message published after it connected — the first");
  log("two are gone forever. No queue, no backlog, no replay. That's the trade:");
  log("pub/sub is instant and cheap, but delivers only to who's listening NOW.");
  log("(Compare Phase 6/Kafka, which KEEPS the log so late readers can catch up.)");

  for (const c of [publisher, subA, subB, subC]) c.disconnect();
  process.exit(0);
}

main().catch((err) => {
  console.error("Phase 7 error:", err.message, "\nIs Redis running? redis-cli ping");
  process.exit(1);
});
