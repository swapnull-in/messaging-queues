/**
 * Phase 6 — KAFKA-STYLE STREAMING: the log, consumer groups, offsets, replay.
 *
 * Run with:  npm run phase6   (needs a Kafka broker on localhost:9092 —
 * we use Redpanda, a single-container Kafka-compatible broker)
 *
 * ─── The big mental shift ────────────────────────────────────────────────
 * Every queue so far DELETES a message once it's processed. Kafka doesn't. A
 * topic is an append-only LOG that is RETAINED (for days, or forever). Reading
 * doesn't remove anything — each consumer just tracks its own position (OFFSET)
 * in the log. That unlocks things a queue can't do:
 *
 *   - REPLAY:        reset your offset to 0 and re-read all of history.
 *   - MANY READERS:  independent consumer GROUPS each read the full stream at
 *                    their own pace, with their own offset. Add a new consumer
 *                    of old data any time — the log is still there.
 *   - PARTITIONS:    the log is split into partitions for parallelism; messages
 *                    with the same KEY always go to the same partition, so
 *                    per-key ORDER is preserved.
 *
 *   Topic "events"  (offsets never rewind unless you ask):
 *     partition 0: [e0][e3] ...
 *     partition 1: [e1][e4] ...     group "realtime"  reads all, offset=head
 *     partition 2: [e2][e5] ...     group "batch"     reads all, its own offset
 *
 * Queue = "do this work once, then forget it".  Kafka = "here is the stream of
 * everything that happened; read it however and whenever you like."
 */

import { Kafka, logLevel } from "kafkajs";
import { log } from "../phase1/queue.ts";

const TOPIC = "events";
const kafka = new Kafka({ clientId: "phase6", brokers: ["localhost:9092"], logLevel: logLevel.NOTHING });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Consume a whole topic from the beginning with one group, stop after `expect` msgs. */
async function readAll(groupId: string, expect: number): Promise<string[]> {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  const got: string[] = [];
  await new Promise<void>(async (resolve) => {
    const done = setTimeout(resolve, 6_000); // safety valve
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const value = message.value?.toString() ?? "";
        got.push(value);
        log(`   [group ${groupId}] read "${value}"  (partition ${partition}, offset ${message.offset})`);
        if (got.length >= expect) { clearTimeout(done); resolve(); }
      },
    });
  });
  await consumer.disconnect();
  return got;
}

async function main() {
  const admin = kafka.admin();
  await admin.connect();
  // Fresh topic each run so the demo is reproducible: 3 partitions.
  await admin.deleteTopics({ topics: [TOPIC] }).catch(() => {});
  await sleep(500);
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 3 }] });
  await admin.disconnect();
  log(`created topic "${TOPIC}" with 3 partitions`);

  // ─── Produce: 6 events keyed by user (same key → same partition → order) ─
  const producer = kafka.producer();
  await producer.connect();
  const events = [
    { key: "user-A", value: "A: login" },
    { key: "user-B", value: "B: login" },
    { key: "user-A", value: "A: add-to-cart" },
    { key: "user-C", value: "C: login" },
    { key: "user-A", value: "A: checkout" },
    { key: "user-B", value: "B: logout" },
  ];
  for (const e of events) await producer.send({ topic: TOPIC, messages: [e] });
  await producer.disconnect();
  log(`produced ${events.length} events (user-A's 3 events all land on one partition, in order)`);
  log("");

  // ─── Group 1 reads the whole stream ─────────────────────────────────────
  log('═══ consumer group "realtime" reads the log ═══');
  await readAll("realtime", events.length);

  // ─── Group 2 reads the SAME stream independently — the log is still there ─
  log("");
  log('═══ consumer group "batch" reads the SAME log — nothing was consumed away ═══');
  const batch = await readAll("batch", events.length);

  log("");
  log(`Both groups read all ${events.length} events. In a queue, "realtime" taking a`);
  log(`message would have removed it — "batch" got ${batch.length}/${events.length} because the log is`);
  log("RETAINED and each group has its own offset. That's replay + fan-out: the");
  log("superpower that makes Kafka an event stream, not just a task queue.");

  process.exit(0);
}

main().catch((err) => {
  console.error("Phase 6 error:", err.message);
  console.error("Is the broker up?  docker run -d --name redpanda -p 9092:9092 \\");
  console.error("  docker.redpanda.com/redpandadata/redpanda redpanda start --mode dev-container");
  process.exit(1);
});
