# Study Guide — Messaging & Event-Driven Systems

This repo is the runnable half of **[Core Course/04-messaging-event-driven.md](https://github.com/swapnull-in/system-design/blob/main/Core%20Course/04-messaging-event-driven.md)**: every concept the module argues in prose, a phase here demonstrates in a script you can run, read, and break. Study loop: read the module section, run the matching phase (`npm run phaseN`), read its heavily commented source top to bottom, then drill the flashcards in the web lab (`npm run web`) until the staff-level one-liners are automatic. Phases 2, 5, 6, and 7 use real infrastructure (Redis/BullMQ, RabbitMQ, Kafka/Redpanda, Redis Pub/Sub); everything else runs on plain Node.

## Phase → Module map

| Phase | What it builds | Module section | The staff insight |
|---|---|---|---|
| 1 | A queue from scratch: producer, consumer, ack, retry, dead-letter (~150 lines) | §3 Dead Letter Queues, §5 Visibility Timeout | "Standard DLQ after max-receives, alarm on depth, redrive after fix — moving on." |
| 2 | The same app on BullMQ + Redis — persistent, distributed, multi-process | §1 Why Messaging? | "Messaging is not a performance optimization — it is an architectural commitment to eventual consistency." |
| 3 | Idempotency: a message delivered twice, deduped safely | §17 Idempotent Consumers & Effective-Once | "The side effect and the dedup-record write must be in one transaction, or you've just moved the race." |
| 4 | Scheduling: delayed jobs, repeatable (cron) jobs, rate limiting | §4 Retry Queues, Backoff & Delay Queues | "Exponential backoff with full jitter and a hard max attempts; classify errors retryable vs terminal first." |
| 5 | RabbitMQ exchanges & routing — one event fanned out to many queues | §2 Queues vs Pub/Sub | "SNS→SQS fan-out gives broadcast at the topic level and load-balanced work distribution at the queue level." |
| 6 | Kafka-style streaming: the log, consumer groups, offsets, replay | §10 Kafka Deep Dive, §15 Ordering | "Partition count is a one-way door for ordering — pick generously up front." |
| 7 | Redis Pub/Sub: fire-and-forget broadcast — no persistence, no acks | §2 Queues vs Pub/Sub, §14 Delivery Guarantees | "At-most-once because the data is high-volume and lossy-tolerant." |
| 8 | Priority queues: urgent jobs jump the line | §20 Decision Guide (broker-queue features) | "RabbitMQ for rich routing / priorities / request-reply — pick one out loud." |
| 9 | The dual-write bug, the transactional outbox, and CDC | §16 Dual-Write Problem & Outbox + CDC | "'Just publish after the DB commit' is the wrong answer 100% of the time because of the crash window." |
| 10 | Event sourcing: state as a fold of events; replay, time-travel, snapshots | §7 Event Sourcing | "Event sourcing's killer feature isn't audit — it's that you can build a brand-new read model from history." |
| 11 | CQRS: command side + projected read models; the read-lag window | §8 CQRS | "The hardest part is never the split; it's managing the eventual consistency window the user experiences." |
| 12 | Backpressure: fast producer vs slow consumer — block, shed, or scale | §18 Backpressure | "Scaling consumers without checking the downstream database just relocates the bottleneck." |
| Capstone (`src/app`) | Webhook Delivery Service: API + worker + retries + backoff + DLQ + live dashboard | §3–§5 assembled; the "why queues exist" argument of §1 | "Set visibility timeout to 2-3x p99 processing latency, extend via heartbeat, and make the consumer idempotent." |

## Go deeper

- **[Deep Dives/02-kafka.md](https://github.com/swapnull-in/system-design/blob/main/Deep%20Dives/02-kafka.md)** — the full Kafka internals story (ISR, EOS, compaction, rebalancing) beyond what Phase 6 demonstrates.
- **[Deep Dives/32-design-message-queue.md](https://github.com/swapnull-in/system-design/blob/main/Deep%20Dives/32-design-message-queue.md)** — design the broker itself: what Phase 1's 150-line queue looks like at distributed, production scale.
- **[Deep Dives/33-messaging-mastery-bar.md](https://github.com/swapnull-in/system-design/blob/main/Deep%20Dives/33-messaging-mastery-bar.md)** — the self-test: what a staff-level messaging answer must contain before you're done with this module.
- **[Deep Dives/19-pattern-long-running-tasks.md](https://github.com/swapnull-in/system-design/blob/main/Deep%20Dives/19-pattern-long-running-tasks.md)** — the async job pattern the capstone app implements (accept fast, 202, poll status), generalized.
- **[Deep Dives/23-exactly-once-purchase.md](https://github.com/swapnull-in/system-design/blob/main/Deep%20Dives/23-exactly-once-purchase.md)** — Phase 3 + Phase 9 combined into one end-to-end problem: a payment that must happen effectively once.
