/** Drill data — mined from Core Course/04-messaging-event-driven.md. Loaded by index.html's Drill panel. */
window.DRILL = {
  module: "Module 04 — Messaging & Event-Driven",
  source: "Core Course/04-messaging-event-driven.md",
  cheats: [
    "Messaging is a <b>commitment to eventual consistency and idempotent consumers</b>, not a perf trick.",
    "<b>Queue</b> = consumed-means-gone, one consumer per message. <b>Log</b> = durable, replayable, many independent consumers.",
    "<b>SNS → SQS fan-out</b> gives broadcast at the topic and competing consumers per queue.",
    "DLQ after <code>maxReceiveCount</code>; in Kafka a poison message causes <b>head-of-line blocking</b> — skip to a dead-letter topic and commit the offset.",
    "Exponential backoff <b>with full jitter</b> and a hard max attempts; classify errors <em>retryable vs terminal</em> first.",
    "<b>Visibility timeout</b> is why SQS is at-least-once — message deleted only on explicit ack.",
    "Kafka ordering is <b>per-partition only</b>; same key → same partition → ordered. Partition count is a <em>one-way door</em> for ordering — over-provision.",
    "Durability triad: <code>acks=all</code> + <code>min.insync.replicas=2</code> + RF=3 + <code>unclean.leader.election.enable=false</code>.",
    "Kafka EOS is exactly-once <em>processing within Kafka</em>. Cross to a DB/API → at-least-once → need idempotency.",
    "Exactly-once <em>delivery</em> is impossible (Two Generals). Build <b>effective-once</b>: at-least-once + idempotency key, dedup atomic with the side effect.",
    "Dual-write problem → <b>transactional outbox + CDC (Debezium)</b> off the WAL/binlog. At-least-once, so idempotent consumers.",
    "Backpressure = monitor <b>consumer lag</b>, autoscale consumers <em>with a ceiling</em> so you don't overwhelm the downstream DB.",
    "News feed fan-out = <b>hybrid</b>: on-write for normal users, on-read for celebrities, merge at read time.",
    "Deliver decisions as <em>choice because requirement; revisit if trigger</em> — never 'it depends' and stop."
  ],
  cards: [
    {
      topic: "outbox",
      q: "Design a system that reliably emits an event whenever an order is created. What's the failure mode of the naive approach?",
      a: "The naive approach writes the order row then publishes to Kafka — two systems, no shared transaction: the dual-write problem, where a crash between the two diverges DB state and the event stream. Don't use 2PC (slow, poor Kafka support). Use the transactional outbox: insert the order and an outbox event row in one local DB transaction, then relay to Kafka via CDC (Debezium tailing the WAL). That's at-least-once, so consumers must be idempotent; partition by orderId to keep per-order events ordered."
    },
    {
      topic: "exactly-once",
      q: "What does 'exactly-once' actually mean, and how do you achieve it?",
      a: "True exactly-once delivery over a network is impossible — Two Generals: an ack can be lost, forcing a choice between duplicate or loss. Kafka EOS gives exactly-once processing within Kafka via idempotent producer plus transactions (output records and offset commit are atomic), but only inside the Kafka boundary. Everywhere else you build effective-once: at-least-once delivery plus an idempotency key, with the dedup-record write atomic with the side effect. Never claim 'exactly-once delivery' in a design review."
    },
    {
      topic: "ordering",
      q: "How does Kafka guarantee ordering, and how do you scale while preserving it?",
      a: "Ordering holds only within a partition: partition = hash(key) % numPartitions, so all events for one key are strictly ordered while different keys parallelize. Pick the key as the smallest entity whose order matters (userId, accountId, conversationId) — high-cardinality and evenly distributed to avoid hot partitions. Increasing partition count remaps the hash and breaks historical per-key ordering, so over-provision up front (often 2-3x current consumer count). Push back on 'global ordering' — it caps throughput at one partition and is almost never the real requirement."
    },
    {
      topic: "poison message",
      q: "A consumer in your Kafka group hits a malformed message it can never process. What happens and what do you do?",
      a: "Offsets are a sequential high-water mark, so retrying the poison record forever head-of-line-blocks the entire partition. Classify the failure: terminal errors go straight to a dead-letter topic and you commit the offset to move on; retryable errors go through a tiered retry-topic ladder (retry-5s, retry-1m, retry-30m) with backoff and jitter, then dead-letter after max attempts. Alarm on dead-letter-topic depth — at scale a frozen partition is a whole shard of traffic stuck."
    },
    {
      topic: "fan-out",
      q: "Design the Twitter/Instagram timeline. How do you fan out?",
      a: "Pure fan-out on write gives cheap reads but dies on celebrities — one post to 100M followers is 100M writes. Pure fan-out on read gives cheap writes but expensive reads under heavy traffic. The answer is hybrid: push into follower feeds for normal users, pull at read time for celebrities, and merge the two at read. The one tunable to instrument is the follower-count threshold where a user flips from push to pull."
    },
    {
      topic: "broker choice",
      q: "When would you choose RabbitMQ over Kafka, and vice versa?",
      a: "RabbitMQ when you need smart broker-side routing (topic/header exchanges), priorities, per-message TTL, request/reply, or low-latency task distribution with rich ack semantics — it's a broker. Kafka when you need a durable, replayable log consumed independently by many teams with high throughput and per-key ordering. The filter is the replay question: consumed-means-gone work distribution → RabbitMQ; replay plus multiple independent consumers → Kafka. Kafka as a job queue means fighting head-of-line blocking and hand-rolling DLQs; RabbitMQ as a million-msg/s stream hits its ceiling."
    },
    {
      topic: "durability",
      q: "Explain Kafka's durability model. How do you guarantee no data loss on a broker failure?",
      a: "Each partition has a leader and followers (RF typically 3); the ISR is the set of replicas caught up to the leader. Set acks=all so a write is acked only when all in-sync replicas have it, and min.insync.replicas=2 so a write is rejected unless at least two replicas hold it — surviving one broker loss with no loss. Keep unclean.leader.election.enable=false so an out-of-sync replica is never promoted, preferring brief unavailability over data loss. Quote the full quad — acks=all + min.insync.replicas=2 + RF=3 + unclean=false — or you don't really know the failure model."
    },
    {
      topic: "backpressure",
      q: "Your consumers can't keep up and the queue is growing. Walk me through your response.",
      a: "First distinguish a burst (queues absorb it) from a sustained imbalance (lag grows unbounded — must act). Monitor consumer lag as the signal, then: scale consumers horizontally (up to partition count in Kafka), optimize per-message work, batch, or shed low-priority load. Autoscale with a hard ceiling — adding consumers without checking the downstream store just relocates the bottleneck and can take the database down. Always trace backpressure to the true constraint, usually a stateful store."
    },
    {
      topic: "eda flavors",
      q: "Compare event notification, event-carried state transfer, and event sourcing.",
      a: "Notification: minimal event ('OrderPlaced{id}'), consumers call back for data — small events but re-introduces synchronous coupling. State transfer: the event carries full state, so consumers act autonomously, at the cost of duplication, staleness, and schema coupling. Event sourcing: every change is an immutable event and the log is the source of truth — audit, replay, new read models from history, but it taxes every schema change and complicates GDPR deletion. Default to state transfer service-to-service, notification for huge payloads, event sourcing only for bounded contexts that truly need audit/replay."
    },
    {
      topic: "es + cqrs",
      q: "What's the relationship between Event Sourcing and CQRS — do you need both?",
      a: "They're orthogonal but synergistic: CQRS splits the write model from query-optimized read models kept in sync asynchronously; event sourcing stores state as an event log. They compose because the event store naturally emits the change stream CQRS needs — adding a read model is just a new projector plus a replay. But adopt CQRS first (it solves real read/write scale divergence) and add event sourcing only when audit/replay is a hard requirement — combining them stacks schema-evolution and GDPR taxes onto the consistency-window tax."
    },
    {
      topic: "delivery guarantees",
      q: "Why is SQS at-least-once, and what does that force on every consumer?",
      a: "SQS never deletes on receive — it hides the message for the visibility timeout, and deletes only on explicit ack. Any crash between doing the work and acking causes redelivery, which is correct: no message is ever lost, but duplicates are guaranteed possible. That structurally forces idempotent consumers. The interview-grade line: set visibility timeout to 2-3x p99 processing latency, extend via heartbeat for long jobs, and make redelivery harmless."
    },
    {
      topic: "altitude",
      q: "You're 25 minutes into a messaging design and the interviewer hasn't steered. Where do you spend your remaining time?",
      a: "Skip the basics fast — what a queue is, DLQ hygiene, backoff-with-jitter — name them in a sentence and move; over-explaining them is an anti-signal. Spend the time on the risk crux: ordering (partition-key choice and the one-way-door remap), exactly-once (effective-once via idempotency plus the Kafka-boundary caveat), and the dual-write problem (outbox + CDC). Explicitly call out what you're not building — no event sourcing, no global ordering — with the trigger that would change your mind."
    },
    {
      topic: "pushback",
      q: "An interviewer pushes for 'exactly-once' and won't accept 'it's impossible'. How do you handle it?",
      a: "Don't argue semantics — reframe to what they actually want: no duplicated side effects. Exactly-once delivery is impossible across a network, but exactly-once effect is achievable: at-least-once delivery plus an idempotency key, with the dedup write atomic with the side effect. If the whole loop lives inside Kafka, EOS transactions give true exactly-once processing — but the instant an external DB or API appears, you're back to idempotency at that sink. That names the boundary precisely and delivers the guarantee they care about."
    }
  ]
};
