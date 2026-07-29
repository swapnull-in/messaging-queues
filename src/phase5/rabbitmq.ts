/**
 * Phase 5 — RabbitMQ: exchanges & routing (the thing BullMQ doesn't do).
 *
 * Run with:  npm run phase5   (needs RabbitMQ on localhost:5672)
 *
 * ─── How RabbitMQ differs from BullMQ ────────────────────────────────────
 * In BullMQ you `add` a job to ONE queue and a worker pulls it. In RabbitMQ a
 * producer NEVER publishes to a queue directly — it publishes to an EXCHANGE,
 * and the exchange decides which queues get a copy based on the message's
 * ROUTING KEY and the QUEUES' BINDINGS. That indirection is the whole point:
 * one published event can fan out to many independent consumers.
 *
 *   producer → [ EXCHANGE ] --routing key--> binding? --> queue → consumer
 *                                          \-> binding? --> queue → consumer
 *
 * We model an e-commerce "orders" system with a TOPIC exchange:
 *   - shipping  cares about  order.created, order.shipped
 *   - billing   cares about  order.created, order.paid
 *   - analytics cares about  order.#   (everything)
 *
 * Publishing ONE "order.created" event lands in shipping + billing + analytics
 * at once — no producer code knows who's listening. That's pub/sub routing.
 *
 * We also show:
 *   - explicit ACK  (RabbitMQ pushes; you must confirm each message)
 *   - NACK → DEAD-LETTER EXCHANGE  (a rejected message is routed to a DLQ)
 *   - prefetch(1)   (fair dispatch: don't flood one slow consumer)
 */

import amqp from "amqplib";
import { log } from "../phase1/queue.ts";

const URL = "amqp://localhost:5672";
const EXCHANGE = "orders"; // topic exchange
const DLX = "orders.dlx"; // dead-letter exchange
const DLQ = "orders.dead"; // dead-letter queue

interface OrderEvent {
  orderId: number;
  amountCents?: number;
  poison?: boolean; // used to demonstrate a rejected message
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const conn = await amqp.connect(URL);
  const ch = await conn.createChannel();
  await ch.prefetch(1); // fair dispatch: one unacked message per consumer at a time

  // ─── Topology: exchange, dead-letter plumbing, queues, bindings ────────
  await ch.assertExchange(EXCHANGE, "topic", { durable: false });
  await ch.assertExchange(DLX, "fanout", { durable: false });
  await ch.assertQueue(DLQ, { durable: false });
  await ch.bindQueue(DLQ, DLX, "");

  // Each work queue dead-letters rejected messages to the DLX.
  const queueOpts = { durable: false, arguments: { "x-dead-letter-exchange": DLX } };
  await ch.assertQueue("shipping", queueOpts);
  await ch.assertQueue("billing", queueOpts);
  await ch.assertQueue("analytics", queueOpts);

  // Bindings = "which routing keys does this queue want?" (topic patterns:
  // '*' = one word, '#' = zero or more words).
  await ch.bindQueue("shipping", EXCHANGE, "order.created");
  await ch.bindQueue("shipping", EXCHANGE, "order.shipped");
  await ch.bindQueue("billing", EXCHANGE, "order.created");
  await ch.bindQueue("billing", EXCHANGE, "order.paid");
  await ch.bindQueue("analytics", EXCHANGE, "order.#"); // every order event

  // ─── Consumers ─────────────────────────────────────────────────────────
  const consumer = (queue: string) =>
    ch.consume(queue, (msg) => {
      if (!msg) return;
      const key = msg.fields.routingKey;
      const body = JSON.parse(msg.content.toString()) as OrderEvent;

      // Business rule: billing rejects an order with no/invalid amount. A
      // reject with requeue=false sends the message to the dead-letter exchange.
      if (queue === "billing" && (!body.amountCents || body.amountCents <= 0)) {
        log(`  ✗ [billing] rejecting ${key} order#${body.orderId} (bad amount) → dead-letter`);
        ch.nack(msg, false, false); // (message, allUpTo=false, requeue=false)
        return;
      }
      if (body.poison) {
        log(`  ✗ [${queue}] poison message ${key} → dead-letter`);
        ch.nack(msg, false, false);
        return;
      }

      log(`  ▶ [${queue}] handled ${key}  (order#${body.orderId})`);
      ch.ack(msg); // confirm — RabbitMQ deletes it only now
    });

  await consumer("shipping");
  await consumer("billing");
  await consumer("analytics");

  // A consumer on the dead-letter queue so we can see what got rejected.
  await ch.consume(DLQ, (msg) => {
    if (!msg) return;
    const body = JSON.parse(msg.content.toString());
    log(`  ☠ [dead-letter] order#${body.orderId} (was "${msg.fields.routingKey}")`);
    ch.ack(msg);
  });

  // ─── Producer: publish events to the EXCHANGE (not to any queue) ───────
  const publish = (routingKey: string, event: OrderEvent) => {
    ch.publish(EXCHANGE, routingKey, Buffer.from(JSON.stringify(event)));
    log(`→ published ${routingKey}  order#${event.orderId}`);
  };

  log("═══ Watch one event fan out to multiple queues by routing key ═══");
  publish("order.created", { orderId: 1, amountCents: 4999 }); // → shipping+billing+analytics
  await sleep(400);
  publish("order.paid", { orderId: 1, amountCents: 4999 }); // → billing+analytics
  await sleep(400);
  publish("order.shipped", { orderId: 1 }); // → shipping+analytics
  await sleep(400);
  publish("order.created", { orderId: 2, amountCents: 0 }); // billing rejects → dead-letter
  await sleep(400);
  publish("order.refunded", { orderId: 1, poison: true }); // → analytics only, poison → dead-letter

  await sleep(1_500);

  log("──────────────────────────────────────────");
  log("Takeaway: the producer addressed an EXCHANGE + routing key, never a queue.");
  log("Consumers subscribe by BINDING patterns. That decoupling — one event, many");
  log("independent subscribers — is RabbitMQ's core strength over a plain job queue.");

  // Clean up so re-runs start fresh.
  for (const q of ["shipping", "billing", "analytics", DLQ]) await ch.deleteQueue(q);
  await ch.deleteExchange(EXCHANGE);
  await ch.deleteExchange(DLX);
  await ch.close();
  await conn.close();
  process.exit(0);
}

main().catch((err) => {
  console.error("Phase 5 error:", err.message);
  console.error("Is RabbitMQ running?  brew services start rabbitmq   (or: docker run -d -p 5672:5672 rabbitmq)");
  process.exit(1);
});
