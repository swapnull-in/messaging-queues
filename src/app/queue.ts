/**
 * Webhook Delivery Service — shared queue definitions.
 *
 * THE FEATURE: clients call our API to say "deliver this event to this URL".
 * Delivering a webhook means making an HTTP call to someone else's server —
 * slow, and it fails constantly (their server is down, slow, rate-limiting us).
 *
 * If we delivered inline in the request, the client would wait seconds and our
 * API would fall over under load. So we QUEUE the delivery and return instantly.
 * A worker delivers it in the background, retries with backoff, and dead-letters
 * anything that fails too many times. This is the textbook reason queues exist.
 */

import { Queue } from "bullmq";

export const connection = { host: "127.0.0.1", port: 6379, maxRetriesPerRequest: null };

export const DELIVERY_QUEUE = "webhook-deliveries";
export const DLQ = "webhook-deliveries-dead";

/** The payload for one delivery attempt. */
export interface DeliveryJob {
  url: string; // where to POST
  event: string; // e.g. "order.created"
  payload: unknown; // the body the receiver gets
}

/** Producer-side handle to the delivery queue (used by the API server). */
export const deliveryQueue = new Queue<DeliveryJob>(DELIVERY_QUEUE, {
  connection,
  defaultJobOptions: {
    attempts: 5, // try up to 5 times before giving up
    backoff: { type: "exponential", delay: 1_000 }, // 1s, 2s, 4s, 8s...
    removeOnComplete: 1_000, // keep the last 1000 successes for status lookups
    removeOnFail: false, // keep failures so we can inspect them
  },
});

/** Dead-letter queue for deliveries that exhausted all retries. */
export const deadLetterQueue = new Queue(DLQ, { connection });
