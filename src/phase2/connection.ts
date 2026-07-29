/**
 * Shared config for Phase 2.
 *
 * The big difference from Phase 1: the queue no longer lives inside one Node
 * process. It lives in REDIS. That single change is what makes it:
 *   - persistent  — jobs survive a process restart (they're in Redis, not RAM)
 *   - distributed — producers and workers can be different processes, even on
 *                   different machines, as long as they point at the same Redis.
 *
 * So instead of `new MessageQueue()` shared via an import, both sides connect
 * to Redis and address the same queue BY NAME ("emails").
 */

import type { ConnectionOptions } from "bullmq";

export const connection: ConnectionOptions = {
  host: "127.0.0.1",
  port: 6379,
  // BullMQ workers require this to be null (they use blocking commands).
  maxRetriesPerRequest: null,
};

/** Every process refers to the same queue by this name. */
export const QUEUE_NAME = "emails";

export interface EmailJob {
  to: string;
  subject: string;
}
