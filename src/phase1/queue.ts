/**
 * A tiny, in-memory message queue — built from scratch to show how real
 * brokers (SQS, RabbitMQ, BullMQ) work underneath.
 *
 * The 5 ideas every queue is built on:
 *
 *   1. PRODUCER   — code that puts a message on the queue and moves on.
 *   2. CONSUMER   — a worker that pulls messages off and does the work.
 *   3. ACK        — the worker tells the queue "done, delete it". Until an
 *                   ack arrives, the queue assumes the work might have failed.
 *   4. RETRY      — if a worker crashes or throws, the message goes back on
 *                   the queue and is tried again (up to a limit).
 *   5. DEAD-LETTER— a message that fails too many times is moved aside to a
 *                   "dead-letter queue" (DLQ) so it stops blocking others and
 *                   a human can inspect it.
 *
 * The mechanism that ties ack + retry together is the VISIBILITY TIMEOUT:
 * when a worker takes a message, the queue hides it (makes it "invisible")
 * for N milliseconds instead of deleting it. If the worker acks in time, it's
 * deleted. If not (crash, hang, throw), the message becomes visible again and
 * another worker picks it up. This gives "at-least-once" delivery.
 */

export interface Message<T> {
  id: string;
  body: T;
  /** How many times this message has been DELIVERED to a worker. */
  attempts: number;
  enqueuedAt: number;
}

/** A worker's job: process a message. Resolve = success. Throw = failure. */
export type Handler<T> = (msg: Message<T>) => Promise<void>;

export interface QueueOptions {
  /** Deliveries allowed before a message is dead-lettered. Default 3. */
  maxAttempts?: number;
  /** How long a message stays hidden while a worker processes it. Default 5s. */
  visibilityTimeoutMs?: number;
  /** How many messages a single consumer processes at once. Default 1. */
  concurrency?: number;
}

let idCounter = 0;
const nextId = () => `msg_${++idCounter}`;

export class MessageQueue<T> {
  /** Messages waiting to be delivered (FIFO). */
  private ready: Message<T>[] = [];
  /** Messages handed to a worker but not yet acked, keyed by id. */
  private inFlight = new Map<string, { msg: Message<T>; timer: NodeJS.Timeout }>();
  /** Messages that failed too many times — parked for inspection. */
  private deadLetter: Message<T>[] = [];

  private readonly maxAttempts: number;
  private readonly visibilityTimeoutMs: number;

  /** Consumers waiting for a message, so we can wake them the instant one arrives. */
  private waiters: Array<() => void> = [];

  constructor(opts: QueueOptions = {}) {
    this.maxAttempts = opts.maxAttempts ?? 3;
    this.visibilityTimeoutMs = opts.visibilityTimeoutMs ?? 5_000;
  }

  // ─── PRODUCER SIDE ──────────────────────────────────────────────────────

  /** Put a message on the queue and return immediately. This is "publishing". */
  produce(body: T): Message<T> {
    const msg: Message<T> = {
      id: nextId(),
      body,
      attempts: 0,
      enqueuedAt: Date.now(),
    };
    this.ready.push(msg);
    log("→ produced", msg.id, body);
    this.wakeOneWaiter();
    return msg;
  }

  // ─── CONSUMER SIDE ──────────────────────────────────────────────────────

  /**
   * Take the next ready message and mark it in-flight (invisible). If nothing
   * is ready, wait until something is produced. This is the core of "pull"
   * delivery: workers ask for work rather than being pushed to.
   */
  private async receive(): Promise<Message<T>> {
    while (this.ready.length === 0) {
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
    const msg = this.ready.shift()!;
    msg.attempts += 1;

    // Start the visibility timeout. If the worker doesn't ack before it fires,
    // we assume the work was lost and make the message visible again.
    const timer = setTimeout(() => {
      log("⏰ visibility timeout — redelivering", msg.id);
      this.inFlight.delete(msg.id);
      this.requeueOrDeadLetter(msg);
    }, this.visibilityTimeoutMs);

    this.inFlight.set(msg.id, { msg, timer });
    return msg;
  }

  /** Worker says "done" — delete the message for good. */
  private ack(id: string): void {
    const entry = this.inFlight.get(id);
    if (!entry) return; // already timed out & redelivered
    clearTimeout(entry.timer);
    this.inFlight.delete(id);
    log("✓ acked", id);
  }

  /** Worker says "I failed" — retry now instead of waiting for the timeout. */
  private nack(id: string): void {
    const entry = this.inFlight.get(id);
    if (!entry) return;
    clearTimeout(entry.timer);
    this.inFlight.delete(id);
    this.requeueOrDeadLetter(entry.msg);
  }

  private requeueOrDeadLetter(msg: Message<T>): void {
    if (msg.attempts >= this.maxAttempts) {
      this.deadLetter.push(msg);
      log("☠ dead-lettered after", msg.attempts, "attempts:", msg.id);
      return;
    }
    log("↩ requeued", msg.id, `(attempt ${msg.attempts}/${this.maxAttempts})`);
    this.ready.push(msg);
    this.wakeOneWaiter();
  }

  /**
   * Register a worker. It loops forever: receive → run handler → ack (or nack
   * on throw), running up to `concurrency` messages at the same time.
   */
  consume(handler: Handler<T>, opts: { concurrency?: number } = {}): void {
    const concurrency = opts.concurrency ?? 1;
    for (let i = 0; i < concurrency; i++) {
      void this.workerLoop(handler, i + 1);
    }
  }

  private async workerLoop(handler: Handler<T>, workerId: number): Promise<void> {
    while (true) {
      const msg = await this.receive();
      log(`  [worker ${workerId}] processing`, msg.id);
      try {
        await handler(msg);
        this.ack(msg.id);
      } catch (err) {
        log(`  [worker ${workerId}] handler threw:`, (err as Error).message);
        this.nack(msg.id);
      }
    }
  }

  private wakeOneWaiter(): void {
    const wake = this.waiters.shift();
    if (wake) wake();
  }

  // ─── INSPECTION (for the demo) ──────────────────────────────────────────

  stats() {
    return {
      ready: this.ready.length,
      inFlight: this.inFlight.size,
      deadLetter: this.deadLetter.length,
    };
  }

  getDeadLetter(): ReadonlyArray<Message<T>> {
    return this.deadLetter;
  }
}

/** Timestamped logging so you can watch the flow happen in real time. */
export function log(...args: unknown[]): void {
  const t = new Date().toISOString().slice(11, 23); // HH:MM:SS.mmm
  console.log(`[${t}]`, ...args);
}
