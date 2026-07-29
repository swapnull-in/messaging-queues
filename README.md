# Learn Message Queues in TypeScript

A hands-on, runnable project for understanding message queues — from building a
tiny queue **from scratch** to running a real feature on **BullMQ + Redis**.

Every phase is a small script you can run and read. No build step: modern Node
runs the TypeScript directly.

## Prerequisites

- **Node.js 22+** (uses native `.ts` execution)
- **Redis** running locally on `localhost:6379` (needed from Phase 2 on)
  ```bash
  # macOS:      brew install redis && brew services start redis
  # Docker:     docker run -d -p 6379:6379 redis
  # check it:   redis-cli ping   # → PONG
  ```

## Setup

```bash
git clone https://github.com/swapnull-in/messaging-queues.git
cd messaging-queues
npm install
```

## The lessons

Run them in order. Each one prints a timestamped log so you can watch what happens.

| Command | What you learn |
|---|---|
| `npm run phase1` | Build a queue **from scratch** — producer, consumer, ack, retry, dead-letter |
| `npm run phase2:demo` | The same app on **BullMQ + Redis** (persistent, distributed) |
| `npm run phase3` | **Idempotency** — safely handling a message that arrives twice |
| `npm run phase4` | **Scheduling** — delayed, repeatable (cron), and rate-limited jobs |

The code in `src/phase1/queue.ts` is the one to read first — it's the whole
queue engine in ~150 commented lines.

### Phase 2 the "real" way (two terminals)

```bash
npm run phase2:worker    # terminal 1 — the worker, leave it running
npm run phase2:produce   # terminal 2 — sends jobs, then exits
```

## Capstone: a real feature + dashboard

A **Webhook Delivery Service** — an API that accepts requests and returns
instantly, while a worker delivers them in the background with retries, backoff,
and a dead-letter queue. This is the classic reason queues exist.

Run the three parts in three terminals:

```bash
npm run app:target    # a fake, flaky receiver on :4000
npm run app:worker    # the delivery worker
npm run app:server    # the API + dashboard on :3000
```

Then open **http://localhost:3000/** to try it in the browser: send deliveries,
watch them retry and back off live, and see failed ones land in the dead-letter
queue.

Prefer curl?

```bash
curl -X POST localhost:3000/webhooks -H 'content-type: application/json' \
  -d '{"url":"http://localhost:4000/hook","event":"order.created","payload":{"orderId":42}}'
```

## Project layout

```
src/
  phase1/   queue built from scratch (+ demo)
  phase2/   same app on BullMQ + Redis
  phase3/   idempotency
  phase4/   scheduling
  app/      the webhook delivery service + browser dashboard
```

## License

MIT — see [LICENSE](LICENSE). Use it, fork it, learn from it.
