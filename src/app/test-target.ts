/**
 * A fake receiver to deliver webhooks TO — run with:  npm run app:target
 *
 * This stands in for someone else's server. It's deliberately UNRELIABLE so you
 * can watch the queue's retry/backoff/dead-letter machinery do real work:
 *
 *   POST /hook    → succeeds ~60% of the time, else returns HTTP 503
 *   POST /always-fail → always 500 (use this to watch a job hit the DLQ)
 *   POST /ok      → always 200
 *
 * It's a plain counter-based flake (no randomness) so behavior is reproducible.
 */

import { createServer } from "node:http";
import { log } from "../phase1/queue.ts";

const PORT = 4000;
let hits = 0;

const server = createServer(async (req, res) => {
  const path = req.url ?? "/";

  // Drain the body (the delivery payload) so we can log what we received.
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  const bodyText = Buffer.concat(chunks).toString() || "{}";

  if (path === "/always-fail") {
    log(`  [target] ${path} → 500 (intentional)`);
    res.writeHead(500).end("nope");
    return;
  }

  if (path === "/ok") {
    log(`  [target] ${path} → 200`);
    res.writeHead(200).end("ok");
    return;
  }

  // /hook — flaky: fail 2 out of every 5 requests.
  hits += 1;
  const shouldFail = hits % 5 === 3 || hits % 5 === 4;
  if (shouldFail) {
    log(`  [target] /hook hit #${hits} → 503 (flaking, will make the worker retry)`);
    res.writeHead(503).end("temporarily unavailable");
  } else {
    const parsed = JSON.parse(bodyText);
    log(`  [target] /hook hit #${hits} → 200  ✓ received event="${parsed.event}" attempt=${parsed.attempt}`);
    res.writeHead(200).end("accepted");
  }
});

server.listen(PORT, () => {
  log(`test receiver listening on http://localhost:${PORT}`);
  log("endpoints:  POST /hook (flaky)   POST /always-fail (DLQ demo)   POST /ok");
});
