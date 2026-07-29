/**
 * The API server (the PRODUCER) — run with:  npm run app:server
 *
 * Exposes a tiny HTTP API on :3000. It does NO delivery work itself — it just
 * validates the request, drops a job on the queue, and returns 202 Accepted
 * immediately. That's the core queue pattern: accept fast, process later.
 *
 *   POST /webhooks           enqueue a delivery   → 202 { id, status }
 *   GET  /webhooks/:id        check delivery status → 200 { id, state, attempts }
 *   GET  /health             liveness
 *
 * It also serves a small browser dashboard so you can try all of this by hand:
 *   GET  /                   the UI (public/index.html)
 *   GET  /api/stats          live queue counts
 *   GET  /api/deliveries     recent deliveries + their state
 *   GET  /api/dead-letter    dead-lettered deliveries
 *
 * Uses Node's built-in http server — no framework needed to see the idea.
 */

import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { deliveryQueue, deadLetterQueue, type DeliveryJob } from "./queue.ts";
import { log } from "../phase1/queue.ts";

const PORT = 3000;
const UI_HTML = readFileSync(new URL("./public/index.html", import.meta.url), "utf8");

/** Collect recent jobs across states, newest first, for the dashboard table. */
async function listDeliveries() {
  const states = ["active", "waiting", "delayed", "failed", "completed"] as const;
  const rows: any[] = [];
  const seen = new Set<string>();
  for (const state of states) {
    const jobs = await deliveryQueue.getJobs([state], 0, 50);
    for (const j of jobs) {
      if (!j?.id || seen.has(j.id)) continue;
      seen.add(j.id);
      rows.push({
        id: j.id,
        state,
        target: j.data.url,
        event: j.data.event,
        attemptsMade: j.attemptsMade,
        failedReason: j.failedReason ?? null,
        timestamp: j.timestamp,
      });
    }
  }
  return rows.sort((a, b) => b.timestamp - a.timestamp).slice(0, 40);
}

function send(res: import("node:http").ServerResponse, status: number, body: unknown) {
  const json = JSON.stringify(body, null, 2);
  res.writeHead(status, { "content-type": "application/json" });
  res.end(json);
}

async function readJson(req: import("node:http").IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString());
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);

    // ─── POST /webhooks — enqueue a delivery ─────────────────────────────
    if (req.method === "POST" && url.pathname === "/webhooks") {
      const body = await readJson(req);
      if (!body.url || !body.event) {
        return send(res, 400, { error: "body must include { url, event, payload? }" });
      }

      const job: DeliveryJob = { url: body.url, event: body.event, payload: body.payload ?? {} };

      // Idempotency: if the client sends an Idempotency-Key, use it as the
      // jobId so retrying the SAME request doesn't enqueue a duplicate delivery.
      const idempotencyKey = req.headers["idempotency-key"] as string | undefined;
      const added = await deliveryQueue.add("deliver", job, idempotencyKey ? { jobId: idempotencyKey } : undefined);

      log(`→ queued delivery ${added.id} → ${job.url} (${job.event})`);
      // 202 Accepted: "I've taken responsibility for this, but it isn't done yet."
      return send(res, 202, { id: added.id, status: "queued" });
    }

    // ─── GET /webhooks/:id — check status ────────────────────────────────
    if (req.method === "GET" && url.pathname.startsWith("/webhooks/")) {
      const id = url.pathname.split("/")[2];
      const job = await deliveryQueue.getJob(id);
      if (!job) return send(res, 404, { error: "no such delivery" });
      const state = await job.getState(); // waiting | active | completed | failed | delayed
      return send(res, 200, {
        id: job.id,
        state,
        attemptsMade: job.attemptsMade,
        target: job.data.url,
        failedReason: job.failedReason ?? null,
      });
    }

    if (req.method === "GET" && url.pathname === "/health") {
      return send(res, 200, { ok: true });
    }

    // ─── Dashboard UI + its read-only data endpoints ─────────────────────
    if (req.method === "GET" && url.pathname === "/") {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      return res.end(UI_HTML);
    }

    if (req.method === "GET" && url.pathname === "/api/stats") {
      const counts = await deliveryQueue.getJobCounts();
      const dead = await deadLetterQueue.getJobCounts();
      return send(res, 200, { ...counts, dead: (dead.waiting ?? 0) + (dead.completed ?? 0) });
    }

    if (req.method === "GET" && url.pathname === "/api/deliveries") {
      return send(res, 200, await listDeliveries());
    }

    if (req.method === "GET" && url.pathname === "/api/dead-letter") {
      const jobs = await deadLetterQueue.getJobs(["waiting", "completed", "active", "failed"], 0, 40);
      const rows = jobs.filter(Boolean).map((j) => ({
        id: j.id,
        target: j.data?.original?.url,
        event: j.data?.original?.event,
        error: j.data?.error,
      }));
      return send(res, 200, rows);
    }

    return send(res, 404, { error: "not found" });
  } catch (err) {
    return send(res, 500, { error: (err as Error).message });
  }
});

server.listen(PORT, () => {
  log(`API server listening on http://localhost:${PORT}`);
  log(`open the dashboard:  http://localhost:${PORT}/`);
});
