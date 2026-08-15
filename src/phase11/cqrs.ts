/**
 * Phase 11 — CQRS: Command Query Responsibility Segregation.
 *
 * Run with:  node "src/phase11/cqrs.ts"   (dependency-free, in-process)
 *
 * ─── The big idea ────────────────────────────────────────────────────────
 * Most systems use ONE model for everything: the same tables you write to are
 * the tables you read from. CQRS splits that in two:
 *
 *   WRITE side  — accepts COMMANDS ("change the price to 12"), enforces
 *                 invariants (stock can't go negative), and emits EVENTS
 *                 describing what happened. Optimized for consistency.
 *   READ side   — one or more READ MODELS (a.k.a. projections): denormalized,
 *                 pre-computed views shaped for a specific query. Optimized for
 *                 fast reads with NO joins.
 *
 * The two sides are connected by an EVENT STREAM. A PROJECTOR consumes the
 * events and updates the read models. Because one write emits one event, and
 * many projectors can subscribe, a SINGLE write can feed MANY purpose-built
 * read models — each shaped for its own query:
 *
 *     command ─▶ WRITE model ─▶ [ event ] ─▶ stream ─┬─▶ ProductView  (by id)
 *                                                     └─▶ Leaderboard  (top-liked)
 *
 * The catch: the read side is updated ASYNCHRONOUSLY, so it's EVENTUALLY
 * CONSISTENT. Right after a write, a query can still see the OLD value — a
 * "read-lag window" (the read-your-writes gap) you must design around. And
 * because the events are retained in a log, you can DROP a read model and
 * REBUILD it later by replaying the whole log — which is why CQRS pairs so
 * naturally with event sourcing: add a new view any time and backfill it.
 *
 * TAKEAWAY: CQRS splits writes from reads so each is optimized independently,
 * and one write can feed many purpose-built read models — but the read side is
 * eventually consistent (a read-lag window you must design for), and you can
 * add or rebuild read models any time by replaying the events.
 */

// ─── THE EVENT STREAM: the append-only spine that connects write → read ─────

type DomainEvent =
  | { seq: number; type: "ProductCreated"; id: string; name: string; price: number; stock: number }
  | { seq: number; type: "PriceChanged"; id: string; price: number }
  | { seq: number; type: "StockAdjusted"; id: string; delta: number }
  | { seq: number; type: "ProductLiked"; id: string };

/** What the write side emits before the seq number is assigned by the stream.
 *  Distributive so each union member keeps its own fields (a plain Omit over a
 *  union collapses to just the shared keys). */
type DistributiveOmit<T, K extends PropertyKey> = T extends unknown ? Omit<T, K> : never;
type NewEvent = DistributiveOmit<DomainEvent, "seq">;

class EventStream {
  /** The retained log — the source of truth we can replay to rebuild views. */
  private readonly log: DomainEvent[] = [];
  private seqCounter = 0;
  private readonly subscribers: Array<(e: DomainEvent) => void> = [];

  /** Append an event, stamp it with a monotonic seq, and notify subscribers. */
  append(e: NewEvent): DomainEvent {
    const stored = { ...e, seq: ++this.seqCounter } as DomainEvent;
    this.log.push(stored);
    for (const notify of this.subscribers) notify(stored);
    return stored;
  }

  subscribe(fn: (e: DomainEvent) => void): void {
    this.subscribers.push(fn);
  }

  /** The whole history, for replaying into a fresh read model. */
  all(): ReadonlyArray<DomainEvent> {
    return this.log;
  }
}

// ─── THE WRITE MODEL: commands in, events out, invariants enforced ──────────

interface WriteRecord {
  name: string;
  price: number;
  stock: number;
}

/**
 * The command side. It keeps just enough state to enforce invariants (a product
 * must exist before you can price it; stock can never go negative), then records
 * an event. It never answers queries — that's the read side's job.
 */
class Catalog {
  private readonly records = new Map<string, WriteRecord>();
  private readonly stream: EventStream;

  constructor(stream: EventStream) {
    this.stream = stream;
  }

  createProduct(id: string, name: string, price: number, stock: number): void {
    if (this.records.has(id)) throw new Error(`product ${id} already exists`);
    this.records.set(id, { name, price, stock });
    this.stream.append({ type: "ProductCreated", id, name, price, stock });
    log(`  cmd  CreateProduct  ${id} "${name}" $${price} stock:${stock}`);
  }

  changePrice(id: string, price: number): void {
    const rec = this.mustGet(id);
    if (price <= 0) throw new Error("price must be positive");
    rec.price = price;
    this.stream.append({ type: "PriceChanged", id, price });
    log(`  cmd  ChangePrice    ${id} → $${price}`);
  }

  adjustStock(id: string, delta: number): void {
    const rec = this.mustGet(id);
    if (rec.stock + delta < 0) throw new Error("stock cannot go negative"); // the invariant
    rec.stock += delta;
    this.stream.append({ type: "StockAdjusted", id, delta });
    log(`  cmd  AdjustStock    ${id} ${delta >= 0 ? "+" : ""}${delta} → ${rec.stock}`);
  }

  like(id: string): void {
    this.mustGet(id);
    this.stream.append({ type: "ProductLiked", id });
    log(`  cmd  LikeProduct    ${id}`);
  }

  private mustGet(id: string): WriteRecord {
    const rec = this.records.get(id);
    if (!rec) throw new Error(`no such product ${id}`);
    return rec;
  }
}

// ─── READ MODEL #1: a denormalized product view (query by id, no joins) ─────

interface ProductView {
  id: string;
  name: string;
  price: number;
  stock: number;
  likes: number;
}

/** A read model must know how to fold an event into itself, and how to reset. */
interface ReadModel {
  apply(e: DomainEvent): void;
  clear(): void;
}

class ProductViewStore implements ReadModel {
  private views = new Map<string, ProductView>();

  apply(e: DomainEvent): void {
    switch (e.type) {
      case "ProductCreated":
        this.views.set(e.id, { id: e.id, name: e.name, price: e.price, stock: e.stock, likes: 0 });
        break;
      case "PriceChanged":
        this.views.get(e.id)!.price = e.price;
        break;
      case "StockAdjusted":
        this.views.get(e.id)!.stock += e.delta;
        break;
      case "ProductLiked":
        this.views.get(e.id)!.likes += 1;
        break;
    }
  }

  /** One map lookup returns the whole denormalized row — no joins, no assembly. */
  get(id: string): ProductView | undefined {
    const v = this.views.get(id);
    return v ? { ...v } : undefined;
  }

  clear(): void {
    this.views = new Map();
  }
}

// ─── READ MODEL #2: a top-liked leaderboard, from the SAME events ───────────

class Leaderboard implements ReadModel {
  private likes = new Map<string, { name: string; likes: number }>();

  apply(e: DomainEvent): void {
    switch (e.type) {
      case "ProductCreated":
        this.likes.set(e.id, { name: e.name, likes: 0 });
        break;
      case "ProductLiked":
        this.likes.get(e.id)!.likes += 1;
        break;
      // PriceChanged / StockAdjusted are irrelevant to a leaderboard — ignore.
    }
  }

  top(n: number): Array<{ name: string; likes: number }> {
    return [...this.likes.values()]
      .sort((a, b) => b.likes - a.likes || a.name.localeCompare(b.name)) // deterministic ties
      .slice(0, n);
  }

  clear(): void {
    this.likes = new Map();
  }
}

// ─── THE PROJECTOR: consumes events and updates read models — ASYNC ─────────

/**
 * The projector is the bridge. It subscribes to the stream and folds each event
 * into every read model it owns. We apply after a fixed `lagMs` delay to model
 * the real world: projections run out-of-band, so the read side trails the
 * write side by a small window. That window is CQRS's central tradeoff.
 */
class Projector {
  private readonly models: ReadModel[];
  private readonly lagMs: number;

  constructor(models: ReadModel[], lagMs: number) {
    this.models = models;
    this.lagMs = lagMs;
  }

  onEvent(e: DomainEvent): void {
    setTimeout(() => {
      for (const m of this.models) m.apply(e);
    }, this.lagMs);
  }

  /** Rebuild a fresh read model from scratch by replaying the whole log NOW. */
  static rebuild(model: ReadModel, history: ReadonlyArray<DomainEvent>): void {
    model.clear();
    for (const e of history) model.apply(e);
  }
}

// ─── DEMO ───────────────────────────────────────────────────────────────────

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const LAG_MS = 250; // the projection delay — the read-lag window we design for

async function main() {
  const stream = new EventStream();
  const catalog = new Catalog(stream);
  const productView = new ProductViewStore();
  const leaderboard = new Leaderboard();

  // ONE projector, TWO read models — both fed by the SAME event stream.
  const projector = new Projector([productView, leaderboard], LAG_MS);
  stream.subscribe((e) => projector.onEvent(e));

  // ─── A) COMMAND → EVENT → PROJECTION ──────────────────────────────────────
  log("═══ A) command → event → projection ═══");
  catalog.createProduct("p1", "Mechanical Keyboard", 90, 5);
  catalog.createProduct("p2", "Wrist Rest", 20, 40);
  catalog.changePrice("p1", 99);
  catalog.adjustStock("p1", 3);
  await sleep(LAG_MS + 100); // let the projector catch up

  const kb = productView.get("p1")!;
  log(`  query getProduct("p1") → { name:"${kb.name}", price:$${kb.price}, stock:${kb.stock}, likes:${kb.likes} }`);
  log("  ↑ one denormalized row, one lookup, zero joins — the read model is shaped for this query.");
  log("");

  // ─── B) MULTIPLE READ MODELS FROM THE SAME EVENTS ─────────────────────────
  log("═══ B) two purpose-built read models from ONE event stream ═══");
  catalog.like("p1");
  catalog.like("p1");
  catalog.like("p2");
  catalog.like("p1");
  await sleep(LAG_MS + 100);

  log(`  ProductView(p1).likes = ${productView.get("p1")!.likes}   (view keyed by id, for detail pages)`);
  log("  Leaderboard.top(3):");
  for (const row of leaderboard.top(3)) log(`      ${row.name} — ${row.likes} likes`);
  log("  ↑ same 4 LikeProduct events, two differently-shaped views. Each query gets its own model.");
  log("");

  // ─── C) EVENTUAL CONSISTENCY / READ-YOUR-WRITES GAP ───────────────────────
  log("═══ C) eventual consistency: a write, then an IMMEDIATE read is stale ═══");
  log(`  before: ProductView(p1).price = $${productView.get("p1")!.price}`);
  catalog.changePrice("p1", 129); // write side is instantly consistent...
  const staleRead = productView.get("p1")!.price; // ...but the read model hasn't caught up
  log(`  immediately after ChangePrice→$129, query reads price = $${staleRead}  ← STALE (read-lag window)`);
  await sleep(LAG_MS + 100); // projector catches up
  log(`  after projector catches up, query reads price = $${productView.get("p1")!.price}  ← CONVERGED`);
  log("  ↑ this gap is the price of CQRS. It's small and bounded, but it's real — design for it.");
  log("");

  // ─── D) REBUILD A READ MODEL BY REPLAYING THE EVENTS ──────────────────────
  log("═══ D) drop a read model and rebuild it by replaying the log ═══");
  leaderboard.clear();
  log(`  dropped leaderboard → top(3) is now: [${leaderboard.top(3).map((r) => r.name).join(", ") || "empty"}]`);
  Projector.rebuild(leaderboard, stream.all());
  log(`  replayed ${stream.all().length} events → leaderboard rebuilt:`);
  for (const row of leaderboard.top(3)) log(`      ${row.name} — ${row.likes} likes`);
  log("  ↑ the event log is retained, so any read model can be rebuilt — or ADDED later — from history.");
  log("");

  log("──────────────────────────────────────────");
  log("CQRS: writes and reads are optimized independently, one write feeds many");
  log("purpose-built read models, the read side is eventually consistent (a read-lag");
  log("window you design for), and you rebuild/add read models by replaying events.");

  process.exit(0);
}

/** Timestamped logging so you can watch the write → projection lag happen. */
function log(...args: unknown[]): void {
  const t = new Date().toISOString().slice(11, 23); // HH:MM:SS.mmm
  console.log(`[${t}]`, ...args);
}

main();
