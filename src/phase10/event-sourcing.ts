/**
 * Phase 10 — EVENT SOURCING: store the EVENTS, derive the state.
 *
 * Run with:  node "src/phase10/event-sourcing.ts"   (dependency-free, in-process)
 *
 * ─── The big mental shift ────────────────────────────────────────────────
 * State-oriented storage keeps one row per thing and OVERWRITES it in place:
 * the cart table says {milk, eggs} today, and yesterday's contents are gone
 * forever. You stored the ANSWER but threw away the QUESTIONS that produced it.
 *
 * Event sourcing flips that. The source of truth is an append-only EVENT STORE
 * — an ordered, versioned log of everything that HAPPENED (ItemAdded,
 * ItemRemoved, CheckedOut). Current state is not stored at all; it is COMPUTED
 * as a left-fold of the events:
 *
 *     state = events.reduce(apply, EMPTY)
 *
 * `apply(state, event)` is the reducer that evolves state one event at a time.
 * That single equation buys you a lot:
 *
 *   - REBUILD:      lose the in-memory state? Replay the log from zero and it
 *                   comes back, exactly. State is reproducible, not precious.
 *   - AUDIT/TIME-TRAVEL: fold only the first k events to see the state "as of"
 *                   any past point — a free temporal query and audit trail.
 *   - READ MODELS:  a new projection (a report, a search index, a cache) is just
 *                   a different fold over the same log — build it any time.
 *   - SNAPSHOT:     for a long-lived aggregate with millions of events, persist
 *                   the folded state at version k, then rebuild = load snapshot
 *                   + fold only the events AFTER k. Bounds the replay cost.
 *
 * This is the same retained-log intuition from phase6 (Kafka): reading never
 * destroys anything, so many readers can replay history at their own pace. Here
 * the "reader" is our fold, and "history" is the life of one aggregate.
 *
 * The trade-off vs. state-oriented rows: event sourcing keeps WHY (the intent
 * behind every change) and enables audit/replay/temporal queries, at the cost of
 * more storage and eventually-consistent projections. Splitting the write side
 * (events) from the read side (projections) is the sibling pattern, CQRS.
 *
 * TAKEAWAY: event sourcing stores the events and derives state by folding them —
 * so state is reproducible, auditable, and time-travellable, and you can rebuild
 * any read model by replaying; snapshots bound the replay cost. The price is more
 * storage and thinking in events, not rows.
 */

// ─── EVENTS: the facts. Past tense, because they already happened. ─────────
// Each is a small immutable record of intent — the "WHY" a row would discard.

interface ItemAdded {
  type: "ItemAdded";
  item: string;
  price: number; // cents
}
interface ItemRemoved {
  type: "ItemRemoved";
  item: string;
}
interface CheckedOut {
  type: "CheckedOut";
}

type CartEvent = ItemAdded | ItemRemoved | CheckedOut;

/** What the event store actually persists: the event, plus its version + time. */
interface StoredEvent {
  version: number; // 1-based position in this aggregate's log
  at: number; // logical timestamp (a counter — deterministic, no wall clock)
  event: CartEvent;
}

// ─── STATE: the derived view. Never stored; always folded from events. ─────

interface CartState {
  items: Record<string, number>; // item -> count
  totalCents: number;
  checkedOut: boolean;
}

const EMPTY: CartState = { items: {}, totalCents: 0, checkedOut: false };

// A tiny price book so ItemRemoved knows what to subtract from the total.
const PRICES: Record<string, number> = { milk: 250, eggs: 400, bread: 300 };

/**
 * apply(state, event) — THE FOLD FUNCTION (a reducer).
 *
 * Pure: takes the old state + one event, returns the new state. It never
 * mutates its input, so folding the same events always yields the same result.
 * This one function IS the business logic; everything else just replays it.
 */
function apply(state: CartState, event: CartEvent): CartState {
  switch (event.type) {
    case "ItemAdded":
      return {
        ...state,
        items: { ...state.items, [event.item]: (state.items[event.item] ?? 0) + 1 },
        totalCents: state.totalCents + event.price,
      };
    case "ItemRemoved": {
      const count = state.items[event.item] ?? 0;
      if (count === 0) return state; // nothing to remove
      const items = { ...state.items };
      if (count === 1) delete items[event.item];
      else items[event.item] = count - 1;
      return { ...state, items, totalCents: state.totalCents - (PRICES[event.item] ?? 0) };
    }
    case "CheckedOut":
      return { ...state, checkedOut: true };
  }
}

/** Fold a slice of stored events into a state, starting from `base`. */
function fold(events: StoredEvent[], base: CartState = EMPTY): CartState {
  return events.reduce((state, stored) => apply(state, stored.event), base);
}

// ─── THE EVENT STORE: append-only, ordered, versioned. ─────────────────────
// Real stores are EventStoreDB / a Kafka topic / an `events` table with a
// (stream_id, version) primary key. Ours is an array — same shape, no I/O.

let clock = 0; // logical time source: deterministic, replaces Date.now()

class EventStore {
  private events: StoredEvent[] = [];

  /** The ONLY write operation: append. Events are immutable once stored. */
  append(event: CartEvent): StoredEvent {
    const stored: StoredEvent = { version: this.events.length + 1, at: ++clock, event };
    this.events.push(stored);
    log(`  ⤵ appended  v${stored.version}  ${describe(event)}`);
    return stored;
  }

  /** Read the whole log (or up to `toVersion` inclusive) — reading never mutates. */
  read(toVersion = Infinity): StoredEvent[] {
    return this.events.filter((e) => e.version <= toVersion);
  }

  get length(): number {
    return this.events.length;
  }
}

/** A SNAPSHOT: the folded state cached at a version, so replay can skip ahead. */
interface Snapshot {
  version: number; // state is valid as of this version...
  state: CartState; // ...and equals fold(events[1..version])
}

// ─── COMMANDS: requests to change state. A command may be rejected; an event ─
// is a fact that already happened. Here commands map 1:1 to events for clarity.

function addItem(store: EventStore, item: string): void {
  store.append({ type: "ItemAdded", item, price: PRICES[item] ?? 0 });
}
function removeItem(store: EventStore, item: string): void {
  store.append({ type: "ItemRemoved", item });
}
function checkout(store: EventStore): void {
  store.append({ type: "CheckedOut" });
}

async function main(): Promise<void> {
  const store = new EventStore();

  // ─── A) COMMANDS → EVENTS → STATE ───────────────────────────────────────
  log("═══ A) COMMANDS → EVENTS → STATE ═══");
  log("issue commands; each appends one event to the store:");
  addItem(store, "milk"); // v1
  addItem(store, "eggs"); // v2
  addItem(store, "bread"); // v3
  removeItem(store, "bread"); // v4  (changed our mind)
  addItem(store, "eggs"); // v5  (a second carton)

  const live = fold(store.read());
  log("current state, derived by folding all 5 events:");
  log("   " + render(live));
  log("");

  // ─── B) REBUILD BY REPLAY ───────────────────────────────────────────────
  // The defining property: throw the state away and reconstruct it purely from
  // the log. Nothing about `live` was persisted — only the events were.
  log("═══ B) REBUILD BY REPLAY ═══");
  log("discard the in-memory state; rebuild it from the event log alone:");
  const rebuilt = fold(store.read());
  log("   " + render(rebuilt));
  log(`   matches the live state? ${equal(live, rebuilt) ? "✓ yes — state is reproducible" : "✗ no"}`);
  log("");

  // ─── C) TIME-TRAVEL / AUDIT ─────────────────────────────────────────────
  // Fold only the first k events to see the state "as of" an earlier version.
  // State-oriented storage overwrote these; the log still has every step.
  log("═══ C) TIME-TRAVEL / AUDIT ═══");
  log("fold only the first k events to query the state 'as of' version k:");
  for (let k = 1; k <= store.length; k++) {
    log(`   as of v${k}:  ${render(fold(store.read(k)))}`);
  }
  log("→ a free audit trail: every intermediate state is recoverable.");
  log("");

  // ─── D) SNAPSHOT ────────────────────────────────────────────────────────
  // For a long-lived aggregate, folding millions of events on every read is
  // wasteful. Persist a snapshot at version k; then rebuild = snapshot + fold
  // ONLY the events after k. Same answer, far fewer events replayed.
  log("═══ D) SNAPSHOT ═══");
  const snapAt = 3;
  const snapshot: Snapshot = { version: snapAt, state: fold(store.read(snapAt)) };
  log(`took a snapshot at v${snapshot.version}:  ${render(snapshot.state)}`);

  // A few more commands arrive after the snapshot.
  checkout(store); // v6

  // Naive rebuild: fold the entire log from zero.
  const tail = store.read().filter((e) => e.version > snapshot.version);
  const fromZero = fold(store.read());
  // Snapshot rebuild: start from the snapshot, fold only the tail.
  const fromSnapshot = fold(tail, snapshot.state);

  log(`rebuild from zero      → folded ${store.length} events:  ${render(fromZero)}`);
  log(`rebuild from snapshot  → folded ${tail.length} event(s): ${render(fromSnapshot)}`);
  log(`   identical result? ${equal(fromZero, fromSnapshot) ? "✓ yes" : "✗ no"}`);
  log(
    `   replay cost: ${store.length} events → ${tail.length}. For an aggregate with a`,
  );
  log("   million events, the snapshot is the difference between usable and not.");
  log("");

  // ─── Contrast with state-oriented storage ───────────────────────────────
  log("═══ EVENT-SOURCED vs. STATE-ORIENTED ═══");
  log("state-oriented row would hold only the final answer:");
  log(`   cart = ${render(fromZero)}   ← no history, no 'why'`);
  log("the event log holds the whole story that produced it:");
  for (const e of store.read()) log(`   v${e.version} @t${e.at}  ${describe(e.event)}`);
  log("→ event sourcing keeps the intent behind every change, so you can audit,");
  log("  replay, time-travel, and rebuild new read models (that's CQRS) — paying");
  log("  in storage + eventual-consistency projections, and thinking in events.");

  process.exit(0);
}

// ─── Small presentation + logging helpers (this repo has no shared lib) ────

/** One-line human description of an event, for the log. */
function describe(event: CartEvent): string {
  switch (event.type) {
    case "ItemAdded":
      return `ItemAdded(${event.item}, ${money(event.price)})`;
    case "ItemRemoved":
      return `ItemRemoved(${event.item})`;
    case "CheckedOut":
      return "CheckedOut()";
  }
}

/** Render a derived state compactly. */
function render(state: CartState): string {
  const items = Object.entries(state.items)
    .map(([item, n]) => (n > 1 ? `${item}×${n}` : item))
    .join(", ");
  const co = state.checkedOut ? ", checked-out" : "";
  return `{ ${items || "empty"} }  total ${money(state.totalCents)}${co}`;
}

/** Structural equality — proves two folds produced the same state. */
function equal(a: CartState, b: CartState): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

const money = (cents: number): string => `$${(cents / 100).toFixed(2)}`;

/**
 * Timestamped logging. To stay DETERMINISTIC (see the header: no wall clock in
 * logic), the timestamp is a monotonic counter, not Date.now(). Same run, same
 * output, every time.
 */
let logSeq = 0;
function log(...args: unknown[]): void {
  const t = String(++logSeq).padStart(3, "0");
  console.log(`[t${t}]`, ...args);
}

main();
