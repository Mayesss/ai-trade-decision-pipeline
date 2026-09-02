import { RESTING_ENTRY_MAX_AGE_MINUTES } from './decisionConfig';

// Resting-entry chart windows: each resting entry (pullback limit or stop
// entry) drawn as a side-colored dashed segment at its resting price, spanning
// the time it actually rested.
//
// Historical windows come from the indexed BUY/SELL rows carrying EITHER
// resting price — a stop entry (entry_stop_price) rests exactly like a limit
// and must draw the same way; reading only entry_limit_price left every stop
// entry invisible here. Consecutive re-issues of the same price merge into one
// segment. An issue rests until something actually cancels it — a later
// BUY/SELL supersede or a withdraw_resting_entry, capped at the age backstop —
// and a fill clips the segment at the position's entry. Live resting orders
// extend their chain (or open a fresh segment) up to now.
//
// Times are epoch seconds, like candles/markers.
export type RestingEntryWindow = {
  side: 'buy' | 'sell';
  price: number;
  fromTime: number;
  toTime: number;
  filled: boolean;
};

// Structural views of the rows this reads: indexed decision history, the chart
// position overlays (an opaque Record on the endpoint) and live broker orders.
type DecisionRow = {
  timestamp?: unknown;
  dryRun?: unknown;
  aiDecision?: Record<string, unknown> | null;
};
type PositionRow = { side?: unknown; entryTime?: unknown; entryPrice?: unknown };
type PendingOrderRow = { side: 'buy' | 'sell' | null; price: number; createdAtMs?: number | null };

// A standing entry survives evaluations and is bounded only by the age
// backstop, so a segment must be allowed to span that. 65min was the old
// one-tick-TTL assumption and truncated every long-resting order.
const REST_MAX_MS = RESTING_ENTRY_MAX_AGE_MINUTES * 60_000;
// Re-issues of the same price further apart than this are separate orders.
const REISSUE_MERGE_MS = 75 * 60_000;
// A fill's timestamp can sit slightly outside the window: it may have raced the
// cancel (analyze.ts's pending_entry_filled_during_supersede), and a fill's
// entry timestamp is the venue's, not the decision's.
const FILL_GRACE_MS = 2 * 60_000;
// A resting order fills at its price or better; a market entry at the same
// moment must not claim a segment the resting order never filled.
const FILL_PRICE_TOLERANCE = 0.01;

const normalizeSide = (value: unknown): 'buy' | 'sell' | null => {
  const raw = String(value ?? '').toLowerCase();
  if (raw.startsWith('sell') || raw.startsWith('short')) return 'sell';
  if (raw.startsWith('buy') || raw.startsWith('long')) return 'buy';
  return null;
};

export function buildRestingEntryWindows(params: {
  nowMs: number;
  history: DecisionRow[] | null | undefined;
  positions: PositionRow[];
  pendingOrders: PendingOrderRow[];
}): RestingEntryWindow[] {
  const { nowMs, positions, pendingOrders } = params;
  const history = params.history || [];
  const restRows = history
    .filter((h) => {
      if (h?.dryRun === true) return false;
      const a = String(h?.aiDecision?.action || '').toUpperCase();
      const rest = Number(h?.aiDecision?.entry_limit_price ?? h?.aiDecision?.entry_stop_price);
      return (a === 'BUY' || a === 'SELL') && Number.isFinite(rest) && rest > 0;
    })
    .map((h) => ({
      side: (String(h.aiDecision?.action).toUpperCase() === 'SELL' ? 'sell' : 'buy') as 'buy' | 'sell',
      price: Number(h.aiDecision?.entry_limit_price ?? h.aiDecision?.entry_stop_price),
      tsMs: Number(h.timestamp),
    }))
    .filter((row) => Number.isFinite(row.tsMs))
    .sort((a, b) => a.tsMs - b.tsMs);
  // What actually ends a resting window, per analyze.ts's resting-entry
  // reconcile: a later BUY/SELL supersedes the order (cancel + place) and a
  // flat HOLD with withdraw_resting_entry takes it back. A plain HOLD does
  // NOT — silence keeps the order resting, which is the whole point of a
  // resting entry. Ending the segment at the next indexed tick regardless (the
  // old rule) truncated every order that rested through HOLDs to a single tick,
  // and hid its fill too, since the fill search is bounded by that same end.
  const cancelTimesMs = history
    .filter((h) => {
      if (h?.dryRun === true) return false;
      const a = String(h?.aiDecision?.action || '').toUpperCase();
      return a === 'BUY' || a === 'SELL' || h?.aiDecision?.withdraw_resting_entry === true;
    })
    .map((h) => Number(h.timestamp))
    .filter((t) => Number.isFinite(t))
    .sort((a, b) => a - b);
  // Fill candidates carry side and entry price too: over a window that can span
  // hours, time alone would let an unrelated entry claim a segment.
  const positionEntries = positions
    .map((p) => ({
      ms: Number(p.entryTime) * 1000,
      side: normalizeSide(p.side),
      price: Number(p.entryPrice),
    }))
    .filter((e) => Number.isFinite(e.ms) && e.ms > 0)
    .sort((a, b) => a.ms - b.ms);

  const chains: Array<{ side: 'buy' | 'sell'; price: number; firstMs: number; lastMs: number }> = [];
  for (const row of restRows) {
    const prev = chains[chains.length - 1];
    if (prev && prev.side === row.side && prev.price === row.price && row.tsMs - prev.lastMs <= REISSUE_MERGE_MS) {
      prev.lastMs = row.tsMs;
    } else {
      chains.push({ side: row.side, price: row.price, firstMs: row.tsMs, lastMs: row.tsMs });
    }
  }

  const windows: RestingEntryWindow[] = chains.map((chain) => {
    // A re-issue is a cancel + place, so the age backstop runs from the last issue.
    const cancelMs = cancelTimesMs.find((t) => t > chain.lastMs);
    let endMs = Math.min(cancelMs ?? Infinity, chain.lastMs + REST_MAX_MS, nowMs);
    const fill = positionEntries.find(
      (e) =>
        e.side === chain.side &&
        e.ms >= chain.firstMs - FILL_GRACE_MS &&
        e.ms <= endMs + FILL_GRACE_MS &&
        (!Number.isFinite(e.price) ||
          e.price <= 0 ||
          Math.abs(e.price - chain.price) / chain.price <= FILL_PRICE_TOLERANCE),
    );
    if (fill) {
      endMs = Math.min(Math.max(fill.ms, chain.firstMs + 60_000), nowMs);
    } else {
      // Nothing rests through an open position: entries are only placed while
      // flat, and one that survives to a fill IS the position. So a position
      // opening inside the window ends it even when the match above did not
      // recognize it as the fill (a badly slipped stop entry, a position row
      // whose price we could not read).
      const opened = positionEntries.find((e) => e.ms > chain.firstMs + FILL_GRACE_MS && e.ms <= endMs);
      if (opened) endMs = Math.max(opened.ms, chain.firstMs + 60_000);
    }
    return {
      side: chain.side,
      price: chain.price,
      fromTime: Math.floor(chain.firstMs / 1000),
      toTime: Math.floor(endMs / 1000),
      filled: fill !== undefined,
    };
  });

  // A live resting order is still resting right now, whatever the history says:
  // extend its chain to now, or open a fresh segment when no chain matches.
  for (const order of pendingOrders) {
    if (!order.side) continue;
    const chain = windows.find(
      (s) =>
        !s.filled &&
        s.side === order.side &&
        s.price === order.price &&
        (order.createdAtMs == null || order.createdAtMs / 1000 <= s.toTime + 120),
    );
    if (chain) {
      chain.toTime = Math.floor(nowMs / 1000);
    } else {
      const createdMs = order.createdAtMs ?? nowMs - 15 * 60_000;
      windows.push({
        side: order.side,
        price: order.price,
        fromTime: Math.floor(createdMs / 1000),
        toTime: Math.floor(nowMs / 1000),
        filled: false,
      });
    }
  }
  windows.sort((a, b) => a.fromTime - b.fromTime);
  return windows;
}
