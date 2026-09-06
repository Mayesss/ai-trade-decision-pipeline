import { kvSetJson } from '../kv';

// Single source of truth for the dashboard summary cache keys, shared by the
// summary endpoint (writer) and the analyze cron (staleness marker). The summary
// is expensive to build (per-symbol broker calls + decision history), so we cache
// it for a long window; the cycle's warm rewrites it, and a decision recorded in
// between marks it stale rather than removing it.
export const SWING_SUMMARY_CACHE_KEY_PREFIX = 'swing:dashboard:summary:v6';
// When a decision last made every range blob out of date. Compared against a
// blob's own generatedAtMs, so one key covers all four ranges.
export const SWING_SUMMARY_STALE_KEY = 'swing:dashboard:summary:stale:v1';
// Long enough to outlive any blob (the blobs carry a 1h TTL), short enough that
// a dead marker cannot linger forever.
const STALE_MARKER_TTL_SECONDS = 6 * 60 * 60;

export function swingSummaryCacheKey(range: string): string {
  return `${SWING_SUMMARY_CACHE_KEY_PREFIX}:${range}`;
}

// A new decision landed, so every range blob is now behind. Marks the moment
// instead of DELETING the blobs, which is what this used to do.
//
// Deleting made the next dashboard load rebuild from scratch: 130 KV commands
// and ~5s of per-symbol broker calls, paid by whoever happened to open the page.
// And it was not rare — every persisted decision invalidates (~100/hour across
// the universe), while the rebuild only happens at the END of the 15-minute
// cycle via the warm latch, so the cache sat empty through the whole fan-out,
// four times an hour, with no single-flight guard: two concurrent loads in that
// window each paid the full rebuild.
//
// Marking instead keeps the previous blob servable. A reader gets it
// immediately, flagged `stale`, and the cycle's warm (or the fallback cron)
// replaces it minutes later. Same one command as the DEL it replaces; the
// reader folds this key into an MGET it was already issuing, so it is free
// there too. Best-effort — never throws into the trading path.
export async function markSwingSummaryStale(): Promise<void> {
  await kvSetJson(SWING_SUMMARY_STALE_KEY, { at: Date.now() }, STALE_MARKER_TTL_SECONDS).catch(() => undefined);
}
