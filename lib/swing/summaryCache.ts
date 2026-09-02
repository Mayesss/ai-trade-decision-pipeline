import { kvDel } from '../kv';

// Single source of truth for the dashboard summary cache keys, shared by the
// summary endpoint (writer) and the analyze cron (invalidator). The summary is
// expensive to build (per-symbol broker calls + decision history), so we cache it
// for a long window and bust it from the cron whenever a new decision is recorded
// — fresh right after each hourly tick, served from KV in between.
export const SWING_SUMMARY_CACHE_KEY_PREFIX = 'swing:dashboard:summary:v6';
const SUMMARY_RANGES = ['1D', '7D', '30D', '6M'] as const;

export function swingSummaryCacheKey(range: string): string {
  return `${SWING_SUMMARY_CACHE_KEY_PREFIX}:${range}`;
}

// Drop all range variants so the next dashboard load recomputes once with fresh
// data. One multi-key DEL — Upstash bills per command, so four separate DELs
// cost four commands on every persisted decision (~228/day). Best-effort —
// never throws into the trading path.
export async function invalidateSwingSummaryCache(): Promise<void> {
  await kvDel(...SUMMARY_RANGES.map((range) => swingSummaryCacheKey(range))).catch(() => undefined);
}
