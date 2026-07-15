import { kvExpire, kvGetJson, kvIncr, kvSetJson } from '../kv';
import { getCronSymbolConfigs } from '../symbolRegistry';

// Countdown latch for the dashboard summary warm. All swing analyze crons fire
// on the same 15-minute schedule (vercel.json); each invocation increments its
// cycle's counter when it finishes, and the one that brings the count to the
// number of configured analyze crons rebuilds the summary blobs. That way the
// warm always runs after the LAST decision of the cycle landed, instead of at a
// fixed +3min offset that races long-running analyzes. The summary-warm-fallback
// cron covers cycles where an analyze crashed and the latch never completed.
const CYCLE_MS = 15 * 60 * 1000;
// Long enough to outlive any cycle (analyzes run 1-2min, maxDuration 800s),
// short enough that stale latch keys never collide with a future cycle.
const LATCH_TTL_SECONDS = 20 * 60;

// 15-minute bucket aligned to :00/:15/:30/:45 — every cron invocation of the
// same firing resolves to the same id regardless of start jitter.
export function swingWarmCycleId(nowMs: number): number {
  return Math.floor(nowMs / CYCLE_MS);
}

function latchKey(cycleId: number): string {
  return `swing:warm:latch:${cycleId}`;
}

function doneKey(cycleId: number): string {
  return `swing:warm:done:${cycleId}`;
}

// Count this invocation toward the cycle's latch. Returns true iff this caller
// is the last expected finisher — INCR is atomic, so exactly one caller per
// cycle sees true. The expected count comes from vercel.json via the symbol
// registry, so adding/removing an analyze cron adjusts the latch automatically.
export async function recordSwingAnalyzeFinished(cycleId: number): Promise<boolean> {
  const expected = getCronSymbolConfigs().length;
  if (expected <= 0) return false;
  const count = await kvIncr(latchKey(cycleId));
  await kvExpire(latchKey(cycleId), LATCH_TTL_SECONDS).catch(() => undefined);
  return count === expected;
}

// Monotonic "a warm completed" stamp for open dashboards (see the warm-status
// endpoint): unlike the per-cycle done flag it lives under one stable key and
// never expires, so a client can poll it cheaply and refresh exactly when the
// warmedAtMs moves forward.
const LAST_WARM_KEY = 'swing:warm:last';

export type SwingWarmLast = { warmedAtMs: number; cycleId: number };

export async function markSwingWarmDone(cycleId: number): Promise<void> {
  const stamp: SwingWarmLast = { warmedAtMs: Date.now(), cycleId };
  await kvSetJson(doneKey(cycleId), stamp, LATCH_TTL_SECONDS);
  await kvSetJson(LAST_WARM_KEY, stamp).catch(() => undefined);
}

export async function isSwingWarmDone(cycleId: number): Promise<boolean> {
  const flag = await kvGetJson<SwingWarmLast>(doneKey(cycleId)).catch(() => null);
  return Boolean(flag?.warmedAtMs);
}

export async function readSwingWarmLast(): Promise<SwingWarmLast | null> {
  return kvGetJson<SwingWarmLast>(LAST_WARM_KEY).catch(() => null);
}
