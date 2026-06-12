const ONE_DAY_MS = 24 * 60 * 60 * 1000;
export const SCALP_REGIME_ONE_WEEK_MS = 7 * ONE_DAY_MS;

export function startOfUtcDayMs(tsMs: number): number {
  const d = new Date(Math.floor(Number(tsMs) || 0));
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

export function startOfUtcWeekMondayMs(tsMs: number): number {
  const dayStart = startOfUtcDayMs(tsMs);
  const dayOfWeek = new Date(dayStart).getUTCDay();
  return dayStart - ((dayOfWeek + 6) % 7) * ONE_DAY_MS;
}

export function completedPriorWeekStartForDecisionMs(nowMs: number): number {
  return startOfUtcWeekMondayMs(nowMs) - SCALP_REGIME_ONE_WEEK_MS;
}

export function validityWeekStartFromCompletedWeekMs(completedWeekStartMs: number): number {
  return startOfUtcWeekMondayMs(completedWeekStartMs) + SCALP_REGIME_ONE_WEEK_MS;
}

export function weekStartForEntryMs(entryTsMs: number): number {
  return startOfUtcWeekMondayMs(entryTsMs);
}

