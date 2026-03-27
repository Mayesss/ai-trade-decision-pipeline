const DAY_MS = 24 * 60 * 60 * 1000;
const WEEK_MS = 7 * DAY_MS;

export function startOfScalpV2UtcDay(tsMs: number): number {
  const d = new Date(tsMs);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

export function startOfScalpV2WeekMondayUtc(tsMs: number): number {
  const dayStartMs = startOfScalpV2UtcDay(tsMs);
  const dayOfWeek = new Date(dayStartMs).getUTCDay();
  const daysSinceMonday = (dayOfWeek + 6) % 7;
  return dayStartMs - daysSinceMonday * DAY_MS;
}

export function isScalpV2UtcSunday(tsMs: number): boolean {
  return new Date(startOfScalpV2UtcDay(tsMs)).getUTCDay() === 0;
}

export function resolveScalpV2CompletedWeekWindowToUtc(tsMs: number): number {
  const mondayStartMs = startOfScalpV2WeekMondayUtc(tsMs);
  // V2 rolls completed-week windows on Sunday UTC because Sunday is non-trading.
  return isScalpV2UtcSunday(tsMs) ? mondayStartMs + WEEK_MS : mondayStartMs;
}
