const DAY_MS = 24 * 60 * 60 * 1000;
const WEEK_MS = 7 * DAY_MS;

export function startOfUtcDay(tsMs: number): number {
  const d = new Date(tsMs);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

export function startOfWeekMondayUtc(tsMs: number): number {
  const dayStartMs = startOfUtcDay(tsMs);
  const dayOfWeek = new Date(dayStartMs).getUTCDay();
  const daysSinceMonday = (dayOfWeek + 6) % 7;
  return dayStartMs - daysSinceMonday * DAY_MS;
}

export function isUtcSunday(tsMs: number): boolean {
  return new Date(startOfUtcDay(tsMs)).getUTCDay() === 0;
}

export function resolveCompletedWeekWindowToUtc(tsMs: number): number {
  const mondayStartMs = startOfWeekMondayUtc(tsMs);
  // Pipeline rolls completed-week windows on Sunday (UTC) because Sunday is non-trading.
  return isUtcSunday(tsMs) ? mondayStartMs + WEEK_MS : mondayStartMs;
}

export function resolveLastCompletedWeekBoundsUtc(nowMs: number): {
  startCurrentWeekMondayMs: number;
  lastCompletedWeekEndMs: number;
} {
  const startCurrentWeekMondayMs = resolveCompletedWeekWindowToUtc(nowMs);
  return {
    startCurrentWeekMondayMs,
    lastCompletedWeekEndMs: startCurrentWeekMondayMs - 1,
  };
}
