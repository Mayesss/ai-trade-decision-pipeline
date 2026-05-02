const DAY_MS = 24 * 60 * 60 * 1000;
const WEEK_MS = 7 * DAY_MS;
const MINUTE_MS = 60 * 1000;

type WeekCompleteSession = "tokyo" | "berlin" | "newyork" | "pacific" | "sydney";

const DEFAULT_WEEK_COMPLETE_FINAL_SESSION: WeekCompleteSession = "pacific";
const DEFAULT_WEEK_COMPLETE_GRACE_MINUTES = 60;

const WEEK_COMPLETE_SESSION_DEFINITIONS: Record<
  WeekCompleteSession,
  { timeZone: string; closeClock: string }
> = {
  tokyo: { timeZone: "Asia/Tokyo", closeClock: "13:00" },
  berlin: { timeZone: "Europe/Berlin", closeClock: "12:00" },
  newyork: { timeZone: "America/New_York", closeClock: "12:00" },
  pacific: { timeZone: "America/Los_Angeles", closeClock: "14:00" },
  sydney: { timeZone: "Australia/Sydney", closeClock: "12:00" },
};

function parseWeekCompleteSession(value: string | undefined): WeekCompleteSession {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (
    normalized === "tokyo" ||
    normalized === "berlin" ||
    normalized === "newyork" ||
    normalized === "pacific" ||
    normalized === "sydney"
  ) {
    return normalized;
  }
  return DEFAULT_WEEK_COMPLETE_FINAL_SESSION;
}

function parseGraceMinutes(value: string | undefined): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return DEFAULT_WEEK_COMPLETE_GRACE_MINUTES;
  return Math.min(24 * 60, Math.floor(n));
}

function partsForTimeZone(
  tsMs: number,
  timeZone: string,
): { y: number; m: number; d: number; hh: number; mm: number } {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    hourCycle: "h23",
  });
  const parts = fmt.formatToParts(new Date(tsMs));
  const read = (type: Intl.DateTimeFormatPartTypes, fallback: number): number => {
    const raw = parts.find((p) => p.type === type)?.value;
    const n = Number(raw);
    return Number.isFinite(n) ? n : fallback;
  };
  const hh = read("hour", 0);
  return {
    y: read("year", 1970),
    m: read("month", 1),
    d: read("day", 1),
    hh: hh === 24 ? 0 : hh,
    mm: read("minute", 0),
  };
}

function utcMsFromZonedClock(params: {
  y: number;
  m: number;
  d: number;
  clock: string;
  timeZone: string;
}): number {
  const match = params.clock.match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
  const targetHour = match ? Number(match[1]) : 0;
  const targetMinute = match ? Number(match[2]) : 0;
  const targetDayInt = params.y * 10_000 + params.m * 100 + params.d;
  const targetMinuteOfDay = targetHour * 60 + targetMinute;
  let guessMs = Date.UTC(params.y, params.m - 1, params.d, targetHour, targetMinute, 0, 0);

  for (let i = 0; i < 6; i += 1) {
    const local = partsForTimeZone(guessMs, params.timeZone);
    const localDayInt = local.y * 10_000 + local.m * 100 + local.d;
    const dayDelta =
      localDayInt === targetDayInt ? 0 : localDayInt < targetDayInt ? 1 : -1;
    const localMinuteOfDay = local.hh * 60 + local.mm;
    const deltaMinutes = dayDelta * 1440 + (targetMinuteOfDay - localMinuteOfDay);
    if (deltaMinutes === 0) break;
    guessMs += deltaMinutes * MINUTE_MS;
  }

  return guessMs;
}

export function resolveScalpV2WeekCompleteConfig(): {
  finalSession: WeekCompleteSession;
  graceMinutes: number;
} {
  return {
    finalSession: parseWeekCompleteSession(
      process.env.SCALP_V2_WEEK_COMPLETE_FINAL_SESSION,
    ),
    graceMinutes: parseGraceMinutes(
      process.env.SCALP_V2_WEEK_COMPLETE_GRACE_MINUTES,
    ),
  };
}

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

export function resolveScalpV2WeekCompleteAtUtc(tsMs: number): number {
  const weekStartMs = startOfScalpV2WeekMondayUtc(tsMs);
  const saturdayNoonUtcMs = weekStartMs + 5 * DAY_MS + 12 * 60 * MINUTE_MS;
  const { finalSession, graceMinutes } = resolveScalpV2WeekCompleteConfig();
  const definition = WEEK_COMPLETE_SESSION_DEFINITIONS[finalSession];
  const finalSessionLocalSaturday = partsForTimeZone(
    saturdayNoonUtcMs,
    definition.timeZone,
  );
  const finalSessionCloseMs = utcMsFromZonedClock({
    y: finalSessionLocalSaturday.y,
    m: finalSessionLocalSaturday.m,
    d: finalSessionLocalSaturday.d,
    clock: definition.closeClock,
    timeZone: definition.timeZone,
  });
  return finalSessionCloseMs + graceMinutes * MINUTE_MS;
}

export function resolveScalpV2CompletedWeekWindowToUtc(tsMs: number): number {
  const mondayStartMs = startOfScalpV2WeekMondayUtc(tsMs);
  // V2 rolls completed-week windows once the configured final Saturday
  // session has closed and a grace period has elapsed.
  const weekCompleteAtMs = resolveScalpV2WeekCompleteAtUtc(tsMs);
  return tsMs >= weekCompleteAtMs ? mondayStartMs + WEEK_MS : mondayStartMs;
}
