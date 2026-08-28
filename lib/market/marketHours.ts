import type {
  OpeningHoursSchedule
} from "./symbolMarketMetadata";
type MarketHoursConfig = {
  fallbackFridayCloseHourUtc: number;
  fallbackSundayOpenHourUtc: number;
  entryBlockMinutes: number;
  forceCloseMinutes: number;
};

export type MarketGate = {
  marketClosed: boolean;
  entryBlocked: boolean;
  forceCloseNow: boolean;
  reasonCode:
    | "MARKET_OPEN"
    | "MARKET_CLOSED_WEEKEND"
    | "WEEKEND_ENTRY_BLOCK"
    | "WEEKEND_FORCE_CLOSE"
    | "WEEKEND_POLICY_DISABLED"
    | "MARKET_CLOSED_SESSION"
    | "SESSION_ENTRY_BLOCK"
    | "SESSION_FORCE_CLOSE";
  reopensAtMs: number | null;
  closesAtMs: number | null;
  config: MarketHoursConfig;
};

export type WeekendGate = MarketGate;

type ResolvedScheduleWindow = {
  openAtMs: number;
  closeAtMs: number;
};

const DAY_MS = 24 * 60 * 60_000;

function parseClockMinutes(value: string): number | null {
  const match = String(value || "")
    .trim()
    .match(/^(\d{2}):(\d{2})$/);
  if (!match?.[1] || !match?.[2]) return null;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
  return hour * 60 + minute;
}

function weekStartMondayUtcMs(nowMs: number): number {
  const now = new Date(nowMs);
  const startOfDayUtc = Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
  );
  const day = new Date(startOfDayUtc).getUTCDay();
  const daysSinceMonday = (day + 6) % 7;
  return startOfDayUtc - daysSinceMonday * DAY_MS;
}

function dayOffsetFromMonday(day: string): number {
  if (day === "mon") return 0;
  if (day === "tue") return 1;
  if (day === "wed") return 2;
  if (day === "thu") return 3;
  if (day === "fri") return 4;
  if (day === "sat") return 5;
  return 6;
}

function supportsUtcSchedule(
  openingHours: OpeningHoursSchedule | null | undefined,
): boolean {
  if (!openingHours) return false;
  const zone = String(openingHours.zone || "")
    .trim()
    .toUpperCase();
  return !zone || zone === "UTC" || zone === "GMT" || zone === "ETC/UTC";
}

function buildScheduleWindows(
  openingHours: OpeningHoursSchedule,
  nowMs: number,
): ResolvedScheduleWindow[] {
  const weekStartMs = weekStartMondayUtcMs(nowMs);
  const out: ResolvedScheduleWindow[] = [];
  for (const weekOffset of [0, 1]) {
    const baseWeekMs = weekStartMs + weekOffset * 7 * DAY_MS;
    for (const row of openingHours.windows) {
      const openMinutes = parseClockMinutes(row.openTime);
      const closeMinutes = parseClockMinutes(row.closeTime);
      if (openMinutes === null || closeMinutes === null) continue;
      const startOfDayMs = baseWeekMs + dayOffsetFromMonday(row.day) * DAY_MS;
      const openAtMs = startOfDayMs + openMinutes * 60_000;
      const closeAtMs =
        startOfDayMs + Math.min(24 * 60, closeMinutes + 1) * 60_000;
      if (!(closeAtMs > openAtMs)) continue;
      out.push({ openAtMs, closeAtMs });
    }
  }
  return out.sort((lhs, rhs) => lhs.openAtMs - rhs.openAtMs);
}

export type OpeningHoursState = {
  // null = unknown (no schedule, or a non-UTC zone we can't safely interpret).
  // Callers must treat null as "no data", never as "closed".
  isOpen: boolean | null;
  // Start of the CURRENT merged session span (null when closed/unknown) —
  // "how long has this session been open" for post-open warmup gating.
  openedAtMs: number | null;
  closesAtMs: number | null;
  nextOpenAtMs: number | null;
};

// Point-in-time view of a venue schedule for callers that need "when does the
// current session end / when does the next one begin" (e.g. the swing prompt's
// venue-session context) rather than the replay gate semantics above.
// Contiguous windows (an overnight "10:00-00:00" split at midnight plus the
// next day's "00:00-02:00") are merged into continuous spans so closesAtMs is
// the real session end, not a midnight day-boundary artifact.
export function resolveOpeningHoursState(
  openingHours: OpeningHoursSchedule | null | undefined,
  nowMs: number,
): OpeningHoursState {
  if (!openingHours || !supportsUtcSchedule(openingHours)) {
    return { isOpen: null, openedAtMs: null, closesAtMs: null, nextOpenAtMs: null };
  }
  if (openingHours.alwaysOpen) {
    return { isOpen: true, openedAtMs: null, closesAtMs: null, nextOpenAtMs: null };
  }
  const windows = buildScheduleWindows(openingHours, nowMs);
  if (!windows.length) {
    return { isOpen: null, openedAtMs: null, closesAtMs: null, nextOpenAtMs: null };
  }
  const spans: ResolvedScheduleWindow[] = [];
  for (const window of windows) {
    const last = spans[spans.length - 1];
    if (last && window.openAtMs <= last.closeAtMs) {
      last.closeAtMs = Math.max(last.closeAtMs, window.closeAtMs);
    } else {
      spans.push({ ...window });
    }
  }
  const current = spans.find(
    (span) => span.openAtMs <= nowMs && nowMs < span.closeAtMs,
  );
  if (current) {
    const reopen = spans.find((span) => span.openAtMs >= current.closeAtMs);
    return {
      isOpen: true,
      openedAtMs: current.openAtMs,
      closesAtMs: current.closeAtMs,
      nextOpenAtMs: reopen?.openAtMs ?? null,
    };
  }
  const next = spans.find((span) => span.openAtMs > nowMs);
  return {
    isOpen: false,
    openedAtMs: null,
    closesAtMs: next?.closeAtMs ?? null,
    nextOpenAtMs: next?.openAtMs ?? null,
  };
}
