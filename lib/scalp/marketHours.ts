import type {
  ScalpOpeningHoursSchedule,
  ScalpSymbolMarketMetadata,
} from "./symbolMarketMetadata";
import { isWeekendClosedScalpSymbol } from "./symbolInfo";

type ScalpReplayMarketHoursConfig = {
  fallbackFridayCloseHourUtc: number;
  fallbackSundayOpenHourUtc: number;
  entryBlockMinutes: number;
  forceCloseMinutes: number;
};

export type ScalpReplayMarketGate = {
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
  config: ScalpReplayMarketHoursConfig;
};

export type ScalpReplayWeekendGate = ScalpReplayMarketGate;

type ResolvedScheduleWindow = {
  openAtMs: number;
  closeAtMs: number;
};

const DAY_MS = 24 * 60 * 60_000;

function toHour(value: string | undefined, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const rounded = Math.floor(n);
  if (rounded < 0 || rounded > 23) return fallback;
  return rounded;
}

function toNonNegativeInt(value: string | undefined, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return fallback;
  return Math.max(0, Math.floor(n));
}

function nextSundayAtHourUtc(fromMs: number, hourUtc: number): number {
  const date = new Date(fromMs);
  const day = date.getUTCDay();
  const daysUntilSunday = day === 0 ? 0 : 7 - day;
  const sunday = new Date(
    Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate() + daysUntilSunday,
      hourUtc,
      0,
      0,
      0,
    ),
  );
  if (sunday.getTime() <= fromMs) {
    sunday.setUTCDate(sunday.getUTCDate() + 7);
  }
  return sunday.getTime();
}

function fridayCloseForWeekUtc(nowMs: number, hourUtc: number): number {
  const now = new Date(nowMs);
  const day = now.getUTCDay();
  const startOfDayUtc = Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
  );
  const daysUntilFriday = 5 - day;
  return startOfDayUtc + daysUntilFriday * DAY_MS + hourUtc * 60 * 60_000;
}

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
  openingHours: ScalpOpeningHoursSchedule | null | undefined,
): boolean {
  if (!openingHours) return false;
  const zone = String(openingHours.zone || "")
    .trim()
    .toUpperCase();
  return !zone || zone === "UTC" || zone === "GMT" || zone === "ETC/UTC";
}

function buildScheduleWindows(
  openingHours: ScalpOpeningHoursSchedule,
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

function evaluateUtcOpeningHoursGate(params: {
  openingHours: ScalpOpeningHoursSchedule;
  nowMs: number;
  config: ScalpReplayMarketHoursConfig;
}): ScalpReplayMarketGate {
  const { openingHours, nowMs, config } = params;
  if (openingHours.alwaysOpen) {
    return {
      marketClosed: false,
      entryBlocked: false,
      forceCloseNow: false,
      reasonCode: "MARKET_OPEN",
      reopensAtMs: null,
      closesAtMs: null,
      config,
    };
  }

  const windows = buildScheduleWindows(openingHours, nowMs);
  const current = windows.find(
    (row) => row.openAtMs <= nowMs && nowMs < row.closeAtMs,
  );
  if (!current) {
    const nextOpen = windows.find((row) => row.openAtMs > nowMs) || null;
    return {
      marketClosed: true,
      entryBlocked: true,
      forceCloseNow: false,
      reasonCode: "MARKET_CLOSED_SESSION",
      reopensAtMs: nextOpen?.openAtMs ?? null,
      closesAtMs: nextOpen?.closeAtMs ?? null,
      config,
    };
  }

  const msUntilClose = current.closeAtMs - nowMs;
  const entryBlocked =
    config.entryBlockMinutes > 0 &&
    msUntilClose <= config.entryBlockMinutes * 60_000;
  const forceCloseNow =
    config.forceCloseMinutes > 0 &&
    msUntilClose <= config.forceCloseMinutes * 60_000;

  return {
    marketClosed: false,
    entryBlocked,
    forceCloseNow,
    reasonCode: forceCloseNow
      ? "SESSION_FORCE_CLOSE"
      : entryBlocked
        ? "SESSION_ENTRY_BLOCK"
        : "MARKET_OPEN",
    reopensAtMs: null,
    closesAtMs: current.closeAtMs,
    config,
  };
}

function evaluateFallbackWeekendGate(
  symbol: string,
  nowMs: number,
  config: ScalpReplayMarketHoursConfig,
): ScalpReplayMarketGate {
  if (!isWeekendClosedScalpSymbol(symbol)) {
    return {
      marketClosed: false,
      entryBlocked: false,
      forceCloseNow: false,
      reasonCode: "WEEKEND_POLICY_DISABLED",
      reopensAtMs: null,
      closesAtMs: null,
      config,
    };
  }

  const now = new Date(nowMs);
  const day = now.getUTCDay();
  const hour = now.getUTCHours();

  const fridayClosed = day === 5 && hour >= config.fallbackFridayCloseHourUtc;
  const saturdayClosed = day === 6;
  const sundayClosed = day === 0 && hour < config.fallbackSundayOpenHourUtc;

  if (fridayClosed || saturdayClosed || sundayClosed) {
    let reopensAtMs: number;
    if (sundayClosed) {
      reopensAtMs = Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        config.fallbackSundayOpenHourUtc,
        0,
        0,
        0,
      );
    } else {
      reopensAtMs = nextSundayAtHourUtc(
        nowMs,
        config.fallbackSundayOpenHourUtc,
      );
    }
    return {
      marketClosed: true,
      entryBlocked: true,
      forceCloseNow: false,
      reasonCode: "MARKET_CLOSED_WEEKEND",
      reopensAtMs,
      closesAtMs: fridayCloseForWeekUtc(
        nowMs,
        config.fallbackFridayCloseHourUtc,
      ),
      config,
    };
  }

  const closesAtMs = fridayCloseForWeekUtc(
    nowMs,
    config.fallbackFridayCloseHourUtc,
  );
  const msUntilClose = closesAtMs - nowMs;
  const entryBlocked =
    day === 5 &&
    config.entryBlockMinutes > 0 &&
    msUntilClose <= config.entryBlockMinutes * 60_000;
  const forceCloseNow =
    day === 5 &&
    config.forceCloseMinutes > 0 &&
    msUntilClose <= config.forceCloseMinutes * 60_000;

  return {
    marketClosed: false,
    entryBlocked,
    forceCloseNow,
    reasonCode: forceCloseNow
      ? "WEEKEND_FORCE_CLOSE"
      : entryBlocked
        ? "WEEKEND_ENTRY_BLOCK"
        : "MARKET_OPEN",
    reopensAtMs: nextSundayAtHourUtc(nowMs, config.fallbackSundayOpenHourUtc),
    closesAtMs,
    config,
  };
}

export function getScalpReplayMarketHoursConfig(): ScalpReplayMarketHoursConfig {
  return {
    fallbackFridayCloseHourUtc: toHour(
      process.env.SCALP_REPLAY_MARKET_CLOSE_FRI_UTC_HOUR,
      22,
    ),
    fallbackSundayOpenHourUtc: toHour(
      process.env.SCALP_REPLAY_MARKET_OPEN_SUN_UTC_HOUR,
      22,
    ),
    entryBlockMinutes: toNonNegativeInt(
      process.env.SCALP_REPLAY_SESSION_ENTRY_BLOCK_MINUTES ??
        process.env.SCALP_REPLAY_WEEKEND_ENTRY_BLOCK_MINUTES,
      60,
    ),
    forceCloseMinutes: toNonNegativeInt(
      process.env.SCALP_REPLAY_SESSION_FORCE_CLOSE_MINUTES ??
        process.env.SCALP_REPLAY_WEEKEND_FORCE_CLOSE_MINUTES,
      15,
    ),
  };
}

export function getScalpReplayWeekendConfig(): ScalpReplayMarketHoursConfig {
  return getScalpReplayMarketHoursConfig();
}

export function evaluateScalpReplayMarketGate(params: {
  symbol: string;
  nowMs?: number;
  config?: ScalpReplayMarketHoursConfig;
  metadata?: ScalpSymbolMarketMetadata | null;
}): ScalpReplayMarketGate {
  const nowMs = Number.isFinite(Number(params.nowMs))
    ? Math.floor(Number(params.nowMs))
    : Date.now();
  const config = params.config ?? getScalpReplayMarketHoursConfig();
  const openingHours = params.metadata?.openingHours || null;
  if (openingHours && supportsUtcSchedule(openingHours)) {
    return evaluateUtcOpeningHoursGate({
      openingHours,
      nowMs,
      config,
    });
  }
  return evaluateFallbackWeekendGate(params.symbol, nowMs, config);
}

export function evaluateScalpReplayWeekendGate(
  symbol: string,
  nowMs = Date.now(),
  config = getScalpReplayMarketHoursConfig(),
  metadata: ScalpSymbolMarketMetadata | null = null,
): ScalpReplayWeekendGate {
  return evaluateScalpReplayMarketGate({
    symbol,
    nowMs,
    config,
    metadata,
  });
}
