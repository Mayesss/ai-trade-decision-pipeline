import type { ScalpAssetCategory } from "./symbolInfo";
import { inferScalpAssetCategory, pipSizeForScalpSymbol } from "./symbolInfo";

export type ScalpMarketDayKey =
  | "mon"
  | "tue"
  | "wed"
  | "thu"
  | "fri"
  | "sat"
  | "sun";

export interface ScalpOpeningHoursWindow {
  day: ScalpMarketDayKey;
  openTime: string;
  closeTime: string;
}

export interface ScalpOpeningHoursSchedule {
  zone: string;
  windows: ScalpOpeningHoursWindow[];
  alwaysOpen: boolean;
}

export interface ScalpSymbolMarketMetadata {
  version: 1;
  symbol: string;
  epic: string | null;
  source: "bitget" | "heuristic";
  assetCategory: ScalpAssetCategory;
  instrumentType: string | null;
  marketStatus: string | null;
  pipSize: number;
  pipPosition: number | null;
  tickSize: number | null;
  decimalPlacesFactor: number | null;
  scalingFactor: number | null;
  minDealSize: number | null;
  sizeDecimals: number | null;
  maxLeverage?: number | null;
  openingHours: ScalpOpeningHoursSchedule | null;
  fetchedAtMs: number;
}

const DAY_KEYS: ScalpMarketDayKey[] = [
  "mon",
  "tue",
  "wed",
  "thu",
  "fri",
  "sat",
  "sun",
];

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function normalizeText(value: unknown): string | null {
  const normalized = String(value || "").trim();
  return normalized ? normalized : null;
}

function toPositiveNumber(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function toInteger(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function decimalPipSize(decimalPlacesFactor: number | null): number | null {
  if (decimalPlacesFactor === null) return null;
  const places = Math.max(0, Math.floor(decimalPlacesFactor));
  const pip = 10 ** -places;
  return Number.isFinite(pip) && pip > 0 ? pip : null;
}

function normalizeBitgetAssetCategory(params: {
  source: ScalpSymbolMarketMetadata["source"];
  symbol: string;
  instrumentType: string | null;
  assetCategory: ScalpAssetCategory;
}): ScalpAssetCategory {
  if (params.source !== "bitget") return params.assetCategory;
  if (params.assetCategory === "crypto") return "crypto";
  const instrumentType = String(params.instrumentType || "").trim().toUpperCase();
  if (params.symbol.endsWith("USDT")) {
    if (
      params.assetCategory === "equity" ||
      params.assetCategory === "other" ||
      instrumentType.includes("PERP") ||
      instrumentType.includes("FUTURE")
    ) {
      return "crypto";
    }
  }
  return params.assetCategory;
}

function normalizeBitgetPipSize(params: {
  source: ScalpSymbolMarketMetadata["source"];
  assetCategory: ScalpAssetCategory;
  explicitPipSize: number | null;
  tickSize: number | null;
  decimalPlacesFactor: number | null;
  fallbackPipSize: number;
}): number {
  const fromDecimalPlaces = decimalPipSize(params.decimalPlacesFactor);
  if (params.source === "bitget" && params.assetCategory === "crypto") {
    return params.tickSize ?? fromDecimalPlaces ?? params.explicitPipSize ?? params.fallbackPipSize;
  }
  return params.explicitPipSize ?? params.fallbackPipSize;
}

function normalizeZone(value: unknown): string {
  const normalized = String(value || "")
    .trim()
    .toUpperCase();
  if (
    normalized === "UTC" ||
    normalized === "GMT" ||
    normalized === "ETC/UTC" ||
    normalized === "ETC/GMT"
  ) {
    return "UTC";
  }
  return "UTC";
}

function normalizeDayKey(value: unknown): ScalpMarketDayKey | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "mon" || normalized === "monday" || normalized === "1")
    return "mon";
  if (
    normalized === "tue" ||
    normalized === "tues" ||
    normalized === "tuesday" ||
    normalized === "2"
  )
    return "tue";
  if (normalized === "wed" || normalized === "wednesday" || normalized === "3")
    return "wed";
  if (
    normalized === "thu" ||
    normalized === "thur" ||
    normalized === "thurs" ||
    normalized === "thursday" ||
    normalized === "4"
  )
    return "thu";
  if (normalized === "fri" || normalized === "friday" || normalized === "5")
    return "fri";
  if (normalized === "sat" || normalized === "saturday" || normalized === "6")
    return "sat";
  if (
    normalized === "sun" ||
    normalized === "sunday" ||
    normalized === "0" ||
    normalized === "7"
  )
    return "sun";
  return null;
}

function nextDayKey(day: ScalpMarketDayKey): ScalpMarketDayKey {
  const index = DAY_KEYS.indexOf(day);
  return DAY_KEYS[(index + 1) % DAY_KEYS.length] || "mon";
}

function normalizeClockTime(value: unknown): string | null {
  const raw = String(value || "").trim();
  const match = raw.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return null;
  return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`;
}

function parseClockMinutes(value: string): number | null {
  const normalized = normalizeClockTime(value);
  if (!normalized) return null;
  const [hour, minute] = normalized.split(":").map((row) => Number(row));
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  return hour * 60 + minute;
}

function pushWindow(
  out: ScalpOpeningHoursWindow[],
  day: ScalpMarketDayKey,
  openTime: string,
  closeTime: string,
): void {
  if (openTime === closeTime) return;
  out.push({ day, openTime, closeTime });
}

function expandDailyWindow(
  day: ScalpMarketDayKey,
  openTime: string,
  closeTime: string,
): ScalpOpeningHoursWindow[] {
  const openMinutes = parseClockMinutes(openTime);
  const closeMinutes = parseClockMinutes(closeTime);
  if (openMinutes === null || closeMinutes === null) return [];
  if (openMinutes === closeMinutes) return [];
  if (openMinutes < closeMinutes) {
    return [{ day, openTime, closeTime }];
  }
  return [
    { day, openTime, closeTime: "23:59" },
    { day: nextDayKey(day), openTime: "00:00", closeTime },
  ];
}

function normalizeWindowEntry(
  day: ScalpMarketDayKey,
  value: unknown,
): ScalpOpeningHoursWindow[] {
  if (!value) return [];
  if (typeof value === "string") {
    const match = value
      .trim()
      .match(/^(\d{1,2}:\d{2}(?::\d{2})?)\s*-\s*(\d{1,2}:\d{2}(?::\d{2})?)$/);
    if (!match?.[1] || !match?.[2]) return [];
    const openTime = normalizeClockTime(match[1]);
    const closeTime = normalizeClockTime(match[2]);
    if (!openTime || !closeTime) return [];
    return expandDailyWindow(day, openTime, closeTime);
  }
  if (typeof value === "object") {
    const row = value as Record<string, unknown>;
    const openTime = normalizeClockTime(row.openTime ?? row.open ?? row.from);
    const closeTime = normalizeClockTime(row.closeTime ?? row.close ?? row.to);
    if (!openTime || !closeTime) return [];
    return expandDailyWindow(day, openTime, closeTime);
  }
  return [];
}

function sortWindows(
  windows: ScalpOpeningHoursWindow[],
): ScalpOpeningHoursWindow[] {
  const dayOrder = new Map(DAY_KEYS.map((day, index) => [day, index]));
  return windows.slice().sort((lhs, rhs) => {
    const lhsDay = dayOrder.get(lhs.day) ?? 0;
    const rhsDay = dayOrder.get(rhs.day) ?? 0;
    if (lhsDay !== rhsDay) return lhsDay - rhsDay;
    const lhsOpen = parseClockMinutes(lhs.openTime) ?? 0;
    const rhsOpen = parseClockMinutes(rhs.openTime) ?? 0;
    if (lhsOpen !== rhsOpen) return lhsOpen - rhsOpen;
    const lhsClose = parseClockMinutes(lhs.closeTime) ?? 0;
    const rhsClose = parseClockMinutes(rhs.closeTime) ?? 0;
    return lhsClose - rhsClose;
  });
}

function dedupeWindows(
  windows: ScalpOpeningHoursWindow[],
): ScalpOpeningHoursWindow[] {
  const seen = new Set<string>();
  const out: ScalpOpeningHoursWindow[] = [];
  for (const row of sortWindows(windows)) {
    const key = `${row.day}:${row.openTime}:${row.closeTime}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out;
}

function detectAlwaysOpen(windows: ScalpOpeningHoursWindow[]): boolean {
  if (windows.length < DAY_KEYS.length) return false;
  return DAY_KEYS.every((day) =>
    windows.some(
      (row) =>
        row.day === day &&
        row.openTime === "00:00" &&
        (row.closeTime === "23:59" || row.closeTime === "23:58"),
    ),
  );
}

export function buildScalpOpeningHoursSchedule(params: {
  zone?: unknown;
  days?: Partial<Record<ScalpMarketDayKey, unknown>>;
  windows?: unknown;
  alwaysOpen?: unknown;
}): ScalpOpeningHoursSchedule | null {
  const normalizedWindows: ScalpOpeningHoursWindow[] = [];

  if (Array.isArray(params.windows)) {
    for (const row of params.windows) {
      const value = row as Record<string, unknown>;
      const day = normalizeDayKey(value?.day);
      const openTime = normalizeClockTime(value?.openTime);
      const closeTime = normalizeClockTime(value?.closeTime);
      if (!day || !openTime || !closeTime) continue;
      normalizedWindows.push(...expandDailyWindow(day, openTime, closeTime));
    }
  } else {
    for (const day of DAY_KEYS) {
      const values = params.days?.[day];
      const rows = Array.isArray(values) ? values : values ? [values] : [];
      for (const row of rows) {
        normalizedWindows.push(...normalizeWindowEntry(day, row));
      }
    }
  }

  const windows = dedupeWindows(normalizedWindows);
  const alwaysOpen =
    params.alwaysOpen === true ||
    (windows.length > 0 && detectAlwaysOpen(windows));
  if (!windows.length && !alwaysOpen) return null;

  return {
    zone: normalizeZone(params.zone),
    windows,
    alwaysOpen,
  };
}

export function normalizeScalpOpeningHours(
  value: unknown,
): ScalpOpeningHoursSchedule | null {
  if (!value || typeof value !== "object") return null;
  const row = value as Record<string, unknown>;
  return buildScalpOpeningHoursSchedule({
    zone: row.zone,
    windows: row.windows,
    alwaysOpen: row.alwaysOpen,
  });
}

export function scalpAssetCategoryFromInstrumentType(
  symbol: string,
  instrumentTypeRaw: unknown,
): ScalpAssetCategory {
  const instrumentType = String(instrumentTypeRaw || "")
    .trim()
    .toUpperCase();
  if (instrumentType === "CURRENCIES") return "forex";
  if (instrumentType === "CRYPTOCURRENCIES") return "crypto";
  if (instrumentType === "COMMODITIES") return "commodity";
  if (instrumentType === "INDICES") return "index";
  if (instrumentType === "SHARES") return "equity";
  return inferScalpAssetCategory(symbol);
}

export function normalizeScalpSymbolMarketMetadata(
  value: Partial<ScalpSymbolMarketMetadata> & { symbol: string },
): ScalpSymbolMarketMetadata {
  const symbol = normalizeSymbol(value.symbol);
  const epic = normalizeText(value.epic)
    ? String(value.epic).trim().toUpperCase()
    : null;
  const source: ScalpSymbolMarketMetadata["source"] =
    value.source === "bitget" ? "bitget" : "heuristic";
  const instrumentType =
    normalizeText(value.instrumentType)?.toUpperCase() || null;
  const unresolvedAssetCategory =
    value.assetCategory ||
    scalpAssetCategoryFromInstrumentType(symbol, instrumentType);
  const assetCategory = normalizeBitgetAssetCategory({
    source,
    symbol,
    instrumentType,
    assetCategory: unresolvedAssetCategory,
  });
  const pipPosition = toInteger(value.pipPosition);
  const tickSize = toPositiveNumber(value.tickSize);
  const decimalPlacesFactor = toInteger(value.decimalPlacesFactor);
  const scalingFactor = toInteger(value.scalingFactor);
  const minDealSize = toPositiveNumber(value.minDealSize);
  const sizeDecimals = toInteger(value.sizeDecimals);
  const maxLeverageRaw = toPositiveNumber(value.maxLeverage);
  const maxLeverage =
    maxLeverageRaw !== null ? Math.max(1, Math.floor(maxLeverageRaw)) : null;
  const openingHours = normalizeScalpOpeningHours(value.openingHours);
  const pipSizeCandidate = normalizeBitgetPipSize({
    source,
    assetCategory,
    explicitPipSize: toPositiveNumber(value.pipSize),
    tickSize,
    decimalPlacesFactor,
    fallbackPipSize: pipSizeForScalpSymbol(symbol),
  });

  return {
    version: 1,
    symbol,
    epic,
    source,
    assetCategory,
    instrumentType,
    marketStatus: normalizeText(value.marketStatus)?.toUpperCase() || null,
    pipSize: pipSizeCandidate,
    pipPosition,
    tickSize,
    decimalPlacesFactor,
    scalingFactor,
    minDealSize,
    sizeDecimals,
    maxLeverage,
    openingHours,
    fetchedAtMs: Math.max(
      0,
      Math.floor(Number(value.fetchedAtMs) || Date.now()),
    ),
  };
}

export function buildHeuristicScalpSymbolMarketMetadata(
  symbolRaw: string,
  params: {
    epic?: string | null;
    source?: "bitget" | "heuristic";
    fetchedAtMs?: number;
  } = {},
): ScalpSymbolMarketMetadata {
  const symbol = normalizeSymbol(symbolRaw);
  return normalizeScalpSymbolMarketMetadata({
    symbol,
    epic: params.epic ?? null,
    source: params.source ?? "heuristic",
    assetCategory: inferScalpAssetCategory(symbol),
    pipSize: pipSizeForScalpSymbol(symbol),
    fetchedAtMs: params.fetchedAtMs ?? Date.now(),
    openingHours:
      inferScalpAssetCategory(symbol) === "crypto"
        ? {
            zone: "UTC",
            alwaysOpen: true,
            windows: DAY_KEYS.map((day) => ({
              day,
              openTime: "00:00",
              closeTime: "23:59",
            })),
          }
        : null,
  });
}
