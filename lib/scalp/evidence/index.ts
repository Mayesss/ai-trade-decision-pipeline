import {
  buildForexEventContext,
  ensureForexEventsState,
  type ForexCompactEvent,
} from "../../swing/forexEvents";
import type { ScalpReplayTrade } from "../replay/types";
import type { ScalpEntrySessionProfile } from "../types";

import type { ScalpComposerSession, ScalpComposerVenue } from "../composer/types";

const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;

const SESSION_TIME_ZONE: Record<ScalpComposerSession, string> = {
  tokyo: "Asia/Tokyo",
  berlin: "Europe/Berlin",
  newyork: "America/New_York",
  pacific: "America/Los_Angeles",
  sydney: "Australia/Sydney",
};

const SESSION_START_MINUTE: Record<ScalpComposerSession, number> = {
  tokyo: 9 * 60,
  berlin: 8 * 60,
  newyork: 8 * 60,
  pacific: 10 * 60,
  sydney: 8 * 60,
};

export type ScalpComposerV3TemporalFilter = {
  variantId?: string;
  variantKind?: "baseline" | "session_slot" | "weekday" | "utc_hour" | "slot_weekday";
  sessionSlotMinutes?: number;
  allowedSessionWindowSlots?: number[];
  allowedWeekdaysLocal?: number[];
  allowedUtcHours?: number[];
};

export type ScalpComposerV3Ranking = {
  version: "scalp_v2_v3_r1";
  priorScore: number;
  supportRegularizer: number;
  diversityScore: number;
  edgeScore?: number | null;
  minVariantTrades: number;
  variantTradeFloorPassed?: boolean;
  stats?: Record<string, number | null>;
};

export type ScalpComposerV3NewsBlackout = {
  blocked: boolean;
  reasonCodes: string[];
  tier: "tier1" | "tier2" | null;
  staleData: boolean;
  activeEvents: ForexCompactEvent[];
};

export type ScalpComposerV3EntryWindowSpec = {
  session: ScalpComposerSession;
  filter?: ScalpComposerV3TemporalFilter | null;
};

function envBool(name: string, fallback: boolean): boolean {
  const raw = String(process.env[name] || "").trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function envInt(name: string, fallback: number, min: number, max: number): number {
  const n = Math.floor(Number(process.env[name]));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function envNumber(name: string, fallback: number, min: number, max: number): number {
  const n = Number(process.env[name]);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

export function isScalpComposerV3ResearchEnabled(): boolean {
  return String(process.env.SCALP_COMPOSER_RESEARCH_VERSION || "v3").trim().toLowerCase() === "v3";
}

export function resolveScalpComposerV3Config() {
  return {
    enabled: isScalpComposerV3ResearchEnabled(),
    sessionSlotMinutes: envInt("SCALP_EVIDENCE_SESSION_SLOT_MINUTES", 30, 5, 240),
    temporalVariantQuotaPct: envNumber("SCALP_EVIDENCE_TEMPORAL_VARIANT_QUOTA_PCT", 0.35, 0, 1),
    minVariantTrades: envInt("SCALP_EVIDENCE_MIN_VARIANT_TRADES", 8, 1, 10_000),
    holdoutWeeks: envInt("SCALP_EVIDENCE_HOLDOUT_WEEKS", 6, 1, 26),
    hardGateMinCandidates: envInt("SCALP_EVIDENCE_HARD_GATE_MIN_CANDIDATES", 50, 1, 100_000),
    bootstrapResamples: envInt("SCALP_EVIDENCE_BOOTSTRAP_RESAMPLES", 2_000, 0, 10_000),
    newsBlackoutEnabled: envBool("SCALP_EVIDENCE_NEWS_BLACKOUT_ENABLED", true),
    driftMinTrades: envInt("SCALP_EVIDENCE_DRIFT_MIN_TRADES", 20, 1, 10_000),
    driftMinWeeks: envInt("SCALP_EVIDENCE_DRIFT_MIN_WEEKS", 2, 1, 52),
    driftAutoPause: envBool("SCALP_EVIDENCE_DRIFT_AUTO_PAUSE", false),
    promotionFreezeBypass: envBool("SCALP_EVIDENCE_PROMOTION_FREEZE_BYPASS", false),
  };
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function finite(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function mean(values: number[]): number {
  return values.length ? values.reduce((acc, row) => acc + row, 0) / values.length : 0;
}

function sampleStd(values: number[]): number {
  if (values.length < 2) return 0;
  const m = mean(values);
  const variance =
    values.reduce((acc, row) => acc + (row - m) * (row - m), 0) / (values.length - 1);
  return variance > 0 ? Math.sqrt(variance) : 0;
}

function maxDrawdown(values: number[]): number {
  let equity = 0;
  let peak = 0;
  let dd = 0;
  for (const value of values) {
    equity += value;
    peak = Math.max(peak, equity);
    dd = Math.max(dd, peak - equity);
  }
  return dd;
}

function profitFactor(values: number[]): number | null {
  const gp = values.reduce((acc, row) => acc + Math.max(0, row), 0);
  const gl = values.reduce((acc, row) => acc + Math.max(0, -row), 0);
  if (gl <= 1e-9) return gp > 0 ? Number.POSITIVE_INFINITY : null;
  return gp / gl;
}

export function startOfUtcWeekMonday(tsMs: number): number {
  const d = new Date(tsMs);
  const dayStart = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
  const dayOfWeek = new Date(dayStart).getUTCDay();
  return dayStart - ((dayOfWeek + 6) % 7) * ONE_DAY_MS;
}

function topPositiveNetConcentrationPct(weeklyNetR: number[]): number {
  const positive = weeklyNetR.filter((row) => row > 0);
  const total = positive.reduce((acc, row) => acc + row, 0);
  if (total <= 0) return 100;
  return (Math.max(...positive) / total) * 100;
}

export function computeScalpComposerV3EdgeScore(params: {
  trades: ScalpReplayTrade[];
  weeklyNetR?: Record<string, number>;
  minVariantTrades?: number;
  isTemporalVariant?: boolean;
}): ScalpComposerV3Ranking {
  const r = (params.trades || []).map((row) => finite(row.rMultiple)).filter(Number.isFinite);
  const trades = r.length;
  const netR = r.reduce((acc, row) => acc + row, 0);
  const meanR = trades > 0 ? netR / trades : 0;
  const stdR = sampleStd(r);
  const stderrR = trades > 0 ? stdR / Math.sqrt(trades) : 0;
  const lowerBoundR = meanR - 1.64 * stderrR;
  const maxDrawdownR = maxDrawdown(r);
  const calmarR = clamp(netR / Math.max(0.3, maxDrawdownR), -5, 5);
  const weeklyRows = Object.values(params.weeklyNetR || {}).map((row) => finite(row));
  const topWeekPnlConcentrationPct = topPositiveNetConcentrationPct(weeklyRows);
  const worstWeeklyNetR = weeklyRows.length ? Math.min(...weeklyRows) : 0;
  const topWeekPenalty = Math.min(1, Math.max(0, (topWeekPnlConcentrationPct - 45) / 55));
  const worstWeekPenalty = Math.min(1, Math.max(0, -worstWeeklyNetR / Math.max(1, Math.abs(netR))));
  const minVariantTrades = Math.max(1, Math.floor(params.minVariantTrades || resolveScalpComposerV3Config().minVariantTrades));
  const variantTradeFloorPassed = !params.isTemporalVariant || trades >= minVariantTrades;
  const edgeScore = variantTradeFloorPassed
    ? lowerBoundR * Math.sqrt(Math.max(1, trades)) + 0.25 * calmarR - topWeekPenalty - worstWeekPenalty
    : Number.NEGATIVE_INFINITY;

  return {
    version: "scalp_v2_v3_r1",
    priorScore: 0,
    supportRegularizer: 0,
    diversityScore: 0,
    edgeScore,
    minVariantTrades,
    variantTradeFloorPassed,
    stats: {
      trades,
      netR,
      meanR,
      stdR,
      stderrR,
      lowerBoundR,
      maxDrawdownR,
      calmarR,
      topWeekPnlConcentrationPct,
      worstWeeklyNetR,
      topWeekPenalty,
      worstWeekPenalty,
      profitFactor: profitFactor(r),
    },
  };
}

function localParts(tsMs: number, timeZone: string): { weekday: number; minuteOfDay: number } {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    hourCycle: "h23",
  }).formatToParts(new Date(tsMs));
  const rawWeekday = parts.find((p) => p.type === "weekday")?.value || "Mon";
  const wdMap: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  const hh = Number(parts.find((p) => p.type === "hour")?.value || 0);
  const mm = Number(parts.find((p) => p.type === "minute")?.value || 0);
  return { weekday: wdMap[rawWeekday] ?? 1, minuteOfDay: (hh === 24 ? 0 : hh) * 60 + mm };
}

export function evaluateScalpComposerV3TemporalFilter(params: {
  nowMs: number;
  session: ScalpEntrySessionProfile;
  filter?: ScalpComposerV3TemporalFilter | null;
}): { allowed: boolean; reasonCodes: string[]; slotIndex: number | null; weekdayLocal: number; utcHour: number } {
  const filter = params.filter || {};
  const session = String(params.session || "berlin") as ScalpComposerSession;
  const timeZone = SESSION_TIME_ZONE[session] || "Europe/Berlin";
  const local = localParts(params.nowMs, timeZone);
  const slotMinutes = Math.max(5, Math.floor(filter.sessionSlotMinutes || resolveScalpComposerV3Config().sessionSlotMinutes));
  const sessionStart = SESSION_START_MINUTE[session] ?? 8 * 60;
  const minuteOffset = local.minuteOfDay - sessionStart;
  const slotIndex = minuteOffset >= 0 ? Math.floor(minuteOffset / slotMinutes) : null;
  const utcHour = new Date(params.nowMs).getUTCHours();
  const reasonCodes: string[] = [];
  if (
    Array.isArray(filter.allowedSessionWindowSlots) &&
    filter.allowedSessionWindowSlots.length > 0 &&
    (slotIndex === null || !filter.allowedSessionWindowSlots.includes(slotIndex))
  ) {
    reasonCodes.push("V3_TEMPORAL_SLOT_BLOCKED");
    if (slotIndex !== null) reasonCodes.push(`V3_TEMPORAL_SLOT_${slotIndex}_BLOCKED`);
  }
  if (
    Array.isArray(filter.allowedWeekdaysLocal) &&
    filter.allowedWeekdaysLocal.length > 0 &&
    !filter.allowedWeekdaysLocal.includes(local.weekday)
  ) {
    reasonCodes.push("V3_TEMPORAL_WEEKDAY_BLOCKED", `V3_TEMPORAL_WEEKDAY_${local.weekday}_BLOCKED`);
  }
  if (
    Array.isArray(filter.allowedUtcHours) &&
    filter.allowedUtcHours.length > 0 &&
    !filter.allowedUtcHours.includes(utcHour)
  ) {
    reasonCodes.push("V3_TEMPORAL_UTC_HOUR_BLOCKED", `V3_TEMPORAL_UTC_HOUR_${utcHour}_BLOCKED`);
  }
  return {
    allowed: reasonCodes.length === 0,
    reasonCodes,
    slotIndex,
    weekdayLocal: local.weekday,
    utcHour,
  };
}

function isInsideSessionWindow(params: {
  nowMs: number;
  session: ScalpComposerSession;
}): boolean {
  const timeZone = SESSION_TIME_ZONE[params.session] || "Europe/Berlin";
  const local = localParts(params.nowMs, timeZone);
  const sessionStart = SESSION_START_MINUTE[params.session] ?? 8 * 60;
  const minuteOffset = local.minuteOfDay - sessionStart;
  return minuteOffset >= 0 && minuteOffset < 4 * 60;
}

function sessionSlotMinutes(filter?: ScalpComposerV3TemporalFilter | null): number {
  return Math.max(
    5,
    Math.floor(filter?.sessionSlotMinutes || resolveScalpComposerV3Config().sessionSlotMinutes),
  );
}

function buildEntryWindowCells(params: {
  spec: ScalpComposerV3EntryWindowSpec;
  nowMs: number;
  granularityMinutes: number;
}): Set<number> {
  const start = startOfUtcWeekMonday(params.nowMs);
  const stepMs = Math.max(5, Math.floor(params.granularityMinutes)) * 60_000;
  const cells = new Set<number>();
  for (let ts = start; ts < start + 14 * ONE_DAY_MS; ts += stepMs) {
    if (!isInsideSessionWindow({ nowMs: ts, session: params.spec.session })) continue;
    const temporal = evaluateScalpComposerV3TemporalFilter({
      nowMs: ts,
      session: params.spec.session,
      filter: params.spec.filter || null,
    });
    if (!temporal.allowed) continue;
    cells.add(Math.floor(ts / stepMs));
  }
  return cells;
}

export function scalpComposerV3EntryWindowsOverlap(params: {
  a: ScalpComposerV3EntryWindowSpec;
  b: ScalpComposerV3EntryWindowSpec;
  nowMs?: number;
}): boolean {
  const granularityMinutes = Math.min(
    15,
    sessionSlotMinutes(params.a.filter),
    sessionSlotMinutes(params.b.filter),
  );
  const nowMs = Math.floor(Number(params.nowMs) || Date.now());
  const aCells = buildEntryWindowCells({
    spec: params.a,
    nowMs,
    granularityMinutes,
  });
  if (aCells.size <= 0) return false;
  const bCells = buildEntryWindowCells({
    spec: params.b,
    nowMs,
    granularityMinutes,
  });
  for (const cell of bCells) {
    if (aCells.has(cell)) return true;
  }
  return false;
}

function isTier1EventName(name: string): boolean {
  const normalized = String(name || "").toLowerCase();
  return [
    "cpi",
    "non-farm",
    "nonfarm",
    "nfp",
    "fomc",
    "federal funds",
    "interest rate",
    "rate decision",
    "central bank",
    "ecb",
    "boe",
    "boj",
    "snb",
    "boc",
    "rba",
    "rbnz",
  ].some((token) => normalized.includes(token));
}

function recurringTier1FallbackActive(nowMs: number): boolean {
  const d = new Date(nowMs);
  const day = d.getUTCDate();
  const dow = d.getUTCDay();
  const hh = d.getUTCHours();
  const mm = d.getUTCMinutes();
  const minuteOfDay = hh * 60 + mm;
  const firstFridayNfp = dow === 5 && day <= 7;
  if (firstFridayNfp) {
    const eventMinute = 13 * 60 + 30;
    return minuteOfDay >= eventMinute - 30 && minuteOfDay <= eventMinute + 60;
  }
  return false;
}

export function resolveScalpComposerV3StaleNewsBlackout(nowMs: number): ScalpComposerV3NewsBlackout {
  if (recurringTier1FallbackActive(nowMs)) {
    return {
      blocked: true,
      reasonCodes: ["V3_NEWS_BLACKOUT_TIER1_STALE_FAIL_CLOSED"],
      tier: "tier1",
      staleData: true,
      activeEvents: [],
    };
  }
  return {
    blocked: false,
    reasonCodes: ["V3_NEWS_BLACKOUT_UNAVAILABLE"],
    tier: null,
    staleData: true,
    activeEvents: [],
  };
}

export async function evaluateScalpComposerV3NewsBlackout(params: {
  venue: ScalpComposerVenue;
  symbol: string;
  nowMs?: number;
}): Promise<ScalpComposerV3NewsBlackout> {
  const cfg = resolveScalpComposerV3Config();
  const nowMs = Math.floor(Number(params.nowMs) || Date.now());
  if (!cfg.newsBlackoutEnabled || params.venue !== "capital") {
    return { blocked: false, reasonCodes: [], tier: null, staleData: false, activeEvents: [] };
  }
  const state = await ensureForexEventsState(nowMs).catch(() => null);
  if (!state || state.stale) {
    const fallback = resolveScalpComposerV3StaleNewsBlackout(nowMs);
    return state?.stale && !fallback.blocked
      ? { ...fallback, reasonCodes: ["V3_NEWS_BLACKOUT_DATA_STALE"] }
      : fallback;
  }

  const context = buildForexEventContext({ symbol: params.symbol, state, nowMs });
  const tier1 = context.activeEvents.filter((event) => isTier1EventName(event.event_name));
  if (tier1.length > 0) {
    return {
      blocked: true,
      reasonCodes: ["V3_NEWS_BLACKOUT_ACTIVE", "V3_NEWS_BLACKOUT_TIER1_ACTIVE"],
      tier: "tier1",
      staleData: false,
      activeEvents: tier1,
    };
  }
  if (context.status === "active" && context.activeEvents.length > 0) {
    return {
      blocked: true,
      reasonCodes: ["V3_NEWS_BLACKOUT_ACTIVE", "V3_NEWS_BLACKOUT_TIER2_ACTIVE"],
      tier: "tier2",
      staleData: false,
      activeEvents: context.activeEvents,
    };
  }
  return { blocked: false, reasonCodes: context.reasonCodes, tier: null, staleData: false, activeEvents: [] };
}
