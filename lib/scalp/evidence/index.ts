import crypto from "crypto";

import {
  buildForexEventContext,
  ensureForexEventsState,
  type ForexCompactEvent,
} from "../../swing/forexEvents";
import type { ScalpReplayTrade } from "../replay/types";
import type { ScalpEntrySessionProfile } from "../types";

import type { ScalpComposerDeployment, ScalpComposerSession, ScalpComposerVenue } from "../composer/types";

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

export type ScalpComposerV3Bootstrap = {
  version: "scalp_v2_v3_bootstrap_r1";
  resamples: number;
  expectancyPositivePct: number;
  lowerP05R: number;
  seed: string;
};

export type ScalpComposerV3Holdout = {
  version: "scalp_v2_v3_holdout_r1";
  weeks: number;
  fromTs: number;
  toTs: number;
  trades: number;
  netR: number;
  expectancyR: number;
  maxDrawdownR: number;
  profitFactor: number | null;
  trainingNetR: number;
  trainingExpectancyR: number;
  holdoutToTrainingExpectancyRatio: number | null;
  passed: boolean;
  reason: string | null;
};

export type ScalpComposerV3Drift = {
  version: "scalp_v2_v3_drift_r1";
  status: "healthy" | "low_sample" | "drifting";
  checkedAtMs: number;
  liveTrades: number;
  liveWeeks: number;
  liveNetR: number;
  liveExpectancyR: number;
  liveMaxDrawdownR: number;
  researchExpectancyR: number | null;
  researchMaxDrawdownR: number | null;
  expectancyRatio: number | null;
  reason: string | null;
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

export function computeScalpComposerV3PriorScore(params: {
  compositeScore: number;
  confidence: number;
  supportScore: number;
  blocksByFamily?: Record<string, string[]>;
  seed?: string;
}): ScalpComposerV3Ranking {
  const supportRegularizer = clamp(finite(params.supportScore) / 12, 0, 1);
  const families = Object.values(params.blocksByFamily || {}).filter(
    (rows) => Array.isArray(rows) && rows.length > 0,
  ).length;
  const uniqueBlocks = new Set(Object.values(params.blocksByFamily || {}).flat()).size;
  const diversityScore = clamp(families / 6, 0, 1) * 0.65 + clamp(uniqueBlocks / 10, 0, 1) * 0.35;
  const hashBump =
    crypto.createHash("sha1").update(String(params.seed || "")).digest().readUInt32BE(0) / 0xffffffff / 1000;
  const priorScore =
    clamp(finite(params.compositeScore), 0, 1) * 70 +
    clamp(finite(params.confidence), 0, 1) * 12 +
    diversityScore * 12 +
    supportRegularizer * 6 +
    hashBump;
  return {
    version: "scalp_v2_v3_r1",
    priorScore,
    supportRegularizer,
    diversityScore,
    edgeScore: null,
    minVariantTrades: resolveScalpComposerV3Config().minVariantTrades,
  };
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

export function computeScalpComposerV3Bootstrap(params: {
  trades: ScalpReplayTrade[];
  resamples?: number;
  seed?: string;
}): ScalpComposerV3Bootstrap | null {
  const r = (params.trades || []).map((row) => finite(row.rMultiple)).filter(Number.isFinite);
  const resamples = Math.max(0, Math.floor(params.resamples ?? resolveScalpComposerV3Config().bootstrapResamples));
  if (r.length < 2 || resamples <= 0) return null;
  let seed = crypto
    .createHash("sha1")
    .update(params.seed || JSON.stringify(r.slice(0, 64)))
    .digest()
    .readUInt32BE(0);
  const rand = () => {
    seed = (1664525 * seed + 1013904223) >>> 0;
    return seed / 0x100000000;
  };
  const rows: number[] = [];
  let positive = 0;
  for (let i = 0; i < resamples; i += 1) {
    let sum = 0;
    for (let j = 0; j < r.length; j += 1) {
      sum += r[Math.floor(rand() * r.length)] || 0;
    }
    const m = sum / r.length;
    if (m > 0) positive += 1;
    rows.push(m);
  }
  rows.sort((a, b) => a - b);
  return {
    version: "scalp_v2_v3_bootstrap_r1",
    resamples,
    expectancyPositivePct: (positive / resamples) * 100,
    lowerP05R: rows[Math.max(0, Math.min(rows.length - 1, Math.floor(rows.length * 0.05)))] || 0,
    seed: String(params.seed || ""),
  };
}

export function synthesizeScalpComposerV3HoldoutFromStages(params: {
  stageB: {
    netR: number;
    trades: number;
    expectancyR?: number;
    maxDrawdownR?: number;
    profitFactor?: number | null;
    fromTs?: number;
    toTs?: number;
    weeks?: number;
  } | null;
  stageC: { netR: number; trades: number } | null;
  minHoldoutTrades?: number;
}): (ScalpComposerV3Holdout & { source: "v2_backfill"; trainingTrades: number }) | null {
  const stageB = params.stageB;
  const stageC = params.stageC;
  if (!stageB || !stageC) return null;
  const holdoutTrades = Math.floor(stageB.trades || 0);
  const totalTrades = Math.floor(stageC.trades || 0);
  const trainingTrades = totalTrades - holdoutTrades;
  if (holdoutTrades <= 0 || trainingTrades <= 0) return null;
  const holdoutNetR = finite(stageB.netR);
  const trainingNetR = finite(stageC.netR) - holdoutNetR;
  const holdoutExpectancyR = holdoutNetR / holdoutTrades;
  const trainingExpectancyR = trainingNetR / trainingTrades;
  const ratio =
    Math.abs(trainingExpectancyR) > 1e-9
      ? holdoutExpectancyR / trainingExpectancyR
      : null;
  const minTrades = Math.max(1, Math.floor(params.minHoldoutTrades || 6));
  let reason: string | null = null;
  if (holdoutTrades < minTrades) reason = "holdout_min_trades_not_met";
  else if (ratio === null || ratio < 0.5)
    reason = "holdout_expectancy_ratio_below_threshold";
  else if (holdoutNetR < -0.25 * Math.abs(trainingNetR))
    reason = "holdout_net_r_materially_negative";
  return {
    version: "scalp_v2_v3_holdout_r1",
    weeks: Math.max(1, Math.floor(stageB.weeks || 6)),
    fromTs: Math.floor(stageB.fromTs || 0),
    toTs: Math.floor(stageB.toTs || 0),
    trades: holdoutTrades,
    netR: holdoutNetR,
    expectancyR: holdoutExpectancyR,
    maxDrawdownR: finite(stageB.maxDrawdownR),
    profitFactor:
      stageB.profitFactor != null && Number.isFinite(Number(stageB.profitFactor))
        ? finite(stageB.profitFactor)
        : null,
    trainingNetR,
    trainingExpectancyR,
    holdoutToTrainingExpectancyRatio: ratio,
    passed: reason === null,
    reason,
    source: "v2_backfill",
    trainingTrades,
  };
}

export function computeScalpComposerV3Holdout(params: {
  trades: ScalpReplayTrade[];
  windowToTs: number;
  holdoutWeeks?: number;
  trainingNetR: number;
  trainingExpectancyR: number;
  minTrades: number;
}): ScalpComposerV3Holdout {
  const weeks = Math.max(1, Math.floor(params.holdoutWeeks || resolveScalpComposerV3Config().holdoutWeeks));
  const toTs = Math.floor(params.windowToTs);
  const fromTs = toTs - weeks * ONE_WEEK_MS;
  const scoped = (params.trades || []).filter((row) => {
    const ts = Math.floor(Number(row.exitTs) || 0);
    return ts >= fromTs && ts < toTs;
  });
  const r = scoped.map((row) => finite(row.rMultiple)).filter(Number.isFinite);
  const trades = r.length;
  const netR = r.reduce((acc, row) => acc + row, 0);
  const expectancyR = trades ? netR / trades : 0;
  const maxDrawdownR = maxDrawdown(r);
  const trainingExpectancyR = finite(params.trainingExpectancyR);
  const ratio =
    Math.abs(trainingExpectancyR) > 1e-9 ? expectancyR / trainingExpectancyR : null;
  let reason: string | null = null;
  if (trades < Math.max(1, Math.floor(params.minTrades || 1))) reason = "holdout_min_trades_not_met";
  else if (ratio === null || ratio < 0.5) reason = "holdout_expectancy_ratio_below_threshold";
  else if (netR < -0.25 * Math.abs(finite(params.trainingNetR))) reason = "holdout_net_r_materially_negative";
  return {
    version: "scalp_v2_v3_holdout_r1",
    weeks,
    fromTs,
    toTs,
    trades,
    netR,
    expectancyR,
    maxDrawdownR,
    profitFactor: profitFactor(r),
    trainingNetR: finite(params.trainingNetR),
    trainingExpectancyR,
    holdoutToTrainingExpectancyRatio: ratio,
    passed: reason === null,
    reason,
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

export function buildScalpComposerV3TemporalVariants(params: {
  baseTuneId: string;
  session: ScalpComposerSession;
  venue: ScalpComposerVenue;
  symbol: string;
  maxVariants: number;
  includeUtcHours?: boolean;
  includeSlotWeekdayCombos?: boolean;
  variantOffset?: number;
}): Array<{ tuneDigestSeed: string; filter: ScalpComposerV3TemporalFilter }> {
  const cfg = resolveScalpComposerV3Config();
  const slots = Math.max(1, Math.floor((4 * 60) / cfg.sessionSlotMinutes));
  const all: Array<{ tuneDigestSeed: string; filter: ScalpComposerV3TemporalFilter }> = [];
  const push = (kind: ScalpComposerV3TemporalFilter["variantKind"], suffix: string, filter: ScalpComposerV3TemporalFilter) => {
    all.push({
      tuneDigestSeed: `${params.baseTuneId}:${suffix}`,
      filter: {
        sessionSlotMinutes: cfg.sessionSlotMinutes,
        variantId: suffix,
        variantKind: kind,
        ...filter,
      },
    });
  };
  for (let slot = 0; slot < slots; slot += 1) {
    push("session_slot", `v3sl${slot}`, { allowedSessionWindowSlots: [slot] });
  }
  const weekdays = params.venue === "bitget" ? [1, 2, 3, 4, 5, 6] : [1, 2, 3, 4, 5];
  for (const weekday of weekdays) {
    push("weekday", `v3wd${weekday}`, { allowedWeekdaysLocal: [weekday] });
  }
  if (params.includeUtcHours !== false) {
    const utcHours = params.venue === "capital" ? [7, 8, 12, 13, 14, 15, 19] : [0, 7, 8, 13, 14, 15, 20, 21];
    for (const hour of utcHours) {
      push("utc_hour", `v3uh${hour}`, { allowedUtcHours: [hour] });
    }
  }
  if (params.includeSlotWeekdayCombos) {
    const topSlots = Array.from({ length: Math.min(3, slots) }, (_, idx) => idx);
    const topWeekdays = params.venue === "bitget" ? [1, 2] : [1, 3];
    for (const slot of topSlots) {
      for (const weekday of topWeekdays) {
        push("slot_weekday", `v3sw${slot}d${weekday}`, {
          allowedSessionWindowSlots: [slot],
          allowedWeekdaysLocal: [weekday],
        });
      }
    }
  }
  const maxVariants = Math.max(0, Math.floor(params.maxVariants));
  if (maxVariants <= 0 || all.length <= maxVariants) return all.slice(0, maxVariants);
  const offset = Math.max(0, Math.floor(params.variantOffset || 0)) % all.length;
  return Array.from({ length: maxVariants }, (_, idx) => all[(offset + idx) % all.length]!);
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

export function computeScalpComposerV3Drift(params: {
  deployment: ScalpComposerDeployment;
  ledgerRows: Array<{ tsExitMs: number; rMultiple: number }>;
  nowMs: number;
}): ScalpComposerV3Drift {
  const cfg = resolveScalpComposerV3Config();
  const since = params.nowMs - 30 * ONE_DAY_MS;
  const scoped = params.ledgerRows
    .filter((row) => finite(row.tsExitMs) >= since && finite(row.tsExitMs) <= params.nowMs)
    .sort((a, b) => finite(a.tsExitMs) - finite(b.tsExitMs));
  const r = scoped.map((row) => finite(row.rMultiple)).filter(Number.isFinite);
  const weeks = new Set(scoped.map((row) => startOfUtcWeekMonday(finite(row.tsExitMs)))).size;
  const liveTrades = r.length;
  const liveNetR = r.reduce((acc, row) => acc + row, 0);
  const liveExpectancyR = liveTrades ? liveNetR / liveTrades : 0;
  const liveMaxDrawdownR = maxDrawdown(r);
  const gate = params.deployment.promotionGate || {};
  const worker = (gate.worker && typeof gate.worker === "object" ? gate.worker : {}) as Record<string, any>;
  const stageC = (worker.stageC && typeof worker.stageC === "object" ? worker.stageC : {}) as Record<string, unknown>;
  const researchExpectancyR = Number.isFinite(Number(stageC.expectancyR)) ? Number(stageC.expectancyR) : null;
  const researchMaxDrawdownR = Number.isFinite(Number(stageC.maxDrawdownR)) ? Number(stageC.maxDrawdownR) : null;
  const expectancyRatio =
    researchExpectancyR !== null && Math.abs(researchExpectancyR) > 1e-9
      ? liveExpectancyR / researchExpectancyR
      : null;
  let status: ScalpComposerV3Drift["status"] = "healthy";
  let reason: string | null = null;
  if (liveTrades < cfg.driftMinTrades || weeks < cfg.driftMinWeeks) {
    status = "low_sample";
    reason = "drift_low_sample";
  } else if (expectancyRatio !== null && expectancyRatio < 0.5) {
    status = "drifting";
    reason = "drift_expectancy_ratio_below_threshold";
  } else if (
    researchMaxDrawdownR !== null &&
    researchMaxDrawdownR > 0 &&
    liveMaxDrawdownR > researchMaxDrawdownR * 1.5
  ) {
    status = "drifting";
    reason = "drift_drawdown_above_research_band";
  }
  return {
    version: "scalp_v2_v3_drift_r1",
    status,
    checkedAtMs: params.nowMs,
    liveTrades,
    liveWeeks: weeks,
    liveNetR,
    liveExpectancyR,
    liveMaxDrawdownR,
    researchExpectancyR,
    researchMaxDrawdownR,
    expectancyRatio,
    reason,
  };
}
