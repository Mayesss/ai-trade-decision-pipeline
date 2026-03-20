import { Prisma } from "@prisma/client";

import {
  fetchCapitalCandlesByEpicDateRange,
  resolveCapitalEpicRuntime,
} from "../capital";
import { bitgetFetch, resolveProductType } from "../bitget";
import {
  loadScalpCandleHistory,
  mergeScalpCandleHistory,
  saveScalpCandleHistory,
  timeframeToMs,
} from "./candleHistory";
import {
  listScalpDeploymentRegistryEntries,
  upsertScalpDeploymentRegistryEntriesBulk,
  type ScalpDeploymentLifecycleState,
  type ScalpDeploymentPromotionLifecycle,
  type ScalpDeploymentPromotionHysteresis,
  type ScalpDeploymentPromotionGate,
  type ScalpDeploymentRegistryEntry,
  type ScalpForwardValidationMetrics,
} from "./deploymentRegistry";
import {
  buildBestEligibleTuneDeploymentIdSet,
  buildGlobalSymbolRankedDeploymentIdSet,
  buildForwardValidationByCandidateFromTasks,
  evaluateFreshCompletedDeploymentWeeks,
  evaluateWeeklyRobustnessGate,
  type ScalpPromotionForwardValidationCandidate,
  type ScalpWeeklyRobustnessMetrics,
  type SyncResearchWeeklyPolicy,
} from "./promotionPolicy";
import { buildScalpResearchTuneVariants } from "./researchTuner";
import { runScalpReplay } from "./replay/harness";
import { buildScalpReplayRuntimeFromDeployment } from "./replay/runtimeConfig";
import { isScalpPgConfigured, scalpPrisma } from "./pg/client";
import {
  resolveScalpDeployment,
  resolveScalpDeploymentVenueFromId,
} from "./deployments";
import { listScalpStrategies } from "./strategies/registry";
import {
  loadScalpSymbolDiscoveryPolicy,
  resolveCompletedWeekCoverageStartMs,
  resolveRecommendedStrategiesForSymbol,
  runScalpSymbolDiscoveryCycle,
} from "./symbolDiscovery";
import { pipSizeForScalpSymbol } from "./marketData";
import { ensureScalpSymbolMarketMetadata } from "./symbolMarketMetadataSync";
import { loadScalpSymbolMarketMetadata } from "./symbolMarketMetadataStore";
import type { ScalpCandle } from "./types";
import type { ScalpVenue } from "./venue";

const ONE_MINUTE_MS = 60_000;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;
const BITGET_HISTORY_CANDLES_MAX_LIMIT = 200;

export const SCALP_PIPELINE_JOB_KINDS = [
  "discover",
  "load_candles",
  "prepare",
  "worker",
  "promotion",
] as const;

export type ScalpPipelineJobKind = (typeof SCALP_PIPELINE_JOB_KINDS)[number];

export type ScalpPipelineQueueStatus =
  | "pending"
  | "running"
  | "succeeded"
  | "retry_wait"
  | "failed";

export interface ScalpPipelineJobDiagnostics {
  startedAtMs: number | null;
  finishedAtMs: number | null;
  durationMs: number | null;
}

export interface ScalpPipelineJobExecutionResult {
  ok: boolean;
  busy: boolean;
  jobKind: ScalpPipelineJobKind;
  processed: number;
  succeeded: number;
  retried: number;
  failed: number;
  pendingAfter: number;
  downstreamRequested: boolean;
  progressLabel: string | null;
  details: Record<string, unknown>;
  diagnostics?: ScalpPipelineJobDiagnostics;
  error?: string;
}

export interface ScalpPipelineJobHealth {
  jobKind: ScalpPipelineJobKind;
  status: string;
  locked: boolean;
  runningSinceAtMs: number | null;
  runningDurationMs: number | null;
  lastRunAtMs: number | null;
  lastDurationMs: number | null;
  lastSuccessAtMs: number | null;
  nextRunAtMs: number | null;
  lastError: string | null;
  progressLabel: string | null;
  progress: Record<string, unknown> | null;
  queue: {
    pending: number;
    running: number;
    retryWait: number;
    failed: number;
    succeeded: number;
  };
}

export interface ScalpDeploymentWeeklyMetricSnapshotRow {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  workerId: string | null;
  weekStartMs: number;
  weekEndMs: number;
  status: string;
  attempts: number;
  startedAtMs: number | null;
  finishedAtMs: number | null;
  durationMs: number | null;
  errorCode: string | null;
  errorMessage: string | null;
  trades: number | null;
  netR: number | null;
  expectancyR: number | null;
  profitFactor: number | null;
  maxDrawdownR: number | null;
}

export type ScalpDurationTimelineSource = "pipeline" | "worker";

export interface ScalpDurationTimelineRun {
  source: ScalpDurationTimelineSource;
  status: string;
  startedAtMs: number | null;
  finishedAtMs: number | null;
  durationMs: number | null;
  jobKind?: ScalpPipelineJobKind;
  processed?: number;
  succeeded?: number;
  retried?: number;
  failed?: number;
  pendingAfter?: number;
  downstreamRequested?: boolean;
  workerId?: string | null;
  taskCount?: number;
  succeededCount?: number;
  failedCount?: number;
}

function toFiniteNumber(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function toPositiveInt(
  value: unknown,
  fallback: number,
  max = Number.POSITIVE_INFINITY,
): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

function toBoundedPercent(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(100, n));
}

function envBool(name: string, fallback: boolean): boolean {
  const raw = String(process.env[name] || "")
    .trim()
    .toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function envNumber(name: string, fallback: number): number {
  const n = Number(process.env[name]);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

export function buildPipelineJobDiagnostics(
  startedAtMs: number | null,
  finishedAtMs: number | null,
): ScalpPipelineJobDiagnostics {
  const started =
    typeof startedAtMs === "number" && Number.isFinite(startedAtMs)
      ? Math.max(0, Math.floor(startedAtMs))
      : null;
  const finished =
    typeof finishedAtMs === "number" && Number.isFinite(finishedAtMs)
      ? Math.max(0, Math.floor(finishedAtMs))
      : null;
  const durationMs =
    started !== null && finished !== null && finished >= started
      ? finished - started
      : null;
  return {
    startedAtMs: started,
    finishedAtMs: finished,
    durationMs,
  };
}

function isScalpPipelineBitgetOnlyEnabled(): boolean {
  return envBool("SCALP_PIPELINE_BITGET_ONLY", true);
}

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function isBitgetPipelineSymbol(symbolRaw: string): boolean {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol) return false;
  const productType = String(resolveProductType() || "usdt-futures")
    .trim()
    .toLowerCase();
  if (productType === "usdc-futures") return symbol.endsWith("USDC");
  if (productType === "coin-futures") return symbol.endsWith("USD");
  return symbol.endsWith("USDT");
}

async function resolvePipelineDeploymentVenue(
  symbolRaw: string,
): Promise<ScalpVenue> {
  const symbol = normalizeSymbol(symbolRaw);
  if (!symbol) return "capital";
  if (isScalpPipelineBitgetOnlyEnabled() && isBitgetPipelineSymbol(symbol)) {
    return "bitget";
  }

  const history = await loadScalpCandleHistory(symbol, "1m");
  if (history.record?.source === "bitget" || history.record?.source === "capital") {
    return history.record.source;
  }

  const metadata = await loadScalpSymbolMarketMetadata(symbol);
  if (metadata?.source === "bitget" || metadata?.source === "capital") {
    return metadata.source;
  }

  return isBitgetPipelineSymbol(symbol) ? "bitget" : "capital";
}

function asJsonObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function normalizeForStableComparison(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((row) => normalizeForStableComparison(row));
  }
  const obj = asJsonObject(value);
  if (!obj) return value ?? null;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(obj).sort((a, b) => a.localeCompare(b))) {
    const raw = obj[key];
    if (raw === undefined) continue;
    out[key] = normalizeForStableComparison(raw);
  }
  return out;
}

function stableStringify(value: unknown): string {
  return JSON.stringify(normalizeForStableComparison(value));
}

function normalizePromotionGateForCompare(
  gate: ScalpDeploymentPromotionGate | null | undefined,
): Record<string, unknown> | null {
  const root = asJsonObject(gate);
  if (!root) return null;
  const normalized: Record<string, unknown> = {
    ...root,
    // Ignore wall-clock-only fields so we only persist semantic changes.
    evaluatedAtMs: 0,
  };
  const forwardValidation = asJsonObject(normalized.forwardValidation);
  if (forwardValidation) {
    normalized.forwardValidation = {
      ...forwardValidation,
      weeklyEvaluatedAtMs: 0,
      confirmationEvaluatedAtMs: 0,
    };
  }
  return normalized;
}

function hasPromotionStateChanged(params: {
  previous: Pick<ScalpDeploymentRegistryEntry, "enabled" | "promotionGate">;
  next: Pick<ScalpDeploymentRegistryEntry, "enabled" | "promotionGate">;
}): boolean {
  if (Boolean(params.previous.enabled) !== Boolean(params.next.enabled)) return true;
  const prevGate = normalizePromotionGateForCompare(params.previous.promotionGate);
  const nextGate = normalizePromotionGateForCompare(params.next.promotionGate);
  return stableStringify(prevGate) !== stableStringify(nextGate);
}

function asTsMs(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function asNullableFiniteNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return n;
}

const LIFECYCLE_SUSPEND_WINDOW_MS = 180 * ONE_DAY_MS;
const LIFECYCLE_SUSPEND_EXACT_MS = 12 * ONE_WEEK_MS;
const LIFECYCLE_SUSPEND_NEIGHBOR_MS = 8 * ONE_WEEK_MS;
const LIFECYCLE_RETIRE_MS = 180 * ONE_DAY_MS;

export function resolveLifecycleTuneFamily(tuneIdRaw: unknown): string {
  const tuneId = String(tuneIdRaw || "")
    .trim()
    .toLowerCase();
  if (!tuneId || tuneId === "default" || tuneId === "base") return "base";
  if (tuneId.startsWith("auto_mix")) return "auto_mix";
  if (tuneId.startsWith("auto_tr")) return "auto_tr";
  if (tuneId.startsWith("auto_ts")) return "auto_ts";
  if (tuneId.startsWith("auto_tp")) return "auto_tp";
  if (tuneId.startsWith("auto_sw")) return "auto_sw";
  if (tuneId.startsWith("auto_bh")) return "auto_bh";
  if (tuneId.startsWith("auto_sp")) return "auto_sp";
  const split = tuneId.split("_").filter(Boolean);
  return split[0] || "base";
}

function normalizeLifecycleState(
  value: unknown,
  fallback: ScalpDeploymentLifecycleState,
): ScalpDeploymentLifecycleState {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (
    normalized === "candidate" ||
    normalized === "incumbent_refresh" ||
    normalized === "graduated" ||
    normalized === "suspended" ||
    normalized === "retired"
  ) {
    return normalized;
  }
  return fallback;
}

function normalizeLifecycleFromGate(params: {
  gate: ScalpDeploymentPromotionGate | null | undefined;
  tuneId: string;
  enabled: boolean;
  nowMs: number;
}): ScalpDeploymentPromotionLifecycle {
  const fallbackState: ScalpDeploymentLifecycleState = params.enabled
    ? "graduated"
    : "candidate";
  const lifecycleRaw = asJsonObject(params.gate?.lifecycle);
  const state = normalizeLifecycleState(lifecycleRaw?.state, fallbackState);
  const suspendedUntilMs = asTsMs(lifecycleRaw?.suspendedUntilMs);
  const retiredUntilMs = asTsMs(lifecycleRaw?.retiredUntilMs);
  const suspensionEventsMs = Array.isArray(lifecycleRaw?.suspensionEventsMs)
    ? lifecycleRaw.suspensionEventsMs
        .map((row) => asTsMs(row))
        .filter((row): row is number => row !== null)
        .sort((a, b) => a - b)
    : [];
  const activeWindowStart = params.nowMs - LIFECYCLE_SUSPEND_WINDOW_MS;
  const rollingEvents = suspensionEventsMs.filter((row) => row >= activeWindowStart);
  const suspensionCount180d = Math.max(
    0,
    Math.floor(Number(lifecycleRaw?.suspensionCount180d) || rollingEvents.length),
  );
  const lifecycle: ScalpDeploymentPromotionLifecycle = {
    state,
    tuneFamily:
      String(lifecycleRaw?.tuneFamily || "").trim().toLowerCase() ||
      resolveLifecycleTuneFamily(params.tuneId),
    suspendedUntilMs,
    retiredUntilMs,
    suspensionEventsMs: rollingEvents,
    suspensionCount180d,
    lastRolloverBerlinWeekStartMs: asTsMs(lifecycleRaw?.lastRolloverBerlinWeekStartMs),
    lastSeatReleaseAtMs: asTsMs(lifecycleRaw?.lastSeatReleaseAtMs),
  };
  if (
    lifecycle.state === "suspended" &&
    lifecycle.suspendedUntilMs !== null &&
    lifecycle.suspendedUntilMs <= params.nowMs
  ) {
    lifecycle.state = params.enabled ? "graduated" : "candidate";
    lifecycle.suspendedUntilMs = null;
  }
  if (
    lifecycle.state === "retired" &&
    lifecycle.retiredUntilMs !== null &&
    lifecycle.retiredUntilMs <= params.nowMs
  ) {
    lifecycle.state = params.enabled ? "graduated" : "candidate";
    lifecycle.retiredUntilMs = null;
  }
  return lifecycle;
}

function lifecycleIsSuppressed(
  lifecycle: ScalpDeploymentPromotionLifecycle,
  nowMs: number,
): boolean {
  if (lifecycle.state === "retired") {
    return lifecycle.retiredUntilMs === null || lifecycle.retiredUntilMs > nowMs;
  }
  if (lifecycle.state === "suspended") {
    return lifecycle.suspendedUntilMs === null || lifecycle.suspendedUntilMs > nowMs;
  }
  return false;
}

function withBerlinEntrySessionProfile(
  configOverride: Record<string, unknown> | null | undefined,
): Record<string, unknown> {
  const root = asJsonObject(configOverride)
    ? (JSON.parse(JSON.stringify(configOverride)) as Record<string, unknown>)
    : {};
  const existingSessions = asJsonObject(root.sessions);
  const sessions = existingSessions ? { ...existingSessions } : {};
  sessions.entrySessionProfile = "berlin";
  root.sessions = sessions;
  return root;
}

function applyLifecycleSuspension(params: {
  lifecycle: ScalpDeploymentPromotionLifecycle;
  nowMs: number;
  durationMs: number;
}): ScalpDeploymentPromotionLifecycle {
  const nowMs = params.nowMs;
  const events = params.lifecycle.suspensionEventsMs
    .filter((row) => row >= nowMs - LIFECYCLE_SUSPEND_WINDOW_MS)
    .concat(nowMs)
    .sort((a, b) => a - b);
  const suspensionCount180d = events.length;
  if (suspensionCount180d >= 3) {
    return {
      ...params.lifecycle,
      state: "retired",
      suspendedUntilMs: null,
      retiredUntilMs: nowMs + LIFECYCLE_RETIRE_MS,
      suspensionEventsMs: events,
      suspensionCount180d,
    };
  }
  return {
    ...params.lifecycle,
    state: "suspended",
    suspendedUntilMs: nowMs + Math.max(ONE_DAY_MS, params.durationMs),
    retiredUntilMs: null,
    suspensionEventsMs: events,
    suspensionCount180d,
  };
}

function parseDayKey(dayKey: string): { y: number; m: number; d: number } {
  const match = String(dayKey || "")
    .trim()
    .match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) throw new Error(`Invalid day key: ${dayKey}`);
  return { y: Number(match[1]), m: Number(match[2]), d: Number(match[3]) };
}

function parseClock(clock: string): { hh: number; mm: number } {
  const match = String(clock || "")
    .trim()
    .match(/^([01]?\d|2[0-3]):([0-5]\d)$/);
  if (!match) throw new Error(`Invalid clock: ${clock}`);
  return { hh: Number(match[1]), mm: Number(match[2]) };
}

function partsForTimeZone(
  tsMs: number,
  timeZone: string,
): { y: number; m: number; d: number; weekday: number; hh: number; mm: number } {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    hourCycle: "h23",
  });
  const parts = fmt.formatToParts(new Date(tsMs));
  const read = (
    type: Intl.DateTimeFormatPartTypes,
    fallback: string,
  ): string => parts.find((row) => row.type === type)?.value || fallback;
  const y = Number(read("year", "1970"));
  const m = Number(read("month", "01"));
  const d = Number(read("day", "01"));
  const hhRaw = Number(read("hour", "00"));
  const hh = hhRaw === 24 ? 0 : hhRaw;
  const mm = Number(read("minute", "00"));
  const weekdayLabel = read("weekday", "Mon").toLowerCase();
  const weekdayMap: Record<string, number> = {
    mon: 1,
    tue: 2,
    wed: 3,
    thu: 4,
    fri: 5,
    sat: 6,
    sun: 7,
  };
  return {
    y: Number.isFinite(y) ? y : 1970,
    m: Number.isFinite(m) ? m : 1,
    d: Number.isFinite(d) ? d : 1,
    weekday: weekdayMap[weekdayLabel.slice(0, 3)] || 1,
    hh: Number.isFinite(hh) ? hh : 0,
    mm: Number.isFinite(mm) ? mm : 0,
  };
}

function utcMsFromZoned(dayKey: string, clock: string, timeZone: string): number {
  const date = parseDayKey(dayKey);
  const t = parseClock(clock);
  let guessMs = Date.UTC(date.y, date.m - 1, date.d, t.hh, t.mm, 0, 0);
  const targetDayInt = date.y * 10_000 + date.m * 100 + date.d;
  const targetMinuteOfDay = t.hh * 60 + t.mm;
  for (let i = 0; i < 6; i += 1) {
    const local = partsForTimeZone(guessMs, timeZone);
    const localDayInt = local.y * 10_000 + local.m * 100 + local.d;
    const dayDelta =
      localDayInt === targetDayInt ? 0 : localDayInt < targetDayInt ? 1 : -1;
    const localMinuteOfDay = local.hh * 60 + local.mm;
    const deltaMinutes = dayDelta * 1440 + (targetMinuteOfDay - localMinuteOfDay);
    if (deltaMinutes === 0) break;
    guessMs += deltaMinutes * ONE_MINUTE_MS;
  }
  return guessMs;
}

export function startOfBerlinWeekMonday(tsMs: number): number {
  const local = partsForTimeZone(tsMs, "Europe/Berlin");
  const dayKey = `${local.y.toString().padStart(4, "0")}-${local.m
    .toString()
    .padStart(2, "0")}-${local.d.toString().padStart(2, "0")}`;
  const localMidnightMs = utcMsFromZoned(dayKey, "00:00", "Europe/Berlin");
  const daysSinceMonday = (local.weekday + 6) % 7;
  return localMidnightMs - daysSinceMonday * ONE_DAY_MS;
}

function startOfUtcDay(tsMs: number): number {
  const d = new Date(tsMs);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

function startOfWeekMondayUtc(tsMs: number): number {
  const dayStartMs = startOfUtcDay(tsMs);
  const dayOfWeek = new Date(dayStartMs).getUTCDay();
  const daysSinceMonday = (dayOfWeek + 6) % 7;
  return dayStartMs - daysSinceMonday * ONE_DAY_MS;
}

function resolveLastCompletedWeekBoundsUtc(nowMs: number): {
  startCurrentWeekMondayMs: number;
  lastSundayEndMs: number;
} {
  const startCurrentWeekMondayMs = startOfWeekMondayUtc(nowMs);
  return {
    startCurrentWeekMondayMs,
    lastSundayEndMs: startCurrentWeekMondayMs - 1,
  };
}

function resolveRequiredSuccessiveWeeks(): number {
  return Math.max(
    13,
    Math.min(
      52,
      toPositiveInt(process.env.SCALP_PIPELINE_REQUIRED_SUCCESSIVE_WEEKS, 13),
    ),
  );
}

function resolvePromotionFreshWeeks(): number {
  return Math.max(
    12,
    Math.min(
      52,
      toPositiveInt(envNumber("SCALP_PROMOTION_FRESH_WEEKS", 12), 12),
    ),
  );
}

function findEarliestMissingCompletedWeekStartMs(
  candles: ScalpCandle[],
  nowMs: number,
  requiredWeeks: number,
): number | null {
  if (requiredWeeks <= 0) return null;
  const { startCurrentWeekMondayMs, lastSundayEndMs } =
    resolveLastCompletedWeekBoundsUtc(nowMs);
  const firstRequiredWeekStartMs =
    startCurrentWeekMondayMs - requiredWeeks * ONE_WEEK_MS;
  const presentWeekIndexes = new Set<number>();
  for (const candle of candles) {
    const ts = Number(candle?.[0] || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    if (ts < firstRequiredWeekStartMs || ts > lastSundayEndMs) continue;
    const index = Math.floor((ts - firstRequiredWeekStartMs) / ONE_WEEK_MS);
    if (index >= 0 && index < requiredWeeks) {
      presentWeekIndexes.add(index);
    }
  }
  for (let i = 0; i < requiredWeeks; i += 1) {
    if (!presentWeekIndexes.has(i)) {
      return firstRequiredWeekStartMs + i * ONE_WEEK_MS;
    }
  }
  return null;
}

function countCoveredCompletedWeeks(
  candles: ScalpCandle[],
  nowMs: number,
  requiredWeeks: number,
): { covered: number; latestWeekStartMs: number | null } {
  const { startCurrentWeekMondayMs, lastSundayEndMs } =
    resolveLastCompletedWeekBoundsUtc(nowMs);
  const firstRequiredWeekStartMs =
    startCurrentWeekMondayMs - requiredWeeks * ONE_WEEK_MS;
  const presentWeekIndexes = new Set<number>();
  let latestWeekStartMs: number | null = null;
  for (const candle of candles) {
    const ts = Number(candle?.[0] || 0);
    if (!Number.isFinite(ts) || ts <= 0) continue;
    if (ts < firstRequiredWeekStartMs || ts > lastSundayEndMs) continue;
    const weekStartMs = startOfWeekMondayUtc(ts);
    const index = Math.floor(
      (weekStartMs - firstRequiredWeekStartMs) / ONE_WEEK_MS,
    );
    if (index >= 0 && index < requiredWeeks) {
      presentWeekIndexes.add(index);
      latestWeekStartMs =
        latestWeekStartMs === null
          ? weekStartMs
          : Math.max(latestWeekStartMs, weekStartMs);
    }
  }
  return { covered: presentWeekIndexes.size, latestWeekStartMs };
}

function normalizeFetchedCandles(rows: unknown[]): ScalpCandle[] {
  return rows
    .map((row) => {
      const value = Array.isArray(row) ? row : [];
      const ts = Number(value[0]);
      const open = Number(value[1]);
      const high = Number(value[2]);
      const low = Number(value[3]);
      const close = Number(value[4]);
      const volume = Number(value[5] ?? 0);
      if (
        ![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)
      )
        return null;
      return [
        Math.floor(ts),
        open,
        high,
        low,
        close,
        Number.isFinite(volume) ? volume : 0,
      ] as ScalpCandle;
    })
    .filter((row): row is ScalpCandle => Boolean(row))
    .sort((a, b) => a[0] - b[0]);
}

function toReplayCandles(
  candles: ScalpCandle[],
  spreadPips: number,
): Array<{
  ts: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  spreadPips: number;
}> {
  return candles.map((row) => ({
    ts: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5] || 0),
    spreadPips: Number.isFinite(spreadPips) ? spreadPips : 0,
  }));
}

function normalizeBitgetHistoryGranularity(timeframe: string): string {
  const normalized = String(timeframe || "").trim();
  const lower = normalized.toLowerCase();
  if (lower === "1m") return "1m";
  if (lower === "3m") return "3m";
  if (lower === "5m") return "5m";
  if (lower === "15m") return "15m";
  if (lower === "30m") return "30m";
  if (lower === "1h") return "1H";
  if (lower === "2h") return "2H";
  if (lower === "4h") return "4H";
  if (lower === "6h") return "6H";
  if (lower === "12h") return "12H";
  if (lower === "1d") return "1D";
  if (lower === "1w" || lower === "4d") return "1W";
  if (lower === "1mo" || lower === "1mth" || lower === "1month") return "1M";
  return normalized || "1m";
}

async function fetchBitgetCandlesByEpicDateRange(
  epic: string,
  timeframe: string,
  fromTsMs: number,
  toTsMs: number,
  opts: {
    maxPerRequest?: number;
    maxRequests?: number;
  } = {},
): Promise<ScalpCandle[]> {
  const symbol = normalizeSymbol(epic);
  if (!symbol) return [];

  const startMs = Math.floor(Math.min(fromTsMs, toTsMs));
  const endMs = Math.floor(Math.max(fromTsMs, toTsMs));
  if (
    !(Number.isFinite(startMs) && Number.isFinite(endMs) && endMs > startMs)
  ) {
    return [];
  }

  const granularity = normalizeBitgetHistoryGranularity(timeframe);
  const timeframeMs = Math.max(ONE_MINUTE_MS, timeframeToMs("1m"));
  const requestLimit = Math.max(
    20,
    Math.min(
      BITGET_HISTORY_CANDLES_MAX_LIMIT,
      Math.floor(opts.maxPerRequest ?? BITGET_HISTORY_CANDLES_MAX_LIMIT),
    ),
  );
  const maxRequests = Math.max(40, Math.floor(opts.maxRequests ?? 800));
  const requestSpanBars = Math.max(220, requestLimit + 20);
  const requestSpanMs = requestSpanBars * timeframeMs;
  const productType = String(resolveProductType() || "usdt-futures")
    .trim()
    .toUpperCase();

  const candlesByTs = new Map<number, ScalpCandle>();
  let cursorEnd = endMs;
  let requests = 0;
  while (cursorEnd >= startMs) {
    if (requests >= maxRequests) {
      throw new Error(`bitget_history_max_requests_reached_for_${symbol}`);
    }
    const startTime = Math.max(
      startMs,
      cursorEnd - requestSpanMs + timeframeMs,
    );
    const rows = await bitgetFetch(
      "GET",
      "/api/v2/mix/market/history-candles",
      {
        symbol,
        productType,
        granularity,
        limit: requestLimit,
        startTime,
        endTime: cursorEnd,
      },
    );
    requests += 1;

    const parsedRows = Array.isArray(rows)
      ? normalizeFetchedCandles(rows).filter(
          (row) => row[0] >= startMs && row[0] <= endMs,
        )
      : [];
    if (!parsedRows.length) {
      if (startTime <= startMs) break;
      cursorEnd = startTime - timeframeMs;
      continue;
    }

    let oldestTs = Number.POSITIVE_INFINITY;
    for (const candle of parsedRows) {
      candlesByTs.set(candle[0], candle);
      if (candle[0] < oldestTs) oldestTs = candle[0];
    }
    if (!Number.isFinite(oldestTs)) break;
    if (oldestTs >= cursorEnd) {
      cursorEnd -= requestSpanMs;
    } else {
      cursorEnd = oldestTs - 1;
    }
  }

  return Array.from(candlesByTs.values()).sort((a, b) => a[0] - b[0]);
}

async function ensurePipelineJobRow(
  jobKind: ScalpPipelineJobKind,
): Promise<void> {
  const db = scalpPrisma();
  await db.$executeRaw(Prisma.sql`
        INSERT INTO scalp_pipeline_jobs(job_kind, status, next_run_at, created_at, updated_at)
        VALUES(${jobKind}, 'idle', NOW(), NOW(), NOW())
        ON CONFLICT(job_kind) DO NOTHING;
    `);
}

async function acquirePipelineJobLock(params: {
  jobKind: ScalpPipelineJobKind;
  lockToken: string;
  lockMs: number;
}): Promise<boolean> {
  await ensurePipelineJobRow(params.jobKind);
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ jobKind: string }>>(Prisma.sql`
        UPDATE scalp_pipeline_jobs
        SET
            status = 'running',
            lock_token = ${params.lockToken},
            lock_expires_at = NOW() + make_interval(secs => ${Math.max(5, Math.floor(params.lockMs / 1000))}),
            running_since = COALESCE(running_since, NOW()),
            attempts = attempts + 1,
            last_run_at = NOW(),
            updated_at = NOW()
        WHERE job_kind = ${params.jobKind}
          AND (lock_expires_at IS NULL OR lock_expires_at < NOW())
        RETURNING job_kind AS "jobKind";
    `);
  return rows.length > 0;
}

async function pulsePipelineJobProgress(params: {
  jobKind: ScalpPipelineJobKind;
  lockToken: string;
  lockMs: number;
  progressLabel?: string | null;
  progress?: Record<string, unknown> | null;
}): Promise<void> {
  const db = scalpPrisma();
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_pipeline_jobs
        SET
            lock_expires_at = NOW() + make_interval(secs => ${Math.max(5, Math.floor(params.lockMs / 1000))}),
            progress_label = ${params.progressLabel || null},
            progress_json = ${params.progress ? JSON.stringify(params.progress) : null}::jsonb,
            updated_at = NOW()
        WHERE job_kind = ${params.jobKind}
          AND lock_token = ${params.lockToken};
    `);
}

async function releasePipelineJobLock(params: {
  jobKind: ScalpPipelineJobKind;
  lockToken: string;
  success: boolean;
  lastDurationMs?: number | null;
  nextRunAtMs?: number | null;
  lastError?: string | null;
  progressLabel?: string | null;
  progress?: Record<string, unknown> | null;
}): Promise<void> {
  const nextRunAt =
    typeof params.nextRunAtMs === "number" &&
    Number.isFinite(params.nextRunAtMs) &&
    params.nextRunAtMs > 0
      ? new Date(params.nextRunAtMs)
      : null;
  const db = scalpPrisma();
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_pipeline_jobs
        SET
            status = ${params.success ? "idle" : "failed"},
            lock_token = NULL,
            lock_expires_at = NULL,
            running_since = NULL,
            next_run_at = COALESCE(${nextRunAt}, next_run_at),
            last_success_at = CASE WHEN ${params.success} THEN NOW() ELSE last_success_at END,
            last_duration_ms = ${params.lastDurationMs ?? null},
            last_error = ${params.lastError || null},
            progress_label = ${params.progressLabel || null},
            progress_json = ${params.progress ? JSON.stringify(params.progress) : null}::jsonb,
            updated_at = NOW()
        WHERE job_kind = ${params.jobKind}
          AND lock_token = ${params.lockToken};
    `);
}

async function insertPipelineJobRun(params: {
  jobKind: ScalpPipelineJobKind;
  status: "succeeded" | "failed";
  diagnostics: ScalpPipelineJobDiagnostics;
  result: Omit<ScalpPipelineJobExecutionResult, "jobKind" | "busy">;
}): Promise<void> {
  const startedAt = params.diagnostics.startedAtMs;
  const finishedAt = params.diagnostics.finishedAtMs;
  const durationMs = params.diagnostics.durationMs;
  if (
    startedAt === null ||
    finishedAt === null ||
    durationMs === null ||
    finishedAt < startedAt
  ) {
    return;
  }
  const db = scalpPrisma();
  try {
    await db.$executeRaw(
      Prisma.sql`
      INSERT INTO scalp_pipeline_job_runs(
        job_kind,
        status,
        started_at,
        finished_at,
        duration_ms,
        processed,
        succeeded,
        retried,
        failed,
        pending_after,
        downstream_requested,
        error,
        progress_label,
        details_json
      )
      VALUES(
        ${params.jobKind},
        ${params.status},
        ${new Date(startedAt)},
        ${new Date(finishedAt)},
        ${Math.max(0, Math.floor(durationMs))},
        ${Math.max(0, Math.floor(Number(params.result.processed || 0)))},
        ${Math.max(0, Math.floor(Number(params.result.succeeded || 0)))},
        ${Math.max(0, Math.floor(Number(params.result.retried || 0)))},
        ${Math.max(0, Math.floor(Number(params.result.failed || 0)))},
        ${Math.max(0, Math.floor(Number(params.result.pendingAfter || 0)))},
        ${Boolean(params.result.downstreamRequested)},
        ${String(params.result.error || "").trim() || null},
        ${params.result.progressLabel || null},
        ${params.result.details ? JSON.stringify(params.result.details) : null}::jsonb
      );
    `,
    );
  } catch (err: any) {
    console.warn("[scalp-pipeline-jobs] failed to persist pipeline run", {
      jobKind: params.jobKind,
      status: params.status,
      message: err?.message || String(err),
    });
  }
}

async function runWithPipelineJobLock(
  jobKind: ScalpPipelineJobKind,
  run: (ctx: {
    lockToken: string;
    lockMs: number;
  }) => Promise<Omit<ScalpPipelineJobExecutionResult, "jobKind" | "busy">>,
): Promise<ScalpPipelineJobExecutionResult> {
  const emptyDiagnostics = buildPipelineJobDiagnostics(null, null);
  if (!isScalpPgConfigured()) {
    return {
      ok: false,
      busy: false,
      jobKind,
      processed: 0,
      succeeded: 0,
      retried: 0,
      failed: 0,
      pendingAfter: 0,
      downstreamRequested: false,
      progressLabel: null,
      details: {},
      diagnostics: emptyDiagnostics,
      error: "scalp_pg_not_configured",
    };
  }
  const lockMs = Math.max(
    30_000,
    Math.min(
      20 * 60_000,
      toPositiveInt(process.env.SCALP_PIPELINE_JOB_LOCK_MS, 6 * 60_000),
    ),
  );
  const lockToken = `${jobKind}:${Date.now()}:${Math.floor(Math.random() * 1_000_000)}`;
  const acquired = await acquirePipelineJobLock({ jobKind, lockToken, lockMs });
  if (!acquired) {
    return {
      ok: true,
      busy: true,
      jobKind,
      processed: 0,
      succeeded: 0,
      retried: 0,
      failed: 0,
      pendingAfter: 0,
      downstreamRequested: false,
      progressLabel: "busy",
      details: {},
      diagnostics: emptyDiagnostics,
    };
  }

  const runStartedAtMs = Date.now();
  try {
    const result = await run({ lockToken, lockMs });
    const diagnostics = buildPipelineJobDiagnostics(runStartedAtMs, Date.now());
    await releasePipelineJobLock({
      jobKind,
      lockToken,
      success: result.ok,
      lastDurationMs: diagnostics.durationMs,
      lastError: result.ok ? null : String(result.details?.error || ""),
      progressLabel: result.progressLabel || null,
      progress: result.details,
    });
    await insertPipelineJobRun({
      jobKind,
      status: result.ok ? "succeeded" : "failed",
      diagnostics,
      result,
    });
    return {
      ...result,
      busy: false,
      jobKind,
      diagnostics,
    };
  } catch (err: any) {
    const error = String(err?.message || err || "pipeline_job_failed");
    const diagnostics = buildPipelineJobDiagnostics(runStartedAtMs, Date.now());
    const result = {
      ok: false,
      processed: 0,
      succeeded: 0,
      retried: 0,
      failed: 0,
      pendingAfter: 0,
      downstreamRequested: false,
      progressLabel: "failed",
      details: { error },
      error,
    } satisfies Omit<ScalpPipelineJobExecutionResult, "jobKind" | "busy">;
    await releasePipelineJobLock({
      jobKind,
      lockToken,
      success: false,
      lastDurationMs: diagnostics.durationMs,
      lastError: error.slice(0, 500),
      progressLabel: "failed",
      progress: { error },
    });
    await insertPipelineJobRun({
      jobKind,
      status: "failed",
      diagnostics,
      result,
    });
    return { ...result, busy: false, jobKind, diagnostics };
  }
}

async function countPendingLoadSymbols(): Promise<number> {
  const berlinWeekStartMs = startOfBerlinWeekMonday(Date.now());
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ count: bigint | number | string }>
  >(Prisma.sql`
        WITH due_enabled_symbols AS (
            SELECT DISTINCT d.symbol
            FROM scalp_deployments d
            WHERE d.enabled = TRUE
              AND COALESCE((d.promotion_gate #>> '{lifecycle,lastRolloverBerlinWeekStartMs}')::bigint, 0) < ${berlinWeekStartMs}
        )
        SELECT COUNT(*)::bigint AS count
        FROM scalp_pipeline_symbols
        WHERE (
                active = TRUE
                OR symbol IN (SELECT symbol FROM due_enabled_symbols)
              )
          AND load_status IN ('pending', 'retry_wait')
          AND COALESCE(load_next_run_at, NOW()) <= NOW();
    `);
  return Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
}

async function countActivePipelineSymbols(): Promise<number> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ count: bigint | number | string }>>(
    Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_pipeline_symbols
        WHERE active = TRUE;
    `,
  );
  return Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
}

async function countPendingPrepareSymbols(): Promise<number> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ count: bigint | number | string }>
  >(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_pipeline_symbols
        WHERE active = TRUE
          AND load_status = 'succeeded'
          AND prepare_status IN ('pending', 'retry_wait')
          AND COALESCE(prepare_next_run_at, NOW()) <= NOW();
    `);
  return Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
}

async function countPendingWorkerRows(): Promise<number> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ count: bigint | number | string }>
  >(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_deployment_weekly_metrics m
        INNER JOIN scalp_deployments d
          ON d.deployment_id = m.deployment_id
        WHERE d.in_universe = TRUE
          AND m.status IN ('pending', 'retry_wait')
          AND m.next_run_at <= NOW();
    `);
  return Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
}

async function countPendingPromotionRows(): Promise<number> {
  const nowMs = Date.now();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ count: bigint | number | string }>
  >(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_deployments
        WHERE (
                in_universe = TRUE
                AND promotion_dirty = TRUE
              )
           OR (
                COALESCE((promotion_gate #>> '{lifecycle,state}')::text, '') = 'suspended'
                AND COALESCE((promotion_gate #>> '{lifecycle,suspendedUntilMs}')::bigint, 0) > 0
                AND COALESCE((promotion_gate #>> '{lifecycle,suspendedUntilMs}')::bigint, 0) <= ${nowMs}
              )
           OR (
                COALESCE((promotion_gate #>> '{lifecycle,state}')::text, '') = 'retired'
                AND COALESCE((promotion_gate #>> '{lifecycle,retiredUntilMs}')::bigint, 0) > 0
                AND COALESCE((promotion_gate #>> '{lifecycle,retiredUntilMs}')::bigint, 0) <= ${nowMs}
              );
    `);
  return Math.max(0, Math.floor(Number(rows[0]?.count || 0)));
}

function resolveWeeklyPolicyDefaults(): SyncResearchWeeklyPolicy {
  return {
    enabled: envBool("SCALP_WEEKLY_ROBUSTNESS_ENABLED", true),
    topKPerSymbol: Math.max(
      1,
      toPositiveInt(envNumber("SCALP_WEEKLY_ROBUSTNESS_TOPK_PER_SYMBOL", 2), 2),
    ),
    globalMaxSymbols: Math.max(
      1,
      Math.min(
        200,
        toPositiveInt(
          envNumber("SCALP_WEEKLY_ROBUSTNESS_GLOBAL_MAX_SYMBOLS", 6),
          6,
        ),
      ),
    ),
    globalMaxDeployments: Math.max(
      1,
      Math.min(
        1_000,
        toPositiveInt(
          envNumber("SCALP_WEEKLY_ROBUSTNESS_GLOBAL_MAX_DEPLOYMENTS", 12),
          12,
        ),
      ),
    ),
    lookbackDays: Math.max(
      28,
      toPositiveInt(envNumber("SCALP_WEEKLY_ROBUSTNESS_LOOKBACK_DAYS", 91), 91),
    ),
    minCandlesPerSlice: Math.max(
      120,
      toPositiveInt(
        envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_CANDLES_PER_SLICE", 180),
        180,
      ),
    ),
    requireWinnerShortlist: envBool(
      "SCALP_WEEKLY_ROBUSTNESS_REQUIRE_WINNER_SHORTLIST",
      true,
    ),
    minSlices: Math.max(
      2,
      toPositiveInt(envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_SLICES", 8), 8),
    ),
    minProfitablePct: toBoundedPercent(
      envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_PROFITABLE_PCT", 55),
      55,
    ),
    minMedianExpectancyR: toFiniteNumber(
      envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_MEDIAN_EXPECTANCY_R", 0.02),
      0.02,
    ),
    minP25ExpectancyR: toFiniteNumber(
      envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_P25_EXPECTANCY_R", -0.02),
      -0.02,
    ),
    minWorstNetR: toFiniteNumber(
      envNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_WORST_NET_R", -1.5),
      -1.5,
    ),
    maxTopWeekPnlConcentrationPct: toBoundedPercent(
      envNumber(
        "SCALP_WEEKLY_ROBUSTNESS_MAX_TOP_WEEK_PNL_CONCENTRATION_PCT",
        55,
      ),
      55,
    ),
  };
}

function toBoundedFraction(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(1, n));
}

function resolvePromotionExplorationShare(): number {
  return toBoundedFraction(
    process.env.SCALP_PROMOTION_EXPLORATION_SHARE,
    0.4,
  );
}

function resolvePromotionHysteresisFailThreshold(): number {
  return Math.max(
    1,
    Math.min(
      10,
      toPositiveInt(process.env.SCALP_PROMOTION_HYSTERESIS_FAIL_STREAK, 2),
    ),
  );
}

function resolvePromotionHysteresisPassThreshold(): number {
  return Math.max(
    1,
    Math.min(
      10,
      toPositiveInt(process.env.SCALP_PROMOTION_HYSTERESIS_PASS_STREAK, 2),
    ),
  );
}

function resolvePromotionLoadNudgeMaxSymbols(): number {
  return Math.max(
    1,
    Math.min(
      1000,
      toPositiveInt(process.env.SCALP_PROMOTION_NUDGE_LOAD_SYMBOLS_MAX, 40),
    ),
  );
}

function resolvePromotionWorkerNudgeMaxDeployments(): number {
  return Math.max(
    1,
    Math.min(
      5000,
      toPositiveInt(
        process.env.SCALP_PROMOTION_NUDGE_WORKER_DEPLOYMENTS_MAX,
        120,
      ),
    ),
  );
}

function resolvePromotionSelectionShrinkageK(): number {
  return Math.max(
    1,
    Math.min(
      52,
      toPositiveInt(process.env.SCALP_WEEKLY_SELECTION_SHRINKAGE_K, 6),
    ),
  );
}

function resolveWorkerMaxClaimPerRun(): number {
  return Math.max(
    1,
    Math.min(
      400,
      toPositiveInt(process.env.SCALP_PIPELINE_WORKER_MAX_CLAIM_PER_RUN, 10),
    ),
  );
}

function resolveWorkerRowTimeoutMs(lockMs: number): number {
  const configured = Math.max(
    30_000,
    Math.min(
      15 * 60_000,
      toPositiveInt(process.env.SCALP_PIPELINE_WORKER_ROW_TIMEOUT_MS, 180_000),
    ),
  );
  const lockBound = Math.max(30_000, lockMs - 15_000);
  return Math.min(configured, lockBound);
}

function resolveWorkerReplayProgressEveryRuns(): number {
  return Math.max(
    20,
    Math.min(
      10_000,
      toPositiveInt(process.env.SCALP_PIPELINE_WORKER_PROGRESS_EVERY_RUNS, 300),
    ),
  );
}

function resolveWorkerReplayProgressMinIntervalMs(): number {
  return Math.max(
    250,
    Math.min(
      15_000,
      toPositiveInt(
        process.env.SCALP_PIPELINE_WORKER_PROGRESS_MIN_INTERVAL_MS,
        1_000,
      ),
    ),
  );
}

class WorkerRowTimeoutError extends Error {
  timeoutMs: number;

  constructor(message: string, timeoutMs: number) {
    super(message);
    this.name = "WorkerRowTimeoutError";
    this.timeoutMs = timeoutMs;
  }
}

function candidateMedianExpectancyR(
  candidate: Pick<ScalpPromotionForwardValidationCandidate, "meanExpectancyR"> & {
    medianExpectancyR?: number | null;
  },
): number {
  const medianValue = Number(candidate.medianExpectancyR);
  if (Number.isFinite(medianValue)) return medianValue;
  return Number(candidate.meanExpectancyR) || 0;
}

function candidateTopWindowPnlConcentrationPct(candidate: {
  topWindowPnlConcentrationPct?: number | null;
}): number {
  const concentration = Number(candidate.topWindowPnlConcentrationPct);
  if (!Number.isFinite(concentration)) return 0;
  return Math.max(0, Math.min(100, concentration));
}

function candidateSelectionScoreForPipeline(
  candidate: Pick<ScalpPromotionForwardValidationCandidate, "meanExpectancyR"> & {
    trimmedMeanExpectancyR?: number | null;
    medianExpectancyR?: number | null;
    topWindowPnlConcentrationPct?: number | null;
    selectionScore?: number | null;
    rollCount?: number;
  },
): number {
  const explicit = Number(candidate.selectionScore);
  if (Number.isFinite(explicit)) return explicit;
  const trimmedMeanValue = Number(candidate.trimmedMeanExpectancyR);
  const robustMean = Number.isFinite(trimmedMeanValue)
    ? trimmedMeanValue
    : (Number(candidate.meanExpectancyR) +
        candidateMedianExpectancyR(candidate)) /
      2;
  const smoothedExpectancy =
    (robustMean + candidateMedianExpectancyR(candidate)) / 2;
  const concentrationPenalty = Math.max(
    0,
    candidateTopWindowPnlConcentrationPct(candidate) - 50,
  );
  const concentrationAdjusted =
    smoothedExpectancy * (1 - concentrationPenalty / 100);
  const rollCount = Math.max(0, Math.floor(Number(candidate.rollCount) || 0));
  const shrinkageK = resolvePromotionSelectionShrinkageK();
  const sampleWeight = rollCount > 0 ? rollCount / (rollCount + shrinkageK) : 0;
  return concentrationAdjusted * sampleWeight;
}

function candidateProfitFactorForRanking(
  candidate: Pick<ScalpPromotionForwardValidationCandidate, "meanProfitFactor">,
): number {
  const profitFactor = Number(candidate.meanProfitFactor);
  if (!Number.isFinite(profitFactor)) return Number.NEGATIVE_INFINITY;
  return profitFactor;
}

function comparePromotionCandidates(
  a: ScalpPromotionForwardValidationCandidate,
  b: ScalpPromotionForwardValidationCandidate,
): number {
  const aSelectionScore = candidateSelectionScoreForPipeline(a);
  const bSelectionScore = candidateSelectionScoreForPipeline(b);
  if (bSelectionScore !== aSelectionScore)
    return bSelectionScore - aSelectionScore;
  if (b.profitableWindowPct !== a.profitableWindowPct)
    return b.profitableWindowPct - a.profitableWindowPct;
  const aProfitFactor = candidateProfitFactorForRanking(a);
  const bProfitFactor = candidateProfitFactorForRanking(b);
  if (bProfitFactor !== aProfitFactor) return bProfitFactor - aProfitFactor;
  if (a.maxDrawdownR !== b.maxDrawdownR) return a.maxDrawdownR - b.maxDrawdownR;
  const aMedianExpectancyR = candidateMedianExpectancyR(a);
  const bMedianExpectancyR = candidateMedianExpectancyR(b);
  if (bMedianExpectancyR !== aMedianExpectancyR)
    return bMedianExpectancyR - aMedianExpectancyR;
  if (b.meanExpectancyR !== a.meanExpectancyR)
    return b.meanExpectancyR - a.meanExpectancyR;
  if (b.rollCount !== a.rollCount) return b.rollCount - a.rollCount;
  if (a.strategyId !== b.strategyId)
    return a.strategyId.localeCompare(b.strategyId);
  if (a.tuneId !== b.tuneId) return a.tuneId.localeCompare(b.tuneId);
  return a.deploymentId.localeCompare(b.deploymentId);
}

export interface PromotionSelectionRow {
  deploymentId: string;
  symbol: string;
  incumbent: boolean;
  candidate: ScalpPromotionForwardValidationCandidate;
}

export interface PromotionSelectionResult {
  winnerIds: Set<string>;
  selectedRows: PromotionSelectionRow[];
  explorationSlots: number;
  exploitSlots: number;
  explorationSelected: number;
  exploitSelected: number;
}

export function selectPromotionWinnerRowsWithExploration(params: {
  rows: PromotionSelectionRow[];
  explorationShare: number;
  maxSymbols: number;
  maxPerSymbol: number;
  maxDeployments: number;
}): PromotionSelectionResult {
  const maxSymbols = Math.max(1, Math.floor(Number(params.maxSymbols) || 1));
  const maxPerSymbol = Math.max(
    1,
    Math.floor(Number(params.maxPerSymbol) || 1),
  );
  const maxDeployments = Math.max(
    1,
    Math.floor(Number(params.maxDeployments) || 1),
  );
  const rows = Array.isArray(params.rows) ? params.rows.slice() : [];
  if (!rows.length) {
    return {
      winnerIds: new Set<string>(),
      selectedRows: [],
      explorationSlots: 0,
      exploitSlots: 0,
      explorationSelected: 0,
      exploitSelected: 0,
    };
  }

  const totalCap = Math.min(maxDeployments, rows.length);
  const incumbents = rows
    .filter((row) => row.incumbent)
    .sort((a, b) => comparePromotionCandidates(a.candidate, b.candidate));
  const challengers = rows
    .filter((row) => !row.incumbent)
    .sort((a, b) => comparePromotionCandidates(a.candidate, b.candidate));
  const explorationShare = toBoundedFraction(params.explorationShare, 0.4);
  let explorationSlots =
    challengers.length > 0 ? Math.floor(totalCap * explorationShare) : 0;
  if (challengers.length > 0 && totalCap > 1) {
    explorationSlots = Math.max(1, explorationSlots);
  }
  if (!incumbents.length) explorationSlots = totalCap;
  explorationSlots = Math.max(0, Math.min(totalCap, explorationSlots));
  let exploitSlots = Math.max(0, totalCap - explorationSlots);
  if (!challengers.length) {
    explorationSlots = 0;
    exploitSlots = totalCap;
  }

  const selectedRows: PromotionSelectionRow[] = [];
  const selectedIds = new Set<string>();
  const selectedSymbols = new Set<string>();
  const perSymbolCount = new Map<string, number>();
  let exploitSelected = 0;
  let explorationSelected = 0;

  const canSelect = (row: PromotionSelectionRow): boolean => {
    if (selectedIds.has(row.deploymentId)) return false;
    const symbol = normalizeSymbol(row.symbol);
    if (!symbol) return false;
    const currentPerSymbol = perSymbolCount.get(symbol) || 0;
    if (currentPerSymbol >= maxPerSymbol) return false;
    if (!selectedSymbols.has(symbol) && selectedSymbols.size >= maxSymbols) {
      return false;
    }
    return true;
  };

  const addSelectedRow = (row: PromotionSelectionRow): boolean => {
    if (!canSelect(row)) return false;
    const symbol = normalizeSymbol(row.symbol);
    selectedRows.push(row);
    selectedIds.add(row.deploymentId);
    selectedSymbols.add(symbol);
    perSymbolCount.set(symbol, (perSymbolCount.get(symbol) || 0) + 1);
    if (row.incumbent) exploitSelected += 1;
    else explorationSelected += 1;
    return true;
  };

  const selectFromPool = (pool: PromotionSelectionRow[], target: number): number => {
    let picked = 0;
    if (target <= 0) return picked;
    for (const row of pool) {
      if (picked >= target) break;
      if (!addSelectedRow(row)) continue;
      picked += 1;
    }
    return picked;
  };

  // Keep at least one candidate per currently active symbol when capacity allows.
  const incumbentSymbolFloorRows = Array.from(
    new Set(incumbents.map((row) => normalizeSymbol(row.symbol)).filter(Boolean)),
  )
    .map((symbol) =>
      incumbents.find((row) => normalizeSymbol(row.symbol) === symbol) || null,
    )
    .filter((row): row is PromotionSelectionRow => Boolean(row))
    .sort((a, b) => comparePromotionCandidates(a.candidate, b.candidate));
  for (const row of incumbentSymbolFloorRows) {
    if (selectedRows.length >= totalCap) break;
    addSelectedRow(row);
  }

  const remainingExploitTarget = Math.max(0, exploitSlots - exploitSelected);
  const remainingExplorationTarget = Math.max(
    0,
    explorationSlots - explorationSelected,
  );
  selectFromPool(incumbents, remainingExploitTarget);
  selectFromPool(challengers, remainingExplorationTarget);

  if (selectedRows.length < totalCap) {
    const combined = rows
      .slice()
      .sort((a, b) => comparePromotionCandidates(a.candidate, b.candidate));
    selectFromPool(combined, totalCap - selectedRows.length);
  }

  return {
    winnerIds: new Set(selectedRows.map((row) => row.deploymentId)),
    selectedRows,
    explorationSlots,
    exploitSlots,
    explorationSelected,
    exploitSelected,
  };
}

export interface PromotionEnabledUniquenessRow {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  enabled: boolean;
  shortlistIncluded: boolean;
  candidate?: ScalpPromotionForwardValidationCandidate | null;
  promotionGate?: ScalpDeploymentPromotionGate | null;
}

export interface PromotionEnabledUniquenessResult {
  primaryEnabledIds: Set<string>;
  demotedIds: Set<string>;
}

function asFiniteNumberOr(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function resolveUniquenessForwardValidationScore(
  forwardValidation: ScalpForwardValidationMetrics | null | undefined,
): {
  selectionScore: number;
  profitableWindowPct: number;
  meanProfitFactor: number;
  maxDrawdownR: number;
} {
  if (!forwardValidation) {
    return {
      selectionScore: Number.NEGATIVE_INFINITY,
      profitableWindowPct: Number.NEGATIVE_INFINITY,
      meanProfitFactor: Number.NEGATIVE_INFINITY,
      maxDrawdownR: Number.POSITIVE_INFINITY,
    };
  }
  const score = candidateSelectionScoreForPipeline({
    meanExpectancyR: asFiniteNumberOr(
      forwardValidation.weeklyMeanExpectancyR,
      asFiniteNumberOr(forwardValidation.meanExpectancyR, 0),
    ),
    trimmedMeanExpectancyR: asFiniteNumberOr(
      forwardValidation.weeklyTrimmedMeanExpectancyR,
      asFiniteNumberOr(forwardValidation.meanExpectancyR, 0),
    ),
    medianExpectancyR: asFiniteNumberOr(
      forwardValidation.weeklyMedianExpectancyR,
      asFiniteNumberOr(forwardValidation.meanExpectancyR, 0),
    ),
    topWindowPnlConcentrationPct: asFiniteNumberOr(
      forwardValidation.weeklyTopWeekPnlConcentrationPct,
      0,
    ),
    rollCount: Math.max(
      0,
      Math.floor(
        asFiniteNumberOr(
          forwardValidation.weeklySlices,
          asFiniteNumberOr(forwardValidation.rollCount, 0),
        ),
      ),
    ),
  });
  const profitableWindowPct = asFiniteNumberOr(
    forwardValidation.weeklyProfitablePct,
    asFiniteNumberOr(forwardValidation.profitableWindowPct, Number.NEGATIVE_INFINITY),
  );
  const meanProfitFactor = asFiniteNumberOr(
    forwardValidation.meanProfitFactor,
    Number.NEGATIVE_INFINITY,
  );
  const maxDrawdownR = asFiniteNumberOr(
    forwardValidation.maxDrawdownR,
    Number.POSITIVE_INFINITY,
  );
  return {
    selectionScore: score,
    profitableWindowPct,
    meanProfitFactor,
    maxDrawdownR,
  };
}

function compareEnabledRowsForUniqueness(
  a: PromotionEnabledUniquenessRow,
  b: PromotionEnabledUniquenessRow,
): number {
  if (a.shortlistIncluded !== b.shortlistIncluded) {
    return a.shortlistIncluded ? -1 : 1;
  }
  if (a.candidate && b.candidate) {
    const byCandidate = comparePromotionCandidates(a.candidate, b.candidate);
    if (byCandidate !== 0) return byCandidate;
  } else if (a.candidate || b.candidate) {
    return a.candidate ? -1 : 1;
  }
  const aForward = resolveUniquenessForwardValidationScore(
    a.promotionGate?.forwardValidation,
  );
  const bForward = resolveUniquenessForwardValidationScore(
    b.promotionGate?.forwardValidation,
  );
  if (bForward.selectionScore !== aForward.selectionScore) {
    return bForward.selectionScore - aForward.selectionScore;
  }
  if (bForward.profitableWindowPct !== aForward.profitableWindowPct) {
    return bForward.profitableWindowPct - aForward.profitableWindowPct;
  }
  if (bForward.meanProfitFactor !== aForward.meanProfitFactor) {
    return bForward.meanProfitFactor - aForward.meanProfitFactor;
  }
  if (aForward.maxDrawdownR !== bForward.maxDrawdownR) {
    return aForward.maxDrawdownR - bForward.maxDrawdownR;
  }
  const aPass = Math.max(
    0,
    Math.floor(Number(a.promotionGate?.hysteresis?.passStreak) || 0),
  );
  const bPass = Math.max(
    0,
    Math.floor(Number(b.promotionGate?.hysteresis?.passStreak) || 0),
  );
  if (bPass !== aPass) return bPass - aPass;
  if (a.tuneId !== b.tuneId) return a.tuneId.localeCompare(b.tuneId);
  return a.deploymentId.localeCompare(b.deploymentId);
}

export function enforceSingleEnabledPerSymbolStrategy(params: {
  rows: PromotionEnabledUniquenessRow[];
}): PromotionEnabledUniquenessResult {
  const rows = Array.isArray(params.rows) ? params.rows : [];
  const enabledRows = rows.filter((row) => Boolean(row.enabled));
  const bySymbolStrategy = new Map<string, PromotionEnabledUniquenessRow[]>();
  for (const row of enabledRows) {
    const symbol = normalizeSymbol(row.symbol);
    const strategyId = String(row.strategyId || "")
      .trim()
      .toLowerCase();
    if (!symbol || !strategyId) continue;
    const key = `${symbol}::${strategyId}`;
    const bucket = bySymbolStrategy.get(key) || [];
    bucket.push(row);
    bySymbolStrategy.set(key, bucket);
  }
  const primaryEnabledIds = new Set<string>();
  const demotedIds = new Set<string>();
  for (const groupRows of bySymbolStrategy.values()) {
    if (!groupRows.length) continue;
    const sorted = groupRows
      .slice()
      .sort((a, b) => compareEnabledRowsForUniqueness(a, b));
    const primary = sorted[0];
    if (!primary) continue;
    primaryEnabledIds.add(primary.deploymentId);
    for (let i = 1; i < sorted.length; i += 1) {
      demotedIds.add(sorted[i].deploymentId);
    }
  }
  return {
    primaryEnabledIds,
    demotedIds,
  };
}

export interface PromotionHysteresisResult {
  enabled: boolean;
  hysteresis: ScalpDeploymentPromotionHysteresis;
  transition: "enabled" | "disabled" | "held";
}

export function applyPromotionHysteresis(params: {
  currentlyEnabled: boolean;
  shouldEnableNow: boolean;
  previous: ScalpDeploymentPromotionHysteresis | null | undefined;
  passThreshold: number;
  failThreshold: number;
  nowMs: number;
  lockEnabled?: boolean;
}): PromotionHysteresisResult {
  const passThreshold = Math.max(1, Math.floor(Number(params.passThreshold) || 1));
  const failThreshold = Math.max(1, Math.floor(Number(params.failThreshold) || 1));
  const lockEnabled = Boolean(params.lockEnabled);
  const previousPass = Math.max(
    0,
    Math.floor(Number(params.previous?.passStreak) || 0),
  );
  const previousFail = Math.max(
    0,
    Math.floor(Number(params.previous?.failStreak) || 0),
  );
  const passStreak = params.shouldEnableNow
    ? Math.min(passThreshold, previousPass + 1)
    : 0;
  const failStreak = params.shouldEnableNow
    ? 0
    : Math.min(failThreshold, previousFail + 1);
  let enabled = Boolean(params.currentlyEnabled);
  if (enabled && lockEnabled) {
    enabled = true;
  } else if (enabled) {
    if (!params.shouldEnableNow && failStreak >= failThreshold) {
      enabled = false;
    }
  } else if (params.shouldEnableNow && passStreak >= passThreshold) {
    enabled = true;
  }
  const transition: PromotionHysteresisResult["transition"] =
    enabled !== Boolean(params.currentlyEnabled)
      ? enabled
        ? "enabled"
        : "disabled"
      : "held";
  const hysteresis: ScalpDeploymentPromotionHysteresis = {
    passStreak,
    failStreak,
    lastStateChangeAtMs:
      transition === "held"
        ? params.previous?.lastStateChangeAtMs || null
        : params.nowMs,
    lastDecision:
      transition === "held"
        ? "hold"
        : transition === "enabled"
          ? "enable"
          : "disable",
  };
  return {
    enabled,
    hysteresis,
    transition,
  };
}

export interface DiscoverSyncExistingRow {
  symbol: string;
  active: boolean;
}

export interface DiscoverSymbolSyncPlan {
  activeSymbols: string[];
  catalogSymbols: string[];
  addedActiveSymbols: string[];
  removedActiveSymbols: string[];
  catalogAddedSymbols: string[];
}

export function buildDiscoverSymbolSyncPlan(params: {
  existingRows: DiscoverSyncExistingRow[];
  activeSymbols: string[];
  catalogSymbols: string[];
}): DiscoverSymbolSyncPlan {
  const existingRows = Array.isArray(params.existingRows) ? params.existingRows : [];
  const existingActive = new Set(
    existingRows
      .filter((row) => Boolean(row.active))
      .map((row) => normalizeSymbol(row.symbol))
      .filter(Boolean),
  );
  const existingCatalog = new Set(
    existingRows.map((row) => normalizeSymbol(row.symbol)).filter(Boolean),
  );
  const activeSymbols = Array.from(
    new Set(
      (params.activeSymbols || []).map((row) => normalizeSymbol(row)).filter(Boolean),
    ),
  );
  const catalogSymbols = Array.from(
    new Set(
      (params.catalogSymbols || [])
        .map((row) => normalizeSymbol(row))
        .filter(Boolean),
    ),
  );
  const activeSet = new Set(activeSymbols);
  return {
    activeSymbols,
    catalogSymbols,
    addedActiveSymbols: activeSymbols.filter((symbol) => !existingActive.has(symbol)),
    removedActiveSymbols: Array.from(existingActive).filter((symbol) => !activeSet.has(symbol)),
    catalogAddedSymbols: catalogSymbols.filter((symbol) => !existingCatalog.has(symbol)),
  };
}

function median(values: number[]): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid] || 0;
  const left = sorted[mid - 1] || 0;
  const right = sorted[mid] || 0;
  return (left + right) / 2;
}

function mean(values: number[]): number {
  if (!values.length) return 0;
  return values.reduce((acc, row) => acc + row, 0) / values.length;
}

function quantile(values: number[], p: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const clampedP = Math.max(0, Math.min(1, p));
  const index = (sorted.length - 1) * clampedP;
  const low = Math.floor(index);
  const high = Math.ceil(index);
  const lowValue = sorted[low] || 0;
  const highValue = sorted[high] || 0;
  if (low === high) return lowValue;
  const weight = index - low;
  return lowValue + (highValue - lowValue) * weight;
}

function trimmedMean(values: number[], trimRatio: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const clampedTrim = Math.max(0, Math.min(0.49, trimRatio));
  const trimCount = Math.floor(sorted.length * clampedTrim);
  if (trimCount * 2 >= sorted.length) return mean(sorted);
  const trimmed = sorted.slice(trimCount, sorted.length - trimCount);
  return mean(trimmed.length ? trimmed : sorted);
}

function resolveWeeklySelectionTrimRatio(): number {
  return Math.max(
    0,
    Math.min(
      0.4,
      toFiniteNumber(envNumber("SCALP_WEEKLY_SELECTION_TRIM_RATIO", 0.15), 0.15),
    ),
  );
}

function topPositiveNetConcentrationPct(values: number[]): number {
  const positiveNet = values.map((value) => Math.max(0, value));
  const totalPositive = positiveNet.reduce((acc, value) => acc + value, 0);
  if (totalPositive <= 0) return 100;
  const topPositive = positiveNet.length ? Math.max(...positiveNet) : 0;
  return (topPositive / totalPositive) * 100;
}

function computeWeeklyRobustnessFromTasks(params: {
  tasks: Array<{ netR: number; expectancyR: number; maxDrawdownR: number }>;
  nowMs: number;
}): ScalpWeeklyRobustnessMetrics | null {
  if (!params.tasks.length) return null;
  const profitableSlices = params.tasks.filter((row) => row.netR > 0).length;
  const expectancyRows = params.tasks.map((row) => row.expectancyR);
  const netRows = params.tasks.map((row) => row.netR);
  const maxDrawdownRows = params.tasks.map((row) => row.maxDrawdownR);
  const slices = params.tasks.length;
  const totalNetR = netRows.reduce((acc, row) => acc + row, 0);
  const meanExpectancyR =
    expectancyRows.reduce((acc, row) => acc + row, 0) / slices;
  const trimmedMeanExpectancyR = trimmedMean(
    expectancyRows,
    resolveWeeklySelectionTrimRatio(),
  );
  const p25ExpectancyR = quantile(expectancyRows, 0.25);
  const medianExpectancyR = median(expectancyRows);
  const worstNetR = netRows.reduce(
    (acc, row) => Math.min(acc, row),
    Number.POSITIVE_INFINITY,
  );
  const worstMaxDrawdownR = maxDrawdownRows.reduce(
    (acc, row) => Math.max(acc, row),
    0,
  );
  return {
    slices,
    profitableSlices,
    profitablePct: (profitableSlices / slices) * 100,
    meanExpectancyR,
    trimmedMeanExpectancyR,
    p25ExpectancyR,
    medianExpectancyR,
    worstNetR: Number.isFinite(worstNetR) ? worstNetR : 0,
    worstMaxDrawdownR,
    topWeekPnlConcentrationPct: topPositiveNetConcentrationPct(netRows),
    totalNetR,
    evaluatedAtMs: params.nowMs,
  };
}

export async function runDiscoverPipelineJob(
  params: {
    dryRun?: boolean;
    includeLiveQuotes?: boolean;
    maxCandidates?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock("discover", async ({ lockToken, lockMs }) => {
    const dryRun = Boolean(params.dryRun);
    const nowMs = Date.now();
    const bitgetOnly = isScalpPipelineBitgetOnlyEnabled();
    const snapshot = await runScalpSymbolDiscoveryCycle({
      dryRun,
      includeLiveQuotes: params.includeLiveQuotes ?? true,
      nowMs,
      maxCandidatesOverride: params.maxCandidates,
      seedTopSymbols: 12,
      seedTargetHistoryDays: 95,
      seedMaxHistoryDays: 110,
      seedChunkDays: 5,
      seedMaxRequestsPerSymbol: 30,
      seedMaxSymbolsPerRun: 12,
      seedTimeframe: "1m",
      seedOnDryRun: false,
      seedAllowBootstrapSymbols: true,
      sourceOverrides: bitgetOnly
        ? {
            includeCapitalMarketsApi: false,
            includeBitgetMarketsApi: true,
            includeDeploymentSymbols: false,
            includeHistorySymbols: false,
            requireHistoryPresence: false,
          }
        : undefined,
    });
    const discoveredActiveSymbols = Array.from(
      new Set(
        (snapshot.selectedSymbols || [])
          .map((row) => normalizeSymbol(row))
          .filter(Boolean),
      ),
    );
    const discoveredCatalogSymbols = Array.from(
      new Set(
        [
          ...(snapshot.selectedSymbols || []),
          ...((snapshot.selectedRows || []).map((row) => row.symbol) || []),
          ...((snapshot.topRejectedRows || []).map((row) => row.symbol) || []),
        ]
          .map((row) => normalizeSymbol(row))
          .filter(Boolean),
      ),
    );
    const db = scalpPrisma();
    const activeSymbols = Array.from(
      new Set(
        bitgetOnly
          ? discoveredActiveSymbols.filter((symbol) => isBitgetPipelineSymbol(symbol))
          : discoveredActiveSymbols,
      ),
    );
    const catalogSymbols = Array.from(
      new Set(
        (bitgetOnly
          ? discoveredCatalogSymbols.filter((symbol) => isBitgetPipelineSymbol(symbol))
          : discoveredCatalogSymbols
        ).concat(activeSymbols),
      ),
    );
    const droppedNonBitgetSymbols = bitgetOnly
      ? discoveredCatalogSymbols.filter((symbol) => !catalogSymbols.includes(symbol))
      : [];

    const existingRows = await db.$queryRaw<
      Array<{
        symbol: string;
        active: boolean;
        loadStatus: string;
        prepareStatus: string;
      }>
    >(Prisma.sql`
            SELECT
                symbol,
                active,
                load_status AS "loadStatus",
                prepare_status AS "prepareStatus"
            FROM scalp_pipeline_symbols;
        `);
    const existingBySymbol = new Map(
      existingRows.map((row) => [normalizeSymbol(row.symbol), row]),
    );
    const syncPlan = buildDiscoverSymbolSyncPlan({
      existingRows,
      activeSymbols,
      catalogSymbols,
    });
    const activeSet = new Set(syncPlan.activeSymbols);
    const addedActiveSymbols = syncPlan.addedActiveSymbols;
    const removedActiveSymbols = syncPlan.removedActiveSymbols;
    const catalogAddedSymbols = syncPlan.catalogAddedSymbols;

    if (!dryRun) {
      for (const symbol of catalogSymbols) {
        const existing = existingBySymbol.get(symbol) || null;
        const previouslyActive = Boolean(existing?.active);
        const isActive = activeSet.has(symbol);
        const activatedNow = isActive && !previouslyActive;
        const fallbackLoadStatus = isActive
          ? activatedNow
            ? "pending"
            : "succeeded"
          : "succeeded";
        const fallbackPrepareStatus = isActive
          ? activatedNow
            ? "pending"
            : "succeeded"
          : "succeeded";
        const fallbackLoadNextRunAt = activatedNow ? new Date(nowMs) : null;
        const fallbackPrepareNextRunAt = activatedNow ? new Date(nowMs) : null;
        await db.$executeRaw(Prisma.sql`
                INSERT INTO scalp_pipeline_symbols(
                    symbol,
                    active,
                    discover_status,
                    discover_attempts,
                    discover_next_run_at,
                    discover_error,
                    last_discovered_at,
                    load_status,
                    load_next_run_at,
                    prepare_status,
                    prepare_next_run_at,
                    updated_at
                )
                VALUES(
                    ${symbol},
                    ${isActive},
                    'succeeded',
                    0,
                    NULL,
                    NULL,
                    NOW(),
                    ${fallbackLoadStatus},
                    ${fallbackLoadNextRunAt},
                    ${fallbackPrepareStatus},
                    ${fallbackPrepareNextRunAt},
                    NOW()
                )
                ON CONFLICT(symbol)
                DO UPDATE SET
                    active = ${isActive},
                    discover_status = 'succeeded',
                    discover_attempts = 0,
                    discover_next_run_at = NULL,
                    discover_error = NULL,
                    last_discovered_at = NOW(),
                    load_status = CASE
                        WHEN ${isActive} = FALSE THEN scalp_pipeline_symbols.load_status
                        WHEN scalp_pipeline_symbols.load_status IN ('pending', 'running', 'retry_wait') THEN scalp_pipeline_symbols.load_status
                        ELSE ${fallbackLoadStatus}
                    END,
                    load_next_run_at = CASE
                        WHEN ${isActive} = FALSE THEN scalp_pipeline_symbols.load_next_run_at
                        WHEN scalp_pipeline_symbols.load_status IN ('pending', 'running', 'retry_wait') THEN scalp_pipeline_symbols.load_next_run_at
                        ELSE ${fallbackLoadNextRunAt}
                    END,
                    prepare_status = CASE
                        WHEN ${isActive} = FALSE THEN scalp_pipeline_symbols.prepare_status
                        WHEN scalp_pipeline_symbols.prepare_status IN ('pending', 'running', 'retry_wait') THEN scalp_pipeline_symbols.prepare_status
                        ELSE ${fallbackPrepareStatus}
                    END,
                    prepare_next_run_at = CASE
                        WHEN ${isActive} = FALSE THEN scalp_pipeline_symbols.prepare_next_run_at
                        WHEN scalp_pipeline_symbols.prepare_status IN ('pending', 'running', 'retry_wait') THEN scalp_pipeline_symbols.prepare_next_run_at
                        ELSE ${fallbackPrepareNextRunAt}
                    END,
                    updated_at = NOW();
            `);
      }

      if (removedActiveSymbols.length > 0) {
        await db.$executeRaw(Prisma.sql`
                UPDATE scalp_pipeline_symbols
                SET
                    active = FALSE,
                    updated_at = NOW()
                WHERE active = TRUE
                  AND symbol IN (${Prisma.join(removedActiveSymbols)});
            `);
      }

      if (removedActiveSymbols.length > 0) {
        await db.$executeRaw(Prisma.sql`
                UPDATE scalp_deployments
                SET
                    in_universe = FALSE,
                    retired_at = NOW(),
                    worker_dirty = FALSE,
                    promotion_dirty = FALSE,
                    updated_by = 'pipeline:discover',
                    updated_at = NOW()
                WHERE symbol IN (${Prisma.join(removedActiveSymbols)})
                  AND enabled = FALSE;
            `);
      }
    }

    const pendingAfter = await countPendingLoadSymbols();
    await pulsePipelineJobProgress({
      jobKind: "discover",
      lockToken,
      lockMs,
      progressLabel: `selected ${activeSymbols.length}`,
      progress: {
        dryRun,
        selected: activeSymbols.length,
        activeSymbols: activeSymbols.length,
        catalogSymbols: catalogSymbols.length,
        added: addedActiveSymbols.length,
        removed: removedActiveSymbols.length,
        wouldRemove: removedActiveSymbols.length,
        addedActiveSymbols: addedActiveSymbols.length,
        removedActiveSymbols: removedActiveSymbols.length,
        catalogAddedSymbols: catalogAddedSymbols.length,
        droppedNonBitget: droppedNonBitgetSymbols.length,
        pendingLoad: pendingAfter,
      },
    });

    return {
      ok: true,
      processed: activeSymbols.length,
      succeeded: activeSymbols.length,
      retried: 0,
      failed: 0,
      pendingAfter,
      downstreamRequested: pendingAfter > 0 || activeSymbols.length > 0,
      progressLabel: `selected ${activeSymbols.length}`,
      details: {
        dryRun,
        generatedAtIso: snapshot.generatedAtIso,
        selected: activeSymbols.length,
        addedSymbols: addedActiveSymbols,
        removedSymbols: removedActiveSymbols,
        wouldRemoveSymbols: removedActiveSymbols,
        activeSymbols,
        addedActiveSymbols,
        removedActiveSymbols,
        catalogAddedSymbols,
        catalogSymbols,
        droppedNonBitgetSymbols,
        candidatesEvaluated: snapshot.candidatesEvaluated,
      },
    };
  });
}

async function claimLoadSymbols(
  limit: number,
  berlinWeekStartMs: number,
): Promise<Array<{ symbol: string; attempts: number }>> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ symbol: string; attempts: number }>
  >(Prisma.sql`
        WITH due_enabled_symbols AS (
            SELECT DISTINCT d.symbol
            FROM scalp_deployments d
            WHERE d.enabled = TRUE
              AND COALESCE((d.promotion_gate #>> '{lifecycle,lastRolloverBerlinWeekStartMs}')::bigint, 0) < ${berlinWeekStartMs}
        ),
        candidate AS (
            SELECT symbol
            FROM scalp_pipeline_symbols
            WHERE (
                    active = TRUE
                    OR symbol IN (SELECT symbol FROM due_enabled_symbols)
                  )
              AND load_status IN ('pending', 'retry_wait')
              AND COALESCE(load_next_run_at, NOW()) <= NOW()
            ORDER BY COALESCE(load_next_run_at, NOW()) ASC, symbol ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ${limit}
        )
        UPDATE scalp_pipeline_symbols s
        SET
            load_status = 'running',
            load_attempts = s.load_attempts + 1,
            load_error = NULL,
            updated_at = NOW()
        FROM candidate c
        WHERE s.symbol = c.symbol
        RETURNING s.symbol, s.load_attempts AS attempts;
    `);
  return rows;
}

async function ensureLoadQueueSymbolsForDueIncumbents(
  berlinWeekStartMs: number,
): Promise<{ symbols: string[]; queued: number }> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ symbol: string }>>(Prisma.sql`
        SELECT DISTINCT d.symbol
        FROM scalp_deployments d
        WHERE d.enabled = TRUE
          AND COALESCE((d.promotion_gate #>> '{lifecycle,lastRolloverBerlinWeekStartMs}')::bigint, 0) < ${berlinWeekStartMs}
        ORDER BY d.symbol ASC
        LIMIT 2000;
    `);
  const symbols = rows
    .map((row) => normalizeSymbol(row.symbol))
    .filter(Boolean);
  if (!symbols.length) return { symbols: [], queued: 0 };
  let queued = 0;
  for (const symbol of symbols) {
    queued += Number(
      await db.$executeRaw(Prisma.sql`
            INSERT INTO scalp_pipeline_symbols(
                symbol,
                active,
                discover_status,
                discover_attempts,
                discover_next_run_at,
                discover_error,
                last_discovered_at,
                load_status,
                load_attempts,
                load_next_run_at,
                load_error,
                prepare_status,
                prepare_attempts,
                prepare_next_run_at,
                prepare_error,
                updated_at
            )
            VALUES(
                ${symbol},
                FALSE,
                'succeeded',
                0,
                NULL,
                NULL,
                NOW(),
                'pending',
                0,
                NOW(),
                NULL,
                'succeeded',
                0,
                NULL,
                NULL,
                NOW()
            )
            ON CONFLICT(symbol)
            DO UPDATE SET
                load_status = CASE
                    WHEN scalp_pipeline_symbols.load_status = 'running' THEN scalp_pipeline_symbols.load_status
                    ELSE 'pending'
                END,
                load_next_run_at = CASE
                    WHEN scalp_pipeline_symbols.load_status = 'running' THEN scalp_pipeline_symbols.load_next_run_at
                    ELSE NOW()
                END,
                load_error = CASE
                    WHEN scalp_pipeline_symbols.load_status = 'running' THEN scalp_pipeline_symbols.load_error
                    ELSE NULL
                END,
                updated_at = NOW();
        `),
    );
  }
  return { symbols, queued };
}

async function updateLoadSymbolStatus(params: {
  symbol: string;
  status: ScalpPipelineQueueStatus;
  attempts: number;
  error?: string | null;
  retryAfterMs?: number;
  weeksCovered?: number;
  latestWeekStartMs?: number | null;
  markPreparePending?: boolean;
}): Promise<void> {
  const db = scalpPrisma();
  const nextRunAt =
    typeof params.retryAfterMs === "number" && params.retryAfterMs > 0
      ? new Date(Date.now() + params.retryAfterMs)
      : null;
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_pipeline_symbols
        SET
            load_status = ${params.status},
            load_attempts = ${params.attempts},
            load_next_run_at = ${nextRunAt},
            load_error = ${params.error || null},
            weeks_covered = ${typeof params.weeksCovered === "number" ? params.weeksCovered : 0},
            latest_week_start = ${params.latestWeekStartMs ? new Date(params.latestWeekStartMs) : null},
            last_loaded_at = CASE WHEN ${params.status === "succeeded"} THEN NOW() ELSE last_loaded_at END,
            prepare_status = CASE
                WHEN ${params.markPreparePending === true} THEN 'pending'
                ELSE prepare_status
            END,
            prepare_next_run_at = CASE
                WHEN ${params.markPreparePending === true} THEN NOW()
                ELSE prepare_next_run_at
            END,
            prepare_error = CASE
                WHEN ${params.markPreparePending === true} THEN NULL
                ELSE prepare_error
            END,
            updated_at = NOW()
        WHERE symbol = ${params.symbol};
    `);
}

async function ensureSymbolWeeklyCoverage(params: {
  symbol: string;
  nowMs: number;
  requiredWeeks: number;
  maxRequestsPerSymbol: number;
}): Promise<{
  ok: boolean;
  weeksCovered: number;
  latestWeekStartMs: number | null;
  existingCount: number;
  fetchedCount: number;
  addedCount: number;
  error: string | null;
}> {
  const history = await loadScalpCandleHistory(params.symbol, "1m");
  const existing = history.record?.candles || [];
  const bitgetOnly = isScalpPipelineBitgetOnlyEnabled();
  if (bitgetOnly && !isBitgetPipelineSymbol(params.symbol)) {
    const coverage = countCoveredCompletedWeeks(
      existing,
      params.nowMs,
      params.requiredWeeks,
    );
    return {
      ok: false,
      weeksCovered: coverage.covered,
      latestWeekStartMs: coverage.latestWeekStartMs,
      existingCount: existing.length,
      fetchedCount: 0,
      addedCount: 0,
      error: "bitget_only_symbol_unsupported",
    };
  }
  const earliestMissingWeekStartMs = findEarliestMissingCompletedWeekStartMs(
    existing,
    params.nowMs,
    params.requiredWeeks,
  );
  const seedTfMs = Math.max(ONE_MINUTE_MS, timeframeToMs("1m"));
  const requiredCoverageStartMs = resolveCompletedWeekCoverageStartMs(
    params.nowMs,
    params.requiredWeeks,
  );
  const coverage = countCoveredCompletedWeeks(
    existing,
    params.nowMs,
    params.requiredWeeks,
  );
  const latestExistingTs = existing.length
    ? Number(existing[existing.length - 1]?.[0] || 0)
    : 0;
  const hasCoverage =
    earliestMissingWeekStartMs === null && coverage.covered >= params.requiredWeeks;

  if (hasCoverage) {
    return {
      ok: true,
      weeksCovered: coverage.covered,
      latestWeekStartMs: coverage.latestWeekStartMs,
      existingCount: existing.length,
      fetchedCount: 0,
      addedCount: 0,
      error: null,
    };
  }

  const marketMetadata = bitgetOnly
    ? null
    : await ensureScalpSymbolMarketMetadata(params.symbol, {
        fetchIfMissing: true,
      });
  const marketSource: "capital" | "bitget" = bitgetOnly
    ? "bitget"
    : marketMetadata?.source === "bitget"
      ? "bitget"
      : "capital";
  const epic = bitgetOnly
    ? params.symbol
    : marketMetadata?.epic ||
      (marketSource === "bitget"
        ? params.symbol
        : (await resolveCapitalEpicRuntime(params.symbol)).epic);

  const incrementalFetchFromMs = (() => {
    if (earliestMissingWeekStartMs !== null) {
      return Math.max(earliestMissingWeekStartMs, requiredCoverageStartMs);
    }
    if (
      existing.length > 0 &&
      Number.isFinite(latestExistingTs) &&
      latestExistingTs > 0
    ) {
      return Math.max(
        requiredCoverageStartMs,
        Math.floor(latestExistingTs - seedTfMs * 2),
      );
    }
    return requiredCoverageStartMs;
  })();
  const fetchToMs = params.nowMs;

  const fetchedRaw =
    marketSource === "bitget"
      ? await fetchBitgetCandlesByEpicDateRange(
          epic,
          "1m",
          incrementalFetchFromMs,
          fetchToMs,
          {
            maxPerRequest: BITGET_HISTORY_CANDLES_MAX_LIMIT,
            maxRequests: params.maxRequestsPerSymbol,
          },
        )
      : await fetchCapitalCandlesByEpicDateRange(
          epic,
          "1m",
          incrementalFetchFromMs,
          fetchToMs,
          {
            maxPerRequest: 1000,
            maxRequests: params.maxRequestsPerSymbol,
            debug: false,
            debugLabel: `pipeline-load:${params.symbol}:1m`,
          },
        );
  const fetched = normalizeFetchedCandles(fetchedRaw);
  const merged = mergeScalpCandleHistory(existing, fetched);

  if (merged.length > existing.length) {
    await saveScalpCandleHistory({
      symbol: params.symbol,
      timeframe: "1m",
      epic,
      source: marketSource,
      candles: merged,
    });
  }

  const mergedMissing = findEarliestMissingCompletedWeekStartMs(
    merged,
    params.nowMs,
    params.requiredWeeks,
  );
  const mergedCoverage = countCoveredCompletedWeeks(
    merged,
    params.nowMs,
    params.requiredWeeks,
  );
  const mergedHasCoverage =
    mergedMissing === null && mergedCoverage.covered >= params.requiredWeeks;

  return {
    ok: mergedHasCoverage,
    weeksCovered: mergedCoverage.covered,
    latestWeekStartMs: mergedCoverage.latestWeekStartMs,
    existingCount: existing.length,
    fetchedCount: fetched.length,
    addedCount: Math.max(0, merged.length - existing.length),
    error: mergedHasCoverage ? null : "insufficient_completed_week_coverage",
  };
}

export async function runLoadCandlesPipelineJob(
  params: {
    batchSize?: number;
    maxAttempts?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock(
    "load_candles",
    async ({ lockToken, lockMs }) => {
      const berlinWeekStartMs = startOfBerlinWeekMonday(Date.now());
      const requiredWeeks = resolveRequiredSuccessiveWeeks();
      const batchSize = Math.max(
        1,
        Math.min(40, toPositiveInt(params.batchSize, 6)),
      );
      const maxAttempts = Math.max(
        1,
        Math.min(20, toPositiveInt(params.maxAttempts, 5)),
      );
      const retryAfterMs = Math.max(
        5 * 60_000,
        Math.min(
          3 * 60 * 60_000,
          toPositiveInt(process.env.SCALP_PIPELINE_LOAD_RETRY_MS, 15 * 60_000),
        ),
      );
      const maxRequestsPerSymbol = Math.max(
        40,
        Math.min(
              2500,
              toPositiveInt(
                process.env.SCALP_PIPELINE_LOAD_MAX_REQUESTS_PER_SYMBOL,
                600,
              ),
            ),
          );

      const dueIncumbents = await ensureLoadQueueSymbolsForDueIncumbents(
        berlinWeekStartMs,
      );
      const claimed = await claimLoadSymbols(batchSize, berlinWeekStartMs);
      const activeSymbols = await countActivePipelineSymbols();
      let succeeded = 0;
      let retried = 0;
      let failed = 0;

      for (let idx = 0; idx < claimed.length; idx += 1) {
        const row = claimed[idx]!;
        const symbol = normalizeSymbol(row.symbol);
        if (!symbol) continue;
        try {
          const coverage = await ensureSymbolWeeklyCoverage({
            symbol,
            nowMs: Date.now(),
            requiredWeeks,
            maxRequestsPerSymbol,
          });
          if (coverage.ok) {
            await updateLoadSymbolStatus({
              symbol,
              status: "succeeded",
              attempts: row.attempts,
              error: null,
              weeksCovered: coverage.weeksCovered,
              latestWeekStartMs: coverage.latestWeekStartMs,
              markPreparePending: true,
            });
            succeeded += 1;
          } else if (row.attempts >= maxAttempts) {
            await updateLoadSymbolStatus({
              symbol,
              status: "failed",
              attempts: row.attempts,
              error: coverage.error,
              weeksCovered: coverage.weeksCovered,
              latestWeekStartMs: coverage.latestWeekStartMs,
            });
            failed += 1;
          } else {
            await updateLoadSymbolStatus({
              symbol,
              status: "retry_wait",
              attempts: row.attempts,
              error: coverage.error,
              retryAfterMs,
              weeksCovered: coverage.weeksCovered,
              latestWeekStartMs: coverage.latestWeekStartMs,
            });
            retried += 1;
          }
        } catch (err: any) {
          const message = String(
            err?.message || err || "load_symbol_failed",
          ).slice(0, 500);
          if (row.attempts >= maxAttempts) {
            await updateLoadSymbolStatus({
              symbol,
              status: "failed",
              attempts: row.attempts,
              error: message,
            });
            failed += 1;
          } else {
            await updateLoadSymbolStatus({
              symbol,
              status: "retry_wait",
              attempts: row.attempts,
              error: message,
              retryAfterMs,
            });
            retried += 1;
          }
        }
        await pulsePipelineJobProgress({
          jobKind: "load_candles",
          lockToken,
          lockMs,
          progressLabel: `processed ${idx + 1}/${claimed.length}`,
          progress: {
            processed: idx + 1,
            total: claimed.length,
            activeSymbols,
            dueIncumbentSymbolsQueued: dueIncumbents.symbols.length,
            succeeded,
            retried,
            failed,
          },
        });
      }

      const pendingAfter = await countPendingLoadSymbols();
      return {
        ok: true,
        processed: claimed.length,
        succeeded,
        retried,
        failed,
        pendingAfter,
        downstreamRequested: succeeded > 0,
        progressLabel:
          claimed.length > 0 ? `processed ${claimed.length}` : "idle",
        details: {
          requiredWeeks,
          activeSymbols,
          dueIncumbentSymbolsQueued: dueIncumbents.symbols.length,
          claimed: claimed.length,
          succeeded,
          retried,
          failed,
        },
      };
    },
  );
}

async function claimPrepareSymbols(
  limit: number,
): Promise<Array<{ symbol: string; attempts: number }>> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ symbol: string; attempts: number }>
  >(Prisma.sql`
        WITH candidate AS (
            SELECT symbol
            FROM scalp_pipeline_symbols
            WHERE active = TRUE
              AND load_status = 'succeeded'
              AND prepare_status IN ('pending', 'retry_wait')
              AND COALESCE(prepare_next_run_at, NOW()) <= NOW()
            ORDER BY COALESCE(prepare_next_run_at, NOW()) ASC, symbol ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ${limit}
        )
        UPDATE scalp_pipeline_symbols s
        SET
            prepare_status = 'running',
            prepare_attempts = s.prepare_attempts + 1,
            prepare_error = NULL,
            updated_at = NOW()
        FROM candidate c
        WHERE s.symbol = c.symbol
        RETURNING s.symbol, s.prepare_attempts AS attempts;
    `);
  return rows;
}

async function upsertWeeklyQueueRowsForDeployment(params: {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  nowMs: number;
  requiredWeeks: number;
  refreshRecentWeeks: number;
}): Promise<number> {
  const currentWeekStart = startOfWeekMondayUtc(params.nowMs);
  const firstWeekStart = currentWeekStart - params.requiredWeeks * ONE_WEEK_MS;
  const refreshStart =
    currentWeekStart - Math.max(1, params.refreshRecentWeeks) * ONE_WEEK_MS;
  const db = scalpPrisma();
  let upserted = 0;
  for (
    let weekStartMs = firstWeekStart;
    weekStartMs < currentWeekStart;
    weekStartMs += ONE_WEEK_MS
  ) {
    const weekEndMs = weekStartMs + ONE_WEEK_MS;
    await db.$executeRaw(Prisma.sql`
            INSERT INTO scalp_deployment_weekly_metrics(
                deployment_id,
                symbol,
                strategy_id,
                tune_id,
                week_start,
                week_end,
                status,
                attempts,
                next_run_at,
                created_at,
                updated_at
            )
            VALUES(
                ${params.deploymentId},
                ${params.symbol},
                ${params.strategyId},
                ${params.tuneId},
                ${new Date(weekStartMs)},
                ${new Date(weekEndMs)},
                'pending',
                0,
                NOW(),
                NOW(),
                NOW()
            )
            ON CONFLICT(deployment_id, week_start)
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                strategy_id = EXCLUDED.strategy_id,
                tune_id = EXCLUDED.tune_id,
                week_end = EXCLUDED.week_end,
                status = CASE
                    WHEN scalp_deployment_weekly_metrics.status IN ('pending', 'running', 'retry_wait') THEN scalp_deployment_weekly_metrics.status
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)}
                        AND scalp_deployment_weekly_metrics.status IN ('failed')
                    THEN 'pending'
                    ELSE scalp_deployment_weekly_metrics.status
                END,
                attempts = CASE
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)}
                        AND scalp_deployment_weekly_metrics.status IN ('failed')
                    THEN 0
                    ELSE scalp_deployment_weekly_metrics.attempts
                END,
                next_run_at = CASE
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)}
                        AND scalp_deployment_weekly_metrics.status IN ('failed')
                    THEN NOW()
                    ELSE scalp_deployment_weekly_metrics.next_run_at
                END,
                error_code = CASE
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)}
                        AND scalp_deployment_weekly_metrics.status IN ('failed')
                    THEN NULL
                    ELSE scalp_deployment_weekly_metrics.error_code
                END,
                error_message = CASE
                    WHEN EXCLUDED.week_start >= ${new Date(refreshStart)}
                        AND scalp_deployment_weekly_metrics.status IN ('failed')
                    THEN NULL
                    ELSE scalp_deployment_weekly_metrics.error_message
                END,
                updated_at = NOW();
        `);
    upserted += 1;
  }
  return upserted;
}

async function ensureWeeklyQueueRowsExistForDeployment(params: {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  nowMs: number;
  requiredWeeks: number;
}): Promise<number> {
  const currentWeekStart = startOfWeekMondayUtc(params.nowMs);
  const firstWeekStart = currentWeekStart - params.requiredWeeks * ONE_WEEK_MS;
  const db = scalpPrisma();
  let inserted = 0;
  for (
    let weekStartMs = firstWeekStart;
    weekStartMs < currentWeekStart;
    weekStartMs += ONE_WEEK_MS
  ) {
    const weekEndMs = weekStartMs + ONE_WEEK_MS;
    const insertedRows = Number(
      await db.$executeRaw(Prisma.sql`
            INSERT INTO scalp_deployment_weekly_metrics(
                deployment_id,
                symbol,
                strategy_id,
                tune_id,
                week_start,
                week_end,
                status,
                attempts,
                next_run_at,
                created_at,
                updated_at
            )
            VALUES(
                ${params.deploymentId},
                ${params.symbol},
                ${params.strategyId},
                ${params.tuneId},
                ${new Date(weekStartMs)},
                ${new Date(weekEndMs)},
                'pending',
                0,
                NOW(),
                NOW(),
                NOW()
            )
            ON CONFLICT(deployment_id, week_start)
            DO NOTHING;
        `),
    );
    inserted += Math.max(0, insertedRows);
  }
  return inserted;
}

async function updatePrepareSymbolStatus(params: {
  symbol: string;
  status: ScalpPipelineQueueStatus;
  attempts: number;
  preparedDeployments: number;
  error?: string | null;
  retryAfterMs?: number;
}): Promise<void> {
  const db = scalpPrisma();
  const nextRunAt =
    typeof params.retryAfterMs === "number" && params.retryAfterMs > 0
      ? new Date(Date.now() + params.retryAfterMs)
      : null;
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_pipeline_symbols
        SET
            prepare_status = ${params.status},
            prepare_attempts = ${params.attempts},
            prepare_next_run_at = ${nextRunAt},
            prepare_error = ${params.error || null},
            prepared_deployments = ${Math.max(0, Math.floor(params.preparedDeployments))},
            last_prepared_at = CASE WHEN ${params.status === "succeeded"} THEN NOW() ELSE last_prepared_at END,
            updated_at = NOW()
        WHERE symbol = ${params.symbol};
    `);
}

export async function runPreparePipelineJob(
  params: {
    batchSize?: number;
    maxAttempts?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock("prepare", async ({ lockToken, lockMs }) => {
    const bitgetOnly = isScalpPipelineBitgetOnlyEnabled();
    const batchSize = Math.max(
      1,
      Math.min(30, toPositiveInt(params.batchSize, 4)),
    );
    const maxAttempts = Math.max(
      1,
      Math.min(20, toPositiveInt(params.maxAttempts, 5)),
    );
    const retryAfterMs = Math.max(
      5 * 60_000,
      Math.min(
        3 * 60 * 60_000,
        toPositiveInt(process.env.SCALP_PIPELINE_PREPARE_RETRY_MS, 10 * 60_000),
      ),
    );
    const requiredWeeks = resolveRequiredSuccessiveWeeks();
    const refreshRecentWeeks = Math.max(
      1,
      Math.min(
        8,
        toPositiveInt(process.env.SCALP_PIPELINE_WORKER_REFRESH_WEEKS, 2),
      ),
    );
    const nowMs = Date.now();

    const policy = await loadScalpSymbolDiscoveryPolicy();
    const strategies = new Set(listScalpStrategies().map((row) => row.id));
    const claimed = await claimPrepareSymbols(batchSize);
    const activeSymbols = await countActivePipelineSymbols();
    const db = scalpPrisma();

    let succeeded = 0;
    let retried = 0;
    let failed = 0;
    let queuedWeeklyRows = 0;

    for (let idx = 0; idx < claimed.length; idx += 1) {
      const row = claimed[idx]!;
      const symbol = normalizeSymbol(row.symbol);
      if (!symbol) continue;
      if (bitgetOnly && !isBitgetPipelineSymbol(symbol)) {
        await db.$executeRaw(Prisma.sql`
                    UPDATE scalp_deployments
                    SET
                        in_universe = FALSE,
                        retired_at = NOW(),
                        worker_dirty = FALSE,
                        promotion_dirty = FALSE,
                        updated_by = 'pipeline:prepare',
                        updated_at = NOW()
                    WHERE symbol = ${symbol}
                      AND enabled = FALSE;
                `);
        await updatePrepareSymbolStatus({
          symbol,
          status: "failed",
          attempts: row.attempts,
          preparedDeployments: 0,
          error: "bitget_only_symbol_unsupported",
        });
        failed += 1;
        await pulsePipelineJobProgress({
          jobKind: "prepare",
          lockToken,
          lockMs,
          progressLabel: `processed ${idx + 1}/${claimed.length}`,
          progress: {
            processed: idx + 1,
            total: claimed.length,
            activeSymbols,
            succeeded,
            retried,
            failed,
            queuedWeeklyRows,
          },
        });
        continue;
      }
      try {
        const strategyIds = resolveRecommendedStrategiesForSymbol(
          symbol,
          policy.strategyAllowlist,
        ).filter((id) => strategies.has(id));
        const selectedStrategies =
          strategyIds.length > 0
            ? strategyIds
            : Array.from(strategies).slice(0, 1);
        const symbolVenue = await resolvePipelineDeploymentVenue(symbol);
        const existingDeployments = await db.$queryRaw<
          Array<{
            deploymentId: string;
            strategyId: string;
            tuneId: string;
            enabled: boolean;
            promotionGate: unknown;
          }>
        >(Prisma.sql`
                    SELECT
                        deployment_id AS "deploymentId",
                        strategy_id AS "strategyId",
                        tune_id AS "tuneId",
                        enabled,
                        promotion_gate AS "promotionGate"
                    FROM scalp_deployments
                    WHERE symbol = ${symbol};
                `);
        const existingByKey = new Map<
          string,
          {
            deploymentId: string;
            enabled: boolean;
            promotionGate: unknown;
          }
        >();
        for (const dep of existingDeployments) {
          const key = `${dep.strategyId}::${dep.tuneId}`;
          if (existingByKey.has(key)) continue;
          const depVenue = resolveScalpDeploymentVenueFromId(dep.deploymentId);
          if (depVenue !== symbolVenue) continue;
          existingByKey.set(key, dep);
        }
        const preparedIds: string[] = [];

        for (const strategyId of selectedStrategies) {
          const variants = buildScalpResearchTuneVariants({
            symbol,
            strategyId,
            includeBaseline: true,
            includeSessionProfileVariants: false,
            maxVariantsPerStrategy: 4,
          }).slice(0, 4);
          const rows = variants
            .map((variant) => {
              const existing =
                existingByKey.get(`${strategyId}::${variant.tuneId}`) || null;
              const existingLifecycle = existing
                ? normalizeLifecycleFromGate({
                    gate: asJsonObject(existing.promotionGate) as any,
                    tuneId: variant.tuneId,
                    enabled: Boolean(existing.enabled),
                    nowMs,
                  })
                : null;
              if (existingLifecycle && lifecycleIsSuppressed(existingLifecycle, nowMs)) {
                return null;
              }
              return {
                deploymentId:
                  existing?.deploymentId ||
                  resolveScalpDeployment({
                    venue: symbolVenue,
                    symbol,
                    strategyId,
                    tuneId: variant.tuneId,
                  }).deploymentId,
                symbol,
                strategyId,
                tuneId: variant.tuneId,
                source: "matrix" as const,
                enabled: Boolean(existing?.enabled),
                configOverride: withBerlinEntrySessionProfile(
                  (variant.configOverride || null) as Record<string, unknown> | null,
                ),
                updatedBy: "pipeline:prepare",
              };
            })
            .filter(
              (
                row,
              ): row is {
                deploymentId: string;
                symbol: string;
                strategyId: string;
                tuneId: string;
                source: "matrix";
                enabled: boolean;
                configOverride: Record<string, unknown>;
                updatedBy: string;
              } => Boolean(row),
            );
          if (!rows.length) continue;
          const upserted = await upsertScalpDeploymentRegistryEntriesBulk(rows);
          for (const entry of upserted.entries) {
            const lifecycle = normalizeLifecycleFromGate({
              gate: entry.promotionGate,
              tuneId: entry.tuneId,
              enabled: entry.enabled,
              nowMs,
            });
            if (!entry.enabled || lifecycle.state === "incumbent_refresh") {
              preparedIds.push(entry.deploymentId);
            }
          }
        }

        const uniqPreparedIds = Array.from(new Set(preparedIds));
        if (uniqPreparedIds.length > 0) {
          await db.$executeRaw(Prisma.sql`
                        UPDATE scalp_deployments
                        SET
                            in_universe = TRUE,
                            retired_at = NULL,
                            worker_dirty = TRUE,
                            updated_by = 'pipeline:prepare',
                            last_prepared_at = NOW(),
                            updated_at = NOW()
                        WHERE deployment_id IN (${Prisma.join(uniqPreparedIds)});
                    `);
          await db.$executeRaw(Prisma.sql`
                        UPDATE scalp_deployments
                        SET
                            in_universe = FALSE,
                            retired_at = NOW(),
                            worker_dirty = FALSE,
                            promotion_dirty = FALSE,
                            updated_by = 'pipeline:prepare',
                            updated_at = NOW()
                        WHERE symbol = ${symbol}
                          AND deployment_id NOT IN (${Prisma.join(uniqPreparedIds)})
                          AND enabled = FALSE;
                    `);
        }

        for (const deploymentId of uniqPreparedIds) {
          const depRows = await db.$queryRaw<
            Array<{
              deploymentId: string;
              symbol: string;
              strategyId: string;
              tuneId: string;
            }>
          >(Prisma.sql`
                        SELECT
                            deployment_id AS "deploymentId",
                            symbol,
                            strategy_id AS "strategyId",
                            tune_id AS "tuneId"
                        FROM scalp_deployments
                        WHERE deployment_id = ${deploymentId}
                        LIMIT 1;
                    `);
          const dep = depRows[0];
          if (!dep) continue;
          queuedWeeklyRows += await upsertWeeklyQueueRowsForDeployment({
            deploymentId: dep.deploymentId,
            symbol: dep.symbol,
            strategyId: dep.strategyId,
            tuneId: dep.tuneId,
            nowMs: Date.now(),
            requiredWeeks,
            refreshRecentWeeks,
          });
        }

        await updatePrepareSymbolStatus({
          symbol,
          status: "succeeded",
          attempts: row.attempts,
          preparedDeployments: uniqPreparedIds.length,
          error: null,
        });
        succeeded += 1;
      } catch (err: any) {
        const message = String(
          err?.message || err || "prepare_symbol_failed",
        ).slice(0, 500);
        if (row.attempts >= maxAttempts) {
          await updatePrepareSymbolStatus({
            symbol,
            status: "failed",
            attempts: row.attempts,
            preparedDeployments: 0,
            error: message,
          });
          failed += 1;
        } else {
          await updatePrepareSymbolStatus({
            symbol,
            status: "retry_wait",
            attempts: row.attempts,
            preparedDeployments: 0,
            error: message,
            retryAfterMs,
          });
          retried += 1;
        }
      }
      await pulsePipelineJobProgress({
        jobKind: "prepare",
        lockToken,
        lockMs,
        progressLabel: `processed ${idx + 1}/${claimed.length}`,
        progress: {
          processed: idx + 1,
          total: claimed.length,
          activeSymbols,
          succeeded,
          retried,
          failed,
          queuedWeeklyRows,
        },
      });
    }

    const pendingAfter = await countPendingPrepareSymbols();
    return {
      ok: true,
      processed: claimed.length,
      succeeded,
      retried,
      failed,
      pendingAfter,
      downstreamRequested: queuedWeeklyRows > 0,
      progressLabel:
        claimed.length > 0 ? `processed ${claimed.length}` : "idle",
      details: {
        activeSymbols,
        claimed: claimed.length,
        succeeded,
        retried,
        failed,
        queuedWeeklyRows,
      },
    };
  });
}

async function claimWorkerRows(
  limit: number,
  workerId: string,
): Promise<
  Array<{
    id: bigint;
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    weekStart: Date;
    weekEnd: Date;
    attempts: number;
  }>
> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      id: bigint;
      deploymentId: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      weekStart: Date;
      weekEnd: Date;
      attempts: number;
    }>
  >(Prisma.sql`
        WITH candidate AS (
            SELECT id
            FROM scalp_deployment_weekly_metrics m
            INNER JOIN scalp_deployments d
              ON d.deployment_id = m.deployment_id
            WHERE d.in_universe = TRUE
              AND m.status IN ('pending', 'retry_wait')
              AND m.next_run_at <= NOW()
            ORDER BY m.next_run_at ASC, m.week_start ASC, m.id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ${limit}
        )
        UPDATE scalp_deployment_weekly_metrics m
        SET
            status = 'running',
            attempts = m.attempts + 1,
            worker_id = ${workerId},
            started_at = NOW(),
            finished_at = NULL,
            error_code = NULL,
            error_message = NULL,
            updated_at = NOW()
        FROM candidate c
        WHERE m.id = c.id
        RETURNING
            m.id,
            m.deployment_id AS "deploymentId",
            m.symbol,
            m.strategy_id AS "strategyId",
            m.tune_id AS "tuneId",
            m.week_start AS "weekStart",
            m.week_end AS "weekEnd",
            m.attempts;
    `);
  return rows;
}

async function completeWorkerRow(params: {
  id: bigint;
  workerId: string;
  success: boolean;
  retry: boolean;
  retryAfterMs?: number;
  errorCode?: string | null;
  errorMessage?: string | null;
  metrics?: {
    trades: number;
    winRatePct: number;
    netR: number;
    expectancyR: number;
    profitFactor: number | null;
    maxDrawdownR: number;
    avgHoldMinutes: number;
    netPnlUsd: number;
    grossProfitR: number;
    grossLossR: number;
  };
}): Promise<void> {
  const db = scalpPrisma();
  const status = params.success
    ? "succeeded"
    : params.retry
      ? "retry_wait"
      : "failed";
  const nextRunAt =
    params.retry && params.retryAfterMs
      ? new Date(Date.now() + params.retryAfterMs)
      : null;
  const metricsJson = params.metrics ? JSON.stringify(params.metrics) : null;
  await db.$executeRaw(Prisma.sql`
        UPDATE scalp_deployment_weekly_metrics
        SET
            status = ${status},
            next_run_at = COALESCE(${nextRunAt}, next_run_at),
            worker_id = NULL,
            finished_at = NOW(),
            error_code = ${params.errorCode || null},
            error_message = ${params.errorMessage || null},
            trades = ${params.metrics?.trades ?? null},
            win_rate_pct = ${params.metrics?.winRatePct ?? null},
            net_r = ${params.metrics?.netR ?? null},
            expectancy_r = ${params.metrics?.expectancyR ?? null},
            profit_factor = ${params.metrics?.profitFactor ?? null},
            max_drawdown_r = ${params.metrics?.maxDrawdownR ?? null},
            avg_hold_minutes = ${params.metrics?.avgHoldMinutes ?? null},
            net_pnl_usd = ${params.metrics?.netPnlUsd ?? null},
            gross_profit_r = ${params.metrics?.grossProfitR ?? null},
            gross_loss_r = ${params.metrics?.grossLossR ?? null},
            metrics_json = ${metricsJson}::jsonb,
            updated_at = NOW()
        WHERE id = ${params.id}
          AND (worker_id = ${params.workerId} OR worker_id IS NULL);
    `);
}

export async function runWorkerPipelineJob(
  params: {
    batchSize?: number;
    maxAttempts?: number;
    minCandlesPerWeek?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock("worker", async ({ lockToken, lockMs }) => {
    const workerId = lockToken;
    const requestedBatchSize = Math.max(
      1,
      Math.min(400, toPositiveInt(params.batchSize, 80)),
    );
    const batchSize = Math.min(requestedBatchSize, resolveWorkerMaxClaimPerRun());
    const maxAttempts = Math.max(
      1,
      Math.min(20, toPositiveInt(params.maxAttempts, 5)),
    );
    const retryAfterMs = Math.max(
      5 * 60_000,
      Math.min(
        3 * 60 * 60_000,
        toPositiveInt(process.env.SCALP_PIPELINE_WORKER_RETRY_MS, 15 * 60_000),
      ),
    );
    const minCandlesPerWeek = Math.max(
      60,
      Math.min(20_000, toPositiveInt(params.minCandlesPerWeek, 180)),
    );
    const rowTimeoutMs = resolveWorkerRowTimeoutMs(lockMs);
    const replayProgressEveryRuns = resolveWorkerReplayProgressEveryRuns();
    const replayProgressMinIntervalMs = resolveWorkerReplayProgressMinIntervalMs();

    const claimed = await claimWorkerRows(batchSize, workerId);
    const activeSymbols = await countActivePipelineSymbols();
    let succeeded = 0;
    let retried = 0;
    let failed = 0;
    const db = scalpPrisma();

    for (let idx = 0; idx < claimed.length; idx += 1) {
      const row = claimed[idx]!;
      try {
        const depRows = await db.$queryRaw<
          Array<{
            deploymentId: string;
            symbol: string;
            strategyId: string;
            tuneId: string;
            configOverride: unknown;
          }>
        >(Prisma.sql`
                    SELECT
                        deployment_id AS "deploymentId",
                        symbol,
                        strategy_id AS "strategyId",
                        tune_id AS "tuneId",
                        config_override AS "configOverride"
                    FROM scalp_deployments
                    WHERE deployment_id = ${row.deploymentId}
                    LIMIT 1;
                `);
        const dep = depRows[0];
        if (!dep) {
          await completeWorkerRow({
            id: row.id,
            workerId,
            success: false,
            retry: false,
            errorCode: "deployment_missing",
            errorMessage: "deployment_missing",
          });
          failed += 1;
          continue;
        }
        const history = await loadScalpCandleHistory(dep.symbol, "1m");
        const candles = (history.record?.candles || []).filter((candle) => {
          const ts = Number(candle?.[0] || 0);
          return ts >= row.weekStart.getTime() && ts < row.weekEnd.getTime();
        });
        if (candles.length < minCandlesPerWeek) {
          const retry = row.attempts < maxAttempts;
          await completeWorkerRow({
            id: row.id,
            workerId,
            success: false,
            retry,
            retryAfterMs,
            errorCode: "insufficient_weekly_candles",
            errorMessage: `insufficient_weekly_candles:${candles.length}`,
          });
          if (retry) retried += 1;
          else failed += 1;
          continue;
        }

        const meta = await ensureScalpSymbolMarketMetadata(dep.symbol, {
          fetchIfMissing: true,
        });
        const deploymentRef = resolveScalpDeployment({
          symbol: dep.symbol,
          strategyId: dep.strategyId,
          tuneId: dep.tuneId,
          deploymentId: dep.deploymentId,
        });
        const runtime = buildScalpReplayRuntimeFromDeployment({
          deployment: deploymentRef,
          configOverride: asJsonObject(dep.configOverride) as any,
        });
        const rowStartedAtMs = Date.now();
        const replay = await runScalpReplay({
          candles: toReplayCandles(candles, runtime.defaultSpreadPips),
          pipSize: pipSizeForScalpSymbol(dep.symbol, meta),
          config: runtime,
          captureTimeline: false,
          symbolMeta: meta,
          progress: {
            everyRuns: replayProgressEveryRuns,
            minIntervalMs: replayProgressMinIntervalMs,
            onProgress: (progress) => {
              if (progress.elapsedMs > rowTimeoutMs) {
                throw new WorkerRowTimeoutError(
                  `worker_row_timeout:${dep.deploymentId}:${progress.elapsedMs}`,
                  rowTimeoutMs,
                );
              }
            },
          },
        });
        const replayElapsedMs = Date.now() - rowStartedAtMs;
        if (replayElapsedMs > rowTimeoutMs) {
          throw new WorkerRowTimeoutError(
            `worker_row_timeout:${dep.deploymentId}:${replayElapsedMs}`,
            rowTimeoutMs,
          );
        }

        const metrics = {
          trades: replay.summary.trades,
          winRatePct: replay.summary.winRatePct,
          netR: replay.summary.netR,
          expectancyR: replay.summary.expectancyR,
          profitFactor: replay.summary.profitFactor,
          maxDrawdownR: replay.summary.maxDrawdownR,
          avgHoldMinutes: replay.summary.avgHoldMinutes,
          netPnlUsd: replay.summary.netPnlUsd,
          grossProfitR: replay.summary.grossProfitR,
          grossLossR: replay.summary.grossLossR,
        };
        await completeWorkerRow({
          id: row.id,
          workerId,
          success: true,
          retry: false,
          metrics,
        });
        await db.$executeRaw(Prisma.sql`
                    UPDATE scalp_deployments
                    SET
                        promotion_dirty = TRUE,
                        worker_dirty = FALSE,
                        updated_by = 'pipeline:worker',
                        updated_at = NOW()
                    WHERE deployment_id = ${dep.deploymentId};
                `);
        succeeded += 1;
      } catch (err: any) {
        const message = String(
          err?.message || err || "worker_row_failed",
        ).slice(0, 500);
        const timeoutError = err instanceof WorkerRowTimeoutError;
        const retry = row.attempts < maxAttempts;
        await completeWorkerRow({
          id: row.id,
          workerId,
          success: false,
          retry,
          retryAfterMs,
          errorCode: timeoutError ? "worker_row_timeout" : "worker_replay_failed",
          errorMessage: message,
        });
        if (retry) retried += 1;
        else failed += 1;
      }

      await pulsePipelineJobProgress({
        jobKind: "worker",
        lockToken,
        lockMs,
        progressLabel: `processed ${idx + 1}/${claimed.length}`,
        progress: {
          processed: idx + 1,
          total: claimed.length,
          requestedBatchSize,
          effectiveBatchSize: batchSize,
          rowTimeoutMs,
          activeSymbols,
          succeeded,
          retried,
          failed,
        },
      });
    }

    const pendingAfter = await countPendingWorkerRows();
    return {
      ok: true,
      processed: claimed.length,
      succeeded,
      retried,
      failed,
      pendingAfter,
      downstreamRequested: succeeded > 0,
      progressLabel:
        claimed.length > 0 ? `processed ${claimed.length}` : "idle",
      details: {
        activeSymbols,
        claimed: claimed.length,
        requestedBatchSize,
        effectiveBatchSize: batchSize,
        rowTimeoutMs,
        succeeded,
        retried,
        failed,
      },
    };
  });
}

export async function runPromotionPipelineJob(
  params: {
    batchSize?: number;
  } = {},
): Promise<ScalpPipelineJobExecutionResult> {
  return runWithPipelineJobLock("promotion", async ({ lockToken, lockMs }) => {
    const batchSize = Math.max(
      1,
      Math.min(600, toPositiveInt(params.batchSize, 200)),
    );
    const policy = resolveWeeklyPolicyDefaults();
    const explorationShare = resolvePromotionExplorationShare();
    const hysteresisFailThreshold = resolvePromotionHysteresisFailThreshold();
    const hysteresisPassThreshold = resolvePromotionHysteresisPassThreshold();
    const maxLoadNudgeSymbols = resolvePromotionLoadNudgeMaxSymbols();
    const maxWorkerNudgeDeployments =
      resolvePromotionWorkerNudgeMaxDeployments();
    const nowMs = Date.now();
    const berlinWeekStartMs = startOfBerlinWeekMonday(nowMs);
    const requiredWeeks = resolvePromotionFreshWeeks();
    const windowToTs = startOfWeekMondayUtc(nowMs);
    const windowFromTs = windowToTs - policy.lookbackDays * ONE_DAY_MS;
    const previousWeekStartTs = windowToTs - ONE_WEEK_MS;

    const db = scalpPrisma();
    const rolloverDueRows = await db.$queryRaw<
      Array<{
        deploymentId: string;
        symbol: string;
        strategyId: string;
        tuneId: string;
      }>
    >(Prisma.sql`
            SELECT
                deployment_id AS "deploymentId",
                symbol,
                strategy_id AS "strategyId",
                tune_id AS "tuneId"
            FROM scalp_deployments
            WHERE enabled = TRUE
              AND COALESCE((promotion_gate #>> '{lifecycle,lastRolloverBerlinWeekStartMs}')::bigint, 0) < ${berlinWeekStartMs}
            ORDER BY updated_at ASC, deployment_id ASC
            LIMIT 5000;
        `);
    const rolloverDueIds = rolloverDueRows.map((row) => row.deploymentId);
    let rolloverIncumbentsQueued = 0;
    if (rolloverDueIds.length > 0) {
      rolloverIncumbentsQueued = rolloverDueIds.length;
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_deployments
                SET
                    in_universe = TRUE,
                    promotion_dirty = TRUE,
                    updated_by = 'pipeline:promotion',
                    updated_at = NOW()
                WHERE deployment_id IN (${Prisma.join(rolloverDueIds)});
            `);
      for (const row of rolloverDueRows) {
        await ensureWeeklyQueueRowsExistForDeployment({
          deploymentId: row.deploymentId,
          symbol: row.symbol,
          strategyId: row.strategyId,
          tuneId: row.tuneId,
          nowMs,
          requiredWeeks,
        });
        await db.$executeRaw(Prisma.sql`
                    UPDATE scalp_deployment_weekly_metrics
                    SET
                        status = 'pending',
                        attempts = 0,
                        next_run_at = NOW(),
                        error_code = NULL,
                        error_message = NULL,
                        updated_at = NOW()
                    WHERE deployment_id = ${row.deploymentId}
                      AND week_start = ${new Date(previousWeekStartTs)}
                      AND status IN ('pending', 'retry_wait', 'failed');
                `);
      }
    }
    const dirtyRows = await db.$queryRaw<
      Array<{ deploymentId: string }>
    >(Prisma.sql`
            SELECT d.deployment_id AS "deploymentId"
            FROM scalp_deployments d
            WHERE (
                    d.in_universe = TRUE
                    AND (
                        d.promotion_dirty = TRUE
                        OR (
                            d.enabled = TRUE
                            AND COALESCE((d.promotion_gate #>> '{freshness,windowToTs}')::bigint, 0) < ${windowToTs}
                        )
                    )
                  )
               OR (
                    COALESCE((d.promotion_gate #>> '{lifecycle,state}')::text, '') = 'suspended'
                    AND COALESCE((d.promotion_gate #>> '{lifecycle,suspendedUntilMs}')::bigint, 0) > 0
                    AND COALESCE((d.promotion_gate #>> '{lifecycle,suspendedUntilMs}')::bigint, 0) <= ${nowMs}
                  )
               OR (
                    COALESCE((d.promotion_gate #>> '{lifecycle,state}')::text, '') = 'retired'
                    AND COALESCE((d.promotion_gate #>> '{lifecycle,retiredUntilMs}')::bigint, 0) > 0
                    AND COALESCE((d.promotion_gate #>> '{lifecycle,retiredUntilMs}')::bigint, 0) <= ${nowMs}
                  )
               OR (
                    d.enabled = TRUE
                    AND EXISTS (
                        SELECT 1
                        FROM scalp_deployments d2
                        WHERE d2.symbol = d.symbol
                          AND d2.strategy_id = d.strategy_id
                          AND d2.enabled = TRUE
                          AND d2.deployment_id <> d.deployment_id
                    )
                  )
            ORDER BY
                CASE
                    WHEN d.promotion_dirty = TRUE THEN 0
                    WHEN d.enabled = TRUE THEN 1
                    ELSE 2
                END ASC,
                d.updated_at ASC,
                d.deployment_id ASC
            LIMIT ${batchSize};
        `);
    if (!dirtyRows.length) {
      return {
        ok: true,
        processed: 0,
        succeeded: 0,
        retried: 0,
        failed: 0,
        pendingAfter: 0,
        downstreamRequested: false,
        progressLabel: "idle",
        details: {
          reason: "no_promotion_dirty_deployments",
          rolloverIncumbentsQueued,
        },
      };
    }

    const dirtySet = new Set(dirtyRows.map((row) => row.deploymentId));
    const allDeployments = await listScalpDeploymentRegistryEntries();
    const inUniverseRows = await db.$queryRaw<
      Array<{
        deploymentId: string;
        inUniverse: boolean;
        enabled: boolean;
        promotionGate: unknown;
      }>
    >(Prisma.sql`
            SELECT
                deployment_id AS "deploymentId",
                in_universe AS "inUniverse",
                enabled,
                promotion_gate AS "promotionGate"
            FROM scalp_deployments;
        `);
    const inUniverseByDeploymentId = new Map(
      inUniverseRows.map((row) => [row.deploymentId, row]),
    );

    const rolloverDueSet = new Set(rolloverDueIds);
    const consideredDeployments = allDeployments.filter((row) => {
      const inUniverse =
        inUniverseByDeploymentId.get(row.deploymentId)?.inUniverse === true;
      const currentlyEnabled =
        inUniverseByDeploymentId.get(row.deploymentId)?.enabled === true ||
        row.enabled === true;
      return (
        inUniverse ||
        dirtySet.has(row.deploymentId) ||
        rolloverDueSet.has(row.deploymentId) ||
        currentlyEnabled
      );
    });
    const consideredByDeploymentId = new Map(
      consideredDeployments.map((row) => [row.deploymentId, row]),
    );
    const consideredIds = consideredDeployments.map((row) => row.deploymentId);
    if (!consideredIds.length) {
      return {
        ok: true,
        processed: dirtyRows.length,
        succeeded: dirtyRows.length,
        retried: 0,
        failed: 0,
        pendingAfter: 0,
        downstreamRequested: false,
        progressLabel: "idle",
        details: {
          reason: "no_in_universe_deployments",
        },
      };
    }

    let weeklyQueueRowsInserted = 0;
    const dirtyDeployments = consideredDeployments.filter((row) =>
      dirtySet.has(row.deploymentId),
    );
    for (const deployment of dirtyDeployments) {
      const rowState = inUniverseByDeploymentId.get(deployment.deploymentId);
      const currentlyEnabled = Boolean(rowState?.enabled ?? deployment.enabled);
      const lifecycle = normalizeLifecycleFromGate({
        gate:
          (asJsonObject(rowState?.promotionGate) as ScalpDeploymentPromotionGate | null) ||
          deployment.promotionGate,
        tuneId: deployment.tuneId,
        enabled: currentlyEnabled,
        nowMs,
      });
      if (lifecycleIsSuppressed(lifecycle, nowMs)) continue;
      if (currentlyEnabled && lifecycle.state !== "incumbent_refresh") continue;
      weeklyQueueRowsInserted += await ensureWeeklyQueueRowsExistForDeployment({
        deploymentId: deployment.deploymentId,
        symbol: deployment.symbol,
        strategyId: deployment.strategyId,
        tuneId: deployment.tuneId,
        nowMs,
        requiredWeeks,
      });
    }

    const metricsRows = await db.$queryRaw<
      Array<{
        deploymentId: string;
        symbol: string;
        strategyId: string;
        tuneId: string;
        weekStart: Date;
        weekEnd: Date;
        trades: number | null;
        winRatePct: number | null;
        netR: number | null;
        expectancyR: number | null;
        profitFactor: number | null;
        maxDrawdownR: number | null;
        avgHoldMinutes: number | null;
        netPnlUsd: number | null;
        grossProfitR: number | null;
        grossLossR: number | null;
      }>
    >(Prisma.sql`
            SELECT
                deployment_id AS "deploymentId",
                symbol,
                strategy_id AS "strategyId",
                tune_id AS "tuneId",
                week_start AS "weekStart",
                week_end AS "weekEnd",
                trades,
                win_rate_pct AS "winRatePct",
                net_r AS "netR",
                expectancy_r AS "expectancyR",
                profit_factor AS "profitFactor",
                max_drawdown_r AS "maxDrawdownR",
                avg_hold_minutes AS "avgHoldMinutes",
                net_pnl_usd AS "netPnlUsd",
                gross_profit_r AS "grossProfitR",
                gross_loss_r AS "grossLossR"
            FROM scalp_deployment_weekly_metrics
            WHERE deployment_id IN (${Prisma.join(consideredIds)})
              AND status = 'succeeded'
              AND week_start >= ${new Date(windowFromTs)}
              AND week_start < ${new Date(windowToTs)}
            ORDER BY week_start ASC;
        `);

    const tasksByDeploymentId = new Map<string, Array<any>>();
    for (const row of metricsRows) {
      const bucket = tasksByDeploymentId.get(row.deploymentId) || [];
      bucket.push({
        version: 1,
        cycleId: "pipeline",
        taskId: `${row.deploymentId}:${row.weekStart.getTime()}`,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        deploymentId: row.deploymentId,
        windowFromTs: row.weekStart.getTime(),
        windowToTs: row.weekEnd.getTime(),
        status: "completed",
        attempts: 1,
        createdAtMs: row.weekStart.getTime(),
        updatedAtMs: nowMs,
        workerId: null,
        startedAtMs: null,
        finishedAtMs: null,
        errorCode: null,
        errorMessage: null,
        result: {
          symbol: row.symbol,
          strategyId: row.strategyId,
          tuneId: row.tuneId,
          deploymentId: row.deploymentId,
          windowFromTs: row.weekStart.getTime(),
          windowToTs: row.weekEnd.getTime(),
          trades: Math.max(0, Math.floor(Number(row.trades || 0))),
          winRatePct: toFiniteNumber(row.winRatePct, 0),
          netR: toFiniteNumber(row.netR, 0),
          expectancyR: toFiniteNumber(row.expectancyR, 0),
          profitFactor:
            row.profitFactor === null || row.profitFactor === undefined
              ? null
              : toFiniteNumber(row.profitFactor, Number.NaN),
          maxDrawdownR: Math.max(0, toFiniteNumber(row.maxDrawdownR, 0)),
          avgHoldMinutes: toFiniteNumber(row.avgHoldMinutes, 0),
          netPnlUsd: toFiniteNumber(row.netPnlUsd, 0),
          grossProfitR: toFiniteNumber(row.grossProfitR, 0),
          grossLossR: toFiniteNumber(row.grossLossR, 0),
        },
      });
      tasksByDeploymentId.set(row.deploymentId, bucket);
    }

    const freshReadyTasks: any[] = [];
    const freshnessByDeploymentId = new Map<
      string,
      ReturnType<typeof evaluateFreshCompletedDeploymentWeeks>
    >();
    const gapSymbols = new Set<string>();
    const gapMissingWeeksBySymbol = new Map<string, number>();
    const gapWindowsByDeploymentId = new Map<
      string,
      {
        windowFromTs: number;
        windowToTs: number;
        missingWeekStarts: number[];
        missingWeeks: number;
      }
    >();
    for (const deployment of consideredDeployments) {
      const deploymentTasks =
        tasksByDeploymentId.get(deployment.deploymentId) || [];
      const freshness = evaluateFreshCompletedDeploymentWeeks({
        tasks: deploymentTasks,
        nowMs,
        requiredWeeks,
      });
      freshnessByDeploymentId.set(deployment.deploymentId, freshness);
      if (freshness.ready) {
        for (const task of freshness.readyTasks) {
          freshReadyTasks.push(task);
        }
      } else if (freshness.missingWeeks > 0) {
        const symbol = normalizeSymbol(deployment.symbol);
        if (symbol) {
          gapSymbols.add(symbol);
          gapMissingWeeksBySymbol.set(
            symbol,
            Math.max(
              gapMissingWeeksBySymbol.get(symbol) || 0,
              freshness.missingWeeks,
            ),
          );
        }
        gapWindowsByDeploymentId.set(deployment.deploymentId, {
          windowFromTs: freshness.windowFromTs,
          windowToTs: freshness.windowToTs,
          missingWeekStarts: freshness.missingWeekStarts || [],
          missingWeeks: freshness.missingWeeks,
        });
      }
    }

    let nudgedLoadSymbols = 0;
    let nudgedWorkerRows = 0;
    const loadSymbolsToNudge = Array.from(gapSymbols)
      .sort((a, b) => {
        const aMissing = gapMissingWeeksBySymbol.get(a) || 0;
        const bMissing = gapMissingWeeksBySymbol.get(b) || 0;
        if (bMissing !== aMissing) return bMissing - aMissing;
        return a.localeCompare(b);
      })
      .slice(0, maxLoadNudgeSymbols);
    if (loadSymbolsToNudge.length > 0) {
      nudgedLoadSymbols = Number(
        await db.$executeRaw(Prisma.sql`
                UPDATE scalp_pipeline_symbols
                SET
                    load_status = CASE
                        WHEN load_status = 'running' THEN load_status
                        ELSE 'pending'
                    END,
                    load_next_run_at = CASE
                        WHEN load_status = 'running' THEN load_next_run_at
                        ELSE NOW()
                    END,
                    load_error = CASE
                        WHEN load_status = 'running' THEN load_error
                        ELSE NULL
                    END,
                    updated_at = NOW()
                WHERE symbol IN (${Prisma.join(loadSymbolsToNudge)});
            `),
      );
    }
    const workerDeploymentsToNudge = Array.from(gapWindowsByDeploymentId.entries())
      .sort((a, b) => {
        if (b[1].missingWeeks !== a[1].missingWeeks) {
          return b[1].missingWeeks - a[1].missingWeeks;
        }
        if (a[1].windowFromTs !== b[1].windowFromTs) {
          return a[1].windowFromTs - b[1].windowFromTs;
        }
        return a[0].localeCompare(b[0]);
      })
      .map(([deploymentId]) => deploymentId)
      .slice(0, maxWorkerNudgeDeployments);
    if (workerDeploymentsToNudge.length > 0) {
      for (const deploymentId of workerDeploymentsToNudge) {
        const gapWindow = gapWindowsByDeploymentId.get(deploymentId);
        if (!gapWindow) continue;
        nudgedWorkerRows += Number(
          await db.$executeRaw(Prisma.sql`
                    UPDATE scalp_deployment_weekly_metrics
                    SET
                        status = 'pending',
                        next_run_at = NOW(),
                        error_code = NULL,
                        error_message = NULL,
                        updated_at = NOW()
                    WHERE deployment_id = ${deploymentId}
                      AND week_start >= ${new Date(gapWindow.windowFromTs)}
                      AND week_start < ${new Date(gapWindow.windowToTs)}
                      AND status IN ('pending', 'retry_wait', 'failed');
                `),
        );
      }
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_deployments
                SET
                    worker_dirty = TRUE,
                    updated_by = 'pipeline:promotion',
                    updated_at = NOW()
                WHERE deployment_id IN (${Prisma.join(workerDeploymentsToNudge)});
            `);
    }

    const candidates = buildForwardValidationByCandidateFromTasks({
      tasks: freshReadyTasks,
      selectionWindowDays: policy.lookbackDays,
      forwardWindowDays: 7,
    });
    const candidateByKey = new Map(
      candidates.map((row) => [
        `${row.symbol}::${row.strategyId}::${row.tuneId}`,
        row,
      ]),
    );

    const weeklyByKey = new Map<string, ScalpWeeklyRobustnessMetrics | null>();
    const weeklyGateReasonByKey = new Map<string, string | null>();
    const weeklyTasksByCandidateKey = new Map<
      string,
      Array<{ netR: number; expectancyR: number; maxDrawdownR: number }>
    >();
    for (const task of freshReadyTasks) {
      const symbol = String(task.symbol || "").trim().toUpperCase();
      const strategyId = String(task.strategyId || "")
        .trim()
        .toLowerCase();
      const tuneId = String(task.tuneId || "")
        .trim()
        .toLowerCase();
      if (!symbol || !strategyId || !tuneId) continue;
      const key = `${symbol}::${strategyId}::${tuneId}`;
      const bucket = weeklyTasksByCandidateKey.get(key) || [];
      bucket.push({
        netR: toFiniteNumber(task.result?.netR, 0),
        expectancyR: toFiniteNumber(task.result?.expectancyR, 0),
        maxDrawdownR: toFiniteNumber(task.result?.maxDrawdownR, 0),
      });
      weeklyTasksByCandidateKey.set(key, bucket);
    }

    for (const candidate of candidates) {
      const key = `${candidate.symbol}::${candidate.strategyId}::${candidate.tuneId}`;
      const weeklyMetrics = computeWeeklyRobustnessFromTasks({
        tasks: weeklyTasksByCandidateKey.get(key) || [],
        nowMs,
      });
      weeklyByKey.set(key, weeklyMetrics);
      const weeklyGate = evaluateWeeklyRobustnessGate(weeklyMetrics, policy);
      weeklyGateReasonByKey.set(
        key,
        weeklyGate.passed
          ? null
          : weeklyGate.reason || "weekly_robustness_failed",
      );

      candidate.forwardValidation.weeklySlices = weeklyMetrics?.slices ?? null;
      candidate.forwardValidation.weeklyProfitablePct =
        weeklyMetrics?.profitablePct ?? null;
      candidate.forwardValidation.weeklyMeanExpectancyR =
        weeklyMetrics?.meanExpectancyR ?? null;
      candidate.forwardValidation.weeklyTrimmedMeanExpectancyR =
        weeklyMetrics?.trimmedMeanExpectancyR ?? null;
      candidate.forwardValidation.weeklyP25ExpectancyR =
        weeklyMetrics?.p25ExpectancyR ?? null;
      candidate.forwardValidation.weeklyMedianExpectancyR =
        weeklyMetrics?.medianExpectancyR ?? null;
      candidate.forwardValidation.weeklyWorstNetR =
        weeklyMetrics?.worstNetR ?? null;
      candidate.forwardValidation.weeklyTopWeekPnlConcentrationPct =
        weeklyMetrics?.topWeekPnlConcentrationPct ?? null;
      candidate.forwardValidation.weeklyEvaluatedAtMs = nowMs;
    }

    const tempDeploymentsForWinners = consideredDeployments.map(
      (deployment) => {
        const key = `${deployment.symbol}::${deployment.strategyId}::${deployment.tuneId}`;
        const candidate = candidateByKey.get(key) || null;
        const currentlyEnabled = Boolean(
          inUniverseByDeploymentId.get(deployment.deploymentId)?.enabled,
        );
        const baseLifecycle = normalizeLifecycleFromGate({
          gate:
            (asJsonObject(
              inUniverseByDeploymentId.get(deployment.deploymentId)?.promotionGate || null,
            ) as ScalpDeploymentPromotionGate | null) || deployment.promotionGate,
          tuneId: deployment.tuneId,
          enabled: currentlyEnabled,
          nowMs,
        });
        const lifecycle =
          currentlyEnabled && rolloverDueSet.has(deployment.deploymentId)
            ? {
                ...baseLifecycle,
                state: "incumbent_refresh" as const,
                lastRolloverBerlinWeekStartMs: berlinWeekStartMs,
              }
            : baseLifecycle;
        const suppressed = lifecycleIsSuppressed(lifecycle, nowMs);
        const freshness = freshnessByDeploymentId.get(deployment.deploymentId);
        const freshnessState = freshness
          ? {
              requiredWeeks: freshness.requiredWeeks,
              completedWeeks: freshness.completedWeeks,
              missingWeeks: freshness.missingWeeks,
              windowFromTs: freshness.windowFromTs,
              windowToTs: freshness.windowToTs,
              missingWeekStarts: freshness.missingWeekStarts || [],
            }
          : null;
        const weeklyFailReason = weeklyGateReasonByKey.get(key) || null;
        const eligible = Boolean(
          !suppressed && candidate && freshness?.ready && !weeklyFailReason,
        );
        return {
          deploymentId: deployment.deploymentId,
          symbol: deployment.symbol,
          strategyId: deployment.strategyId,
          tuneId: deployment.tuneId,
          enabled: currentlyEnabled,
          promotionGate: {
            eligible,
            reason: eligible
              ? "weekly_robustness_passed"
              : suppressed
                ? lifecycle.state === "retired"
                  ? "retired_cooldown"
                  : "suspended_cooldown"
              : weeklyFailReason ||
                (!freshness?.ready
                  ? "fresh_weeks_incomplete"
                  : "candidate_missing"),
            source: "walk_forward",
            evaluatedAtMs: nowMs,
            forwardValidation: candidate?.forwardValidation || null,
            thresholds:
              (asJsonObject(
                inUniverseByDeploymentId.get(deployment.deploymentId)
                  ?.promotionGate || null,
              )?.thresholds as any) || null,
            freshness: freshnessState,
            lifecycle,
          } as ScalpDeploymentPromotionGate,
        };
      },
    );

    const strategyWinnerIds = buildBestEligibleTuneDeploymentIdSet({
      deployments: tempDeploymentsForWinners,
      candidates,
    });
    const strategyWinnerDeployments = tempDeploymentsForWinners.filter((row) =>
      strategyWinnerIds.has(row.deploymentId),
    );
    const globalWinnerIds = buildGlobalSymbolRankedDeploymentIdSet({
      deployments: strategyWinnerDeployments,
      candidates,
      maxSymbols: policy.globalMaxSymbols,
      maxPerSymbol: policy.topKPerSymbol,
      maxDeployments: policy.globalMaxDeployments,
    });
    const selectionRows: PromotionSelectionRow[] = strategyWinnerDeployments
      .map((row) => {
        const key = `${row.symbol}::${row.strategyId}::${row.tuneId}`;
        const candidate = candidateByKey.get(key) || null;
        if (!candidate) return null;
        return {
          deploymentId: row.deploymentId,
          symbol: row.symbol,
          incumbent: Boolean(
            inUniverseByDeploymentId.get(row.deploymentId)?.enabled,
          ),
          candidate,
        };
      })
      .filter((row): row is PromotionSelectionRow => Boolean(row));
    const explorationSelection = selectPromotionWinnerRowsWithExploration({
      rows: selectionRows,
      explorationShare,
      maxSymbols: policy.globalMaxSymbols,
      maxPerSymbol: policy.topKPerSymbol,
      maxDeployments: policy.globalMaxDeployments,
    });
    const explorationWinnerIds = explorationSelection.winnerIds;
    const winnerIds = policy.requireWinnerShortlist
      ? explorationWinnerIds
      : strategyWinnerIds;

    interface PromotionUpdateDraft {
      entry: ScalpDeploymentRegistryEntry;
      lifecycle: ScalpDeploymentPromotionLifecycle;
      inUniverseNext: boolean;
      exactLoser: boolean;
      currentlyEnabled: boolean;
      shortlistIncluded: boolean;
      forcedGraduatedNoSeat: boolean;
    }

    let enabledByHysteresis = 0;
    let disabledByHysteresis = 0;
    let disabledByUniqueness = 0;
    let suspendedExact = 0;
    let suspendedNeighbors = 0;
    let retiredCount = 0;
    const drafts: PromotionUpdateDraft[] = consideredDeployments.map((deployment) => {
      const key = `${deployment.symbol}::${deployment.strategyId}::${deployment.tuneId}`;
      const candidate = candidateByKey.get(key) || null;
      const freshness = freshnessByDeploymentId.get(deployment.deploymentId);
      const freshnessState = freshness
        ? {
            requiredWeeks: freshness.requiredWeeks,
            completedWeeks: freshness.completedWeeks,
            missingWeeks: freshness.missingWeeks,
            windowFromTs: freshness.windowFromTs,
            windowToTs: freshness.windowToTs,
            missingWeekStarts: freshness.missingWeekStarts || [],
          }
        : null;
      const rowState = inUniverseByDeploymentId.get(deployment.deploymentId);
      const currentlyEnabled = Boolean(rowState?.enabled ?? deployment.enabled);
      let lifecycle = normalizeLifecycleFromGate({
        gate:
          (asJsonObject(rowState?.promotionGate) as ScalpDeploymentPromotionGate | null) ||
          deployment.promotionGate,
        tuneId: deployment.tuneId,
        enabled: currentlyEnabled,
        nowMs,
      });
      if (currentlyEnabled && rolloverDueSet.has(deployment.deploymentId)) {
        lifecycle = {
          ...lifecycle,
          state: "incumbent_refresh",
          lastRolloverBerlinWeekStartMs: berlinWeekStartMs,
        };
      }
      const suppressed = lifecycleIsSuppressed(lifecycle, nowMs);
      const weeklyFailReason = weeklyGateReasonByKey.get(key) || null;
      const eligible = Boolean(
        !suppressed && candidate && freshness?.ready && !weeklyFailReason,
      );
      const shortlistIncluded = winnerIds.has(deployment.deploymentId);
      const reason = eligible
        ? shortlistIncluded
          ? "weekly_robustness_passed"
          : "winner_shortlist_excluded"
        : suppressed
          ? lifecycle.state === "retired"
            ? "retired_cooldown"
            : "suspended_cooldown"
          : weeklyFailReason ||
            (!freshness?.ready
              ? "fresh_weeks_incomplete"
              : "candidate_missing");
      const promotionGate: ScalpDeploymentPromotionGate = {
        eligible,
        reason,
        source: "walk_forward",
        evaluatedAtMs: nowMs,
        forwardValidation: candidate?.forwardValidation || null,
        thresholds: deployment.promotionGate?.thresholds || null,
        freshness: freshnessState,
        hysteresis: null,
        lifecycle,
      };
      const shouldEnableNow = eligible && shortlistIncluded;
      const hysteresis = applyPromotionHysteresis({
        currentlyEnabled,
        shouldEnableNow,
        previous: deployment.promotionGate?.hysteresis || null,
        passThreshold: hysteresisPassThreshold,
        failThreshold: hysteresisFailThreshold,
        nowMs,
        lockEnabled: false,
      });
      promotionGate.hysteresis = hysteresis.hysteresis;
      if (hysteresis.transition === "enabled") enabledByHysteresis += 1;
      if (hysteresis.transition === "disabled") disabledByHysteresis += 1;

      const exactLoser = Boolean(
        !hysteresis.enabled &&
          !suppressed &&
          freshness?.ready &&
          weeklyFailReason,
      );
      if (exactLoser) {
        const nextLifecycle = applyLifecycleSuspension({
          lifecycle,
          nowMs,
          durationMs: LIFECYCLE_SUSPEND_EXACT_MS,
        });
        if (nextLifecycle.state === "retired") {
          if (lifecycle.state !== "retired") retiredCount += 1;
          promotionGate.reason = "retired_cooldown";
        } else {
          suspendedExact += 1;
          promotionGate.reason = "suspended_exact_loser";
        }
        lifecycle = nextLifecycle;
        promotionGate.lifecycle = lifecycle;
      }

      if (hysteresis.enabled && lifecycle.state !== "incumbent_refresh") {
        lifecycle.state = "graduated";
      } else if (
        !hysteresis.enabled &&
        lifecycle.state !== "suspended" &&
        lifecycle.state !== "retired"
      ) {
        lifecycle.state = "candidate";
      }

      promotionGate.lifecycle = lifecycle;
      return {
        entry: {
          ...deployment,
          enabled: hysteresis.enabled,
          promotionGate,
          updatedAtMs: nowMs,
          updatedBy: "pipeline:promotion",
        },
        lifecycle,
        inUniverseNext: Boolean(rowState?.inUniverse),
        exactLoser,
        currentlyEnabled,
        shortlistIncluded,
        forcedGraduatedNoSeat: false,
      };
    });

    const neighborSuspended = new Set<string>();
    for (const loser of drafts.filter((row) => row.exactLoser)) {
      const loserFamily =
        loser.lifecycle.tuneFamily || resolveLifecycleTuneFamily(loser.entry.tuneId);
      for (const candidate of drafts) {
        if (candidate.entry.deploymentId === loser.entry.deploymentId) continue;
        if (candidate.entry.enabled) continue;
        if (neighborSuspended.has(candidate.entry.deploymentId)) continue;
        if (candidate.entry.symbol !== loser.entry.symbol) continue;
        if (candidate.entry.strategyId !== loser.entry.strategyId) continue;
        const candidateFamily =
          candidate.lifecycle.tuneFamily ||
          resolveLifecycleTuneFamily(candidate.entry.tuneId);
        if (candidateFamily !== loserFamily) continue;
        if (lifecycleIsSuppressed(candidate.lifecycle, nowMs)) continue;
        const nextLifecycle = applyLifecycleSuspension({
          lifecycle: candidate.lifecycle,
          nowMs,
          durationMs: LIFECYCLE_SUSPEND_NEIGHBOR_MS,
        });
        if (nextLifecycle.state === "retired") {
          if (candidate.lifecycle.state !== "retired") retiredCount += 1;
          candidate.entry.promotionGate = {
            ...(candidate.entry.promotionGate || ({} as ScalpDeploymentPromotionGate)),
            reason: "retired_cooldown",
            lifecycle: nextLifecycle,
          };
        } else {
          suspendedNeighbors += 1;
          candidate.entry.promotionGate = {
            ...(candidate.entry.promotionGate || ({} as ScalpDeploymentPromotionGate)),
            reason: "suspended_neighbor_family",
            lifecycle: nextLifecycle,
          };
        }
        candidate.lifecycle = nextLifecycle;
        neighborSuspended.add(candidate.entry.deploymentId);
      }
    }

    const uniquenessResolution = enforceSingleEnabledPerSymbolStrategy({
      rows: drafts.map((row) => {
        const key = `${row.entry.symbol}::${row.entry.strategyId}::${row.entry.tuneId}`;
        return {
          deploymentId: row.entry.deploymentId,
          symbol: row.entry.symbol,
          strategyId: row.entry.strategyId,
          tuneId: row.entry.tuneId,
          enabled: row.entry.enabled,
          shortlistIncluded: row.shortlistIncluded,
          candidate: candidateByKey.get(key) || null,
          promotionGate: row.entry.promotionGate || null,
        };
      }),
    });
    if (uniquenessResolution.demotedIds.size > 0) {
      for (const row of drafts) {
        if (!uniquenessResolution.demotedIds.has(row.entry.deploymentId)) continue;
        if (!row.entry.enabled) continue;
        row.entry.enabled = false;
        row.shortlistIncluded = false;
        row.forcedGraduatedNoSeat = true;
        const previousHysteresis = row.entry.promotionGate?.hysteresis || null;
        const nextHysteresis: ScalpDeploymentPromotionHysteresis = {
          passStreak: 0,
          failStreak: Math.max(
            hysteresisFailThreshold,
            Math.floor(Number(previousHysteresis?.failStreak) || 0),
          ),
          lastStateChangeAtMs: nowMs,
          lastDecision: "disable",
        };
        if (!row.entry.promotionGate) {
          row.entry.promotionGate = {
            eligible: false,
            reason: "symbol_strategy_uniqueness_demoted",
            source: "walk_forward",
            evaluatedAtMs: nowMs,
            forwardValidation: null,
            thresholds: null,
            freshness: null,
            hysteresis: nextHysteresis,
            lifecycle: row.lifecycle,
          };
        } else {
          row.entry.promotionGate.reason = "symbol_strategy_uniqueness_demoted";
          row.entry.promotionGate.hysteresis = nextHysteresis;
          row.entry.promotionGate.lifecycle = row.lifecycle;
        }
        row.lifecycle.state = "graduated";
        if (row.entry.promotionGate) {
          row.entry.promotionGate.lifecycle = row.lifecycle;
        }
        disabledByUniqueness += 1;
      }
    }

    for (const row of drafts) {
      const freshness = freshnessByDeploymentId.get(row.entry.deploymentId);
      const lifecycle = row.lifecycle;
      if (lifecycleIsSuppressed(lifecycle, nowMs)) {
        row.inUniverseNext = false;
      } else if (row.entry.enabled) {
        if (lifecycle.state === "incumbent_refresh") {
          if (freshness?.ready) {
            lifecycle.state = "graduated";
            lifecycle.lastSeatReleaseAtMs = nowMs;
            row.inUniverseNext = false;
          } else {
            row.inUniverseNext = true;
          }
        } else {
          lifecycle.state = "graduated";
          row.inUniverseNext = false;
        }
      } else if (row.shortlistIncluded) {
        lifecycle.state = "candidate";
        row.inUniverseNext = true;
      } else if (row.forcedGraduatedNoSeat) {
        lifecycle.state = "graduated";
        row.inUniverseNext = false;
      } else {
        if (lifecycle.state !== "suspended" && lifecycle.state !== "retired") {
          lifecycle.state = "candidate";
        }
        row.inUniverseNext = false;
      }
      if (row.entry.promotionGate) {
        row.entry.promotionGate.lifecycle = lifecycle;
      }
    }

    const updates = drafts.map((row) => row.entry);

    const updatesToPersist = drafts
      .filter((row) => {
        if (dirtySet.has(row.entry.deploymentId)) return true;
        const previousInUniverse = Boolean(
          inUniverseByDeploymentId.get(row.entry.deploymentId)?.inUniverse,
        );
        if (previousInUniverse !== row.inUniverseNext) return true;
        const previous = consideredByDeploymentId.get(row.entry.deploymentId);
        if (!previous) return true;
        return hasPromotionStateChanged({
          previous,
          next: row.entry,
        });
      })
      .map((row) => row.entry);
    const seatSyncRows = drafts.filter((row) => {
      const previousInUniverse = Boolean(
        inUniverseByDeploymentId.get(row.entry.deploymentId)?.inUniverse,
      );
      const previousLifecycleState = String(
        asJsonObject(
          asJsonObject(inUniverseByDeploymentId.get(row.entry.deploymentId)?.promotionGate)
            ?.lifecycle,
        )?.state || "",
      )
        .trim()
        .toLowerCase();
      const previousRetired = previousLifecycleState === "retired";
      const nextRetired = row.lifecycle.state === "retired";
      return previousInUniverse !== row.inUniverseNext || previousRetired !== nextRetired;
    });

    if (updatesToPersist.length > 0) {
      await upsertScalpDeploymentRegistryEntriesBulk(
        updatesToPersist.map((row) => ({
          deploymentId: row.deploymentId,
          symbol: row.symbol,
          strategyId: row.strategyId,
          tuneId: row.tuneId,
          source: row.source,
          enabled: row.enabled,
          configOverride: row.configOverride,
          promotionGate: row.promotionGate,
          updatedBy: "pipeline:promotion",
        })),
      );
    }

    if (seatSyncRows.length > 0) {
      for (const row of seatSyncRows) {
        await db.$executeRaw(Prisma.sql`
                    UPDATE scalp_deployments
                    SET
                        in_universe = ${row.inUniverseNext},
                        retired_at = ${row.lifecycle.state === "retired" ? new Date(nowMs) : null},
                        worker_dirty = CASE
                            WHEN ${row.inUniverseNext} = TRUE THEN worker_dirty
                            ELSE FALSE
                        END,
                        updated_by = 'pipeline:promotion',
                        updated_at = NOW()
                    WHERE deployment_id = ${row.entry.deploymentId};
                `);
      }
    }

    if (dirtySet.size > 0) {
      await db.$executeRaw(Prisma.sql`
                UPDATE scalp_deployments
                SET
                    promotion_dirty = FALSE,
                    updated_by = 'pipeline:promotion',
                    updated_at = NOW()
                WHERE deployment_id IN (${Prisma.join(Array.from(dirtySet))});
            `);
    }

    const consideredSymbolCount = new Set(
      consideredDeployments.map((row) => normalizeSymbol(row.symbol)),
    ).size;
    const candidateSymbolCount = new Set(
      candidates.map((row) => normalizeSymbol(row.symbol)),
    ).size;
    const lifecycleCounts = {
      candidate: 0,
      incumbent_refresh: 0,
      graduated: 0,
      suspended: 0,
      retired: 0,
    };
    for (const row of drafts) {
      const state = row.lifecycle.state;
      if (state in lifecycleCounts) {
        lifecycleCounts[state as keyof typeof lifecycleCounts] += 1;
      }
    }
    const pendingAfter = await countPendingPromotionRows();
    await pulsePipelineJobProgress({
      jobKind: "promotion",
      lockToken,
      lockMs,
      progressLabel: `updated ${updates.length}`,
      progress: {
        processedDirtyDeployments: dirtyRows.length,
        consideredDeployments: consideredDeployments.length,
        consideredSymbols: consideredSymbolCount,
        persistedDeployments: updatesToPersist.length,
        candidateCount: candidates.length,
        candidateSymbols: candidateSymbolCount,
        strategyWinnerCount: strategyWinnerIds.size,
        globalWinnerCount: globalWinnerIds.size,
        winnerCount: winnerIds.size,
        explorationShare,
        explorationSlots: explorationSelection.explorationSlots,
        exploitSlots: explorationSelection.exploitSlots,
        explorationSelected: explorationSelection.explorationSelected,
        exploitSelected: explorationSelection.exploitSelected,
        enabledByHysteresis,
        disabledByHysteresis,
        disabledByUniqueness,
        rolloverIncumbentsQueued,
        suspendedExact,
        suspendedNeighbors,
        retiredCount,
        weeklyQueueRowsInserted,
        loadNudgeSymbolsQueued: loadSymbolsToNudge.length,
        workerNudgeDeploymentsQueued: workerDeploymentsToNudge.length,
        nudgedLoadSymbols,
        nudgedWorkerRows,
        lifecycleCounts,
        pendingAfter,
      },
    });

    return {
      ok: true,
      processed: dirtyRows.length,
      succeeded: dirtyRows.length,
      retried: 0,
      failed: 0,
      pendingAfter,
      downstreamRequested: nudgedLoadSymbols > 0 || nudgedWorkerRows > 0,
      progressLabel: `updated ${updates.length}`,
      details: {
        policy,
        explorationShare,
        dirtyDeployments: dirtyRows.length,
        consideredDeployments: consideredDeployments.length,
        consideredSymbols: consideredSymbolCount,
        persistedDeployments: updatesToPersist.length,
        candidateCount: candidates.length,
        candidateSymbols: candidateSymbolCount,
        strategyWinnerCount: strategyWinnerIds.size,
        globalWinnerCount: globalWinnerIds.size,
        winnerCount: winnerIds.size,
        explorationSlots: explorationSelection.explorationSlots,
        exploitSlots: explorationSelection.exploitSlots,
        explorationSelected: explorationSelection.explorationSelected,
        exploitSelected: explorationSelection.exploitSelected,
        enabledByHysteresis,
        disabledByHysteresis,
        disabledByUniqueness,
        rolloverIncumbentsQueued,
        suspendedExact,
        suspendedNeighbors,
        retiredCount,
        weeklyQueueRowsInserted,
        loadNudgeSymbolsQueued: loadSymbolsToNudge.length,
        workerNudgeDeploymentsQueued: workerDeploymentsToNudge.length,
        nudgedLoadSymbols,
        nudgedWorkerRows,
        lifecycleCounts,
      },
    };
  });
}

export async function loadScalpPipelineJobsHealth(): Promise<
  ScalpPipelineJobHealth[]
> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  for (const jobKind of SCALP_PIPELINE_JOB_KINDS) {
    await ensurePipelineJobRow(jobKind);
  }

  const [jobRows, symbolRows, workerRows, deploymentRows] = await Promise.all([
    db.$queryRaw<
      Array<{
        jobKind: string;
        status: string;
        lockToken: string | null;
        lockExpiresAtMs: bigint | number | null;
        runningSinceAtMs: bigint | number | null;
        lastRunAtMs: bigint | number | null;
        lastDurationMs: number | null;
        lastSuccessAtMs: bigint | number | null;
        nextRunAtMs: bigint | number | null;
        lastError: string | null;
        progressLabel: string | null;
        progressJson: unknown;
      }>
    >(Prisma.sql`
            SELECT
                job_kind AS "jobKind",
                status,
                lock_token AS "lockToken",
                (EXTRACT(EPOCH FROM lock_expires_at) * 1000)::bigint AS "lockExpiresAtMs",
                (EXTRACT(EPOCH FROM running_since) * 1000)::bigint AS "runningSinceAtMs",
                (EXTRACT(EPOCH FROM last_run_at) * 1000)::bigint AS "lastRunAtMs",
                last_duration_ms AS "lastDurationMs",
                (EXTRACT(EPOCH FROM last_success_at) * 1000)::bigint AS "lastSuccessAtMs",
                (EXTRACT(EPOCH FROM next_run_at) * 1000)::bigint AS "nextRunAtMs",
                last_error AS "lastError",
                progress_label AS "progressLabel",
                progress_json AS "progressJson"
            FROM scalp_pipeline_jobs;
        `),
    db.$queryRaw<
      Array<{
        pendingLoad: bigint | number | null;
        runningLoad: bigint | number | null;
        retryLoad: bigint | number | null;
        failedLoad: bigint | number | null;
        succeededLoad: bigint | number | null;
        pendingPrepare: bigint | number | null;
        runningPrepare: bigint | number | null;
        retryPrepare: bigint | number | null;
        failedPrepare: bigint | number | null;
        succeededPrepare: bigint | number | null;
      }>
    >(Prisma.sql`
            SELECT
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'pending')::bigint AS "pendingLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'running')::bigint AS "runningLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'retry_wait')::bigint AS "retryLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'failed')::bigint AS "failedLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND load_status = 'succeeded')::bigint AS "succeededLoad",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'pending')::bigint AS "pendingPrepare",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'running')::bigint AS "runningPrepare",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'retry_wait')::bigint AS "retryPrepare",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'failed')::bigint AS "failedPrepare",
                COUNT(*) FILTER (WHERE active = TRUE AND prepare_status = 'succeeded')::bigint AS "succeededPrepare"
            FROM scalp_pipeline_symbols;
        `),
    db.$queryRaw<
      Array<{
        pendingWorker: bigint | number | null;
        runningWorker: bigint | number | null;
        retryWorker: bigint | number | null;
        failedWorker: bigint | number | null;
        succeededWorker: bigint | number | null;
      }>
    >(Prisma.sql`
            SELECT
                COUNT(*) FILTER (WHERE status = 'pending')::bigint AS "pendingWorker",
                COUNT(*) FILTER (WHERE status = 'running')::bigint AS "runningWorker",
                COUNT(*) FILTER (WHERE status = 'retry_wait')::bigint AS "retryWorker",
                COUNT(*) FILTER (WHERE status = 'failed')::bigint AS "failedWorker",
                COUNT(*) FILTER (WHERE status = 'succeeded')::bigint AS "succeededWorker"
            FROM scalp_deployment_weekly_metrics;
        `),
    db.$queryRaw<
      Array<{
        pendingPromotion: bigint | number | null;
        runningPromotion: bigint | number | null;
        retryPromotion: bigint | number | null;
        failedPromotion: bigint | number | null;
        succeededPromotion: bigint | number | null;
      }>
    >(Prisma.sql`
            SELECT
                COUNT(*) FILTER (WHERE in_universe = TRUE AND promotion_dirty = TRUE)::bigint AS "pendingPromotion",
                0::bigint AS "runningPromotion",
                0::bigint AS "retryPromotion",
                0::bigint AS "failedPromotion",
                COUNT(*) FILTER (WHERE in_universe = TRUE AND promotion_dirty = FALSE)::bigint AS "succeededPromotion"
            FROM scalp_deployments;
        `),
  ]);

  const symbolAgg = symbolRows[0] || ({} as any);
  const workerAgg = workerRows[0] || ({} as any);
  const promotionAgg = deploymentRows[0] || ({} as any);
  const nowMs = Date.now();

  const byJobKind = new Map(jobRows.map((row) => [String(row.jobKind), row]));

  const queueByJobKind: Record<
    ScalpPipelineJobKind,
    ScalpPipelineJobHealth["queue"]
  > = {
    discover: {
      pending: Math.max(0, Math.floor(Number(symbolAgg.pendingLoad || 0))),
      running: 0,
      retryWait: 0,
      failed: 0,
      succeeded: Math.max(0, Math.floor(Number(symbolAgg.succeededLoad || 0))),
    },
    load_candles: {
      pending: Math.max(0, Math.floor(Number(symbolAgg.pendingLoad || 0))),
      running: Math.max(0, Math.floor(Number(symbolAgg.runningLoad || 0))),
      retryWait: Math.max(0, Math.floor(Number(symbolAgg.retryLoad || 0))),
      failed: Math.max(0, Math.floor(Number(symbolAgg.failedLoad || 0))),
      succeeded: Math.max(0, Math.floor(Number(symbolAgg.succeededLoad || 0))),
    },
    prepare: {
      pending: Math.max(0, Math.floor(Number(symbolAgg.pendingPrepare || 0))),
      running: Math.max(0, Math.floor(Number(symbolAgg.runningPrepare || 0))),
      retryWait: Math.max(0, Math.floor(Number(symbolAgg.retryPrepare || 0))),
      failed: Math.max(0, Math.floor(Number(symbolAgg.failedPrepare || 0))),
      succeeded: Math.max(
        0,
        Math.floor(Number(symbolAgg.succeededPrepare || 0)),
      ),
    },
    worker: {
      pending: Math.max(0, Math.floor(Number(workerAgg.pendingWorker || 0))),
      running: Math.max(0, Math.floor(Number(workerAgg.runningWorker || 0))),
      retryWait: Math.max(0, Math.floor(Number(workerAgg.retryWorker || 0))),
      failed: Math.max(0, Math.floor(Number(workerAgg.failedWorker || 0))),
      succeeded: Math.max(
        0,
        Math.floor(Number(workerAgg.succeededWorker || 0)),
      ),
    },
    promotion: {
      pending: Math.max(
        0,
        Math.floor(Number(promotionAgg.pendingPromotion || 0)),
      ),
      running: Math.max(
        0,
        Math.floor(Number(promotionAgg.runningPromotion || 0)),
      ),
      retryWait: Math.max(
        0,
        Math.floor(Number(promotionAgg.retryPromotion || 0)),
      ),
      failed: Math.max(
        0,
        Math.floor(Number(promotionAgg.failedPromotion || 0)),
      ),
      succeeded: Math.max(
        0,
        Math.floor(Number(promotionAgg.succeededPromotion || 0)),
      ),
    },
  };

  return SCALP_PIPELINE_JOB_KINDS.map((jobKind) => {
    const row = byJobKind.get(jobKind) || null;
    const lockExpiresAtMs = asTsMs(row?.lockExpiresAtMs);
    const runningSinceAtMs = asTsMs(row?.runningSinceAtMs);
    const runningDurationMs =
      runningSinceAtMs !== null && runningSinceAtMs <= nowMs
        ? nowMs - runningSinceAtMs
        : null;
    const lastDurationMsRaw = Number(row?.lastDurationMs);
    const lastDurationMs =
      Number.isFinite(lastDurationMsRaw) && lastDurationMsRaw >= 0
        ? Math.floor(lastDurationMsRaw)
        : null;
    return {
      jobKind,
      status: String(row?.status || "idle"),
      locked: typeof lockExpiresAtMs === "number" && lockExpiresAtMs > nowMs,
      runningSinceAtMs,
      runningDurationMs,
      lastRunAtMs: asTsMs(row?.lastRunAtMs),
      lastDurationMs,
      lastSuccessAtMs: asTsMs(row?.lastSuccessAtMs),
      nextRunAtMs: asTsMs(row?.nextRunAtMs),
      lastError: String(row?.lastError || "").trim() || null,
      progressLabel: String(row?.progressLabel || "").trim() || null,
      progress: asJsonObject(row?.progressJson),
      queue: queueByJobKind[jobKind],
    };
  });
}

export async function listScalpDeploymentWeeklyMetricRows(
  params: { limit?: number } = {},
): Promise<ScalpDeploymentWeeklyMetricSnapshotRow[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(20_000, toPositiveInt(params.limit, 8_000)));
  const rows = await db.$queryRaw<
    Array<{
      deploymentId: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      workerId: string | null;
      weekStartMs: bigint | number | null;
      weekEndMs: bigint | number | null;
      status: string;
      attempts: number | null;
      startedAtMs: bigint | number | null;
      finishedAtMs: bigint | number | null;
      errorCode: string | null;
      errorMessage: string | null;
      trades: number | null;
      netR: unknown;
      expectancyR: unknown;
      profitFactor: unknown;
      maxDrawdownR: unknown;
    }>
  >(Prisma.sql`
        SELECT
            m.deployment_id AS "deploymentId",
            m.symbol,
            m.strategy_id AS "strategyId",
            m.tune_id AS "tuneId",
            m.worker_id AS "workerId",
            (EXTRACT(EPOCH FROM m.week_start) * 1000)::bigint AS "weekStartMs",
            (EXTRACT(EPOCH FROM m.week_end) * 1000)::bigint AS "weekEndMs",
            m.status,
            m.attempts,
            (EXTRACT(EPOCH FROM m.started_at) * 1000)::bigint AS "startedAtMs",
            (EXTRACT(EPOCH FROM m.finished_at) * 1000)::bigint AS "finishedAtMs",
            m.error_code AS "errorCode",
            m.error_message AS "errorMessage",
            m.trades,
            m.net_r AS "netR",
            m.expectancy_r AS "expectancyR",
            m.profit_factor AS "profitFactor",
            m.max_drawdown_r AS "maxDrawdownR"
        FROM scalp_deployment_weekly_metrics m
        JOIN scalp_deployments d
          ON d.deployment_id = m.deployment_id
        ORDER BY m.week_start DESC, m.deployment_id ASC
        LIMIT ${limit};
    `);

  return rows
    .map((row) => {
      const deploymentId = String(row.deploymentId || "").trim();
      const symbol = String(row.symbol || "")
        .trim()
        .toUpperCase();
      const strategyId = String(row.strategyId || "").trim();
      const tuneId = String(row.tuneId || "").trim() || "default";
      const weekStartMs = asTsMs(row.weekStartMs);
      const weekEndMs = asTsMs(row.weekEndMs);
      if (!deploymentId || !symbol || !strategyId) return null;
      if (weekStartMs === null || weekEndMs === null) return null;
      const startedAtMs = asTsMs(row.startedAtMs);
      const finishedAtMs = asTsMs(row.finishedAtMs);
      const durationMs =
        startedAtMs !== null &&
        finishedAtMs !== null &&
        finishedAtMs >= startedAtMs
          ? finishedAtMs - startedAtMs
          : null;
      return {
        deploymentId,
        symbol,
        strategyId,
        tuneId,
        workerId: String(row.workerId || "").trim() || null,
        weekStartMs,
        weekEndMs,
        status: String(row.status || "pending")
          .trim()
          .toLowerCase(),
        attempts: Math.max(0, Math.floor(Number(row.attempts || 0))),
        startedAtMs,
        finishedAtMs,
        durationMs,
        errorCode: String(row.errorCode || "").trim() || null,
        errorMessage: String(row.errorMessage || "").trim() || null,
        trades:
          row.trades === null || row.trades === undefined
            ? null
            : Math.max(0, Math.floor(Number(row.trades))),
        netR: asNullableFiniteNumber(row.netR),
        expectancyR: asNullableFiniteNumber(row.expectancyR),
        profitFactor: asNullableFiniteNumber(row.profitFactor),
        maxDrawdownR: asNullableFiniteNumber(row.maxDrawdownR),
      } satisfies ScalpDeploymentWeeklyMetricSnapshotRow;
    })
    .filter((row): row is ScalpDeploymentWeeklyMetricSnapshotRow => row !== null);
}

export async function listScalpDurationTimelineRuns(params: {
  source?: "all" | ScalpDurationTimelineSource;
  jobKind?: "all" | ScalpPipelineJobKind;
  fromMs?: number;
  toMs?: number;
  limit?: number;
}): Promise<ScalpDurationTimelineRun[]> {
  if (!isScalpPgConfigured()) return [];
  const source = params.source === "pipeline" || params.source === "worker"
    ? params.source
    : "all";
  const jobKind = SCALP_PIPELINE_JOB_KINDS.includes(
    params.jobKind as ScalpPipelineJobKind,
  )
    ? (params.jobKind as ScalpPipelineJobKind)
    : "all";
  const nowMs = Date.now();
  const fromMs =
    typeof params.fromMs === "number" && Number.isFinite(params.fromMs)
      ? Math.floor(params.fromMs)
      : nowMs - 7 * ONE_DAY_MS;
  const toMs =
    typeof params.toMs === "number" && Number.isFinite(params.toMs)
      ? Math.floor(params.toMs)
      : nowMs;
  const rangeStartMs = Math.min(fromMs, toMs);
  const rangeEndMs = Math.max(fromMs, toMs);
  const limit = Math.max(1, Math.min(500, toPositiveInt(params.limit, 50)));
  const db = scalpPrisma();

  const pipelineRows =
    source === "worker"
      ? []
      : await db.$queryRaw<
          Array<{
            jobKind: string;
            status: string;
            startedAtMs: bigint | number | null;
            finishedAtMs: bigint | number | null;
            durationMs: number | null;
            processed: number | null;
            succeeded: number | null;
            retried: number | null;
            failed: number | null;
            pendingAfter: number | null;
            downstreamRequested: boolean | null;
          }>
        >(Prisma.sql`
          SELECT
              r.job_kind AS "jobKind",
              r.status,
              (EXTRACT(EPOCH FROM r.started_at) * 1000)::bigint AS "startedAtMs",
              (EXTRACT(EPOCH FROM r.finished_at) * 1000)::bigint AS "finishedAtMs",
              r.duration_ms AS "durationMs",
              r.processed,
              r.succeeded,
              r.retried,
              r.failed,
              r.pending_after AS "pendingAfter",
              r.downstream_requested AS "downstreamRequested"
          FROM scalp_pipeline_job_runs r
          WHERE r.started_at >= ${new Date(rangeStartMs)}
            AND r.started_at <= ${new Date(rangeEndMs)}
            ${
              jobKind === "all"
                ? Prisma.empty
                : Prisma.sql`AND r.job_kind = ${jobKind}`
            }
          ORDER BY r.finished_at DESC
          LIMIT ${limit};
        `);

  const workerRows =
    source === "pipeline"
      ? []
      : await db.$queryRaw<
          Array<{
            workerId: string;
            startedAtMs: bigint | number | null;
            finishedAtMs: bigint | number | null;
            taskCount: bigint | number | null;
            succeededCount: bigint | number | null;
            failedCount: bigint | number | null;
          }>
        >(Prisma.sql`
          SELECT
              m.worker_id AS "workerId",
              (EXTRACT(EPOCH FROM MIN(m.started_at)) * 1000)::bigint AS "startedAtMs",
              (EXTRACT(EPOCH FROM MAX(m.finished_at)) * 1000)::bigint AS "finishedAtMs",
              COUNT(*)::bigint AS "taskCount",
              COUNT(*) FILTER (WHERE m.status = 'succeeded')::bigint AS "succeededCount",
              COUNT(*) FILTER (WHERE m.status = 'failed')::bigint AS "failedCount"
          FROM scalp_deployment_weekly_metrics m
          WHERE m.worker_id IS NOT NULL
            AND m.started_at IS NOT NULL
            AND m.started_at >= ${new Date(rangeStartMs)}
            AND m.started_at <= ${new Date(rangeEndMs)}
          GROUP BY m.worker_id
          ORDER BY MAX(m.finished_at) DESC NULLS LAST, MIN(m.started_at) DESC
          LIMIT ${limit};
        `);

  const pipelineRuns = pipelineRows
    .map((row) => {
      const normalizedJobKind = String(row.jobKind || "").trim().toLowerCase();
      if (
        !SCALP_PIPELINE_JOB_KINDS.includes(
          normalizedJobKind as ScalpPipelineJobKind,
        )
      ) {
        return null;
      }
      const startedAtMs = asTsMs(row.startedAtMs);
      const finishedAtMs = asTsMs(row.finishedAtMs);
      const rawDurationMs = Number(row.durationMs);
      const durationMs =
        Number.isFinite(rawDurationMs) && rawDurationMs >= 0
          ? Math.floor(rawDurationMs)
          : startedAtMs !== null &&
              finishedAtMs !== null &&
              finishedAtMs >= startedAtMs
            ? finishedAtMs - startedAtMs
            : null;
      return {
        source: "pipeline",
        status: String(row.status || "unknown").trim().toLowerCase() || "unknown",
        startedAtMs,
        finishedAtMs,
        durationMs,
        jobKind: normalizedJobKind as ScalpPipelineJobKind,
        processed: Math.max(0, Math.floor(Number(row.processed || 0))),
        succeeded: Math.max(0, Math.floor(Number(row.succeeded || 0))),
        retried: Math.max(0, Math.floor(Number(row.retried || 0))),
        failed: Math.max(0, Math.floor(Number(row.failed || 0))),
        pendingAfter: Math.max(0, Math.floor(Number(row.pendingAfter || 0))),
        downstreamRequested: Boolean(row.downstreamRequested),
      } satisfies ScalpDurationTimelineRun;
    })
    .filter((row) => row !== null) as ScalpDurationTimelineRun[];

  const workerRuns = workerRows
    .map((row) => {
      const workerId = String(row.workerId || "").trim();
      if (!workerId) return null;
      const startedAtMs = asTsMs(row.startedAtMs);
      const finishedAtMs = asTsMs(row.finishedAtMs);
      const durationMs =
        startedAtMs !== null &&
        finishedAtMs !== null &&
        finishedAtMs >= startedAtMs
          ? finishedAtMs - startedAtMs
          : null;
      const failedCount = Math.max(0, Math.floor(Number(row.failedCount || 0)));
      const succeededCount = Math.max(
        0,
        Math.floor(Number(row.succeededCount || 0)),
      );
      const status = failedCount > 0
        ? "failed"
        : finishedAtMs === null
          ? "running"
          : "succeeded";
      return {
        source: "worker",
        status,
        startedAtMs,
        finishedAtMs,
        durationMs,
        workerId,
        taskCount: Math.max(0, Math.floor(Number(row.taskCount || 0))),
        succeededCount,
        failedCount,
      } satisfies ScalpDurationTimelineRun;
    })
    .filter((row) => row !== null) as ScalpDurationTimelineRun[];

  return [...pipelineRuns, ...workerRuns]
    .sort((a, b) => {
      const aSortTs = a.finishedAtMs ?? a.startedAtMs ?? 0;
      const bSortTs = b.finishedAtMs ?? b.startedAtMs ?? 0;
      return bSortTs - aSortTs;
    })
    .slice(0, limit);
}
