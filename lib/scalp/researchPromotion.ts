import { Prisma } from "@prisma/client";

import {
  loadScalpDeploymentRegistry,
  upsertScalpDeploymentRegistryEntriesBulk,
  type ScalpDeploymentPromotionGate,
  type ScalpDeploymentRegistryEntry,
  type ScalpDeploymentRegistrySource,
  type ScalpDeploymentRegistryWriteParams,
  type ScalpForwardValidationMetrics,
} from "./deploymentRegistry";
import { loadScalpCandleHistory } from "./candleHistory";
import { pipSizeForScalpSymbol } from "./marketData";
import { isScalpPgConfigured, scalpPrisma } from "./pg/client";
import { runScalpReplay } from "./replay/harness";
import { buildScalpReplayRuntimeFromDeployment } from "./replay/runtimeConfig";
import type { ScalpSymbolMarketMetadata } from "./symbolMarketMetadata";
import { loadScalpSymbolMarketMetadata } from "./symbolMarketMetadataStore";
import {
  listLatestResearchTasksByWindow,
  loadActiveResearchCycleId,
  loadLatestCompletedResearchCycleId,
  loadResearchCycle,
  type ScalpResearchCycleSnapshot,
  type ScalpResearchTask,
} from "./researchCycle";

const DAY_MS = 24 * 60 * 60_000;
const WEEK_MS = 7 * DAY_MS;
const PROMOTION_SYNC_STATE_DEDUPE_KEY = "state:latest:v1";
const PROMOTION_SYNC_PROGRESS_DEDUPE_KEY = "state:progress:v1";

type CandleRow = [number, number, number, number, number, number];

type WeeklySliceMetric = {
  fromTs: number;
  toTs: number;
  trades: number;
  netR: number;
  expectancyR: number;
  maxDrawdownR: number;
};

export interface ScalpWeeklyRobustnessMetrics {
  slices: number;
  profitableSlices: number;
  profitablePct: number;
  meanExpectancyR: number;
  medianExpectancyR: number;
  worstNetR: number;
  worstMaxDrawdownR: number;
  topWeekPnlConcentrationPct: number;
  totalNetR: number;
  evaluatedAtMs: number;
}

export interface SyncResearchWeeklyPolicy {
  enabled: boolean;
  topKPerSymbol: number;
  lookbackDays: number;
  minCandlesPerSlice: number;
  requireWinnerShortlist: boolean;
  minSlices: number;
  minProfitablePct: number;
  minMedianExpectancyR: number;
  maxTopWeekPnlConcentrationPct: number;
}

interface SyncResearchConfirmationPolicy {
  enabled: boolean;
  topKPerSymbol: number;
  lookbackDays: number;
}

function keyOf(symbol: string, strategyId: string, tuneId?: string): string {
  return `${String(symbol || "")
    .trim()
    .toUpperCase()}::${String(strategyId || "")
    .trim()
    .toLowerCase()}::${String(tuneId || "")
    .trim()
    .toLowerCase()}`;
}

function strategyKeyOf(symbol: string, strategyId: string): string {
  return `${String(symbol || "")
    .trim()
    .toUpperCase()}::${String(strategyId || "")
    .trim()
    .toLowerCase()}`;
}

function toFinite(value: unknown, fallback = 0): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function toPositiveInt(value: unknown, fallback: number): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return n;
}

function toNonNegativeInt(value: unknown, fallback: number): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n < 0) return fallback;
  return n;
}

function toBool(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "off"].includes(normalized)) return false;
  }
  return fallback;
}

function toBoundedPercent(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(100, n));
}

function envOrFallbackNumber(envKey: string, fallback: number): number {
  const n = Number(process.env[envKey]);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function envOrFallbackBool(envKey: string, fallback: boolean): boolean {
  const raw = process.env[envKey];
  if (raw === undefined) return fallback;
  return toBool(raw, fallback);
}

function startOfUtcDay(tsMs: number): number {
  const d = new Date(tsMs);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

function startOfWeekMondayUtc(tsMs: number): number {
  const dayStartMs = startOfUtcDay(tsMs);
  const dayOfWeek = new Date(dayStartMs).getUTCDay(); // 0=Sunday ... 6=Saturday
  const daysSinceMonday = (dayOfWeek + 6) % 7;
  return dayStartMs - daysSinceMonday * DAY_MS;
}

function nextWeekMondayUtc(tsMs: number): number {
  const weekStart = startOfWeekMondayUtc(tsMs);
  return tsMs === weekStart ? weekStart : weekStart + WEEK_MS;
}

function toReplayCandles(rows: CandleRow[], spreadPips: number) {
  return rows.map((row) => ({
    ts: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5] ?? 0),
    spreadPips,
  }));
}

function median(values: number[]): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[mid] as number;
  }
  return ((sorted[mid - 1] as number) + (sorted[mid] as number)) / 2;
}

function candidateMedianExpectancyR(
  candidate: Pick<ScalpResearchForwardValidationCandidate, "meanExpectancyR"> & {
    medianExpectancyR?: number | null;
  },
): number {
  const medianValue = Number(candidate.medianExpectancyR);
  if (Number.isFinite(medianValue)) return medianValue;
  return Number(candidate.meanExpectancyR) || 0;
}

function candidateTopWindowPnlConcentrationPct(
  candidate: {
    topWindowPnlConcentrationPct?: number | null;
  },
): number {
  const concentration = Number(candidate.topWindowPnlConcentrationPct);
  if (!Number.isFinite(concentration)) return 0;
  return Math.max(0, Math.min(100, concentration));
}

function candidateSelectionScore(
  candidate: Pick<ScalpResearchForwardValidationCandidate, "meanExpectancyR"> & {
    medianExpectancyR?: number | null;
    topWindowPnlConcentrationPct?: number | null;
    selectionScore?: number | null;
  },
): number {
  const explicit = Number(candidate.selectionScore);
  if (Number.isFinite(explicit)) return explicit;
  const smoothedExpectancy =
    (Number(candidate.meanExpectancyR) + candidateMedianExpectancyR(candidate)) /
    2;
  const concentrationPenalty = Math.max(
    0,
    candidateTopWindowPnlConcentrationPct(candidate) - 50,
  );
  return smoothedExpectancy * (1 - concentrationPenalty / 100);
}

function candidateProfitFactorForRanking(
  candidate: Pick<ScalpResearchForwardValidationCandidate, "meanProfitFactor">,
): number {
  const profitFactor = Number(candidate.meanProfitFactor);
  if (!Number.isFinite(profitFactor)) return Number.NEGATIVE_INFINITY;
  return profitFactor;
}

function compareCandidates(
  a: ScalpResearchForwardValidationCandidate,
  b: ScalpResearchForwardValidationCandidate,
): number {
  const aSelectionScore = candidateSelectionScore(a);
  const bSelectionScore = candidateSelectionScore(b);
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
  return a.tuneId.localeCompare(b.tuneId);
}

function buildBestTuneCandidatesBySymbolStrategy(
  candidates: ScalpResearchForwardValidationCandidate[],
): ScalpResearchForwardValidationCandidate[] {
  const bestBySymbolStrategy = new Map<
    string,
    ScalpResearchForwardValidationCandidate
  >();
  for (const candidate of candidates) {
    const strategyKey = strategyKeyOf(candidate.symbol, candidate.strategyId);
    const current = bestBySymbolStrategy.get(strategyKey) || null;
    if (!current || compareCandidates(candidate, current) < 0) {
      bestBySymbolStrategy.set(strategyKey, candidate);
    }
  }
  return Array.from(bestBySymbolStrategy.values()).sort(compareCandidates);
}

export function buildCandidateMaterializationShortlist(
  candidates: ScalpResearchForwardValidationCandidate[],
  topKPerSymbol: number,
): ScalpResearchForwardValidationCandidate[] {
  const bySymbol = new Map<string, ScalpResearchForwardValidationCandidate[]>();
  for (const candidate of candidates) {
    if (!bySymbol.has(candidate.symbol)) {
      bySymbol.set(candidate.symbol, []);
    }
    bySymbol.get(candidate.symbol)!.push(candidate);
  }

  const shortlist: ScalpResearchForwardValidationCandidate[] = [];
  const topK = Math.max(1, Math.floor(topKPerSymbol));
  for (const rows of bySymbol.values()) {
    rows.sort(compareCandidates);
    shortlist.push(...rows.slice(0, Math.min(topK, rows.length)));
  }

  return shortlist.sort(compareCandidates);
}

export function buildWinnerCandidateKeySet(
  candidates: ScalpResearchForwardValidationCandidate[],
  topKPerSymbol: number,
): Set<string> {
  const winnerKeys = new Set<string>();
  const bySymbol = new Map<string, ScalpResearchForwardValidationCandidate[]>();
  const strategyWinners = buildBestTuneCandidatesBySymbolStrategy(candidates);
  for (const row of strategyWinners) {
    if (!bySymbol.has(row.symbol)) {
      bySymbol.set(row.symbol, []);
    }
    bySymbol.get(row.symbol)!.push(row);
  }

  const topK = Math.max(1, Math.floor(topKPerSymbol));
  for (const rows of bySymbol.values()) {
    rows.sort(compareCandidates);
    for (const row of rows.slice(0, Math.min(topK, rows.length))) {
      winnerKeys.add(keyOf(row.symbol, row.strategyId, row.tuneId));
    }
  }
  return winnerKeys;
}

function topPositiveNetConcentrationPct(values: number[]): number {
  const positiveNet = values.map((value) => Math.max(0, value));
  const totalPositive = positiveNet.reduce((acc, value) => acc + value, 0);
  if (totalPositive <= 0) return 100;
  const topPositive = positiveNet.length ? Math.max(...positiveNet) : 0;
  return (topPositive / totalPositive) * 100;
}

function buildForwardSelectionScore(params: {
  meanExpectancyR: number;
  medianExpectancyR: number;
  topWindowPnlConcentrationPct: number;
}): number {
  const smoothedExpectancy =
    (params.meanExpectancyR + params.medianExpectancyR) / 2;
  const concentrationPenalty = Math.max(
    0,
    params.topWindowPnlConcentrationPct - 50,
  );
  return smoothedExpectancy * (1 - concentrationPenalty / 100);
}

export function evaluateWeeklyRobustnessGate(
  metrics: ScalpWeeklyRobustnessMetrics | null,
  policy: SyncResearchWeeklyPolicy,
): { passed: boolean; reason: string | null } {
  if (!policy.enabled) return { passed: true, reason: null };
  if (!metrics) return { passed: false, reason: "weekly_robustness_missing" };
  if (metrics.slices < policy.minSlices) {
    return { passed: false, reason: "weekly_slice_count_below_threshold" };
  }
  if (metrics.profitablePct < policy.minProfitablePct) {
    return { passed: false, reason: "weekly_profitable_pct_below_threshold" };
  }
  if (metrics.medianExpectancyR < policy.minMedianExpectancyR) {
    return {
      passed: false,
      reason: "weekly_median_expectancy_below_threshold",
    };
  }
  if (
    metrics.topWeekPnlConcentrationPct > policy.maxTopWeekPnlConcentrationPct
  ) {
    return {
      passed: false,
      reason: "weekly_top_week_concentration_above_threshold",
    };
  }
  return { passed: true, reason: null };
}

function resolveWeeklyPolicy(
  params: SyncResearchPromotionParams,
  cycle: ScalpResearchCycleSnapshot,
): SyncResearchWeeklyPolicy {
  const enabled =
    params.weeklyRobustnessEnabled ??
    envOrFallbackBool("SCALP_WEEKLY_ROBUSTNESS_ENABLED", true);
  const topKPerSymbol = toPositiveInt(
    params.weeklyRobustnessTopKPerSymbol ??
      envOrFallbackNumber("SCALP_WEEKLY_ROBUSTNESS_TOPK_PER_SYMBOL", 2),
    2,
  );
  const lookbackDays = toPositiveInt(
    params.weeklyRobustnessLookbackDays ??
      envOrFallbackNumber(
        "SCALP_WEEKLY_ROBUSTNESS_LOOKBACK_DAYS",
        cycle.params.lookbackDays,
      ),
    cycle.params.lookbackDays,
  );
  const minCandlesPerSlice = toPositiveInt(
    params.weeklyRobustnessMinCandlesPerSlice ??
      envOrFallbackNumber(
        "SCALP_WEEKLY_ROBUSTNESS_MIN_CANDLES_PER_SLICE",
        Math.max(180, Math.floor(cycle.params.minCandlesPerTask / 2)),
      ),
    Math.max(180, Math.floor(cycle.params.minCandlesPerTask / 2)),
  );
  const requireWinnerShortlist =
    params.weeklyRobustnessRequireWinnerShortlist ??
    envOrFallbackBool("SCALP_WEEKLY_ROBUSTNESS_REQUIRE_WINNER_SHORTLIST", true);
  const minSlices = toPositiveInt(
    params.weeklyRobustnessMinSlices ??
      envOrFallbackNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_SLICES", 8),
    8,
  );
  const minProfitablePct = toBoundedPercent(
    params.weeklyRobustnessMinProfitablePct ??
      envOrFallbackNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_PROFITABLE_PCT", 45),
    45,
  );
  const minMedianExpectancyR = toFinite(
    params.weeklyRobustnessMinMedianExpectancyR ??
      envOrFallbackNumber("SCALP_WEEKLY_ROBUSTNESS_MIN_MEDIAN_EXPECTANCY_R", 0),
    0,
  );
  const maxTopWeekPnlConcentrationPct = toBoundedPercent(
    params.weeklyRobustnessMaxTopWeekPnlConcentrationPct ??
      envOrFallbackNumber(
        "SCALP_WEEKLY_ROBUSTNESS_MAX_TOP_WEEK_PNL_CONCENTRATION_PCT",
        80,
      ),
    80,
  );

  return {
    enabled,
    topKPerSymbol,
    lookbackDays,
    minCandlesPerSlice,
    requireWinnerShortlist,
    minSlices,
    minProfitablePct,
    minMedianExpectancyR,
    maxTopWeekPnlConcentrationPct,
  };
}

function resolveConfirmationPolicy(
  params: SyncResearchPromotionParams,
  cycle: ScalpResearchCycleSnapshot,
): SyncResearchConfirmationPolicy {
  const enabled =
    params.confirmationEnabled ??
    envOrFallbackBool("SCALP_RESEARCH_CONFIRMATION_ENABLED", true);
  const topKPerSymbol = toPositiveInt(
    params.confirmationTopKPerSymbol ??
      envOrFallbackNumber("SCALP_RESEARCH_CONFIRMATION_TOPK_PER_SYMBOL", 2),
    2,
  );
  const lookbackDays = Math.max(
    cycle.params.lookbackDays,
    toPositiveInt(
      params.confirmationLookbackDays ??
        envOrFallbackNumber("SCALP_RESEARCH_CONFIRMATION_LOOKBACK_DAYS", 364),
      364,
    ),
  );
  return {
    enabled,
    topKPerSymbol,
    lookbackDays,
  };
}

function buildReplayRuntimeForDeployment(entry: ScalpDeploymentRegistryEntry) {
  return buildScalpReplayRuntimeFromDeployment({
    deployment: entry,
    configOverride: entry.configOverride,
  });
}

async function runWeeklyRobustnessForDeployment(params: {
  deployment: ScalpDeploymentRegistryEntry;
  candles: CandleRow[];
  symbolMeta: ScalpSymbolMarketMetadata | null;
  nowMs: number;
  lookbackDays: number;
  minCandlesPerSlice: number;
}): Promise<ScalpWeeklyRobustnessMetrics | null> {
  const currentWeekStart = startOfWeekMondayUtc(params.nowMs);
  const lookbackStartTs =
    params.nowMs - Math.max(1, Math.floor(params.lookbackDays)) * DAY_MS;
  const firstSliceFrom = nextWeekMondayUtc(lookbackStartTs);
  const windowRows = params.candles.filter(
    (row) => row[0] >= lookbackStartTs && row[0] < currentWeekStart,
  );
  if (windowRows.length < params.minCandlesPerSlice) return null;

  const runtime = buildReplayRuntimeForDeployment(params.deployment);
  const slices: WeeklySliceMetric[] = [];

  for (
    let sliceFrom = firstSliceFrom;
    sliceFrom + WEEK_MS <= currentWeekStart;
    sliceFrom += WEEK_MS
  ) {
    const sliceTo = sliceFrom + WEEK_MS;
    const rows = windowRows.filter(
      (row) => row[0] >= sliceFrom && row[0] < sliceTo,
    );
    if (rows.length < params.minCandlesPerSlice) continue;

    const replay = await runScalpReplay({
      candles: toReplayCandles(rows, runtime.defaultSpreadPips),
      pipSize: pipSizeForScalpSymbol(
        params.deployment.symbol,
        params.symbolMeta,
      ),
      config: runtime,
      captureTimeline: false,
      symbolMeta: params.symbolMeta,
    });
    slices.push({
      fromTs: sliceFrom,
      toTs: sliceTo,
      trades: replay.summary.trades,
      netR: replay.summary.netR,
      expectancyR: replay.summary.expectancyR,
      maxDrawdownR: replay.summary.maxDrawdownR,
    });
  }

  if (!slices.length) return null;

  const profitableSlices = slices.filter((row) => row.netR > 0).length;
  const profitablePct = (profitableSlices / slices.length) * 100;
  const expectancyRows = slices.map((row) => row.expectancyR);
  const meanExpectancyR =
    expectancyRows.reduce((acc, row) => acc + row, 0) / slices.length;
  const medianExpectancyR = median(expectancyRows);
  const worstNetR = slices.reduce(
    (acc, row) => Math.min(acc, row.netR),
    Number.POSITIVE_INFINITY,
  );
  const worstMaxDrawdownR = slices.reduce(
    (acc, row) => Math.max(acc, row.maxDrawdownR),
    0,
  );
  const totalNetR = slices.reduce((acc, row) => acc + row.netR, 0);

  const positiveNet = slices.map((row) => Math.max(0, row.netR));
  const totalPositive = positiveNet.reduce((acc, row) => acc + row, 0);
  const topPositive = positiveNet.length ? Math.max(...positiveNet) : 0;
  const topWeekPnlConcentrationPct =
    totalPositive > 0 ? (topPositive / totalPositive) * 100 : 100;

  return {
    slices: slices.length,
    profitableSlices,
    profitablePct,
    meanExpectancyR,
    medianExpectancyR,
    worstNetR: Number.isFinite(worstNetR) ? worstNetR : 0,
    worstMaxDrawdownR,
    topWeekPnlConcentrationPct,
    totalNetR,
    evaluatedAtMs: params.nowMs,
  };
}

export interface ScalpResearchForwardValidationCandidate {
  symbol: string;
  strategyId: string;
  tuneId: string;
  deploymentId: string;
  rollCount: number;
  profitableWindowPct: number;
  profitableWindows: number;
  meanExpectancyR: number;
  medianExpectancyR?: number | null;
  meanProfitFactor: number | null;
  maxDrawdownR: number;
  topWindowPnlConcentrationPct?: number | null;
  selectionScore?: number | null;
  minTradesPerWindow: number | null;
  totalTrades: number;
  selectionWindowDays: number;
  forwardWindowDays: number;
  forwardValidation: ScalpForwardValidationMetrics;
}

function inferForwardWindowDaysFromTasks(
  tasks: ScalpResearchTask[],
  fallbackDays: number,
): number {
  const windowDays = tasks
    .map((task) =>
      Math.round(
        Math.max(1, (task.windowToTs - task.windowFromTs) / DAY_MS),
      ),
    )
    .filter((days) => Number.isFinite(days) && days > 0)
    .sort((a, b) => a - b);
  if (!windowDays.length) return Math.max(1, Math.floor(fallbackDays));
  return Math.max(1, Math.round(median(windowDays)));
}

export function buildForwardValidationByCandidateFromTasks(params: {
  tasks: ScalpResearchTask[];
  selectionWindowDays: number;
  forwardWindowDays: number;
}): ScalpResearchForwardValidationCandidate[] {
  const byCandidate = new Map<
    string,
    {
      symbol: string;
      strategyId: string;
      tuneId: string;
      deploymentId: string;
      rollCount: number;
      profitableWindows: number;
      expectancySum: number;
      expectancyRows: number[];
      netRows: number[];
      profitFactorSum: number;
      profitFactorCount: number;
      maxDrawdownR: number;
      minTradesPerWindow: number | null;
      totalTrades: number;
    }
  >();

  for (const task of params.tasks) {
    if (task.status !== "completed" || !task.result) continue;
    const symbol = String(task.symbol || "")
      .trim()
      .toUpperCase();
    const strategyId = String(task.strategyId || "")
      .trim()
      .toLowerCase();
    const tuneId = String(task.tuneId || task.result.tuneId || "")
      .trim()
      .toLowerCase();
    const deploymentId = String(
      task.deploymentId || task.result.deploymentId || "",
    ).trim();
    if (!symbol || !strategyId || !tuneId || !deploymentId) continue;

    const key = keyOf(symbol, strategyId, tuneId);
    if (!byCandidate.has(key)) {
      byCandidate.set(key, {
        symbol,
        strategyId,
        tuneId,
        deploymentId,
        rollCount: 0,
        profitableWindows: 0,
        expectancySum: 0,
        expectancyRows: [],
        netRows: [],
        profitFactorSum: 0,
        profitFactorCount: 0,
        maxDrawdownR: 0,
        minTradesPerWindow: null,
        totalTrades: 0,
      });
    }

    const row = byCandidate.get(key)!;
    const trades = Math.max(0, Math.floor(toFinite(task.result.trades, 0)));
    const expectancyR = toFinite(task.result.expectancyR, 0);
    const netR = toFinite(task.result.netR, 0);
    const maxDrawdownR = Math.max(0, toFinite(task.result.maxDrawdownR, 0));
    const rawProfitFactor = task.result.profitFactor;
    const profitFactor =
      rawProfitFactor === null || rawProfitFactor === undefined
        ? Number.NaN
        : Number(rawProfitFactor);

    row.rollCount += 1;
    row.totalTrades += trades;
    row.expectancySum += expectancyR;
    row.expectancyRows.push(expectancyR);
    row.netRows.push(netR);
    row.maxDrawdownR = Math.max(row.maxDrawdownR, maxDrawdownR);
    row.minTradesPerWindow =
      row.minTradesPerWindow === null
        ? trades
        : Math.min(row.minTradesPerWindow, trades);
    if (netR > 0) {
      row.profitableWindows += 1;
    }
    if (Number.isFinite(profitFactor) && profitFactor >= 0) {
      row.profitFactorSum += profitFactor;
      row.profitFactorCount += 1;
    }
  }

  return Array.from(byCandidate.values())
    .filter((row) => row.rollCount > 0)
    .map((row) => {
      const profitableWindowPct = (row.profitableWindows / row.rollCount) * 100;
      const meanExpectancyR = row.expectancySum / row.rollCount;
      const medianExpectancyR = median(row.expectancyRows);
      const topWindowPnlConcentrationPct = topPositiveNetConcentrationPct(
        row.netRows,
      );
      const selectionScore = buildForwardSelectionScore({
        meanExpectancyR,
        medianExpectancyR,
        topWindowPnlConcentrationPct,
      });
      const meanProfitFactor =
        row.profitFactorCount > 0
          ? row.profitFactorSum / row.profitFactorCount
          : null;
      const forwardValidation: ScalpForwardValidationMetrics = {
        rollCount: row.rollCount,
        profitableWindowPct,
        meanExpectancyR,
        meanProfitFactor,
        maxDrawdownR: row.maxDrawdownR,
        minTradesPerWindow: row.minTradesPerWindow,
        selectionWindowDays: params.selectionWindowDays,
        forwardWindowDays: params.forwardWindowDays,
        weeklySlices: null,
        weeklyProfitablePct: null,
        weeklyMeanExpectancyR: null,
        weeklyMedianExpectancyR: null,
        weeklyWorstNetR: null,
        weeklyTopWeekPnlConcentrationPct: null,
        weeklyEvaluatedAtMs: null,
      };
      return {
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        deploymentId: row.deploymentId,
        rollCount: row.rollCount,
        profitableWindowPct,
        profitableWindows: row.profitableWindows,
        meanExpectancyR,
        medianExpectancyR,
        meanProfitFactor,
        maxDrawdownR: row.maxDrawdownR,
        topWindowPnlConcentrationPct,
        selectionScore,
        minTradesPerWindow: row.minTradesPerWindow,
        totalTrades: row.totalTrades,
        selectionWindowDays: params.selectionWindowDays,
        forwardWindowDays: params.forwardWindowDays,
        forwardValidation,
      };
    })
    .sort(compareCandidates);
}

export function buildForwardValidationByCandidate(
  cycle: ScalpResearchCycleSnapshot,
  tasks: ScalpResearchTask[],
): ScalpResearchForwardValidationCandidate[] {
  return buildForwardValidationByCandidateFromTasks({
    tasks,
    selectionWindowDays: cycle.params.lookbackDays,
    forwardWindowDays: cycle.params.chunkDays,
  });
}

export interface SyncResearchPromotionParams {
  cycleId?: string;
  dryRun?: boolean;
  requireCompletedCycle?: boolean;
  debug?: boolean;
  sources?: ScalpDeploymentRegistrySource[];
  updatedBy?: string;
  nowMs?: number;
  weeklyRobustnessEnabled?: boolean;
  weeklyRobustnessTopKPerSymbol?: number;
  weeklyRobustnessLookbackDays?: number;
  weeklyRobustnessMinCandlesPerSlice?: number;
  weeklyRobustnessRequireWinnerShortlist?: boolean;
  weeklyRobustnessMinSlices?: number;
  weeklyRobustnessMinProfitablePct?: number;
  weeklyRobustnessMinMedianExpectancyR?: number;
  weeklyRobustnessMaxTopWeekPnlConcentrationPct?: number;
  confirmationEnabled?: boolean;
  confirmationTopKPerSymbol?: number;
  confirmationLookbackDays?: number;
  materializeMissingCandidates?: boolean;
  materializeTopKPerSymbol?: number;
  materializeSource?: "matrix" | "backtest";
  materializeEnabled?: boolean;
  materializeMinTradesPerWindow?: number;
  materializeMinMeanExpectancyR?: number;
}

export interface SyncResearchMaterializationQualityPolicy {
  minTradesPerWindow: number;
  minMeanExpectancyR: number;
}

export function filterMaterializationCandidatesByQuality(
  candidates: ScalpResearchForwardValidationCandidate[],
  policy: SyncResearchMaterializationQualityPolicy,
): ScalpResearchForwardValidationCandidate[] {
  return candidates.filter((candidate) => {
    const minTradesPerWindow = Number.isFinite(
      Number(candidate.minTradesPerWindow),
    )
      ? Number(candidate.minTradesPerWindow)
      : 0;
    return (
      minTradesPerWindow >= policy.minTradesPerWindow &&
      Number(candidate.meanExpectancyR) >= policy.minMeanExpectancyR
    );
  });
}

export interface SyncResearchPromotionRow {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  source: ScalpDeploymentRegistrySource;
  inWinnerShortlist: boolean;
  weeklyRobustness: ScalpWeeklyRobustnessMetrics | null;
  weeklyGateReason: string | null;
  matchedCandidate: Pick<
    ScalpResearchForwardValidationCandidate,
    | "rollCount"
    | "profitableWindowPct"
    | "meanExpectancyR"
    | "meanProfitFactor"
    | "maxDrawdownR"
    | "minTradesPerWindow"
  > | null;
  previousGate: ScalpDeploymentPromotionGate | null;
  nextGate: ScalpDeploymentPromotionGate | null;
  changed: boolean;
}

export interface SyncResearchPromotionResult {
  ok: boolean;
  cycleId: string | null;
  cycleStatus: string | null;
  dryRun: boolean;
  requireCompletedCycle: boolean;
  reason: string | null;
  weeklyPolicy: SyncResearchWeeklyPolicy;
  confirmationPolicy: SyncResearchConfirmationPolicy | null;
  candidates: ScalpResearchForwardValidationCandidate[];
  materialization: {
    enabled: boolean;
    source: "matrix" | "backtest";
    topKPerSymbol: number;
    qualityPolicy: SyncResearchMaterializationQualityPolicy;
    qualityEligibleCandidates: number;
    qualityRejectedCandidates: number;
    shortlistedCandidates: number;
    missingCandidates: number;
    createdCandidates: number;
    rows: Array<{
      deploymentId: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      source: "matrix" | "backtest";
      exists: boolean;
      created: boolean;
    }>;
  };
  deploymentsConsidered: number;
  deploymentsMatched: number;
  deploymentsUpdated: number;
  rows: SyncResearchPromotionRow[];
}

function resolveMaterializeSource(
  value: unknown,
  fallback: "matrix" | "backtest",
): "matrix" | "backtest" {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "matrix" || normalized === "backtest") return normalized;
  return fallback;
}

function changedPromotionGate(
  a: ScalpDeploymentPromotionGate | null | undefined,
  b: ScalpDeploymentPromotionGate | null | undefined,
): boolean {
  return JSON.stringify(a || null) !== JSON.stringify(b || null);
}

function buildForcedIneligibleGate(params: {
  baseGate: ScalpDeploymentPromotionGate | null;
  reason: string;
  forwardValidation: ScalpForwardValidationMetrics;
  nowMs: number;
}): ScalpDeploymentPromotionGate {
  return {
    eligible: false,
    reason: params.reason,
    source: "walk_forward",
    evaluatedAtMs: params.nowMs,
    forwardValidation:
      params.baseGate?.forwardValidation || params.forwardValidation,
    thresholds: params.baseGate?.thresholds || null,
  };
}

export function buildBestEligibleTuneDeploymentIdSet(params: {
  deployments: Array<
    Pick<
      ScalpDeploymentRegistryEntry,
      "deploymentId" | "symbol" | "strategyId" | "tuneId" | "promotionGate"
    >
  >;
  candidates: ScalpResearchForwardValidationCandidate[];
}): Set<string> {
  const candidateByKey = new Map(
    params.candidates.map((row) => [keyOf(row.symbol, row.strategyId, row.tuneId), row]),
  );
  const bestBySymbolStrategy = new Map<
    string,
    {
      deploymentId: string;
      candidate: ScalpResearchForwardValidationCandidate;
    }
  >();

  for (const deployment of params.deployments) {
    if (!deployment.promotionGate?.eligible) continue;
    const candidate = candidateByKey.get(
      keyOf(deployment.symbol, deployment.strategyId, deployment.tuneId),
    );
    if (!candidate) continue;
    const symbolStrategyKey = strategyKeyOf(
      deployment.symbol,
      deployment.strategyId,
    );
    const current = bestBySymbolStrategy.get(symbolStrategyKey) || null;
    if (!current || compareCandidates(candidate, current.candidate) < 0) {
      bestBySymbolStrategy.set(symbolStrategyKey, {
        deploymentId: deployment.deploymentId,
        candidate,
      });
    }
  }

  return new Set(
    Array.from(bestBySymbolStrategy.values()).map((row) => row.deploymentId),
  );
}

type PromotionSyncStateSnapshot = {
  version: 1;
  signature: string;
  cycleId: string;
  syncedAtMs: number;
  deploymentsConsidered: number;
  deploymentsMatched: number;
  deploymentsUpdated: number;
  materializationCreated: number;
};

export type PromotionSyncProgressStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed";

export type PromotionSyncProgressSnapshot = {
  version: 1;
  status: PromotionSyncProgressStatus;
  cycleId: string | null;
  dryRun: boolean;
  requireCompletedCycle: boolean;
  phase: string | null;
  startedAtMs: number | null;
  updatedAtMs: number;
  finishedAtMs: number | null;
  totalDeployments: number | null;
  processedDeployments: number;
  matchedDeployments: number;
  updatedDeployments: number;
  currentSymbol: string | null;
  currentStrategyId: string | null;
  currentTuneId: string | null;
  reason: string | null;
  lastError: string | null;
};

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function normalizePromotionSyncState(
  raw: unknown,
): PromotionSyncStateSnapshot | null {
  const row = asRecord(raw);
  const signature = String(row.signature || "").trim();
  const cycleId = String(row.cycleId || "").trim();
  const syncedAtMs = Number(row.syncedAtMs);
  if (!signature || !cycleId || !Number.isFinite(syncedAtMs) || syncedAtMs <= 0)
    return null;
  return {
    version: 1,
    signature,
    cycleId,
    syncedAtMs: Math.floor(syncedAtMs),
    deploymentsConsidered: Math.max(
      0,
      Math.floor(Number(row.deploymentsConsidered) || 0),
    ),
    deploymentsMatched: Math.max(
      0,
      Math.floor(Number(row.deploymentsMatched) || 0),
    ),
    deploymentsUpdated: Math.max(
      0,
      Math.floor(Number(row.deploymentsUpdated) || 0),
    ),
    materializationCreated: Math.max(
      0,
      Math.floor(Number(row.materializationCreated) || 0),
    ),
  };
}

function normalizePromotionSyncProgressSnapshot(
  raw: unknown,
): PromotionSyncProgressSnapshot | null {
  const row = asRecord(raw);
  const status = String(row.status || "").trim().toLowerCase();
  if (
    status !== "queued" &&
    status !== "running" &&
    status !== "succeeded" &&
    status !== "failed"
  ) {
    return null;
  }
  const updatedAtMs = Number(row.updatedAtMs);
  if (!Number.isFinite(updatedAtMs) || updatedAtMs <= 0) return null;
  const cycleId = String(row.cycleId || "").trim() || null;
  const phase = String(row.phase || "").trim() || null;
  const currentSymbol = String(row.currentSymbol || "").trim().toUpperCase() || null;
  const currentStrategyId = String(row.currentStrategyId || "").trim().toLowerCase() || null;
  const currentTuneId = String(row.currentTuneId || "").trim().toLowerCase() || null;
  const reason = String(row.reason || "").trim() || null;
  const lastError = String(row.lastError || "").trim() || null;
  const startedAtMs = Number(row.startedAtMs);
  const finishedAtMs = Number(row.finishedAtMs);
  return {
    version: 1,
    status,
    cycleId,
    dryRun: toBool(row.dryRun, false),
    requireCompletedCycle: toBool(row.requireCompletedCycle, true),
    phase,
    startedAtMs:
      Number.isFinite(startedAtMs) && startedAtMs > 0
        ? Math.floor(startedAtMs)
        : null,
    updatedAtMs: Math.floor(updatedAtMs),
    finishedAtMs:
      Number.isFinite(finishedAtMs) && finishedAtMs > 0
        ? Math.floor(finishedAtMs)
        : null,
    totalDeployments: Number.isFinite(Number(row.totalDeployments))
      ? Math.max(0, Math.floor(Number(row.totalDeployments)))
      : null,
    processedDeployments: Math.max(
      0,
      Math.floor(Number(row.processedDeployments) || 0),
    ),
    matchedDeployments: Math.max(
      0,
      Math.floor(Number(row.matchedDeployments) || 0),
    ),
    updatedDeployments: Math.max(
      0,
      Math.floor(Number(row.updatedDeployments) || 0),
    ),
    currentSymbol,
    currentStrategyId,
    currentTuneId,
    reason,
    lastError,
  };
}

export async function loadPromotionSyncStateFromPg(): Promise<PromotionSyncStateSnapshot | null> {
  if (!isScalpPgConfigured()) return null;
  try {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{ payload: unknown }>>(Prisma.sql`
            SELECT payload
            FROM scalp_jobs
            WHERE kind = 'promotion_sync'::scalp_job_kind
              AND dedupe_key = ${PROMOTION_SYNC_STATE_DEDUPE_KEY}
            LIMIT 1;
        `);
    return normalizePromotionSyncState(rows[0]?.payload);
  } catch {
    return null;
  }
}

export async function loadPromotionSyncProgressSnapshot(): Promise<PromotionSyncProgressSnapshot | null> {
  if (!isScalpPgConfigured()) return null;
  try {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{ payload: unknown }>>(Prisma.sql`
            SELECT payload
            FROM scalp_jobs
            WHERE kind = 'promotion_sync'::scalp_job_kind
              AND dedupe_key = ${PROMOTION_SYNC_PROGRESS_DEDUPE_KEY}
            LIMIT 1;
        `);
    return normalizePromotionSyncProgressSnapshot(rows[0]?.payload);
  } catch {
    return null;
  }
}

async function savePromotionSyncStateToPg(
  snapshot: PromotionSyncStateSnapshot,
): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(
    Prisma.sql`
            INSERT INTO scalp_jobs(
                kind,
                dedupe_key,
                payload,
                status,
                attempts,
                max_attempts,
                scheduled_for,
                next_run_at,
                last_error
            )
            VALUES(
                'promotion_sync'::scalp_job_kind,
                ${PROMOTION_SYNC_STATE_DEDUPE_KEY},
                ${JSON.stringify(snapshot)}::jsonb,
                'succeeded'::scalp_job_status,
                1,
                1,
                NOW(),
                NOW(),
                NULL
            )
            ON CONFLICT(kind, dedupe_key)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                status = EXCLUDED.status,
                attempts = EXCLUDED.attempts,
                max_attempts = EXCLUDED.max_attempts,
                scheduled_for = EXCLUDED.scheduled_for,
                next_run_at = EXCLUDED.next_run_at,
                locked_by = NULL,
                locked_at = NULL,
                last_error = NULL,
                updated_at = NOW();
        `,
  );
}

export async function savePromotionSyncProgressSnapshot(
  snapshot: PromotionSyncProgressSnapshot,
): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  const jobStatus =
    snapshot.status === "failed"
      ? "failed_permanent"
      : snapshot.status === "succeeded"
        ? "succeeded"
        : "running";
  await db.$executeRaw(
    Prisma.sql`
            INSERT INTO scalp_jobs(
                kind,
                dedupe_key,
                payload,
                status,
                attempts,
                max_attempts,
                scheduled_for,
                next_run_at,
                last_error
            )
            VALUES(
                'promotion_sync'::scalp_job_kind,
                ${PROMOTION_SYNC_PROGRESS_DEDUPE_KEY},
                ${JSON.stringify(snapshot)}::jsonb,
                ${jobStatus}::scalp_job_status,
                1,
                1,
                NOW(),
                NOW(),
                ${snapshot.lastError || null}
            )
            ON CONFLICT(kind, dedupe_key)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                status = EXCLUDED.status,
                attempts = EXCLUDED.attempts,
                max_attempts = EXCLUDED.max_attempts,
                scheduled_for = EXCLUDED.scheduled_for,
                next_run_at = EXCLUDED.next_run_at,
                locked_by = NULL,
                locked_at = NULL,
                last_error = EXCLUDED.last_error,
                updated_at = NOW();
        `,
  );
}

function buildPromotionSyncSignature(params: {
  cycle: ScalpResearchCycleSnapshot;
  allowedSources: Set<ScalpDeploymentRegistrySource>;
  weeklyPolicy: SyncResearchWeeklyPolicy;
  confirmationPolicy: SyncResearchConfirmationPolicy;
  materializeMissingCandidates: boolean;
  materializeTopKPerSymbol: number;
  materializationQualityPolicy: SyncResearchMaterializationQualityPolicy;
  materializeSource: "matrix" | "backtest";
  materializeEnabled: boolean;
  deployments: ScalpDeploymentRegistryEntry[];
}): string {
  const allowedSources = Array.from(params.allowedSources).sort();
  const deploymentStamp = params.deployments
    .map((row) =>
      [
        row.deploymentId,
        row.source,
        row.enabled ? "1" : "0",
        row.promotionGate?.eligible ? "1" : "0",
        row.promotionGate?.reason || "",
        Number.isFinite(Number(row.updatedAtMs))
          ? String(Math.floor(Number(row.updatedAtMs)))
          : "0",
      ].join(":"),
    )
    .sort();
  return JSON.stringify({
    algorithmVersion: 2,
    cycleId: params.cycle.cycleId,
    cycleStatus: params.cycle.status,
    cycleUpdatedAtMs: params.cycle.updatedAtMs,
    allowedSources,
    weeklyPolicy: params.weeklyPolicy,
    confirmationPolicy: params.confirmationPolicy,
    materializeMissingCandidates: params.materializeMissingCandidates,
    materializeTopKPerSymbol: params.materializeTopKPerSymbol,
    materializationQualityPolicy: params.materializationQualityPolicy,
    materializeSource: params.materializeSource,
    materializeEnabled: params.materializeEnabled,
    deploymentStamp,
  });
}

export async function syncResearchCyclePromotionGates(
  params: SyncResearchPromotionParams = {},
): Promise<SyncResearchPromotionResult> {
  const debug = params.debug === true;
  const startedAtMs = Date.now();
  const emitSyncLog = (
    event: string,
    payload: Record<string, unknown>,
    force = false,
    level: "info" | "warn" = "info",
  ): void => {
    if (!force && !debug) return;
    const line = JSON.stringify({
      scope: "scalp_research_promotion_sync",
      event,
      elapsedMs: Date.now() - startedAtMs,
      ...payload,
    });
    if (level === "warn") console.warn(line);
    else console.info(line);
  };
  let progressSnapshot: PromotionSyncProgressSnapshot | null = null;
  const persistProgress = async (
    patch: Partial<PromotionSyncProgressSnapshot>,
  ): Promise<void> => {
    if (params.dryRun === true) return;
    const base: PromotionSyncProgressSnapshot =
      progressSnapshot || {
        version: 1,
        status: "running",
        cycleId: null,
        dryRun: false,
        requireCompletedCycle: params.requireCompletedCycle ?? true,
        phase: null,
        startedAtMs: startedAtMs,
        updatedAtMs: startedAtMs,
        finishedAtMs: null,
        totalDeployments: null,
        processedDeployments: 0,
        matchedDeployments: 0,
        updatedDeployments: 0,
        currentSymbol: null,
        currentStrategyId: null,
        currentTuneId: null,
        reason: null,
        lastError: null,
      };
    progressSnapshot = {
      ...base,
      ...patch,
      cycleId:
        patch.cycleId !== undefined
          ? patch.cycleId
          : base.cycleId,
      updatedAtMs:
        patch.updatedAtMs !== undefined
          ? Math.max(0, Math.floor(Number(patch.updatedAtMs) || Date.now()))
          : Date.now(),
      processedDeployments: Math.max(
        0,
        Math.floor(
          Number(
            patch.processedDeployments !== undefined
              ? patch.processedDeployments
              : base.processedDeployments,
          ) || 0,
        ),
      ),
      matchedDeployments: Math.max(
        0,
        Math.floor(
          Number(
            patch.matchedDeployments !== undefined
              ? patch.matchedDeployments
              : base.matchedDeployments,
          ) || 0,
        ),
      ),
      updatedDeployments: Math.max(
        0,
        Math.floor(
          Number(
            patch.updatedDeployments !== undefined
              ? patch.updatedDeployments
              : base.updatedDeployments,
          ) || 0,
        ),
      ),
    };
    await savePromotionSyncProgressSnapshot(progressSnapshot);
  };
  const requestedCycleId = String(params.cycleId || "").trim();
  let cycleId = requestedCycleId || (await loadActiveResearchCycleId());
  if (!cycleId) {
    cycleId = await loadLatestCompletedResearchCycleId();
  }
  const dryRun = Boolean(params.dryRun);
  const requireCompletedCycle = params.requireCompletedCycle ?? true;
  const allowedSources = new Set<ScalpDeploymentRegistrySource>(
    (params.sources && params.sources.length
      ? params.sources
      : ["matrix", "backtest"]
    )
      .map((row) =>
        String(row || "")
          .trim()
          .toLowerCase(),
      )
      .filter(
        (row): row is ScalpDeploymentRegistrySource =>
          row === "manual" || row === "backtest" || row === "matrix",
      ),
  );
  const defaultMaterializeSource: "matrix" | "backtest" = allowedSources.has(
    "matrix",
  )
    ? "matrix"
    : allowedSources.has("backtest")
      ? "backtest"
      : "matrix";
  const materializeSource = resolveMaterializeSource(
    params.materializeSource,
    defaultMaterializeSource,
  );
  const materializeMissingCandidates =
    params.materializeMissingCandidates ??
    envOrFallbackBool("SCALP_RESEARCH_MATERIALIZE_MISSING_CANDIDATES", true);
  const materializeEnabled =
    params.materializeEnabled ??
    envOrFallbackBool("SCALP_RESEARCH_MATERIALIZE_ENABLED", true);
  const materializeTopKPerSymbol = toPositiveInt(
    params.materializeTopKPerSymbol ??
      envOrFallbackNumber("SCALP_RESEARCH_MATERIALIZE_TOPK_PER_SYMBOL", 2),
    2,
  );
  const materializationQualityPolicy: SyncResearchMaterializationQualityPolicy =
    {
      minTradesPerWindow: toNonNegativeInt(
        params.materializeMinTradesPerWindow ??
          envOrFallbackNumber(
            "SCALP_RESEARCH_MATERIALIZE_MIN_TRADES_PER_WINDOW",
            2,
          ),
        2,
      ),
      minMeanExpectancyR: toFinite(
        params.materializeMinMeanExpectancyR ??
          envOrFallbackNumber(
            "SCALP_RESEARCH_MATERIALIZE_MIN_MEAN_EXPECTANCY_R",
            0,
          ),
        0,
      ),
    };
  const emptyMaterialization = {
    enabled: materializeMissingCandidates,
    source: materializeSource,
    topKPerSymbol: materializeTopKPerSymbol,
    qualityPolicy: materializationQualityPolicy,
    qualityEligibleCandidates: 0,
    qualityRejectedCandidates: 0,
    shortlistedCandidates: 0,
    missingCandidates: 0,
    createdCandidates: 0,
    rows: [] as Array<{
      deploymentId: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      source: "matrix" | "backtest";
      exists: boolean;
      created: boolean;
    }>,
  };

  const fallbackPolicy: SyncResearchWeeklyPolicy = {
    enabled: true,
    topKPerSymbol: 2,
    lookbackDays: 90,
    minCandlesPerSlice: 180,
    requireWinnerShortlist: true,
    minSlices: 8,
    minProfitablePct: 45,
    minMedianExpectancyR: 0,
    maxTopWeekPnlConcentrationPct: 80,
  };

  if (!cycleId) {
    emitSyncLog(
      "early_exit_cycle_not_found",
      {
        requestedCycleId: requestedCycleId || null,
      },
      true,
      "warn",
    );
    return {
      ok: false,
      cycleId: null,
      cycleStatus: null,
      dryRun,
      requireCompletedCycle,
      reason: "cycle_not_found",
      weeklyPolicy: fallbackPolicy,
      confirmationPolicy: null,
      candidates: [],
      materialization: emptyMaterialization,
      deploymentsConsidered: 0,
      deploymentsMatched: 0,
      deploymentsUpdated: 0,
      rows: [],
    };
  }

  const cycle = await loadResearchCycle(cycleId);
  if (!cycle) {
    emitSyncLog(
      "early_exit_cycle_not_loaded",
      {
        cycleId,
      },
      true,
      "warn",
    );
    return {
      ok: false,
      cycleId,
      cycleStatus: null,
      dryRun,
      requireCompletedCycle,
      reason: "cycle_not_found",
      weeklyPolicy: fallbackPolicy,
      confirmationPolicy: null,
      candidates: [],
      materialization: emptyMaterialization,
      deploymentsConsidered: 0,
      deploymentsMatched: 0,
      deploymentsUpdated: 0,
      rows: [],
    };
  }

  if (requireCompletedCycle && cycle.status !== "completed") {
    emitSyncLog(
      "early_exit_cycle_not_completed",
      {
        cycleId,
        cycleStatus: cycle.status,
      },
      true,
      "warn",
    );
    return {
      ok: false,
      cycleId,
      cycleStatus: cycle.status,
      dryRun,
      requireCompletedCycle,
      reason: "cycle_not_completed",
      weeklyPolicy: resolveWeeklyPolicy(params, cycle),
      confirmationPolicy: resolveConfirmationPolicy(params, cycle),
      candidates: [],
      materialization: emptyMaterialization,
      deploymentsConsidered: 0,
      deploymentsMatched: 0,
      deploymentsUpdated: 0,
      rows: [],
    };
  }

  const nowMs = Number.isFinite(Number(params.nowMs))
    ? Math.floor(Number(params.nowMs))
    : Date.now();
  const weeklyPolicy = resolveWeeklyPolicy(params, cycle);
  const confirmationPolicy = resolveConfirmationPolicy(params, cycle);
  await persistProgress({
    status: "running",
    cycleId,
    dryRun,
    requireCompletedCycle,
    phase: "load_cycle",
    startedAtMs,
    finishedAtMs: null,
    reason: null,
    lastError: null,
  });
  emitSyncLog("cycle_loaded", {
    cycleId,
    cycleStatus: cycle.status,
    dryRun,
    requireCompletedCycle,
    weeklyPolicy,
    confirmationPolicy,
  });

  const deploymentSnapshot = await loadScalpDeploymentRegistry();
  let deployments = deploymentSnapshot.deployments.slice();
  await persistProgress({
    phase: "load_deployments",
    totalDeployments: deployments.length,
  });
  emitSyncLog("deployments_loaded", {
    totalDeployments: deployments.length,
    allowedSources: Array.from(allowedSources).sort(),
  });
  const preSyncSignature = buildPromotionSyncSignature({
    cycle,
    allowedSources,
    weeklyPolicy,
    confirmationPolicy,
    materializeMissingCandidates,
    materializeTopKPerSymbol,
    materializationQualityPolicy,
    materializeSource,
    materializeEnabled,
    deployments,
  });
  const preSyncConsidered = deployments.filter((row) =>
    allowedSources.has(row.source),
  ).length;

  if (!dryRun) {
    const lastSync = await loadPromotionSyncStateFromPg();
    if (lastSync?.signature === preSyncSignature) {
      await persistProgress({
        status: "succeeded",
        cycleId,
        phase: "already_current",
        finishedAtMs: Date.now(),
        totalDeployments: preSyncConsidered,
        processedDeployments: preSyncConsidered,
        currentSymbol: null,
        currentStrategyId: null,
        currentTuneId: null,
        reason: "sync_already_current",
        lastError: null,
      });
      emitSyncLog("early_exit_sync_already_current", {
        cycleId,
        deploymentsConsidered: preSyncConsidered,
        lastSyncedAtMs: lastSync.syncedAtMs,
      });
      return {
        ok: true,
        cycleId,
        cycleStatus: cycle.status,
        dryRun,
        requireCompletedCycle,
        reason: "sync_already_current",
        weeklyPolicy,
        confirmationPolicy,
        candidates: [],
        materialization: emptyMaterialization,
        deploymentsConsidered: Number.isFinite(
          Number(lastSync.deploymentsConsidered),
        )
          ? Number(lastSync.deploymentsConsidered)
          : preSyncConsidered,
        deploymentsMatched: Number.isFinite(Number(lastSync.deploymentsMatched))
          ? Number(lastSync.deploymentsMatched)
          : 0,
        deploymentsUpdated: Number.isFinite(Number(lastSync.deploymentsUpdated))
          ? Number(lastSync.deploymentsUpdated)
          : 0,
        rows: [],
      };
    }
  }

  const selectionWindowToTs = startOfWeekMondayUtc(nowMs);
  const selectionWindowFromTs =
    selectionWindowToTs - Math.max(1, cycle.params.lookbackDays) * DAY_MS;
  const candidateSymbols = Array.from(
    new Set(
      deployments
        .filter((row) => allowedSources.has(row.source))
        .map((row) => row.symbol)
        .filter((row) => Boolean(row)),
    ),
  );
  const canonicalTasks = await listLatestResearchTasksByWindow({
    symbols: candidateSymbols,
    windowFromTs: selectionWindowFromTs,
    windowToTs: selectionWindowToTs,
  });
  emitSyncLog("canonical_history_tasks_loaded", {
    cycleId,
    symbols: candidateSymbols.length,
    taskCount: canonicalTasks.length,
    windowFromTs: selectionWindowFromTs,
    windowToTs: selectionWindowToTs,
  });
  const candidates = buildForwardValidationByCandidateFromTasks({
    tasks: canonicalTasks,
    selectionWindowDays: cycle.params.lookbackDays,
    forwardWindowDays: inferForwardWindowDaysFromTasks(
      canonicalTasks,
      cycle.params.chunkDays,
    ),
  });
  emitSyncLog("canonical_history_candidates_built", {
    cycleId,
    candidateCount: candidates.length,
  });
  const materializationCandidatePool = filterMaterializationCandidatesByQuality(
    candidates,
    materializationQualityPolicy,
  );
  const materializationRejectedByQuality = Math.max(
    0,
    candidates.length - materializationCandidatePool.length,
  );
  const candidateByKey = new Map(
    candidates.map((row) => [
      keyOf(row.symbol, row.strategyId, row.tuneId),
      row,
    ]),
  );
  const candidatesBySymbolStrategy = new Map<
    string,
    ScalpResearchForwardValidationCandidate[]
  >();
  for (const row of candidates) {
    const strategyKey = strategyKeyOf(row.symbol, row.strategyId);
    if (!candidatesBySymbolStrategy.has(strategyKey)) {
      candidatesBySymbolStrategy.set(strategyKey, []);
    }
    candidatesBySymbolStrategy.get(strategyKey)!.push(row);
  }
  for (const rows of candidatesBySymbolStrategy.values()) {
    rows.sort(compareCandidates);
  }
  const winnerCandidateKeys = buildWinnerCandidateKeySet(
    candidates,
    weeklyPolicy.topKPerSymbol,
  );
  const confirmationShortlist = confirmationPolicy.enabled
    ? buildCandidateMaterializationShortlist(
        candidates,
        confirmationPolicy.topKPerSymbol,
      )
    : [];
  const confirmationCandidateKeys = new Set(
    confirmationShortlist.map((row) =>
      keyOf(row.symbol, row.strategyId, row.tuneId),
    ),
  );
  const confirmationByKey = new Map<
    string,
    ScalpResearchForwardValidationCandidate
  >();
  if (confirmationCandidateKeys.size > 0) {
    const confirmationSymbols = Array.from(
      new Set(confirmationShortlist.map((row) => row.symbol)),
    );
    const confirmationWindowToTs = startOfWeekMondayUtc(nowMs);
    const confirmationWindowFromTs =
      confirmationWindowToTs -
      Math.max(1, confirmationPolicy.lookbackDays) * DAY_MS;
    const historicalTasks = await listLatestResearchTasksByWindow({
      symbols: confirmationSymbols,
      windowFromTs: confirmationWindowFromTs,
      windowToTs: confirmationWindowToTs,
    });
    emitSyncLog("confirmation_history_loaded", {
      confirmationSymbols: confirmationSymbols.length,
      confirmationCandidates: confirmationCandidateKeys.size,
      historicalTaskRows: historicalTasks.length,
      windowFromTs: confirmationWindowFromTs,
      windowToTs: confirmationWindowToTs,
    });
    const scopedHistoricalTasks = historicalTasks.filter((task) =>
      confirmationCandidateKeys.has(
        keyOf(task.symbol, task.strategyId, task.tuneId),
      ),
    );
    const confirmationRows = buildForwardValidationByCandidateFromTasks({
      tasks: scopedHistoricalTasks,
      selectionWindowDays: confirmationPolicy.lookbackDays,
      forwardWindowDays: inferForwardWindowDaysFromTasks(
        scopedHistoricalTasks,
        cycle.params.chunkDays,
      ),
    });
    for (const row of confirmationRows) {
      confirmationByKey.set(keyOf(row.symbol, row.strategyId, row.tuneId), row);
    }
    emitSyncLog("confirmation_candidates_built", {
      confirmationRows: confirmationRows.length,
    });
  }
  const materializationShortlist = buildCandidateMaterializationShortlist(
    materializationCandidatePool,
    materializeTopKPerSymbol,
  );
  emitSyncLog("materialization_shortlist_built", {
    qualityEligibleCandidates: materializationCandidatePool.length,
    qualityRejectedCandidates: materializationRejectedByQuality,
    shortlistedCandidates: materializationShortlist.length,
  });

  const deploymentIds = new Set(deployments.map((row) => row.deploymentId));
  const materializationRowDrafts: Array<{
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    source: "matrix" | "backtest";
    exists: boolean;
  }> = [];
  const materializationUpserts: ScalpDeploymentRegistryWriteParams[] = [];
  let materializationMissing = 0;

  for (const candidate of materializationShortlist) {
    const exists = deploymentIds.has(candidate.deploymentId);
    if (!exists) {
      materializationMissing += 1;
      if (materializeMissingCandidates && !dryRun) {
        materializationUpserts.push({
          deploymentId: candidate.deploymentId,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          source: materializeSource,
          enabled: materializeEnabled,
          forwardValidation: candidate.forwardValidation,
          notes: `auto_materialized_from_cycle:${cycleId}`,
          updatedBy: params.updatedBy || "research-cycle-sync",
        });
      }
    }
    materializationRowDrafts.push({
      deploymentId: candidate.deploymentId,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
      source: materializeSource,
      exists,
    });
  }

  const materializedDeploymentIds = new Set<string>();
  if (materializationUpserts.length > 0) {
    const upserted = await upsertScalpDeploymentRegistryEntriesBulk(
      materializationUpserts,
    );
    deployments = upserted.snapshot.deployments.slice();
    for (const row of upserted.entries) {
      materializedDeploymentIds.add(row.deploymentId);
    }
  }

  const materializationRows = materializationRowDrafts.map((row) => ({
    ...row,
    created: materializedDeploymentIds.has(row.deploymentId),
  }));
  const materialization = {
    enabled: materializeMissingCandidates,
    source: materializeSource,
    topKPerSymbol: materializeTopKPerSymbol,
    qualityPolicy: materializationQualityPolicy,
    qualityEligibleCandidates: materializationCandidatePool.length,
    qualityRejectedCandidates: materializationRejectedByQuality,
    shortlistedCandidates: materializationShortlist.length,
    missingCandidates: materializationMissing,
    createdCandidates: materializedDeploymentIds.size,
    rows: materializationRows,
  };

  const considered = deployments.filter((row) =>
    allowedSources.has(row.source),
  );
  await persistProgress({
    phase: "evaluate_deployments",
    totalDeployments: considered.length,
  });
  emitSyncLog("considered_deployments_built", {
    consideredDeployments: considered.length,
  });

  const rows: SyncResearchPromotionRow[] = [];
  let deploymentsMatched = 0;
  let deploymentsUpdated = 0;
  const updatedDeploymentIds = new Set<string>();

  const candlesBySymbol = new Map<string, CandleRow[]>();
  const symbolMetadataBySymbol = new Map<
    string,
    ScalpSymbolMarketMetadata | null
  >();
  const rowDrafts: Array<
    Omit<SyncResearchPromotionRow, "nextGate" | "changed">
  > = [];
  const baselineUpserts: ScalpDeploymentRegistryWriteParams[] = [];
  const forcedUpserts: ScalpDeploymentRegistryWriteParams[] = [];
  const forcedFromBaseline = new Map<
    string,
    {
      source: ScalpDeploymentRegistrySource;
      enabled: boolean;
      reason: string;
      forwardValidation: ScalpForwardValidationMetrics;
    }
  >();

  for (let deploymentIndex = 0; deploymentIndex < considered.length; deploymentIndex += 1) {
    const deployment = considered[deploymentIndex];
    if (
      debug &&
      (deploymentIndex < 3 ||
        (deploymentIndex + 1) % 25 === 0 ||
        deploymentIndex === considered.length - 1)
    ) {
      emitSyncLog("deployment_progress", {
        processed: deploymentIndex,
        total: considered.length,
        symbol: deployment.symbol,
        strategyId: deployment.strategyId,
        tuneId: deployment.tuneId,
      });
      await persistProgress({
        phase: "evaluate_deployments",
        totalDeployments: considered.length,
        processedDeployments: deploymentIndex,
        matchedDeployments: deploymentsMatched,
        updatedDeployments: deploymentsUpdated,
        currentSymbol: deployment.symbol,
        currentStrategyId: deployment.strategyId,
        currentTuneId: deployment.tuneId,
      });
    }
    const candidateKey = keyOf(
      deployment.symbol,
      deployment.strategyId,
      deployment.tuneId,
    );
    const candidate =
      candidateByKey.get(candidateKey) ||
      candidatesBySymbolStrategy.get(
        strategyKeyOf(deployment.symbol, deployment.strategyId),
      )?.[0] ||
      null;
    if (!candidate) {
      const weeklyGateReason = "missing_cycle_candidate";
      const draft: Omit<SyncResearchPromotionRow, "nextGate" | "changed"> = {
        deploymentId: deployment.deploymentId,
        symbol: deployment.symbol,
        strategyId: deployment.strategyId,
        tuneId: deployment.tuneId,
        source: deployment.source,
        inWinnerShortlist: false,
        weeklyRobustness: null,
        weeklyGateReason,
        matchedCandidate: null,
        previousGate: deployment.promotionGate,
      };

      if (dryRun) {
        rows.push({
          ...draft,
          nextGate: null,
          changed: false,
        });
        await persistProgress({
          phase: "evaluate_deployments",
          totalDeployments: considered.length,
          processedDeployments: deploymentIndex + 1,
          matchedDeployments: deploymentsMatched,
          updatedDeployments: deploymentsUpdated,
          currentSymbol: deployment.symbol,
          currentStrategyId: deployment.strategyId,
          currentTuneId: deployment.tuneId,
        });
        continue;
      }

      const forcedGate = buildForcedIneligibleGate({
        baseGate: deployment.promotionGate,
        reason: weeklyGateReason,
        forwardValidation: deployment.promotionGate?.forwardValidation || {
          rollCount: 0,
          profitableWindowPct: 0,
          meanExpectancyR: 0,
          meanProfitFactor: null,
          maxDrawdownR: null,
          minTradesPerWindow: null,
          selectionWindowDays: null,
          forwardWindowDays: null,
        },
        nowMs,
      });
      forcedUpserts.push({
        deploymentId: deployment.deploymentId,
        source: deployment.source,
        enabled: deployment.enabled,
        promotionGate: forcedGate,
        updatedBy: params.updatedBy || "research-cycle-sync",
      });
      rowDrafts.push(draft);
      await persistProgress({
        phase: "evaluate_deployments",
        totalDeployments: considered.length,
        processedDeployments: deploymentIndex + 1,
        matchedDeployments: deploymentsMatched,
        updatedDeployments: deploymentsUpdated,
        currentSymbol: deployment.symbol,
        currentStrategyId: deployment.strategyId,
        currentTuneId: deployment.tuneId,
      });
      continue;
    }
    deploymentsMatched += 1;

    const matchedCandidate = {
      rollCount: candidate.rollCount,
      profitableWindowPct: candidate.profitableWindowPct,
      meanExpectancyR: candidate.meanExpectancyR,
      meanProfitFactor: candidate.meanProfitFactor,
      maxDrawdownR: candidate.maxDrawdownR,
      minTradesPerWindow: candidate.minTradesPerWindow,
    };

    const inWinnerShortlist = winnerCandidateKeys.has(
      keyOf(candidate.symbol, candidate.strategyId, candidate.tuneId),
    );
    const confirmation = confirmationByKey.get(candidateKey) || null;
    let weeklyRobustness: ScalpWeeklyRobustnessMetrics | null = null;
    let weeklyGateReason: string | null = null;

    if (weeklyPolicy.enabled) {
      if (!inWinnerShortlist && weeklyPolicy.requireWinnerShortlist) {
        weeklyGateReason = "not_in_90d_winner_shortlist";
      } else if (inWinnerShortlist) {
        if (!candlesBySymbol.has(deployment.symbol)) {
          emitSyncLog("weekly_robustness_load_candles", {
            symbol: deployment.symbol,
          });
          const history = await loadScalpCandleHistory(deployment.symbol, "1m");
          candlesBySymbol.set(
            deployment.symbol,
            (history.record?.candles || []) as CandleRow[],
          );
          emitSyncLog("weekly_robustness_candles_loaded", {
            symbol: deployment.symbol,
            candles: (history.record?.candles || []).length,
          });
        }
        if (!symbolMetadataBySymbol.has(deployment.symbol)) {
          emitSyncLog("weekly_robustness_load_symbol_meta", {
            symbol: deployment.symbol,
          });
          symbolMetadataBySymbol.set(
            deployment.symbol,
            await loadScalpSymbolMarketMetadata(deployment.symbol),
          );
        }
        const symbolCandles = candlesBySymbol.get(deployment.symbol) || [];
        weeklyRobustness = await runWeeklyRobustnessForDeployment({
          deployment,
          candles: symbolCandles,
          symbolMeta: symbolMetadataBySymbol.get(deployment.symbol) || null,
          nowMs,
          lookbackDays: weeklyPolicy.lookbackDays,
          minCandlesPerSlice: weeklyPolicy.minCandlesPerSlice,
        });
        emitSyncLog("weekly_robustness_completed", {
          symbol: deployment.symbol,
          strategyId: deployment.strategyId,
          tuneId: deployment.tuneId,
          slices: weeklyRobustness?.slices ?? 0,
          profitablePct: weeklyRobustness?.profitablePct ?? null,
        });
        const weeklyGate = evaluateWeeklyRobustnessGate(
          weeklyRobustness,
          weeklyPolicy,
        );
        if (!weeklyGate.passed) {
          weeklyGateReason = weeklyGate.reason || "weekly_robustness_failed";
        }
      }
    }

    const forwardValidation: ScalpForwardValidationMetrics = {
      ...candidate.forwardValidation,
      weeklySlices: weeklyRobustness?.slices ?? null,
      weeklyProfitablePct: weeklyRobustness?.profitablePct ?? null,
      weeklyMeanExpectancyR: weeklyRobustness?.meanExpectancyR ?? null,
      weeklyMedianExpectancyR: weeklyRobustness?.medianExpectancyR ?? null,
      weeklyWorstNetR: weeklyRobustness?.worstNetR ?? null,
      weeklyTopWeekPnlConcentrationPct:
        weeklyRobustness?.topWeekPnlConcentrationPct ?? null,
      weeklyEvaluatedAtMs: weeklyRobustness?.evaluatedAtMs ?? null,
      confirmationWindowDays:
        confirmation?.forwardValidation.selectionWindowDays ??
        (confirmationCandidateKeys.has(candidateKey)
          ? confirmationPolicy.lookbackDays
          : null),
      confirmationForwardWindowDays:
        confirmation?.forwardValidation.forwardWindowDays ?? null,
      confirmationRollCount: confirmation?.rollCount ?? null,
      confirmationProfitableWindowPct:
        confirmation?.profitableWindowPct ?? null,
      confirmationMeanExpectancyR: confirmation?.meanExpectancyR ?? null,
      confirmationMeanProfitFactor: confirmation?.meanProfitFactor ?? null,
      confirmationMaxDrawdownR: confirmation?.maxDrawdownR ?? null,
      confirmationMinTradesPerWindow:
        confirmation?.minTradesPerWindow ?? null,
      confirmationTotalTrades: confirmation?.totalTrades ?? null,
      confirmationEvaluatedAtMs: confirmation ? nowMs : null,
    };

    const draft: Omit<SyncResearchPromotionRow, "nextGate" | "changed"> = {
      deploymentId: deployment.deploymentId,
      symbol: deployment.symbol,
      strategyId: deployment.strategyId,
      tuneId: deployment.tuneId,
      source: deployment.source,
      inWinnerShortlist,
      weeklyRobustness,
      weeklyGateReason,
      matchedCandidate,
      previousGate: deployment.promotionGate,
    };

    if (dryRun) {
      rows.push({
        ...draft,
        nextGate: null,
        changed: false,
      });
      await persistProgress({
        phase: "evaluate_deployments",
        totalDeployments: considered.length,
        processedDeployments: deploymentIndex + 1,
        matchedDeployments: deploymentsMatched,
        updatedDeployments: deploymentsUpdated,
        currentSymbol: deployment.symbol,
        currentStrategyId: deployment.strategyId,
        currentTuneId: deployment.tuneId,
      });
      continue;
    }

    baselineUpserts.push({
      deploymentId: deployment.deploymentId,
      source: deployment.source,
      enabled: deployment.enabled,
      forwardValidation,
      updatedBy: params.updatedBy || "research-cycle-sync",
    });
    if (weeklyGateReason) {
      forcedFromBaseline.set(deployment.deploymentId, {
        source: deployment.source,
        enabled: deployment.enabled,
        reason: weeklyGateReason,
        forwardValidation,
      });
    }
    rowDrafts.push(draft);
    await persistProgress({
      phase: "evaluate_deployments",
      totalDeployments: considered.length,
      processedDeployments: deploymentIndex + 1,
      matchedDeployments: deploymentsMatched,
      updatedDeployments: deploymentsUpdated,
      currentSymbol: deployment.symbol,
      currentStrategyId: deployment.strategyId,
      currentTuneId: deployment.tuneId,
    });
  }

  if (!dryRun) {
    if (baselineUpserts.length > 0) {
      await persistProgress({
        phase: "persist_baseline",
        processedDeployments: considered.length,
        matchedDeployments: deploymentsMatched,
        updatedDeployments: deploymentsUpdated,
      });
      emitSyncLog("baseline_upserts_start", {
        count: baselineUpserts.length,
      });
      const baselineOut =
        await upsertScalpDeploymentRegistryEntriesBulk(baselineUpserts);
      deployments = baselineOut.snapshot.deployments.slice();
      emitSyncLog("baseline_upserts_completed", {
        count: baselineUpserts.length,
      });
    }

    if (forcedFromBaseline.size > 0) {
      const deploymentById = new Map(
        deployments.map((row) => [row.deploymentId, row] as const),
      );
      for (const [deploymentId, row] of forcedFromBaseline.entries()) {
        const baseGate =
          deploymentById.get(deploymentId)?.promotionGate || null;
        const forcedGate = buildForcedIneligibleGate({
          baseGate,
          reason: row.reason,
          forwardValidation: row.forwardValidation,
          nowMs,
        });
        forcedUpserts.push({
          deploymentId,
          source: row.source,
          enabled: row.enabled,
          promotionGate: forcedGate,
          updatedBy: params.updatedBy || "research-cycle-sync",
        });
      }
    }

    if (forcedUpserts.length > 0) {
      await persistProgress({
        phase: "persist_forced",
        processedDeployments: considered.length,
        matchedDeployments: deploymentsMatched,
        updatedDeployments: deploymentsUpdated,
      });
      emitSyncLog("forced_upserts_start", {
        count: forcedUpserts.length,
      });
      const forcedOut =
        await upsertScalpDeploymentRegistryEntriesBulk(forcedUpserts);
      deployments = forcedOut.snapshot.deployments.slice();
      emitSyncLog("forced_upserts_completed", {
        count: forcedUpserts.length,
      });
    }

    const currentConsideredDeployments = considered.map(
      (row) =>
        deployments.find((deployment) => deployment.deploymentId === row.deploymentId) ||
        row,
    );
    const autoEnableWinnerIds = buildBestEligibleTuneDeploymentIdSet({
      deployments: currentConsideredDeployments,
      candidates,
    });
    const enablementUpserts = currentConsideredDeployments
      .flatMap((deployment) => {
        const shouldEnable = autoEnableWinnerIds.has(deployment.deploymentId);
        if (deployment.enabled === shouldEnable) return [];
        updatedDeploymentIds.add(deployment.deploymentId);
        return [
          {
            deploymentId: deployment.deploymentId,
            source: deployment.source,
            enabled: shouldEnable,
            updatedBy: params.updatedBy || "research-cycle-sync",
          } satisfies ScalpDeploymentRegistryWriteParams,
        ];
      });

    if (enablementUpserts.length > 0) {
      await persistProgress({
        phase: "persist_enablement",
        processedDeployments: considered.length,
        matchedDeployments: deploymentsMatched,
        updatedDeployments: updatedDeploymentIds.size,
      });
      emitSyncLog("enablement_upserts_start", {
        count: enablementUpserts.length,
        winnerCount: autoEnableWinnerIds.size,
      });
      const enablementOut = await upsertScalpDeploymentRegistryEntriesBulk(
        enablementUpserts,
      );
      deployments = enablementOut.snapshot.deployments.slice();
      emitSyncLog("enablement_upserts_completed", {
        count: enablementUpserts.length,
        winnerCount: autoEnableWinnerIds.size,
      });
    }

    const nextByDeploymentId = new Map(
      deployments.map((row) => [row.deploymentId, row] as const),
    );
    for (const row of rowDrafts) {
      const nextGate =
        nextByDeploymentId.get(row.deploymentId)?.promotionGate || null;
      const changed = changedPromotionGate(row.previousGate, nextGate);
      if (changed) updatedDeploymentIds.add(row.deploymentId);
      rows.push({
        ...row,
        nextGate,
        changed,
      });
    }
    deploymentsUpdated = updatedDeploymentIds.size;
  }

  if (!dryRun) {
    const finalSignature = buildPromotionSyncSignature({
      cycle,
      allowedSources,
      weeklyPolicy,
      confirmationPolicy,
      materializeMissingCandidates,
      materializeTopKPerSymbol,
      materializationQualityPolicy,
      materializeSource,
      materializeEnabled,
      deployments,
    });
    await savePromotionSyncStateToPg({
      version: 1,
      signature: finalSignature,
      cycleId,
      syncedAtMs: nowMs,
      deploymentsConsidered: considered.length,
      deploymentsMatched,
      deploymentsUpdated,
      materializationCreated: materialization.createdCandidates,
    } as PromotionSyncStateSnapshot);
    await persistProgress({
      status: "succeeded",
      phase: "completed",
      processedDeployments: considered.length,
      matchedDeployments: deploymentsMatched,
      updatedDeployments: deploymentsUpdated,
      finishedAtMs: Date.now(),
      currentSymbol: null,
      currentStrategyId: null,
      currentTuneId: null,
      reason: null,
      lastError: null,
    });
  }

  emitSyncLog("sync_completed", {
    cycleId,
    consideredDeployments: considered.length,
    matchedDeployments: deploymentsMatched,
    updatedDeployments: deploymentsUpdated,
    rowCount: rows.length,
    materializedCreated: materialization.createdCandidates,
  });

  return {
    ok: true,
    cycleId,
    cycleStatus: cycle.status,
    dryRun,
    requireCompletedCycle,
    reason: null,
    weeklyPolicy,
    confirmationPolicy,
    candidates,
    materialization,
    deploymentsConsidered: considered.length,
    deploymentsMatched,
    deploymentsUpdated,
    rows,
  };
}

export function inferCyclePromotionSummaryRows(
  deployments: ScalpDeploymentRegistryEntry[],
): Array<{
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  source: ScalpDeploymentRegistrySource;
  enabled: boolean;
  promotionEligible: boolean;
  promotionReason: string | null;
}> {
  return deployments.map((row) => ({
    deploymentId: row.deploymentId,
    symbol: row.symbol,
    strategyId: row.strategyId,
    tuneId: row.tuneId,
    source: row.source,
    enabled: row.enabled,
    promotionEligible: Boolean(row.promotionGate?.eligible),
    promotionReason: row.promotionGate?.reason || null,
  }));
}
