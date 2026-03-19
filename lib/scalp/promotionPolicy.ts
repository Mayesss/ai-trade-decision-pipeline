import type {
  ScalpDeploymentRegistryEntry,
  ScalpForwardValidationMetrics,
} from "./deploymentRegistry";

const DAY_MS = 24 * 60 * 60_000;
const WEEK_MS = 7 * DAY_MS;

export interface ScalpWeeklyRobustnessMetrics {
  slices: number;
  profitableSlices: number;
  profitablePct: number;
  meanExpectancyR: number;
  trimmedMeanExpectancyR: number;
  p25ExpectancyR: number;
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
  globalMaxSymbols: number;
  globalMaxDeployments: number;
  lookbackDays: number;
  minCandlesPerSlice: number;
  requireWinnerShortlist: boolean;
  minSlices: number;
  minProfitablePct: number;
  minMedianExpectancyR: number;
  minP25ExpectancyR: number;
  minWorstNetR: number;
  maxTopWeekPnlConcentrationPct: number;
}

export interface ScalpPromotionTaskResult {
  symbol?: string;
  strategyId?: string;
  tuneId?: string;
  deploymentId?: string;
  trades?: number;
  expectancyR?: number;
  netR?: number;
  profitFactor?: number | null;
  maxDrawdownR?: number;
}

export interface ScalpPromotionTaskLike {
  deploymentId?: string;
  symbol?: string;
  strategyId?: string;
  tuneId?: string;
  status?: string;
  result?: ScalpPromotionTaskResult | null;
  windowFromTs: number;
  windowToTs: number;
}

export interface ScalpPromotionForwardValidationCandidate {
  symbol: string;
  strategyId: string;
  tuneId: string;
  deploymentId: string;
  rollCount: number;
  profitableWindowPct: number;
  profitableWindows: number;
  meanExpectancyR: number;
  trimmedMeanExpectancyR?: number | null;
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

function startOfUtcDay(tsMs: number): number {
  const d = new Date(tsMs);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate());
}

function startOfWeekMondayUtc(tsMs: number): number {
  const dayStartMs = startOfUtcDay(tsMs);
  const dayOfWeek = new Date(dayStartMs).getUTCDay();
  const daysSinceMonday = (dayOfWeek + 6) % 7;
  return dayStartMs - daysSinceMonday * DAY_MS;
}

function normalizePromotionTriggerWeeks(value: unknown, fallback: number): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, n);
}

function resolveDeploymentPromotionTriggerWeeks(raw?: unknown): number {
  const resolved = normalizePromotionTriggerWeeks(
    raw ?? process.env.SCALP_RESEARCH_DEPLOYMENT_PROMOTION_TRIGGER_WEEKS,
    12,
  );
  return Math.max(12, resolved);
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

function mean(values: number[]): number {
  if (!values.length) return 0;
  return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function quantile(values: number[], p: number): number {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const clampedP = Math.max(0, Math.min(1, p));
  const index = (sorted.length - 1) * clampedP;
  const low = Math.floor(index);
  const high = Math.ceil(index);
  const lowValue = sorted[low] as number;
  const highValue = sorted[high] as number;
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

function resolveSelectionTrimRatio(): number {
  const env = Number(process.env.SCALP_WEEKLY_SELECTION_TRIM_RATIO);
  if (!Number.isFinite(env)) return 0.15;
  return Math.max(0, Math.min(0.4, env));
}

function resolveSelectionShrinkageK(): number {
  const env = Number(process.env.SCALP_WEEKLY_SELECTION_SHRINKAGE_K);
  if (!Number.isFinite(env)) return 6;
  return Math.max(1, Math.min(52, Math.floor(env)));
}

function topPositiveNetConcentrationPct(values: number[]): number {
  const positiveNet = values.map((value) => Math.max(0, value));
  const totalPositive = positiveNet.reduce((acc, value) => acc + value, 0);
  if (totalPositive <= 0) return 100;
  const topPositive = positiveNet.length ? Math.max(...positiveNet) : 0;
  return (topPositive / totalPositive) * 100;
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

function candidateSelectionScore(
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
  const rollCount = Math.max(
    0,
    Math.floor(Number(candidate.rollCount) || 0),
  );
  const shrinkageK = resolveSelectionShrinkageK();
  const sampleWeight =
    rollCount > 0 ? rollCount / (rollCount + shrinkageK) : 0;
  return concentrationAdjusted * sampleWeight;
}

function candidateProfitFactorForRanking(
  candidate: Pick<ScalpPromotionForwardValidationCandidate, "meanProfitFactor">,
): number {
  const profitFactor = Number(candidate.meanProfitFactor);
  if (!Number.isFinite(profitFactor)) return Number.NEGATIVE_INFINITY;
  return profitFactor;
}

function compareCandidates(
  a: ScalpPromotionForwardValidationCandidate,
  b: ScalpPromotionForwardValidationCandidate,
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

function buildForwardSelectionScore(params: {
  meanExpectancyR: number;
  trimmedMeanExpectancyR: number;
  medianExpectancyR: number;
  topWindowPnlConcentrationPct: number;
  rollCount: number;
}): number {
  const robustMean =
    Number.isFinite(params.trimmedMeanExpectancyR)
      ? params.trimmedMeanExpectancyR
      : params.meanExpectancyR;
  const smoothedExpectancy = (robustMean + params.medianExpectancyR) / 2;
  const concentrationPenalty = Math.max(
    0,
    params.topWindowPnlConcentrationPct - 50,
  );
  const concentrationAdjusted = smoothedExpectancy * (1 - concentrationPenalty / 100);
  const shrinkageK = resolveSelectionShrinkageK();
  const rollCount = Math.max(0, Math.floor(params.rollCount || 0));
  const sampleWeight = rollCount > 0 ? rollCount / (rollCount + shrinkageK) : 0;
  return concentrationAdjusted * sampleWeight;
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
  if (metrics.p25ExpectancyR < policy.minP25ExpectancyR) {
    return {
      passed: false,
      reason: "weekly_p25_expectancy_below_threshold",
    };
  }
  if (metrics.worstNetR < policy.minWorstNetR) {
    return {
      passed: false,
      reason: "weekly_worst_net_r_below_threshold",
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

export function buildForwardValidationByCandidateFromTasks(params: {
  tasks: ScalpPromotionTaskLike[];
  selectionWindowDays: number;
  forwardWindowDays: number;
}): ScalpPromotionForwardValidationCandidate[] {
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
      const selectionTrimRatio = resolveSelectionTrimRatio();
      const profitableWindowPct = (row.profitableWindows / row.rollCount) * 100;
      const meanExpectancyR = row.expectancySum / row.rollCount;
      const trimmedMeanExpectancyR = trimmedMean(
        row.expectancyRows,
        selectionTrimRatio,
      );
      const medianExpectancyR = median(row.expectancyRows);
      const topWindowPnlConcentrationPct = topPositiveNetConcentrationPct(
        row.netRows,
      );
      const selectionScore = buildForwardSelectionScore({
        meanExpectancyR,
        trimmedMeanExpectancyR,
        medianExpectancyR,
        topWindowPnlConcentrationPct,
        rollCount: row.rollCount,
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
        weeklyTrimmedMeanExpectancyR: null,
        weeklyP25ExpectancyR: null,
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
        trimmedMeanExpectancyR,
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

export function evaluateFreshCompletedDeploymentWeeks(params: {
  tasks: Array<
    Pick<
      ScalpPromotionTaskLike,
      | "deploymentId"
      | "symbol"
      | "strategyId"
      | "tuneId"
      | "status"
      | "result"
      | "windowFromTs"
      | "windowToTs"
    >
  >;
  nowMs: number;
  requiredWeeks?: number;
}): {
  ready: boolean;
  requiredWeeks: number;
  completedWeeks: number;
  missingWeeks: number;
  missingWeekStarts: number[];
  windowFromTs: number;
  windowToTs: number;
  readyTasks: Array<
    Pick<
      ScalpPromotionTaskLike,
      | "deploymentId"
      | "symbol"
      | "strategyId"
      | "tuneId"
      | "status"
      | "result"
      | "windowFromTs"
      | "windowToTs"
    >
  >;
} {
  const requiredWeeks = resolveDeploymentPromotionTriggerWeeks(
    params.requiredWeeks,
  );
  const windowToTs = startOfWeekMondayUtc(params.nowMs);
  const windowFromTs = windowToTs - requiredWeeks * WEEK_MS;
  const completedTaskByWeekStart = new Map<
    number,
    Pick<
      ScalpPromotionTaskLike,
      | "deploymentId"
      | "symbol"
      | "strategyId"
      | "tuneId"
      | "status"
      | "result"
      | "windowFromTs"
      | "windowToTs"
    >
  >();

  for (const task of params.tasks) {
    if (task.status !== "completed" || !task.result) continue;
    const fromTs = Math.floor(Number(task.windowFromTs) || 0);
    const toTs = Math.floor(Number(task.windowToTs) || 0);
    if (toTs - fromTs !== WEEK_MS) continue;
    if (toTs > windowToTs) continue;
    completedTaskByWeekStart.set(fromTs, task);
  }

  const completedWeekStarts = Array.from(completedTaskByWeekStart.keys()).sort(
    (a, b) => a - b,
  );
  const latestCompleteWeekStart = windowToTs - WEEK_MS;
  const earliestCompleteWeekStart = completedWeekStarts[0] ?? null;

  let selectedWindowFromTs: number | null = null;
  let readyTasks: Array<
    Pick<
      ScalpPromotionTaskLike,
      | "deploymentId"
      | "symbol"
      | "strategyId"
      | "tuneId"
      | "status"
      | "result"
      | "windowFromTs"
      | "windowToTs"
    >
  > = [];

  if (earliestCompleteWeekStart !== null) {
    const minEndStart =
      earliestCompleteWeekStart + (requiredWeeks - 1) * WEEK_MS;
    for (
      let endWeekStart = latestCompleteWeekStart;
      endWeekStart >= minEndStart;
      endWeekStart -= WEEK_MS
    ) {
      const startWeekStart = endWeekStart - (requiredWeeks - 1) * WEEK_MS;
      const candidateTasks: Array<
        Pick<
          ScalpPromotionTaskLike,
          | "deploymentId"
          | "symbol"
          | "strategyId"
          | "tuneId"
          | "status"
          | "result"
          | "windowFromTs"
          | "windowToTs"
        >
      > = [];
      let missing = false;
      for (let i = 0; i < requiredWeeks; i += 1) {
        const weekStart = startWeekStart + i * WEEK_MS;
        const task = completedTaskByWeekStart.get(weekStart);
        if (!task) {
          missing = true;
          break;
        }
        candidateTasks.push(task);
      }
      if (!missing) {
        selectedWindowFromTs = startWeekStart;
        readyTasks = candidateTasks;
        break;
      }
    }
  }

  let completedWeeks = 0;
  const missingWeekStarts: number[] = [];
  for (let i = 0; i < requiredWeeks; i += 1) {
    const weekStart = windowFromTs + i * WEEK_MS;
    if (completedTaskByWeekStart.has(weekStart)) {
      completedWeeks += 1;
    } else {
      missingWeekStarts.push(weekStart);
    }
  }
  const missingWeeks = missingWeekStarts.length;
  const ready = selectedWindowFromTs !== null;
  const missingForOutput = ready ? [] : missingWeekStarts;
  const selectedFromTs =
    selectedWindowFromTs !== null ? selectedWindowFromTs : windowFromTs;
  const selectedToTs = selectedFromTs + requiredWeeks * WEEK_MS;

  return {
    ready,
    requiredWeeks,
    completedWeeks,
    missingWeeks,
    missingWeekStarts: missingForOutput,
    windowFromTs: selectedFromTs,
    windowToTs: selectedToTs,
    readyTasks,
  };
}

function shouldReplaceIncumbentTune(params: {
  incumbent: ScalpPromotionForwardValidationCandidate;
  challenger: ScalpPromotionForwardValidationCandidate;
}): boolean {
  if (compareCandidates(params.challenger, params.incumbent) >= 0) return false;
  const selectionScoreDelta =
    candidateSelectionScore(params.challenger) -
    candidateSelectionScore(params.incumbent);
  const profitableWindowPctDelta =
    params.challenger.profitableWindowPct - params.incumbent.profitableWindowPct;
  const profitFactorDelta =
    candidateProfitFactorForRanking(params.challenger) -
    candidateProfitFactorForRanking(params.incumbent);
  const maxDrawdownImprovement =
    params.incumbent.maxDrawdownR - params.challenger.maxDrawdownR;
  const meanExpectancyDelta =
    params.challenger.meanExpectancyR - params.incumbent.meanExpectancyR;
  if (selectionScoreDelta >= 0.05) return true;
  if (profitableWindowPctDelta >= 5 && profitFactorDelta >= 0) return true;
  if (profitFactorDelta >= 0.35 && maxDrawdownImprovement >= -0.25) return true;
  if (meanExpectancyDelta >= 0.08 && maxDrawdownImprovement >= 0) return true;
  return false;
}

export function buildBestEligibleTuneDeploymentIdSet(params: {
  deployments: Array<
    Pick<
      ScalpDeploymentRegistryEntry,
      | "deploymentId"
      | "symbol"
      | "strategyId"
      | "tuneId"
      | "promotionGate"
      | "enabled"
    >
  >;
  candidates: ScalpPromotionForwardValidationCandidate[];
}): Set<string> {
  const candidateByKey = new Map(
    params.candidates.map((row) => [keyOf(row.symbol, row.strategyId, row.tuneId), row]),
  );
  const eligibleBySymbolStrategy = new Map<
    string,
    Array<{
      deploymentId: string;
      enabled: boolean;
      candidate: ScalpPromotionForwardValidationCandidate;
    }>
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
    if (!eligibleBySymbolStrategy.has(symbolStrategyKey)) {
      eligibleBySymbolStrategy.set(symbolStrategyKey, []);
    }
    eligibleBySymbolStrategy.get(symbolStrategyKey)!.push({
      deploymentId: deployment.deploymentId,
      enabled: Boolean(deployment.enabled),
      candidate,
    });
  }

  const winnerIds = new Set<string>();
  for (const rows of eligibleBySymbolStrategy.values()) {
    if (!rows.length) continue;
    rows.sort((a, b) => compareCandidates(a.candidate, b.candidate));
    const best = rows[0] || null;
    if (!best) continue;
    const incumbent = rows.find((row) => row.enabled) || null;
    if (!incumbent || incumbent.deploymentId === best.deploymentId) {
      winnerIds.add(best.deploymentId);
      continue;
    }
    if (
      shouldReplaceIncumbentTune({
        incumbent: incumbent.candidate,
        challenger: best.candidate,
      })
    ) {
      winnerIds.add(best.deploymentId);
      continue;
    }
    winnerIds.add(incumbent.deploymentId);
  }

  return winnerIds;
}

export function buildGlobalSymbolRankedDeploymentIdSet(params: {
  deployments: Array<
    Pick<
      ScalpDeploymentRegistryEntry,
      | "deploymentId"
      | "symbol"
      | "strategyId"
      | "tuneId"
      | "promotionGate"
      | "enabled"
    >
  >;
  candidates: ScalpPromotionForwardValidationCandidate[];
  maxSymbols: number;
  maxPerSymbol: number;
  maxDeployments: number;
}): Set<string> {
  const maxSymbols = Math.max(1, toPositiveInt(params.maxSymbols, 9999));
  const maxPerSymbol = Math.max(1, toPositiveInt(params.maxPerSymbol, 1));
  const maxDeployments = Math.max(1, toPositiveInt(params.maxDeployments, 9999));

  const candidateByKey = new Map(
    params.candidates.map((row) => [keyOf(row.symbol, row.strategyId, row.tuneId), row]),
  );
  const bySymbol = new Map<
    string,
    Array<{
      deploymentId: string;
      candidate: ScalpPromotionForwardValidationCandidate;
    }>
  >();

  for (const deployment of params.deployments) {
    if (!deployment.promotionGate?.eligible) continue;
    const candidate = candidateByKey.get(
      keyOf(deployment.symbol, deployment.strategyId, deployment.tuneId),
    );
    if (!candidate) continue;
    const symbol = String(deployment.symbol || "").trim().toUpperCase();
    if (!symbol) continue;
    if (!bySymbol.has(symbol)) bySymbol.set(symbol, []);
    bySymbol.get(symbol)!.push({
      deploymentId: deployment.deploymentId,
      candidate,
    });
  }

  const symbolGroups = Array.from(bySymbol.entries())
    .map(([symbol, rows]) => {
      rows.sort((a, b) => compareCandidates(a.candidate, b.candidate));
      return { symbol, rows, leader: rows[0] || null };
    })
    .filter((row) => row.leader !== null);

  symbolGroups.sort((a, b) => {
    if (!a.leader || !b.leader) return a.symbol.localeCompare(b.symbol);
    const cmp = compareCandidates(a.leader.candidate, b.leader.candidate);
    if (cmp !== 0) return cmp;
    return a.symbol.localeCompare(b.symbol);
  });

  const selectedSymbols = symbolGroups.slice(0, Math.min(maxSymbols, symbolGroups.length));
  const rankedDeployments: Array<{
    deploymentId: string;
    symbol: string;
    candidate: ScalpPromotionForwardValidationCandidate;
  }> = [];

  for (const group of selectedSymbols) {
    for (const row of group.rows.slice(0, Math.min(maxPerSymbol, group.rows.length))) {
      rankedDeployments.push({
        deploymentId: row.deploymentId,
        symbol: group.symbol,
        candidate: row.candidate,
      });
    }
  }

  rankedDeployments.sort((a, b) => {
    const cmp = compareCandidates(a.candidate, b.candidate);
    if (cmp !== 0) return cmp;
    if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
    return String(a.deploymentId || "").localeCompare(String(b.deploymentId || ""));
  });

  return new Set(
    rankedDeployments
      .slice(0, Math.min(maxDeployments, rankedDeployments.length))
      .map((row) => row.deploymentId),
  );
}

export function buildWinnerCandidateKeySet(
  candidates: ScalpPromotionForwardValidationCandidate[],
  topKPerSymbol: number,
): Set<string> {
  const winnerKeys = new Set<string>();
  const bySymbol = new Map<string, ScalpPromotionForwardValidationCandidate[]>();
  const bestByStrategy = new Map<string, ScalpPromotionForwardValidationCandidate>();

  for (const candidate of candidates) {
    const strategyKey = strategyKeyOf(candidate.symbol, candidate.strategyId);
    const current = bestByStrategy.get(strategyKey) || null;
    if (!current || compareCandidates(candidate, current) < 0) {
      bestByStrategy.set(strategyKey, candidate);
    }
  }

  for (const row of bestByStrategy.values()) {
    if (!bySymbol.has(row.symbol)) {
      bySymbol.set(row.symbol, []);
    }
    bySymbol.get(row.symbol)!.push(row);
  }

  const topK = Math.max(1, toPositiveInt(topKPerSymbol, 1));
  for (const rows of bySymbol.values()) {
    rows.sort(compareCandidates);
    for (const row of rows.slice(0, Math.min(topK, rows.length))) {
      winnerKeys.add(keyOf(row.symbol, row.strategyId, row.tuneId));
    }
  }
  return winnerKeys;
}
