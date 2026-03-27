import crypto from "crypto";

import { fetchCapitalOpenPositionSnapshots } from "../capital";
import { loadScalpCandleHistoryInRange } from "../scalp/candleHistory";
import { pipSizeForScalpSymbol } from "../scalp/marketData";
import {
  defaultScalpReplayConfig,
  runScalpReplay,
} from "../scalp/replay/harness";
import type {
  ScalpReplayCandle,
  ScalpReplayTrade,
} from "../scalp/replay/types";

import {
  getScalpV2RuntimeConfig,
  isScalpV2RuntimeSymbolInScope,
} from "./config";
import {
  MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
  resolveModelGuidedComposerExecutionPlanFromBlocks,
} from "./composerExecution";
import { runScalpV2ExecuteCycle } from "./executeAdapter";
import {
  appendScalpV2ExecutionEvent,
  buildScalpV2JobResult,
  claimScalpV2Job,
  enforceScalpV2EnabledCap,
  finalizeScalpV2Job,
  listScalpV2Candidates,
  listScalpV2Deployments,
  listScalpV2LedgerRows,
  listScalpV2OpenPositions,
  loadScalpV2ResearchCursor,
  loadScalpV2RuntimeConfig,
  snapshotScalpV2DailyMetrics,
  toDeploymentId,
  trimScalpV2CandidatesByBudget,
  updateScalpV2CandidateStatuses,
  upsertScalpV2Candidates,
  upsertScalpV2Deployments,
  upsertScalpV2PositionSnapshot,
  upsertScalpV2ResearchCursor,
  upsertScalpV2ResearchHighlights,
} from "./db";
import {
  enforceCandidateBudgets,
  isScalpV2DiscoverSymbolAllowed,
  isScalpV2SundayUtc,
} from "./logic";
import {
  buildScalpV2ModelGuidedComposerGrid,
  resolveScalpV2CandidateEvaluationWindow,
  toScalpV2ResearchCursorKey,
} from "./research";
import { getScalpV2VenueAdapter } from "./venueAdapter";
import {
  resolveScalpV2CompletedWeekWindowToUtc,
  startOfScalpV2WeekMondayUtc,
} from "./weekWindows";
import type {
  ScalpV2ExecutionEvent,
  ScalpV2JobResult,
  ScalpV2Session,
  ScalpV2Venue,
} from "./types";

function hashScoreSeed(value: string): number {
  let hash = 0;
  const input = String(value || "");
  for (let idx = 0; idx < input.length; idx += 1) {
    hash = (hash << 5) - hash + input.charCodeAt(idx);
    hash |= 0;
  }
  const positive = Math.abs(hash);
  return positive % 1000;
}

function nowMs(): number {
  return Date.now();
}

const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;
const LIFECYCLE_SUSPEND_WINDOW_MS = 180 * ONE_DAY_MS;

type V2LifecycleState = "candidate" | "graduated" | "suspended" | "retired";

interface V2PromotionLifecycle {
  state: V2LifecycleState;
  tuneFamily: string;
  suspendedUntilMs: number | null;
  retiredUntilMs: number | null;
  suspensionEventsMs: number[];
  suspensionCount180d: number;
  lastSeatReleaseAtMs: number | null;
}

interface ScalpV2PromotionPolicy {
  minCompletedWeeks: number;
  minTradesPerWeek: number;
  minTotalTrades: number;
  minSlices: number;
  minProfitablePct: number;
  minMedianExpectancyR: number;
  minP25ExpectancyR: number;
  minWorstNetR: number;
  maxTopWeekPnlConcentrationPct: number;
  minFourWeekNetR: number;
  fourWeekGroupCount: number;
  fourWeekGroupSize: number;
  exactLoserSuspendMs: number;
  neighborSuspendMs: number;
  retireMs: number;
  retireOnSuspensionCount: number;
}

type PromotionFreshness = {
  ready: boolean;
  requiredWeeks: number;
  completedWeeks: number;
  missingWeeks: number;
  windowFromTs: number;
  windowToTs: number;
  missingWeekStarts: number[];
};

type WeeklyAggregationRow = {
  weekStartTs: number;
  trades: number;
  netR: number;
  expectancyR: number;
};

type WeeklyRobustnessMetrics = {
  slices: number;
  profitableSlices: number;
  profitablePct: number;
  meanExpectancyR: number;
  trimmedMeanExpectancyR: number;
  p25ExpectancyR: number;
  medianExpectancyR: number;
  worstNetR: number;
  topWeekPnlConcentrationPct: number;
  totalNetR: number;
  fourWeekGroupNetR: number[];
  fourWeekGroupsEvaluated: number;
  fourWeekMinNetR: number | null;
  minTradesPerWeekObserved: number;
  totalTrades: number;
  evaluatedAtMs: number;
};

type ScalpV2WorkerStageId = "a" | "b" | "c";

type ScalpV2WorkerStagePolicy = {
  id: ScalpV2WorkerStageId;
  weeks: number;
  minTrades: number;
  minNetR: number;
  minConsecutiveWinningWeeks: number;
  minProfitFactor: number;
  maxDrawdownR: number;
};

type ScalpV2WorkerPolicy = {
  allowSunday: boolean;
  maxBatchSize: number;
  maxCandidatesPerSession: number;
  stageA: ScalpV2WorkerStagePolicy;
  stageB: ScalpV2WorkerStagePolicy;
  stageC: ScalpV2WorkerStagePolicy;
  minCandles: number;
  maxHighlightsPerRun: number;
};

type ScalpV2WorkerStageResult = {
  id: ScalpV2WorkerStageId;
  weeks: number;
  fromTs: number;
  toTs: number;
  executed: boolean;
  passed: boolean;
  reason: string;
  candles: number;
  trades: number;
  netR: number;
  expectancyR: number;
  winRatePct: number;
  maxDrawdownR: number;
  profitFactor: number | null;
  winningWeeks: number;
  consecutiveWinningWeeks: number;
  durationMs: number;
};

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
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

function toFinite(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

function toBoundedPercent(value: unknown, fallback: number): number {
  const n = toFinite(value, fallback);
  return Math.max(0, Math.min(100, n));
}

function toPositiveInt(value: unknown, fallback: number, max = 100_000): number {
  const n = Math.floor(toFinite(value, fallback));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

function resolvePromotionPolicy(): ScalpV2PromotionPolicy {
  const minCompletedWeeks = Math.max(
    12,
    Math.min(
      52,
      toPositiveInt(process.env.SCALP_V2_PROMOTION_MIN_COMPLETED_WEEKS, 12, 52),
    ),
  );
  const minTradesPerWeek = Math.max(
    0,
    Math.min(
      500,
      Math.floor(
        toFinite(
          process.env.SCALP_V2_PROMOTION_MIN_TRADES_PER_WEEK,
          2,
        ),
      ),
    ),
  );
  const minTotalTradesDefault = Math.max(0, minCompletedWeeks * minTradesPerWeek);
  return {
    minCompletedWeeks,
    minTradesPerWeek,
    minTotalTrades: Math.max(
      0,
      Math.floor(
        toFinite(
          process.env.SCALP_V2_PROMOTION_MIN_TOTAL_TRADES,
          minTotalTradesDefault,
        ),
      ),
    ),
    minSlices: Math.max(
      2,
      Math.min(
        104,
        toPositiveInt(process.env.SCALP_V2_PROMOTION_MIN_SLICES, 8, 104),
      ),
    ),
    minProfitablePct: toBoundedPercent(
      process.env.SCALP_V2_PROMOTION_MIN_PROFITABLE_PCT,
      55,
    ),
    minMedianExpectancyR: toFinite(
      process.env.SCALP_V2_PROMOTION_MIN_MEDIAN_EXPECTANCY_R,
      0.02,
    ),
    minP25ExpectancyR: toFinite(
      process.env.SCALP_V2_PROMOTION_MIN_P25_EXPECTANCY_R,
      -0.02,
    ),
    minWorstNetR: toFinite(
      process.env.SCALP_V2_PROMOTION_MIN_WORST_NET_R,
      -1.5,
    ),
    maxTopWeekPnlConcentrationPct: toBoundedPercent(
      process.env.SCALP_V2_PROMOTION_MAX_TOP_WEEK_PNL_CONCENTRATION_PCT,
      55,
    ),
    minFourWeekNetR: toFinite(process.env.SCALP_V2_PROMOTION_MIN_4W_NET_R, 8),
    fourWeekGroupCount: Math.max(
      1,
      Math.min(
        13,
        toPositiveInt(process.env.SCALP_V2_PROMOTION_4W_GROUP_COUNT, 3, 13),
      ),
    ),
    fourWeekGroupSize: Math.max(
      2,
      Math.min(
        8,
        toPositiveInt(process.env.SCALP_V2_PROMOTION_4W_GROUP_SIZE, 4, 8),
      ),
    ),
    exactLoserSuspendMs:
      Math.max(
        1,
        Math.min(
          52,
          toPositiveInt(
            process.env.SCALP_V2_PROMOTION_EXACT_LOSER_SUSPEND_WEEKS,
            12,
            52,
          ),
        ),
      ) * ONE_WEEK_MS,
    neighborSuspendMs:
      Math.max(
        1,
        Math.min(
          52,
          toPositiveInt(
            process.env.SCALP_V2_PROMOTION_NEIGHBOR_SUSPEND_WEEKS,
            8,
            52,
          ),
        ),
      ) * ONE_WEEK_MS,
    retireMs:
      Math.max(
        7,
        Math.min(
          365,
          toPositiveInt(process.env.SCALP_V2_PROMOTION_RETIRE_DAYS, 180, 365),
        ),
      ) * ONE_DAY_MS,
    retireOnSuspensionCount: Math.max(
      2,
      Math.min(
        10,
        toPositiveInt(
          process.env.SCALP_V2_PROMOTION_RETIRE_ON_SUSPENSION_COUNT,
          3,
          10,
        ),
      ),
    ),
  };
}

function resolveWorkerPolicy(): ScalpV2WorkerPolicy {
  return {
    allowSunday: envBool("SCALP_V2_ALLOW_SUNDAY_WORKER", false),
    maxBatchSize: Math.max(
      1,
      Math.min(
        600,
        toPositiveInt(process.env.SCALP_V2_WORKER_BATCH_SIZE, 60, 600),
      ),
    ),
    maxCandidatesPerSession: Math.max(
      1,
      Math.min(
        128,
        toPositiveInt(
          process.env.SCALP_V2_WORKER_MAX_CANDIDATES_PER_SESSION,
          24,
          128,
        ),
      ),
    ),
    stageA: {
      id: "a",
      weeks: 4,
      minTrades: Math.max(
        0,
        Math.min(
          10_000,
          Math.floor(
            toFinite(process.env.SCALP_V2_WORKER_STAGE_A_MIN_TRADES, 8),
          ),
        ),
      ),
      minNetR: toFinite(process.env.SCALP_V2_WORKER_STAGE_A_MIN_NET_R, 0.5),
      minConsecutiveWinningWeeks: Math.max(
        0,
        Math.min(
          4,
          Math.floor(
            toFinite(
              process.env.SCALP_V2_WORKER_STAGE_A_MIN_CONSEC_WIN_WEEKS,
              2,
            ),
          ),
        ),
      ),
      minProfitFactor: Math.max(
        0,
        toFinite(process.env.SCALP_V2_WORKER_STAGE_A_MIN_PROFIT_FACTOR, 1.01),
      ),
      maxDrawdownR: Math.max(
        0.1,
        toFinite(process.env.SCALP_V2_WORKER_STAGE_A_MAX_DD_R, 8),
      ),
    },
    stageB: {
      id: "b",
      weeks: 6,
      minTrades: Math.max(
        0,
        Math.min(
          10_000,
          Math.floor(
            toFinite(process.env.SCALP_V2_WORKER_STAGE_B_MIN_TRADES, 14),
          ),
        ),
      ),
      minNetR: toFinite(process.env.SCALP_V2_WORKER_STAGE_B_MIN_NET_R, 1),
      minConsecutiveWinningWeeks: Math.max(
        0,
        Math.min(
          6,
          Math.floor(
            toFinite(
              process.env.SCALP_V2_WORKER_STAGE_B_MIN_CONSEC_WIN_WEEKS,
              3,
            ),
          ),
        ),
      ),
      minProfitFactor: Math.max(
        0,
        toFinite(process.env.SCALP_V2_WORKER_STAGE_B_MIN_PROFIT_FACTOR, 1.03),
      ),
      maxDrawdownR: Math.max(
        0.1,
        toFinite(process.env.SCALP_V2_WORKER_STAGE_B_MAX_DD_R, 10),
      ),
    },
    stageC: {
      id: "c",
      weeks: 12,
      minTrades: Math.max(
        0,
        Math.min(
          20_000,
          Math.floor(
            toFinite(process.env.SCALP_V2_WORKER_STAGE_C_MIN_TRADES, 24),
          ),
        ),
      ),
      minNetR: toFinite(process.env.SCALP_V2_WORKER_STAGE_C_MIN_NET_R, 2),
      minConsecutiveWinningWeeks: Math.max(
        0,
        Math.min(
          12,
          Math.floor(
            toFinite(
              process.env.SCALP_V2_WORKER_STAGE_C_MIN_CONSEC_WIN_WEEKS,
              4,
            ),
          ),
        ),
      ),
      minProfitFactor: Math.max(
        0,
        toFinite(process.env.SCALP_V2_WORKER_STAGE_C_MIN_PROFIT_FACTOR, 1.05),
      ),
      maxDrawdownR: Math.max(
        0.1,
        toFinite(process.env.SCALP_V2_WORKER_STAGE_C_MAX_DD_R, 12),
      ),
    },
    minCandles: Math.max(
      120,
      Math.min(
        2_000_000,
        toPositiveInt(process.env.SCALP_V2_WORKER_MIN_CANDLES, 8_000, 2_000_000),
      ),
    ),
    maxHighlightsPerRun: Math.max(
      1,
      Math.min(
        500,
        toPositiveInt(process.env.SCALP_V2_WORKER_MAX_HIGHLIGHTS_PER_RUN, 120, 500),
      ),
    ),
  };
}

function isFiniteNumber(value: unknown): boolean {
  const n = Number(value);
  return Number.isFinite(n);
}

function toReplayCandlesFromHistory(
  candles: Array<[number, number, number, number, number, number]>,
  spreadPips: number,
): ScalpReplayCandle[] {
  const out: ScalpReplayCandle[] = [];
  for (const row of candles || []) {
    const ts = Math.floor(Number(row?.[0] || 0));
    const open = Number(row?.[1] || 0);
    const high = Number(row?.[2] || 0);
    const low = Number(row?.[3] || 0);
    const close = Number(row?.[4] || 0);
    const volume = Number(row?.[5] || 0);
    if (
      !Number.isFinite(ts) ||
      ![open, high, low, close].every((value) => Number.isFinite(value) && value > 0)
    ) {
      continue;
    }
    out.push({
      ts,
      open,
      high,
      low,
      close,
      volume: Number.isFinite(volume) ? volume : 0,
      spreadPips,
    });
  }
  return out.sort((a, b) => a.ts - b.ts);
}

function filterSundayReplayCandles(candles: ScalpReplayCandle[]): ScalpReplayCandle[] {
  return (candles || []).filter(
    (row) => new Date(row.ts).getUTCDay() !== 0,
  );
}

function countWinningWeekStreak(params: {
  trades: ScalpReplayTrade[];
  fromTs: number;
  toTs: number;
}): { winningWeeks: number; consecutiveWinningWeeks: number } {
  const fromTs = Math.floor(Number(params.fromTs) || 0);
  const toTs = Math.floor(Number(params.toTs) || 0);
  if (!Number.isFinite(fromTs) || !Number.isFinite(toTs) || toTs <= fromTs) {
    return { winningWeeks: 0, consecutiveWinningWeeks: 0 };
  }

  const weekNetByStart = new Map<number, number>();
  for (const trade of params.trades || []) {
    const ts = Math.floor(Number(trade.exitTs || 0));
    if (!Number.isFinite(ts) || ts < fromTs || ts >= toTs) continue;
    const weekStart = startOfScalpV2WeekMondayUtc(ts);
    weekNetByStart.set(
      weekStart,
      (weekNetByStart.get(weekStart) || 0) + Number(trade.rMultiple || 0),
    );
  }

  let winningWeeks = 0;
  let consecutiveWinningWeeks = 0;
  let currentStreak = 0;
  for (let weekStart = fromTs; weekStart < toTs; weekStart += ONE_WEEK_MS) {
    const netR = weekNetByStart.get(weekStart) || 0;
    if (netR > 0) {
      winningWeeks += 1;
      currentStreak += 1;
      consecutiveWinningWeeks = Math.max(consecutiveWinningWeeks, currentStreak);
    } else {
      currentStreak = 0;
    }
  }
  return { winningWeeks, consecutiveWinningWeeks };
}

function normalizeProfitFactorForGate(params: {
  profitFactor: number | null;
  grossProfitR: number;
  grossLossR: number;
}): number {
  const profitFactor = Number(params.profitFactor);
  if (Number.isFinite(profitFactor)) return profitFactor;
  const grossProfitR = Number(params.grossProfitR || 0);
  const grossLossR = Number(params.grossLossR || 0);
  if (grossProfitR > 0 && Math.abs(grossLossR) <= 1e-9) {
    return Number.POSITIVE_INFINITY;
  }
  return 0;
}

function evaluateWorkerStageGate(params: {
  stage: ScalpV2WorkerStagePolicy;
  stageResult: ScalpV2WorkerStageResult;
}): { passed: boolean; reason: string | null } {
  const { stageResult, stage } = params;
  if (!stageResult.executed) {
    return { passed: false, reason: stageResult.reason || "stage_not_executed" };
  }
  if (stageResult.candles <= 0) {
    return { passed: false, reason: "stage_no_candles" };
  }
  if (stageResult.trades < stage.minTrades) {
    return { passed: false, reason: "stage_min_trades_not_met" };
  }
  if (stageResult.netR < stage.minNetR) {
    return { passed: false, reason: "stage_min_net_r_not_met" };
  }
  if (stageResult.consecutiveWinningWeeks < stage.minConsecutiveWinningWeeks) {
    return { passed: false, reason: "stage_consecutive_winning_weeks_not_met" };
  }
  if (stageResult.maxDrawdownR > stage.maxDrawdownR) {
    return { passed: false, reason: "stage_max_drawdown_exceeded" };
  }
  if ((stageResult.profitFactor || 0) < stage.minProfitFactor) {
    return { passed: false, reason: "stage_min_profit_factor_not_met" };
  }
  return { passed: true, reason: null };
}

function buildWorkerStageSkeleton(params: {
  stage: ScalpV2WorkerStagePolicy;
  fromTs: number;
  toTs: number;
  reason: string;
}): ScalpV2WorkerStageResult {
  return {
    id: params.stage.id,
    weeks: params.stage.weeks,
    fromTs: params.fromTs,
    toTs: params.toTs,
    executed: false,
    passed: false,
    reason: params.reason,
    candles: 0,
    trades: 0,
    netR: 0,
    expectancyR: 0,
    winRatePct: 0,
    maxDrawdownR: 0,
    profitFactor: null,
    winningWeeks: 0,
    consecutiveWinningWeeks: 0,
    durationMs: 0,
  };
}

function resolveWorkerStageCPass(metadata: unknown): {
  stageCPass: boolean;
  hasWorkerState: boolean;
  reason: "worker_stage_c_missing" | "worker_stage_c_failed" | null;
} {
  const meta = asRecord(metadata);
  const worker = asRecord(meta.worker);
  const stageC = asRecord(worker.stageC);
  const finalPassRaw = worker.finalPass;
  const stageCPassRaw = stageC.passed;
  const hasWorkerState =
    Object.keys(worker).length > 0 ||
    Object.keys(stageC).length > 0 ||
    isFiniteNumber(worker.evaluatedAtMs);
  const stageCPass =
    finalPassRaw === true || stageCPassRaw === true;
  if (stageCPass) {
    return {
      stageCPass: true,
      hasWorkerState,
      reason: null,
    };
  }
  return {
    stageCPass: false,
    hasWorkerState,
    reason: hasWorkerState ? "worker_stage_c_failed" : "worker_stage_c_missing",
  };
}

function normalizeTuneFamily(tuneIdRaw: unknown): string {
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
  fallback: V2LifecycleState,
): V2LifecycleState {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "candidate") return "candidate";
  if (normalized === "graduated") return "graduated";
  if (normalized === "suspended") return "suspended";
  if (normalized === "retired") return "retired";
  return fallback;
}

function asTsMs(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function normalizeLifecycle(params: {
  promotionGate: Record<string, unknown> | null;
  tuneId: string;
  enabled: boolean;
  nowTs: number;
}): V2PromotionLifecycle {
  const fallbackState: V2LifecycleState = params.enabled
    ? "graduated"
    : "candidate";
  const lifecycleRaw = asRecord(asRecord(params.promotionGate).lifecycle);
  const state = normalizeLifecycleState(lifecycleRaw.state, fallbackState);
  const suspendedUntilMs = asTsMs(lifecycleRaw.suspendedUntilMs);
  const retiredUntilMs = asTsMs(lifecycleRaw.retiredUntilMs);
  const eventsRaw = Array.isArray(lifecycleRaw.suspensionEventsMs)
    ? lifecycleRaw.suspensionEventsMs
    : [];
  const events = eventsRaw
    .map((row) => asTsMs(row))
    .filter((row): row is number => row !== null)
    .sort((a, b) => a - b)
    .filter((row) => row >= params.nowTs - LIFECYCLE_SUSPEND_WINDOW_MS);
  const suspensionCount180d = Math.max(
    events.length,
    Math.max(0, Math.floor(Number(lifecycleRaw.suspensionCount180d) || 0)),
  );
  const lifecycle: V2PromotionLifecycle = {
    state,
    tuneFamily:
      String(lifecycleRaw.tuneFamily || "").trim().toLowerCase() ||
      normalizeTuneFamily(params.tuneId),
    suspendedUntilMs,
    retiredUntilMs,
    suspensionEventsMs: events,
    suspensionCount180d,
    lastSeatReleaseAtMs: asTsMs(lifecycleRaw.lastSeatReleaseAtMs),
  };
  if (
    lifecycle.state === "suspended" &&
    lifecycle.suspendedUntilMs !== null &&
    lifecycle.suspendedUntilMs <= params.nowTs
  ) {
    lifecycle.state = params.enabled ? "graduated" : "candidate";
    lifecycle.suspendedUntilMs = null;
  }
  if (
    lifecycle.state === "retired" &&
    lifecycle.retiredUntilMs !== null &&
    lifecycle.retiredUntilMs <= params.nowTs
  ) {
    lifecycle.state = params.enabled ? "graduated" : "candidate";
    lifecycle.retiredUntilMs = null;
  }
  return lifecycle;
}

function lifecycleIsSuppressed(
  lifecycle: V2PromotionLifecycle,
  nowTs: number,
): boolean {
  if (lifecycle.state === "retired") {
    return lifecycle.retiredUntilMs === null || lifecycle.retiredUntilMs > nowTs;
  }
  if (lifecycle.state === "suspended") {
    return (
      lifecycle.suspendedUntilMs === null || lifecycle.suspendedUntilMs > nowTs
    );
  }
  return false;
}

function applyLifecycleSuspension(params: {
  lifecycle: V2PromotionLifecycle;
  nowTs: number;
  durationMs: number;
  retireMs: number;
  retireOnSuspensionCount: number;
}): V2PromotionLifecycle {
  const events = params.lifecycle.suspensionEventsMs
    .filter((row) => row >= params.nowTs - LIFECYCLE_SUSPEND_WINDOW_MS)
    .concat(params.nowTs)
    .sort((a, b) => a - b);
  const suspensionCount180d = events.length;
  if (suspensionCount180d >= params.retireOnSuspensionCount) {
    return {
      ...params.lifecycle,
      state: "retired",
      suspendedUntilMs: null,
      retiredUntilMs: params.nowTs + params.retireMs,
      suspensionEventsMs: events,
      suspensionCount180d,
    };
  }
  return {
    ...params.lifecycle,
    state: "suspended",
    suspendedUntilMs: params.nowTs + Math.max(ONE_DAY_MS, params.durationMs),
    retiredUntilMs: null,
    suspensionEventsMs: events,
    suspensionCount180d,
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

function mean(values: number[]): number {
  if (!values.length) return 0;
  return values.reduce((acc, row) => acc + row, 0) / values.length;
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

function topPositiveNetConcentrationPct(values: number[]): number {
  const positiveNet = values.map((value) => Math.max(0, value));
  const totalPositive = positiveNet.reduce((acc, value) => acc + value, 0);
  if (totalPositive <= 0) return 100;
  const topPositive = positiveNet.length ? Math.max(...positiveNet) : 0;
  return (topPositive / totalPositive) * 100;
}

function buildFreshness(params: {
  weeklyByWeekStart: Map<number, { trades: number; netR: number }>;
  requiredWeeks: number;
  nowTs: number;
}): PromotionFreshness {
  const requiredWeeks = Math.max(1, Math.min(52, params.requiredWeeks));
  const windowToTs = resolveScalpV2CompletedWeekWindowToUtc(params.nowTs);
  const windowFromTs = windowToTs - requiredWeeks * ONE_WEEK_MS;
  let completedWeeks = 0;
  const missingWeekStarts: number[] = [];

  for (let i = 0; i < requiredWeeks; i += 1) {
    const weekStart = windowFromTs + i * ONE_WEEK_MS;
    if (params.weeklyByWeekStart.has(weekStart)) completedWeeks += 1;
    else missingWeekStarts.push(weekStart);
  }

  const missingWeeks = missingWeekStarts.length;
  return {
    ready: missingWeeks === 0 && completedWeeks === requiredWeeks,
    requiredWeeks,
    completedWeeks,
    missingWeeks,
    windowFromTs,
    windowToTs,
    missingWeekStarts: missingWeeks > 0 ? missingWeekStarts : [],
  };
}

function computeWeeklyMetrics(params: {
  weeklyRows: WeeklyAggregationRow[];
  nowTs: number;
  fourWeekGroupSize: number;
  fourWeekGroupCount: number;
}): WeeklyRobustnessMetrics | null {
  if (!params.weeklyRows.length) return null;
  const orderedRows = params.weeklyRows
    .slice()
    .sort((a, b) => a.weekStartTs - b.weekStartTs);
  const profitableSlices = orderedRows.filter((row) => row.netR > 0).length;
  const expectancyRows = orderedRows.map((row) => row.expectancyR);
  const netRows = orderedRows.map((row) => row.netR);
  const tradesRows = orderedRows.map((row) => row.trades);
  const slices = orderedRows.length;
  const recentNetRows = netRows.slice(
    -params.fourWeekGroupSize * params.fourWeekGroupCount,
  );
  const fourWeekGroupNetR: number[] = [];
  for (let idx = 0; idx < params.fourWeekGroupCount; idx += 1) {
    const start = idx * params.fourWeekGroupSize;
    const end = start + params.fourWeekGroupSize;
    if (end > recentNetRows.length) break;
    fourWeekGroupNetR.push(
      recentNetRows.slice(start, end).reduce((acc, row) => acc + row, 0),
    );
  }
  const totalTrades = tradesRows.reduce((acc, row) => acc + row, 0);
  return {
    slices,
    profitableSlices,
    profitablePct: (profitableSlices / slices) * 100,
    meanExpectancyR: mean(expectancyRows),
    trimmedMeanExpectancyR: trimmedMean(expectancyRows, 0.15),
    p25ExpectancyR: quantile(expectancyRows, 0.25),
    medianExpectancyR: median(expectancyRows),
    worstNetR: netRows.reduce(
      (acc, row) => Math.min(acc, row),
      Number.POSITIVE_INFINITY,
    ),
    topWeekPnlConcentrationPct: topPositiveNetConcentrationPct(netRows),
    totalNetR: netRows.reduce((acc, row) => acc + row, 0),
    fourWeekGroupNetR,
    fourWeekGroupsEvaluated: fourWeekGroupNetR.length,
    fourWeekMinNetR: fourWeekGroupNetR.length ? Math.min(...fourWeekGroupNetR) : null,
    minTradesPerWeekObserved: tradesRows.length ? Math.min(...tradesRows) : 0,
    totalTrades,
    evaluatedAtMs: params.nowTs,
  };
}

function longestConsecutiveWinningWeeks(rows: WeeklyAggregationRow[]): number {
  if (!rows.length) return 0;
  let current = 0;
  let longest = 0;
  for (const row of rows) {
    if (row.netR > 0) {
      current += 1;
      if (current > longest) longest = current;
    } else {
      current = 0;
    }
  }
  return longest;
}

function evaluateWeeklyGate(params: {
  metrics: WeeklyRobustnessMetrics | null;
  policy: ScalpV2PromotionPolicy;
}): { passed: boolean; reason: string | null } {
  const metrics = params.metrics;
  const policy = params.policy;
  if (!metrics) return { passed: false, reason: "weekly_robustness_missing" };
  if (metrics.slices < policy.minSlices) {
    return { passed: false, reason: "weekly_slice_count_below_threshold" };
  }
  if (metrics.fourWeekGroupsEvaluated < policy.fourWeekGroupCount) {
    return { passed: false, reason: "weekly_four_week_groups_missing" };
  }
  if (
    (metrics.fourWeekMinNetR ?? Number.NEGATIVE_INFINITY) < policy.minFourWeekNetR
  ) {
    return { passed: false, reason: "weekly_four_week_net_r_below_threshold" };
  }
  if (metrics.profitablePct < policy.minProfitablePct) {
    return { passed: false, reason: "weekly_profitable_pct_below_threshold" };
  }
  if (metrics.medianExpectancyR < policy.minMedianExpectancyR) {
    return { passed: false, reason: "weekly_median_expectancy_below_threshold" };
  }
  if (metrics.p25ExpectancyR < policy.minP25ExpectancyR) {
    return { passed: false, reason: "weekly_p25_expectancy_below_threshold" };
  }
  if (metrics.worstNetR < policy.minWorstNetR) {
    return { passed: false, reason: "weekly_worst_net_r_below_threshold" };
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

function buildEvent(params: {
  deploymentId: string;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpV2Session;
  eventType: ScalpV2ExecutionEvent["eventType"];
  reasonCodes?: string[];
  brokerRef?: string | null;
  rawPayload?: Record<string, unknown>;
  sourceOfTruth?: ScalpV2ExecutionEvent["sourceOfTruth"];
}): ScalpV2ExecutionEvent {
  return {
    id: crypto.randomUUID(),
    tsMs: nowMs(),
    deploymentId: params.deploymentId,
    venue: params.venue,
    symbol: params.symbol,
    strategyId: params.strategyId,
    tuneId: params.tuneId,
    entrySessionProfile: params.entrySessionProfile,
    eventType: params.eventType,
    brokerRef: params.brokerRef || null,
    reasonCodes: params.reasonCodes || [],
    sourceOfTruth: params.sourceOfTruth || "system",
    rawPayload: params.rawPayload || {},
  };
}

function lockOwner(jobKind: string): string {
  return `scalp_v2_${jobKind}_${nowMs()}_${Math.floor(Math.random() * 1_000_000)}`;
}

function toDeploymentSessionKey(
  deploymentId: string,
  entrySessionProfile: ScalpV2Session,
): string {
  return `${deploymentId}::${entrySessionProfile}`;
}

async function updateResearchCursorSafe(params: {
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
  phase: "scan" | "score" | "validate" | "promote";
  lastCandidateOffset?: number;
  lastWeekStartMs?: number | null;
  progress?: Record<string, unknown>;
}): Promise<void> {
  const cursorKey = toScalpV2ResearchCursorKey({
    venue: params.venue,
    symbol: params.symbol,
    entrySessionProfile: params.entrySessionProfile,
  });
  const existingCursor =
    params.lastCandidateOffset === undefined || params.lastWeekStartMs === undefined
      ? await loadScalpV2ResearchCursor({ cursorKey }).catch(() => null)
      : null;

  await upsertScalpV2ResearchCursor({
    cursorKey,
    venue: params.venue,
    symbol: params.symbol,
    entrySessionProfile: params.entrySessionProfile,
    phase: params.phase,
    lastCandidateOffset:
      params.lastCandidateOffset === undefined
        ? existingCursor?.lastCandidateOffset || 0
        : params.lastCandidateOffset,
    lastWeekStartMs:
      params.lastWeekStartMs === undefined
        ? existingCursor?.lastWeekStartMs ?? null
        : params.lastWeekStartMs,
    progress: params.progress || {},
  }).catch(() => undefined);
}

export async function runScalpV2DiscoverJob(): Promise<ScalpV2JobResult> {
  const owner = lockOwner("discover");
  const claimed = await claimScalpV2Job({ jobKind: "discover", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "discover",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const runtime = await loadScalpV2RuntimeConfig();
    if (!runtime.enabled) {
      details = { skipped: true, reason: "SCALP_V2_DISABLED" };
      return buildScalpV2JobResult({
        jobKind: "discover",
        processed,
        succeeded,
        failed,
        pendingAfter: 0,
        details,
      });
    }

    const persistCandidates = envBool(
      "SCALP_V2_DISCOVER_PERSIST_CANDIDATES",
      false,
    );
    const rows: Parameters<typeof upsertScalpV2Candidates>[0]["rows"] = [];
    const maxCandidatesPerSession = Math.max(
      1,
      Math.min(
        96,
        toPositiveInt(
          process.env.SCALP_V2_COMPOSER_MAX_CANDIDATES_PER_SESSION,
          24,
          96,
        ),
      ),
    );
    let droppedByVenuePolicy = 0;
    let discoveredByComposer = 0;
    let scopesInspected = 0;
    const countsByCursor = new Map<
      string,
      {
        venue: ScalpV2Venue;
        symbol: string;
        session: ScalpV2Session;
        count: number;
      }
    >();
    for (const venue of runtime.supportedVenues) {
      const symbols = runtime.seedSymbolsByVenue[venue] || [];
      for (const symbol of symbols) {
        if (!isScalpV2DiscoverSymbolAllowed(venue, symbol)) {
          droppedByVenuePolicy += 1;
          continue;
        }
        for (const session of runtime.supportedSessions) {
          scopesInspected += 1;
          const composerCandidates = buildScalpV2ModelGuidedComposerGrid({
            venue,
            symbol,
            entrySessionProfile: session,
            maxCandidates: maxCandidatesPerSession,
          });
          const normalizedSymbol = String(symbol || "").trim().toUpperCase();
          const cursorKey = `${venue}:${normalizedSymbol}:${session}`;
          const current = countsByCursor.get(cursorKey) || {
            venue,
            symbol: normalizedSymbol,
            session,
            count: 0,
          };
          current.count += composerCandidates.length;
          countsByCursor.set(cursorKey, current);
          discoveredByComposer += composerCandidates.length;

          if (!persistCandidates) continue;
          for (const candidateDsl of composerCandidates) {
            const model = candidateDsl.model;
            const executionPlan =
              resolveModelGuidedComposerExecutionPlanFromBlocks(
                candidateDsl.blocksByFamily,
              );
            const supportScore = Number.isFinite(Number(candidateDsl.supportScore))
              ? Number(candidateDsl.supportScore)
              : 0;
            const score =
              20 +
              model.compositeScore * 65 +
              model.confidence * 12 +
              Math.min(12, supportScore) * 0.9 +
              hashScoreSeed(
                `${venue}:${symbol}:${session}:${candidateDsl.candidateId}`,
              ) /
                1000;
            rows.push({
              venue,
              symbol,
              strategyId: MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
              tuneId: candidateDsl.tuneId,
              entrySessionProfile: session,
              score,
              status: "discovered",
              reasonCodes: ["SCALP_V2_DISCOVERY_MODEL_COMPOSER"],
              metadata: {
                discoveredAtMs: nowMs(),
                source: "model_guided_composer",
                researchCandidateId: candidateDsl.candidateId,
                researchDsl: candidateDsl.blocksByFamily,
                researchReferences: candidateDsl.referenceStrategyIds,
                researchSupportScore: supportScore,
                composerModel: candidateDsl.model,
                composerExecutionPlan: executionPlan,
              },
            });
          }
        }
      }
    }

    const insertedOrUpdated = rows.length;
    let trimmed = 0;
    if (persistCandidates && rows.length > 0) {
      await upsertScalpV2Candidates({ rows });
      const trim = await trimScalpV2CandidatesByBudget({
        maxCandidatesTotal: runtime.budgets.maxCandidatesTotal,
        maxCandidatesPerSymbol: runtime.budgets.maxCandidatesPerSymbol,
      });
      trimmed = trim.deleted;
    }
    processed = persistCandidates ? rows.length : discoveredByComposer;
    succeeded = processed;
    details = {
      insertedOrUpdated,
      trimmed,
      droppedByVenuePolicy,
      discoveredByComposer,
      scannedOnly: !persistCandidates,
      persistedCandidates: persistCandidates,
      scopesInspected,
      maxCandidatesPerSession,
      budgets: runtime.budgets,
    };

    await Promise.all(
      Array.from(countsByCursor.values()).map((row) =>
        updateResearchCursorSafe({
          venue: row.venue,
          symbol: row.symbol,
          entrySessionProfile: row.session,
          phase: "scan",
          progress: {
            discoveredCandidates: row.count,
            droppedByVenuePolicy,
            trimmedByBudget: trimmed,
            persistedCandidates: persistCandidates,
            updatedAtMs: nowMs(),
          },
        }),
      ),
    );

    return buildScalpV2JobResult({
      jobKind: "discover",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "discover",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "discover",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

export async function runScalpV2EvaluateJob(params: {
  batchSize?: number;
} = {}): Promise<ScalpV2JobResult> {
  const owner = lockOwner("evaluate");
  const claimed = await claimScalpV2Job({ jobKind: "evaluate", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "evaluate",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const runtime = await loadScalpV2RuntimeConfig();
    if (!runtime.enabled) {
      details = { skipped: true, reason: "SCALP_V2_DISABLED" };
      return buildScalpV2JobResult({
        jobKind: "evaluate",
        processed,
        succeeded,
        failed,
        pendingAfter: 0,
        details,
      });
    }

    const batchSize = Math.max(
      1,
      Math.min(2_000, Math.floor(params.batchSize || 200)),
    );
    const maxCandidatesPerSession = Math.max(
      1,
      Math.min(
        96,
        toPositiveInt(
          process.env.SCALP_V2_COMPOSER_MAX_CANDIDATES_PER_SESSION,
          24,
          96,
        ),
      ),
    );

    const scopes: Array<{
      venue: ScalpV2Venue;
      symbol: string;
      session: ScalpV2Session;
    }> = [];
    let droppedByVenuePolicy = 0;
    for (const venue of runtime.supportedVenues) {
      const symbols = runtime.seedSymbolsByVenue[venue] || [];
      for (const symbolRaw of symbols) {
        const symbol = String(symbolRaw || "").trim().toUpperCase();
        if (!symbol) continue;
        if (!isScalpV2DiscoverSymbolAllowed(venue, symbol)) {
          droppedByVenuePolicy += 1;
          continue;
        }
        if (
          !isScalpV2RuntimeSymbolInScope({
            runtime,
            venue,
            symbol,
          })
        ) {
          continue;
        }
        for (const session of runtime.supportedSessions) {
          scopes.push({ venue, symbol, session });
        }
      }
    }

    if (!scopes.length) {
      details = {
        evaluated: 0,
        reason: "no_runtime_seed_scopes",
        droppedByVenuePolicy,
      };
      return buildScalpV2JobResult({
        jobKind: "evaluate",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const evaluatedRows: Parameters<typeof upsertScalpV2Candidates>[0]["rows"] = [];
    const cursorUpdates: Array<{
      venue: ScalpV2Venue;
      symbol: string;
      session: ScalpV2Session;
      poolSize: number;
      selectedCount: number;
      startOffset: number;
      nextOffset: number;
      perScopeCap: number;
    }> = [];
    let selectedTotal = 0;
    let poolSizeTotal = 0;

    for (let idx = 0; idx < scopes.length; idx += 1) {
      if (selectedTotal >= batchSize) break;
      const scope = scopes[idx]!;
      const remainingScopes = scopes.length - idx;
      const remainingBudget = Math.max(1, batchSize - selectedTotal);
      const perScopeCap = Math.max(
        1,
        Math.min(maxCandidatesPerSession, Math.ceil(remainingBudget / remainingScopes)),
      );
      const composerCandidates = buildScalpV2ModelGuidedComposerGrid({
        venue: scope.venue,
        symbol: scope.symbol,
        entrySessionProfile: scope.session,
        maxCandidates: maxCandidatesPerSession,
      });
      poolSizeTotal += composerCandidates.length;

      const cursorKey = toScalpV2ResearchCursorKey({
        venue: scope.venue,
        symbol: scope.symbol,
        entrySessionProfile: scope.session,
      });
      const existingCursor = await loadScalpV2ResearchCursor({
        cursorKey,
      }).catch(() => null);
      const window = resolveScalpV2CandidateEvaluationWindow({
        candidates: composerCandidates,
        maxCandidates: perScopeCap,
        startOffset: existingCursor?.lastCandidateOffset || 0,
      });

      cursorUpdates.push({
        venue: scope.venue,
        symbol: scope.symbol,
        session: scope.session,
        poolSize: window.poolSize,
        selectedCount: window.evaluatedCount,
        startOffset: window.startOffset,
        nextOffset: window.nextOffset,
        perScopeCap,
      });

      if (!window.selectedCandidates.length) continue;
      selectedTotal += window.selectedCandidates.length;
      const evaluatedAtMs = nowMs();
      for (const candidateDsl of window.selectedCandidates) {
        const model = candidateDsl.model;
        const executionPlan = resolveModelGuidedComposerExecutionPlanFromBlocks(
          candidateDsl.blocksByFamily,
        );
        const supportScore = Number.isFinite(Number(candidateDsl.supportScore))
          ? Number(candidateDsl.supportScore)
          : 0;
        const previousScore =
          20 +
          model.compositeScore * 65 +
          model.confidence * 12 +
          Math.min(12, supportScore) * 0.9 +
          hashScoreSeed(
            `${scope.venue}:${scope.symbol}:${scope.session}:${candidateDsl.candidateId}`,
          ) /
            1000;
        const supportNorm = Math.max(0, Math.min(12, supportScore)) / 12;
        const score =
          12 +
          Math.max(0, previousScore) * 0.12 +
          model.compositeScore * 66 +
          model.confidence * 14 +
          supportNorm * 8 +
          hashScoreSeed(
            `${scope.venue}:${scope.symbol}:${scope.session}:${candidateDsl.tuneId}`,
          ) /
            1000;
        evaluatedRows.push({
          venue: scope.venue,
          symbol: scope.symbol,
          strategyId: MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
          tuneId: candidateDsl.tuneId,
          entrySessionProfile: scope.session,
          score,
          status: "evaluated",
          reasonCodes: ["SCALP_V2_MODEL_GUIDED_EVALUATED"],
          metadata: {
            discoveredAtMs: candidateDsl.generatedAtMs,
            source: "model_guided_composer",
            researchCandidateId: candidateDsl.candidateId,
            researchDsl: candidateDsl.blocksByFamily,
            researchReferences: candidateDsl.referenceStrategyIds,
            researchSupportScore: supportScore,
            composerModel: candidateDsl.model,
            composerExecutionPlan: executionPlan,
            evaluatedAtMs,
            evaluator: "v2_model_guided_composer",
            liveEnabled: runtime.liveEnabled,
            scoreBreakdown: {
              previousScore,
              modelComposite: model.compositeScore,
              modelConfidence: model.confidence,
              modelFamily: model.family,
              supportScore,
              startOffset: window.startOffset,
              nextOffset: window.nextOffset,
              perScopeCap,
            },
          },
        });
      }
    }

    await upsertScalpV2Candidates({ rows: evaluatedRows });
    processed = evaluatedRows.length;
    succeeded = evaluatedRows.length;

    details = {
      evaluated: evaluatedRows.length,
      batchSize,
      maxCandidatesPerSession,
      scopeCount: scopes.length,
      droppedByVenuePolicy,
      poolSizeTotal,
      cursorUpdatedScopes: cursorUpdates.length,
      requestedBudgetReached: selectedTotal >= batchSize,
    };

    await Promise.all(
      cursorUpdates.map((row) =>
        updateResearchCursorSafe({
          venue: row.venue,
          symbol: row.symbol,
          entrySessionProfile: row.session,
          phase: "score",
          lastCandidateOffset: row.nextOffset,
          progress: {
            evaluatedCandidates: row.selectedCount,
            poolSize: row.poolSize,
            startOffset: row.startOffset,
            nextOffset: row.nextOffset,
            perScopeCap: row.perScopeCap,
            batchSize,
            updatedAtMs: nowMs(),
          },
        }),
      ),
    );

    return buildScalpV2JobResult({
      jobKind: "evaluate",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "evaluate",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "evaluate",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

export async function runScalpV2WorkerJob(params: {
  batchSize?: number;
} = {}): Promise<ScalpV2JobResult> {
  const owner = lockOwner("worker");
  const claimed = await claimScalpV2Job({ jobKind: "worker", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "worker",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const runtime = await loadScalpV2RuntimeConfig();
    if (!runtime.enabled) {
      details = { skipped: true, reason: "SCALP_V2_DISABLED" };
      return buildScalpV2JobResult({
        jobKind: "worker",
        processed,
        succeeded,
        failed,
        pendingAfter: 0,
        details,
      });
    }

    const workerPolicy = resolveWorkerPolicy();
    const nowTs = nowMs();
    if (!workerPolicy.allowSunday && isScalpV2SundayUtc(nowTs)) {
      details = {
        skipped: true,
        reason: "sunday_utc_worker_blocked",
        sundayUtc: true,
        allowSundayWorker: workerPolicy.allowSunday,
      };
      return buildScalpV2JobResult({
        jobKind: "worker",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const batchSize = Math.max(
      1,
      Math.min(
        workerPolicy.maxBatchSize,
        Math.floor(params.batchSize || workerPolicy.maxBatchSize),
      ),
    );
    const windowToTs = resolveScalpV2CompletedWeekWindowToUtc(nowTs);
    const stagePolicies = [
      workerPolicy.stageA,
      workerPolicy.stageB,
      workerPolicy.stageC,
    ] as const;
    const minWindowFromTs = windowToTs - workerPolicy.stageC.weeks * ONE_WEEK_MS;

    const allCandidatesRaw = await listScalpV2Candidates({ limit: 10_000 });
    const allCandidates = allCandidatesRaw.filter((row) => {
      if (
        row.status !== "evaluated" &&
        row.status !== "promoted" &&
        row.status !== "shadow"
      ) {
        return false;
      }
      return isScalpV2RuntimeSymbolInScope({
        runtime,
        venue: row.venue,
        symbol: row.symbol,
      });
    });
    const filteredCandidatesOutOfScope = Math.max(
      0,
      allCandidatesRaw.length - allCandidates.length,
    );

    if (!allCandidates.length) {
      details = {
        reason: "no_evaluated_candidates",
        workerBatchSize: batchSize,
        filteredCandidatesOutOfScope,
      };
      return buildScalpV2JobResult({
        jobKind: "worker",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const scopes: Array<{
      venue: ScalpV2Venue;
      symbol: string;
      session: ScalpV2Session;
    }> = [];
    for (const venue of runtime.supportedVenues) {
      const symbols = runtime.seedSymbolsByVenue[venue] || [];
      for (const symbolRaw of symbols) {
        const symbol = String(symbolRaw || "").trim().toUpperCase();
        if (!symbol) continue;
        if (!isScalpV2DiscoverSymbolAllowed(venue, symbol)) continue;
        if (
          !isScalpV2RuntimeSymbolInScope({
            runtime,
            venue,
            symbol,
          })
        ) {
          continue;
        }
        for (const session of runtime.supportedSessions) {
          scopes.push({ venue, symbol, session });
        }
      }
    }

    if (!scopes.length) {
      details = {
        reason: "no_runtime_seed_scopes",
        workerBatchSize: batchSize,
      };
      return buildScalpV2JobResult({
        jobKind: "worker",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const selectedById = new Map<number, (typeof allCandidates)[number]>();
    const scopeCursorUpdates: Array<{
      venue: ScalpV2Venue;
      symbol: string;
      session: ScalpV2Session;
      poolSize: number;
      selectedCount: number;
      startOffset: number;
      nextOffset: number;
      perScopeCap: number;
    }> = [];
    let selectedTotal = 0;

    for (let idx = 0; idx < scopes.length; idx += 1) {
      if (selectedTotal >= batchSize) break;
      const scope = scopes[idx]!;
      const scopeCandidates = allCandidates
        .filter(
          (row) =>
            row.venue === scope.venue &&
            row.symbol === scope.symbol &&
            row.entrySessionProfile === scope.session,
        )
        .sort((a, b) => b.score - a.score || a.id - b.id);
      if (!scopeCandidates.length) continue;

      const remainingScopes = scopes.length - idx;
      const remainingBudget = Math.max(1, batchSize - selectedTotal);
      const perScopeCap = Math.max(
        1,
        Math.min(
          workerPolicy.maxCandidatesPerSession,
          Math.ceil(remainingBudget / remainingScopes),
        ),
      );
      const cursorKey = toScalpV2ResearchCursorKey({
        venue: scope.venue,
        symbol: scope.symbol,
        entrySessionProfile: scope.session,
      });
      const existingCursor = await loadScalpV2ResearchCursor({
        cursorKey,
      }).catch(() => null);
      const window = resolveScalpV2CandidateEvaluationWindow({
        candidates: scopeCandidates,
        maxCandidates: perScopeCap,
        startOffset: existingCursor?.lastCandidateOffset || 0,
      });
      scopeCursorUpdates.push({
        venue: scope.venue,
        symbol: scope.symbol,
        session: scope.session,
        poolSize: window.poolSize,
        selectedCount: window.evaluatedCount,
        startOffset: window.startOffset,
        nextOffset: window.nextOffset,
        perScopeCap,
      });
      selectedTotal += window.selectedCandidates.length;
      for (const row of window.selectedCandidates) {
        selectedById.set(row.id, row);
      }
    }

    const selectedCandidates = Array.from(selectedById.values());
    if (!selectedCandidates.length) {
      details = {
        reason: "no_candidates_selected_for_worker",
        workerBatchSize: batchSize,
        scopeCount: scopes.length,
        cursorUpdatedScopes: scopeCursorUpdates.length,
      };
      return buildScalpV2JobResult({
        jobKind: "worker",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const candidateRows: Parameters<typeof upsertScalpV2Candidates>[0]["rows"] = [];
    const highlightRows: Parameters<typeof upsertScalpV2ResearchHighlights>[0]["rows"] = [];
    const candleCache = new Map<string, ScalpReplayCandle[]>();
    const scopeStats = new Map<
      string,
      {
        processed: number;
        stageCPass: number;
        stageCFail: number;
      }
    >();

    let stageAPass = 0;
    let stageAFail = 0;
    let stageBPass = 0;
    let stageBFail = 0;
    let stageCPass = 0;
    let stageCFail = 0;
    let replayErrors = 0;

    for (const candidate of selectedCandidates) {
      const scopeKey = `${candidate.venue}:${candidate.symbol}:${candidate.entrySessionProfile}`;
      const existingScopeStats = scopeStats.get(scopeKey) || {
        processed: 0,
        stageCPass: 0,
        stageCFail: 0,
      };
      existingScopeStats.processed += 1;

      const stageAFromTs = windowToTs - workerPolicy.stageA.weeks * ONE_WEEK_MS;
      const stageBFromTs = windowToTs - workerPolicy.stageB.weeks * ONE_WEEK_MS;
      const stageCFromTs = windowToTs - workerPolicy.stageC.weeks * ONE_WEEK_MS;

      try {
        let symbolCandles = candleCache.get(candidate.symbol) || null;
        if (!symbolCandles) {
          const history = await loadScalpCandleHistoryInRange(
            candidate.symbol,
            "1m",
            minWindowFromTs,
            windowToTs,
          );
          const baseReplayConfig = defaultScalpReplayConfig(candidate.symbol);
          const replayCandles = filterSundayReplayCandles(
            toReplayCandlesFromHistory(
              (history.record?.candles || []) as Array<
                [number, number, number, number, number, number]
              >,
              baseReplayConfig.defaultSpreadPips,
            ),
          );
          symbolCandles = replayCandles;
          candleCache.set(candidate.symbol, replayCandles);
        }

        let blockedStages = false;
        const stageResults: Record<ScalpV2WorkerStageId, ScalpV2WorkerStageResult> = {
          a: buildWorkerStageSkeleton({
            stage: workerPolicy.stageA,
            fromTs: stageAFromTs,
            toTs: windowToTs,
            reason: "stage_not_started",
          }),
          b: buildWorkerStageSkeleton({
            stage: workerPolicy.stageB,
            fromTs: stageBFromTs,
            toTs: windowToTs,
            reason: "stage_not_started",
          }),
          c: buildWorkerStageSkeleton({
            stage: workerPolicy.stageC,
            fromTs: stageCFromTs,
            toTs: windowToTs,
            reason: "stage_not_started",
          }),
        };

        for (const stage of stagePolicies) {
          const fromTs = windowToTs - stage.weeks * ONE_WEEK_MS;
          if (blockedStages) {
            stageResults[stage.id] = buildWorkerStageSkeleton({
              stage,
              fromTs,
              toTs: windowToTs,
              reason: "blocked_prior_stage_failed",
            });
            continue;
          }

          const stageCandles = symbolCandles.filter(
            (row) => row.ts >= fromTs && row.ts < windowToTs,
          );
          if (stageCandles.length < workerPolicy.minCandles) {
            const failedResult = buildWorkerStageSkeleton({
              stage,
              fromTs,
              toTs: windowToTs,
              reason: "insufficient_candles",
            });
            stageResults[stage.id] = failedResult;
            blockedStages = true;
            continue;
          }

          const stageStartedAtMs = nowMs();
          const replayBaseConfig = defaultScalpReplayConfig(candidate.symbol);
          const replayConfig = {
            ...replayBaseConfig,
            symbol: candidate.symbol,
            strategyId: candidate.strategyId,
            tuneId: candidate.tuneId,
            deploymentId: toDeploymentId({
              venue: candidate.venue,
              symbol: candidate.symbol,
              strategyId: candidate.strategyId,
              tuneId: candidate.tuneId,
              session: candidate.entrySessionProfile,
            }),
            tuneLabel: candidate.tuneId,
            strategy: {
              ...replayBaseConfig.strategy,
              entrySessionProfile: candidate.entrySessionProfile,
            },
          };
          const replay = await runScalpReplay({
            candles: stageCandles,
            pipSize: pipSizeForScalpSymbol(candidate.symbol),
            config: replayConfig,
            captureTimeline: false,
          });
          const weeklyStats = countWinningWeekStreak({
            trades: replay.trades,
            fromTs,
            toTs: windowToTs,
          });
          const profitFactorForGate = normalizeProfitFactorForGate({
            profitFactor: replay.summary.profitFactor,
            grossProfitR: replay.summary.grossProfitR,
            grossLossR: replay.summary.grossLossR,
          });
          const stageResult: ScalpV2WorkerStageResult = {
            id: stage.id,
            weeks: stage.weeks,
            fromTs,
            toTs: windowToTs,
            executed: true,
            passed: false,
            reason: "stage_pending_gate",
            candles: stageCandles.length,
            trades: replay.summary.trades,
            netR: replay.summary.netR,
            expectancyR: replay.summary.expectancyR,
            winRatePct: replay.summary.winRatePct,
            maxDrawdownR: replay.summary.maxDrawdownR,
            profitFactor: Number.isFinite(Number(replay.summary.profitFactor))
              ? Number(replay.summary.profitFactor)
              : Number.isFinite(profitFactorForGate)
                ? Number(profitFactorForGate)
                : null,
            winningWeeks: weeklyStats.winningWeeks,
            consecutiveWinningWeeks: weeklyStats.consecutiveWinningWeeks,
            durationMs: Math.max(0, nowMs() - stageStartedAtMs),
          };
          const gate = evaluateWorkerStageGate({
            stage,
            stageResult,
          });
          stageResult.passed = gate.passed;
          stageResult.reason = gate.reason || "stage_passed";
          stageResults[stage.id] = stageResult;
          if (!gate.passed) {
            blockedStages = true;
          }
        }

        const stageAResult = stageResults.a;
        const stageBResult = stageResults.b;
        const stageCResult = stageResults.c;
        const finalPass = stageCResult.passed;

        if (stageAResult.passed) stageAPass += 1;
        else stageAFail += 1;
        if (stageBResult.executed && stageBResult.passed) stageBPass += 1;
        else if (stageBResult.executed) stageBFail += 1;
        if (stageCResult.executed && stageCResult.passed) stageCPass += 1;
        else if (stageCResult.executed) stageCFail += 1;

        const metadata = {
          ...asRecord(candidate.metadata || {}),
          worker: {
            version: "v2_worker_stage_replay_r1",
            evaluatedAtMs: nowMs(),
            policy: {
              stageA: workerPolicy.stageA,
              stageB: workerPolicy.stageB,
              stageC: workerPolicy.stageC,
              minCandles: workerPolicy.minCandles,
            },
            windowToTs,
            stageA: stageAResult,
            stageB: stageBResult,
            stageC: stageCResult,
            finalPass,
          },
        };

        candidateRows.push({
          venue: candidate.venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          entrySessionProfile: candidate.entrySessionProfile,
          score: candidate.score,
          status: candidate.status,
          reasonCodes: [
            ...(candidate.reasonCodes || []),
            finalPass
              ? "SCALP_V2_WORKER_STAGE_C_PASS"
              : "SCALP_V2_WORKER_STAGE_C_FAIL",
          ],
          metadata,
        });

        if (finalPass) {
          existingScopeStats.stageCPass += 1;
          const deploymentId = toDeploymentId({
            venue: candidate.venue,
            symbol: candidate.symbol,
            strategyId: candidate.strategyId,
            tuneId: candidate.tuneId,
            session: candidate.entrySessionProfile,
          });
          const candidateMeta = asRecord(candidate.metadata || {});
          highlightRows.push({
            candidateId: deploymentId,
            venue: candidate.venue,
            symbol: candidate.symbol,
            entrySessionProfile: candidate.entrySessionProfile,
            score: candidate.score,
            trades12w: stageCResult.trades,
            winningWeeks12w: stageCResult.winningWeeks,
            consecutiveWinningWeeks: stageCResult.consecutiveWinningWeeks,
            robustness: {
              source: "v2_worker_stage_c",
              stageA: stageAResult,
              stageB: stageBResult,
              stageC: stageCResult,
            },
            dsl: asRecord(candidateMeta.researchDsl || {}),
            notes: "worker_stage_c_pass",
            remarkable: true,
          });
        } else {
          existingScopeStats.stageCFail += 1;
        }
      } catch (err: any) {
        replayErrors += 1;
        existingScopeStats.stageCFail += 1;
        const message = String(err?.message || err || "worker_replay_failed").slice(
          0,
          300,
        );
        const metadata = {
          ...asRecord(candidate.metadata || {}),
          worker: {
            version: "v2_worker_stage_replay_r1",
            evaluatedAtMs: nowMs(),
            policy: {
              stageA: workerPolicy.stageA,
              stageB: workerPolicy.stageB,
              stageC: workerPolicy.stageC,
              minCandles: workerPolicy.minCandles,
            },
            windowToTs,
            stageA: buildWorkerStageSkeleton({
              stage: workerPolicy.stageA,
              fromTs: stageAFromTs,
              toTs: windowToTs,
              reason: "worker_replay_exception",
            }),
            stageB: buildWorkerStageSkeleton({
              stage: workerPolicy.stageB,
              fromTs: stageBFromTs,
              toTs: windowToTs,
              reason: "blocked_prior_stage_failed",
            }),
            stageC: buildWorkerStageSkeleton({
              stage: workerPolicy.stageC,
              fromTs: stageCFromTs,
              toTs: windowToTs,
              reason: "blocked_prior_stage_failed",
            }),
            finalPass: false,
            error: message,
          },
        };
        candidateRows.push({
          venue: candidate.venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          entrySessionProfile: candidate.entrySessionProfile,
          score: candidate.score,
          status: candidate.status,
          reasonCodes: [
            ...(candidate.reasonCodes || []),
            "SCALP_V2_WORKER_STAGE_C_FAIL",
          ],
          metadata,
        });
      }

      scopeStats.set(scopeKey, existingScopeStats);
    }

    if (candidateRows.length > 0) {
      await upsertScalpV2Candidates({ rows: candidateRows });
    }
    const highlightsUpserted = await upsertScalpV2ResearchHighlights({
      rows: highlightRows.slice(0, workerPolicy.maxHighlightsPerRun),
    }).catch(() => 0);

    await Promise.all(
      scopeCursorUpdates.map((row) => {
        const key = `${row.venue}:${row.symbol}:${row.session}`;
        const stats = scopeStats.get(key) || {
          processed: 0,
          stageCPass: 0,
          stageCFail: 0,
        };
        return updateResearchCursorSafe({
          venue: row.venue,
          symbol: row.symbol,
          entrySessionProfile: row.session,
          phase: "validate",
          lastCandidateOffset: row.nextOffset,
          lastWeekStartMs: minWindowFromTs,
          progress: {
            workerProcessed: stats.processed,
            workerStageCPass: stats.stageCPass,
            workerStageCFail: stats.stageCFail,
            poolSize: row.poolSize,
            selectedCount: row.selectedCount,
            startOffset: row.startOffset,
            nextOffset: row.nextOffset,
            workerBatchSize: batchSize,
            updatedAtMs: nowMs(),
          },
        });
      }),
    );

    processed = selectedCandidates.length;
    succeeded = Math.max(0, processed - replayErrors);
    failed = 0;
    details = {
      workerBatchSize: batchSize,
      processedCandidates: selectedCandidates.length,
      replayErrors,
      stageAPass,
      stageAFail,
      stageBPass,
      stageBFail,
      stageCPass,
      stageCFail,
      highlightsUpserted,
      scopeCount: scopes.length,
      cursorUpdatedScopes: scopeCursorUpdates.length,
      filteredCandidatesOutOfScope,
      policy: {
        stageA: workerPolicy.stageA,
        stageB: workerPolicy.stageB,
        stageC: workerPolicy.stageC,
        minCandles: workerPolicy.minCandles,
      },
    };

    return buildScalpV2JobResult({
      jobKind: "worker",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "worker",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "worker",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

export async function runScalpV2PromoteJob(): Promise<ScalpV2JobResult> {
  const owner = lockOwner("promote");
  const claimed = await claimScalpV2Job({ jobKind: "promote", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "promote",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const runtime = await loadScalpV2RuntimeConfig();
    const nowTs = nowMs();
    const policy = resolvePromotionPolicy();
    const requireWinnerShortlist = envBool(
      "SCALP_V2_REQUIRE_WINNER_SHORTLIST",
      true,
    );
    const allCandidatesRaw = await listScalpV2Candidates({ limit: 10_000 });
    const allCandidates = allCandidatesRaw.filter((row) =>
      isScalpV2RuntimeSymbolInScope({
        runtime,
        venue: row.venue,
        symbol: row.symbol,
      }),
    );
    const filteredCandidatesOutOfScope = Math.max(
      0,
      allCandidatesRaw.length - allCandidates.length,
    );
    const promotionPool = allCandidates.filter(
      (row) =>
        row.status === "evaluated" ||
        row.status === "promoted" ||
        row.status === "shadow",
    );
    const existingDeploymentsRaw = await listScalpV2Deployments({ limit: 10_000 });
    const existingDeployments = existingDeploymentsRaw.filter((row) =>
      isScalpV2RuntimeSymbolInScope({
        runtime,
        venue: row.venue,
        symbol: row.symbol,
        includeLiveSeeds: true,
      }),
    );
    const filteredDeploymentsOutOfScope = Math.max(
      0,
      existingDeploymentsRaw.length - existingDeployments.length,
    );
    const offScopeEnabledDeployments = existingDeploymentsRaw.filter(
      (row) =>
        row.enabled &&
        !isScalpV2RuntimeSymbolInScope({
          runtime,
          venue: row.venue,
          symbol: row.symbol,
          includeLiveSeeds: true,
        }),
    );
    if (offScopeEnabledDeployments.length > 0) {
      await upsertScalpV2Deployments({
        rows: offScopeEnabledDeployments.map((row) => ({
          candidateId: row.candidateId,
          venue: row.venue,
          symbol: row.symbol,
          strategyId: row.strategyId,
          tuneId: row.tuneId,
          entrySessionProfile: row.entrySessionProfile,
          enabled: false,
          liveMode: "shadow",
          promotionGate: {
            ...asRecord(row.promotionGate || {}),
            eligible: false,
            reason: "outside_runtime_symbol_scope",
            source: "v2_scope_guard",
            evaluatedAtMs: nowTs,
          },
          riskProfile: row.riskProfile,
        })),
      });
    }
    if (!promotionPool.length && !existingDeployments.length) {
      details = {
        promoted: 0,
        reason: "no_promotable_candidates",
        filteredCandidatesOutOfScope,
        filteredDeploymentsOutOfScope,
        demotedOutOfScopeEnabled: offScopeEnabledDeployments.length,
      };
      return buildScalpV2JobResult({
        jobKind: "promote",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const trimmed = enforceCandidateBudgets({
      candidates: promotionPool,
      budgets: runtime.budgets,
    });

    const candidateByDeploymentId = new Map(trimmed.kept.map((candidate) => [
      toDeploymentId({
        venue: candidate.venue,
        symbol: candidate.symbol,
        strategyId: candidate.strategyId,
        tuneId: candidate.tuneId,
        session: candidate.entrySessionProfile,
      }),
      candidate,
    ]));
    const droppedDeploymentIds = new Set(
      trimmed.dropped.map((candidate) =>
        toDeploymentId({
          venue: candidate.venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          session: candidate.entrySessionProfile,
        }),
      ),
    );
    const existingByDeploymentId = new Map(
      existingDeployments.map((row) => [row.deploymentId, row]),
    );
    const consideredDeploymentIds = Array.from(
      new Set([
        ...Array.from(existingByDeploymentId.keys()),
        ...Array.from(candidateByDeploymentId.keys()),
      ]),
    );
    if (!consideredDeploymentIds.length) {
      details = { promoted: 0, reason: "no_considered_deployments" };
      return buildScalpV2JobResult({
        jobKind: "promote",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const windowToTs = resolveScalpV2CompletedWeekWindowToUtc(nowTs);
    const windowFromTs = windowToTs - policy.minCompletedWeeks * ONE_WEEK_MS;
    const ledgerRows = await listScalpV2LedgerRows({
      deploymentIds: consideredDeploymentIds,
      fromTsMs: windowFromTs,
      toTsMs: windowToTs,
      limit: Math.max(50_000, consideredDeploymentIds.length * 5_000),
    });
    const weeklyByDeploymentSession = new Map<
      string,
      Map<number, { trades: number; netR: number }>
    >();
    const sessionEvidenceByDeployment = new Map<
      string,
      Map<ScalpV2Session, number>
    >();
    for (const row of ledgerRows) {
      const deploymentId = String(row.deploymentId || "").trim();
      if (!deploymentId) continue;
      const tsExitMs = Math.floor(Number(row.tsExitMs) || 0);
      if (!Number.isFinite(tsExitMs) || tsExitMs <= 0) continue;
      const weekStartTs = startOfScalpV2WeekMondayUtc(tsExitMs);
      if (weekStartTs < windowFromTs || weekStartTs >= windowToTs) continue;
      const entrySessionProfile = row.entrySessionProfile;
      const sessionKey = toDeploymentSessionKey(deploymentId, entrySessionProfile);
      const weekly = weeklyByDeploymentSession.get(sessionKey) || new Map();
      const current = weekly.get(weekStartTs) || { trades: 0, netR: 0 };
      current.trades += 1;
      current.netR += Number.isFinite(Number(row.rMultiple))
        ? Number(row.rMultiple)
        : 0;
      weekly.set(weekStartTs, current);
      weeklyByDeploymentSession.set(sessionKey, weekly);

      const bySession = sessionEvidenceByDeployment.get(deploymentId) || new Map();
      bySession.set(
        entrySessionProfile,
        (bySession.get(entrySessionProfile) || 0) + 1,
      );
      sessionEvidenceByDeployment.set(deploymentId, bySession);
    }

    type PromotionDraft = {
      deploymentId: string;
      candidateId: number | null;
      venue: ScalpV2Venue;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: ScalpV2Session;
      score: number;
      currentlyEnabled: boolean;
      droppedByBudget: boolean;
      lifecycle: V2PromotionLifecycle;
      suppressed: boolean;
      strictSessionEvidence: {
        expected: ScalpV2Session;
        matchedTrades: number;
        mismatchedTrades: number;
        mismatchedSessions: ScalpV2Session[];
      };
      freshness: PromotionFreshness;
      weeklyMetrics: WeeklyRobustnessMetrics | null;
      winningWeeks12w: number;
      consecutiveWinningWeeks: number;
      weeklyGateReason: string | null;
      workerStageCPass: boolean;
      workerStageReason: string | null;
      eligible: boolean;
      reason: string;
      enabled: boolean;
      shortlistIncluded: boolean;
      exactLoser: boolean;
      riskProfile: typeof runtime.riskProfile;
      promotedAtMs: number | null;
    };

    const drafts: PromotionDraft[] = [];
    for (const deploymentId of consideredDeploymentIds) {
      const candidate = candidateByDeploymentId.get(deploymentId) || null;
      const existing = existingByDeploymentId.get(deploymentId) || null;
      const venue = (candidate?.venue || existing?.venue) as ScalpV2Venue | undefined;
      const symbol = String(candidate?.symbol || existing?.symbol || "")
        .trim()
        .toUpperCase();
      const strategyId = String(
        candidate?.strategyId || existing?.strategyId || "",
      )
        .trim()
        .toLowerCase();
      const tuneId = String(candidate?.tuneId || existing?.tuneId || "default")
        .trim()
        .toLowerCase();
      const entrySessionProfile = (candidate?.entrySessionProfile ||
        existing?.entrySessionProfile ||
        "berlin") as ScalpV2Session;
      if (
        !venue ||
        !symbol ||
        !strategyId ||
        !entrySessionProfile
      ) {
        continue;
      }
      const currentlyEnabled = Boolean(existing?.enabled);
      const lifecycle = normalizeLifecycle({
        promotionGate: existing?.promotionGate || null,
        tuneId,
        enabled: currentlyEnabled,
        nowTs,
      });
      const suppressed = lifecycleIsSuppressed(lifecycle, nowTs);
      const weeklyByWeekStart =
        weeklyByDeploymentSession.get(
          toDeploymentSessionKey(deploymentId, entrySessionProfile),
        ) || new Map();
      const sessionEvidenceBySession =
        sessionEvidenceByDeployment.get(deploymentId) || new Map();
      const matchedSessionTrades = sessionEvidenceBySession.get(entrySessionProfile) || 0;
      const mismatchedSessions = Array.from(
        new Set(
          Array.from(sessionEvidenceBySession.keys()).filter(
            (session) => session !== entrySessionProfile,
          ),
        ),
      ).sort();
      const mismatchedTrades = mismatchedSessions.reduce(
        (acc, session) => acc + (sessionEvidenceBySession.get(session) || 0),
        0,
      );
      const strictSessionEvidence = {
        expected: entrySessionProfile,
        matchedTrades: matchedSessionTrades,
        mismatchedTrades,
        mismatchedSessions,
      };
      const workerGate = resolveWorkerStageCPass(candidate?.metadata || null);
      const hasMixedSessionEvidence =
        strictSessionEvidence.mismatchedTrades > 0 ||
        strictSessionEvidence.mismatchedSessions.length > 0;
      const freshness = buildFreshness({
        weeklyByWeekStart,
        requiredWeeks: policy.minCompletedWeeks,
        nowTs,
      });
      const weeklyRows: WeeklyAggregationRow[] = [];
      for (let idx = 0; idx < freshness.requiredWeeks; idx += 1) {
        const weekStartTs = freshness.windowFromTs + idx * ONE_WEEK_MS;
        const bucket = weeklyByWeekStart.get(weekStartTs);
        if (!bucket) continue;
        const trades = Math.max(0, Math.floor(bucket.trades));
        weeklyRows.push({
          weekStartTs,
          trades,
          netR: bucket.netR,
          expectancyR: trades > 0 ? bucket.netR / trades : 0,
        });
      }
      const weeklyMetrics = computeWeeklyMetrics({
        weeklyRows,
        nowTs,
        fourWeekGroupSize: policy.fourWeekGroupSize,
        fourWeekGroupCount: policy.fourWeekGroupCount,
      });
      const winningWeeks12w = weeklyRows.filter((row) => row.netR > 0).length;
      const consecutiveWinningWeeks = longestConsecutiveWinningWeeks(weeklyRows);
      const weeklyGate = evaluateWeeklyGate({
        metrics: weeklyMetrics,
        policy,
      });
      const hasPerWeekTradesDeficit =
        policy.minTradesPerWeek > 0 &&
        freshness.ready &&
        weeklyRows.some((row) => row.trades < policy.minTradesPerWeek);
      const totalTrades =
        weeklyMetrics?.totalTrades ||
        weeklyRows.reduce((acc, row) => acc + row.trades, 0);

      let reason = "promotion_not_eligible";
      let eligible = false;
      if (suppressed) {
        reason =
          lifecycle.state === "retired"
            ? "retired_cooldown"
            : "suspended_cooldown";
      } else if (!candidate) {
        reason = droppedDeploymentIds.has(deploymentId)
          ? "budget_cap_rejected"
          : "candidate_missing";
      } else if (!workerGate.stageCPass) {
        reason = workerGate.reason || "worker_stage_c_failed";
      } else if (hasMixedSessionEvidence) {
        reason = "forward_session_evidence_mixed";
      } else if (!freshness.ready) {
        reason = "forward_session_12w_incomplete";
      } else if (hasPerWeekTradesDeficit) {
        reason = "forward_session_min_trades_per_window_below_threshold";
      } else if (totalTrades < policy.minTotalTrades) {
        reason = "forward_session_total_trades_below_threshold";
      } else if (!weeklyGate.passed) {
        reason = weeklyGate.reason || "weekly_robustness_failed";
      } else {
        eligible = true;
        reason = "weekly_robustness_passed";
      }

      const promotedAtMs = Number(
        asRecord(existing?.promotionGate || {}).promotedAtMs,
      );

      drafts.push({
        deploymentId,
        candidateId: candidate?.id ?? existing?.candidateId ?? null,
        venue,
        symbol,
        strategyId,
        tuneId,
        entrySessionProfile,
        score: candidate?.score ?? Number.NEGATIVE_INFINITY,
        currentlyEnabled,
        droppedByBudget: droppedDeploymentIds.has(deploymentId),
        lifecycle,
        suppressed,
        strictSessionEvidence,
        freshness,
        weeklyMetrics,
        winningWeeks12w,
        consecutiveWinningWeeks,
        weeklyGateReason: weeklyGate.reason,
        workerStageCPass: workerGate.stageCPass,
        workerStageReason: workerGate.reason,
        eligible,
        reason,
        enabled: false,
        shortlistIncluded: false,
        exactLoser: false,
        riskProfile: existing?.riskProfile || runtime.riskProfile,
        promotedAtMs: Number.isFinite(promotedAtMs) ? promotedAtMs : null,
      });
    }

    const winnerBySymbolStrategySession = new Map<string, PromotionDraft>();
    for (const row of drafts) {
      if (!row.eligible) continue;
      const key = `${row.venue}:${row.symbol}:${row.strategyId}:${row.entrySessionProfile}`;
      const current = winnerBySymbolStrategySession.get(key) || null;
      if (!current || row.score > current.score) {
        winnerBySymbolStrategySession.set(key, row);
      }
    }
    const shortlistRows = Array.from(winnerBySymbolStrategySession.values()).sort(
      (a, b) => b.score - a.score,
    );
    const winnerIds = new Set(
      shortlistRows
        .slice(
          0,
          Math.max(1, Math.floor(runtime.budgets.maxEnabledDeployments || 1)),
        )
        .map((row) => row.deploymentId),
    );
    for (const row of drafts) {
      row.shortlistIncluded = winnerIds.has(row.deploymentId);
      row.enabled =
        row.eligible &&
        (requireWinnerShortlist ? row.shortlistIncluded : true);
      if (row.eligible && requireWinnerShortlist && !row.shortlistIncluded) {
        row.reason = "winner_shortlist_excluded";
      }
      if (row.enabled) row.promotedAtMs = nowTs;
    }

    const loserFailReasons = new Set([
      "weekly_slice_count_below_threshold",
      "weekly_four_week_groups_missing",
      "weekly_four_week_net_r_below_threshold",
      "weekly_profitable_pct_below_threshold",
      "weekly_median_expectancy_below_threshold",
      "weekly_p25_expectancy_below_threshold",
      "weekly_worst_net_r_below_threshold",
      "weekly_top_week_concentration_above_threshold",
      "forward_session_min_trades_per_window_below_threshold",
      "forward_session_total_trades_below_threshold",
    ]);
    for (const row of drafts) {
      row.exactLoser = Boolean(
        row.currentlyEnabled &&
          !row.enabled &&
          row.freshness.ready &&
          !row.suppressed &&
          loserFailReasons.has(row.reason),
      );
      if (!row.exactLoser) continue;
      row.lifecycle = applyLifecycleSuspension({
        lifecycle: row.lifecycle,
        nowTs,
        durationMs: policy.exactLoserSuspendMs,
        retireMs: policy.retireMs,
        retireOnSuspensionCount: policy.retireOnSuspensionCount,
      });
      row.reason =
        row.lifecycle.state === "retired"
          ? "retired_cooldown"
          : "suspended_exact_loser";
    }

    const neighborSuspended = new Set<string>();
    for (const loser of drafts.filter((row) => row.exactLoser)) {
      const loserFamily = loser.lifecycle.tuneFamily || normalizeTuneFamily(loser.tuneId);
      for (const row of drafts) {
        if (row.deploymentId === loser.deploymentId) continue;
        if (row.enabled) continue;
        if (neighborSuspended.has(row.deploymentId)) continue;
        if (row.venue !== loser.venue) continue;
        if (row.symbol !== loser.symbol) continue;
        if (row.strategyId !== loser.strategyId) continue;
        if (row.entrySessionProfile !== loser.entrySessionProfile) continue;
        const family = row.lifecycle.tuneFamily || normalizeTuneFamily(row.tuneId);
        if (family !== loserFamily) continue;
        if (lifecycleIsSuppressed(row.lifecycle, nowTs)) continue;
        row.lifecycle = applyLifecycleSuspension({
          lifecycle: row.lifecycle,
          nowTs,
          durationMs: policy.neighborSuspendMs,
          retireMs: policy.retireMs,
          retireOnSuspensionCount: policy.retireOnSuspensionCount,
        });
        row.reason =
          row.lifecycle.state === "retired"
            ? "retired_cooldown"
            : "suspended_neighbor_family";
        neighborSuspended.add(row.deploymentId);
      }
    }

    const enabledByUniquenessKey = new Map<string, PromotionDraft[]>();
    for (const row of drafts.filter((draft) => draft.enabled)) {
      const key = `${row.venue}:${row.symbol}:${row.strategyId}:${row.entrySessionProfile}`;
      const bucket = enabledByUniquenessKey.get(key) || [];
      bucket.push(row);
      enabledByUniquenessKey.set(key, bucket);
    }
    let demotedByUniqueness = 0;
    for (const bucket of enabledByUniquenessKey.values()) {
      if (bucket.length <= 1) continue;
      bucket.sort((a, b) => b.score - a.score);
      for (let idx = 1; idx < bucket.length; idx += 1) {
        const row = bucket[idx]!;
        row.enabled = false;
        row.reason = "symbol_strategy_uniqueness_demoted";
        row.lifecycle.state = "candidate";
        row.lifecycle.lastSeatReleaseAtMs = nowTs;
        demotedByUniqueness += 1;
      }
    }

    for (const row of drafts) {
      if (!row.enabled && row.currentlyEnabled) {
        row.lifecycle.lastSeatReleaseAtMs = row.lifecycle.lastSeatReleaseAtMs || nowTs;
      }
      if (!lifecycleIsSuppressed(row.lifecycle, nowTs)) {
        row.lifecycle.state = row.enabled ? "graduated" : "candidate";
      }
    }

    const rows: Parameters<typeof upsertScalpV2Deployments>[0]["rows"] = drafts.map(
      (row) => ({
        candidateId: row.candidateId,
        venue: row.venue,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        entrySessionProfile: row.entrySessionProfile,
        enabled: row.enabled,
        liveMode: row.enabled && runtime.liveEnabled ? "live" : "shadow",
        promotionGate: {
          eligible: row.eligible,
          reason: row.reason,
          source: "v2_forward_evidence",
          evaluatedAtMs: nowTs,
          promotedAtMs: row.promotedAtMs,
          score: Number.isFinite(row.score) ? row.score : null,
          droppedByBudget: row.droppedByBudget,
          strictSessionEvidence: row.strictSessionEvidence,
          freshness: row.freshness,
          weekly: row.weeklyMetrics,
          lifecycle: row.lifecycle,
          thresholds: {
            minCompletedWeeks: policy.minCompletedWeeks,
            strictPerSession: true,
            minTradesPerWeek: policy.minTradesPerWeek,
            minTotalTrades: policy.minTotalTrades,
            minSlices: policy.minSlices,
            minProfitablePct: policy.minProfitablePct,
            minMedianExpectancyR: policy.minMedianExpectancyR,
            minP25ExpectancyR: policy.minP25ExpectancyR,
            minWorstNetR: policy.minWorstNetR,
            maxTopWeekPnlConcentrationPct:
              policy.maxTopWeekPnlConcentrationPct,
            minFourWeekNetR: policy.minFourWeekNetR,
            fourWeekGroupCount: policy.fourWeekGroupCount,
            fourWeekGroupSize: policy.fourWeekGroupSize,
          },
          shortlistIncluded: row.shortlistIncluded,
        },
        riskProfile: row.riskProfile,
      }),
    );

    await upsertScalpV2Deployments({ rows });

    const promotedIds = drafts
      .filter((row) => row.candidateId !== null && row.enabled)
      .map((row) => Number(row.candidateId));
    const shadowIds = drafts
      .filter((row) => row.candidateId !== null && !row.enabled)
      .map((row) => Number(row.candidateId));
    const rejectedIds = trimmed.dropped.map((row) => row.id);
    if (promotedIds.length > 0) {
      await updateScalpV2CandidateStatuses({
        ids: promotedIds,
        status: "promoted",
        metadataPatch: { promotedAtMs: nowTs },
      });
    }
    if (shadowIds.length > 0) {
      await updateScalpV2CandidateStatuses({
        ids: shadowIds,
        status: "shadow",
        metadataPatch: { shadowedAtMs: nowTs },
      });
    }
    if (rejectedIds.length > 0) {
      await updateScalpV2CandidateStatuses({
        ids: rejectedIds,
        status: "rejected",
        metadataPatch: { rejectedAtMs: nowTs, reason: "BUDGET_CAP" },
      });
    }

    const capOut = await enforceScalpV2EnabledCap({
      maxEnabledDeployments: runtime.budgets.maxEnabledDeployments,
    });

    const highlightRows = drafts
      .filter((row) => row.freshness.ready && row.weeklyMetrics && row.eligible)
      .sort((a, b) => (b.score || 0) - (a.score || 0))
      .slice(0, 200)
      .map((row) => {
        const candidate = candidateByDeploymentId.get(row.deploymentId) || null;
        const candidateMeta = asRecord(candidate?.metadata || {});
        return {
          candidateId: row.deploymentId,
          venue: row.venue,
          symbol: row.symbol,
          entrySessionProfile: row.entrySessionProfile,
          score: Number.isFinite(row.score) ? row.score : 0,
          trades12w: row.weeklyMetrics?.totalTrades || 0,
          winningWeeks12w: row.winningWeeks12w,
          consecutiveWinningWeeks: row.consecutiveWinningWeeks,
          robustness: {
            reason: row.reason,
            strictSessionEvidence: row.strictSessionEvidence,
            freshness: row.freshness,
            weekly: row.weeklyMetrics,
            thresholds: {
              minCompletedWeeks: policy.minCompletedWeeks,
              strictPerSession: true,
              minTradesPerWeek: policy.minTradesPerWeek,
              minTotalTrades: policy.minTotalTrades,
              minProfitablePct: policy.minProfitablePct,
              minMedianExpectancyR: policy.minMedianExpectancyR,
              minP25ExpectancyR: policy.minP25ExpectancyR,
              minWorstNetR: policy.minWorstNetR,
              minFourWeekNetR: policy.minFourWeekNetR,
            },
          },
          dsl: asRecord(candidateMeta.researchDsl || candidateMeta.dsl || {}),
          notes: row.enabled
            ? "eligible_and_enabled"
            : "eligible_not_shortlisted_or_capped",
          remarkable: true,
        };
      });
    const highlightsUpserted = await upsertScalpV2ResearchHighlights({
      rows: highlightRows,
    }).catch(() => 0);

    const cursorAggregates = new Map<
      string,
      {
        venue: ScalpV2Venue;
        symbol: string;
        entrySessionProfile: ScalpV2Session;
        considered: number;
        eligible: number;
        enabled: number;
        highlighted: number;
      }
    >();
    for (const row of drafts) {
      const key = `${row.venue}:${row.symbol}:${row.entrySessionProfile}`;
      const current = cursorAggregates.get(key) || {
        venue: row.venue,
        symbol: row.symbol,
        entrySessionProfile: row.entrySessionProfile,
        considered: 0,
        eligible: 0,
        enabled: 0,
        highlighted: 0,
      };
      current.considered += 1;
      if (row.eligible) current.eligible += 1;
      if (row.enabled) current.enabled += 1;
      if (row.freshness.ready && row.weeklyMetrics && row.eligible) {
        current.highlighted += 1;
      }
      cursorAggregates.set(key, current);
    }
    await Promise.all(
      Array.from(cursorAggregates.values()).map((row) =>
        updateResearchCursorSafe({
          venue: row.venue,
          symbol: row.symbol,
          entrySessionProfile: row.entrySessionProfile,
          phase: row.enabled > 0 ? "promote" : "validate",
          lastWeekStartMs: windowFromTs,
          progress: {
            considered: row.considered,
            eligible: row.eligible,
            enabled: row.enabled,
            highlighted: row.highlighted,
            policyWeeks: policy.minCompletedWeeks,
            updatedAtMs: nowMs(),
          },
        }),
      ),
    );

    const reasonCounts = drafts.reduce<Record<string, number>>((acc, row) => {
      const key = row.reason || "unknown";
      acc[key] = (acc[key] || 0) + 1;
      return acc;
    }, {});
    const suppressedCount = drafts.filter((row) =>
      lifecycleIsSuppressed(row.lifecycle, nowTs),
    ).length;
    const enabledCount = drafts.filter((row) => row.enabled).length;

    processed = promotionPool.length;
    succeeded = rows.length;
    details = {
      considered: promotionPool.length,
      deploymentsConsidered: drafts.length,
      promoted: promotedIds.length,
      shadowed: shadowIds.length,
      rejectedByBudget: trimmed.dropped.length,
      suppressedCount,
      enabledCount,
      requireWinnerShortlist,
      demotedByUniqueness,
      demotedByEnabledCap: capOut.demoted,
      exactLosers: drafts.filter((row) => row.exactLoser).length,
      neighborSuspended: neighborSuspended.size,
      freshnessWindowWeeks: policy.minCompletedWeeks,
      strictPerSessionForwardGate: true,
      filteredCandidatesOutOfScope,
      filteredDeploymentsOutOfScope,
      demotedOutOfScopeEnabled: offScopeEnabledDeployments.length,
      sessionEvidenceMixedCount: drafts.filter(
        (row) => row.reason === "forward_session_evidence_mixed",
      ).length,
      minTradesPerWeek: policy.minTradesPerWeek,
      minTotalTrades: policy.minTotalTrades,
      enabledSlots: runtime.budgets.maxEnabledDeployments,
      highlightsUpserted,
      workerStageMissingCount: drafts.filter(
        (row) => row.reason === "worker_stage_c_missing",
      ).length,
      workerStageFailedCount: drafts.filter(
        (row) => row.reason === "worker_stage_c_failed",
      ).length,
      reasonCounts,
      liveEnabled: runtime.liveEnabled,
    };

    return buildScalpV2JobResult({
      jobKind: "promote",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "promote",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "promote",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

function pickBitgetSnapshotBySymbol(
  snapshots: Array<{
    epic: string;
    dealId: string | null;
    dealReference: string | null;
    side: "long" | "short" | null;
    entryPrice: number | null;
    leverage: number | null;
    size: number | null;
    updatedAtMs: number;
  }>,
  symbol: string,
) {
  const target = String(symbol || "").trim().toUpperCase();
  return snapshots.find((row) => String(row.epic || "").trim().toUpperCase() === target) || null;
}

function pickCapitalSnapshotBySymbol(
  snapshots: Array<{
    epic: string;
    dealId: string | null;
    dealReference: string | null;
    side: "long" | "short" | null;
    entryPrice: number | null;
    leverage: number | null;
    size: number | null;
    updatedAtMs: number;
  }>,
  symbol: string,
) {
  const target = String(symbol || "").trim().toUpperCase();
  return snapshots.find((row) => String(row.epic || "").trim().toUpperCase() === target) || null;
}

export async function runScalpV2ExecuteJob(params: {
  dryRun?: boolean;
  session?: ScalpV2Session;
  venue?: ScalpV2Venue;
} = {}): Promise<ScalpV2JobResult> {
  const owner = lockOwner("execute");
  const claimed = await claimScalpV2Job({ jobKind: "execute", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "execute",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const runtime = await loadScalpV2RuntimeConfig();
    const effectiveDryRun = params.dryRun ?? runtime.dryRunDefault;
    const nowTs = nowMs();
    const allowSundayExecute = envBool("SCALP_V2_ALLOW_SUNDAY_EXECUTE", false);
    if (!allowSundayExecute && isScalpV2SundayUtc(nowTs)) {
      details = {
        executedDeployments: 0,
        skipped: true,
        reason: "sunday_utc_execution_blocked",
        sundayUtc: true,
        allowSundayExecute,
      };
      return buildScalpV2JobResult({
        jobKind: "execute",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const deployments = await listScalpV2Deployments({
      enabledOnly: true,
      venue: params.venue,
      session: params.session,
      limit: 500,
    });
    const scopedDeployments = deployments.filter((deployment) =>
      isScalpV2RuntimeSymbolInScope({
        runtime,
        venue: deployment.venue,
        symbol: deployment.symbol,
        includeLiveSeeds: true,
      }),
    );
    const filteredOutOfScope = Math.max(0, deployments.length - scopedDeployments.length);

    if (!scopedDeployments.length) {
      details = {
        executed: 0,
        reason:
          deployments.length > 0
            ? "no_enabled_deployments_in_runtime_scope"
            : "no_enabled_deployments",
        filteredOutOfScope,
      };
      return buildScalpV2JobResult({
        jobKind: "execute",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const bitgetAdapter = getScalpV2VenueAdapter("bitget");
    const capitalAdapter = getScalpV2VenueAdapter("capital");
    const bitgetSnapshots = await bitgetAdapter.broker.fetchOpenPositionSnapshots().catch(() => []);
    const capitalSnapshots = await capitalAdapter.broker.fetchOpenPositionSnapshots().catch(() => []);

    for (const deployment of scopedDeployments) {
      processed += 1;
      const deploymentDryRun = effectiveDryRun || !runtime.liveEnabled || deployment.liveMode !== "live";
      try {
        const result = await runScalpV2ExecuteCycle({
          venue: deployment.venue,
          symbol: deployment.symbol,
          strategyId: deployment.strategyId,
          tuneId: deployment.tuneId,
          deploymentId: deployment.deploymentId,
          dryRun: deploymentDryRun,
        });

        await appendScalpV2ExecutionEvent(
          buildEvent({
            deploymentId: deployment.deploymentId,
            venue: deployment.venue,
            symbol: deployment.symbol,
            strategyId: deployment.strategyId,
            tuneId: deployment.tuneId,
            entrySessionProfile: deployment.entrySessionProfile,
            eventType: "position_snapshot",
            reasonCodes: result.reasonCodes,
            sourceOfTruth: "system",
            rawPayload: {
              state: result.state,
              dryRun: result.dryRun,
              runLockAcquired: result.runLockAcquired,
            },
          }),
        );

        const snapshot =
          deployment.venue === "capital"
            ? pickCapitalSnapshotBySymbol(capitalSnapshots, deployment.symbol)
            : pickBitgetSnapshotBySymbol(bitgetSnapshots, deployment.symbol);
        await upsertScalpV2PositionSnapshot({
          deploymentId: deployment.deploymentId,
          venue: deployment.venue,
          symbol: deployment.symbol,
          side: snapshot?.side || null,
          entryPrice: snapshot?.entryPrice ?? null,
          leverage: snapshot?.leverage ?? null,
          size: snapshot?.size ?? null,
          dealId: snapshot?.dealId ?? null,
          dealReference: snapshot?.dealReference ?? null,
          brokerSnapshotAtMs: snapshot?.updatedAtMs ?? nowMs(),
          status: snapshot?.side ? "open" : "flat",
          rawPayload: snapshot ? { snapshot } : {},
        });
        succeeded += 1;
      } catch (err: any) {
        failed += 1;
        await appendScalpV2ExecutionEvent(
          buildEvent({
            deploymentId: deployment.deploymentId,
            venue: deployment.venue,
            symbol: deployment.symbol,
            strategyId: deployment.strategyId,
            tuneId: deployment.tuneId,
            entrySessionProfile: deployment.entrySessionProfile,
            eventType: "order_rejected",
            reasonCodes: ["SCALP_V2_EXECUTION_ERROR"],
            sourceOfTruth: "system",
            rawPayload: {
              message: err?.message || String(err),
              dryRun: deploymentDryRun,
            },
          }),
        );
      }
    }

    details = {
      executedDeployments: scopedDeployments.length,
      dryRun: effectiveDryRun,
      liveEnabled: runtime.liveEnabled,
      filteredOutOfScope,
    };

    return buildScalpV2JobResult({
      jobKind: "execute",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "execute",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await snapshotScalpV2DailyMetrics().catch(() => undefined);
    await finalizeScalpV2Job({
      jobKind: "execute",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

function snapshotExists(params: {
  venue: ScalpV2Venue;
  symbol: string;
  side: "long" | "short" | null;
  dealId: string | null;
  dealReference: string | null;
  bitgetSnapshots: Array<{
    epic: string;
    side: "long" | "short" | null;
    dealId: string | null;
    dealReference: string | null;
  }>;
  capitalSnapshots: Array<{
    epic: string;
    side: "long" | "short" | null;
    dealId: string | null;
    dealReference: string | null;
  }>;
}): boolean {
  const targetList = params.venue === "capital" ? params.capitalSnapshots : params.bitgetSnapshots;
  const symbol = String(params.symbol || "").trim().toUpperCase();

  for (const snapshot of targetList) {
    const sameSymbol = String(snapshot.epic || "").trim().toUpperCase() === symbol;
    if (!sameSymbol) continue;

    if (params.dealId && snapshot.dealId && params.dealId === snapshot.dealId) return true;
    if (
      params.dealReference &&
      snapshot.dealReference &&
      params.dealReference === snapshot.dealReference
    ) {
      return true;
    }
    if (params.side && snapshot.side && params.side === snapshot.side) return true;
    if (!params.dealId && !params.dealReference && !params.side) return true;
  }

  return false;
}

export async function runScalpV2ReconcileJob(): Promise<ScalpV2JobResult> {
  const owner = lockOwner("reconcile");
  const claimed = await claimScalpV2Job({ jobKind: "reconcile", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "reconcile",
      processed: 0,
      succeeded: 0,
      failed: 0,
      busy: true,
      pendingAfter: 0,
      details: { reason: "job_locked" },
    });
  }

  let ok = true;
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let details: Record<string, unknown> = {};

  try {
    const openPositions = await listScalpV2OpenPositions();
    const bitgetAdapter = getScalpV2VenueAdapter("bitget");
    const bitgetSnapshots = await bitgetAdapter.broker.fetchOpenPositionSnapshots();
    const capitalSnapshots = await fetchCapitalOpenPositionSnapshots().catch(() => []);

    for (const position of openPositions) {
      processed += 1;
      const exists = snapshotExists({
        venue: position.venue,
        symbol: position.symbol,
        side: position.side,
        dealId: position.dealId,
        dealReference: position.dealReference,
        bitgetSnapshots,
        capitalSnapshots,
      });

      if (exists) {
        succeeded += 1;
        continue;
      }

      try {
        await appendScalpV2ExecutionEvent(
          buildEvent({
            deploymentId: position.deploymentId,
            venue: position.venue,
            symbol: position.symbol,
            strategyId: "unknown",
            tuneId: "unknown",
            entrySessionProfile: "berlin",
            eventType: "reconcile_close",
            reasonCodes: ["SCALP_V2_RECONCILE_CLOSE"],
            sourceOfTruth: "reconciler",
            rawPayload: {
              reason: "broker_position_missing",
              dealId: position.dealId,
              dealReference: position.dealReference,
              rMultiple: 0,
              pnlUsd: null,
            },
          }),
        );

        await upsertScalpV2PositionSnapshot({
          deploymentId: position.deploymentId,
          venue: position.venue,
          symbol: position.symbol,
          side: null,
          entryPrice: null,
          leverage: null,
          size: null,
          dealId: null,
          dealReference: null,
          brokerSnapshotAtMs: nowMs(),
          status: "flat",
          rawPayload: {
            reconciledAtMs: nowMs(),
            reason: "broker_position_missing",
          },
        });
        succeeded += 1;
      } catch {
        failed += 1;
      }
    }

    await snapshotScalpV2DailyMetrics().catch(() => undefined);

    details = {
      examinedOpenPositions: openPositions.length,
      reconciled: Math.max(0, succeeded - (openPositions.length - processed)),
    };

    return buildScalpV2JobResult({
      jobKind: "reconcile",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "reconcile",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "reconcile",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

export async function runScalpV2FullAutoCycle(params: {
  executeDryRun?: boolean;
  venue?: ScalpV2Venue;
  session?: ScalpV2Session;
} = {}): Promise<{
  discover: ScalpV2JobResult;
  evaluate: ScalpV2JobResult;
  worker: ScalpV2JobResult;
  promote: ScalpV2JobResult;
  execute: ScalpV2JobResult;
  reconcile: ScalpV2JobResult;
}> {
  const discover = await runScalpV2DiscoverJob();
  const evaluate = await runScalpV2EvaluateJob();
  const worker = await runScalpV2WorkerJob();
  const promote = await runScalpV2PromoteJob();
  const execute = await runScalpV2ExecuteJob({
    dryRun: params.executeDryRun,
    venue: params.venue,
    session: params.session,
  });
  const reconcile = await runScalpV2ReconcileJob();
  return { discover, evaluate, worker, promote, execute, reconcile };
}
