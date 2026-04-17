import crypto from "crypto";

import { fetchCapitalOpenPositionSnapshots } from "../capital";
import { loadScalpCandleHistoryInRange } from "../scalp/candleHistory";
import { pipSizeForScalpSymbol } from "../scalp/marketData";
import { loadScalpSymbolMarketMetadataBulk } from "../scalp/symbolMarketMetadataStore";
import type { ScalpSymbolMarketMetadata } from "../scalp/symbolMarketMetadata";
import type { ScalpStrategyRuntimeSnapshot } from "../scalp/store";
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
  buildModelGuidedComposerTuneId,
  parseExitRuleFromTuneId,
  parseRegimeGateFromTuneId,
  resolveModelGuidedComposerExecutionPlanFromBlocks,
  resolveModelGuidedComposerExecutionPlanFromTuneId,
} from "./composerExecution";
import {
  ENTRY_TRIGGER_COMPAT,
  resolveEntryTriggerOverrides,
} from "./entryTriggerPresets";
import { resolveExitRuleOverrides } from "./exitRulePresets";
import {
  RISK_RULE_RESEARCH_PROFILES,
  mergeRiskProfileWithOverrides,
  resolveRiskRuleOverrides,
  resolveRiskRuleReplayOverrides,
} from "./riskRulePresets";
import {
  resolveStateMachineOverrides,
  resolveStateMachineReplayOverrides,
  STATE_MACHINE_RESEARCH_PROFILES,
} from "./stateMachinePresets";
import { buildScalpV2ExecuteConfigOverride } from "./executeConfigOverride";
import { runScalpV2ExecuteCycle } from "./executeAdapter";
import { createScalpV2ExecutionPersistenceAdapter } from "./executionPersistence";
import {
  appendScalpV2ExecutionEvent,
  buildScalpV2JobResult,
  claimScalpV2Job,
  countScalpV2CandidatesByStatus,
  enforceScalpV2EnabledCap,
  finalizeScalpV2Job,
  heartbeatScalpV2Job,
  listScalpV2Candidates,
  loadScalpV2EvaluatedCandidateKeys,
  listScalpV2DiscoveredSymbols,
  loadScalpV2WarmUpState,
  upsertScalpV2WarmUpState,
  loadScalpV2PreviousWeekResults,
  loadScalpV2WeeklyCache,
  upsertScalpV2WeeklyCache,
  pruneScalpV2WeeklyCache,
  loadScalpV2ScopeWindowStageStats,
  listScalpV2Deployments,
  listScalpV2LedgerRows,
  listScalpV2OpenPositions,
  loadScalpV2ResearchCursor,
  loadScalpV2RuntimeConfig,
  requeueScalpV2DeploymentCandidatesForWindow,
  snapshotScalpV2DailyMetrics,
  toDeploymentId,
  updateScalpV2CandidateStatuses,
  upsertScalpV2Candidates,
  upsertScalpV2Deployments,
  upsertScalpV2PositionSnapshot,
  upsertScalpV2ResearchCursor,
  upsertScalpV2ResearchHighlights,
  upsertScalpV2RuntimeConfig,
} from "./db";
import {
  isScalpV2DiscoverSymbolAllowed,
  isScalpV2SundayUtc,
} from "./logic";
import {
  buildScalpV2ModelGuidedComposerGrid,
  toScalpV2ResearchCursorKey,
} from "./research";
import { runScalpV2LoadCandlesPipelineJob } from "./pipelineJobsAdapter";
import { inferScalpV2AssetCategory, minSpreadPipsForCategory } from "./symbolInfo";
import { getScalpV2VenueAdapter } from "./venueAdapter";
import {
  resolveScalpV2CompletedWeekWindowToUtc,
  startOfScalpV2WeekMondayUtc,
} from "./weekWindows";
import type {
  ScalpV2ExecutionEvent,
  ScalpV2JobKind,
  ScalpV2JobResult,
  ScalpV2RuntimeConfig,
  ScalpV2RuntimePrunedScopeEntry,
  ScalpV2Session,
  ScalpV2Venue,
  ScalpV2WorkerStageWeeklyMetrics,
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
const ONE_HOUR_MS = 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;
const LIFECYCLE_SUSPEND_WINDOW_MS = 180 * ONE_DAY_MS;
const DEFAULT_SCOPE_PRUNE_TTL_DAYS = 28;


type ScalpV2ScopePrunePolicy = {
  enabled: boolean;
  ttlMs: number;
  ttlDays: number;
  minCandidatesPerWindow: number;
  minStageAFailPct: number;
  requiredWindows: number;
};

function toScalpV2ScopeKey(params: {
  venue: ScalpV2Venue;
  symbol: string;
  session: ScalpV2Session;
}): string {
  const venue = String(params.venue || "bitget").trim().toLowerCase();
  const symbol = String(params.symbol || "").trim().toUpperCase();
  const session = String(params.session || "berlin").trim().toLowerCase();
  return `${venue}:${symbol}:${session}`;
}

function normalizeScopeVenue(value: unknown): ScalpV2Venue | null {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "bitget") return "bitget";
  if (normalized === "capital") return "capital";
  return null;
}

function normalizeScopeSession(value: unknown): ScalpV2Session | null {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "tokyo") return "tokyo";
  if (normalized === "berlin") return "berlin";
  if (normalized === "newyork") return "newyork";
  if (normalized === "pacific") return "pacific";
  if (normalized === "sydney") return "sydney";
  return null;
}

function normalizeActivePrunedScopes(
  runtime: ScalpV2RuntimeConfig,
  nowTs: number,
): Record<string, ScalpV2RuntimePrunedScopeEntry> {
  const raw = asRecord(runtime.prunedScopes);
  const out: Record<string, ScalpV2RuntimePrunedScopeEntry> = {};
  for (const value of Object.values(raw)) {
    const row = asRecord(value);
    const venue = normalizeScopeVenue(row.venue);
    const session = normalizeScopeSession(row.session);
    const symbol = String(row.symbol || "")
      .trim()
      .toUpperCase();
    const expiresAtMs = Math.floor(Number(row.expiresAtMs));
    if (!venue || !session || !symbol || !Number.isFinite(expiresAtMs)) continue;
    if (expiresAtMs <= nowTs) continue;
    const prunedAtMs = Math.floor(Number(row.prunedAtMs));
    const windows = Array.isArray(row.windows)
      ? row.windows
          .map((entry) => Math.floor(Number(entry)))
          .filter((entry) => Number.isFinite(entry) && entry > 0)
      : [];
    const thresholdsRow = asRecord(row.thresholds);
    const key = toScalpV2ScopeKey({ venue, symbol, session });
    out[key] = {
      venue,
      symbol,
      session,
      prunedAtMs: Number.isFinite(prunedAtMs) && prunedAtMs > 0 ? prunedAtMs : nowTs,
      expiresAtMs,
      source: String(row.source || "v2_scope_prune").trim() || "v2_scope_prune",
      reason:
        String(row.reason || "chronic_stage_a_fail").trim() ||
        "chronic_stage_a_fail",
      thresholds: {
        minCandidatesPerWindow: Math.max(
          1,
          Math.floor(Number(thresholdsRow.minCandidatesPerWindow) || 60),
        ),
        minStageAFailPct: Math.max(
          0,
          Math.min(100, Number(thresholdsRow.minStageAFailPct) || 85),
        ),
        requiredWindows: Math.max(
          2,
          Math.min(6, Math.floor(Number(thresholdsRow.requiredWindows) || 2)),
        ),
      },
      windows: Array.from(new Set(windows)).sort((a, b) => b - a),
    };
  }
  return out;
}

function resolveScalpV2ScopePrunePolicy(): ScalpV2ScopePrunePolicy {
  const ttlDays = Math.max(
    7,
    Math.min(
      180,
      toPositiveInt(
        process.env.SCALP_V2_SCOPE_PRUNE_TTL_DAYS,
        DEFAULT_SCOPE_PRUNE_TTL_DAYS,
        180,
      ),
    ),
  );
  return {
    enabled: envBool("SCALP_V2_SCOPE_PRUNE_ENABLED", true),
    ttlMs: ttlDays * ONE_DAY_MS,
    ttlDays,
    minCandidatesPerWindow: Math.max(
      1,
      Math.min(
        1_000,
        toPositiveInt(
          process.env.SCALP_V2_SCOPE_PRUNE_MIN_CANDIDATES_PER_WINDOW,
          60,
          1_000,
        ),
      ),
    ),
    minStageAFailPct: Math.max(
      0,
      Math.min(
        100,
        toFinite(process.env.SCALP_V2_SCOPE_PRUNE_MIN_STAGE_A_FAIL_PCT, 85),
      ),
    ),
    requiredWindows: Math.max(
      2,
      Math.min(
        4,
        toPositiveInt(process.env.SCALP_V2_SCOPE_PRUNE_REQUIRED_WINDOWS, 2, 4),
      ),
    ),
  };
}

async function runScalpV2ScopePrunePass(params: {
  runtime: ScalpV2RuntimeConfig;
  windowToTs: number;
  nowTs: number;
}): Promise<{
  runtime: ScalpV2RuntimeConfig;
  details: Record<string, unknown>;
}> {
  const policy = resolveScalpV2ScopePrunePolicy();
  const activeScopes = normalizeActivePrunedScopes(params.runtime, params.nowTs);
  const meta = asRecord(params.runtime.scopePruneMeta);
  const lastPruneWindowToTs = Math.floor(Number(meta.lastPruneWindowToTs));
  const alreadyPrunedThisWindow =
    Number.isFinite(lastPruneWindowToTs) &&
    lastPruneWindowToTs === params.windowToTs;
  if (!policy.enabled) {
    return {
      runtime: {
        ...params.runtime,
        prunedScopes: activeScopes,
      },
      details: {
        enabled: false,
        skipped: true,
        reason: "scope_prune_disabled",
        activeScopeCount: Object.keys(activeScopes).length,
      },
    };
  }
  if (alreadyPrunedThisWindow) {
    return {
      runtime: {
        ...params.runtime,
        prunedScopes: activeScopes,
      },
      details: {
        enabled: true,
        skipped: true,
        reason: "already_pruned_for_window",
        lastPruneWindowToTs: params.windowToTs,
        activeScopeCount: Object.keys(activeScopes).length,
      },
    };
  }

  const snapshots = await loadScalpV2ScopeWindowStageStats({
    latestWindowCount: policy.requiredWindows,
  });
  const windows = Array.from(
    new Set(
      snapshots
        .map((row) => Math.floor(Number(row.windowToTs)))
        .filter((row) => Number.isFinite(row) && row > 0),
    ),
  ).sort((a, b) => b - a);
  const requiredWindows = windows.slice(0, policy.requiredWindows);

  const byScope = new Map<string, Map<number, (typeof snapshots)[number]>>();
  for (const row of snapshots) {
    if (!requiredWindows.includes(row.windowToTs)) continue;
    const key = toScalpV2ScopeKey({
      venue: row.venue,
      symbol: row.symbol,
      session: row.session,
    });
    const perWindow = byScope.get(key) || new Map();
    perWindow.set(row.windowToTs, row);
    byScope.set(key, perWindow);
  }

  const nextPrunedScopes: Record<string, ScalpV2RuntimePrunedScopeEntry> = {
    ...activeScopes,
  };
  const candidateScopeCount = byScope.size;
  const newlyPrunedScopeKeys: string[] = [];
  let skippedInsufficientWindowHistory = false;

  if (requiredWindows.length < policy.requiredWindows) {
    skippedInsufficientWindowHistory = true;
  } else {
    for (const [scopeKey, perWindow] of byScope.entries()) {
      let shouldPrune = true;
      for (const windowToTs of requiredWindows) {
        const row = perWindow.get(windowToTs);
        if (!row) {
          shouldPrune = false;
          break;
        }
        const total = Number(row.total) || 0;
        const stageAPass = Number(row.stageAPass) || 0;
        const stageCPass = Number(row.stageCPass) || 0;
        const stageAFailPct = total > 0 ? ((total - stageAPass) / total) * 100 : 0;
        if (total < policy.minCandidatesPerWindow) {
          shouldPrune = false;
          break;
        }
        if (stageAFailPct < policy.minStageAFailPct) {
          shouldPrune = false;
          break;
        }
        if (stageCPass > 0) {
          shouldPrune = false;
          break;
        }
      }
      if (!shouldPrune) continue;
      const [venueRaw, symbolRaw, sessionRaw] = scopeKey.split(":");
      const venue = normalizeScopeVenue(venueRaw);
      const session = normalizeScopeSession(sessionRaw);
      const symbol = String(symbolRaw || "").trim().toUpperCase();
      if (!venue || !session || !symbol) continue;
      const previouslyPruned = Boolean(nextPrunedScopes[scopeKey]);
      nextPrunedScopes[scopeKey] = {
        venue,
        symbol,
        session,
        prunedAtMs: params.nowTs,
        expiresAtMs: params.nowTs + policy.ttlMs,
        source: "v2_scope_prune",
        reason: "chronic_stage_a_fail",
        thresholds: {
          minCandidatesPerWindow: policy.minCandidatesPerWindow,
          minStageAFailPct: policy.minStageAFailPct,
          requiredWindows: policy.requiredWindows,
        },
        windows: requiredWindows.slice(),
      };
      if (!previouslyPruned) newlyPrunedScopeKeys.push(scopeKey);
    }
  }

  const updatedRuntime = await upsertScalpV2RuntimeConfig({
    ...params.runtime,
    prunedScopes: nextPrunedScopes,
    scopePruneMeta: {
      lastPruneWindowToTs: params.windowToTs,
      lastPrunedAtMs: params.nowTs,
      lastActiveScopeCount: Object.keys(nextPrunedScopes).length,
      lastNewlyPrunedScopeCount: newlyPrunedScopeKeys.length,
    },
  });

  return {
    runtime: updatedRuntime,
    details: {
      enabled: true,
      skipped: false,
      reason: skippedInsufficientWindowHistory
        ? "insufficient_window_history"
        : "scope_prune_completed",
      windowToTs: params.windowToTs,
      ttlDays: policy.ttlDays,
      requiredWindows,
      policy: {
        minCandidatesPerWindow: policy.minCandidatesPerWindow,
        minStageAFailPct: policy.minStageAFailPct,
        requiredWindows: policy.requiredWindows,
      },
      candidateScopeCount,
      activeScopeCount: Object.keys(nextPrunedScopes).length,
      newlyPrunedScopeCount: newlyPrunedScopeKeys.length,
      newlyPrunedScopes: newlyPrunedScopeKeys,
    },
  };
}

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
  /** Per-week netR keyed by Monday-UTC start timestamp. */
  weeklyNetR?: Record<string, number>;
  /** Max single-week netR across the stage window. */
  maxWeeklyNetR?: number | null;
  /** Largest single-trade R-multiple (absolute). */
  largestTradeR?: number | null;
  /** Exit reason counts. */
  exitReasons?: { stop: number; tp: number; timeStop: number; forceClose: number } | null;
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

function toNonNegativeInt(value: unknown, fallback: number, max = 100_000): number {
  const n = Math.floor(toFinite(value, fallback));
  if (!Number.isFinite(n) || n < 0) return fallback;
  return Math.max(0, Math.min(max, n));
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
    minFourWeekNetR: toFinite(process.env.SCALP_V2_PROMOTION_MIN_4W_NET_R, 4),
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
        toPositiveInt(process.env.SCALP_V2_WORKER_BATCH_SIZE, 12, 600),
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
            toFinite(process.env.SCALP_V2_WORKER_STAGE_A_MIN_TRADES, 4),
          ),
        ),
      ),
      minNetR: toFinite(process.env.SCALP_V2_WORKER_STAGE_A_MIN_NET_R, 0.2),
      minConsecutiveWinningWeeks: Math.max(
        0,
        Math.min(
          4,
          Math.floor(
            toFinite(
              process.env.SCALP_V2_WORKER_STAGE_A_MIN_CONSEC_WIN_WEEKS,
              1,
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

function listWeekStarts(params: { fromTs: number; toTs: number }): number[] {
  const fromTs = Math.floor(Number(params.fromTs) || 0);
  const toTs = Math.floor(Number(params.toTs) || 0);
  if (!Number.isFinite(fromTs) || !Number.isFinite(toTs) || toTs <= fromTs) {
    return [];
  }
  const out: number[] = [];
  for (let weekStart = fromTs; weekStart < toTs; weekStart += ONE_WEEK_MS) {
    out.push(weekStart);
  }
  return out;
}

function buildWorkerStageWeeklyMetrics(params: {
  trades: ScalpReplayTrade[];
  weekStartTs: number;
  weekToTs: number;
}): ScalpV2WorkerStageWeeklyMetrics {
  const weekStartTs = Math.floor(Number(params.weekStartTs) || 0);
  const weekToTs = Math.floor(Number(params.weekToTs) || 0);
  const sortedTrades = [...(params.trades || [])].sort(
    (a, b) =>
      Math.floor(Number(a.exitTs || 0)) - Math.floor(Number(b.exitTs || 0)),
  );
  let trades = 0;
  let wins = 0;
  let netR = 0;
  let grossProfitR = 0;
  let grossLossR = 0;
  let equity = 0;
  let peak = 0;
  let minPrefix = 0;
  let maxDrawdownR = 0;
  let largestTradeR = 0;
  let exitStop = 0;
  let exitTp = 0;
  let exitTimeStop = 0;
  let exitForceClose = 0;

  for (const trade of sortedTrades) {
    const ts = Math.floor(Number(trade.exitTs || 0));
    if (!Number.isFinite(ts) || ts < weekStartTs || ts >= weekToTs) continue;
    const r = Number(trade.rMultiple || 0);
    if (!Number.isFinite(r)) continue;
    trades += 1;
    if (r > 0) wins += 1;
    netR += r;
    if (r > 0) grossProfitR += r;
    if (r < 0) grossLossR += r;
    equity += r;
    if (equity > peak) peak = equity;
    if (equity < minPrefix) minPrefix = equity;
    const drawdown = Math.max(0, peak - equity);
    if (drawdown > maxDrawdownR) maxDrawdownR = drawdown;
    const absR = Math.abs(r);
    if (absR > largestTradeR) largestTradeR = absR;
    if (
      trade.exitReason === "STOP" ||
      trade.exitReason === "STOP_LOSS" ||
      trade.exitReason === "STOP_BE" ||
      trade.exitReason === "STOP_TRAIL"
    ) {
      exitStop += 1;
    } else if (trade.exitReason === "TP") {
      exitTp += 1;
    } else if (trade.exitReason === "TIME_STOP") {
      exitTimeStop += 1;
    } else if (trade.exitReason === "FORCE_CLOSE") {
      exitForceClose += 1;
    }
  }

  return {
    trades,
    wins,
    netR,
    grossProfitR,
    grossLossR,
    maxDrawdownR,
    maxPrefixR: peak,
    minPrefixR: minPrefix,
    largestTradeR,
    exitStop,
    exitTp,
    exitTimeStop,
    exitForceClose,
  };
}

function buildWorkerStageWeeklyMetricsMap(params: {
  trades: ScalpReplayTrade[];
  fromTs: number;
  toTs: number;
}): Map<number, ScalpV2WorkerStageWeeklyMetrics> {
  const out = new Map<number, ScalpV2WorkerStageWeeklyMetrics>();
  const weekStarts = listWeekStarts({
    fromTs: params.fromTs,
    toTs: params.toTs,
  });
  for (const weekStart of weekStarts) {
    out.set(
      weekStart,
      buildWorkerStageWeeklyMetrics({
        trades: params.trades,
        weekStartTs: weekStart,
        weekToTs: weekStart + ONE_WEEK_MS,
      }),
    );
  }
  return out;
}



function aggregateStageFromWeeklyMetrics(params: {
  fromTs: number;
  toTs: number;
  weeklyByStart: Map<number, ScalpV2WorkerStageWeeklyMetrics>;
}): {
  trades: number;
  netR: number;
  expectancyR: number;
  winRatePct: number;
  maxDrawdownR: number;
  profitFactor: number | null;
  winningWeeks: number;
  consecutiveWinningWeeks: number;
  weeklyNetR: Record<string, number>;
  maxWeeklyNetR: number | null;
  largestTradeR: number | null;
  exitReasons: { stop: number; tp: number; timeStop: number; forceClose: number };
} {
  const weekStarts = listWeekStarts({ fromTs: params.fromTs, toTs: params.toTs });
  let trades = 0;
  let wins = 0;
  let netR = 0;
  let grossProfitR = 0;
  let grossLossR = 0;
  let maxDrawdownR = 0;
  let equity = 0;
  let peak = 0;
  let winningWeeks = 0;
  let consecutiveWinningWeeks = 0;
  let streak = 0;
  let maxWeeklyNetR: number | null = null;
  let largestTradeR = 0;
  const weeklyNetR: Record<string, number> = {};
  const exitReasons = { stop: 0, tp: 0, timeStop: 0, forceClose: 0 };

  for (const weekStart of weekStarts) {
    const metrics = params.weeklyByStart.get(weekStart) || {
      trades: 0,
      wins: 0,
      netR: 0,
      grossProfitR: 0,
      grossLossR: 0,
      maxDrawdownR: 0,
      maxPrefixR: 0,
      minPrefixR: 0,
      largestTradeR: 0,
      exitStop: 0,
      exitTp: 0,
      exitTimeStop: 0,
      exitForceClose: 0,
    };
    const crossWeekDrawdown = Math.max(0, peak - (equity + metrics.minPrefixR));
    maxDrawdownR = Math.max(
      maxDrawdownR,
      metrics.maxDrawdownR,
      crossWeekDrawdown,
    );
    peak = Math.max(peak, equity + metrics.maxPrefixR);
    equity += metrics.netR;
    trades += metrics.trades;
    wins += metrics.wins;
    netR += metrics.netR;
    grossProfitR += metrics.grossProfitR;
    grossLossR += metrics.grossLossR;
    largestTradeR = Math.max(largestTradeR, metrics.largestTradeR);
    exitReasons.stop += metrics.exitStop;
    exitReasons.tp += metrics.exitTp;
    exitReasons.timeStop += metrics.exitTimeStop;
    exitReasons.forceClose += metrics.exitForceClose;
    weeklyNetR[String(weekStart)] = metrics.netR;
    if (maxWeeklyNetR === null || metrics.netR > maxWeeklyNetR) {
      maxWeeklyNetR = metrics.netR;
    }
    if (metrics.netR > 0) {
      winningWeeks += 1;
      streak += 1;
      if (streak > consecutiveWinningWeeks) {
        consecutiveWinningWeeks = streak;
      }
    } else {
      streak = 0;
    }
  }

  const expectancyR = trades > 0 ? netR / trades : 0;
  const winRatePct = trades > 0 ? (wins / trades) * 100 : 0;
  let profitFactor: number | null = null;
  if (grossProfitR > 0 && Math.abs(grossLossR) <= 1e-9) {
    profitFactor = Number.POSITIVE_INFINITY;
  } else if (Math.abs(grossLossR) > 1e-9) {
    profitFactor = grossProfitR / Math.abs(grossLossR);
  } else {
    profitFactor = 0;
  }

  return {
    trades,
    netR,
    expectancyR,
    winRatePct,
    maxDrawdownR,
    profitFactor,
    winningWeeks,
    consecutiveWinningWeeks,
    weeklyNetR,
    maxWeeklyNetR,
    largestTradeR: trades > 0 ? largestTradeR : null,
    exitReasons,
  };
}

function hasReusableNonZeroWeeklyCacheMetrics(
  metrics: ScalpV2WorkerStageWeeklyMetrics | null | undefined,
): boolean {
  if (!metrics) return false;
  const netR = Number(metrics.netR);
  return Number.isFinite(netR) && Math.abs(netR) > 1e-9;
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
  /** Minimum 4-week rolling netR from the backtest's stage C weeklyNetR. */
  backtestMin4wNetR: number | null;
  /** Full worker stage data extracted from candidate metadata for deployment storage. */
  workerStages: Record<string, unknown> | null;
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

  // Extract min 4-week rolling netR from backtest weeklyNetR
  const weeklyNetR = asRecord(stageC.weeklyNetR);
  const weekKeys = Object.keys(weeklyNetR)
    .map((k) => Number(k))
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b);
  let backtestMin4wNetR: number | null = null;
  if (weekKeys.length >= 4) {
    for (let i = 0; i <= weekKeys.length - 4; i += 1) {
      let sum = 0;
      for (let j = 0; j < 4; j += 1) {
        sum += Number(weeklyNetR[String(weekKeys[i + j])] || 0);
      }
      if (backtestMin4wNetR === null || sum < backtestMin4wNetR) {
        backtestMin4wNetR = sum;
      }
    }
  }

  // Build worker stages snapshot for deployment storage
  const workerStages = hasWorkerState
    ? {
        stageA: asRecord(worker.stageA),
        stageB: asRecord(worker.stageB),
        stageC,
        finalPass: stageCPass,
        evaluatedAtMs: worker.evaluatedAtMs,
      }
    : null;

  if (stageCPass) {
    return {
      stageCPass: true,
      hasWorkerState,
      reason: null,
      backtestMin4wNetR,
      workerStages,
    };
  }
  return {
    stageCPass: false,
    hasWorkerState,
    reason: hasWorkerState ? "worker_stage_c_failed" : "worker_stage_c_missing",
    backtestMin4wNetR,
    workerStages,
  };
}

function resolveWorkerStageCFreshness(params: {
  workerStages: Record<string, unknown> | null;
  requiredWeeks: number;
  nowTs: number;
}): {
  freshness: PromotionFreshness;
  ready: boolean;
  reason:
    | "worker_stage_c_missing"
    | "worker_stage_c_weekly_netr_missing"
    | "worker_stage_c_freshness_incomplete"
    | "worker_stage_c_not_latest_week"
    | null;
} {
  const emptyFreshness = buildFreshness({
    weeklyByWeekStart: new Map<number, { trades: number; netR: number }>(),
    requiredWeeks: params.requiredWeeks,
    nowTs: params.nowTs,
  });
  const workerStages = asRecord(params.workerStages);
  const stageC = asRecord(workerStages.stageC);
  if (!Object.keys(stageC).length) {
    return {
      freshness: emptyFreshness,
      ready: false,
      reason: "worker_stage_c_missing",
    };
  }
  const weeklyNetR = asRecord(stageC.weeklyNetR);
  const weekStarts = Array.from(
    new Set(
      Object.keys(weeklyNetR)
        .map((key) => Number(key))
        .filter((weekStart) => Number.isFinite(weekStart)),
    ),
  ).sort((a, b) => a - b);
  if (!weekStarts.length) {
    return {
      freshness: emptyFreshness,
      ready: false,
      reason: "worker_stage_c_weekly_netr_missing",
    };
  }
  const weeklyByWeekStart = new Map<number, { trades: number; netR: number }>();
  for (const weekStart of weekStarts) {
    const netR = Number(weeklyNetR[String(weekStart)] || 0);
    weeklyByWeekStart.set(weekStart, {
      trades: 0,
      netR: Number.isFinite(netR) ? netR : 0,
    });
  }
  const freshness = buildFreshness({
    weeklyByWeekStart,
    requiredWeeks: params.requiredWeeks,
    nowTs: params.nowTs,
  });
  if (!freshness.ready) {
    return {
      freshness,
      ready: false,
      reason: "worker_stage_c_freshness_incomplete",
    };
  }
  const stageCToTs = Math.floor(Number(stageC.toTs) || 0);
  if (stageCToTs > 0 && stageCToTs !== freshness.windowToTs) {
    return {
      freshness: { ...freshness, ready: false },
      ready: false,
      reason: "worker_stage_c_not_latest_week",
    };
  }
  return {
    freshness,
    ready: true,
    reason: null,
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

type ScalpV2RuntimeLoadScope = {
  venue: ScalpV2Venue;
  symbol: string;
};

type ScalpV2ResearchFreshnessPolicy = {
  enabled: boolean;
  retries: number;
  loadBatchSize: number;
  loadMaxAttempts: number;
  maxHopsPerRetry: number;
  maxLagBitgetMs: number;
  maxLagCapitalMs: number;
  minWindowCandlesBitget: number;
  minWindowCandlesCapital: number;
  stalePreviewLimit: number;
};

type ScalpV2ResearchFreshnessRow = {
  venue: ScalpV2Venue;
  symbol: string;
  ready: boolean;
  latestCandleTsMs: number | null;
  lagMs: number | null;
  candlesInWindow: number;
  reason: string | null;
};

type ScalpV2ResearchFreshnessAttempt = {
  retryIndex: number;
  staleBefore: number;
  staleAfter: number;
  loadRuns: number;
  loadFailedRuns: number;
  loadFailedScopes: number;
  loadedScopes: number;
};

type ScalpV2ResearchFreshnessGateResult = {
  applied: boolean;
  ready: boolean;
  reason: string | null;
  scopeCount: number;
  staleCount: number;
  retriesConfigured: number;
  retriesUsed: number;
  attempts: ScalpV2ResearchFreshnessAttempt[];
  stalePreview: Array<{
    venue: ScalpV2Venue;
    symbol: string;
    reason: string | null;
    latestCandleTsMs: number | null;
    lagHours: number | null;
    candlesInWindow: number;
  }>;
  policy: {
    retries: number;
    loadBatchSize: number;
    loadMaxAttempts: number;
    maxHopsPerRetry: number;
    maxLagBitgetHours: number;
    maxLagCapitalHours: number;
    minWindowCandlesBitget: number;
    minWindowCandlesCapital: number;
  };
};

function normalizeRuntimeScopeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function collectRuntimeLoadScopes(
  runtime: ScalpV2RuntimeConfig,
): ScalpV2RuntimeLoadScope[] {
  const out = new Map<string, ScalpV2RuntimeLoadScope>();
  for (const venue of runtime.supportedVenues) {
    const seedSymbols = runtime.seedSymbolsByVenue[venue] || [];
    const liveSymbols = runtime.seedLiveSymbolsByVenue[venue] || [];
    for (const rawSymbol of [...seedSymbols, ...liveSymbols]) {
      const symbol = normalizeRuntimeScopeSymbol(rawSymbol);
      if (!symbol) continue;
      if (!isScalpV2DiscoverSymbolAllowed(venue, symbol)) continue;
      if (!isScalpV2RuntimeSymbolInScope({ runtime, venue, symbol })) continue;
      out.set(`${venue}:${symbol}`, { venue, symbol });
    }
  }
  return Array.from(out.values());
}

function resolveResearchFreshnessPolicy(): ScalpV2ResearchFreshnessPolicy {
  const bitgetLagHours = Math.max(
    1,
    Math.min(
      24 * 7,
      toPositiveInt(
        process.env.SCALP_V2_RESEARCH_FRESHNESS_MAX_LAG_HOURS_BITGET,
        36,
        24 * 7,
      ),
    ),
  );
  const capitalLagHours = Math.max(
    1,
    Math.min(
      24 * 7,
      toPositiveInt(
        process.env.SCALP_V2_RESEARCH_FRESHNESS_MAX_LAG_HOURS_CAPITAL,
        84,
        24 * 7,
      ),
    ),
  );
  return {
    enabled: envBool("SCALP_V2_RESEARCH_FRESHNESS_GATE_ENABLED", true),
    retries: toNonNegativeInt(
      process.env.SCALP_V2_RESEARCH_FRESHNESS_RETRIES,
      2,
      12,
    ),
    loadBatchSize: Math.max(
      1,
      Math.min(
        120,
        toPositiveInt(
          process.env.SCALP_V2_RESEARCH_FRESHNESS_LOAD_BATCH_SIZE,
          4,
          120,
        ),
      ),
    ),
    loadMaxAttempts: Math.max(
      1,
      Math.min(
        20,
        toPositiveInt(
          process.env.SCALP_V2_RESEARCH_FRESHNESS_LOAD_MAX_ATTEMPTS,
          5,
          20,
        ),
      ),
    ),
    maxHopsPerRetry: Math.max(
      1,
      Math.min(
        120,
        toPositiveInt(
          process.env.SCALP_V2_RESEARCH_FRESHNESS_MAX_HOPS_PER_RETRY,
          24,
          120,
        ),
      ),
    ),
    maxLagBitgetMs: bitgetLagHours * ONE_HOUR_MS,
    maxLagCapitalMs: capitalLagHours * ONE_HOUR_MS,
    minWindowCandlesBitget: Math.max(
      0,
      Math.min(
        20_000,
        toNonNegativeInt(
          process.env.SCALP_V2_RESEARCH_FRESHNESS_MIN_WEEK_CANDLES_BITGET,
          1_200,
          20_000,
        ),
      ),
    ),
    minWindowCandlesCapital: Math.max(
      0,
      Math.min(
        20_000,
        toNonNegativeInt(
          process.env.SCALP_V2_RESEARCH_FRESHNESS_MIN_WEEK_CANDLES_CAPITAL,
          600,
          20_000,
        ),
      ),
    ),
    stalePreviewLimit: Math.max(
      1,
      Math.min(
        100,
        toPositiveInt(
          process.env.SCALP_V2_RESEARCH_FRESHNESS_STALE_PREVIEW_LIMIT,
          20,
          100,
        ),
      ),
    ),
  };
}

async function evaluateResearchFreshnessRow(params: {
  scope: ScalpV2RuntimeLoadScope;
  windowToTs: number;
  policy: ScalpV2ResearchFreshnessPolicy;
}): Promise<ScalpV2ResearchFreshnessRow> {
  const fromTs = params.windowToTs - ONE_WEEK_MS;
  const minWindowCandles =
    params.scope.venue === "capital"
      ? params.policy.minWindowCandlesCapital
      : params.policy.minWindowCandlesBitget;
  const maxLagMs =
    params.scope.venue === "capital"
      ? params.policy.maxLagCapitalMs
      : params.policy.maxLagBitgetMs;

  const history = await loadScalpCandleHistoryInRange(
    params.scope.symbol,
    "1m",
    fromTs,
    params.windowToTs,
  ).catch(() => null);

  const candles = Array.isArray(history?.record?.candles)
    ? history.record.candles
    : [];
  let latestCandleTsMs: number | null = null;
  let candlesInWindow = 0;

  for (const candle of candles) {
    const ts = Math.floor(Number(candle?.[0] || 0));
    if (!Number.isFinite(ts)) continue;
    if (ts < fromTs || ts >= params.windowToTs) continue;
    candlesInWindow += 1;
    if (latestCandleTsMs === null || ts > latestCandleTsMs) {
      latestCandleTsMs = ts;
    }
  }

  if (latestCandleTsMs === null) {
    return {
      venue: params.scope.venue,
      symbol: params.scope.symbol,
      ready: false,
      latestCandleTsMs: null,
      lagMs: null,
      candlesInWindow,
      reason: "missing_recent_candles",
    };
  }

  const lagMs = Math.max(0, params.windowToTs - latestCandleTsMs);
  if (lagMs > maxLagMs) {
    return {
      venue: params.scope.venue,
      symbol: params.scope.symbol,
      ready: false,
      latestCandleTsMs,
      lagMs,
      candlesInWindow,
      reason: "latest_candle_too_old",
    };
  }
  if (candlesInWindow < minWindowCandles) {
    return {
      venue: params.scope.venue,
      symbol: params.scope.symbol,
      ready: false,
      latestCandleTsMs,
      lagMs,
      candlesInWindow,
      reason: "insufficient_window_candles",
    };
  }
  return {
    venue: params.scope.venue,
    symbol: params.scope.symbol,
    ready: true,
    latestCandleTsMs,
    lagMs,
    candlesInWindow,
    reason: null,
  };
}

async function evaluateResearchFreshness(params: {
  scopes: ScalpV2RuntimeLoadScope[];
  windowToTs: number;
  policy: ScalpV2ResearchFreshnessPolicy;
}): Promise<ScalpV2ResearchFreshnessRow[]> {
  const rows: ScalpV2ResearchFreshnessRow[] = [];
  for (const scope of params.scopes) {
    rows.push(
      await evaluateResearchFreshnessRow({
        scope,
        windowToTs: params.windowToTs,
        policy: params.policy,
      }),
    );
  }
  return rows;
}

async function refreshResearchFreshnessScopes(params: {
  scopes: ScalpV2RuntimeLoadScope[];
  policy: ScalpV2ResearchFreshnessPolicy;
}): Promise<{
  runs: number;
  failedRuns: number;
  failedScopes: number;
  loadedScopes: number;
}> {
  if (!params.scopes.length) {
    return { runs: 0, failedRuns: 0, failedScopes: 0, loadedScopes: 0 };
  }

  let runs = 0;
  let failedRuns = 0;
  let failedScopes = 0;
  let loadedScopes = 0;
  let offset = 0;

  while (offset < params.scopes.length && runs < params.policy.maxHopsPerRetry) {
    const result = await runScalpV2LoadCandlesPipelineJob({
      scopes: params.scopes,
      batchSize: params.policy.loadBatchSize,
      maxAttempts: params.policy.loadMaxAttempts,
      offset,
    });
    runs += 1;
    loadedScopes += result.processed;
    failedScopes += result.failed;
    if (!result.ok) failedRuns += 1;
    const details = asRecord(result.details);
    const nextOffsetRaw = Math.floor(Number(details.nextOffset));
    const fallbackNextOffset = offset + result.processed;
    const nextOffset =
      Number.isFinite(nextOffsetRaw) && nextOffsetRaw > offset
        ? nextOffsetRaw
        : fallbackNextOffset;
    if (result.pendingAfter <= 0 || nextOffset <= offset) break;
    offset = nextOffset;
  }

  return { runs, failedRuns, failedScopes, loadedScopes };
}

async function runScalpV2SundayResearchFreshnessGate(params: {
  runtime: ScalpV2RuntimeConfig;
  nowTs: number;
  windowToTs: number;
}): Promise<ScalpV2ResearchFreshnessGateResult> {
  const policy = resolveResearchFreshnessPolicy();
  const policySummary = {
    retries: policy.retries,
    loadBatchSize: policy.loadBatchSize,
    loadMaxAttempts: policy.loadMaxAttempts,
    maxHopsPerRetry: policy.maxHopsPerRetry,
    maxLagBitgetHours: Math.floor(policy.maxLagBitgetMs / ONE_HOUR_MS),
    maxLagCapitalHours: Math.floor(policy.maxLagCapitalMs / ONE_HOUR_MS),
    minWindowCandlesBitget: policy.minWindowCandlesBitget,
    minWindowCandlesCapital: policy.minWindowCandlesCapital,
  };
  if (!policy.enabled || !isScalpV2SundayUtc(params.nowTs)) {
    return {
      applied: false,
      ready: true,
      reason: null,
      scopeCount: 0,
      staleCount: 0,
      retriesConfigured: policy.retries,
      retriesUsed: 0,
      attempts: [],
      stalePreview: [],
      policy: policySummary,
    };
  }

  const scopes = collectRuntimeLoadScopes(params.runtime);
  if (!scopes.length) {
    return {
      applied: true,
      ready: true,
      reason: null,
      scopeCount: 0,
      staleCount: 0,
      retriesConfigured: policy.retries,
      retriesUsed: 0,
      attempts: [],
      stalePreview: [],
      policy: policySummary,
    };
  }

  let rows = await evaluateResearchFreshness({
    scopes,
    windowToTs: params.windowToTs,
    policy,
  });
  let stale = rows.filter((row) => !row.ready);
  const attempts: ScalpV2ResearchFreshnessAttempt[] = [];
  let retriesUsed = 0;

  for (
    let retryIndex = 1;
    stale.length > 0 && retryIndex <= policy.retries;
    retryIndex += 1
  ) {
    const staleBefore = stale.length;
    retriesUsed = retryIndex;
    const refresh = await refreshResearchFreshnessScopes({
      scopes: stale.map((row) => ({ venue: row.venue, symbol: row.symbol })),
      policy,
    });
    rows = await evaluateResearchFreshness({
      scopes,
      windowToTs: params.windowToTs,
      policy,
    });
    stale = rows.filter((row) => !row.ready);
    attempts.push({
      retryIndex,
      staleBefore,
      staleAfter: stale.length,
      loadRuns: refresh.runs,
      loadFailedRuns: refresh.failedRuns,
      loadFailedScopes: refresh.failedScopes,
      loadedScopes: refresh.loadedScopes,
    });
    if (!stale.length) break;
  }

  const stalePreview = stale.slice(0, policy.stalePreviewLimit).map((row) => ({
    venue: row.venue,
    symbol: row.symbol,
    reason: row.reason,
    latestCandleTsMs: row.latestCandleTsMs,
    lagHours:
      row.lagMs === null
        ? null
        : Number((row.lagMs / ONE_HOUR_MS).toFixed(2)),
    candlesInWindow: row.candlesInWindow,
  }));
  const ready = stale.length <= 0;

  return {
    applied: true,
    ready,
    reason: ready ? null : "research_candle_freshness_gate_failed",
    scopeCount: scopes.length,
    staleCount: stale.length,
    retriesConfigured: policy.retries,
    retriesUsed,
    attempts,
    stalePreview,
    policy: policySummary,
  };
}

/**
 * Discover is now a thin delegate to the evaluate job. The old discover job
 * generated the same composer grid as evaluate but with a weaker scoring model
 * and an env-gated persistence toggle (SCALP_V2_DISCOVER_PERSIST_CANDIDATES)
 * that defaulted to false — making it a no-op in production. Evaluate already
 * does cursor-based windowing, better scoring, and always persists.
 *
 * Kept as a named export so the /api/scalp/v2/cron/discover endpoint and
 * runScalpV2FullAutoCycle continue to work without changes.
 */
export async function runScalpV2DiscoverJob(): Promise<ScalpV2JobResult> {
  const result = await runScalpV2ResearchJob();
  return {
    ...result,
    jobKind: "discover",
    details: { ...result.details, delegatedTo: "research" },
  };
}

/**
 * Unified research job: generates ALL candidates from the deterministic
 * composer grid across all scopes, skips any already backtested this week
 * (via DB cache), then backtests the rest through stages A/B/C in one pass.
 *
 * Only persists candidates that pass the configured minimum stage gate
 * (default: stage A). No cursor or batch-size throttling — the cache
 * ensures each candidate is only backtested once per week window.
 */
export async function runScalpV2ResearchJob(params: {
  batchSize?: number;
} = {}): Promise<ScalpV2JobResult> {
  const owner = lockOwner("research");
  const claimed = await claimScalpV2Job({ jobKind: "research", lockOwner: owner });
  if (!claimed) {
    return buildScalpV2JobResult({
      jobKind: "research",
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
  const heartbeatMinIntervalMs = Math.max(
    5_000,
    Math.min(
      120_000,
      toPositiveInt(
        process.env.SCALP_V2_RESEARCH_HEARTBEAT_MIN_INTERVAL_MS,
        15_000,
        120_000,
      ),
    ),
  );
  let lastHeartbeatAtMs = 0;
  const jobStartMs = nowMs();
  const researchLog: Array<{ t: number; p: string; d?: string }> = [];
  const MAX_LOG_ENTRIES = 60;

  function logResearch(phase: string, detail?: string): void {
    const elapsed = Math.round((nowMs() - jobStartMs) / 1000);
    researchLog.push({ t: elapsed, p: phase, d: detail });
    if (researchLog.length > MAX_LOG_ENTRIES) {
      researchLog.splice(0, researchLog.length - MAX_LOG_ENTRIES);
    }
  }

  async function emitResearchHeartbeat(params: {
    phase: string;
    force?: boolean;
    progress?: Record<string, unknown>;
    extra?: Record<string, unknown>;
  }): Promise<void> {
    const now = nowMs();
    if (!params.force && now - lastHeartbeatAtMs < heartbeatMinIntervalMs) {
      return;
    }
    lastHeartbeatAtMs = now;
    logResearch(params.phase, params.progress?.step as string || undefined);
    await heartbeatScalpV2Job({
      jobKind: "research",
      lockOwner: owner,
      details: {
        phase: params.phase,
        heartbeatAtMs: now,
        progress: {
          processedSoFar: processed,
          succeededSoFar: succeeded,
          failedSoFar: failed,
          ...(params.progress || {}),
        },
        log: researchLog,
        ...(params.extra || {}),
      },
    }).catch(() => undefined);
  }

  await emitResearchHeartbeat({
    phase: "claimed",
    force: true,
    progress: {
      requestedBatchSize: Number.isFinite(Number(params.batchSize))
        ? Math.floor(Number(params.batchSize))
        : null,
    },
  });

  try {
    let runtime = await loadScalpV2RuntimeConfig();
    await emitResearchHeartbeat({
      phase: "runtime_loaded",
      force: true,
      progress: {
        runtimeEnabled: runtime.enabled,
      },
    });
    if (!runtime.enabled) {
      details = { skipped: true, reason: "SCALP_V2_DISABLED" };
      return buildScalpV2JobResult({
        jobKind: "research",
        processed,
        succeeded,
        failed,
        pendingAfter: 0,
        details,
      });
    }

    const workerPolicy = resolveWorkerPolicy();
    const nowTs = nowMs();
    const windowToTs = resolveScalpV2CompletedWeekWindowToUtc(nowTs);
    const freshnessGate = await runScalpV2SundayResearchFreshnessGate({
      runtime,
      nowTs,
      windowToTs,
    });
    await emitResearchHeartbeat({
      phase: "freshness_gate",
      force: true,
      progress: {
        freshnessReady: freshnessGate.ready,
        freshnessStaleCount: freshnessGate.staleCount,
        freshnessRetriesUsed: freshnessGate.retriesUsed,
      },
    });
    if (!freshnessGate.ready) {
      ok = false;
      failed = Math.max(1, freshnessGate.staleCount);
      details = {
        skipped: true,
        reason:
          freshnessGate.reason || "research_candle_freshness_gate_failed",
        freshnessGate,
      };
      return buildScalpV2JobResult({
        jobKind: "research",
        processed,
        succeeded,
        failed,
        pendingAfter: freshnessGate.staleCount,
        details,
      });
    }
    const scopePrune = await runScalpV2ScopePrunePass({
      runtime,
      windowToTs,
      nowTs,
    });
    await emitResearchHeartbeat({
      phase: "scope_prune",
      force: true,
      progress: {
        scopePruneSkipped: Boolean(asRecord(scopePrune.details || {}).skipped),
      },
    });
    runtime = scopePrune.runtime;
    const activePrunedScopes = normalizeActivePrunedScopes(runtime, nowTs);
    const jobStartMs = nowMs();
    const timeBudgetMs = Math.max(
      60_000,
      Math.min(
        780_000,
        toPositiveInt(
          process.env.SCALP_V2_RESEARCH_TIME_BUDGET_MS,
          650_000,
          780_000,
        ),
      ),
    );
    // Research runs on Sundays (after new week candles are loaded).
    // Only execution is blocked on Sunday UTC.

    const maxCandidatesPerSession = 840;
    const minPersistStageRaw = String(
      process.env.SCALP_V2_RESEARCH_MIN_PERSIST_STAGE || "a",
    ).trim().toLowerCase();
    const minPersistStage: ScalpV2WorkerStageId =
      minPersistStageRaw === "c" ? "c" : minPersistStageRaw === "b" ? "b" : "a";
    const configuredBatchSize = Math.max(
      1,
      Math.min(
        10_000,
        toPositiveInt(process.env.SCALP_V2_RESEARCH_BATCH_SIZE, 100, 10_000),
      ),
    );
    const effectiveBatchSize = Math.max(
      1,
      Math.min(
        10_000,
        toPositiveInt(params.batchSize, configuredBatchSize, 10_000),
      ),
    );

    // --- Build scopes ---
    const scopes: Array<{
      venue: ScalpV2Venue;
      symbol: string;
      session: ScalpV2Session;
    }> = [];
    let scopeSymbolsVisited = 0;
    let droppedByVenuePolicy = 0;
    let droppedByScopePrune = 0;
    for (const venue of runtime.supportedVenues) {
      const symbols = runtime.seedSymbolsByVenue[venue] || [];
      for (const symbolRaw of symbols) {
        scopeSymbolsVisited += 1;
        await emitResearchHeartbeat({
          phase: "build_scopes",
          progress: {
            scopeSymbolsVisited,
            scopeCountSoFar: scopes.length,
          },
        });
        const symbol = String(symbolRaw || "").trim().toUpperCase();
        if (!symbol) continue;
        if (!isScalpV2DiscoverSymbolAllowed(venue, symbol)) {
          droppedByVenuePolicy += 1;
          continue;
        }
        if (!isScalpV2RuntimeSymbolInScope({ runtime, venue, symbol })) continue;
        for (const session of runtime.supportedSessions) {
          const scopeKey = toScalpV2ScopeKey({ venue, symbol, session });
          if (activePrunedScopes[scopeKey]) {
            droppedByScopePrune += 1;
            continue;
          }
          scopes.push({ venue, symbol, session });
        }
      }
    }

    if (!scopes.length) {
      details = {
        reason: "no_runtime_seed_scopes",
        droppedByVenuePolicy,
        droppedByScopePrune,
        freshnessGate,
        scopePrune: scopePrune.details,
      };
      return buildScalpV2JobResult({
        jobKind: "research",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    // --- Warm-up: generate and persist candidates once per week ---
    // The scope hash fingerprints the current (scopes × maxCandidatesPerSession).
    // If it matches the DB, generation is skipped entirely — straight to backtest.
    const stagePolicies = [
      workerPolicy.stageA,
      workerPolicy.stageB,
      workerPolicy.stageC,
    ] as const;
    const minWindowFromTs = windowToTs - workerPolicy.stageC.weeks * ONE_WEEK_MS;
    const scopeHash = hashScoreSeed(
      scopes.map((s) => `${s.venue}:${s.symbol}:${s.session}`).sort().join("|") + `|mc=${maxCandidatesPerSession}`,
    ).toString();
    const warmUpState = await loadScalpV2WarmUpState({ windowToTs }).catch(() => null);
    const warmUpComplete = warmUpState !== null && warmUpState.scopeHash === scopeHash;

    type InMemoryCandidate = {
      venue: ScalpV2Venue;
      symbol: string;
      session: ScalpV2Session;
      strategyId: string;
      tuneId: string;
      candidateId: string;
      score: number;
      dsl: ReturnType<typeof buildScalpV2ModelGuidedComposerGrid>[number];
    };

    let allCandidates: InMemoryCandidate[] = [];
    let poolSizeTotal = 0;
    let deploymentVariantsGenerated = 0;

    if (!warmUpComplete) {
      // --- WARM-UP RUN: generate all candidates, persist, save fingerprint ---
      await emitResearchHeartbeat({ phase: "warm_up_generate", force: true });
      let generatedScopeCount = 0;

      for (const scope of scopes) {
        generatedScopeCount += 1;
        const composerCandidates = buildScalpV2ModelGuidedComposerGrid({
          venue: scope.venue,
          symbol: scope.symbol,
          entrySessionProfile: scope.session,
          maxCandidates: maxCandidatesPerSession,
        });
        poolSizeTotal += composerCandidates.length;

        for (const dsl of composerCandidates) {
          const model = dsl.model;
          const supportScore = Number.isFinite(Number(dsl.supportScore))
            ? Number(dsl.supportScore)
            : 0;
          const supportNorm = Math.max(0, Math.min(12, supportScore)) / 12;
          const score =
            12 +
            model.compositeScore * 66 +
            model.confidence * 14 +
            supportNorm * 8 +
            hashScoreSeed(
              `${scope.venue}:${scope.symbol}:${scope.session}:${dsl.tuneId}`,
            ) / 1000;

          allCandidates.push({
            venue: scope.venue,
            symbol: scope.symbol,
            session: scope.session,
            strategyId: MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
            tuneId: dsl.tuneId,
            candidateId: dsl.candidateId,
            score,
            dsl,
          });
        }
        if (generatedScopeCount % 10 === 0 || generatedScopeCount === scopes.length) {
          await emitResearchHeartbeat({
            phase: "warm_up_generate",
            progress: {
              generatedScopeCount,
              totalScopes: scopes.length,
              generatedCandidates: allCandidates.length,
            },
          });
        }
      }

      // Deployment variants
      try {
        const enabledDeployments = await listScalpV2Deployments({ enabledOnly: true, limit: 500 });
        const existingTuneIds = new Set(allCandidates.map((c) => c.tuneId));
        for (const dep of enabledDeployments) {
          if (!isScalpV2RuntimeSymbolInScope({ runtime, venue: dep.venue, symbol: dep.symbol })) continue;
          const session = dep.entrySessionProfile;
          const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(dep.tuneId);
          const depDsl = asRecord(asRecord(dep.promotionGate).dsl || {});
          const toStrArr = (v: unknown): string[] => Array.isArray(v) ? v as string[] : [];
          const baseBlocksByFamily = {
            pattern: toStrArr(depDsl.pattern),
            session_filter: toStrArr(depDsl.session_filter),
            state_machine: toStrArr(depDsl.state_machine),
            entry_trigger: toStrArr(depDsl.entry_trigger),
            exit_rule: toStrArr(depDsl.exit_rule),
            risk_rule: toStrArr(depDsl.risk_rule),
          };
          const baseExitId = parseExitRuleFromTuneId(dep.tuneId)?.toString() || undefined;
          const baseRegimeGateId = parseRegimeGateFromTuneId(dep.tuneId) || undefined;
          const baseModel = {
            family: "interpretable_pattern_blend" as const,
            version: "deployment_variant_v1",
            interpretableScore: 0,
            treeScore: 0,
            sequenceScore: 0,
            compositeScore: 0.5,
            confidence: 0.5,
          };
          const compatEntries = ENTRY_TRIGGER_COMPAT[plan.baseArm] || [];
          const riskProfiles = RISK_RULE_RESEARCH_PROFILES;
          const smProfiles = STATE_MACHINE_RESEARCH_PROFILES;
          for (const entryId of compatEntries) {
            for (const riskId of riskProfiles) {
              for (const smId of [null, ...smProfiles]) {
                const variantTuneId = buildModelGuidedComposerTuneId({
                  armId: plan.armId,
                  digest: `${dep.deploymentId}:${entryId}:${riskId || "d"}:${smId || "d"}`,
                  exitRuleId: baseExitId,
                  entryTriggerId: entryId,
                  riskRuleId: riskId,
                  stateMachineId: smId,
                  regimeGateId: baseRegimeGateId,
                });
                if (existingTuneIds.has(variantTuneId)) continue;
                existingTuneIds.add(variantTuneId);
                const variantScore = 90 + hashScoreSeed(variantTuneId) / 1000;
                allCandidates.push({
                  venue: dep.venue as ScalpV2Venue,
                  symbol: dep.symbol,
                  session,
                  strategyId: dep.strategyId,
                  tuneId: variantTuneId,
                  candidateId: variantTuneId,
                  score: variantScore,
                  dsl: {
                    candidateId: variantTuneId,
                    tuneId: variantTuneId,
                    venue: dep.venue as ScalpV2Venue,
                    symbol: dep.symbol,
                    entrySessionProfile: session,
                    blocksByFamily: {
                      ...baseBlocksByFamily,
                      entry_trigger: [entryId],
                      risk_rule: riskId ? [riskId] : baseBlocksByFamily.risk_rule,
                      state_machine: smId ? [smId] : baseBlocksByFamily.state_machine,
                    },
                    referenceStrategyIds: [],
                    supportScore: 0,
                    generatedAtMs: Date.now(),
                    model: baseModel,
                    regimeGateId: baseRegimeGateId,
                  },
                });
                deploymentVariantsGenerated += 1;
              }
            }
          }
        }
      } catch {
        // Non-fatal
      }

      if (!allCandidates.length) {
        details = { reason: "no_candidates_generated", scopeCount: scopes.length, droppedByScopePrune, poolSizeTotal, scopePrune: scopePrune.details };
        return buildScalpV2JobResult({ jobKind: "research", processed: 0, succeeded: 0, failed: 0, pendingAfter: 0, details });
      }

      // Persist all candidates as "discovered" (upsert — safe for partial reruns)
      await emitResearchHeartbeat({ phase: "warm_up_persist", force: true, progress: { total: allCandidates.length } });
      const rows = allCandidates.map((c) => ({
        venue: c.venue,
        symbol: c.symbol,
        strategyId: c.strategyId,
        tuneId: c.tuneId,
        entrySessionProfile: c.session,
        score: c.score,
        status: "discovered" as const,
        reasonCodes: ["SCALP_V2_WARM_UP"],
        metadata: {
          discoveredAtMs: nowTs,
          source: "model_guided_composer",
          researchCandidateId: c.candidateId,
          researchDsl: c.dsl.blocksByFamily,
          researchReferences: c.dsl.referenceStrategyIds,
          researchSupportScore: c.dsl.supportScore,
          researchRegimeGateId: c.dsl.regimeGateId || null,
          composerModel: c.dsl.model,
        },
      }));
      await upsertScalpV2Candidates({ rows });

      // Save fingerprint so subsequent runs skip generation entirely
      await upsertScalpV2WarmUpState({ windowToTs, scopeHash, candidateCount: allCandidates.length });

      details = {
        reason: "warm_up_complete",
        scopeCount: scopes.length,
        poolSizeTotal,
        persisted: allCandidates.length,
        deploymentVariantsGenerated,
        scopeHash,
        configuredBatchSize,
        effectiveBatchSize,
      };
      return buildScalpV2JobResult({
        jobKind: "research",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: allCandidates.length,
        details,
      });
    }

    // --- BACKTEST RUN: warm-up complete, skip generation entirely ---
    const deploymentRolloverRequeueEnabled = envBool(
      "SCALP_V2_RESEARCH_DEPLOYMENT_ROLLOVER_REQUEUE_ENABLED",
      true,
    );
    const deploymentRolloverPreviousWindowOnly = envBool(
      "SCALP_V2_RESEARCH_DEPLOYMENT_ROLLOVER_PREVIOUS_WINDOW_ONLY",
      true,
    );
    const deploymentRolloverIncludeDisabled = envBool(
      "SCALP_V2_RESEARCH_DEPLOYMENT_ROLLOVER_INCLUDE_DISABLED",
      true,
    );
    let deploymentRolloverRequeued = 0;
    if (deploymentRolloverRequeueEnabled) {
      await emitResearchHeartbeat({
        phase: "deployment_rollover_requeue",
        force: true,
      });
      deploymentRolloverRequeued = await requeueScalpV2DeploymentCandidatesForWindow(
        {
          windowToTs,
          previousWindowOnly: deploymentRolloverPreviousWindowOnly,
          includeDisabledDeployments: deploymentRolloverIncludeDisabled,
        },
      ).catch(() => 0);
      await emitResearchHeartbeat({
        phase: "deployment_rollover_requeue",
        force: true,
        progress: {
          deploymentRolloverRequeued,
          deploymentRolloverPreviousWindowOnly,
          deploymentRolloverIncludeDisabled,
        },
      });
    }

    // Find the next symbol(s) that still have "discovered" candidates, load only those.
    await emitResearchHeartbeat({ phase: "load_backtest_chunk", force: true });
    const maxSymbolsPerRun = Math.max(1, Math.min(50,
      toPositiveInt(process.env.SCALP_V2_RESEARCH_MAX_SYMBOLS_PER_RUN, 1, 50)));
    const discoveredSymbols = await listScalpV2DiscoveredSymbols().catch(() => [] as string[]);
    const symbolsThisRun = discoveredSymbols.slice(0, maxSymbolsPerRun);
    async function resolvePendingAfterBacklog(fallback: number): Promise<number> {
      const discoveredCount = await countScalpV2CandidatesByStatus({
        status: "discovered",
      }).catch(() => -1);
      if (Number.isFinite(Number(discoveredCount)) && Number(discoveredCount) >= 0) {
        return Math.max(0, Math.floor(Number(discoveredCount)));
      }
      return Math.max(0, Math.floor(Number(fallback) || 0));
    }

    if (symbolsThisRun.length > 0) {
      const chunkRows = await listScalpV2Candidates({
        status: "discovered",
        symbols: symbolsThisRun,
        limit: 5_000,
      }).catch(() => [] as Awaited<ReturnType<typeof listScalpV2Candidates>>);

      for (const row of chunkRows) {
        const meta = row.metadata || {};
        const dslRaw = (meta.researchDsl || {}) as Record<string, string[]>;
        allCandidates.push({
          venue: row.venue as ScalpV2Venue,
          symbol: row.symbol,
          session: row.entrySessionProfile as ScalpV2Session,
          strategyId: row.strategyId,
          tuneId: row.tuneId,
          candidateId: String(meta.researchCandidateId || row.tuneId),
          score: row.score,
          dsl: {
            candidateId: String(meta.researchCandidateId || row.tuneId),
            tuneId: row.tuneId,
            venue: row.venue as ScalpV2Venue,
            symbol: row.symbol,
            entrySessionProfile: row.entrySessionProfile as ScalpV2Session,
            blocksByFamily: {
              pattern: dslRaw.pattern || [],
              session_filter: dslRaw.session_filter || [],
              state_machine: dslRaw.state_machine || [],
              entry_trigger: dslRaw.entry_trigger || [],
              exit_rule: dslRaw.exit_rule || [],
              risk_rule: dslRaw.risk_rule || [],
            },
            referenceStrategyIds: (meta.researchReferences || []) as string[],
            supportScore: Number(meta.researchSupportScore) || 0,
            generatedAtMs: Number(meta.discoveredAtMs) || nowTs,
            model: (meta.composerModel || {}) as any,
            regimeGateId: (meta.researchRegimeGateId as string) || undefined,
          },
        });
      }
    }
    const evaluatedKeys = await loadScalpV2EvaluatedCandidateKeys({ windowToTs }).catch(() => new Set<string>());
    const skippedByCache = evaluatedKeys.size;
    const notYetEvaluated = allCandidates.filter((c) => {
      const key = `${c.venue}:${c.symbol}:${c.tuneId}:${c.session}`.toLowerCase();
      return !evaluatedKeys.has(key);
    });

    if (!notYetEvaluated.length) {
      const pendingAfter = await resolvePendingAfterBacklog(0);
      details = {
        reason: "all_candidates_already_evaluated_this_week",
        scopeCount: scopes.length,
        droppedByScopePrune,
        poolSizeTotal,
        deploymentRolloverRequeued,
        totalCandidates: allCandidates.length,
        skippedByCache,
        pendingAfter,
        scopePrune: scopePrune.details,
      };
      return buildScalpV2JobResult({
        jobKind: "research",
        processed: skippedByCache,
        succeeded: 0,
        failed: 0,
        pendingAfter,
        details,
      });
    }

    // --- Smart skip: use previous week's results to avoid re-running
    // candidates that clearly failed and won't flip with 1 new week. ---
    await emitResearchHeartbeat({
      phase: "load_previous_results",
      force: true,
      progress: {
        notYetEvaluated: notYetEvaluated.length,
      },
    });
    const previousResults = await loadScalpV2PreviousWeekResults({
      currentWindowToTs: windowToTs,
      symbols: Array.from(new Set(notYetEvaluated.map((row) => row.symbol))),
      tuneIds: Array.from(new Set(notYetEvaluated.map((row) => row.tuneId))),
    }).catch(
      () =>
        new Map<
          string,
          {
            stageAPassed: boolean;
            stageANetR: number | null;
            stageATrades: number | null;
            stageCPassed: boolean;
            stageCNetR: number | null;
            windowToTs: number;
            stageAWeeklyNetR: Record<string, number>;
          }
        >(),
    );

    // Thresholds for "clear fail" — well below stage A gates, won't flip.
    const clearFailNetR = workerPolicy.stageA.minNetR * -2; // e.g. -0.4 if gate is 0.2
    const clearFailMinTrades = Math.floor(workerPolicy.stageA.minTrades * 0.3); // e.g. 1 if gate is 4

    let skippedByClearFail = 0;
    let skippedByNetRPreFilter = 0;

    // Pre-compute stage A week starts for the weeklyNetR pre-filter.
    const stageAFromTs = windowToTs - workerPolicy.stageA.weeks * ONE_WEEK_MS;
    const stageAPriorWeekStarts = listWeekStarts({ fromTs: stageAFromTs, toTs: windowToTs }).slice(0, -1);

    const selected = notYetEvaluated.filter((c) => {
      const key = `${c.venue}:${c.symbol}:${c.tuneId}:${c.session}`.toLowerCase();
      const prev = previousResults.get(key);
      if (!prev) return true; // Never tested — must run

      // weeklyNetR pre-filter: sum the prior weeks' netR from the previous
      // stage A result (the weeks that overlap with the current window).
      // If the sum is already below minNetR, the newest week alone can't
      // realistically save it — skip the full replay.
      const prevWeeklyNetR = prev.stageAWeeklyNetR;
      if (prevWeeklyNetR && stageAPriorWeekStarts.length > 0) {
        let projectedNetR = 0;
        let hasAllPrior = true;
        for (const ws of stageAPriorWeekStarts) {
          const nr = prevWeeklyNetR[String(ws)];
          if (nr === undefined) { hasAllPrior = false; break; }
          projectedNetR += nr;
        }
        if (hasAllPrior && projectedNetR < workerPolicy.stageA.minNetR) {
          skippedByNetRPreFilter += 1;
          return false;
        }
      }

      // Previous stage C passed — must re-verify with new window
      if (prev.stageCPassed) return true;

      // Previous stage A passed but C failed — worth re-running
      // (the shifted window might help stage B/C)
      if (prev.stageAPassed) return true;

      // Stage A failed: check if it was a clear fail or marginal
      const netR = prev.stageANetR;
      const trades = prev.stageATrades;

      // Clear fail: deeply negative netR or barely any trades
      if (netR !== null && netR < clearFailNetR) {
        skippedByClearFail += 1;
        return false;
      }
      if (trades !== null && trades < clearFailMinTrades) {
        skippedByClearFail += 1;
        return false;
      }

      // Marginal fail — re-run, new week might tip it
      return true;
    });

    if (!selected.length) {
      const pendingAfter = await resolvePendingAfterBacklog(
        skippedByClearFail + skippedByNetRPreFilter,
      );
      details = {
        reason: "all_candidates_skipped",
        scopeCount: scopes.length,
        droppedByScopePrune,
        poolSizeTotal,
        deploymentRolloverRequeued,
        totalCandidates: allCandidates.length,
        skippedByCache,
        skippedByClearFail,
        skippedByNetRPreFilter,
        candidatesWithPreviousResults: previousResults.size,
        pendingAfter,
        scopePrune: scopePrune.details,
      };
      return buildScalpV2JobResult({
        jobKind: "research",
        processed: skippedByCache + skippedByClearFail + skippedByNetRPreFilter,
        succeeded: 0,
        failed: 0,
        pendingAfter,
        details,
      });
    }

    // --- Backtest remaining candidates in one pass ---
    await emitResearchHeartbeat({
      phase: "selection_complete",
      force: true,
      progress: {
        selected: selected.length,
        selectedThisRun: Math.min(selected.length, effectiveBatchSize),
        skippedByCache,
        skippedByClearFail,
        skippedByNetRPreFilter,
      },
    });
    const chunked = selected.slice(0, effectiveBatchSize);
    const deferredCount = Math.max(0, selected.length - chunked.length);

    // Base progress: include overall weekly totals so the UI shows global progress
    const weeklyTotal = warmUpState?.candidateCount || selected.length;
    const weeklyEvaluated = evaluatedKeys.size;
    const baseProgress = {
      selectedCandidates: chunked.length,
      selectedTotal: selected.length,
      weeklyTotal,
      weeklyEvaluated,
      skippedByCache,
      skippedByClearFail,
      skippedByNetRPreFilter,
    };
    await emitResearchHeartbeat({
      phase: "prepare_backtest",
      force: true,
      progress: { ...baseProgress },
    });
    const uniqueSymbols = Array.from(new Set(chunked.map((c) => c.symbol)));
    const symbolMetadataMap = await loadScalpSymbolMarketMetadataBulk(uniqueSymbols).catch(
      () => new Map<string, ScalpSymbolMarketMetadata | null>(),
    );
    const windowSliceCacheEnabled = envBool(
      "SCALP_V2_RESEARCH_WINDOW_SLICE_CACHE_ENABLED",
      true,
    );
    const candleCache = new Map<string, ScalpReplayCandle[]>();
    type SymbolWindowSliceCache = {
      candlesRef: ScalpReplayCandle[];
      windows: Map<string, ScalpReplayCandle[]>;
    };
    const symbolWindowSliceCache = new Map<string, SymbolWindowSliceCache>();
    function getCachedCandleWindow(params: {
      symbol: string;
      candles: ScalpReplayCandle[];
      fromTs: number;
      toTs: number;
    }): ScalpReplayCandle[] {
      if (!windowSliceCacheEnabled) {
        return params.candles.filter(
          (row) => row.ts >= params.fromTs && row.ts < params.toTs,
        );
      }
      const key = `${params.fromTs}:${params.toTs}`;
      const existing = symbolWindowSliceCache.get(params.symbol);
      if (existing && existing.candlesRef === params.candles) {
        const cached = existing.windows.get(key);
        if (cached) return cached;
        const computed = params.candles.filter(
          (row) => row.ts >= params.fromTs && row.ts < params.toTs,
        );
        existing.windows.set(key, computed);
        return computed;
      }
      const computed = params.candles.filter(
        (row) => row.ts >= params.fromTs && row.ts < params.toTs,
      );
      symbolWindowSliceCache.set(params.symbol, {
        candlesRef: params.candles,
        windows: new Map([[key, computed]]),
      });
      return computed;
    }
    let persistedCount = 0;
    let stageAPass = 0;
    let stageAFail = 0;
    let stageBPass = 0;
    let stageBFail = 0;
    let stageCPass = 0;
    let stageCFail = 0;
    let replayErrors = 0;
    let droppedBelowMinStage = 0;
    let incrementalStageReplays = 0;
    let fullStageReplays = 0;
    let cachedStageReuses = 0;

    let timeBudgetExhausted = false;

    // --- Load weekly cache for incremental replays ---
    const weeklyCacheKeys: Array<{
      venue: ScalpV2Venue;
      symbol: string;
      strategyId: string;
      tuneId: string;
      session: ScalpV2Session;
      stageId: ScalpV2WorkerStageId;
    }> = [];
    const seenCacheKeys = new Set<string>();
    for (const c of chunked) {
      for (const stage of stagePolicies) {
        const k = `${c.venue}:${c.symbol}:${c.strategyId}:${c.tuneId}:${c.session}:${stage.id}`.toLowerCase();
        if (seenCacheKeys.has(k)) continue;
        seenCacheKeys.add(k);
        weeklyCacheKeys.push({
          venue: c.venue,
          symbol: c.symbol,
          strategyId: c.strategyId,
          tuneId: c.tuneId,
          session: c.session,
          stageId: stage.id,
        });
      }
    }
    // Skip the heavy cache load if the table is empty (first run after deploy).
    // This avoids 56+ DB round-trips that return nothing.
    const workerStageWeeklyCache = weeklyCacheKeys.length > 0
      ? await loadScalpV2WeeklyCache({
          keys: weeklyCacheKeys,
          fromWeekStartTs: minWindowFromTs,
          toWeekStartTs: windowToTs,
        }).catch(() => new Map<string, Map<number, ScalpV2WorkerStageWeeklyMetrics>>())
      : new Map<string, Map<number, ScalpV2WorkerStageWeeklyMetrics>>();
    await emitResearchHeartbeat({ phase: "prepare_backtest", force: true, progress: { ...baseProgress, step: "cache_loaded", cacheKeys: weeklyCacheKeys.length, cacheHits: workerStageWeeklyCache.size } });

    // Pre-scan: determine which symbols need the full 12-week candle range
    // vs just the newest week. Skip entirely when cache is empty (all need full range).
    const newestWeekStart = startOfScalpV2WeekMondayUtc(windowToTs - ONE_WEEK_MS);
    const cacheIsPopulated = workerStageWeeklyCache.size > 0;
    const symbolsNeedingFullRange = new Set<string>();
    if (cacheIsPopulated) {
      for (const c of chunked) {
        if (symbolsNeedingFullRange.has(c.symbol)) continue;
        for (const stage of stagePolicies) {
          const ck = `${c.venue}:${c.symbol}:${c.strategyId}:${c.tuneId}:${c.session}:${stage.id}`.toLowerCase();
          const cached = workerStageWeeklyCache.get(ck);
          const fromTs = windowToTs - stage.weeks * ONE_WEEK_MS;
          const priorStarts = listWeekStarts({ fromTs, toTs: windowToTs }).slice(0, -1);
          for (const ws of priorStarts) {
            const metrics = cached?.get(ws);
            if (!hasReusableNonZeroWeeklyCacheMetrics(metrics)) {
              symbolsNeedingFullRange.add(c.symbol);
              break;
            }
          }
          if (symbolsNeedingFullRange.has(c.symbol)) break;
        }
      }
    }

    const pendingCacheWrites: Array<{
      venue: ScalpV2Venue;
      symbol: string;
      strategyId: string;
      tuneId: string;
      session: ScalpV2Session;
      stageId: ScalpV2WorkerStageId;
      weekStartTs: number;
      weekToTs: number;
      metrics: ScalpV2WorkerStageWeeklyMetrics;
    }> = [];
    type CandidateUpsertRow = Parameters<typeof upsertScalpV2Candidates>[0]["rows"][number];
    type PendingCandidateUpsert = {
      mode: "evaluated" | "rejected";
      row: CandidateUpsertRow;
    };
    const pendingCandidateUpserts: PendingCandidateUpsert[] = [];
    async function flushPendingCandidateUpserts(): Promise<void> {
      if (!pendingCandidateUpserts.length) return;
      const rows = pendingCandidateUpserts.splice(0, pendingCandidateUpserts.length);

      function applyPersistSuccess(entry: PendingCandidateUpsert) {
        persistedCount += 1;
        processed += 1;
        if (entry.mode === "evaluated") {
          succeeded += 1;
        }
      }

      try {
        await upsertScalpV2Candidates({
          rows: rows.map((entry) => entry.row),
        });
        for (const entry of rows) {
          applyPersistSuccess(entry);
        }
      } catch {
        // Preserve old behavior if the batch write fails:
        // - evaluated persist failure increments replayErrors
        // - rejected persist failure is non-fatal (still processed)
        for (const entry of rows) {
          try {
            await upsertScalpV2Candidates({
              rows: [entry.row],
            });
            applyPersistSuccess(entry);
          } catch {
            processed += 1;
            if (entry.mode === "evaluated") {
              replayErrors += 1;
            }
          }
        }
      }
    }

    const backtestConcurrencyMax = Math.max(
      1,
      Math.min(
        16,
        toPositiveInt(
          process.env.SCALP_V2_RESEARCH_BACKTEST_CONCURRENCY_MAX,
          4,
          16,
        ),
      ),
    );
    const BACKTEST_CONCURRENCY = Math.max(
      1,
      Math.min(
        backtestConcurrencyMax,
        toPositiveInt(
          process.env.SCALP_V2_RESEARCH_BACKTEST_CONCURRENCY,
          1,
          backtestConcurrencyMax,
        ),
      ),
    );
    type StageCandidateRuntime = {
      candidate: (typeof chunked)[number];
      selectedIndex: number;
      deploymentId: string;
      replayConfig: any;
      symbolPipSize: number;
      stageResults: Record<ScalpV2WorkerStageId, ScalpV2WorkerStageResult>;
      finalized: boolean;
    };

    const stagePolicyById = new Map(
      stagePolicies.map((stage) => [stage.id, stage] as const),
    );

    function buildBlockedStageResult(
      stageId: ScalpV2WorkerStageId,
    ): ScalpV2WorkerStageResult {
      const stage = stagePolicyById.get(stageId)!;
      const fromTs = windowToTs - stage.weeks * ONE_WEEK_MS;
      return buildWorkerStageSkeleton({
        stage,
        fromTs,
        toTs: windowToTs,
        reason: "blocked_prior_stage_failed",
      });
    }

    function markDownstreamBlocked(
      runtime: StageCandidateRuntime,
      fromStageId: ScalpV2WorkerStageId,
    ) {
      if (fromStageId === "a") {
        runtime.stageResults.b = buildBlockedStageResult("b");
        runtime.stageResults.c = buildBlockedStageResult("c");
      } else if (fromStageId === "b") {
        runtime.stageResults.c = buildBlockedStageResult("c");
      }
    }

    function updateStageCounters(
      stageId: ScalpV2WorkerStageId,
      stageResult: ScalpV2WorkerStageResult,
    ) {
      if (stageId === "a") {
        if (stageResult.passed) stageAPass += 1;
        else stageAFail += 1;
        return;
      }
      if (stageId === "b") {
        if (!stageResult.executed) return;
        if (stageResult.passed) stageBPass += 1;
        else stageBFail += 1;
        return;
      }
      if (!stageResult.executed) return;
      if (stageResult.passed) stageCPass += 1;
      else stageCFail += 1;
    }

    async function ensureCandidateSymbolCandles(
      runtime: StageCandidateRuntime,
    ): Promise<ScalpReplayCandle[] | null> {
      const candidate = runtime.candidate;
      let symbolCandles = candleCache.get(candidate.symbol) ?? null;
      if (!symbolCandles) {
        // Load only stage A window initially (4 weeks). If later stages need
        // a wider range, stage evaluation lazily extends to the full window.
        const stageAFromTs = windowToTs - workerPolicy.stageA.weeks * ONE_WEEK_MS;
        const candleFromTs =
          cacheIsPopulated && !symbolsNeedingFullRange.has(candidate.symbol)
            ? newestWeekStart
            : stageAFromTs;
        const history = await loadScalpCandleHistoryInRange(
          candidate.symbol,
          "1m",
          candleFromTs,
          windowToTs,
        );
        const meta = symbolMetadataMap.get(candidate.symbol) ?? null;
        const symbolPipSize = pipSizeForScalpSymbol(
          candidate.symbol,
          meta ?? undefined,
        );
        const category = inferScalpV2AssetCategory(candidate.symbol);
        const categoryFloor = minSpreadPipsForCategory(category);
        const baseReplayConfig = defaultScalpReplayConfig(candidate.symbol);
        const tickSpreadPips = meta?.tickSize ? meta.tickSize / symbolPipSize : 0;
        const spreadPips = Math.max(
          baseReplayConfig.defaultSpreadPips,
          categoryFloor,
          tickSpreadPips,
        );
        symbolCandles = filterSundayReplayCandles(
          toReplayCandlesFromHistory(
            (history.record?.candles || []) as Array<
              [number, number, number, number, number, number]
            >,
            spreadPips,
          ),
        );
        candleCache.set(candidate.symbol, symbolCandles);
        await emitResearchHeartbeat({
          phase: "loading_candles",
          progress: {
            ...baseProgress,
            candidateIndex: runtime.selectedIndex + 1,
            totalSelected: chunked.length,
            symbolsLoaded: candleCache.size,
            symbolsTotal: uniqueSymbols.length,
          },
        });
      }
      if (symbolCandles.length < workerPolicy.minCandles) return null;
      return symbolCandles;
    }

    async function evaluateCandidateStage(
      runtime: StageCandidateRuntime,
      stage: ScalpV2WorkerStagePolicy,
    ): Promise<ScalpV2WorkerStageResult> {
      const candidate = runtime.candidate;
      let symbolCandles = candleCache.get(candidate.symbol) ?? [];
      const fromTs = windowToTs - stage.weeks * ONE_WEEK_MS;
      let stageCandles = getCachedCandleWindow({
        symbol: candidate.symbol,
        candles: symbolCandles,
        fromTs,
        toTs: windowToTs,
      });
      const stageStartedAtMs = nowMs();
      const stageWeekStarts = listWeekStarts({ fromTs, toTs: windowToTs });
      const newestWeekStartForStage =
        stageWeekStarts.length > 0
          ? stageWeekStarts[stageWeekStarts.length - 1]!
          : null;
      if (newestWeekStartForStage === null) {
        return buildWorkerStageSkeleton({
          stage,
          fromTs,
          toTs: windowToTs,
          reason: "stage_no_weeks",
        });
      }

      const priorWeekStarts =
        stageWeekStarts.length > 1
          ? stageWeekStarts.slice(0, stageWeekStarts.length - 1)
          : [];
      const cacheKey = `${candidate.venue}:${candidate.symbol}:${candidate.strategyId}:${candidate.tuneId}:${candidate.session}:${stage.id}`.toLowerCase();
      const cachedWeeks =
        workerStageWeeklyCache.get(cacheKey) ||
        new Map<number, ScalpV2WorkerStageWeeklyMetrics>();
      let weeklyByStart = new Map<number, ScalpV2WorkerStageWeeklyMetrics>();
      let missingPriorWeeks = false;
      for (const weekStart of priorWeekStarts) {
        const cached = cachedWeeks.get(weekStart);
        if (!hasReusableNonZeroWeeklyCacheMetrics(cached)) {
          missingPriorWeeks = true;
          break;
        }
        weeklyByStart.set(weekStart, cached);
      }
      const newestWeekCached = cachedWeeks.get(newestWeekStartForStage);
      const canReuseCachedNewestWeek =
        !missingPriorWeeks &&
        hasReusableNonZeroWeeklyCacheMetrics(newestWeekCached);

      if (
        missingPriorWeeks &&
        symbolCandles[0] &&
        symbolCandles[0].ts > fromTs
      ) {
        const extended = await loadScalpCandleHistoryInRange(
          candidate.symbol,
          "1m",
          fromTs,
          windowToTs,
        ).catch(() => null);
        if (extended?.record?.candles) {
          const meta = symbolMetadataMap.get(candidate.symbol) ?? null;
          const pip = pipSizeForScalpSymbol(candidate.symbol, meta ?? undefined);
          const cat = inferScalpV2AssetCategory(candidate.symbol);
          const floor = minSpreadPipsForCategory(cat);
          const base = defaultScalpReplayConfig(candidate.symbol);
          const tick = meta?.tickSize ? meta.tickSize / pip : 0;
          const spread = Math.max(base.defaultSpreadPips, floor, tick);
          symbolCandles = filterSundayReplayCandles(
            toReplayCandlesFromHistory(
              extended.record.candles as Array<
                [number, number, number, number, number, number]
              >,
              spread,
            ),
          );
          candleCache.set(candidate.symbol, symbolCandles);
          stageCandles = getCachedCandleWindow({
            symbol: candidate.symbol,
            candles: symbolCandles,
            fromTs,
            toTs: windowToTs,
          });
        }
      }

      let aggregate: ReturnType<typeof aggregateStageFromWeeklyMetrics>;

      if (missingPriorWeeks) {
        if (stageCandles.length < workerPolicy.minCandles) {
          return buildWorkerStageSkeleton({
            stage,
            fromTs,
            toTs: windowToTs,
            reason: "insufficient_candles",
          });
        }
        const fullReplay = await runScalpReplay({
          candles: stageCandles,
          pipSize: runtime.symbolPipSize,
          config: runtime.replayConfig,
          captureTimeline: false,
          earlyAbortNetR: stage.minNetR * -2,
          earlyAbortAfterPct: 50,
        });
        fullStageReplays += 1;
        logResearch("replay_done", `${stage.id}:full:${candidate.symbol}`);
        weeklyByStart = buildWorkerStageWeeklyMetricsMap({
          trades: fullReplay.trades,
          fromTs,
          toTs: windowToTs,
        });
        aggregate = aggregateStageFromWeeklyMetrics({
          fromTs,
          toTs: windowToTs,
          weeklyByStart,
        });
        const cacheRows: typeof pendingCacheWrites = [];
        for (const [ws, metrics] of weeklyByStart.entries()) {
          cacheRows.push({
            venue: candidate.venue,
            symbol: candidate.symbol,
            strategyId: candidate.strategyId,
            tuneId: candidate.tuneId,
            session: candidate.session,
            stageId: stage.id,
            weekStartTs: ws,
            weekToTs: ws + ONE_WEEK_MS,
            metrics,
          });
        }
        pendingCacheWrites.push(...cacheRows);
      } else if (canReuseCachedNewestWeek) {
        weeklyByStart.set(newestWeekStartForStage, newestWeekCached!);
        aggregate = aggregateStageFromWeeklyMetrics({
          fromTs,
          toTs: windowToTs,
          weeklyByStart,
        });
        cachedStageReuses += 1;
      } else {
        const newestWeekToTs = newestWeekStartForStage + ONE_WEEK_MS;
        const newestWeekCandles = getCachedCandleWindow({
          symbol: candidate.symbol,
          candles: symbolCandles,
          fromTs: newestWeekStartForStage,
          toTs: newestWeekToTs,
        });
        if (newestWeekCandles.length < workerPolicy.minCandles) {
          return buildWorkerStageSkeleton({
            stage,
            fromTs,
            toTs: windowToTs,
            reason: "insufficient_latest_week_candles",
          });
        }
        const newestWeekReplay = await runScalpReplay({
          candles: newestWeekCandles,
          pipSize: runtime.symbolPipSize,
          config: runtime.replayConfig,
          captureTimeline: false,
        });
        incrementalStageReplays += 1;
        logResearch("replay_done", `${stage.id}:incr:${candidate.symbol}`);
        const newestWeekMetrics = buildWorkerStageWeeklyMetrics({
          trades: newestWeekReplay.trades,
          weekStartTs: newestWeekStartForStage,
          weekToTs: newestWeekToTs,
        });
        weeklyByStart.set(newestWeekStartForStage, newestWeekMetrics);
        aggregate = aggregateStageFromWeeklyMetrics({
          fromTs,
          toTs: windowToTs,
          weeklyByStart,
        });
        pendingCacheWrites.push({
          venue: candidate.venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          session: candidate.session,
          stageId: stage.id,
          weekStartTs: newestWeekStartForStage,
          weekToTs: newestWeekToTs,
          metrics: newestWeekMetrics,
        });
      }

      const stageResult: ScalpV2WorkerStageResult = {
        id: stage.id,
        weeks: stage.weeks,
        fromTs,
        toTs: windowToTs,
        executed: true,
        passed: false,
        reason: "stage_pending_gate",
        candles: stageCandles.length,
        trades: aggregate.trades,
        netR: aggregate.netR,
        expectancyR: aggregate.expectancyR,
        winRatePct: aggregate.winRatePct,
        maxDrawdownR: aggregate.maxDrawdownR,
        profitFactor: aggregate.profitFactor,
        winningWeeks: aggregate.winningWeeks,
        consecutiveWinningWeeks: aggregate.consecutiveWinningWeeks,
        durationMs: Math.max(0, nowMs() - stageStartedAtMs),
        weeklyNetR: aggregate.weeklyNetR,
        maxWeeklyNetR: aggregate.maxWeeklyNetR,
        largestTradeR: aggregate.largestTradeR,
        exitReasons: aggregate.exitReasons,
      };
      const gate = evaluateWorkerStageGate({ stage, stageResult });
      stageResult.passed = gate.passed;
      stageResult.reason = gate.reason || "stage_passed";
      return stageResult;
    }

    function finalizeCandidateRuntime(runtime: StageCandidateRuntime) {
      if (runtime.finalized) return;
      const candidate = runtime.candidate;
      const stageAResult = runtime.stageResults.a;
      const stageBResult = runtime.stageResults.b;
      const stageCResult = runtime.stageResults.c;
      const finalPass = stageCResult.passed;
      const meetsMinStage =
        minPersistStage === "a"
          ? stageAResult.passed
          : minPersistStage === "b"
            ? stageBResult.passed
            : stageCResult.passed;

      const workerMeta = {
        version: "v2_research_inline_r3",
        evaluatedAtMs: nowTs,
        policy: {
          stageA: workerPolicy.stageA,
          stageB: workerPolicy.stageB,
          stageC: workerPolicy.stageC,
          minCandles: workerPolicy.minCandles,
          weeklyCacheEnabled: true,
          windowSliceCacheEnabled,
        },
        windowToTs,
        stageA: stageAResult,
        stageB: stageBResult,
        stageC: stageCResult,
        finalPass,
      };

      if (!meetsMinStage) {
        droppedBelowMinStage += 1;
        pendingCandidateUpserts.push({
          mode: "rejected",
          row: {
            venue: candidate.venue,
            symbol: candidate.symbol,
            strategyId: candidate.strategyId,
            tuneId: candidate.tuneId,
            entrySessionProfile: candidate.session,
            score: candidate.score,
            status: "rejected",
            reasonCodes: [
              "SCALP_V2_RESEARCH_INLINE",
              `SCALP_V2_BELOW_MIN_STAGE_${minPersistStage.toUpperCase()}`,
            ],
            metadata: {
              evaluatedAtMs: nowTs,
              evaluator: "v2_research_inline",
              worker: workerMeta,
            },
          },
        });
        runtime.finalized = true;
        return;
      }

      const executionPlan = resolveModelGuidedComposerExecutionPlanFromBlocks(
        candidate.dsl.blocksByFamily,
      );
      const metadata: Record<string, unknown> = {
        discoveredAtMs: candidate.dsl.generatedAtMs,
        source: "model_guided_composer",
        researchCandidateId: candidate.candidateId,
        researchDsl: candidate.dsl.blocksByFamily,
        researchReferences: candidate.dsl.referenceStrategyIds,
        researchSupportScore: candidate.dsl.supportScore,
        researchRegimeGateId: candidate.dsl.regimeGateId || null,
        composerModel: candidate.dsl.model,
        composerExecutionPlan: executionPlan,
        evaluatedAtMs: nowTs,
        evaluator: "v2_research_inline",
        worker: workerMeta,
      };
      pendingCandidateUpserts.push({
        mode: "evaluated",
        row: {
          venue: candidate.venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          entrySessionProfile: candidate.session,
          score: candidate.score,
          status: "evaluated",
          reasonCodes: [
            "SCALP_V2_RESEARCH_INLINE",
            finalPass
              ? "SCALP_V2_WORKER_STAGE_C_PASS"
              : "SCALP_V2_WORKER_STAGE_C_FAIL",
          ],
          metadata,
        },
      });
      runtime.finalized = true;
    }

    const candidateRuntimes: StageCandidateRuntime[] = chunked.map(
      (candidate, selectedIndex) => {
        const deploymentId = toDeploymentId({
          venue: candidate.venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          session: candidate.session,
        });
        const replayBaseConfig = defaultScalpReplayConfig(candidate.symbol);
        const exitOverrides = resolveExitRuleOverrides(
          candidate.dsl.blocksByFamily.exit_rule,
        );
        const entryOverrides = resolveEntryTriggerOverrides(
          candidate.dsl.blocksByFamily.entry_trigger,
        );
        const riskReplayOverrides = resolveRiskRuleReplayOverrides(
          candidate.dsl.blocksByFamily.risk_rule,
        );
        const smReplayOverrides = resolveStateMachineReplayOverrides(
          candidate.dsl.blocksByFamily.state_machine,
        );
        const replayConfig = {
          ...replayBaseConfig,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          deploymentId,
          tuneLabel: candidate.tuneId,
          strategy: {
            ...replayBaseConfig.strategy,
            entrySessionProfile: candidate.session,
            ...exitOverrides,
            ...entryOverrides,
            ...riskReplayOverrides,
            ...smReplayOverrides,
          },
        };
        const candidateMeta = symbolMetadataMap.get(candidate.symbol) ?? null;
        const symbolPipSize = pipSizeForScalpSymbol(
          candidate.symbol,
          candidateMeta ?? undefined,
        );
        return {
          candidate,
          selectedIndex,
          deploymentId,
          replayConfig,
          symbolPipSize,
          stageResults: {
            a: buildWorkerStageSkeleton({
              stage: workerPolicy.stageA,
              fromTs: windowToTs - workerPolicy.stageA.weeks * ONE_WEEK_MS,
              toTs: windowToTs,
              reason: "stage_not_started",
            }),
            b: buildWorkerStageSkeleton({
              stage: workerPolicy.stageB,
              fromTs: windowToTs - workerPolicy.stageB.weeks * ONE_WEEK_MS,
              toTs: windowToTs,
              reason: "stage_not_started",
            }),
            c: buildWorkerStageSkeleton({
              stage: workerPolicy.stageC,
              fromTs: windowToTs - workerPolicy.stageC.weeks * ONE_WEEK_MS,
              toTs: windowToTs,
              reason: "stage_not_started",
            }),
          },
          finalized: false,
        };
      },
    );

    async function runBarrierStage(params: {
      stage: ScalpV2WorkerStagePolicy;
      candidates: StageCandidateRuntime[];
    }): Promise<StageCandidateRuntime[]> {
      const passers: StageCandidateRuntime[] = [];
      let processedInStage = 0;

      for (
        let candidateIdx = 0;
        candidateIdx < params.candidates.length;
        candidateIdx += BACKTEST_CONCURRENCY
      ) {
        const elapsedMs = nowMs() - jobStartMs;
        if (elapsedMs >= timeBudgetMs) {
          timeBudgetExhausted = true;
          break;
        }
        const batch = params.candidates.slice(
          candidateIdx,
          candidateIdx + BACKTEST_CONCURRENCY,
        );
        await emitResearchHeartbeat({
          phase: "backtest_candidates",
          progress: {
            ...baseProgress,
            workerStage: params.stage.id,
            workerStageProcessed: processedInStage + batch.length,
            workerStageTotal: params.candidates.length,
            stageCPass,
            persisted: persistedCount,
            replayErrors,
          },
        });

        const batchResults = await Promise.all(
          batch.map(async (runtime) => {
            if (runtime.finalized) return null;
            try {
              const symbolCandles = await ensureCandidateSymbolCandles(runtime);
              if (!symbolCandles) {
                processed += 1;
                runtime.finalized = true;
                return null;
              }
              if (params.stage.id === "a") {
                logResearch(
                  "replay_start",
                  `${runtime.candidate.symbol}:${runtime.candidate.tuneId.slice(0, 20)}:${runtime.candidate.session}`,
                );
              }
              const stageResult = await evaluateCandidateStage(
                runtime,
                params.stage,
              );
              return { runtime, stageResult };
            } catch (err: any) {
              replayErrors += 1;
              processed += 1;
              runtime.finalized = true;
              return null;
            }
          }),
        );

        for (const row of batchResults) {
          if (!row) continue;
          const { runtime, stageResult } = row;
          runtime.stageResults[params.stage.id] = stageResult;
          updateStageCounters(params.stage.id, stageResult);

          if (params.stage.id === "a") {
            if (stageResult.passed) {
              passers.push(runtime);
            } else {
              markDownstreamBlocked(runtime, "a");
              finalizeCandidateRuntime(runtime);
            }
            continue;
          }

          if (params.stage.id === "b") {
            if (stageResult.passed) {
              passers.push(runtime);
            } else {
              markDownstreamBlocked(runtime, "b");
              finalizeCandidateRuntime(runtime);
            }
            continue;
          }

          finalizeCandidateRuntime(runtime);
        }

        processedInStage += batch.length;

        await flushPendingCandidateUpserts();

        await emitResearchHeartbeat({
          phase: "backtest_candidates",
          force: true,
          progress: {
            ...baseProgress,
            workerStage: params.stage.id,
            workerStageProcessed: processedInStage,
            workerStageTotal: params.candidates.length,
            stageCPass,
            persisted: persistedCount,
            replayErrors,
          },
        });

        if (pendingCacheWrites.length >= 50) {
          await upsertScalpV2WeeklyCache({ rows: pendingCacheWrites }).catch(
            () => 0,
          );
          pendingCacheWrites.length = 0;
        }
      }

      return passers;
    }

    const stageAInput = candidateRuntimes.filter((runtime) => !runtime.finalized);
    const stageAPassers = await runBarrierStage({
      stage: workerPolicy.stageA,
      candidates: stageAInput,
    });

    let stageBPassers: StageCandidateRuntime[] = [];
    if (!timeBudgetExhausted) {
      stageBPassers = await runBarrierStage({
        stage: workerPolicy.stageB,
        candidates: stageAPassers.filter((runtime) => !runtime.finalized),
      });
    }

    if (!timeBudgetExhausted) {
      await runBarrierStage({
        stage: workerPolicy.stageC,
        candidates: stageBPassers.filter((runtime) => !runtime.finalized),
      });
    }

    await flushPendingCandidateUpserts();

    // Flush remaining cache writes
    if (pendingCacheWrites.length > 0) {
      await upsertScalpV2WeeklyCache({ rows: pendingCacheWrites }).catch(() => 0);
    }
    // Prune cache rows older than stage C window + 2 week margin
    const cacheRetentionTs = windowToTs - (workerPolicy.stageC.weeks + 2) * ONE_WEEK_MS;
    await pruneScalpV2WeeklyCache({ olderThanTs: cacheRetentionTs }).catch(() => 0);

    const remaining = Math.max(0, chunked.length - processed);
    const pendingAfter = await resolvePendingAfterBacklog(remaining + deferredCount);

    details = {
      scopeCount: scopes.length,
      poolSizeTotal,
      deploymentVariantsGenerated,
      totalCandidates: allCandidates.length,
      deploymentRolloverRequeued,
      skippedByCache,
      skippedByClearFail,
      skippedByNetRPreFilter,
      candidatesWithPreviousResults: previousResults.size,
      backtested: chunked.length,
      deferredToNextRun: deferredCount,
      symbolsThisRun: uniqueSymbols.length,
      symbolsTotal: discoveredSymbols.length || uniqueSymbols.length,
      processedCandidates: processed,
      configuredBatchSize,
      effectiveBatchSize,
      minPersistStage,
      droppedBelowMinStage,
      persistedCount,
      stageAPass,
      stageAFail,
      stageBPass,
      stageBFail,
      stageCPass,
      stageCFail,
      replayErrors,
      incrementalStageReplays,
      fullStageReplays,
      cachedStageReuses,
      droppedByVenuePolicy,
      droppedByScopePrune,
      freshnessGate,
      timeBudgetExhausted,
      timeBudgetMs,
      elapsedMs: nowMs() - jobStartMs,
      remaining,
      pendingAfter,
      scopePrune: scopePrune.details,
      policy: {
        stageA: workerPolicy.stageA,
        stageB: workerPolicy.stageB,
        stageC: workerPolicy.stageC,
        minCandles: workerPolicy.minCandles,
        weeklyCacheEnabled: true,
      },
    };

    return buildScalpV2JobResult({
      jobKind: "research",
      processed,
      succeeded,
      failed,
      pendingAfter,
      details,
    });
  } catch (err: any) {
    ok = false;
    failed = Math.max(1, failed);
    details = { error: err?.message || String(err) };
    return buildScalpV2JobResult({
      jobKind: "research",
      processed,
      succeeded,
      failed,
      pendingAfter: 0,
      details,
    });
  } finally {
    await finalizeScalpV2Job({
      jobKind: "research",
      lockOwner: owner,
      ok,
      details,
    });
  }
}

/**
 * Evaluate is now a thin delegate to the unified research job.
 * Kept for backward compatibility with cron routes and FullAutoCycle.
 */
export async function runScalpV2EvaluateJob(params: {
  batchSize?: number;
} = {}): Promise<ScalpV2JobResult> {
  const result = await runScalpV2ResearchJob({ batchSize: params.batchSize });
  return {
    ...result,
    jobKind: "evaluate",
    details: { ...result.details, delegatedTo: "research" },
  };
}

/**
 * Worker is now a thin delegate to the unified research job.
 * Kept for backward compatibility with cron routes and FullAutoCycle.
 */
export async function runScalpV2WorkerJob(params: {
  batchSize?: number;
} = {}): Promise<ScalpV2JobResult> {
  const result = await runScalpV2ResearchJob({ batchSize: params.batchSize });
  return {
    ...result,
    jobKind: "worker",
    details: { ...result.details, delegatedTo: "research" },
  };
}

// --- Legacy evaluate/worker implementations removed ---
// Both now delegate to runScalpV2ResearchJob which handles
// candidate generation, cache-based dedup, and inline backtesting
// in a single pass with no cursor or batch-size throttling.

function _legacyEvaluateRemoved(): never {
  // The old runScalpV2EvaluateJob (cursor-based, no backtest) and
  // runScalpV2WorkerJob (DB-loaded candidates, separate backtest)
  // have been replaced by the unified research job above.
  throw new Error("Legacy evaluate/worker removed — use runScalpV2ResearchJob");
}
// Marker to prevent accidental re-addition. If TypeScript shows this as
// unused, that's expected — it exists purely as a code-archaeology marker.
void _legacyEvaluateRemoved;

// Old evaluate/worker implementations were here (1000+ lines).
// Removed in Phase 2 simplification — both now delegate to research.

const _legacyCodeRemoved = true; void _legacyCodeRemoved;
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
    const activePrunedScopes = normalizeActivePrunedScopes(runtime, nowTs);
    const policy = resolvePromotionPolicy();
    const requireWinnerShortlist = envBool(
      "SCALP_V2_REQUIRE_WINNER_SHORTLIST",
      true,
    );
    const allCandidatesRaw = await listScalpV2Candidates({ limit: 10_000 });
    const allCandidates = allCandidatesRaw.filter((row) => {
      if (
        !isScalpV2RuntimeSymbolInScope({
          runtime,
          venue: row.venue,
          symbol: row.symbol,
        })
      ) {
        return false;
      }
      const scopeKey = toScalpV2ScopeKey({
        venue: row.venue,
        symbol: row.symbol,
        session: row.entrySessionProfile,
      });
      if (activePrunedScopes[scopeKey]) return false;
      return true;
    });
    const filteredCandidatesOutOfScope = Math.max(
      0,
      allCandidatesRaw.length - allCandidates.length,
    );
    const filteredCandidatesByScopePrune = allCandidatesRaw.filter((row) => {
      const scopeKey = toScalpV2ScopeKey({
        venue: row.venue,
        symbol: row.symbol,
        session: row.entrySessionProfile,
      });
      return Boolean(activePrunedScopes[scopeKey]);
    }).length;
    const promotionPool = allCandidates.filter(
      (row) =>
        row.status === "evaluated" ||
        row.status === "promoted" ||
        row.status === "shadow" ||
        row.status === "rejected",
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
        filteredCandidatesByScopePrune,
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

    const candidateByDeploymentId = new Map(
      promotionPool.map((candidate) => [
        toDeploymentId({
          venue: candidate.venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          session: candidate.entrySessionProfile,
        }),
        candidate,
      ]),
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
      workerStages: Record<string, unknown> | null;
      eligible: boolean;
      shadowEligible: boolean;
      reason: string;
      enabled: boolean;
      shortlistIncluded: boolean;
      exactLoser: boolean;
      riskProfile: typeof runtime.riskProfile;
      promotedAtMs: number | null;
    };

    let skippedWorkerMissing = 0;
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

      // Phase 3: skip candidates where the worker has never run — no point
      // creating disabled deployments for them. Only process candidates that
      // either (a) already have a deployment row, or (b) have worker results.
      const workerGate = resolveWorkerStageCPass(candidate?.metadata || null);
      if (!existing && !workerGate.hasWorkerState) {
        skippedWorkerMissing += 1;
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
      const hasMixedSessionEvidence =
        strictSessionEvidence.mismatchedTrades > 0 ||
        strictSessionEvidence.mismatchedSessions.length > 0;
      const workerFreshness = resolveWorkerStageCFreshness({
        workerStages: workerGate.workerStages,
        requiredWeeks: policy.minCompletedWeeks,
        nowTs,
      });
      const freshness = workerFreshness.freshness;
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

      // Direct promotion: backtest stage C pass is sufficient for live
      // deployment. Backtests mirror live conditions so no shadow probation
      // period is needed — stage C already validates 12 weeks of data.
      let reason = "promotion_not_eligible";
      let eligible = false;
      let shadowEligible = false;
      // Check backtest 4-week rolling netR against promotion threshold.
      const backtestFourWeekGateFailed =
        workerGate.backtestMin4wNetR !== null &&
        workerGate.backtestMin4wNetR < policy.minFourWeekNetR;

      if (suppressed) {
        reason =
          lifecycle.state === "retired"
            ? "retired_cooldown"
            : "suspended_cooldown";
      } else if (!candidate) {
        reason = "candidate_missing";
      } else if (!workerGate.stageCPass) {
        reason = workerGate.reason || "worker_stage_c_failed";
      } else if (!workerFreshness.ready) {
        reason = workerFreshness.reason || "worker_stage_c_freshness_incomplete";
      } else if (backtestFourWeekGateFailed) {
        reason = "backtest_4w_net_r_below_threshold";
      } else {
        // Stage C passed with acceptable 4-week rolling netR —
        // promote directly to live.
        eligible = true;
        shadowEligible = true;
        reason = "stage_c_passed";
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
        droppedByBudget: false,
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
        workerStages: workerGate.workerStages,
        eligible,
        shadowEligible,
        reason,
        enabled: false,
        shortlistIncluded: false,
        exactLoser: false,
        riskProfile: mergeRiskProfileWithOverrides(
          existing?.riskProfile || runtime.riskProfile,
          resolveRiskRuleOverrides(
            Array.isArray(asRecord(candidate?.metadata?.researchDsl || {}).risk_rule)
              ? (asRecord(candidate?.metadata?.researchDsl || {}).risk_rule as string[])
              : null,
          ),
        ),
        promotedAtMs: Number.isFinite(promotedAtMs) ? promotedAtMs : null,
      });
    }

    // Winner shortlist: pick the best fully-eligible candidate per scope key,
    // then cap to maxEnabledDeployments for live slots.
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

      if (row.eligible) {
        // Tier 2: fully eligible (forward evidence passed) — enable, respect shortlist
        row.enabled =
          requireWinnerShortlist ? row.shortlistIncluded : true;
        if (requireWinnerShortlist && !row.shortlistIncluded) {
          row.reason = "winner_shortlist_excluded";
        }
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
          shadowEligible: row.shadowEligible,
          reason: row.reason,
          source: "v2_forward_evidence",
          evaluatedAtMs: nowTs,
          promotedAtMs: row.promotedAtMs,
          score: Number.isFinite(row.score) ? row.score : null,
          droppedByBudget: row.droppedByBudget,
          strictSessionEvidence: row.strictSessionEvidence,
          freshness: row.freshness,
          weekly: row.weeklyMetrics,
          worker: row.workerStages,
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
          dsl: asRecord(
            asRecord(
              candidateByDeploymentId.get(
                toDeploymentId({
                  venue: row.venue,
                  symbol: row.symbol,
                  strategyId: row.strategyId,
                  tuneId: row.tuneId,
                  session: row.entrySessionProfile,
                }),
              )?.metadata || {},
            ).researchDsl || {},
          ),
        },
        riskProfile: row.riskProfile,
      }),
    );

    await upsertScalpV2Deployments({ rows });

    const promotedIds = drafts
      .filter((row) => row.candidateId !== null && row.enabled)
      .map((row) => Number(row.candidateId));
    const notPromotedIds = drafts
      .filter((row) => row.candidateId !== null && !row.enabled)
      .map((row) => Number(row.candidateId));
    if (promotedIds.length > 0) {
      await updateScalpV2CandidateStatuses({
        ids: promotedIds,
        status: "promoted",
        metadataPatch: { promotedAtMs: nowTs },
      });
    }
    if (notPromotedIds.length > 0) {
      await updateScalpV2CandidateStatuses({
        ids: notPromotedIds,
        status: "evaluated",
        metadataPatch: { evaluatedAtMs: nowTs },
      });
    }

    const capOut = await enforceScalpV2EnabledCap({
      maxEnabledDeployments: runtime.budgets.maxEnabledDeployments,
    });

    const highlightRows = drafts
      .filter((row) => row.eligible)
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
    const liveEnabledCount = drafts.filter(
      (row) => row.enabled && row.eligible,
    ).length;

    processed = promotionPool.length;
    succeeded = rows.length;
    details = {
      considered: promotionPool.length,
      deploymentsConsidered: drafts.length,
      promoted: promotedIds.length,
      notPromoted: notPromotedIds.length,
      rejectedByBudget: 0,
      suppressedCount,
      enabledCount,
      liveEnabledCount,
      skippedWorkerMissing,
      requireWinnerShortlist,
      demotedByUniqueness,
      demotedByEnabledCap: capOut.demoted,
      exactLosers: drafts.filter((row) => row.exactLoser).length,
      neighborSuspended: neighborSuspended.size,
      freshnessWindowWeeks: policy.minCompletedWeeks,
      filteredCandidatesOutOfScope,
      filteredCandidatesByScopePrune,
      filteredDeploymentsOutOfScope,
      demotedOutOfScopeEnabled: offScopeEnabledDeployments.length,
      enabledSlots: runtime.budgets.maxEnabledDeployments,
      highlightsUpserted,
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

function buildScalpV2RuntimeSnapshotForDeployment(params: {
  strategyId: string;
}): ScalpStrategyRuntimeSnapshot {
  const strategyId = String(params.strategyId || "").trim().toLowerCase();
  const now = Date.now();
  const strategy = {
    strategyId,
    shortName: strategyId || "scalp_v2",
    longName: strategyId || "scalp_v2",
    enabled: true,
    envEnabled: true,
    kvEnabled: true,
    updatedAtMs: now,
    updatedBy: "scalp_v2_runtime",
  };
  return {
    defaultStrategyId: strategyId,
    strategyId,
    strategy,
    strategies: [strategy],
  };
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
        const rp = deployment.riskProfile;
        const dslFromGate = asRecord(asRecord(deployment.promotionGate).dsl || {});
        const smOverrides = resolveStateMachineOverrides(
          Array.isArray(dslFromGate.state_machine) ? (dslFromGate.state_machine as string[]) : null,
        );
        const configOverride = buildScalpV2ExecuteConfigOverride({
          entrySessionProfile: deployment.entrySessionProfile,
          riskProfile: rp,
          stateMachineOverrides: smOverrides,
        });
        const runtimeSnapshot = buildScalpV2RuntimeSnapshotForDeployment({
          strategyId: deployment.strategyId,
        });
        const persistence = createScalpV2ExecutionPersistenceAdapter({
          runtimeSnapshot,
        });
        const result = await runScalpV2ExecuteCycle({
          venue: deployment.venue,
          symbol: deployment.symbol,
          strategyId: deployment.strategyId,
          tuneId: deployment.tuneId,
          deploymentId: deployment.deploymentId,
          dryRun: deploymentDryRun,
          configOverride,
          runtimeSnapshot,
          persistence,
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
  researchBatchSize?: number;
} = {}): Promise<{
  discover: ScalpV2JobResult;
  evaluate: ScalpV2JobResult;
  worker: ScalpV2JobResult;
  promote: ScalpV2JobResult;
  execute: ScalpV2JobResult;
  reconcile: ScalpV2JobResult;
}> {
  // research replaces both evaluate + worker in a single in-memory pass
  const skippedStub = (
    jobKind: ScalpV2JobKind,
    reason = "handled_by_dedicated_cron",
  ): ScalpV2JobResult => ({
    ok: true,
    busy: false,
    jobKind,
    processed: 0,
    succeeded: 0,
    failed: 0,
    pendingAfter: 0,
    details: { skipped: true, reason },
  });
  const cycleResearchBatchSize = Math.max(
    1,
    Math.min(
      10_000,
      toPositiveInt(
        params.researchBatchSize,
        toPositiveInt(process.env.SCALP_V2_RESEARCH_BATCH_SIZE, 100, 10_000),
        10_000,
      ),
    ),
  );
  const research = await runScalpV2ResearchJob({
    batchSize: cycleResearchBatchSize,
  });
  const discover: ScalpV2JobResult = {
    ...research,
    jobKind: "discover",
    details: { ...research.details, delegatedTo: "research" },
  };
  const evaluate: ScalpV2JobResult = {
    ...research,
    jobKind: "evaluate",
    details: { ...research.details, delegatedTo: "research" },
  };
  const worker: ScalpV2JobResult = {
    ...research,
    jobKind: "worker",
    details: { ...research.details, delegatedTo: "research" },
  };
  const promote =
    research.ok && !research.busy && research.pendingAfter <= 0
      ? await runScalpV2PromoteJob()
      : skippedStub("promote", "research_pending");

  // Execute and reconcile run on their own dedicated crons (every 1m and 2m
  // respectively) to match live market takt. The cycle cron only handles
  // research + promote. Return skipped stubs for API compatibility.
  const execute = skippedStub("execute");
  const reconcile = skippedStub("reconcile");

  return { discover, evaluate, worker, promote, execute, reconcile };
}
