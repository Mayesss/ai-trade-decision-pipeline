import crypto from "crypto";

import { fetchCapitalOpenPositionSnapshots } from "../capital";
import { getScalpVenueAdapter } from "../scalp/adapters";
import { runScalpExecuteCycle } from "../scalp/engine";
import {
  resolveCompletedWeekWindowToUtc,
  startOfWeekMondayUtc,
} from "../scalp/weekWindows";

import { getScalpV2RuntimeConfig } from "./config";
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
  loadScalpV2RuntimeConfig,
  snapshotScalpV2DailyMetrics,
  toDeploymentId,
  trimScalpV2CandidatesByBudget,
  updateScalpV2CandidateStatuses,
  upsertScalpV2Candidates,
  upsertScalpV2Deployments,
  upsertScalpV2PositionSnapshot,
} from "./db";
import { enforceCandidateBudgets, isScalpV2DiscoverSymbolAllowed } from "./logic";
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
  const windowToTs = resolveCompletedWeekWindowToUtc(params.nowTs);
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

    const rows: Parameters<typeof upsertScalpV2Candidates>[0]["rows"] = [];

    let droppedByVenuePolicy = 0;
    for (const venue of runtime.supportedVenues) {
      const symbols = runtime.seedSymbolsByVenue[venue] || [];
      for (const symbol of symbols) {
        if (!isScalpV2DiscoverSymbolAllowed(venue, symbol)) {
          droppedByVenuePolicy += 1;
          continue;
        }
        for (const session of runtime.supportedSessions) {
          const score = 50 + hashScoreSeed(`${venue}:${symbol}:${session}`) / 100;
          rows.push({
            venue,
            symbol,
            strategyId: runtime.defaultStrategyId,
            tuneId: runtime.defaultTuneId,
            entrySessionProfile: session,
            score,
            status: "discovered",
            reasonCodes: ["SCALP_V2_DISCOVERY_SEED"],
            metadata: {
              discoveredAtMs: nowMs(),
              source: "seed_universe",
            },
          });
        }
      }
    }

    processed = rows.length;
    await upsertScalpV2Candidates({ rows });
    const trim = await trimScalpV2CandidatesByBudget({
      maxCandidatesTotal: runtime.budgets.maxCandidatesTotal,
      maxCandidatesPerSymbol: runtime.budgets.maxCandidatesPerSymbol,
    });
    succeeded = rows.length;
    details = {
      insertedOrUpdated: rows.length,
      trimmed: trim.deleted,
      droppedByVenuePolicy,
      budgets: runtime.budgets,
    };

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
    const batchSize = Math.max(1, Math.min(2_000, Math.floor(params.batchSize || 200)));
    const candidates = await listScalpV2Candidates({ status: "discovered", limit: batchSize });
    if (!candidates.length) {
      details = { evaluated: 0, reason: "no_discovered_candidates" };
      return buildScalpV2JobResult({
        jobKind: "evaluate",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const evaluatedRows: Parameters<typeof upsertScalpV2Candidates>[0]["rows"] = candidates.map((candidate) => ({
      venue: candidate.venue,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
      entrySessionProfile: candidate.entrySessionProfile,
      score:
        candidate.score +
        hashScoreSeed(
          `${candidate.venue}:${candidate.symbol}:${candidate.entrySessionProfile}:${candidate.strategyId}`,
        ) /
          100,
      status: "evaluated",
      reasonCodes: ["SCALP_V2_EVALUATED"],
      metadata: {
        evaluatedAtMs: nowMs(),
        evaluator: "v2_alpha",
        liveEnabled: runtime.liveEnabled,
      },
    }));

    await upsertScalpV2Candidates({ rows: evaluatedRows });
    processed = candidates.length;
    succeeded = candidates.length;

    details = {
      evaluated: candidates.length,
      batchSize,
    };

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
    const policy = resolvePromotionPolicy();
    const requireWinnerShortlist = envBool(
      "SCALP_V2_REQUIRE_WINNER_SHORTLIST",
      true,
    );
    const allCandidates = await listScalpV2Candidates({ limit: 10_000 });
    const promotionPool = allCandidates.filter(
      (row) =>
        row.status === "evaluated" ||
        row.status === "promoted" ||
        row.status === "shadow",
    );
    const existingDeployments = await listScalpV2Deployments({ limit: 10_000 });
    if (!promotionPool.length && !existingDeployments.length) {
      details = { promoted: 0, reason: "no_promotable_candidates" };
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

    const nowTs = nowMs();
    const windowToTs = resolveCompletedWeekWindowToUtc(nowTs);
    const windowFromTs = windowToTs - policy.minCompletedWeeks * ONE_WEEK_MS;
    const ledgerRows = await listScalpV2LedgerRows({
      deploymentIds: consideredDeploymentIds,
      fromTsMs: windowFromTs,
      toTsMs: windowToTs,
      limit: Math.max(50_000, consideredDeploymentIds.length * 5_000),
    });
    const weeklyByDeploymentId = new Map<
      string,
      Map<number, { trades: number; netR: number }>
    >();
    for (const row of ledgerRows) {
      const deploymentId = String(row.deploymentId || "").trim();
      if (!deploymentId) continue;
      const tsExitMs = Math.floor(Number(row.tsExitMs) || 0);
      if (!Number.isFinite(tsExitMs) || tsExitMs <= 0) continue;
      const weekStartTs = startOfWeekMondayUtc(tsExitMs);
      if (weekStartTs < windowFromTs || weekStartTs >= windowToTs) continue;
      const weekly = weeklyByDeploymentId.get(deploymentId) || new Map();
      const current = weekly.get(weekStartTs) || { trades: 0, netR: 0 };
      current.trades += 1;
      current.netR += Number.isFinite(Number(row.rMultiple))
        ? Number(row.rMultiple)
        : 0;
      weekly.set(weekStartTs, current);
      weeklyByDeploymentId.set(deploymentId, weekly);
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
      freshness: PromotionFreshness;
      weeklyMetrics: WeeklyRobustnessMetrics | null;
      weeklyGateReason: string | null;
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
      const weeklyByWeekStart = weeklyByDeploymentId.get(deploymentId) || new Map();
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
      } else if (!freshness.ready) {
        reason = "fresh_weeks_incomplete";
      } else if (hasPerWeekTradesDeficit) {
        reason = "forward_min_trades_per_window_below_threshold";
      } else if (totalTrades < policy.minTotalTrades) {
        reason = "forward_total_trades_below_threshold";
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
        freshness,
        weeklyMetrics,
        weeklyGateReason: weeklyGate.reason,
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
      "forward_min_trades_per_window_below_threshold",
      "forward_total_trades_below_threshold",
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
          freshness: row.freshness,
          weekly: row.weeklyMetrics,
          lifecycle: row.lifecycle,
          thresholds: {
            minCompletedWeeks: policy.minCompletedWeeks,
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
      minTradesPerWeek: policy.minTradesPerWeek,
      minTotalTrades: policy.minTotalTrades,
      enabledSlots: runtime.budgets.maxEnabledDeployments,
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

    const deployments = await listScalpV2Deployments({
      enabledOnly: true,
      venue: params.venue,
      session: params.session,
      limit: 500,
    });

    if (!deployments.length) {
      details = { executed: 0, reason: "no_enabled_deployments" };
      return buildScalpV2JobResult({
        jobKind: "execute",
        processed: 0,
        succeeded: 0,
        failed: 0,
        pendingAfter: 0,
        details,
      });
    }

    const bitgetAdapter = getScalpVenueAdapter("bitget");
    const capitalAdapter = getScalpVenueAdapter("capital");
    const bitgetSnapshots = await bitgetAdapter.broker.fetchOpenPositionSnapshots().catch(() => []);
    const capitalSnapshots = await capitalAdapter.broker.fetchOpenPositionSnapshots().catch(() => []);

    for (const deployment of deployments) {
      processed += 1;
      const deploymentDryRun = effectiveDryRun || !runtime.liveEnabled || deployment.liveMode !== "live";
      try {
        const result = await runScalpExecuteCycle({
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
      executedDeployments: deployments.length,
      dryRun: effectiveDryRun,
      liveEnabled: runtime.liveEnabled,
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
    const bitgetAdapter = getScalpVenueAdapter("bitget");
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
  promote: ScalpV2JobResult;
  execute: ScalpV2JobResult;
  reconcile: ScalpV2JobResult;
}> {
  const discover = await runScalpV2DiscoverJob();
  const evaluate = await runScalpV2EvaluateJob();
  const promote = await runScalpV2PromoteJob();
  const execute = await runScalpV2ExecuteJob({
    dryRun: params.executeDryRun,
    venue: params.venue,
    session: params.session,
  });
  const reconcile = await runScalpV2ReconcileJob();
  return { discover, evaluate, promote, execute, reconcile };
}
