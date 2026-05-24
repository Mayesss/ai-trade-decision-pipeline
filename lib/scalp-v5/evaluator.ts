// v5 evaluator: replays the last `holdoutWeeks` of 1m candles for a
// deployment, tags each replayed trade with the regime cell that was
// active at its timestamp, and persists per-cell expectancy onto the
// deployment row. Replaces v4's 104-week sweep for live-deployment
// kill-switch purposes.

import { ensureScalpSymbolMarketMetadata } from "../scalp/symbolMarketMetadataSync";
import { loadScalpCandleHistoryInRange } from "../scalp/candleHistory";
import { resolveScalpDeployment } from "../scalp/deployments";
import { pipSizeForScalpSymbol } from "../scalp/marketData";
import { computeReplayConfigHash, runScalpReplay } from "../scalp/replay/harness";
import { buildScalpReplayRuntimeFromDeployment } from "../scalp/replay/runtimeConfig";
import type {
  ScalpReplayCandle,
  ScalpReplayCheckpoint,
  ScalpReplayRuntimeConfig,
} from "../scalp/replay/types";
import type { ScalpCandle, ScalpDeploymentRef } from "../scalp/types";
import { resolveEntryTriggerOverrides } from "../scalp-v2/entryTriggerPresets";
import { buildScalpV2ExecuteConfigOverride } from "../scalp-v2/executeConfigOverride";
import { resolveExitRuleOverrides } from "../scalp-v2/exitRulePresets";
import { resolveRiskRuleReplayOverrides } from "../scalp-v2/riskRulePresets";
import { resolveStateMachineOverrides } from "../scalp-v2/stateMachinePresets";
import type { ScalpV2Session } from "../scalp-v2/types";
import type { ScalpV2V3TemporalFilter } from "../scalp-v3";
import { loadScalpV4RegimeSnapshotsBulk } from "../scalp-v4/pg";
import type { ScalpV4CellId, ScalpV4Venue } from "../scalp-v4/types";
import { startOfUtcWeekMondayMs } from "../scalp-v4/week";
import {
  SCALP_V5_VERSION,
  buildScalpV5CellEvidence,
  mergeIncrementalCellEvidence,
  resolveScalpV5Config,
  tagTradesWithCells,
} from "./index";
import type { ScalpV5CellEvidence } from "./index";
import {
  loadScalpV5DeploymentCheckpoint,
  loadScalpV5DeploymentEvidence,
  loadScalpV5DeploymentsForEvaluation,
  upsertScalpV5DeploymentEvidence,
  type ScalpV5DeploymentRow,
} from "./pg";
import {
  runScalpV5CandlePreflight,
  type ScalpV5CandlePreflightResult,
} from "./candlePreflight";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asDslList(value: unknown): string[] | null {
  return Array.isArray(value) ? (value as unknown[]).map((v) => String(v)).filter(Boolean) : null;
}

const WEEK_MS = 7 * 24 * 60 * 60 * 1000;

function toReplayCandles(
  rows: ScalpCandle[],
  spreadPips: number,
): ScalpReplayCandle[] {
  return rows.map((row) => ({
    ts: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5] || 0),
    spreadPips,
  }));
}

export interface ScalpV5EvaluationOutcome {
  deploymentId: string;
  ok: boolean;
  reason?: string;
  enabled?: boolean;
  evidence?: ScalpV5CellEvidence;
  durationMs: number;
  tradeCount?: number;
  eligibleCells?: string[];
  // Which evaluator path produced this outcome. "full" = 12-week replay
  // (first eval or any time the incremental prerequisites failed).
  // "incremental" = 1-week resume from the previous checkpoint. Useful for
  // operator visibility during the rollout — once steady state is reached
  // most outcomes should be "incremental".
  mode?: "full" | "incremental";
}

// Resolve the holdout boundary. On Sunday UTC the trading week (Mon-Sat) has
// just ended; we advance the boundary to the upcoming Monday so the just-
// completed week is the 12th holdout week. Mon-Sat use the standard "12
// completed weeks before this week" semantics.
function resolveHoldoutWindow(nowMs: number, holdoutWeeks: number): {
  holdoutFromMs: number;
  holdoutToMs: number;
} {
  const weekStart = startOfUtcWeekMondayMs(nowMs);
  const holdoutToMs =
    new Date(nowMs).getUTCDay() === 0 ? weekStart + WEEK_MS : weekStart;
  const holdoutFromMs = holdoutToMs - holdoutWeeks * WEEK_MS;
  return { holdoutFromMs, holdoutToMs };
}

// Build the per-deployment replay runtime + its config hash. Same logic for
// full and incremental paths, factored so we only build it once and so the
// dispatcher can compute the hash before deciding which path to take.
function buildDeploymentRuntime(deploymentRow: ScalpV5DeploymentRow): {
  runtime: ScalpReplayRuntimeConfig;
  configHash: string;
  deploymentRef: ScalpDeploymentRef;
} {
  const deploymentRef = resolveScalpDeployment({
    venue: deploymentRow.venue,
    symbol: deploymentRow.symbol,
    strategyId: deploymentRow.strategyId,
    tuneId: deploymentRow.tuneId,
    deploymentId: deploymentRow.deploymentId,
  });
  const dslFromGate = asRecord(asRecord(deploymentRow.promotionGate).dsl);
  const entryOverrides = resolveEntryTriggerOverrides(asDslList(dslFromGate.entry_trigger));
  const exitOverrides = resolveExitRuleOverrides(asDslList(dslFromGate.exit_rule));
  const riskReplayOverrides = resolveRiskRuleReplayOverrides(asDslList(dslFromGate.risk_rule));
  const smOverrides = resolveStateMachineOverrides(asDslList(dslFromGate.state_machine));
  const temporalFilterRaw = asRecord(deploymentRow.promotionGate).v3TemporalFilter;
  const temporalFilter =
    Object.keys(asRecord(temporalFilterRaw)).length > 0
      ? (asRecord(temporalFilterRaw) as ScalpV2V3TemporalFilter)
      : null;
  const configOverride = buildScalpV2ExecuteConfigOverride({
    entrySessionProfile: deploymentRow.entrySessionProfile as ScalpV2Session,
    riskProfile: deploymentRow.riskProfile,
    entryTriggerOverrides: entryOverrides,
    exitRuleOverrides: exitOverrides,
    riskRuleReplayOverrides: riskReplayOverrides,
    stateMachineOverrides: smOverrides,
    temporalFilter,
    // Intentionally omit entryBlockReasonCodes — those are a *live* gate
    // applied per-entry. Historical replay should not block on them.
  });
  const runtime = buildScalpReplayRuntimeFromDeployment({
    deployment: deploymentRef,
    configOverride,
  });
  // Don't force-close positions at the end of a chunked replay — the
  // checkpoint carries the open position to the next week. Full replays
  // still benefit from this: a position open at the end of the 12-week
  // holdout was never actually realized, so it shouldn't synthesize a
  // fake R contribution. Live-trading semantics: only naturally-closed
  // trades count toward evidence.
  runtime.forceCloseAtEnd = false;
  const configHash = computeReplayConfigHash(runtime);
  return { runtime, configHash, deploymentRef };
}

// Top-level entry: pick the cheapest evaluation that produces correct
// evidence for the new holdout window. Falls back to a full replay when
// the previous evaluation's checkpoint isn't reusable (no checkpoint,
// version mismatch, config drift, holdout gap, etc.).
export async function evaluateScalpV5ForDeployment(params: {
  deployment: ScalpV5DeploymentRow;
  nowMs?: number;
}): Promise<ScalpV5EvaluationOutcome> {
  const startedAt = Date.now();
  const deploymentRow = params.deployment;
  const cfg = resolveScalpV5Config();
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  const { holdoutFromMs, holdoutToMs } = resolveHoldoutWindow(nowMs, cfg.holdoutWeeks);
  const { runtime, configHash, deploymentRef } = buildDeploymentRuntime(deploymentRow);

  // Try incremental first. The dispatcher's only job is to gate the
  // incremental path on tight prerequisites; the actual work happens in
  // evaluateScalpV5Incremental.
  const existing = await loadScalpV5DeploymentEvidence({ deploymentId: deploymentRow.deploymentId }).catch(
    () => null,
  );
  const evidence = existing?.evidence ?? null;
  const checkpoint = evidence
    ? await loadScalpV5DeploymentCheckpoint({ deploymentId: deploymentRow.deploymentId }).catch(() => null)
    : null;
  const canIncremental =
    evidence !== null &&
    evidence.version === SCALP_V5_VERSION &&
    evidence.holdoutToMs === holdoutToMs - WEEK_MS &&
    evidence.holdoutFromMs === holdoutFromMs - WEEK_MS &&
    evidence.classifierVersion === cfg.classifierVersion &&
    evidence.minTradesPerCell === cfg.minTradesPerCell &&
    checkpoint !== null &&
    checkpoint.configHash === configHash;

  if (canIncremental) {
    return await evaluateScalpV5Incremental({
      deploymentRow,
      existing: evidence!,
      checkpoint: checkpoint!,
      runtime,
      deploymentRef,
      nowMs,
      cfg,
      holdoutFromMs,
      holdoutToMs,
      startedAt,
    });
  }

  return await evaluateScalpV5Full({
    deploymentRow,
    runtime,
    deploymentRef,
    nowMs,
    cfg,
    holdoutFromMs,
    holdoutToMs,
    startedAt,
  });
}

// Full 12-week replay: used the first time a deployment is evaluated and
// any time the incremental prerequisites fail (config drift, missing
// checkpoint, evidence-version mismatch, etc.). Writes a fresh checkpoint
// at the end so the NEXT evaluation can go incremental.
async function evaluateScalpV5Full(params: {
  deploymentRow: ScalpV5DeploymentRow;
  runtime: ScalpReplayRuntimeConfig;
  deploymentRef: ScalpDeploymentRef;
  nowMs: number;
  cfg: ReturnType<typeof resolveScalpV5Config>;
  holdoutFromMs: number;
  holdoutToMs: number;
  startedAt: number;
}): Promise<ScalpV5EvaluationOutcome> {
  const { deploymentRow, runtime, nowMs, cfg, holdoutFromMs, holdoutToMs, startedAt } = params;
  void params.deploymentRef;
  let history;
  try {
    history = await loadScalpCandleHistoryInRange(
      deploymentRow.symbol,
      "1m",
      holdoutFromMs,
      holdoutToMs,
    );
  } catch (err) {
    return {
      deploymentId: deploymentRow.deploymentId,
      ok: false,
      reason: `candle_load_failed:${err instanceof Error ? err.message : String(err)}`,
      durationMs: Date.now() - startedAt,
    };
  }
  const rawCandles = (history?.record?.candles || []) as ScalpCandle[];
  if (rawCandles.length === 0) {
    return {
      deploymentId: deploymentRow.deploymentId,
      ok: false,
      reason: "no_candles",
      durationMs: Date.now() - startedAt,
    };
  }
  await ensureScalpSymbolMarketMetadata(deploymentRow.symbol, {
    fetchIfMissing: true,
  }).catch(() => null);

  const pipSize = pipSizeForScalpSymbol(deploymentRow.symbol);
  const replayCandles = toReplayCandles(rawCandles, runtime.defaultSpreadPips);
  const replay = await runScalpReplay({
    candles: replayCandles,
    pipSize,
    config: runtime,
    captureTimeline: false,
  });

  // Snapshot bounds match the holdout exactly. The candle loader excludes
  // weeks with week_start >= holdoutToMs, so no trade can fall in the
  // current week.
  const snapshotMap = await loadScalpV4RegimeSnapshotsBulk({
    pairs: [{ venue: deploymentRow.venue as ScalpV4Venue, symbol: deploymentRow.symbol }],
    classifierVersion: cfg.classifierVersion,
    fromMs: holdoutFromMs,
    toMs: holdoutToMs,
  });
  const snaps = snapshotMap.get(`${deploymentRow.venue}:${deploymentRow.symbol}`) || [];
  const snapshotsByWeekStart = new Map<number, ScalpV4CellId>();
  for (const snap of snaps) {
    snapshotsByWeekStart.set(snap.weekStartMs, snap.cellId);
  }

  const tagged = tagTradesWithCells({ trades: replay.trades, snapshotsByWeekStart });
  const evidence = buildScalpV5CellEvidence({
    tagged,
    classifierVersion: cfg.classifierVersion,
    evaluatedAtMs: nowMs,
    holdoutFromMs,
    holdoutToMs,
    minTradesPerCell: cfg.minTradesPerCell,
  });
  const enabled = evidence.eligibleCells.length > 0;

  await upsertScalpV5DeploymentEvidence({
    deploymentId: deploymentRow.deploymentId,
    evidence,
    enabled,
    checkpoint: replay.finalCheckpoint ?? null,
  });

  return {
    deploymentId: deploymentRow.deploymentId,
    ok: true,
    enabled,
    evidence,
    durationMs: Date.now() - startedAt,
    tradeCount: replay.trades.length,
    eligibleCells: evidence.eligibleCells,
    mode: "full",
  };
}

// Incremental update: replay only the newly-completed week, merge into the
// existing 12-week evidence (drop oldest, append new). Strategy state is
// resumed from the previous checkpoint so indicators see the full
// historical context, not just one week's worth of warmup.
async function evaluateScalpV5Incremental(params: {
  deploymentRow: ScalpV5DeploymentRow;
  existing: ScalpV5CellEvidence;
  checkpoint: ScalpReplayCheckpoint;
  runtime: ScalpReplayRuntimeConfig;
  deploymentRef: ScalpDeploymentRef;
  nowMs: number;
  cfg: ReturnType<typeof resolveScalpV5Config>;
  holdoutFromMs: number;
  holdoutToMs: number;
  startedAt: number;
}): Promise<ScalpV5EvaluationOutcome> {
  const { deploymentRow, existing, checkpoint, runtime, nowMs, cfg, holdoutFromMs, holdoutToMs, startedAt } =
    params;
  void params.deploymentRef;
  const newWeekStartMs = holdoutToMs - WEEK_MS;
  const newWeekEndMs = holdoutToMs;

  let history;
  try {
    history = await loadScalpCandleHistoryInRange(
      deploymentRow.symbol,
      "1m",
      newWeekStartMs,
      newWeekEndMs,
    );
  } catch (err) {
    return {
      deploymentId: deploymentRow.deploymentId,
      ok: false,
      reason: `incremental_candle_load_failed:${err instanceof Error ? err.message : String(err)}`,
      durationMs: Date.now() - startedAt,
    };
  }
  const rawCandles = (history?.record?.candles || []) as ScalpCandle[];
  // Filter to candles strictly inside the new week — the loader returns
  // whole-week blobs and the bucket containing newWeekStartMs/EndMs may
  // also contain candles outside the new week (e.g. when the upper bound
  // sits at a week boundary).
  const newWeekCandles = rawCandles.filter(
    (c) => Number(c[0]) >= newWeekStartMs && Number(c[0]) < newWeekEndMs,
  );
  if (newWeekCandles.length === 0) {
    return {
      deploymentId: deploymentRow.deploymentId,
      ok: false,
      reason: "incremental_no_candles",
      durationMs: Date.now() - startedAt,
    };
  }
  await ensureScalpSymbolMarketMetadata(deploymentRow.symbol, {
    fetchIfMissing: true,
  }).catch(() => null);

  const pipSize = pipSizeForScalpSymbol(deploymentRow.symbol);
  const replayCandles = toReplayCandles(newWeekCandles, runtime.defaultSpreadPips);
  const replay = await runScalpReplay({
    candles: replayCandles,
    pipSize,
    config: runtime,
    captureTimeline: false,
    initialCheckpoint: checkpoint,
  });

  // Only count trades whose ENTRY fell inside the new week. A position
  // carried over from last week and closed in this week would otherwise be
  // attributed to the new week, double-counting it relative to last week's
  // evidence (which already had it open at the boundary).
  const newWeekTrades = replay.trades.filter(
    (t) => t.entryTs >= newWeekStartMs && t.entryTs < newWeekEndMs,
  );

  // Look up the new week's regime cell. Just one snapshot expected.
  const snapshotMap = await loadScalpV4RegimeSnapshotsBulk({
    pairs: [{ venue: deploymentRow.venue as ScalpV4Venue, symbol: deploymentRow.symbol }],
    classifierVersion: cfg.classifierVersion,
    fromMs: newWeekStartMs,
    toMs: newWeekEndMs,
  });
  const snaps = snapshotMap.get(`${deploymentRow.venue}:${deploymentRow.symbol}`) || [];
  const snapshotsByWeekStart = new Map<number, ScalpV4CellId>();
  for (const snap of snaps) {
    snapshotsByWeekStart.set(snap.weekStartMs, snap.cellId);
  }
  const tagged = tagTradesWithCells({ trades: newWeekTrades, snapshotsByWeekStart });

  const evidence = mergeIncrementalCellEvidence({
    existing,
    newWeekTagged: tagged,
    newHoldoutFromMs: holdoutFromMs,
    newHoldoutToMs: holdoutToMs,
    classifierVersion: cfg.classifierVersion,
    evaluatedAtMs: nowMs,
    minTradesPerCell: cfg.minTradesPerCell,
  });
  const enabled = evidence.eligibleCells.length > 0;

  await upsertScalpV5DeploymentEvidence({
    deploymentId: deploymentRow.deploymentId,
    evidence,
    enabled,
    checkpoint: replay.finalCheckpoint ?? null,
  });

  return {
    deploymentId: deploymentRow.deploymentId,
    ok: true,
    enabled,
    evidence,
    durationMs: Date.now() - startedAt,
    tradeCount: newWeekTrades.length,
    eligibleCells: evidence.eligibleCells,
    mode: "incremental",
  };
}

export interface ScalpV5BulkResult {
  processed: number;
  succeeded: number;
  failed: number;
  enabled: number;
  disabled: number;
  // Split of how the successful evaluations were produced. Sum equals
  // `succeeded`. During the first Sunday after deploy, expect full ≫
  // incremental; on subsequent weeks, expect the opposite.
  fullCount: number;
  incrementalCount: number;
  outcomes: ScalpV5EvaluationOutcome[];
  preflight: ScalpV5CandlePreflightResult | null;
  skippedReason?: "v5_candle_preflight_not_ready";
  details: {
    classifierVersion: string;
    holdoutWeeks: number;
    minTradesPerCell: number;
    nowMs: number;
  };
}

export function shouldRunScalpV5EvaluationCandlePreflight(params: {
  nowMs: number;
  preflightCandles?: boolean;
  forcePreflight?: boolean;
}): boolean {
  if (params.preflightCandles === false) return false;
  if (params.forcePreflight) return true;
  return new Date(params.nowMs).getUTCDay() === 0;
}

export async function runScalpV5EvaluationBatch(params: {
  limit?: number;
  staleOlderThanMs?: number;
  nowMs?: number;
  preflightCandles?: boolean;
  forcePreflight?: boolean;
  preflightBatchSize?: number;
  preflightMaxAttempts?: number;
  // Optional sharding: when shardCount >= 2, only deployments whose stable
  // hash of deployment_id falls into the chosen shardIndex are returned.
  // Lets multiple bulk processes run in parallel on disjoint row sets.
  shardCount?: number;
  shardIndex?: number;
} = {}): Promise<ScalpV5BulkResult> {
  const cfg = resolveScalpV5Config();
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  let preflight: ScalpV5CandlePreflightResult | null = null;
  if (
    shouldRunScalpV5EvaluationCandlePreflight({
      nowMs,
      preflightCandles: params.preflightCandles,
      forcePreflight: params.forcePreflight,
    })
  ) {
    preflight = await runScalpV5CandlePreflight({
      nowMs,
      batchSize: params.preflightBatchSize,
      maxAttempts: params.preflightMaxAttempts,
      auditTrigger: params.forcePreflight ? "evaluate_force_preflight" : "evaluate_sunday_preflight",
    });
    if (!preflight.ready) {
      return {
        processed: 0,
        succeeded: 0,
        failed: preflight.blockingFailures.length,
        enabled: 0,
        disabled: 0,
        fullCount: 0,
        incrementalCount: 0,
        outcomes: [],
        preflight,
        skippedReason: "v5_candle_preflight_not_ready",
        details: {
          classifierVersion: cfg.classifierVersion,
          holdoutWeeks: cfg.holdoutWeeks,
          minTradesPerCell: cfg.minTradesPerCell,
          nowMs,
        },
      };
    }
  }
  const deployments = await loadScalpV5DeploymentsForEvaluation({
    limit: params.limit,
    staleOlderThanMs: params.staleOlderThanMs,
    nowMs,
    shardCount: params.shardCount,
    shardIndex: params.shardIndex,
  });
  let processed = 0;
  let succeeded = 0;
  let failed = 0;
  let enabled = 0;
  let disabled = 0;
  let fullCount = 0;
  let incrementalCount = 0;
  const outcomes: ScalpV5EvaluationOutcome[] = [];
  for (const deployment of deployments) {
    processed += 1;
    const outcome = await evaluateScalpV5ForDeployment({ deployment, nowMs }).catch((err) => ({
      deploymentId: deployment.deploymentId,
      ok: false,
      reason: `evaluator_threw:${err instanceof Error ? err.message : String(err)}`,
      durationMs: 0,
    } as ScalpV5EvaluationOutcome));
    outcomes.push(outcome);
    if (outcome.ok) {
      succeeded += 1;
      if (outcome.enabled) enabled += 1;
      else disabled += 1;
      if (outcome.mode === "incremental") incrementalCount += 1;
      else fullCount += 1;
    } else {
      failed += 1;
    }
  }
  return {
    processed,
    succeeded,
    failed,
    enabled,
    disabled,
    fullCount,
    incrementalCount,
    outcomes,
    preflight,
    details: {
      classifierVersion: cfg.classifierVersion,
      holdoutWeeks: cfg.holdoutWeeks,
      minTradesPerCell: cfg.minTradesPerCell,
      nowMs,
    },
  };
}
