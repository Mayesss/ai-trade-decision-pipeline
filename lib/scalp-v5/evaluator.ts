// v5 evaluator: replays the last `holdoutWeeks` of 1m candles for a
// deployment, tags each replayed trade with the regime cell that was
// active at its timestamp, and persists per-cell expectancy onto the
// deployment row. Replaces v4's 104-week sweep for live-deployment
// kill-switch purposes.

import { ensureScalpSymbolMarketMetadata } from "../scalp/symbolMarketMetadataSync";
import { loadScalpCandleHistoryInRange } from "../scalp/candleHistory";
import { resolveScalpDeployment } from "../scalp/deployments";
import { pipSizeForScalpSymbol } from "../scalp/marketData";
import { runScalpReplay } from "../scalp/replay/harness";
import { buildScalpReplayRuntimeFromDeployment } from "../scalp/replay/runtimeConfig";
import type { ScalpReplayCandle } from "../scalp/replay/types";
import type { ScalpCandle } from "../scalp/types";
import { loadScalpV4RegimeSnapshotsBulk } from "../scalp-v4/pg";
import type { ScalpV4CellId, ScalpV4Venue } from "../scalp-v4/types";
import { startOfUtcWeekMondayMs } from "../scalp-v4/week";
import {
  buildScalpV5CellEvidence,
  resolveScalpV5Config,
  tagTradesWithCells,
} from "./index";
import type { ScalpV5CellEvidence } from "./index";
import {
  loadScalpV5DeploymentsForEvaluation,
  upsertScalpV5DeploymentEvidence,
  type ScalpV5DeploymentRow,
} from "./pg";

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
}

export async function evaluateScalpV5ForDeployment(params: {
  deployment: ScalpV5DeploymentRow;
  nowMs?: number;
}): Promise<ScalpV5EvaluationOutcome> {
  const startedAt = Date.now();
  const deploymentRow = params.deployment;
  const cfg = resolveScalpV5Config();
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  // Holdout window aligned to Monday so cell-week lookups match the
  // weekly regime snapshots exactly.
  const holdoutToMs = startOfUtcWeekMondayMs(nowMs);
  const holdoutFromMs = holdoutToMs - cfg.holdoutWeeks * WEEK_MS;

  // Build a ScalpDeploymentRef for the replay harness. The deployment row
  // already carries all the fields we need; resolveScalpDeployment
  // normalises tune/strategy formatting.
  const deploymentRef = resolveScalpDeployment({
    venue: deploymentRow.venue,
    symbol: deploymentRow.symbol,
    strategyId: deploymentRow.strategyId,
    tuneId: deploymentRow.tuneId,
    deploymentId: deploymentRow.deploymentId,
  });

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

  const runtime = buildScalpReplayRuntimeFromDeployment({
    deployment: deploymentRef,
    configOverride: null,
  });
  const pipSize = pipSizeForScalpSymbol(deploymentRow.symbol);
  const replayCandles = toReplayCandles(rawCandles, runtime.defaultSpreadPips);
  const replay = await runScalpReplay({
    candles: replayCandles,
    pipSize,
    config: runtime,
    captureTimeline: false,
  });

  const snapshotMap = await loadScalpV4RegimeSnapshotsBulk({
    pairs: [{ venue: deploymentRow.venue as ScalpV4Venue, symbol: deploymentRow.symbol }],
    classifierVersion: cfg.classifierVersion,
    fromMs: holdoutFromMs,
    toMs: holdoutToMs + WEEK_MS,
  });
  const snaps = snapshotMap.get(`${deploymentRow.venue}:${deploymentRow.symbol}`) || [];
  const snapshotsByWeekStart = new Map<number, ScalpV4CellId>();
  for (const snap of snaps) {
    snapshotsByWeekStart.set(snap.weekStartMs, snap.cellId);
  }

  const tagged = tagTradesWithCells({
    trades: replay.trades,
    snapshotsByWeekStart,
  });
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
  });

  return {
    deploymentId: deploymentRow.deploymentId,
    ok: true,
    enabled,
    evidence,
    durationMs: Date.now() - startedAt,
    tradeCount: replay.trades.length,
    eligibleCells: evidence.eligibleCells,
  };
}

export interface ScalpV5BulkResult {
  processed: number;
  succeeded: number;
  failed: number;
  enabled: number;
  disabled: number;
  outcomes: ScalpV5EvaluationOutcome[];
  details: {
    classifierVersion: string;
    holdoutWeeks: number;
    minTradesPerCell: number;
    nowMs: number;
  };
}

export async function runScalpV5EvaluationBatch(params: {
  limit?: number;
  staleOlderThanMs?: number;
  nowMs?: number;
  // Optional sharding: when shardCount >= 2, only deployments whose stable
  // hash of deployment_id falls into the chosen shardIndex are returned.
  // Lets multiple bulk processes run in parallel on disjoint row sets.
  shardCount?: number;
  shardIndex?: number;
} = {}): Promise<ScalpV5BulkResult> {
  const cfg = resolveScalpV5Config();
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
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
    outcomes,
    details: {
      classifierVersion: cfg.classifierVersion,
      holdoutWeeks: cfg.holdoutWeeks,
      minTradesPerCell: cfg.minTradesPerCell,
      nowMs,
    },
  };
}
