import { loadScalpCandleHistoryInRange } from "../scalp/candleHistory";
import { resolveScalpDeployment } from "../scalp/deployments";
import { pipSizeForScalpSymbol } from "../scalp/marketData";
import { runScalpReplay } from "../scalp/replay/harness";
import { buildScalpReplayRuntimeFromDeployment } from "../scalp/replay/runtimeConfig";
import type { ScalpReplayCandle } from "../scalp/replay/types";
import { ensureScalpSymbolMarketMetadata } from "../scalp/symbolMarketMetadataSync";
import { scalpPrisma } from "../scalp/pg/client";
import { sql } from "../scalp/pg/sql";
import { SCALP_V4_CLASSIFIER_VERSION } from "./classifier";
import {
  loadScalpV4CompletedWalkforwardDeploymentIds,
  loadScalpV4RegimeSnapshots,
  upsertScalpV4WalkforwardResult,
} from "./pg";
import { buildScalpV4ClassifierValidityReport } from "./sanity";
import type { ScalpV4Venue } from "./types";
import { runScalpV4WalkForward } from "./walkForward";
import { listScalpV2Candidates } from "../scalp-v2/db";

const WEEK = 7 * 24 * 60 * 60 * 1000;

export interface ScalpV4WalkforwardSweepOptions {
  classifierVersion?: string;
  effectiveTrials?: number;
  windowToMs?: number;
  maxCandidatesPerCall?: number;
  candidateFetchLimit?: number;
  forceValidity?: boolean;
}

export interface ScalpV4WalkforwardSweepResult {
  classifierVersion: string;
  windowFromMs: number;
  windowToMs: number;
  processed: number;
  eligible: number;
  ineligible: number;
  skipped: number;
  results: Array<Record<string, unknown>>;
}

function toReplayCandles(
  rows: Array<[number, number, number, number, number, number]>,
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

function stageCPassed(metadata: Record<string, unknown>): boolean {
  const worker = metadata.worker && typeof metadata.worker === "object" ? (metadata.worker as Record<string, any>) : {};
  if (worker.finalPass === true) return true;
  const stageC = worker.stageC && typeof worker.stageC === "object" ? worker.stageC : {};
  return stageC.passed === true;
}

export async function runScalpV4WalkforwardSweep(
  options: ScalpV4WalkforwardSweepOptions = {},
): Promise<ScalpV4WalkforwardSweepResult> {
  const classifierVersion = options.classifierVersion || SCALP_V4_CLASSIFIER_VERSION;
  const effectiveTrials = Math.max(
    1,
    Math.floor(
      Number(
        options.effectiveTrials ?? process.env.SCALP_V4_EFFECTIVE_TRIALS ?? 2_500_000,
      ),
    ),
  );
  const windowToMsRaw = Math.floor(Number(options.windowToMs || Date.now()));
  const alignedWindowToMs = windowToMsRaw - (windowToMsRaw % WEEK);
  const windowFromMs = alignedWindowToMs - 104 * WEEK;
  const maxCandidatesPerCall = Math.max(
    0,
    Math.floor(
      options.maxCandidatesPerCall ??
        Number(process.env.SCALP_V4_WALKFORWARD_INLINE_MAX_PER_RUN ?? 5),
    ),
  );
  const candidateFetchLimit = Math.max(
    1,
    Math.floor(options.candidateFetchLimit ?? Math.max(maxCandidatesPerCall * 4, 50)),
  );
  if (maxCandidatesPerCall === 0) {
    return {
      classifierVersion,
      windowFromMs,
      windowToMs: alignedWindowToMs,
      processed: 0,
      eligible: 0,
      ineligible: 0,
      skipped: 0,
      results: [],
    };
  }
  const candidates = (await listScalpV2Candidates({ limit: candidateFetchLimit })).filter(
    (row) => stageCPassed(row.metadata),
  );
  const completed = await loadScalpV4CompletedWalkforwardDeploymentIds({
    classifierVersion,
    windowFromMs,
    windowToMs: alignedWindowToMs,
  });
  const results: Array<Record<string, unknown>> = [];
  let eligible = 0;
  let ineligible = 0;
  let skipped = 0;
  let processed = 0;

  for (const candidate of candidates) {
    if (processed >= maxCandidatesPerCall) break;
    const venue = String(candidate.venue || "").toLowerCase() === "capital" ? "capital" : "bitget";
    const deployment = resolveScalpDeployment({
      venue,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
    });
    if (completed.has(deployment.deploymentId)) {
      skipped += 1;
      continue;
    }
    const snapshots = await loadScalpV4RegimeSnapshots({
      venue,
      symbol: candidate.symbol,
      classifierVersion,
      fromMs: windowFromMs,
      toMs: alignedWindowToMs + WEEK,
    });
    if (!snapshots.length) {
      skipped += 1;
      results.push({ candidateId: candidate.id, deploymentId: deployment.deploymentId, skipped: true, reason: "missing_regime_snapshots" });
      continue;
    }
    const validity = buildScalpV4ClassifierValidityReport({ snapshots: snapshots as any });
    if (!validity.passed && !options.forceValidity) {
      skipped += 1;
      results.push({
        candidateId: candidate.id,
        deploymentId: deployment.deploymentId,
        skipped: true,
        reason: `classifier_validity_failed:${validity.reason}`,
      });
      continue;
    }
    const history = await loadScalpCandleHistoryInRange(
      candidate.symbol,
      "1m",
      windowFromMs,
      alignedWindowToMs,
    );
    const candles = (history.record?.candles || []) as Array<[number, number, number, number, number, number]>;
    if (!candles.length) {
      skipped += 1;
      results.push({ candidateId: candidate.id, deploymentId: deployment.deploymentId, skipped: true, reason: "missing_candles" });
      continue;
    }
    const meta = await ensureScalpSymbolMarketMetadata(candidate.symbol, { fetchIfMissing: true });
    const runtime = buildScalpReplayRuntimeFromDeployment({ deployment, configOverride: null });
    const startedAt = Date.now();
    const fullReplayCandles = toReplayCandles(candles, runtime.defaultSpreadPips);
    const run = await runScalpV4WalkForward({
      classifierVersion,
      snapshots: snapshots as any,
      windowFromMs,
      windowToMs: alignedWindowToMs,
      effectiveTrials,
      runWindow: async ({ windowStartMs, windowEndMs }) => {
        const scoped = fullReplayCandles.filter((row) => row.ts >= windowStartMs && row.ts < windowEndMs);
        const replay = await runScalpReplay({
          candles: scoped,
          pipSize: pipSizeForScalpSymbol(candidate.symbol, meta),
          config: runtime,
          captureTimeline: false,
          symbolMeta: meta,
        });
        return replay.trades.map((trade) => ({
          entryTs: trade.entryTs,
          exitTs: trade.exitTs,
          rMultiple: trade.rMultiple,
        }));
      },
    });
    await upsertScalpV4WalkforwardResult({
      candidateId: candidate.id,
      deploymentId: deployment.deploymentId,
      venue: venue as ScalpV4Venue,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
      classifierVersion,
      windowFromMs,
      windowToMs: alignedWindowToMs,
      effectiveTrials,
      status: run.envelope.status,
      envelope: run.envelope,
      windowResults: run.windows.map((row) => ({
        windowStartMs: row.windowStartMs,
        windowEndMs: row.windowEndMs,
        trades: row.trades.length,
        netR: row.trades.reduce((acc, trade) => acc + trade.rMultiple, 0),
      })),
      details: { durationMs: Date.now() - startedAt },
    });
    await scalpPrisma().$executeRaw(sql`
      UPDATE scalp_v2_deployments
      SET
        promotion_gate = jsonb_set(COALESCE(promotion_gate, '{}'::jsonb), '{regimeEnvelope}', ${JSON.stringify(run.envelope)}::jsonb, true),
        updated_at = NOW()
      WHERE deployment_id = ${deployment.deploymentId};
    `);
    if (run.envelope.eligible) eligible += 1;
    else ineligible += 1;
    processed += 1;
    results.push({
      candidateId: candidate.id,
      deploymentId: deployment.deploymentId,
      status: run.envelope.status,
      eligible: run.envelope.eligible,
      allowedCells: run.envelope.allowedCells,
      windows: run.windows.length,
      durationMs: Date.now() - startedAt,
    });
  }

  return {
    classifierVersion,
    windowFromMs,
    windowToMs: alignedWindowToMs,
    processed,
    eligible,
    ineligible,
    skipped,
    results,
  };
}
