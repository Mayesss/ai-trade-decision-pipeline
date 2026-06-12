#!/usr/bin/env node
import {
  applyScalpV4OverbroadAutoRejects,
  buildScalpV4ClassifierValidityReport,
  listScalpV4ResearchCandidates,
  loadScalpV4CompletedWalkforwardDeploymentIds,
  loadScalpV4RegimeSnapshots,
  runScalpV4WalkForward,
  SCALP_V4_CLASSIFIER_VERSION,
  upsertScalpV4WalkforwardResult,
} from "../lib/scalp/regimes";
import { loadScalpCandleHistoryInRange } from "../lib/scalp/candleHistory";
import { resolveScalpDeployment } from "../lib/scalp/deployments";
import { pipSizeForScalpSymbol } from "../lib/scalp/marketData";
import { runScalpReplay } from "../lib/scalp/replay/harness";
import { buildScalpReplayRuntimeFromDeployment } from "../lib/scalp/replay/runtimeConfig";
import type { ScalpReplayCandle } from "../lib/scalp/replay/types";
import { ensureScalpSymbolMarketMetadata } from "../lib/scalp/symbolMarketMetadataSync";
import { scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";

const WEEK = 7 * 24 * 60 * 60 * 1000;

function parseArgs(argv: string[]): Record<string, string | boolean> {
  const out: Record<string, string | boolean> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i]!;
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) out[key] = true;
    else {
      out[key] = next;
      i += 1;
    }
  }
  return out;
}

function toReplayCandles(rows: Array<[number, number, number, number, number, number]>, spreadPips: number): ScalpReplayCandle[] {
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

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const apply = Boolean(args.apply);
  const rerun = Boolean(args.rerun);
  const forceValidity = Boolean(args.forceValidity || args.force);
  const limit = Math.max(1, Math.min(500, Math.floor(Number(args.limit || 55))));
  const classifierVersion = String(args.classifierVersion || SCALP_V4_CLASSIFIER_VERSION);
  const effectiveTrials = Math.max(1, Math.floor(Number(args.effectiveTrials || process.env.SCALP_V4_EFFECTIVE_TRIALS || 2_500_000)));
  const windowToMs = Math.floor(Number(args.windowToMs || Date.now()));
  const alignedWindowToMs = windowToMs - (windowToMs % WEEK);
  const windowFromMs = alignedWindowToMs - 104 * WEEK;
  const candidates = await listScalpV4ResearchCandidates({ limit });
  const autoRejected = apply ? await applyScalpV4OverbroadAutoRejects(Date.now()) : 0;
  const completedDeploymentIds = rerun
    ? new Set<string>()
    : await loadScalpV4CompletedWalkforwardDeploymentIds({
        classifierVersion,
        windowFromMs,
        windowToMs: alignedWindowToMs,
      });
  const results: unknown[] = [];
  for (const candidate of candidates) {
    const deployment = resolveScalpDeployment({
      venue: candidate.venue,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
    });
    if (completedDeploymentIds.has(deployment.deploymentId)) {
      results.push({ candidateId: candidate.id, deploymentId: deployment.deploymentId, skipped: true, reason: "already_completed" });
      continue;
    }
    const snapshots = await loadScalpV4RegimeSnapshots({
      venue: candidate.venue,
      symbol: candidate.symbol,
      classifierVersion,
      fromMs: windowFromMs,
      toMs: alignedWindowToMs + WEEK,
    });
    if (!snapshots.length) {
      results.push({ candidateId: candidate.id, deploymentId: deployment.deploymentId, skipped: true, reason: "missing_regime_snapshots" });
      continue;
    }
    const validity = buildScalpV4ClassifierValidityReport({ snapshots: snapshots as any });
    if (!validity.passed && !forceValidity) {
      throw new Error(
        `v4 classifier validity failed for ${deployment.deploymentId}: ${validity.reason} ` +
          `(epochs=${validity.epochCount}, cells=${validity.cellCount}). Re-run with --forceValidity to override.`,
      );
    }
    const history = await loadScalpCandleHistoryInRange(candidate.symbol, "1m", windowFromMs, alignedWindowToMs);
    const candles = (history.record?.candles || []) as Array<[number, number, number, number, number, number]>;
    if (!candles.length) {
      results.push({ candidateId: candidate.id, deploymentId: deployment.deploymentId, skipped: true, reason: "missing_candles" });
      continue;
    }
    const meta = await ensureScalpSymbolMarketMetadata(candidate.symbol, { fetchIfMissing: true });
    const runtime = buildScalpReplayRuntimeFromDeployment({
      deployment,
      configOverride: null,
    });
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
    if (apply) {
      await upsertScalpV4WalkforwardResult({
        candidateId: candidate.id,
        deploymentId: deployment.deploymentId,
        venue: candidate.venue,
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
    }
    results.push({
      candidateId: candidate.id,
      deploymentId: deployment.deploymentId,
      status: run.envelope.status,
      eligible: run.envelope.eligible,
      allowedCells: run.envelope.allowedCells,
      windows: run.windows.length,
      validity,
      durationMs: Date.now() - startedAt,
    });
  }
  console.log(JSON.stringify({
    ok: true,
    dryRun: !apply,
    rerun,
    forceValidity,
    skippedCompleted: completedDeploymentIds.size,
    processed: results.length,
    autoRejected,
    results,
  }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
