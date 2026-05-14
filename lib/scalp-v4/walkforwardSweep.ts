import { loadScalpCandleHistoryInRange } from "../scalp/candleHistory";
import { resolveScalpDeployment } from "../scalp/deployments";
import { pipSizeForScalpSymbol } from "../scalp/marketData";
import { runScalpReplay } from "../scalp/replay/harness";
import { buildScalpReplayRuntimeFromDeployment } from "../scalp/replay/runtimeConfig";
import type { ScalpReplayCandle } from "../scalp/replay/types";
import { ensureScalpSymbolMarketMetadata } from "../scalp/symbolMarketMetadataSync";
import { scalpPrisma } from "../scalp/pg/client";
import { sql } from "../scalp/pg/sql";
import type { ScalpCandle } from "../scalp/types";
import {
  assessScalpV4CandleCoverage,
  ensureScalpV4CandleCoverage,
} from "./candleCoverage";
import { SCALP_V4_CLASSIFIER_VERSION } from "./classifier";
import {
  claimScalpV4WalkforwardDeployment,
  listScalpV4ResearchCandidates,
  loadScalpV4CompletedWalkforwardDeploymentIds,
  resolveScalpV4WalkforwardClaimLeaseMs,
  loadScalpV4RegimeSnapshots,
  upsertScalpV4WalkforwardResult,
} from "./pg";
import { buildScalpV4ClassifierValidityReport } from "./sanity";
import type { ScalpV4Venue } from "./types";
import { runScalpV4WalkForward } from "./walkForward";

const WEEK = 7 * 24 * 60 * 60 * 1000;

export interface ScalpV4WalkforwardSweepOptions {
  classifierVersion?: string;
  effectiveTrials?: number;
  windowToMs?: number;
  maxCandidatesPerCall?: number;
  candidateFetchLimit?: number;
  forceValidity?: boolean;
  candleCacheRef?: Map<string, ScalpCandle[]>;
  candleCacheSoftCap?: number;
  autoBackfillCandles?: boolean;
  minCandleCoverageRatio?: number;
  candleBackfillChunkWeeks?: number;
  candleBackfillMaxRequestsPerChunk?: number;
  workClaimLeaseMs?: number;
  progressIntervalMs?: number;
  onProgress?: (event: ScalpV4WalkforwardProgressEvent) => void;
}

export interface ScalpV4WalkforwardSweepResult {
  classifierVersion: string;
  windowFromMs: number;
  windowToMs: number;
  processed: number;
  eligible: number;
  ineligible: number;
  skipped: number;
  candleCacheHits: number;
  candleCacheMisses: number;
  candleCacheSize: number;
  candleBackfillsAttempted: number;
  candleBackfillsSucceeded: number;
  candleBackfilledCandles: number;
  candleCoverageFailures: number;
  claimSkipped: number;
  skipReasons: Record<string, number>;
  results: Array<Record<string, unknown>>;
}

export interface ScalpV4WalkforwardProgressEvent {
  phase: "candidate_start" | "candidate_heartbeat" | "candidate_done" | "candidate_skip";
  candidateId: number;
  deploymentId: string;
  venue: ScalpV4Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  processed: number;
  eligible: number;
  ineligible: number;
  skipped: number;
  claimSkipped: number;
  maxCandidatesPerCall: number;
  candidateFetchLimit: number;
  candleCacheSize: number;
  durationMs?: number;
  status?: string;
  reason?: string;
}

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

function normalizeCacheSymbol(symbol: string): string {
  return String(symbol || "").trim().toUpperCase();
}

function evictOldestCandles(
  cache: Map<string, ScalpCandle[]>,
  softCap: number,
): void {
  if (softCap <= 0 || cache.size <= softCap) return;
  const evictTarget = Math.max(1, Math.floor(softCap * 0.8));
  while (cache.size > evictTarget) {
    const oldestKey = cache.keys().next().value;
    if (oldestKey === undefined) break;
    cache.delete(oldestKey);
  }
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
  // Top-N pre-filter: candidates are sorted by stage-C netR DESC in
  // listScalpV4ResearchCandidates, so capping the fetch limit effectively
  // restricts walk-forward to the top-N candidates. With 0% pass rate observed
  // in the bottom half of the stage-C distribution, this lets us focus compute
  // on the candidates most likely to have edge.
  const envTopN = Number(process.env.SCALP_V4_WALKFORWARD_TOP_N);
  const envCandidateFetch = Number(process.env.SCALP_V4_WALKFORWARD_CANDIDATE_FETCH_LIMIT);
  const candidateFetchLimit = Math.max(
    1,
    Math.floor(
      options.candidateFetchLimit ??
        (Number.isFinite(envTopN) && envTopN > 0
          ? envTopN
          : Number.isFinite(envCandidateFetch) && envCandidateFetch > 0
            ? envCandidateFetch
            : Math.max(maxCandidatesPerCall * 4, 50)),
    ),
  );
  const candleCache = options.candleCacheRef ?? new Map<string, ScalpCandle[]>();
  const candleCacheSoftCap = Math.max(
    1,
    Math.floor(
      options.candleCacheSoftCap ??
        Number(process.env.SCALP_V4_WALKFORWARD_CANDLE_CACHE_SOFT_CAP ?? 16),
    ),
  );
  const autoBackfillCandles = options.autoBackfillCandles !== false;
  const minCandleCoverageRatio = Math.max(
    0.1,
    Math.min(
      1,
      Number(
        options.minCandleCoverageRatio ??
          process.env.SCALP_V4_WALKFORWARD_MIN_CANDLE_COVERAGE_RATIO ??
          0.65,
      ),
    ),
  );
  let candleCacheHits = 0;
  let candleCacheMisses = 0;
  let candleBackfillsAttempted = 0;
  let candleBackfillsSucceeded = 0;
  let candleBackfilledCandles = 0;
  let candleCoverageFailures = 0;
  let claimSkipped = 0;
  const workClaimLeaseMs = Math.max(
    5 * 60_000,
    Math.min(24 * 60 * 60_000, Math.floor(Number(options.workClaimLeaseMs) || resolveScalpV4WalkforwardClaimLeaseMs())),
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
      candleCacheHits: 0,
      candleCacheMisses: 0,
      candleCacheSize: candleCache.size,
      candleBackfillsAttempted: 0,
      candleBackfillsSucceeded: 0,
      candleBackfilledCandles: 0,
      candleCoverageFailures: 0,
      claimSkipped: 0,
      skipReasons: {},
      results: [],
    };
  }
  const candidates = await listScalpV4ResearchCandidates({
    limit: candidateFetchLimit,
  });
  const completed = await loadScalpV4CompletedWalkforwardDeploymentIds({
    classifierVersion,
    windowFromMs,
    windowToMs: alignedWindowToMs,
    leaseMs: workClaimLeaseMs,
  });
  const results: Array<Record<string, unknown>> = [];
  let eligible = 0;
  let ineligible = 0;
  let skipped = 0;
  let processed = 0;
  const skipReasons: Record<string, number> = {};
  const recordSkip = (reason: string): void => {
    skipped += 1;
    const key = String(reason || "unknown");
    skipReasons[key] = (skipReasons[key] || 0) + 1;
  };
  const progressIntervalMs = Math.max(
    0,
    Math.min(10 * 60_000, Math.floor(Number(options.progressIntervalMs) || 0)),
  );
  const notifyProgress = (event: ScalpV4WalkforwardProgressEvent): void => {
    if (!options.onProgress) return;
    try {
      options.onProgress(event);
    } catch {
      // Progress logging must never affect research execution.
    }
  };

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
      recordSkip("already_completed_or_claimed");
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
      recordSkip("missing_regime_snapshots");
      results.push({ candidateId: candidate.id, deploymentId: deployment.deploymentId, skipped: true, reason: "missing_regime_snapshots" });
      continue;
    }
    const validity = buildScalpV4ClassifierValidityReport({ snapshots: snapshots as any });
    if (!validity.passed && !options.forceValidity) {
      recordSkip(`classifier_validity_failed:${validity.reason}`);
      results.push({
        candidateId: candidate.id,
        deploymentId: deployment.deploymentId,
        skipped: true,
        reason: `classifier_validity_failed:${validity.reason}`,
      });
      continue;
    }
    const claimed = await claimScalpV4WalkforwardDeployment({
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
      leaseMs: workClaimLeaseMs,
    });
    if (!claimed) {
      recordSkip("claim_skipped");
      claimSkipped += 1;
      continue;
    }
    const candleCacheKey = `${normalizeCacheSymbol(candidate.symbol)}:1m:${windowFromMs}:${alignedWindowToMs}`;
    let candles = candleCache.get(candleCacheKey);
    if (candles) {
      candleCacheHits += 1;
    } else {
      candleCacheMisses += 1;
      const history = await loadScalpCandleHistoryInRange(
        candidate.symbol,
        "1m",
        windowFromMs,
        alignedWindowToMs,
      );
      candles = (history.record?.candles || []) as ScalpCandle[];
      candleCache.set(candleCacheKey, candles);
      evictOldestCandles(candleCache, candleCacheSoftCap);
    }
    const coverage = assessScalpV4CandleCoverage({
      candles,
      fromMs: windowFromMs,
      toMs: alignedWindowToMs,
      minCoverageRatio: minCandleCoverageRatio,
    });
    if (!coverage.ok && autoBackfillCandles) {
      candleBackfillsAttempted += 1;
      const ensured = await ensureScalpV4CandleCoverage({
        venue: venue as ScalpV4Venue,
        symbol: candidate.symbol,
        fromMs: windowFromMs,
        toMs: alignedWindowToMs,
        existingCandles: candles,
        minCoverageRatio: minCandleCoverageRatio,
        chunkWeeks: options.candleBackfillChunkWeeks,
        maxRequestsPerChunk: options.candleBackfillMaxRequestsPerChunk,
      });
      candles = ensured.candles;
      candleBackfilledCandles += ensured.fetchedCandles;
      if (ensured.coverage.ok) candleBackfillsSucceeded += 1;
      candleCache.set(candleCacheKey, candles);
      evictOldestCandles(candleCache, candleCacheSoftCap);
    }
    const finalCoverage = assessScalpV4CandleCoverage({
      candles,
      fromMs: windowFromMs,
      toMs: alignedWindowToMs,
      minCoverageRatio: minCandleCoverageRatio,
    });
    if (!finalCoverage.ok) {
      candleCoverageFailures += 1;
      recordSkip(finalCoverage.reason || "insufficient_candle_coverage");
      notifyProgress({
        phase: "candidate_skip",
        candidateId: Number(candidate.id),
        deploymentId: deployment.deploymentId,
        venue: venue as ScalpV4Venue,
        symbol: candidate.symbol,
        strategyId: candidate.strategyId,
        tuneId: candidate.tuneId,
        processed,
        eligible,
        ineligible,
        skipped,
        claimSkipped,
        maxCandidatesPerCall,
        candidateFetchLimit,
        candleCacheSize: candleCache.size,
        reason: finalCoverage.reason || "insufficient_candle_coverage",
      });
      results.push({
        candidateId: candidate.id,
        deploymentId: deployment.deploymentId,
        skipped: true,
        reason: finalCoverage.reason || "insufficient_candle_coverage",
        candleCoverage: finalCoverage,
      });
      continue;
    }
    const meta = await ensureScalpSymbolMarketMetadata(candidate.symbol, { fetchIfMissing: true });
    const runtime = buildScalpReplayRuntimeFromDeployment({ deployment, configOverride: null });
    const startedAt = Date.now();
    notifyProgress({
      phase: "candidate_start",
      candidateId: Number(candidate.id),
      deploymentId: deployment.deploymentId,
      venue: venue as ScalpV4Venue,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
      processed,
      eligible,
      ineligible,
      skipped,
      claimSkipped,
      maxCandidatesPerCall,
      candidateFetchLimit,
      candleCacheSize: candleCache.size,
    });
    const fullReplayCandles = toReplayCandles(candles, runtime.defaultSpreadPips);
    let heartbeat: ReturnType<typeof setInterval> | null = null;
    if (progressIntervalMs > 0) {
      heartbeat = setInterval(() => {
        notifyProgress({
          phase: "candidate_heartbeat",
          candidateId: Number(candidate.id),
          deploymentId: deployment.deploymentId,
          venue: venue as ScalpV4Venue,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          processed,
          eligible,
          ineligible,
          skipped,
          claimSkipped,
          maxCandidatesPerCall,
          candidateFetchLimit,
          candleCacheSize: candleCache.size,
          durationMs: Date.now() - startedAt,
        });
      }, progressIntervalMs);
    }
    const run = await (async () => {
      try {
        return await runScalpV4WalkForward({
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
      } finally {
        if (heartbeat) clearInterval(heartbeat);
      }
    })();
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
    notifyProgress({
      phase: "candidate_done",
      candidateId: Number(candidate.id),
      deploymentId: deployment.deploymentId,
      venue: venue as ScalpV4Venue,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
      processed,
      eligible,
      ineligible,
      skipped,
      claimSkipped,
      maxCandidatesPerCall,
      candidateFetchLimit,
      candleCacheSize: candleCache.size,
      durationMs: Date.now() - startedAt,
      status: run.envelope.status,
    });
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
    candleCacheHits,
    candleCacheMisses,
    candleCacheSize: candleCache.size,
    candleBackfillsAttempted,
    candleBackfillsSucceeded,
    candleBackfilledCandles,
    candleCoverageFailures,
    claimSkipped,
    skipReasons,
    results,
  };
}
