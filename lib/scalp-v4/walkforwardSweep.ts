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
  loadScalpV4IncrementalStates,
  loadScalpV4WalkforwardClusterCounts,
  resolveScalpV4WalkforwardClaimLeaseMs,
  loadScalpV4RegimeSnapshotsBulk,
  upsertScalpV4WalkforwardResult,
} from "./pg";
import { buildScalpV4ClassifierValidityReport } from "./sanity";
import type { ScalpV4IncrementalState, ScalpV4RegimeSnapshot, ScalpV4Venue } from "./types";
import { buildScalpV4ClusterKey } from "./v4Status";
import { runScalpV4WalkForward } from "./walkForward";
import {
  buildEnvelopeFromIncrementalState,
  foldWindowIntoIncrementalState,
} from "./envelope";
import { startOfUtcWeekMondayMs } from "./week";

const WEEK = 7 * 24 * 60 * 60 * 1000;

export interface ScalpV4WalkforwardSweepOptions {
  classifierVersion?: string;
  effectiveTrials?: number;
  windowToMs?: number;
  maxCandidatesPerCall?: number;
  maxWalkforwardsPerCluster?: number;
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
  // Align window_to to Monday 00:00 UTC, matching startOfUtcWeekMondayMs used
  // by the dashboard, regime build, and validity logic. Raw modulo on WEEK
  // would align to Thursday (Unix epoch was a Thursday), causing rollover to
  // happen on Thursdays instead of Mondays and the bulk sweep to treat
  // candidates as "already done this week" until next Thursday.
  const alignedWindowToMs = startOfUtcWeekMondayMs(windowToMsRaw);
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
  // Exact-week match for the "already done this Sunday" check. Cross-week
  // results are handled by the incremental-state lookup below — those
  // candidates get updated, not skipped.
  const completed = await loadScalpV4CompletedWalkforwardDeploymentIds({
    classifierVersion,
    windowFromMs,
    windowToMs: alignedWindowToMs,
    leaseMs: workClaimLeaseMs,
    reuseWeeks: 0,
  });
  // Pre-load incremental state for every candidate deployment we might touch.
  // Saves a per-candidate round-trip. Candidates with state get the fast path
  // (walk only new windows since last sweep + fold into existing state).
  const candidateDeploymentIds = candidates.map((candidate) => {
    const venue = String(candidate.venue || "").toLowerCase() === "capital" ? "capital" : "bitget";
    return resolveScalpDeployment({
      venue,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
    }).deploymentId;
  });
  const incrementalStates = await loadScalpV4IncrementalStates({
    classifierVersion,
    deploymentIds: candidateDeploymentIds,
  });
  // Bulk-load regime snapshots for every unique (venue, symbol) the sweep
  // might touch. Replaces N per-candidate round-trips with a single query
  // — for 200 candidates over ~50 unique symbols, that's ~150 fewer Neon
  // calls per sweep.
  const uniquePairs = Array.from(
    new Map(
      candidates.map((c) => {
        const v = (String(c.venue || "").toLowerCase() === "capital" ? "capital" : "bitget") as ScalpV4Venue;
        return [`${v}:${c.symbol}`, { venue: v, symbol: c.symbol }];
      }),
    ).values(),
  );
  const snapshotsByVenueSymbol = await loadScalpV4RegimeSnapshotsBulk({
    pairs: uniquePairs,
    classifierVersion,
    fromMs: windowFromMs,
    toMs: alignedWindowToMs + WEEK,
  });
  // Cluster cap — same-bet variations (e.g. 26 LINKUSDT/sydney/mdl_basis
  // variants) waste compute. Default cap = 2 walk-forwards per cluster;
  // candidates are pre-sorted by stage-C netR DESC so the top members of
  // each cluster are evaluated first. Disabled when cap = 0.
  const clusterCap = Math.max(
    0,
    Math.floor(
      Number(options.maxWalkforwardsPerCluster ?? process.env.SCALP_V4_WALKFORWARD_MAX_PER_CLUSTER ?? 2),
    ),
  );
  const clusterCounts =
    clusterCap > 0
      ? await loadScalpV4WalkforwardClusterCounts({
          classifierVersion,
          windowFromMs,
          windowToMs: alignedWindowToMs,
          leaseMs: workClaimLeaseMs,
        })
      : new Map<string, number>();
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
    const clusterKey = buildScalpV4ClusterKey({
      venue,
      symbol: candidate.symbol,
      session: candidate.entrySessionProfile,
      tuneId: candidate.tuneId,
      v3TemporalVariantKind:
        (candidate.metadata as Record<string, unknown> | undefined)?.v3TemporalFilter &&
        typeof (candidate.metadata as any).v3TemporalFilter === "object"
          ? ((candidate.metadata as any).v3TemporalFilter.variantKind as string | undefined)
          : null,
    });
    if (clusterCap > 0 && (clusterCounts.get(clusterKey) ?? 0) >= clusterCap) {
      recordSkip("cluster_cap_exceeded");
      continue;
    }
    // Bulk pre-loaded above; in-memory lookup keyed by `${venue}:${symbol}`.
    const snapshots = snapshotsByVenueSymbol.get(`${venue}:${candidate.symbol}`) ?? [];
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
    // Incremental fork: if we already have aggregated state for this
    // deployment, walk only NEW windows (since lastWindowEndMs) and fold
    // their trades into the existing state. Otherwise full 104-week walk.
    const existingEntry = incrementalStates.get(deployment.deploymentId);
    const isIncrementalPath =
      Boolean(existingEntry) &&
      existingEntry!.incrementalState.classifierVersion === classifierVersion &&
      existingEntry!.incrementalState.lastWindowEndMs < alignedWindowToMs;
    // Walk-from-Ms is narrowed for incremental: the earliest NEW 12-week
    // window starts at (lastWindowEndMs + 1 step) - windowSpan.
    const SELECTION_WEEKS = 12;
    const STEP_WEEKS = 1;
    const walkFromMs = isIncrementalPath
      ? Math.max(
          windowFromMs,
          existingEntry!.incrementalState.lastWindowEndMs - (SELECTION_WEEKS - STEP_WEEKS) * WEEK,
        )
      : windowFromMs;
    const run = await (async () => {
      try {
        return await runScalpV4WalkForward({
          classifierVersion,
          snapshots: snapshots as any,
          windowFromMs: walkFromMs,
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
    // Build snapshot + epoch lookups once for trade attribution.
    const snapshotByWeek = new Map<number, ScalpV4RegimeSnapshot>(
      (snapshots as ScalpV4RegimeSnapshot[]).map((row) => [row.weekStartMs, row]),
    );
    const sortedSnaps = [...(snapshots as ScalpV4RegimeSnapshot[])].sort(
      (a, b) => a.weekStartMs - b.weekStartMs,
    );
    const epochByWeek = new Map<number, number>();
    let epochCounter = 0;
    let prevCellId: string | null = null;
    for (const snap of sortedSnaps) {
      if (snap.cellId !== "unknown" && snap.cellId !== prevCellId) {
        epochCounter += 1;
        prevCellId = snap.cellId;
      }
      epochByWeek.set(snap.weekStartMs, epochCounter);
    }
    // Fold all new windows into state (existing for incremental, fresh for full).
    let state: ScalpV4IncrementalState = isIncrementalPath
      ? existingEntry!.incrementalState
      : {
          version: "scalp_v4_incremental_r1",
          classifierVersion,
          windowFromMs: walkFromMs,
          lastWindowEndMs: walkFromMs,
          cells: {},
        };
    // For incremental: ONLY fold windows ending after lastWindowEndMs (skip
    // the overlap windows we re-replayed). For full: fold everything.
    const foldFromMs = isIncrementalPath ? existingEntry!.incrementalState.lastWindowEndMs : -Infinity;
    for (const window of run.windows) {
      if (window.windowEndMs <= foldFromMs) continue;
      state = foldWindowIntoIncrementalState({
        state,
        window,
        snapshotByWeek,
        epochByWeek,
      });
    }
    const finalEnvelope = buildEnvelopeFromIncrementalState({
      state,
      effectiveTrials,
      evaluatedAtMs: Date.now(),
    });
    const newWindowsAppended = run.windows.filter((w) => w.windowEndMs > foldFromMs).length;
    await upsertScalpV4WalkforwardResult({
      candidateId: candidate.id,
      deploymentId: deployment.deploymentId,
      venue: venue as ScalpV4Venue,
      symbol: candidate.symbol,
      strategyId: candidate.strategyId,
      tuneId: candidate.tuneId,
      classifierVersion,
      windowFromMs: state.windowFromMs,
      windowToMs: alignedWindowToMs,
      effectiveTrials,
      status: finalEnvelope.status,
      envelope: finalEnvelope,
      incrementalState: state,
      nextWindowStartMs: alignedWindowToMs,
      windowResults: run.windows.map((row) => ({
        windowStartMs: row.windowStartMs,
        windowEndMs: row.windowEndMs,
        trades: row.trades.length,
        netR: row.trades.reduce((acc, trade) => acc + trade.rMultiple, 0),
      })),
      details: {
        durationMs: Date.now() - startedAt,
        incremental: isIncrementalPath,
        newWindowsAppended,
        totalWindowsAggregated: Object.values(state.cells).reduce(
          (max, cell) => Math.max(max, cell.windowExpectancyR.length),
          0,
        ),
      },
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
    clusterCounts.set(clusterKey, (clusterCounts.get(clusterKey) ?? 0) + 1);
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
