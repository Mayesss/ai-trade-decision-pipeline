/**
 * Local bulk research runner. Defaults to the latest research version (v4).
 * Set BULK_RESEARCH_VERSION=v2 only for intentional legacy v2/v3 drains.
 *
 * Usage: npx tsx scripts/research-local-bulk.ts
 *
 * Options (env vars):
 *   BULK_RESEARCH_VERSION=v4      — v4 by default; use v2 for legacy v2/v3
 *   BULK_SYMBOLS_PER_BATCH=4      — v2 only: symbols to process per batch
 *   BULK_CANDIDATE_BATCH_SIZE=100 — candidate rows per research call
 *   BULK_BACKTEST_CONCURRENCY=4   — v2 only: replay concurrency
 *   BULK_TIME_BUDGET_MINUTES=30   — v2 only: time budget per batch
 *   BULK_MAX_BATCHES=100          — max batches before stopping (default: unlimited)
 *   BULK_SHARD_COUNT=4            — v2 only: split symbols across N processes
 *   BULK_SHARD_INDEX=0            — v2 only: shard index for this process
 *   BULK_WORK_LEASES=1            — v2 only: claim candidate rows
 *   BULK_V4_FORCE_VALIDITY=1      — v4 only: override classifier validity
 *   BULK_V4_CANDIDATE_FETCH_LIMIT=1000 — v4 only: candidate scan window before claiming work
 *   BULK_V4_CANDLE_CACHE_SOFT_CAP=16 — v4 only: cached 2-year symbol ranges
 *   BULK_V4_BACKFILL_CANDLES=1    — v4 only: fetch/save missing walkforward candles
 *   BULK_V4_WORK_LEASE_MINUTES=120 — v4 only: candidate claim TTL
 *   BULK_V4_PROGRESS_LOG_SECONDS=60 — v4 only: heartbeat while a candidate is running; 0 disables
 *   BULK_V5_LIMIT=25              — v5 only: deployments evaluated per batch (max 500)
 *   BULK_V5_STALE_OLDER_THAN_HOURS=144 — v5 only: re-evaluate rows whose evidence is older than this
 *   BULK_V5_SHARD_COUNT=1         — v5 only: split deployments across N parallel processes
 *   BULK_V5_SHARD_INDEX=0         — v5 only: which shard this process owns (0..count-1)
 */
import nextEnv from '@next/env';
import os from 'node:os';

import {
  countScalpV2CandidatesByStatus,
  loadScalpV2WarmUpState,
} from '../lib/scalp-v2/db';
import { isScalpPgConfigured } from '../lib/scalp-v2/pg';
import type { ScalpReplayCandle } from '../lib/scalp/replay/types';
import type { ScalpCandle } from '../lib/scalp/types';
import { resolveScalpV2CompletedWeekWindowToUtc } from '../lib/scalp-v2/weekWindows';
import type { ScalpV4ResearchJobResult } from '../lib/scalp-v4';
import type { ScalpV4WalkforwardProgressEvent } from '../lib/scalp-v4/walkforwardSweep';

const { loadEnvConfig } = nextEnv;

// Ensure local scripts pick up .env/.env.local like Next.js runtime.
loadEnvConfig(process.cwd());

// Default the bulk path to Neon's HTTP driver. The sweep does 20–30 min of
// in-process CPU work between DB hits and the socket pool was unreliable
// across compute autosuspend events. Each query is now a stateless HTTPS
// request — no idle sockets to die on us. Override with SCALP_PG_USE_HTTP=0
// to fall back to the socket pool.
if (process.env.SCALP_PG_USE_HTTP === undefined) {
  process.env.SCALP_PG_USE_HTTP = '1';
}

type BulkResearchVersion = 'v2' | 'v4' | 'v5';

function resolveBulkResearchVersion(): BulkResearchVersion {
  const raw = String(process.env.BULK_RESEARCH_VERSION || process.env.SCALP_RESEARCH_VERSION || 'v4')
    .trim()
    .toLowerCase();
  if (raw === 'v5') return 'v5';
  if (raw === 'v2' || raw === 'legacy' || raw === 'legacy_v2') return 'v2';
  return 'v4';
}

const BULK_RESEARCH_VERSION = resolveBulkResearchVersion();

let symbolsPerBatch = Math.max(1, Math.floor(Number(process.env.BULK_SYMBOLS_PER_BATCH) || 4));
let candidateBatchSize = Math.max(
  1,
  Math.floor(Number(process.env.BULK_CANDIDATE_BATCH_SIZE) || 100),
);
const cpuCount = Math.max(1, os.cpus().length || 1);
const defaultBacktestConcurrency = Math.max(1, Math.min(8, Math.floor(cpuCount / 2) || 1));
let backtestConcurrency = Math.max(
  1,
  Math.min(16, Math.floor(Number(process.env.BULK_BACKTEST_CONCURRENCY) || defaultBacktestConcurrency)),
);
let timeBudgetMinutes = Math.max(
  1,
  Math.floor(Number(process.env.BULK_TIME_BUDGET_MINUTES) || 30),
);
const MAX_BATCHES = Math.max(0, Math.floor(Number(process.env.BULK_MAX_BATCHES) || 0));
const MIN_SYMBOLS_PER_BATCH = Math.max(
  1,
  Math.floor(Number(process.env.BULK_MIN_SYMBOLS_PER_BATCH) || 1),
);
const MIN_CANDIDATE_BATCH_SIZE = Math.max(
  1,
  Math.floor(Number(process.env.BULK_MIN_CANDIDATE_BATCH_SIZE) || 20),
);
const MIN_BACKTEST_CONCURRENCY = Math.max(
  1,
  Math.floor(Number(process.env.BULK_MIN_BACKTEST_CONCURRENCY) || 2),
);
const LOW_PROGRESS_STREAK_FOR_BACKOFF = Math.max(
  1,
  Math.floor(Number(process.env.BULK_LOW_PROGRESS_STREAK_FOR_BACKOFF) || 2),
);
const BULK_DEBUG = envBool('BULK_DEBUG', false);
const BULK_SHARD_COUNT = Math.max(
  1,
  Math.min(128, Math.floor(Number(process.env.BULK_SHARD_COUNT) || 1)),
);
const BULK_SHARD_INDEX = Math.max(
  0,
  Math.min(
    BULK_SHARD_COUNT - 1,
    Math.floor(Number(process.env.BULK_SHARD_INDEX) || 0),
  ),
);

function envBool(name: string, fallback: boolean): boolean {
  const raw = String(process.env[name] || '').trim().toLowerCase();
  if (!raw) return fallback;
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
  if (['0', 'false', 'no', 'off'].includes(raw)) return false;
  return fallback;
}

function envNumber(name: string, fallback: number): number {
  const raw = String(process.env[name] || '').trim();
  if (!raw) return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

const DISABLE_TIME_BUDGET = envBool('BULK_DISABLE_TIME_BUDGET', true);
const WORK_LEASES = envBool('BULK_WORK_LEASES', true);
const BULK_V4_FORCE_VALIDITY = envBool('BULK_V4_FORCE_VALIDITY', false);
const BULK_V4_BACKFILL_CANDLES = envBool('BULK_V4_BACKFILL_CANDLES', true);
const WORK_LEASE_MINUTES = Math.max(
  1,
  Math.min(24 * 60, Math.floor(Number(process.env.BULK_WORK_LEASE_MINUTES) || 30)),
);

function applyRuntimeOverrides(): void {
  if (BULK_RESEARCH_VERSION !== 'v2') return;

  process.env.SCALP_V2_RESEARCH_MAX_SYMBOLS_PER_RUN = String(symbolsPerBatch);
  process.env.SCALP_V2_RESEARCH_BATCH_SIZE = String(candidateBatchSize);
  process.env.SCALP_V2_RESEARCH_BACKTEST_CONCURRENCY_MAX = '16';
  process.env.SCALP_V2_RESEARCH_BACKTEST_CONCURRENCY = String(backtestConcurrency);
  if (BULK_DEBUG) {
    process.env.SCALP_V2_RESEARCH_DEBUG_TIMING = '1';
  }
  process.env.SCALP_V2_RESEARCH_WORK_LEASES_ENABLED = WORK_LEASES ? '1' : '0';
  if (WORK_LEASES && !process.env.SCALP_V2_RESEARCH_WORK_LEASE_MS) {
    process.env.SCALP_V2_RESEARCH_WORK_LEASE_MS = String(WORK_LEASE_MINUTES * 60 * 1000);
  }
  process.env.SCALP_V2_RESEARCH_SYMBOL_SHARD_COUNT = String(BULK_SHARD_COUNT);
  process.env.SCALP_V2_RESEARCH_SYMBOL_SHARD_INDEX = String(BULK_SHARD_INDEX);
  if (BULK_SHARD_COUNT > 1 && !WORK_LEASES) {
    process.env.SCALP_V2_RESEARCH_LOCK_SCOPE = `bulk-shard-${BULK_SHARD_INDEX}-of-${BULK_SHARD_COUNT}`;
  } else if (WORK_LEASES) {
    delete process.env.SCALP_V2_RESEARCH_LOCK_SCOPE;
  }
  if (DISABLE_TIME_BUDGET) {
    process.env.SCALP_V2_RESEARCH_DISABLE_TIME_BUDGET = '1';
    delete process.env.SCALP_V2_RESEARCH_TIME_BUDGET_MS;
  } else {
    process.env.SCALP_V2_RESEARCH_DISABLE_TIME_BUDGET = '0';
    process.env.SCALP_V2_RESEARCH_TIME_BUDGET_MS = String(timeBudgetMinutes * 60 * 1000);
  }
  // Prevent lock stealing during long local batches.
  process.env.SCALP_V2_JOB_LOCK_STALE_MINUTES = String(
    DISABLE_TIME_BUDGET ? 120 : Math.max(30, Math.ceil((timeBudgetMinutes * 2) + 5)),
  );
}

// Override env so local runs process more work per batch.
applyRuntimeOverrides();

const globalStart = Date.now();
let totalProcessed = 0;
let totalSucceeded = 0;
let batchCount = 0;
let lastPending = -1;
let lowProgressStreak = 0;

// Persistent across all batches in this process — symbol candle history is
// loaded from Neon at most once per symbol per bulk run instead of every batch.
const persistentCandleCache = new Map<string, ScalpReplayCandle[]>();
const PERSISTENT_CANDLE_CACHE_SOFT_CAP = Math.max(
  10,
  Math.floor(Number(process.env.BULK_CANDLE_CACHE_SOFT_CAP) || 60),
);
const PERSISTENT_V4_CANDLE_CACHE_SOFT_CAP = Math.max(
  1,
  Math.floor(Number(process.env.BULK_V4_CANDLE_CACHE_SOFT_CAP) || 16),
);
const BULK_V4_MIN_CANDLE_COVERAGE_RATIO = Math.max(
  0.1,
  Math.min(1, Number(process.env.BULK_V4_MIN_CANDLE_COVERAGE_RATIO) || 0.65),
);
const BULK_V4_BACKFILL_CHUNK_WEEKS = Math.max(
  1,
  Math.min(26, Math.floor(Number(process.env.BULK_V4_BACKFILL_CHUNK_WEEKS) || 8)),
);
const BULK_V4_BACKFILL_MAX_REQUESTS_PER_CHUNK = Math.max(
  40,
  Math.min(5000, Math.floor(Number(process.env.BULK_V4_BACKFILL_MAX_REQUESTS_PER_CHUNK) || 1200)),
);
const BULK_V4_WORK_LEASE_MINUTES = Math.max(
  5,
  Math.min(24 * 60, Math.floor(Number(process.env.BULK_V4_WORK_LEASE_MINUTES) || 120)),
);
const BULK_V4_CANDIDATE_FETCH_LIMIT = Math.max(
  50,
  Math.min(
    5_000,
    Math.floor(envNumber('BULK_V4_CANDIDATE_FETCH_LIMIT', Math.max(candidateBatchSize * 8, 1000))),
  ),
);
const BULK_V4_PROGRESS_LOG_SECONDS = Math.max(
  0,
  Math.min(10 * 60, Math.floor(envNumber('BULK_V4_PROGRESS_LOG_SECONDS', 60))),
);
const persistentV4CandleCache = new Map<string, ScalpCandle[]>();

let runScalpV2ResearchJob: ((params: {
  batchSize?: number;
  lockScope?: string;
  candleCacheRef?: Map<string, ScalpReplayCandle[]>;
}) => Promise<{
  ok: boolean;
  busy?: boolean;
  processed: number;
  succeeded: number;
  failed: number;
  pendingAfter: number;
  details?: Record<string, unknown>;
}>) | null = null;

let runScalpV4ResearchJob: ((params?: {
  classifierVersion?: string;
  forceValidity?: boolean;
  maxCandidatesPerCall?: number;
  candidateFetchLimit?: number;
  effectiveTrials?: number;
  windowToMs?: number;
  candleCacheRef?: Map<string, ScalpCandle[]>;
  candleCacheSoftCap?: number;
  autoBackfillCandles?: boolean;
  minCandleCoverageRatio?: number;
  candleBackfillChunkWeeks?: number;
  candleBackfillMaxRequestsPerChunk?: number;
  workClaimLeaseMs?: number;
  progressIntervalMs?: number;
  onProgress?: (event: ScalpV4WalkforwardProgressEvent) => void;
}) => Promise<ScalpV4ResearchJobResult>) | null = null;

async function getRunScalpV2ResearchJob() {
  if (!runScalpV2ResearchJob) {
    const mod = await import('../lib/scalp-v2/pipeline');
    runScalpV2ResearchJob = mod.runScalpV2ResearchJob;
  }
  return runScalpV2ResearchJob;
}

async function getRunScalpV4ResearchJob() {
  if (!runScalpV4ResearchJob) {
    const mod = await import('../lib/scalp-v4/research');
    runScalpV4ResearchJob = mod.runScalpV4ResearchJob;
  }
  return runScalpV4ResearchJob;
}

const BULK_V5_LIMIT = Math.max(1, Math.min(500, Math.floor(envNumber('BULK_V5_LIMIT', 25))));
const BULK_V5_STALE_OLDER_THAN_HOURS = Math.max(
  1,
  Math.min(24 * 14, Math.floor(envNumber('BULK_V5_STALE_OLDER_THAN_HOURS', 24 * 6))),
);
// v5 sharding (separate from v2's BULK_SHARD_COUNT, which is a symbol-level
// shard). v5 shards by hash(deployment_id) so each process owns a disjoint
// slice of rows — safe to run N terminals at BULK_V5_SHARD_COUNT=N for true
// parallelism (each is its own Node process, its own CPU core).
const BULK_V5_SHARD_COUNT = Math.max(
  1,
  Math.min(128, Math.floor(envNumber('BULK_V5_SHARD_COUNT', 1))),
);
const BULK_V5_SHARD_INDEX = Math.max(
  0,
  Math.min(
    BULK_V5_SHARD_COUNT - 1,
    Math.floor(envNumber('BULK_V5_SHARD_INDEX', 0)),
  ),
);

let runScalpV5EvaluationBatch: ((params?: {
  limit?: number;
  staleOlderThanMs?: number;
  nowMs?: number;
  shardCount?: number;
  shardIndex?: number;
}) => Promise<import('../lib/scalp-v5/evaluator').ScalpV5BulkResult>) | null = null;

async function getRunScalpV5EvaluationBatch() {
  if (!runScalpV5EvaluationBatch) {
    const mod = await import('../lib/scalp-v5/evaluator');
    runScalpV5EvaluationBatch = mod.runScalpV5EvaluationBatch;
  }
  return runScalpV5EvaluationBatch;
}

function currentWindowToTs(): number {
  return resolveScalpV2CompletedWeekWindowToUtc(Date.now());
}

function summarizeCounts(
  counts: Record<string, number> | null | undefined,
  limit = 5,
): string {
  const entries = Object.entries(counts || {})
    .filter(([, count]) => Number(count) > 0)
    .sort((a, b) => Number(b[1]) - Number(a[1]))
    .slice(0, limit);
  return entries.length > 0
    ? entries.map(([reason, count]) => `${reason}:${count}`).join(', ')
    : 'none';
}

function formatDuration(ms: number | null | undefined): string {
  const totalSeconds = Math.max(0, Math.floor(Number(ms || 0) / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return minutes > 0 ? `${minutes}m${String(seconds).padStart(2, '0')}s` : `${seconds}s`;
}

function logV4Progress(event: ScalpV4WalkforwardProgressEvent): void {
  const prefix = `  progress: ${event.symbol} ${event.phase.replace('candidate_', '')}`;
  const counts = `processed=${event.processed}/${event.maxCandidatesPerCall} eligible=${event.eligible} ineligible=${event.ineligible} skipped=${event.skipped} claimSkipped=${event.claimSkipped}`;
  if (event.phase === 'candidate_start') {
    console.log(`${prefix} venue=${event.venue} cache=${event.candleCacheSize} ${counts}`);
    return;
  }
  if (event.phase === 'candidate_heartbeat') {
    console.log(`${prefix} elapsed=${formatDuration(event.durationMs)} cache=${event.candleCacheSize} ${counts}`);
    return;
  }
  if (event.phase === 'candidate_done') {
    console.log(`${prefix} status=${event.status || 'unknown'} elapsed=${formatDuration(event.durationMs)} ${counts}`);
    return;
  }
  console.log(`${prefix} reason=${event.reason || 'unknown'} cache=${event.candleCacheSize} ${counts}`);
}

async function runBatch(): Promise<boolean> {
  batchCount += 1;
  const batchStart = Date.now();
  applyRuntimeOverrides();
  const batchLabel = BULK_RESEARCH_VERSION === 'v4'
    ? `${candidateBatchSize} v4 candidates`
    : BULK_RESEARCH_VERSION === 'v5'
      ? `up to ${BULK_V5_LIMIT} v5 deployments`
      : `${symbolsPerBatch} symbols`;
  console.log(`\n--- Batch ${batchCount} (${batchLabel}, elapsed ${((Date.now() - globalStart) / 60000).toFixed(1)}m) ---`);

  if (BULK_RESEARCH_VERSION === 'v5') {
    const runEval = await getRunScalpV5EvaluationBatch();
    const result = await runEval({
      limit: BULK_V5_LIMIT,
      staleOlderThanMs: BULK_V5_STALE_OLDER_THAN_HOURS * 60 * 60 * 1000,
      shardCount: BULK_V5_SHARD_COUNT,
      shardIndex: BULK_V5_SHARD_INDEX,
    });
    const elapsed = ((Date.now() - batchStart) / 1000).toFixed(1);
    totalProcessed += result.processed;
    totalSucceeded += result.succeeded;
    const shardSuffix = BULK_V5_SHARD_COUNT > 1
      ? ` shard=${BULK_V5_SHARD_INDEX}/${BULK_V5_SHARD_COUNT}`
      : '';
    console.log(
      `  ${elapsed}s | v5${shardSuffix} processed=${result.processed} succeeded=${result.succeeded} failed=${result.failed} enabled=${result.enabled} disabled=${result.disabled}`,
    );
    console.log(
      `  v5 config: classifier=${result.details.classifierVersion} holdoutWeeks=${result.details.holdoutWeeks} minTradesPerCell=${result.details.minTradesPerCell}`,
    );
    const failureReasons = result.outcomes
      .filter((o) => !o.ok && o.reason)
      .reduce<Record<string, number>>((acc, o) => {
        const reason = String(o.reason || 'unknown').split(':')[0]!;
        acc[reason] = (acc[reason] || 0) + 1;
        return acc;
      }, {});
    if (Object.keys(failureReasons).length > 0) {
      console.log(`  v5 failure reasons: ${summarizeCounts(failureReasons)}`);
    }
    if (BULK_DEBUG) {
      for (const o of result.outcomes) {
        console.log(
          `  v5 ${o.deploymentId} ok=${o.ok}${o.enabled !== undefined ? ` enabled=${o.enabled}` : ''}${o.tradeCount !== undefined ? ` trades=${o.tradeCount}` : ''}${o.eligibleCells && o.eligibleCells.length > 0 ? ` cells=[${o.eligibleCells.join('|')}]` : ''}${o.reason ? ` reason=${o.reason}` : ''} (${formatDuration(o.durationMs)})`,
        );
      }
    }
    if (MAX_BATCHES > 0 && batchCount >= MAX_BATCHES) {
      console.log(`\n  Reached max batches (${MAX_BATCHES}) — stopping.`);
      return false;
    }
    if (result.processed === 0) {
      console.log('\n  No v5 deployments needed evaluation — done for now.');
      return false;
    }
    return true;
  }

  if (BULK_RESEARCH_VERSION === 'v4') {
    const runResearch = await getRunScalpV4ResearchJob();
    const result = await runResearch({
      maxCandidatesPerCall: candidateBatchSize,
      candidateFetchLimit: BULK_V4_CANDIDATE_FETCH_LIMIT,
      forceValidity: BULK_V4_FORCE_VALIDITY,
      candleCacheRef: persistentV4CandleCache,
      candleCacheSoftCap: PERSISTENT_V4_CANDLE_CACHE_SOFT_CAP,
      autoBackfillCandles: BULK_V4_BACKFILL_CANDLES,
      minCandleCoverageRatio: BULK_V4_MIN_CANDLE_COVERAGE_RATIO,
      candleBackfillChunkWeeks: BULK_V4_BACKFILL_CHUNK_WEEKS,
      candleBackfillMaxRequestsPerChunk: BULK_V4_BACKFILL_MAX_REQUESTS_PER_CHUNK,
      workClaimLeaseMs: BULK_V4_WORK_LEASE_MINUTES * 60 * 1000,
      progressIntervalMs: BULK_V4_PROGRESS_LOG_SECONDS * 1000,
      onProgress: BULK_V4_PROGRESS_LOG_SECONDS > 0 ? logV4Progress : undefined,
    });
    const elapsed = ((Date.now() - batchStart) / 1000).toFixed(1);
    totalProcessed += result.processed;
    totalSucceeded += result.succeeded;

    const weekly = result.details.weeklyBuild;
    const weeklyResult = weekly.result;
    const walkforward = result.details.walkforward;
    const validityFailures = weeklyResult?.validityFailures?.length ?? 0;
    const symbolsSaved = weeklyResult?.symbolsSaved ?? 0;
    const symbolsRequested = weeklyResult?.symbolsRequested ?? 0;
    const validityFailureReasons = (weeklyResult?.validityFailures || []).reduce<Record<string, number>>(
      (acc, row) => {
        const reason = String(row.reason || 'unknown');
        acc[reason] = (acc[reason] || 0) + 1;
        return acc;
      },
      {},
    );

    console.log(
      `  ${elapsed}s | v4 processed=${result.processed} eligible=${walkforward.eligible} ineligible=${walkforward.ineligible} skipped=${walkforward.skipped} claimSkipped=${walkforward.claimSkipped}`,
    );
    console.log(
      `  regimes: skipped=${weekly.skipped ? 'yes' : 'no'} reason=${weekly.reason || weeklyResult?.reason || 'none'} saved=${symbolsSaved}/${symbolsRequested} validityFailures=${validityFailures}`,
    );
    if (validityFailures > 0) {
      console.log(`  regime validity reasons: ${summarizeCounts(validityFailureReasons)}`);
    }
    console.log(
      `  walkforward: classifier=${walkforward.classifierVersion} window=${new Date(walkforward.windowFromMs).toISOString().slice(0, 10)}..${new Date(walkforward.windowToMs).toISOString().slice(0, 10)}`,
    );
    console.log(`  walkforward skip reasons: ${summarizeCounts(walkforward.skipReasons)}`);
    console.log(
      `  candleCache: hits=${walkforward.candleCacheHits} misses=${walkforward.candleCacheMisses} size=${walkforward.candleCacheSize}`,
    );
    console.log(
      `  candleBackfill: attempted=${walkforward.candleBackfillsAttempted} succeeded=${walkforward.candleBackfillsSucceeded} fetched=${walkforward.candleBackfilledCandles} coverageFailures=${walkforward.candleCoverageFailures}`,
    );
    if (BULK_DEBUG) {
      console.log(`  debug: details=${JSON.stringify(result.details)}`);
    }
    if (MAX_BATCHES > 0 && batchCount >= MAX_BATCHES) {
      console.log(`\n  Reached max batches (${MAX_BATCHES}) — stopping.`);
      return false;
    }
    if (result.processed === 0) {
      if (validityFailures > 0 && symbolsSaved === 0 && !BULK_V4_FORCE_VALIDITY) {
        console.log(
          '  No regimes were saved because classifier validity failed. For a one-time bootstrap run, retry with BULK_V4_FORCE_VALIDITY=1.',
        );
      }
      console.log('\n  No v4 walkforward candidates processed — done for now.');
      return false;
    }
    return true;
  }

  const runResearch = await getRunScalpV2ResearchJob();
  const result = await runResearch({
    batchSize: candidateBatchSize,
    lockScope:
      BULK_SHARD_COUNT > 1 && !WORK_LEASES
        ? `bulk-shard-${BULK_SHARD_INDEX}-of-${BULK_SHARD_COUNT}`
        : undefined,
    candleCacheRef: persistentCandleCache,
  });
  // Soft cap: if cache grows past the threshold, evict the oldest insertions
  // (Map preserves insertion order). Bulk runs typically stay well under this.
  if (persistentCandleCache.size > PERSISTENT_CANDLE_CACHE_SOFT_CAP) {
    const evictTarget = Math.floor(PERSISTENT_CANDLE_CACHE_SOFT_CAP * 0.8);
    while (persistentCandleCache.size > evictTarget) {
      const oldestKey = persistentCandleCache.keys().next().value;
      if (oldestKey === undefined) break;
      persistentCandleCache.delete(oldestKey);
    }
  }

  const elapsed = ((Date.now() - batchStart) / 1000).toFixed(1);
  totalProcessed += result.processed;
  totalSucceeded += result.succeeded;

  const d = result.details as Record<string, any>;
  const wkEval = d.weeklyEvaluated || d.processedCandidates || 0;
  const wkTotal = d.weeklyTotal || d.totalCandidates || 0;
  const symsRun = d.symbolsThisRun || '?';
  const symsTotal = d.symbolsTotal || '?';
  const shardCount = Number(d.symbolShardCount || BULK_SHARD_COUNT);
  const shardIndex = Number(d.symbolShardIndex || BULK_SHARD_INDEX);
  const stgC = d.stageCPass || 0;
  const stgA = d.stageAPass || 0;
  const stgB = d.stageBPass || 0;
  const budgetHit = Boolean(d.timeBudgetExhausted);
  const pending = result.pendingAfter || 0;
  const reason = d.reason || (budgetHit ? 'time_budget_exhausted' : '');

  const candleHits = Number(d.candleCacheHits || 0);
  const candleMisses = Number(d.candleCacheMisses || 0);
  const candleSize = Number(d.candleCacheSize || persistentCandleCache.size);
  const candleHitRate =
    candleHits + candleMisses > 0
      ? Math.round((candleHits / (candleHits + candleMisses)) * 100)
      : 0;

  console.log(`  ${elapsed}s | processed=${result.processed} succeeded=${result.succeeded} failed=${result.failed} stageC=${stgC}`);
  console.log(`  symbols=${symsRun}/${symsTotal}${shardCount > 1 ? ` shard=${shardIndex}/${shardCount}` : ''} | weekly=${wkEval}/${wkTotal} (${wkTotal > 0 ? Math.round(wkEval / wkTotal * 100) : 0}%)`);
  console.log(`  stageA=${stgA} stageB=${stgB} stageC=${stgC} | budgetHit=${budgetHit ? 'yes' : 'no'}`);
  console.log(`  candleCache: hits=${candleHits} misses=${candleMisses} hitRate=${candleHitRate}% size=${candleSize}`);
  console.log(`  pending=${pending} | reason=${reason}`);
  if (BULK_DEBUG) {
    const deferredByCoverage = Number(d.deferredByCandleCoverage || 0);
    const finalizedCoverageDeferrals = Number(d.finalizedCoverageDeferrals || 0);
    const replayErrors = Number(d.replayErrors || 0);
    const persistErrors = Number(d.persistErrors || 0);
    const droppedBelowMinStage = Number(d.droppedBelowMinStage || 0);
    const persistedCount = Number(d.persistedCount || 0);
    const backtested = Number(d.backtested || 0);
    const deferredToNextRun = Number(d.deferredToNextRun || 0);
    const smartSkippedPersisted = Number(d.smartSkippedPersisted || 0);
    const incrementalStageReplays = Number(d.incrementalStageReplays || 0);
    const newestWeekReplayReuses = Number(d.newestWeekReplayReuses || 0);
    const fullStageReplays = Number(d.fullStageReplays || 0);
    const earlyAbortedStageReplays = Number(d.earlyAbortedStageReplays || 0);
    const cachedStageReuses = Number(d.cachedStageReuses || 0);
    const stageBCacheHits = Number(d.stageBCacheHits || 0);
    const stageCCacheHits = Number(d.stageCCacheHits || 0);
    const leaseClaimsAttempted = Number(d.leaseClaimsAttempted || 0);
    const leaseClaimsSucceeded = Number(d.leaseClaimsSucceeded || 0);
    const leaseClaimsLost = Number(d.leaseClaimsLost || 0);
    const researchWorkLeaseMs = Number(d.researchWorkLeaseMs || process.env.SCALP_V2_RESEARCH_WORK_LEASE_MS || 0);
    const freshnessGate = (d.freshnessGate || {}) as Record<string, unknown>;
    const freshnessApplied = Boolean(freshnessGate.applied);
    const freshnessReady = Boolean(freshnessGate.ready);
    const freshnessStale = Number(freshnessGate.staleCount || 0);
    const freshnessReason = String(freshnessGate.reason || '').trim() || null;
    console.log(
      `  debug: backtested=${backtested} persisted=${persistedCount} smartSkipped=${smartSkippedPersisted} droppedBelowMinStage=${droppedBelowMinStage} deferredToNext=${deferredToNextRun}`,
    );
    console.log(
      `  debug: replay(full=${fullStageReplays}, earlyAbort=${earlyAbortedStageReplays}, incr=${incrementalStageReplays}, newestReuse=${newestWeekReplayReuses}, cacheReuse=${cachedStageReuses}, bCache=${stageBCacheHits}, cCache=${stageCCacheHits}, errors=${replayErrors}) persistErrors=${persistErrors} deferredByCoverage=${deferredByCoverage} finalizedCoverageDeferrals=${finalizedCoverageDeferrals}`,
    );
    console.log(
      `  debug: leases(enabled=${Boolean(d.researchWorkLeasesEnabled)}, leaseMs=${researchWorkLeaseMs}, attempted=${leaseClaimsAttempted}, claimed=${leaseClaimsSucceeded}, lost=${leaseClaimsLost}) scope=${String(d.researchLockScope || '')}`,
    );
    const timing = (d.timing || {}) as Record<string, any>;
    const timingLabels = Array.isArray(timing.labels) ? timing.labels.slice(0, 8) : [];
    if (timingLabels.length > 0) {
      console.log(
        `  debug: timing=${timingLabels.map((row: any) => {
          const label = String(row?.label || '?');
          const totalMs = Number(row?.totalMs || 0);
          const count = Number(row?.count || 0);
          const avgMs = Number(row?.avgMs || 0);
          return `${label}:${totalMs}ms/${count}x(avg ${avgMs}ms)`;
        }).join(' | ')}`,
      );
    }
    if (freshnessApplied) {
      console.log(
        `  debug: freshness ready=${freshnessReady} stale=${freshnessStale} reason=${freshnessReason ?? 'none'}`,
      );
    }
    const policy = (d.policy || {}) as Record<string, unknown>;
    if (Object.keys(policy).length > 0) {
      console.log(
        `  debug: policy=${JSON.stringify(policy)}`,
      );
    }
    if (reason && reason !== 'all_candidates_already_evaluated_this_week') {
      console.log(`  debug: details=${JSON.stringify(d)}`);
    }
  }

  // Auto-backoff when we keep burning batches with almost no forward progress.
  const lowProgress =
    pending > 0 &&
    result.processed <= 1 &&
    stgC === 0 &&
    (lastPending < 0 || pending >= lastPending);
  if (lowProgress) {
    lowProgressStreak += 1;
  } else {
    lowProgressStreak = 0;
  }
  if (lowProgressStreak >= LOW_PROGRESS_STREAK_FOR_BACKOFF) {
    const oldSymbols = symbolsPerBatch;
    const oldBatch = candidateBatchSize;
    const oldConc = backtestConcurrency;
    symbolsPerBatch = Math.max(MIN_SYMBOLS_PER_BATCH, Math.floor(symbolsPerBatch / 2));
    candidateBatchSize = Math.max(MIN_CANDIDATE_BATCH_SIZE, Math.floor(candidateBatchSize / 2));
    backtestConcurrency = Math.max(MIN_BACKTEST_CONCURRENCY, Math.floor(backtestConcurrency / 2));
    lowProgressStreak = 0;
    applyRuntimeOverrides();
    console.log(
      `  Auto-backoff applied: symbols ${oldSymbols}->${symbolsPerBatch}, batch ${oldBatch}->${candidateBatchSize}, concurrency ${oldConc}->${backtestConcurrency}`,
    );
  }
  lastPending = pending;

  if (result.busy) {
    if (MAX_BATCHES > 0 && batchCount >= MAX_BATCHES) {
      console.log(`\n  Reached max batches (${MAX_BATCHES}) while job is locked — stopping.`);
      return false;
    }
    console.log('  Job locked by another process — waiting 30s...');
    await new Promise(r => setTimeout(r, 30_000));
    return true; // retry
  }

  // Stop conditions
  if (reason === 'all_discovered_candidates_currently_leased') {
    console.log('  All discovered candidates are currently leased by workers — waiting 30s...');
    await new Promise(r => setTimeout(r, 30_000));
    return true;
  }
  if (reason === 'warm_up_complete') {
    const windowToTs = currentWindowToTs();
    const warmUpState = await loadScalpV2WarmUpState({ windowToTs }).catch(() => null);
    const discovered = await countScalpV2CandidatesByStatus({
      status: 'discovered',
    }).catch(() => -1);
    console.log(
      `  Warm-up probe: dbHash=${warmUpState?.scopeHash ?? 'null'} warmUpCandidates=${warmUpState?.candidateCount ?? 0} discovered=${discovered >= 0 ? discovered : '?'}`,
    );
    if (!warmUpState) {
      console.log('  Warm-up state not persisted/readable. Stopping to avoid infinite warm-up loop.');
      console.log('  Check DB env loading (.env/.env.local) and that script points to the same DB as cron.');
      return false;
    }
    console.log('  Warm-up completed — continuing to backtest...');
    return true;
  }
  if (reason === 'all_candidates_already_evaluated_this_week') {
    if (pending > 0) {
      console.log(`\n  Current selection exhausted, but ${pending} discovered candidates remain — continuing...`);
      return true;
    }
    console.log('\n  ALL CANDIDATES EVALUATED — done!');
    return false;
  }
  if (pending === 0 && result.processed === 0) {
    console.log('\n  No more work — done!');
    return false;
  }
  if (MAX_BATCHES > 0 && batchCount >= MAX_BATCHES) {
    console.log(`\n  Reached max batches (${MAX_BATCHES}) — stopping.`);
    return false;
  }

  return true; // continue
}

async function main() {
  if (!isScalpPgConfigured()) {
    console.error('Scalp PG is not configured in this shell.');
    console.error('Set DATABASE_URL or SCALP_PG_CONNECTION_STRING, or run with .env/.env.local containing one of them.');
    process.exit(1);
  }

  const warmUpBefore = BULK_RESEARCH_VERSION === 'v2'
    ? await loadScalpV2WarmUpState({
      windowToTs: currentWindowToTs(),
    }).catch(() => null)
    : null;
  const discoveredBefore = BULK_RESEARCH_VERSION === 'v2'
    ? await countScalpV2CandidatesByStatus({
      status: 'discovered',
    }).catch(() => -1)
    : -1;

  console.log(`Local bulk research runner (${BULK_RESEARCH_VERSION})`);
  if (BULK_RESEARCH_VERSION !== 'v5') {
    console.log(`Candidates per batch: ${candidateBatchSize}`);
  }
  if (BULK_RESEARCH_VERSION === 'v5') {
    console.log(`V5 deployments per batch: ${BULK_V5_LIMIT}`);
    console.log(`V5 stale threshold: ${BULK_V5_STALE_OLDER_THAN_HOURS}h`);
    if (BULK_V5_SHARD_COUNT > 1) {
      console.log(`V5 shard: ${BULK_V5_SHARD_INDEX}/${BULK_V5_SHARD_COUNT} (run all ${BULK_V5_SHARD_COUNT} shards in parallel for full coverage)`);
    } else {
      console.log(`V5 shard: 1/1 (single process; set BULK_V5_SHARD_COUNT=N for parallel runs)`);
    }
  } else if (BULK_RESEARCH_VERSION === 'v4') {
    console.log(`V4 candidate fetch limit: ${BULK_V4_CANDIDATE_FETCH_LIMIT}`);
    console.log(`V4 force validity: ${BULK_V4_FORCE_VALIDITY ? 'enabled' : 'disabled'}`);
    console.log(`V4 candle cache soft cap: ${PERSISTENT_V4_CANDLE_CACHE_SOFT_CAP} symbol ranges`);
    console.log(
      `V4 candle backfill: ${BULK_V4_BACKFILL_CANDLES ? 'enabled' : 'disabled'} (minCoverage=${Math.round(BULK_V4_MIN_CANDLE_COVERAGE_RATIO * 100)}%, chunkWeeks=${BULK_V4_BACKFILL_CHUNK_WEEKS})`,
    );
    console.log(`V4 work lease: ${BULK_V4_WORK_LEASE_MINUTES} minutes`);
    console.log(
      `V4 progress logs: ${BULK_V4_PROGRESS_LOG_SECONDS > 0 ? `every ${BULK_V4_PROGRESS_LOG_SECONDS}s` : 'disabled'}`,
    );
  } else {
    console.log(`Symbols per batch: ${symbolsPerBatch}`);
    console.log(`Backtest concurrency: ${backtestConcurrency}`);
    console.log(`Work leases: ${WORK_LEASES ? 'enabled' : 'disabled'}`);
    if (BULK_SHARD_COUNT > 1) {
      console.log(`Shard: ${BULK_SHARD_INDEX}/${BULK_SHARD_COUNT}`);
    }
    console.log(
      `Time budget per batch: ${DISABLE_TIME_BUDGET ? 'disabled (bulk mode)' : `${timeBudgetMinutes} min`}`,
    );
    console.log(
      `Initial warm-up state: dbHash=${warmUpBefore?.scopeHash ?? 'null'} candidates=${warmUpBefore?.candidateCount ?? 0} discovered=${discoveredBefore >= 0 ? discoveredBefore : '?'}`,
    );
  }
  if (MAX_BATCHES > 0) console.log(`Max batches: ${MAX_BATCHES}`);
  console.log('');

  let cont = true;
  while (cont) {
    cont = await runBatch();
  }

  const totalMin = ((Date.now() - globalStart) / 60000).toFixed(1);
  console.log(`\n=== DONE ===`);
  console.log(`Total: ${batchCount} batches, ${totalProcessed} processed, ${totalSucceeded} succeeded`);
  console.log(`Elapsed: ${totalMin} minutes`);
  process.exit(0);
}

main().catch((err) => { console.error(err); process.exit(1); });
