/**
 * Local bulk research runner for the composer (session-structure) research path.
 *
 * Usage: npx tsx scripts/research-local-bulk.ts
 *
 * Options (env vars):
 *   BULK_SYMBOLS_PER_BATCH=4      — symbols to process per batch
 *   BULK_CANDIDATE_BATCH_SIZE=100 — candidate rows per research call
 *   BULK_BACKTEST_CONCURRENCY=4   — replay concurrency
 *   BULK_TIME_BUDGET_MINUTES=30   — time budget per batch
 *   BULK_MAX_BATCHES=100          — max batches before stopping (default: unlimited)
 *   BULK_SHARD_COUNT=4            — split symbols across N processes
 *   BULK_SHARD_INDEX=0            — shard index for this process
 *   BULK_WORK_LEASES=1            — claim candidate rows
 *   BULK_RUN_DAY_ROBUSTNESS=1     — run day finalist robustness after full bulk drain
 *   BULK_DAY_ROBUSTNESS_BATCH_SIZE=120 — finalist robustness rows per batch
 *   BULK_DAY_ROBUSTNESS_MAX_BATCHES=0 — 0 means run until robustness queue is empty
 */
import nextEnv from '@next/env';
import os from 'node:os';

import {
  claimScalpComposerJob,
  countScalpComposerCandidatesByStatus,
  finalizeScalpComposerJob,
  heartbeatScalpComposerJob,
  loadScalpComposerWarmUpState,
} from '../lib/scalp/composer/db';
import { isScalpPgConfigured } from '../lib/scalp/composer/pg';
import type { ScalpReplayCandle } from '../lib/scalp/replay/types';
import { resolveScalpComposerCompletedWeekWindowToUtc } from '../lib/scalp/composer/weekWindows';

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
const WORK_LEASE_MINUTES = Math.max(
  1,
  Math.min(24 * 60, Math.floor(Number(process.env.BULK_WORK_LEASE_MINUTES) || 30)),
);

function applyRuntimeOverrides(): void {
  process.env.SCALP_COMPOSER_RESEARCH_MAX_SYMBOLS_PER_RUN = String(symbolsPerBatch);
  process.env.SCALP_COMPOSER_RESEARCH_BATCH_SIZE = String(candidateBatchSize);
  process.env.SCALP_COMPOSER_RESEARCH_BACKTEST_CONCURRENCY_MAX = '16';
  process.env.SCALP_COMPOSER_RESEARCH_BACKTEST_CONCURRENCY = String(backtestConcurrency);
  if (BULK_DEBUG) {
    process.env.SCALP_COMPOSER_RESEARCH_DEBUG_TIMING = '1';
  }
  process.env.SCALP_COMPOSER_RESEARCH_WORK_LEASES_ENABLED = WORK_LEASES ? '1' : '0';
  if (WORK_LEASES && !process.env.SCALP_COMPOSER_RESEARCH_WORK_LEASE_MS) {
    process.env.SCALP_COMPOSER_RESEARCH_WORK_LEASE_MS = String(WORK_LEASE_MINUTES * 60 * 1000);
  }
  process.env.SCALP_COMPOSER_RESEARCH_SYMBOL_SHARD_COUNT = String(BULK_SHARD_COUNT);
  process.env.SCALP_COMPOSER_RESEARCH_SYMBOL_SHARD_INDEX = String(BULK_SHARD_INDEX);
  if (BULK_SHARD_COUNT > 1 && !WORK_LEASES) {
    process.env.SCALP_COMPOSER_RESEARCH_LOCK_SCOPE = `bulk-shard-${BULK_SHARD_INDEX}-of-${BULK_SHARD_COUNT}`;
  } else if (WORK_LEASES) {
    delete process.env.SCALP_COMPOSER_RESEARCH_LOCK_SCOPE;
  }
  if (DISABLE_TIME_BUDGET) {
    process.env.SCALP_COMPOSER_RESEARCH_DISABLE_TIME_BUDGET = '1';
    delete process.env.SCALP_COMPOSER_RESEARCH_TIME_BUDGET_MS;
  } else {
    process.env.SCALP_COMPOSER_RESEARCH_DISABLE_TIME_BUDGET = '0';
    process.env.SCALP_COMPOSER_RESEARCH_TIME_BUDGET_MS = String(timeBudgetMinutes * 60 * 1000);
  }
  // Prevent lock stealing during long local batches.
  process.env.SCALP_COMPOSER_JOB_LOCK_STALE_MINUTES = String(
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
let bulkDrained = false;

// Persistent across all batches in this process — symbol candle history is
// loaded from Neon at most once per symbol per bulk run instead of every batch.
const persistentCandleCache = new Map<string, ScalpReplayCandle[]>();
const PERSISTENT_CANDLE_CACHE_SOFT_CAP = Math.max(
  10,
  Math.floor(Number(process.env.BULK_CANDLE_CACHE_SOFT_CAP) || 60),
);

let runScalpComposerResearchJob: ((params: {
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

async function getRunScalpComposerResearchJob() {
  if (!runScalpComposerResearchJob) {
    const mod = await import('../lib/scalp/composer/pipeline');
    runScalpComposerResearchJob = mod.runScalpComposerResearchJob;
  }
  return runScalpComposerResearchJob;
}

let dayRobustnessModule: typeof import('../lib/scalp/composer/dayRobustness') | null = null;

async function getDayRobustnessModule() {
  if (!dayRobustnessModule) {
    dayRobustnessModule = await import('../lib/scalp/composer/dayRobustness');
  }
  return dayRobustnessModule;
}

function currentWindowToTs(): number {
  return resolveScalpComposerCompletedWeekWindowToUtc(Date.now());
}

async function runPostBulkDayRobustnessOnce(): Promise<void> {
  if (!envBool('BULK_RUN_DAY_ROBUSTNESS', true)) {
    console.log('\nDay finalist robustness: disabled by BULK_RUN_DAY_ROBUSTNESS=0.');
    return;
  }

  const mod = await getDayRobustnessModule();
  const policy = mod.resolveDayRobustnessPolicy();
  if (!policy.enabled) {
    console.log('\nDay finalist robustness: disabled by SCALP_DAY_ROBUSTNESS_ENABLED=0.');
    return;
  }

  const windowToTs = currentWindowToTs();
  const dedupeScope = String(windowToTs);
  const owner = `bulk-day-robustness:${process.pid}:${Date.now().toString(36)}`;
  const claimed = await claimScalpComposerJob({
    jobKind: 'robustness',
    lockOwner: owner,
    dedupeScope,
    allowSucceededRetry: false,
  });
  if (!claimed) {
    console.log('\nDay finalist robustness: already running or completed for this weekly window by another bulk process.');
    return;
  }

  const limit = Math.max(
    1,
    Math.min(
      5_000,
      Math.floor(envNumber('BULK_DAY_ROBUSTNESS_BATCH_SIZE', envNumber('DAY_ROBUSTNESS_BATCH_SIZE', policy.maxCandidates))),
    ),
  );
  const maxBatches = Math.max(
    0,
    Math.min(
      10_000,
      Math.floor(envNumber('BULK_DAY_ROBUSTNESS_MAX_BATCHES', envNumber('DAY_ROBUSTNESS_MAX_BATCHES', 0))),
    ),
  );
  const extended = envBool('BULK_DAY_ROBUSTNESS_EXTENDED', envBool('DAY_ROBUSTNESS_EXTENDED', false));
  const includeFailed = envBool('BULK_DAY_ROBUSTNESS_INCLUDE_FAILED', envBool('DAY_ROBUSTNESS_INCLUDE_FAILED', false));

  let selected = 0;
  let processed = 0;
  let passed = 0;
  let failed = 0;
  let errors = 0;
  let batches = 0;
  const startedAt = Date.now();

  console.log(
    `\nDay finalist robustness: claimed singleton window=${new Date(windowToTs).toISOString()} batchSize=${limit} maxBatches=${maxBatches || 'until-empty'} weeks=${extended ? policy.extendedWeeks : policy.weeks}`,
  );

  try {
    while (maxBatches <= 0 || batches < maxBatches) {
      batches += 1;
      console.log(`\n--- Robustness batch ${batches} ---`);
      const result = await mod.runDayRobustnessBatch({
        windowToTs,
        limit,
        extended,
        includeFailed,
        lockOwner: owner,
        onProgress(event) {
          const phase = String(event.phase || '');
          if (phase !== 'candidate_done' && phase !== 'candidate_error') return;
          const item = Number(event.processed || 0);
          const total = Number(event.total || 0);
          if (phase === 'candidate_done') {
            console.log(
              `  ${item}/${total} ${event.symbol} passed=${event.passed} trades=${event.trades} netR=${Number(event.netR || 0).toFixed(2)}`,
            );
            return;
          }
          console.log(`  ${item}/${total} ${event.symbol} error=${event.error || 'unknown'}`);
        },
      });

      selected += result.selected;
      processed += result.processed;
      passed += result.passed;
      failed += result.failed;
      errors += result.errors;

      await heartbeatScalpComposerJob({
        jobKind: 'robustness',
        lockOwner: owner,
        dedupeScope,
        details: {
          phase: 'running',
          windowToTs,
          batches,
          selected,
          processed,
          passed,
          failed,
          errors,
          updatedAt: new Date().toISOString(),
        },
      });

      console.log(
        `  robustness batch done selected=${result.selected} processed=${result.processed} passed=${result.passed} failed=${result.failed} errors=${result.errors}`,
      );
      if (result.selected === 0) break;
    }

    await finalizeScalpComposerJob({
      jobKind: 'robustness',
      lockOwner: owner,
      dedupeScope,
      ok: errors <= 0,
      details: {
        phase: 'complete',
        windowToTs,
        batches,
        selected,
        processed,
        passed,
        failed,
        errors,
        durationMs: Date.now() - startedAt,
        completedAt: new Date().toISOString(),
      },
    });

    console.log(
      `\nDay finalist robustness complete: batches=${batches} selected=${selected} processed=${processed} passed=${passed} failed=${failed} errors=${errors}`,
    );
  } catch (err) {
    await finalizeScalpComposerJob({
      jobKind: 'robustness',
      lockOwner: owner,
      dedupeScope,
      ok: false,
      details: {
        phase: 'failed',
        windowToTs,
        batches,
        selected,
        processed,
        passed,
        failed,
        errors,
        error: err instanceof Error ? err.message : String(err),
        failedAt: new Date().toISOString(),
      },
    });
    throw err;
  }
}

async function runBatch(): Promise<boolean> {
  batchCount += 1;
  const batchStart = Date.now();
  applyRuntimeOverrides();
  console.log(`\n--- Batch ${batchCount} (${symbolsPerBatch} symbols, elapsed ${((Date.now() - globalStart) / 60000).toFixed(1)}m) ---`);

  const runResearch = await getRunScalpComposerResearchJob();
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
    const researchWorkLeaseMs = Number(d.researchWorkLeaseMs || process.env.SCALP_COMPOSER_RESEARCH_WORK_LEASE_MS || 0);
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
    const warmUpState = await loadScalpComposerWarmUpState({ windowToTs }).catch(() => null);
    const discovered = await countScalpComposerCandidatesByStatus({
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
    bulkDrained = true;
    console.log('\n  ALL CANDIDATES EVALUATED — done!');
    return false;
  }
  if (pending === 0 && result.processed === 0) {
    bulkDrained = true;
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

  const warmUpBefore = await loadScalpComposerWarmUpState({
    windowToTs: currentWindowToTs(),
  }).catch(() => null);
  const discoveredBefore = await countScalpComposerCandidatesByStatus({
    status: 'discovered',
  }).catch(() => -1);

  console.log('Local bulk research runner (composer)');
  console.log(`Candidates per batch: ${candidateBatchSize}`);
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
  if (MAX_BATCHES > 0) console.log(`Max batches: ${MAX_BATCHES}`);
  console.log('');

  let cont = true;
  while (cont) {
    cont = await runBatch();
  }

  if (bulkDrained) {
    await runPostBulkDayRobustnessOnce();
  } else if (envBool('BULK_RUN_DAY_ROBUSTNESS', true)) {
    console.log('\nDay finalist robustness skipped: bulk did not fully drain in this process.');
  }

  const totalMin = ((Date.now() - globalStart) / 60000).toFixed(1);
  console.log(`\n=== DONE ===`);
  console.log(`Total: ${batchCount} batches, ${totalProcessed} processed, ${totalSucceeded} succeeded`);
  console.log(`Elapsed: ${totalMin} minutes`);
  process.exit(0);
}

main().catch((err) => { console.error(err); process.exit(1); });
