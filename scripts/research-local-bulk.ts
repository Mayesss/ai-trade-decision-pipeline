/**
 * Local bulk research runner — processes ALL remaining discovered candidates
 * without Vercel's time/memory limits. Run once to bootstrap the first week,
 * then let the cron handle subsequent weeks incrementally.
 *
 * Usage: npx tsx scripts/research-local-bulk.ts
 *
 * Options (env vars):
 *   BULK_SYMBOLS_PER_BATCH=4      — symbols to process per batch (default 4)
 *   BULK_CANDIDATE_BATCH_SIZE=100 — candidate rows per research call (default 100)
 *   BULK_BACKTEST_CONCURRENCY=4   — replay concurrency (default half CPU cores, max 8)
 *   BULK_TIME_BUDGET_MINUTES=30   — time budget per batch (default 30)
 *   BULK_MAX_BATCHES=100          — max batches before stopping (default: unlimited)
 */
import nextEnv from '@next/env';
import os from 'node:os';

import {
  countScalpV2CandidatesByStatus,
  loadScalpV2WarmUpState,
} from '../lib/scalp-v2/db';
import { isScalpPgConfigured } from '../lib/scalp-v2/pg';
import { resolveScalpV2CompletedWeekWindowToUtc } from '../lib/scalp-v2/weekWindows';

const { loadEnvConfig } = nextEnv;

// Ensure local scripts pick up .env/.env.local like Next.js runtime.
loadEnvConfig(process.cwd());

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

function envBool(name: string, fallback: boolean): boolean {
  const raw = String(process.env[name] || '').trim().toLowerCase();
  if (!raw) return fallback;
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
  if (['0', 'false', 'no', 'off'].includes(raw)) return false;
  return fallback;
}

const DISABLE_TIME_BUDGET = envBool('BULK_DISABLE_TIME_BUDGET', true);

function applyRuntimeOverrides(): void {
  process.env.SCALP_V2_RESEARCH_MAX_SYMBOLS_PER_RUN = String(symbolsPerBatch);
  process.env.SCALP_V2_RESEARCH_BATCH_SIZE = String(candidateBatchSize);
  process.env.SCALP_V2_RESEARCH_BACKTEST_CONCURRENCY_MAX = '16';
  process.env.SCALP_V2_RESEARCH_BACKTEST_CONCURRENCY = String(backtestConcurrency);
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

let runScalpV2ResearchJob: ((params: { batchSize?: number }) => Promise<{
  ok: boolean;
  busy?: boolean;
  processed: number;
  succeeded: number;
  pendingAfter: number;
  details?: Record<string, unknown>;
}>) | null = null;

async function getRunScalpV2ResearchJob() {
  if (!runScalpV2ResearchJob) {
    const mod = await import('../lib/scalp-v2/pipeline');
    runScalpV2ResearchJob = mod.runScalpV2ResearchJob;
  }
  return runScalpV2ResearchJob;
}

function currentWindowToTs(): number {
  return resolveScalpV2CompletedWeekWindowToUtc(Date.now());
}

async function runBatch(): Promise<boolean> {
  batchCount += 1;
  const batchStart = Date.now();
  applyRuntimeOverrides();
  console.log(`\n--- Batch ${batchCount} (${symbolsPerBatch} symbols, elapsed ${((Date.now() - globalStart) / 60000).toFixed(1)}m) ---`);

  const runResearch = await getRunScalpV2ResearchJob();
  const result = await runResearch({ batchSize: candidateBatchSize });

  const elapsed = ((Date.now() - batchStart) / 1000).toFixed(1);
  totalProcessed += result.processed;
  totalSucceeded += result.succeeded;

  const d = result.details as Record<string, any>;
  const wkEval = d.weeklyEvaluated || d.processedCandidates || 0;
  const wkTotal = d.weeklyTotal || d.totalCandidates || 0;
  const symsRun = d.symbolsThisRun || '?';
  const symsTotal = d.symbolsTotal || '?';
  const stgC = d.stageCPass || 0;
  const stgA = d.stageAPass || 0;
  const stgB = d.stageBPass || 0;
  const budgetHit = Boolean(d.timeBudgetExhausted);
  const pending = result.pendingAfter || 0;
  const reason = d.reason || (budgetHit ? 'time_budget_exhausted' : '');

  console.log(`  ${elapsed}s | processed=${result.processed} succeeded=${result.succeeded} stageC=${stgC}`);
  console.log(`  symbols=${symsRun}/${symsTotal} | weekly=${wkEval}/${wkTotal} (${wkTotal > 0 ? Math.round(wkEval / wkTotal * 100) : 0}%)`);
  console.log(`  stageA=${stgA} stageB=${stgB} stageC=${stgC} | budgetHit=${budgetHit ? 'yes' : 'no'}`);
  console.log(`  pending=${pending} | reason=${reason}`);
  if (BULK_DEBUG) {
    const deferredByCoverage = Number(d.deferredByCandleCoverage || 0);
    const replayErrors = Number(d.replayErrors || 0);
    const droppedBelowMinStage = Number(d.droppedBelowMinStage || 0);
    const persistedCount = Number(d.persistedCount || 0);
    const backtested = Number(d.backtested || 0);
    const deferredToNextRun = Number(d.deferredToNextRun || 0);
    const incrementalStageReplays = Number(d.incrementalStageReplays || 0);
    const fullStageReplays = Number(d.fullStageReplays || 0);
    const cachedStageReuses = Number(d.cachedStageReuses || 0);
    const freshnessGate = (d.freshnessGate || {}) as Record<string, unknown>;
    const freshnessApplied = Boolean(freshnessGate.applied);
    const freshnessReady = Boolean(freshnessGate.ready);
    const freshnessStale = Number(freshnessGate.staleCount || 0);
    const freshnessReason = String(freshnessGate.reason || '').trim() || null;
    console.log(
      `  debug: backtested=${backtested} persisted=${persistedCount} droppedBelowMinStage=${droppedBelowMinStage} deferredToNext=${deferredToNextRun}`,
    );
    console.log(
      `  debug: replay(full=${fullStageReplays}, incr=${incrementalStageReplays}, cacheReuse=${cachedStageReuses}, errors=${replayErrors}) deferredByCoverage=${deferredByCoverage}`,
    );
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

  const warmUpBefore = await loadScalpV2WarmUpState({
    windowToTs: currentWindowToTs(),
  }).catch(() => null);
  const discoveredBefore = await countScalpV2CandidatesByStatus({
    status: 'discovered',
  }).catch(() => -1);

  console.log(`Local bulk research runner`);
  console.log(`Symbols per batch: ${symbolsPerBatch}`);
  console.log(`Candidates per batch: ${candidateBatchSize}`);
  console.log(`Backtest concurrency: ${backtestConcurrency}`);
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

  const totalMin = ((Date.now() - globalStart) / 60000).toFixed(1);
  console.log(`\n=== DONE ===`);
  console.log(`Total: ${batchCount} batches, ${totalProcessed} processed, ${totalSucceeded} succeeded`);
  console.log(`Elapsed: ${totalMin} minutes`);
  process.exit(0);
}

main().catch((err) => { console.error(err); process.exit(1); });
