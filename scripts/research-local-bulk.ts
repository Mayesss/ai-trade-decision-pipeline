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

const SYMBOLS_PER_BATCH = Math.max(1, Math.floor(Number(process.env.BULK_SYMBOLS_PER_BATCH) || 4));
const CANDIDATE_BATCH_SIZE = Math.max(
  1,
  Math.floor(Number(process.env.BULK_CANDIDATE_BATCH_SIZE) || 100),
);
const cpuCount = Math.max(1, os.cpus().length || 1);
const defaultBacktestConcurrency = Math.max(1, Math.min(8, Math.floor(cpuCount / 2) || 1));
const BACKTEST_CONCURRENCY = Math.max(
  1,
  Math.min(16, Math.floor(Number(process.env.BULK_BACKTEST_CONCURRENCY) || defaultBacktestConcurrency)),
);
const TIME_BUDGET_MINUTES = Math.max(
  1,
  Math.floor(Number(process.env.BULK_TIME_BUDGET_MINUTES) || 30),
);
const MAX_BATCHES = Math.max(0, Math.floor(Number(process.env.BULK_MAX_BATCHES) || 0));

// Override env so local runs process more work per batch.
process.env.SCALP_V2_RESEARCH_MAX_SYMBOLS_PER_RUN = String(SYMBOLS_PER_BATCH);
process.env.SCALP_V2_RESEARCH_BATCH_SIZE = String(CANDIDATE_BATCH_SIZE);
process.env.SCALP_V2_RESEARCH_BACKTEST_CONCURRENCY_MAX = '16';
process.env.SCALP_V2_RESEARCH_BACKTEST_CONCURRENCY = String(BACKTEST_CONCURRENCY);
process.env.SCALP_V2_RESEARCH_TIME_BUDGET_MS = String(TIME_BUDGET_MINUTES * 60 * 1000);

const globalStart = Date.now();
let totalProcessed = 0;
let totalSucceeded = 0;
let batchCount = 0;

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
  console.log(`\n--- Batch ${batchCount} (${SYMBOLS_PER_BATCH} symbols, elapsed ${((Date.now() - globalStart) / 60000).toFixed(1)}m) ---`);

  const runResearch = await getRunScalpV2ResearchJob();
  const result = await runResearch({ batchSize: CANDIDATE_BATCH_SIZE });

  const elapsed = ((Date.now() - batchStart) / 1000).toFixed(1);
  totalProcessed += result.processed;
  totalSucceeded += result.succeeded;

  const d = result.details as Record<string, any>;
  const wkEval = d.weeklyEvaluated || d.processedCandidates || 0;
  const wkTotal = d.weeklyTotal || d.totalCandidates || 0;
  const symsRun = d.symbolsThisRun || '?';
  const symsTotal = d.symbolsTotal || '?';
  const stgC = d.stageCPass || 0;
  const pending = result.pendingAfter || 0;
  const reason = d.reason || '';

  console.log(`  ${elapsed}s | processed=${result.processed} succeeded=${result.succeeded} stageC=${stgC}`);
  console.log(`  symbols=${symsRun}/${symsTotal} | weekly=${wkEval}/${wkTotal} (${wkTotal > 0 ? Math.round(wkEval / wkTotal * 100) : 0}%)`);
  console.log(`  pending=${pending} | reason=${reason}`);

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
  console.log(`Symbols per batch: ${SYMBOLS_PER_BATCH}`);
  console.log(`Candidates per batch: ${CANDIDATE_BATCH_SIZE}`);
  console.log(`Backtest concurrency: ${BACKTEST_CONCURRENCY}`);
  console.log(`Time budget per batch: ${TIME_BUDGET_MINUTES} min`);
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
