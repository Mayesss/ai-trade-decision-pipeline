#!/usr/bin/env node
import nextEnv from "@next/env";

import { isScalpPgConfigured } from "../lib/scalp-v2/pg";
import {
  loadScalpV4DeploymentSymbols,
  runScalpV4WeeklyRegimeBuild,
} from "../lib/scalp-v4";
import { runScalpV5CandlePreflight } from "../lib/scalp-v5/candlePreflight";
import { runScalpV5EvaluationBatch } from "../lib/scalp-v5/evaluator";
import {
  autoPromoteScalpV5WinnersToEnabled,
  cullBottomPerformersScalpV5Deployments,
  getScalpV5EvaluationQueueStats,
  invalidateAllScalpV5Evidence,
  refillScalpV5DeploymentsFromStageCRankedCandidates,
  retireConsistentlyFailingScalpV5Deployments,
} from "../lib/scalp-v5/pg";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

if (process.env.SCALP_PG_USE_HTTP === undefined) {
  process.env.SCALP_PG_USE_HTTP = "1";
}

type Args = Record<string, string | boolean>;

function parseArgs(argv: string[]): Args {
  const out: Args = {};
  for (let idx = 0; idx < argv.length; idx += 1) {
    const token = argv[idx]!;
    if (!token.startsWith("--")) continue;
    const eqIdx = token.indexOf("=");
    if (eqIdx > 2) {
      out[token.slice(2, eqIdx)] = token.slice(eqIdx + 1);
      continue;
    }
    const key = token.slice(2);
    const next = argv[idx + 1];
    if (!next || next.startsWith("--")) out[key] = true;
    else {
      out[key] = next;
      idx += 1;
    }
  }
  return out;
}

function boolArg(args: Args, key: string, fallback: boolean): boolean {
  const value = args[key];
  if (value === undefined) return fallback;
  if (typeof value === "boolean") return value;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

function intArg(args: Args, key: string, fallback: number, min: number, max: number): number {
  const raw = args[key];
  const parsed = Math.floor(Number(raw));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function numArg(args: Args, key: string, fallback: number, min: number, max: number): number {
  const parsed = Number(args[key]);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function summarizeFailureReasons(outcomes: Array<{ ok: boolean; reason?: string }>): Record<string, number> {
  const out: Record<string, number> = {};
  for (const row of outcomes) {
    if (row.ok) continue;
    const reason = String(row.reason || "unknown").split(":")[0] || "unknown";
    out[reason] = (out[reason] || 0) + 1;
  }
  return out;
}

function compactDeploymentResult<T extends {
  deploymentIds?: string[];
  retired?: number;
  dryRun?: boolean;
  [key: string]: unknown;
}>(value: T): Omit<T, "deploymentIds"> & { deploymentIdsCount: number; sampleDeploymentIds: string[] } {
  const deploymentIds = Array.isArray(value.deploymentIds) ? value.deploymentIds : [];
  const { deploymentIds: _deploymentIds, ...rest } = value;
  void _deploymentIds;
  return {
    ...rest,
    deploymentIdsCount: deploymentIds.length,
    sampleDeploymentIds: deploymentIds.slice(0, 25),
  };
}

async function drainV5Evaluation(params: {
  label: string;
  evalLimit: number;
  staleOlderThanMs: number;
  maxEvalBatches: number;
  deploymentIds?: string[];
}): Promise<{
  label: string;
  batches: number;
  processed: number;
  succeeded: number;
  failed: number;
  enabled: number;
  disabled: number;
  fullCount: number;
  incrementalCount: number;
  failureReasons: Record<string, number>;
}> {
  const totals = {
    label: params.label,
    batches: 0,
    processed: 0,
    succeeded: 0,
    failed: 0,
    enabled: 0,
    disabled: 0,
    fullCount: 0,
    incrementalCount: 0,
    failureReasons: {} as Record<string, number>,
  };
  while (params.maxEvalBatches <= 0 || totals.batches < params.maxEvalBatches) {
    totals.batches += 1;
    const result = await runScalpV5EvaluationBatch({
      limit: params.evalLimit,
      staleOlderThanMs: params.staleOlderThanMs,
      preflightCandles: false,
      deploymentIds: params.deploymentIds,
    });
    totals.processed += result.processed;
    totals.succeeded += result.succeeded;
    totals.failed += result.failed;
    totals.enabled += result.enabled;
    totals.disabled += result.disabled;
    totals.fullCount += result.fullCount;
    totals.incrementalCount += result.incrementalCount;
    const failures = summarizeFailureReasons(result.outcomes);
    for (const [reason, count] of Object.entries(failures)) {
      totals.failureReasons[reason] = (totals.failureReasons[reason] || 0) + count;
    }
    console.log(
      `[${params.label}] batch=${totals.batches} processed=${result.processed} succeeded=${result.succeeded} failed=${result.failed} full=${result.fullCount} incremental=${result.incrementalCount}`,
    );
    if (result.processed === 0) {
      totals.batches -= 1;
      break;
    }
  }
  return totals;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dryRun = boolArg(args, "dryRun", false);
  const forceWeekday = boolArg(args, "forceWeekday", false);
  const cleanup = boolArg(args, "cleanup", true);
  const targetNewSeats = intArg(args, "targetNewSeats", 500, 0, 10_000);
  const evalLimit = intArg(args, "evalLimit", 200, 1, 500);
  const maxEvalBatches = intArg(args, "maxEvalBatches", 0, 0, 10_000);
  const preflightMaxRounds = intArg(args, "preflightMaxRounds", 10, 0, 100);
  const minStageCNetR = numArg(args, "minStageCNetR", 4, -1_000, 1_000);
  const minStageCTrades = intArg(args, "minStageCTrades", 30, 0, 100_000);
  const staleOlderThanHours = intArg(args, "staleOlderThanHours", 24 * 6, 1, 24 * 14);
  const staleOlderThanMs = staleOlderThanHours * 60 * 60_000;
  const utcDay = new Date().getUTCDay();

  if (!isScalpPgConfigured()) {
    throw new Error("scalp_pg_not_configured");
  }
  if (utcDay !== 0 && !forceWeekday && !dryRun) {
    throw new Error(`not_sunday_utc:${utcDay}; pass --forceWeekday to override`);
  }

  console.log(
    `scalp-v5-sunday dryRun=${dryRun} targetNewSeats=${targetNewSeats} evalLimit=${evalLimit} maxEvalBatches=${maxEvalBatches || "unlimited"}`,
  );

  const summary: Record<string, unknown> = {
    ok: true,
    dryRun,
    utcDay,
    params: {
      targetNewSeats,
      evalLimit,
      maxEvalBatches,
      preflightMaxRounds,
      minStageCNetR,
      minStageCTrades,
      cleanup,
      staleOlderThanHours,
    },
  };

  summary.queueBefore = await getScalpV5EvaluationQueueStats({ staleOlderThanMs });

  if (dryRun) {
    summary.preflight = { skipped: true, reason: "dry_run" };
    summary.regimes = { skipped: true, reason: "dry_run" };
    summary.rollover = { skipped: true, reason: "dry_run" };
  } else {
    let preflight = null as Awaited<ReturnType<typeof runScalpV5CandlePreflight>> | null;
    for (let round = 1; round <= preflightMaxRounds; round += 1) {
      preflight = await runScalpV5CandlePreflight({
        batchSize: 200,
        maxAttempts: 10,
        auditTrigger: "local_v5_sunday",
      });
      console.log(
        `[preflight] round=${round} ready=${preflight.ready} checked=${preflight.checked} staleBefore=${preflight.staleBefore.length} staleAfter=${preflight.staleAfter.length}`,
      );
      if (preflight.ready) break;
    }
    summary.preflight = preflight;
    if (!preflight?.ready) {
      summary.ok = false;
      summary.reason = "v5_candle_preflight_not_ready";
      console.log(JSON.stringify(summary, null, 2));
      process.exit(1);
    }

    const regimeSymbols = await loadScalpV4DeploymentSymbols();
    const regimes = await runScalpV4WeeklyRegimeBuild({
      symbols: regimeSymbols,
      forceValidity: true,
    });
    summary.regimes = {
      symbolsRequested: regimeSymbols.length,
      symbolsSaved: regimes.symbolsSaved,
      validityFailures: regimes.validityFailures.length,
    };
    console.log(`[regimes] saved=${regimes.symbolsSaved}/${regimeSymbols.length} validityFailures=${regimes.validityFailures.length}`);

    summary.rollover = await invalidateAllScalpV5Evidence({ mode: "stale" });
    console.log(`[rollover] ${JSON.stringify(summary.rollover)}`);
  }

  summary.evaluation = dryRun
    ? { skipped: true, reason: "dry_run" }
    : await drainV5Evaluation({
        label: "v5-main-eval",
        evalLimit,
        staleOlderThanMs,
        maxEvalBatches,
      });

  summary.firstPromote = await autoPromoteScalpV5WinnersToEnabled({ dryRun });
  console.log(`[promote:first] ${JSON.stringify(summary.firstPromote)}`);

  if (cleanup) {
    const trimTail = await retireConsistentlyFailingScalpV5Deployments({
      stalenessDays: 28,
      dryRun,
    });
    const cullBottom = await cullBottomPerformersScalpV5Deployments({
      percentToRetire: 0.15,
      graceDays: 28,
      minTrades: 30,
      minPoolSize: 1500,
      maxRetireAbs: 500,
      dryRun,
    });
    summary.trimTail = compactDeploymentResult(trimTail);
    summary.cullBottom = compactDeploymentResult(cullBottom);
    console.log(`[cleanup] trim=${trimTail.retired} cull=${cullBottom.retired}`);
  } else {
    summary.trimTail = { skipped: true, reason: "cleanup_disabled" };
    summary.cullBottom = { skipped: true, reason: "cleanup_disabled" };
  }

  const refill = await refillScalpV5DeploymentsFromStageCRankedCandidates({
    targetNewSeats,
    minStageCNetR,
    minStageCTrades,
    dryRun,
  });
  summary.refill = {
    dryRun: refill.dryRun,
    selected: refill.selected,
    upserted: refill.upserted,
    sampleDeploymentIds: refill.deploymentIds.slice(0, 25),
  };
  console.log(`[refill] selected=${refill.selected} upserted=${refill.upserted}`);

  summary.refillEvaluation =
    dryRun || refill.deploymentIds.length === 0
      ? { skipped: true, reason: dryRun ? "dry_run" : "no_refilled_deployments" }
      : await drainV5Evaluation({
          label: "v5-refill-eval",
          evalLimit,
          staleOlderThanMs,
          maxEvalBatches,
          deploymentIds: refill.deploymentIds,
        });

  summary.finalPromote = await autoPromoteScalpV5WinnersToEnabled({ dryRun });
  summary.queueAfter = await getScalpV5EvaluationQueueStats({ staleOlderThanMs });

  console.log("\n=== scalp-v5-sunday summary ===");
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
