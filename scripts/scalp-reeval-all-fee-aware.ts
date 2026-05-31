#!/usr/bin/env node
// Re-evaluate ALL candidate-backed v5 deployments (not just enabled/live ones)
// through the now-deployed fee/spread-aware harness, then optionally auto-promote
// winners. Use after deploying the cost fix so the whole universe is re-selected
// fee-aware (the Sunday pipeline otherwise skips fresh rows via advancement gating).
//
// Steps:
//   1. (optional) invalidateAllScalpV5Evidence({mode:'full'}) — nulls evidence +
//      checkpoint + v5_enabled so every row re-runs a FULL 12-week replay.
//   2. Loop runScalpV5EvaluationBatch until the queue drains (re-costs each row).
//   3. (optional) autoPromoteScalpV5WinnersToEnabled — sets `enabled` (live) for
//      rows that clear the promotion thresholds on the fee-adjusted evidence.
//
// Flags:
//   --invalidate <full|stale|none>  default full
//   --evalLimit <n>                 default 300 (max 500) rows per claim
//   --maxBatches <n>                default 0 = until drained
//   --autopromote                   run auto-promotion after eval (default off)
//   --autopromoteDryRun             preview promotions without writing
//   --measureOnly <n>               eval only n rows then stop (timing probe)
//
// Usage:
//   node scripts/with-db-env.mjs node --import tsx scripts/scalp-reeval-all-fee-aware.ts --measureOnly 20
//   node scripts/with-db-env.mjs node --import tsx scripts/scalp-reeval-all-fee-aware.ts --autopromote

import nextEnv from "@next/env";

import { isScalpPgConfigured, scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";
import { runScalpV5EvaluationBatch } from "../lib/scalp-v5/evaluator";
import {
  autoPromoteScalpV5WinnersToEnabled,
  invalidateAllScalpV5Evidence,
} from "../lib/scalp-v5/pg";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

if (process.env.SCALP_PG_USE_HTTP === undefined) {
  process.env.SCALP_PG_USE_HTTP = "1";
}

function arg(name: string): string | undefined {
  const i = process.argv.indexOf(name);
  return i === -1 ? undefined : process.argv[i + 1];
}
function flag(name: string): boolean {
  return process.argv.includes(name);
}
function intArg(name: string, fallback: number): number {
  const v = Number(arg(name));
  return Number.isFinite(v) ? Math.floor(v) : fallback;
}

async function candidateCount(db: ReturnType<typeof scalpPrisma>): Promise<number> {
  const rows = await db.$queryRaw<Array<{ n: bigint }>>(sql`
    SELECT COUNT(*)::bigint AS n FROM scalp_v2_deployments
    WHERE candidate_id IS NOT NULL AND retired_at IS NULL
  `);
  return Number(rows[0]?.n ?? 0);
}

async function main() {
  if (!isScalpPgConfigured()) throw new Error("scalp_pg_not_configured");
  const db = scalpPrisma();

  const invalidate = (arg("--invalidate") || "full").toLowerCase();
  const evalLimit = Math.max(1, Math.min(500, intArg("--evalLimit", 300)));
  const maxBatches = intArg("--maxBatches", 0);
  const measureOnly = intArg("--measureOnly", 0);
  const doAutopromote = flag("--autopromote");
  const autopromoteDryRun = flag("--autopromoteDryRun");
  // After invalidation v5_evaluated_at IS NULL, so any positive staleness picks
  // every row. Keep it tiny so even freshly-evaluated rows are re-claimed.
  const staleOlderThanMs = 60_000;
  const t0 = Date.now();

  console.log(`scalp-reeval-all-fee-aware — ${new Date().toISOString()}`);
  console.log(`  candidates: ${await candidateCount(db)}  evalLimit=${evalLimit} invalidate=${invalidate} measureOnly=${measureOnly || "off"}`);

  const invalidateOnly = flag("--invalidateOnly");
  if (!measureOnly && (invalidate === "full" || invalidate === "stale")) {
    const inv = await invalidateAllScalpV5Evidence({
      mode: invalidate === "full" ? "full" : "stale",
    });
    console.log(`  invalidated ${inv.invalidated} rows (mode=${inv.mode})`);
  }
  if (invalidateOnly) {
    console.log("  --invalidateOnly: done (no evaluation)");
    await db.$disconnect();
    return;
  }

  let batches = 0;
  let totalProcessed = 0;
  let totalSucceeded = 0;
  let totalFailed = 0;
  let totalEnabled = 0;
  let totalFull = 0;
  const failureReasons = new Map<string, number>();

  while (true) {
    if (maxBatches > 0 && batches >= maxBatches) break;
    if (measureOnly > 0 && totalProcessed >= measureOnly) break;
    const limit = measureOnly > 0 ? Math.min(evalLimit, measureOnly - totalProcessed) : evalLimit;

    const tb = Date.now();
    const result = await runScalpV5EvaluationBatch({
      limit,
      staleOlderThanMs,
      preflightCandles: false,
    });
    batches += 1;
    if (result.processed === 0) {
      console.log(`  [batch ${batches}] 0 rows — queue drained`);
      break;
    }
    totalProcessed += result.processed;
    totalSucceeded += result.succeeded;
    totalFailed += result.failed;
    totalEnabled += result.enabled;
    totalFull += result.fullCount;
    for (const o of result.outcomes) {
      if (!o.ok && o.reason) {
        const key = o.reason.split(":")[0]!;
        failureReasons.set(key, (failureReasons.get(key) || 0) + 1);
      }
    }
    const secs = (Date.now() - tb) / 1000;
    console.log(
      `  [batch ${batches}] processed=${result.processed} ok=${result.succeeded} fail=${result.failed} ` +
        `full=${result.fullCount} v5enabled=${result.enabled} ${secs.toFixed(1)}s ` +
        `(${(secs / Math.max(1, result.processed)).toFixed(2)}s/row) cumulative=${totalProcessed}`,
    );
  }

  const mins = (Date.now() - t0) / 60_000;
  console.log("");
  console.log(
    `Eval done: processed=${totalProcessed} ok=${totalSucceeded} fail=${totalFailed} ` +
      `full=${totalFull} v5enabled=${totalEnabled} in ${mins.toFixed(1)}m`,
  );
  if (failureReasons.size) {
    console.log("  failures: " + [...failureReasons.entries()].map(([k, v]) => `${k}=${v}`).join(" "));
  }

  if (doAutopromote && !measureOnly) {
    console.log("");
    console.log(`Auto-promotion (dryRun=${autopromoteDryRun})...`);
    const promo = await autoPromoteScalpV5WinnersToEnabled({ dryRun: autopromoteDryRun });
    console.log(
      `  promoted=${promo.promoted} candidates=${promo.funnel.candidates} qualified=${promo.funnel.qualified} ` +
        `shortlisted=${promo.funnel.shortlisted}`,
    );
    console.log(
      `  funnel: failedTotalNetR=${promo.funnel.failedTotalNetR} failedTrades=${promo.funnel.failedTotalTrades} ` +
        `failedPositiveWeeks=${promo.funnel.failedPositiveWeeks} failedWorstWeek=${promo.funnel.failedWorstWeek} ` +
        `failedTrailing4w=${promo.funnel.failedTrailing4wNetR}`,
    );
  }

  await db.$disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
