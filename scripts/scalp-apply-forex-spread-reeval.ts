#!/usr/bin/env node
// Apply: full v5 re-evaluation of the allowed Capital (forex) deployments
// through the fee/spread-patched harness, persisting spread-adjusted evidence
// and enabled flags. Because today's stored holdoutToMs != holdoutToMs - 1w,
// the evaluator's incremental path is disabled and each runs a full 12-week
// replay (which now charges the round-trip bid/ask spread). See
// scalp-rescore-forex-spread.ts for the read-only measurement this applies.
//
// Usage:
//   node scripts/with-db-env.mjs node --import tsx scripts/scalp-apply-forex-spread-reeval.ts

import nextEnv from "@next/env";

import { isScalpPgConfigured, scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";
import { runScalpResearchEvaluationBatch } from "../lib/scalp/research/evaluator";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

if (process.env.SCALP_PG_USE_HTTP === undefined) {
  process.env.SCALP_PG_USE_HTTP = "1";
}

async function main() {
  if (!isScalpPgConfigured()) throw new Error("scalp_pg_not_configured");
  const db = scalpPrisma();

  const rows = await db.$queryRaw<Array<{ deploymentId: string; symbol: string; enabled: boolean }>>(sql`
    SELECT deployment_id AS "deploymentId", symbol, enabled
    FROM scalp_v2_deployments
    WHERE venue = 'capital' AND live_mode = 'live' AND retired_at IS NULL
    ORDER BY symbol
  `);
  const deploymentIds = rows.map((r) => r.deploymentId);
  const before = new Map(rows.map((r) => [r.deploymentId, r.enabled]));

  console.log(`Re-evaluating ${deploymentIds.length} Capital deployments (full, spread-charged)...`);
  console.log(`  ${rows.map((r) => r.symbol).join(", ")}`);

  const result = await runScalpResearchEvaluationBatch({
    deploymentIds,
    preflightCandles: false,
    nowMs: Date.now(),
  });

  console.log("");
  console.log(
    `processed=${result.processed} succeeded=${result.succeeded} failed=${result.failed} ` +
      `full=${result.fullCount} incremental=${result.incrementalCount} ` +
      `enabled=${result.enabled} disabled=${result.disabled}`,
  );
  console.log("");
  console.log("  SYMBOL    MODE         WAS_ENABLED  NOW_ENABLED  ELIGIBLE_CELLS");
  console.log("  " + "-".repeat(72));
  const bySymbol = new Map(rows.map((r) => [r.deploymentId, r.symbol]));
  for (const o of result.outcomes) {
    const symbol = bySymbol.get(o.deploymentId) || o.deploymentId;
    if (!o.ok) {
      console.log(`  ${symbol.padEnd(9)} FAILED       ${o.reason ?? ""}`);
      continue;
    }
    console.log(
      "  " +
        symbol.padEnd(9) +
        String(o.mode ?? "?").padEnd(13) +
        String(before.get(o.deploymentId)).padEnd(13) +
        String(o.enabled).padEnd(13) +
        (o.eligibleCells?.length ? o.eligibleCells.join(", ") : "(none)"),
    );
  }
  console.log("");

  await db.$disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
