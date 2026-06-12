#!/usr/bin/env node
// Read-only diagnostic: are Sunday's evaluations actually NULLed on
// scalp_v2_deployments, or is the UI just showing a windowing artifact?
//
// Runs a handful of SELECT-only counts against the same column the
// dashboard reads (v5_evaluated_at). Writes nothing.

import nextEnv from "@next/env";

import { isScalpPgConfigured, scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";
import { startOfUtcWeekMondayMs } from "../lib/scalp/regimes/week";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

if (process.env.SCALP_PG_USE_HTTP === undefined) {
  process.env.SCALP_PG_USE_HTTP = "1";
}

const ONE_HOUR_MS = 60 * 60_000;
const ONE_DAY_MS = 24 * ONE_HOUR_MS;

async function main() {
  if (!isScalpPgConfigured()) {
    throw new Error("scalp_pg_not_configured");
  }
  const db = scalpPrisma();
  const nowMs = Date.now();
  const weekStartMs = startOfUtcWeekMondayMs(nowMs);
  const shiftedWeekStartMs = weekStartMs - ONE_DAY_MS;
  const last24h = new Date(nowMs - ONE_DAY_MS);
  const last48h = new Date(nowMs - 2 * ONE_DAY_MS);
  const last72h = new Date(nowMs - 3 * ONE_DAY_MS);
  const last7d = new Date(nowMs - 7 * ONE_DAY_MS);
  const weekStart = new Date(weekStartMs);
  const shiftedWeekStart = new Date(shiftedWeekStartMs);

  const rows = await db.$queryRaw<Array<{
    total: bigint;
    enabled: bigint;
    evaluated: bigint;
    missing: bigint;
    evaluatedThisWeekMondayBoundary: bigint;
    evaluatedSinceLastSunday: bigint;
    evaluatedLast24h: bigint;
    evaluatedLast48h: bigint;
    evaluatedLast72h: bigint;
    evaluatedLast7d: bigint;
    latestEvaluatedAt: Date | null;
    oldestEvaluatedAt: Date | null;
  }>>(sql`
    SELECT
      COUNT(*)::bigint AS total,
      COUNT(*) FILTER (WHERE enabled = TRUE)::bigint AS enabled,
      COUNT(*) FILTER (WHERE v5_evaluated_at IS NOT NULL)::bigint AS evaluated,
      COUNT(*) FILTER (WHERE v5_evaluated_at IS NULL)::bigint AS missing,
      COUNT(*) FILTER (WHERE v5_evaluated_at >= ${weekStart})::bigint AS "evaluatedThisWeekMondayBoundary",
      COUNT(*) FILTER (WHERE v5_evaluated_at >= ${shiftedWeekStart})::bigint AS "evaluatedSinceLastSunday",
      COUNT(*) FILTER (WHERE v5_evaluated_at >= ${last24h})::bigint AS "evaluatedLast24h",
      COUNT(*) FILTER (WHERE v5_evaluated_at >= ${last48h})::bigint AS "evaluatedLast48h",
      COUNT(*) FILTER (WHERE v5_evaluated_at >= ${last72h})::bigint AS "evaluatedLast72h",
      COUNT(*) FILTER (WHERE v5_evaluated_at >= ${last7d})::bigint AS "evaluatedLast7d",
      MAX(v5_evaluated_at) AS "latestEvaluatedAt",
      MIN(v5_evaluated_at) AS "oldestEvaluatedAt"
    FROM scalp_v2_deployments d
    WHERE d.candidate_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM scalp_v2_candidates c
        WHERE c.id = d.candidate_id
          AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
      );
  `);

  // Retired-at lookup is optional: if the migration for the permanent-ban
  // column hasn't been applied yet, this query throws (column does not
  // exist). Treat the absence as retired=null so the diagnostic still
  // reports the main numbers.
  let retiredTotal: number | null = null;
  try {
    const retiredRows = await db.$queryRaw<Array<{ retiredCount: bigint }>>(sql`
      SELECT COUNT(*)::bigint AS "retiredCount"
      FROM scalp_v2_deployments
      WHERE retired_at IS NOT NULL;
    `);
    retiredTotal = Number(retiredRows[0]?.retiredCount ?? 0);
  } catch {
    retiredTotal = null;
  }

  const r = rows[0]!;
  const num = (b: bigint | null | undefined) =>
    b === null || b === undefined ? 0 : Number(b);

  const result = {
    nowMs,
    nowIso: new Date(nowMs).toISOString(),
    weekStartUTC: weekStart.toISOString(),
    shiftedWeekStartUTC: shiftedWeekStart.toISOString(),
    counts: {
      total: num(r.total),
      enabled: num(r.enabled),
      evaluated_NotNull: num(r.evaluated),
      missing_IsNull: num(r.missing),
      evaluatedThisWeekMondayBoundary: num(r.evaluatedThisWeekMondayBoundary),
      evaluatedSinceLastSunday: num(r.evaluatedSinceLastSunday),
      evaluatedLast24h: num(r.evaluatedLast24h),
      evaluatedLast48h: num(r.evaluatedLast48h),
      evaluatedLast72h: num(r.evaluatedLast72h),
      evaluatedLast7d: num(r.evaluatedLast7d),
    },
    timestamps: {
      latestEvaluatedAt: r.latestEvaluatedAt?.toISOString() ?? null,
      latestEvaluatedAgoHours: r.latestEvaluatedAt
        ? Math.round(((nowMs - r.latestEvaluatedAt.getTime()) / ONE_HOUR_MS) * 10) / 10
        : null,
      oldestEvaluatedAt: r.oldestEvaluatedAt?.toISOString() ?? null,
      oldestEvaluatedAgoHours: r.oldestEvaluatedAt
        ? Math.round(((nowMs - r.oldestEvaluatedAt.getTime()) / ONE_HOUR_MS) * 10) / 10
        : null,
    },
    retired: {
      // Total rows with retired_at IS NOT NULL across the whole table.
      // `null` here means the migration that adds the retired_at column
      // hasn't been applied yet (column missing → query errored, treated
      // as not-applicable rather than zero).
      total: retiredTotal,
    },
    diagnosis: {
      // If missing_IsNull is high (say >100) AND evaluatedLast24h is low,
      // evidence was actually wiped. Otherwise this is a UI windowing
      // artifact and the shifted week boundary fixes it.
      likelyCause: null as string | null,
    },
  };

  if (result.counts.missing_IsNull > 100) {
    result.diagnosis.likelyCause = `missing_IsNull=${result.counts.missing_IsNull} — evidence was actually NULLed; not a UI display issue`;
  } else if (result.counts.evaluatedThisWeekMondayBoundary < result.counts.evaluated_NotNull / 2) {
    result.diagnosis.likelyCause = `evidence intact; UI's "this week" counter reset at Monday 00:00 UTC. evaluatedSinceLastSunday=${result.counts.evaluatedSinceLastSunday} is the honest number.`;
  } else {
    result.diagnosis.likelyCause = "no obvious problem detected";
  }

  console.log(JSON.stringify(result, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
