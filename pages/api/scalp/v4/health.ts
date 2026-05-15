export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import {
  isScalpV4Enabled,
  isScalpV4HardGateEnabled,
  SCALP_V4_CLASSIFIER_VERSION,
  startOfUtcWeekMondayMs,
} from "../../../../lib/scalp-v4";
import { scalpPrisma } from "../../../../lib/scalp/pg/client";
import { sql } from "../../../../lib/scalp/pg/sql";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const db = scalpPrisma();
    const classifierVersion = SCALP_V4_CLASSIFIER_VERSION;
    const nowMs = Date.now();
    const currentWeekStartMs = startOfUtcWeekMondayMs(nowMs);
    const previousWeekStartMs = currentWeekStartMs - 7 * 24 * 60 * 60_000;

    const [
      stageCRows,
      walkforwardCountRows,
      regimeRows,
      throughputRows,
      sparklineRows,
      currentWeekWalkRows,
      previousWeekDeployRows,
      newSurvivorsRows,
      deploymentSymbolRows,
      regimesThisWeekRows,
    ] = await Promise.all([
      db.$queryRaw<Array<{ total: bigint }>>(sql`
        SELECT COUNT(*)::bigint AS total
        FROM scalp_v2_candidates
        WHERE (metadata_json->'worker'->'stageC'->>'passed')::boolean = true
           OR (metadata_json->'worker'->>'finalPass')::boolean = true;
      `),
      db.$queryRaw<Array<{ status: string; count: bigint }>>(sql`
        SELECT status, COUNT(*)::bigint AS count
        FROM scalp_regime_walkforward_results
        WHERE classifier_version = ${classifierVersion}
        GROUP BY status;
      `),
      db.$queryRaw<Array<{ symbols: bigint; weekstart: Date | null }>>(sql`
        SELECT COUNT(DISTINCT venue || ':' || symbol)::bigint AS symbols,
               MAX(week_start) AS weekstart
        FROM scalp_regime_snapshots
        WHERE classifier_version = ${classifierVersion};
      `),
      // Last 1h throughput (completed walk-forwards, excluding still-running).
      db.$queryRaw<Array<{ n: bigint }>>(sql`
        SELECT COUNT(*)::bigint AS n
        FROM scalp_regime_walkforward_results
        WHERE classifier_version = ${classifierVersion}
          AND status <> 'in_progress'
          AND evaluated_at > NOW() - INTERVAL '1 hour';
      `),
      // 12 hourly buckets, most recent first (bucket 0 = last hour).
      db.$queryRaw<Array<{ bucket: number; n: bigint }>>(sql`
        SELECT
          FLOOR(EXTRACT(EPOCH FROM (NOW() - evaluated_at)) / 3600)::int AS bucket,
          COUNT(*)::bigint AS n
        FROM scalp_regime_walkforward_results
        WHERE classifier_version = ${classifierVersion}
          AND status <> 'in_progress'
          AND evaluated_at > NOW() - INTERVAL '12 hour'
        GROUP BY 1;
      `),
      // Rollover progress: distinct deployments walked for THIS week's window.
      db.$queryRaw<Array<{ n: bigint }>>(sql`
        SELECT COUNT(DISTINCT deployment_id)::bigint AS n
        FROM scalp_regime_walkforward_results
        WHERE classifier_version = ${classifierVersion}
          AND window_to = ${new Date(currentWeekStartMs)};
      `),
      // Reference: distinct deployments walked for PREVIOUS week (the target
      // for the current rollover sweep to catch up to).
      db.$queryRaw<Array<{ n: bigint }>>(sql`
        SELECT COUNT(DISTINCT deployment_id)::bigint AS n
        FROM scalp_regime_walkforward_results
        WHERE classifier_version = ${classifierVersion}
          AND window_to = ${new Date(previousWeekStartMs)};
      `),
      // New stage-C survivors discovered this week.
      db.$queryRaw<Array<{ discovered: bigint; walked: bigint }>>(sql`
        WITH new_candidates AS (
          SELECT id
          FROM scalp_v2_candidates
          WHERE created_at >= ${new Date(currentWeekStartMs)}
            AND (
              (metadata_json->'worker'->'stageC'->>'passed')::boolean = true
              OR (metadata_json->'worker'->>'finalPass')::boolean = true
            )
        )
        SELECT
          (SELECT COUNT(*)::bigint FROM new_candidates) AS discovered,
          (
            SELECT COUNT(*)::bigint
            FROM scalp_regime_walkforward_results w
            WHERE w.classifier_version = ${classifierVersion}
              AND w.window_to = ${new Date(currentWeekStartMs)}
              AND w.candidate_id IN (SELECT id FROM new_candidates)
          ) AS walked;
      `),
      // Distinct symbols across enabled-or-eligible deployments — denominator
      // for "regimes built this week".
      db.$queryRaw<Array<{ n: bigint }>>(sql`
        SELECT COUNT(DISTINCT venue || ':' || symbol)::bigint AS n
        FROM scalp_v2_deployments
        WHERE candidate_id IS NOT NULL;
      `),
      // # of (venue, symbol) pairs with a regime snapshot for THIS week.
      db.$queryRaw<Array<{ n: bigint }>>(sql`
        SELECT COUNT(DISTINCT venue || ':' || symbol)::bigint AS n
        FROM scalp_regime_snapshots
        WHERE classifier_version = ${classifierVersion}
          AND week_start = ${new Date(currentWeekStartMs)};
      `),
    ]);

    const stageCSurvivors = Number(stageCRows[0]?.total || 0);
    const walkforwardCounts: Record<string, number> = {};
    for (const row of walkforwardCountRows) walkforwardCounts[row.status] = Number(row.count);
    const walkforwardTotal = Object.values(walkforwardCounts).reduce((a, b) => a + b, 0);
    const pendingWalkforward = Math.max(0, stageCSurvivors - walkforwardTotal);

    const throughputLastHour = Number(throughputRows[0]?.n || 0);
    // Hourly buckets — index 0 is the current hour, 11 is 11h ago.
    const throughputBuckets = Array.from({ length: 12 }, () => 0);
    for (const row of sparklineRows) {
      const idx = Math.max(0, Math.min(11, row.bucket));
      throughputBuckets[idx] = Number(row.n);
    }
    // ETA: hours until pending reaches 0 at current per-hour rate.
    const etaHours = throughputLastHour > 0 ? pendingWalkforward / throughputLastHour : null;

    const rolloverThisWeek = Number(currentWeekWalkRows[0]?.n || 0);
    const rolloverPreviousWeek = Number(previousWeekDeployRows[0]?.n || 0);
    const newSurvivorsDiscovered = Number(newSurvivorsRows[0]?.discovered || 0);
    const newSurvivorsWalked = Number(newSurvivorsRows[0]?.walked || 0);
    const deploymentSymbols = Number(deploymentSymbolRows[0]?.n || 0);

    return res.status(200).json({
      ok: true,
      classifierVersion,
      v4Enabled: isScalpV4Enabled(),
      v4HardGateEnabled: isScalpV4HardGateEnabled(),
      stageCSurvivors,
      walkforwardCounts,
      walkforwardTotal,
      pendingWalkforward,
      regimeBuild: {
        symbolsCovered: Number(regimeRows[0]?.symbols || 0),
        latestWeekStartMs: regimeRows[0]?.weekstart ? regimeRows[0].weekstart.getTime() : null,
      },
      throughput: {
        lastHour: throughputLastHour,
        buckets12h: throughputBuckets,
        etaHours,
      },
      rollover: {
        currentWeekStartMs,
        previousWeekStartMs,
        // # of deployments with a walk-forward for the CURRENT week
        walkedThisWeek: rolloverThisWeek,
        // # of deployments that had results for PREVIOUS week — target to match
        walkedLastWeek: rolloverPreviousWeek,
        regimesBuiltThisWeek: Number(regimesThisWeekRows[0]?.n || 0),
        regimesExpected: deploymentSymbols,
        newSurvivorsDiscovered,
        newSurvivorsWalked,
      },
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
