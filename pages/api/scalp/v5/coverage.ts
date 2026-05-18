// /api/scalp/v5/coverage — section 2 of the v5 dashboard: classifier, config,
// evaluator timestamps, and coverage counts. Pure SQL aggregate, no regime
// snapshot loop. Always returns fast even when the heavier endpoints time out.

export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import {
  isScalpV5Enabled,
  isScalpV5HardGateEnabled,
  resolveScalpV5Config,
} from "../../../../lib/scalp-v5";
import { scalpPrisma } from "../../../../lib/scalp/pg/client";
import { sql } from "../../../../lib/scalp/pg/sql";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const cfg = resolveScalpV5Config();
    const nowMs = Date.now();
    const staleThresholdMs = Math.max(
      60 * 60_000,
      Math.floor(
        Number(process.env.BULK_V5_STALE_OLDER_THAN_HOURS) > 0
          ? Number(process.env.BULK_V5_STALE_OLDER_THAN_HOURS) * 60 * 60_000
          : 6 * 24 * 60 * 60_000,
      ),
    );
    const staleBefore = new Date(nowMs - staleThresholdMs);
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{
      totalDeployments: bigint;
      enabledDeployments: bigint;
      evaluated: bigint;
      missingEvidence: bigint;
      stale: bigint;
      latestEvaluatedAt: Date | null;
      oldestEvaluatedAt: Date | null;
    }>>(sql`
      SELECT
        COUNT(*) AS "totalDeployments",
        COUNT(*) FILTER (WHERE enabled = TRUE) AS "enabledDeployments",
        COUNT(*) FILTER (WHERE v5_evaluated_at IS NOT NULL) AS "evaluated",
        COUNT(*) FILTER (WHERE v5_evaluated_at IS NULL) AS "missingEvidence",
        COUNT(*) FILTER (WHERE v5_evaluated_at IS NOT NULL AND v5_evaluated_at < ${staleBefore}) AS "stale",
        MAX(v5_evaluated_at) AS "latestEvaluatedAt",
        MIN(v5_evaluated_at) AS "oldestEvaluatedAt"
      FROM scalp_v2_deployments
      WHERE candidate_id IS NOT NULL;
    `);
    const r = rows[0] || {
      totalDeployments: BigInt(0),
      enabledDeployments: BigInt(0),
      evaluated: BigInt(0),
      missingEvidence: BigInt(0),
      stale: BigInt(0),
      latestEvaluatedAt: null,
      oldestEvaluatedAt: null,
    };

    return res.status(200).json({
      ok: true,
      classifierVersion: cfg.classifierVersion,
      v5Enabled: isScalpV5Enabled(),
      v5HardGateEnabled: isScalpV5HardGateEnabled(),
      config: {
        holdoutWeeks: cfg.holdoutWeeks,
        minTradesPerCell: cfg.minTradesPerCell,
      },
      nowMs,
      evaluator: {
        latestEvaluationMs: r.latestEvaluatedAt ? r.latestEvaluatedAt.getTime() : null,
        oldestEvaluationMs: r.oldestEvaluatedAt ? r.oldestEvaluatedAt.getTime() : null,
      },
      coverage: {
        totalDeployments: Number(r.totalDeployments),
        enabledDeployments: Number(r.enabledDeployments),
        evaluated: Number(r.evaluated),
        missingEvidence: Number(r.missingEvidence),
        stale: Number(r.stale),
        staleThresholdMs,
      },
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
