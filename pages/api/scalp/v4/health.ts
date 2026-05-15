export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import {
  isScalpV4Enabled,
  isScalpV4HardGateEnabled,
  SCALP_V4_CLASSIFIER_VERSION,
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

    // Three small aggregates, parallel — pool can absorb 3 short queries.
    const [stageCRows, walkforwardRows, regimeRows] = await Promise.all([
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
    ]);

    const stageCSurvivors = Number(stageCRows[0]?.total || 0);
    const walkforwardCounts: Record<string, number> = {};
    for (const row of walkforwardRows) walkforwardCounts[row.status] = Number(row.count);
    const walkforwardTotal = Object.values(walkforwardCounts).reduce((a, b) => a + b, 0);
    const pendingWalkforward = Math.max(0, stageCSurvivors - walkforwardTotal);

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
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
