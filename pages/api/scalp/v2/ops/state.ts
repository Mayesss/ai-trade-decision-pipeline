export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  aggregateScalpV2ParityWindow,
  listScalpV2Candidates,
  listScalpV2Deployments,
  listScalpV2ResearchCursors,
  listScalpV2ResearchHighlights,
  loadScalpV2RuntimeConfig,
  loadScalpV2Summary,
} from "../../../../../lib/scalp-v2/db";
import {
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== "GET") {
    return res
      .status(405)
      .json({ error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const limit = parseIntBounded(req.query.limit, 500, 20, 5_000);
    const parityDays = parseIntBounded(req.query.parityDays, 30, 1, 3650);

    const [runtime, summary, candidates, deployments, parity, researchCursors, researchHighlights] = await Promise.all([
      loadScalpV2RuntimeConfig(),
      loadScalpV2Summary(),
      listScalpV2Candidates({ limit }),
      listScalpV2Deployments({ limit }),
      aggregateScalpV2ParityWindow({ sinceDays: parityDays }),
      listScalpV2ResearchCursors({ limit: Math.min(limit, 500) }),
      listScalpV2ResearchHighlights({ limit: Math.min(limit, 500) }),
    ]);

    return res.status(200).json({
      ok: true,
      mode: "scalp_v2",
      runtime,
      summary,
      counts: {
        candidates: candidates.length,
        deployments: deployments.length,
        enabledDeployments: deployments.filter((row) => row.enabled).length,
        researchCursors: researchCursors.length,
        researchHighlights: researchHighlights.length,
      },
      parity,
      research: {
        cursors: researchCursors,
        highlights: researchHighlights.slice(0, 50),
      },
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_ops_state_failed",
      message: err?.message || String(err),
    });
  }
}
