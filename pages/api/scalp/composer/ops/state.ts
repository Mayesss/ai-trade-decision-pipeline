export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  listScalpComposerCandidates,
  listScalpComposerDeployments,
  listScalpComposerResearchCursors,
  listScalpComposerResearchHighlights,
  loadScalpComposerRuntimeConfig,
  loadScalpComposerSummary,
} from "../../../../../lib/scalp/composer/db";
import {
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp/composer/http";

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

    const [runtime, summary, candidates, deployments, researchCursors, researchHighlights] = await Promise.all([
      loadScalpComposerRuntimeConfig(),
      loadScalpComposerSummary(),
      listScalpComposerCandidates({ limit }),
      listScalpComposerDeployments({ limit }),
      listScalpComposerResearchCursors({ limit: Math.min(limit, 500) }),
      listScalpComposerResearchHighlights({ limit: Math.min(limit, 500) }),
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
