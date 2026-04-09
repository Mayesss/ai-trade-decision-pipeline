export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  listScalpV2Deployments,
  listScalpV2Jobs,
  loadScalpV2RuntimeConfig,
  loadScalpV2Summary,
} from "../../../../../lib/scalp-v2/db";
import {
  parseSession,
  parseVenue,
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
    const deploymentLimit = parseIntBounded(req.query.deploymentLimit, 500, 10, 5_000);
    const jobLimit = parseIntBounded(req.query.jobLimit, 20, 5, 100);
    const session = parseSession(req.query.session);
    const venue = parseVenue(req.query.venue);

    // Single sequential chain — Neon serverless can't handle parallel queries reliably
    const runtime = await loadScalpV2RuntimeConfig();
    const summary = await loadScalpV2Summary();
    const jobs = await listScalpV2Jobs({ limit: jobLimit });
    const deployments = await listScalpV2Deployments({ limit: deploymentLimit, session, venue });

    return res.status(200).json({
      ok: true,
      mode: "scalp_v2",
      runtime,
      summary,
      deployments,
      events: [],
      ledger: [],
      jobs,
      candidates: [],
      researchCursors: [],
      researchHighlights: [],
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_dashboard_summary_failed",
      message: err?.message || String(err),
    });
  }
}
