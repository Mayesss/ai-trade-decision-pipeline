export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  listScalpV2Candidates,
  listScalpV2Deployments,
  listScalpV2ExecutionEvents,
  listScalpV2Jobs,
  listScalpV2RecentLedger,
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
    const eventLimit = parseIntBounded(req.query.eventLimit, 120, 10, 2_000);
    const ledgerLimit = parseIntBounded(req.query.ledgerLimit, 120, 10, 2_000);
    const deploymentLimit = parseIntBounded(req.query.deploymentLimit, 300, 10, 5_000);
    const jobLimit = parseIntBounded(req.query.jobLimit, 20, 5, 100);
    const candidateLimit = parseIntBounded(
      req.query.candidateLimit,
      2_000,
      100,
      10_000,
    );
    const session = parseSession(req.query.session);
    const venue = parseVenue(req.query.venue);

    const [runtime, summary, deployments, events, ledger, jobs, candidates] = await Promise.all([
      loadScalpV2RuntimeConfig(),
      loadScalpV2Summary(),
      listScalpV2Deployments({ limit: deploymentLimit, session, venue }),
      listScalpV2ExecutionEvents({ limit: eventLimit }),
      listScalpV2RecentLedger({ limit: ledgerLimit }),
      listScalpV2Jobs({ limit: jobLimit }),
      listScalpV2Candidates({ limit: candidateLimit, session, venue }),
    ]);

    return res.status(200).json({
      ok: true,
      mode: "scalp_v2",
      runtime,
      summary,
      deployments,
      events,
      ledger,
      jobs,
      candidates,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_dashboard_summary_failed",
      message: err?.message || String(err),
    });
  }
}
