export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { listScalpComposerDeployments } from "../../../../../lib/scalp/composer/db";
import {
  parseBool,
  parseIntBounded,
  parseSession,
  parseVenue,
  setNoStoreHeaders,
} from "../../../../../lib/scalp/composer/http";

type DeploymentScope = "live" | "enabled" | "inactive" | "all";

function parseScope(value: string | string[] | undefined): DeploymentScope {
  const raw = (Array.isArray(value) ? value[0] : value || "").trim().toLowerCase();
  if (raw === "live" || raw === "enabled" || raw === "inactive" || raw === "all") {
    return raw;
  }
  return "live";
}

// Lazy/paginated deployments feed for the dashboard. Kept separate from the
// (heavier) /dashboard/summary so the roster loads on demand, scoped and paged,
// instead of being bundled into the once-per-page summary payload.
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
    const scope = parseScope(req.query.scope);
    const venue = parseVenue(req.query.venue);
    const session = parseSession(req.query.session);
    const limit = parseIntBounded(req.query.limit, 25, 1, 200);
    const offset = parseIntBounded(req.query.offset, 0, 0, 1_000_000);
    const stageCPassed = parseBool(req.query.stageCPassed, false);

    // Fetch one extra row to know whether a next page exists without a COUNT.
    const rows = await listScalpComposerDeployments({
      enabledOnly: scope === "live" || scope === "enabled",
      liveOnly: scope === "live",
      disabledOnly: scope === "inactive",
      stageCPassedOnly: stageCPassed,
      venue,
      session,
      compactPromotionGate: true,
      limit: limit + 1,
      offset,
    });

    const hasMore = rows.length > limit;
    const deployments = hasMore ? rows.slice(0, limit) : rows;

    return res.status(200).json({
      ok: true,
      scope,
      stageCPassed,
      venue: venue || null,
      session: session || null,
      limit,
      offset,
      hasMore,
      count: deployments.length,
      deployments,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_dashboard_deployments_failed",
      message: err?.message || String(err),
    });
  }
}
