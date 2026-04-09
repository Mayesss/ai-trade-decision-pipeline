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
  parseBool,
  parseSession,
  parseVenue,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";

// In-memory cache — avoids hammering Neon on every dashboard refresh
let summaryCache: { data: Record<string, unknown>; ts: number; key: string } | null = null;
const CACHE_TTL_MS = 30_000; // 30 seconds

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
    const deploymentLimit = parseIntBounded(req.query.deploymentLimit, 10, 1, 500);
    const jobLimit = parseIntBounded(req.query.jobLimit, 20, 5, 100);
    const session = parseSession(req.query.session);
    const venue = parseVenue(req.query.venue);
    const fresh = parseBool(req.query.fresh, false);

    const cacheKey = `${session || "all"}:${venue || "all"}:${deploymentLimit}`;
    if (!fresh && summaryCache && summaryCache.key === cacheKey && Date.now() - summaryCache.ts < CACHE_TTL_MS) {
      return res.status(200).json(summaryCache.data);
    }

    // Sequential queries — Neon serverless can't handle parallel reliably
    const runtime = await loadScalpV2RuntimeConfig();
    const summary = await loadScalpV2Summary();
    const jobs = await listScalpV2Jobs({ limit: jobLimit });
    const deployments = await listScalpV2Deployments({ limit: deploymentLimit, session, venue });

    const payload = {
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
    };

    summaryCache = { data: payload, ts: Date.now(), key: cacheKey };
    return res.status(200).json(payload);
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_dashboard_summary_failed",
      message: err?.message || String(err),
    });
  }
}
