export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  listScalpV2ExecutionEvents,
  listScalpV2Deployments,
  listScalpV2JournalRows,
  listScalpV2Jobs,
  listScalpV2LedgerRows,
  listScalpV2SessionSnapshots,
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
    const eventLimit = parseIntBounded(req.query.eventLimit, 240, 20, 2_000);
    const ledgerLimit = parseIntBounded(req.query.ledgerLimit, 300, 20, 5_000);
    const session = parseSession(req.query.session);
    const venue = parseVenue(req.query.venue);
    const fresh = parseBool(req.query.fresh, false);

    const cacheKey = `${session || "all"}:${venue || "all"}:${deploymentLimit}:${jobLimit}:${eventLimit}:${ledgerLimit}`;
    if (!fresh && summaryCache && summaryCache.key === cacheKey && Date.now() - summaryCache.ts < CACHE_TTL_MS) {
      return res.status(200).json(summaryCache.data);
    }

    // Sequential queries — Neon serverless can't handle parallel reliably
    const runtime = await loadScalpV2RuntimeConfig();
    const summary = await loadScalpV2Summary();
    const jobs = await listScalpV2Jobs({ limit: jobLimit });
    const deployments = await listScalpV2Deployments({ limit: deploymentLimit, session, venue });
    const deploymentIds = deployments
      .map((row) => String(row.deploymentId || "").trim())
      .filter(Boolean);
    const events = await listScalpV2ExecutionEvents({
      limit: eventLimit,
      venue,
      session,
    });
    const sessions = await listScalpV2SessionSnapshots({
      deploymentIds,
      limit: Math.max(deploymentLimit * 4, 100),
    });
    const journal = await listScalpV2JournalRows({
      limit: eventLimit,
      venue,
      session,
    });
    const nowMs = Date.now();
    const THIRTY_DAYS_MS = 30 * 24 * 60 * 60 * 1000;
    const fromTsMs = Math.max(0, nowMs - THIRTY_DAYS_MS);
    const ledger = deploymentIds.length
      ? await listScalpV2LedgerRows({
          deploymentIds,
          fromTsMs,
          toTsMs: nowMs + 1,
          limit: ledgerLimit,
        })
      : [];
    const scopedSummary = {
      ...summary,
      events24h: events.filter((row) => row.tsMs >= nowMs - 24 * 60 * 60 * 1000)
        .length,
      ledgerRows30d: ledger.length,
      netR30d: ledger.reduce((acc, row) => acc + (Number(row.rMultiple) || 0), 0),
    };

    const payload = {
      ok: true,
      mode: "scalp_v2",
      runtime,
      summary: scopedSummary,
      deployments,
      events,
      sessions,
      journal,
      ledger,
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
