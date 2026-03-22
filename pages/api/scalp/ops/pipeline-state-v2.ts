export const config = { runtime: "nodejs" };

import { empty, join, raw, sql } from '../../../../lib/scalp/pg/sql';
import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { isScalpPgConfigured, scalpPrisma } from "../../../../lib/scalp/pg/client";
import { loadScalpPipelineJobsHealth } from "../../../../lib/scalp/pipelineJobs";
import {
  listScalpEntrySessionProfiles,
  parseScalpEntrySessionProfileStrict,
} from "../../../../lib/scalp/sessions";

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader(
    "Cache-Control",
    "no-store, no-cache, must-revalidate, proxy-revalidate",
  );
  res.setHeader("Pragma", "no-cache");
  res.setHeader("Expires", "0");
}

function firstQueryValue(
  value: string | string[] | undefined,
): string | undefined {
  if (typeof value === "string") return value.trim() || undefined;
  if (Array.isArray(value) && value.length > 0)
    return String(value[0] || "").trim() || undefined;
  return undefined;
}

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

  const generatedAtMs = Date.now();
  const entrySessionProfile = parseScalpEntrySessionProfileStrict(
    firstQueryValue(req.query.session),
  );
  if (!entrySessionProfile) {
    return res.status(400).json({
      error: "invalid_session",
      message: `Use session=${listScalpEntrySessionProfiles().join("|")}.`,
      generatedAtMs,
    });
  }
  try {
    const jobs = await loadScalpPipelineJobsHealth({ entrySessionProfile });
    if (!isScalpPgConfigured()) {
      return res.status(200).json({
        ok: true,
        v2: true,
        generatedAtMs,
        entrySessionProfile,
        pgConfigured: false,
        jobs,
        discoveredQueue: null,
      });
    }
    const db = scalpPrisma();
    const counts = await db.$queryRaw<
      Array<{
        total: bigint | number | null;
        pendingLoad: bigint | number | null;
        runningLoad: bigint | number | null;
        retryLoad: bigint | number | null;
        succeededLoad: bigint | number | null;
        pendingPrepare: bigint | number | null;
      }>
    >(sql`
      SELECT
          COUNT(*)::bigint AS total,
          COUNT(*) FILTER (WHERE load_status = 'pending')::bigint AS "pendingLoad",
          COUNT(*) FILTER (WHERE load_status = 'running')::bigint AS "runningLoad",
          COUNT(*) FILTER (WHERE load_status = 'retry_wait')::bigint AS "retryLoad",
          COUNT(*) FILTER (WHERE load_status = 'succeeded')::bigint AS "succeededLoad",
          COUNT(*) FILTER (WHERE prepare_status IN ('pending', 'retry_wait'))::bigint AS "pendingPrepare"
      FROM scalp_discovered_symbols;
    `);
    const row = counts[0] || ({} as any);
    return res.status(200).json({
      ok: true,
      v2: true,
      generatedAtMs,
      entrySessionProfile,
      pgConfigured: true,
      jobs,
      discoveredQueue:
        entrySessionProfile === "berlin"
          ? {
              total: Math.max(0, Math.floor(Number(row.total || 0))),
              pendingLoad: Math.max(0, Math.floor(Number(row.pendingLoad || 0))),
              runningLoad: Math.max(0, Math.floor(Number(row.runningLoad || 0))),
              retryLoad: Math.max(0, Math.floor(Number(row.retryLoad || 0))),
              succeededLoad: Math.max(0, Math.floor(Number(row.succeededLoad || 0))),
              pendingPrepare: Math.max(0, Math.floor(Number(row.pendingPrepare || 0))),
            }
          : null,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_pipeline_state_v2_failed",
      message: err?.message || String(err),
      generatedAtMs,
    });
  }
}
