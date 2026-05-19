// /api/scalp/v5/cron/cull-bottom — weekly competitive cull of the worst
// performers in the v5 candidate pool. Complement to trim-tail:
//
//   trim-tail (06:00 UTC Sun): chronic non-eligibles, 28+ days never v5-passed
//   cull-bottom (08:00 UTC Sun): bottom 15% by total NetR among judgeable rows
//
// Strict protections (helper enforces):
//   - skip enabled = TRUE rows (never disrupt live trading)
//   - skip rows younger than `graceDays` (default 28, ~4 Sunday rollovers)
//   - skip rows with < `minTrades` (default 30, can't fairly rank otherwise)
//   - skip rows with total_net_r >= 0 (a "worst" row that's still profitable
//     stays in the pool — we only cull proven losers)
//   - cap retirement at `maxRetireAbs` (default 500 = matches weekly intake)
//   - cap by `minPoolSize` floor (default 1500)
//
// Query params:
//   ?dryRun=true          → preview without writes
//   ?force=true           → bypass the Sunday-only day check
//   ?percentToRetire=0.15 → percent of eligible to cull (0..1)
//   ?graceDays=28
//   ?minTrades=30
//   ?minPoolSize=1500
//   ?maxRetireAbs=500     → 0 or unset disables the absolute cap

export const config = { runtime: "nodejs", maxDuration: 60 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import { cullBottomPerformersScalpV5Deployments } from "../../../../../lib/scalp-v5/pg";

function firstQueryValue(value: string | string[] | undefined): string {
  return Array.isArray(value) ? String(value[0] || "") : String(value || "");
}

function parseBool(value: string | string[] | undefined, fallback: boolean): boolean {
  const raw = firstQueryValue(value).trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function parseIntBounded(
  value: string | string[] | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const parsed = Math.floor(Number(firstQueryValue(value)));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function parseFloatBounded(
  value: string | string[] | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const parsed = Number(firstQueryValue(value));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const startedAt = Date.now();
  const force = parseBool(req.query.force, false);
  const dryRun = parseBool(req.query.dryRun, false);
  const percentToRetire = parseFloatBounded(req.query.percentToRetire, 0.15, 0, 1);
  const graceDays = parseIntBounded(req.query.graceDays, 28, 7, 365);
  const minTrades = parseIntBounded(req.query.minTrades, 30, 0, 10_000);
  const minPoolSize = parseIntBounded(req.query.minPoolSize, 1500, 0, 100_000);
  const maxRetireAbsRaw = parseIntBounded(req.query.maxRetireAbs, 500, 0, 10_000);
  const maxRetireAbs = maxRetireAbsRaw > 0 ? maxRetireAbsRaw : null;
  const nowDay = new Date().getUTCDay(); // 0 = Sunday UTC

  if (nowDay !== 0 && !force && !dryRun) {
    return res.status(200).json({
      ok: true,
      skipped: true,
      reason: `not_sunday_utc (utc_day=${nowDay}); pass ?force=true to override`,
      durationMs: Date.now() - startedAt,
    });
  }

  try {
    const result = await cullBottomPerformersScalpV5Deployments({
      percentToRetire,
      graceDays,
      minTrades,
      minPoolSize,
      maxRetireAbs,
      dryRun,
    });
    return res.status(200).json({
      ok: true,
      durationMs: Date.now() - startedAt,
      utcDay: nowDay,
      forced: force,
      dryRun: result.dryRun,
      params: {
        percentToRetire,
        graceDays,
        minTrades,
        minPoolSize,
        maxRetireAbs,
      },
      result: {
        // poolSize: total active composer rows (before this run).
        // eligibleCount: rows that PASSED the protective filters (judgeable
        // and negative-NetR). retired/sampleDeploymentIds: what actually got
        // (or would get) culled. thresholdNetR: the worst-NetR row we kept
        // — useful tuning signal. If thresholdNetR is e.g. -1.5R, we're
        // culling deeply; if -0.1R, we're only trimming the worst few.
        poolSize: result.poolSize,
        eligibleCount: result.eligibleCount,
        retired: result.retired,
        thresholdNetR: result.thresholdNetR,
        sampleDeploymentIds: result.deploymentIds.slice(0, 50),
      },
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "v5_cull_bottom_failed",
      message: (err as Error)?.message || String(err),
      durationMs: Date.now() - startedAt,
    });
  }
}
