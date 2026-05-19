// /api/scalp/v5/cron/promote — unified strict promote: flip enabled=TRUE
// only on deployments where BOTH v5 AND v2 endorse live trading.
//
// v5-side: fresh evidence + totalNetR/trades/positive-weeks/worst-week
// thresholds (configurable via query params). v2-side: promotion_gate's
// `eligible = TRUE` AND `freshness.ready = TRUE`. Both must hold — this is
// what stops the v5↔v2 flap loop where v5 promoted, v2 immediately demoted.
//
// Only promotion direction is automated. We never auto-demote here — the
// v2 promote cron at /api/scalp/v2/cron/promote and the v2 execute
// freshness guard handle demotion. The live entry gate
// (resolveScalpV5EntryBlock) soft-blocks new entries on rows v5 no longer
// passes.
//
// Honors SCALP_V5_AUTO_PROMOTE_ENABLED — set to "0"/"false"/"off" to
// disable without redeploy.
//
// Query params for tuning the strict criteria (defaults shown):
//   ?minTotalNetR=4        minimum sum-of-cell-netR across 12w
//   ?minTotalTrades=60     minimum sum-of-cell-trades
//   ?minPositiveWeeks=8    minimum weeks-with-positive-cross-cell-netR
//   ?minWorstWeekR=3       worst single week must be >= -minWorstWeekR
//   ?staleOlderThanHours=336 v5 evidence freshness (default 14 days)
//   ?dryRun=true           preview without writes; emits the funnel

export const config = { runtime: "nodejs", maxDuration: 60 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import { autoPromoteScalpV5WinnersToEnabled } from "../../../../../lib/scalp-v5/pg";

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
  // Important: an absent query param produces "" here, and Number("") is 0
  // (not NaN). Short-circuit on empty so the fallback actually fires.
  const raw = firstQueryValue(value).trim();
  if (!raw) return fallback;
  const parsed = Math.floor(Number(raw));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function parseFloatBounded(
  value: string | string[] | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const raw = firstQueryValue(value).trim();
  if (!raw) return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function isAutoPromoteEnabledByEnv(): boolean {
  const raw = String(process.env.SCALP_V5_AUTO_PROMOTE_ENABLED ?? "").trim().toLowerCase();
  if (!raw) return true; // default ON when env unset
  return !["0", "false", "no", "off"].includes(raw);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const startedAt = Date.now();
  const dryRun = parseBool(req.query.dryRun, false);
  const staleOlderThanHours = parseIntBounded(
    req.query.staleOlderThanHours,
    24 * 14, // default 14 days — twice the bulk eval cycle for slack
    1,
    24 * 30,
  );
  const minTotalNetR = parseFloatBounded(req.query.minTotalNetR, 4, -100, 1_000);
  const minTotalTrades = parseIntBounded(req.query.minTotalTrades, 60, 0, 10_000);
  const minPositiveWeeks = parseIntBounded(req.query.minPositiveWeeks, 8, 0, 52);
  const minWorstWeekR = parseFloatBounded(req.query.minWorstWeekR, 3, 0, 100);

  // Env kill switch — overrides query unless explicitly dryRun.
  const enabledByEnv = isAutoPromoteEnabledByEnv();
  if (!enabledByEnv && !dryRun) {
    return res.status(200).json({
      ok: true,
      skipped: true,
      reason: "SCALP_V5_AUTO_PROMOTE_ENABLED is off",
      durationMs: Date.now() - startedAt,
    });
  }

  try {
    const result = await autoPromoteScalpV5WinnersToEnabled({
      staleOlderThanMs: staleOlderThanHours * 60 * 60_000,
      dryRun,
      minTotalNetR,
      minTotalTrades,
      minPositiveWeeks,
      minWorstWeekR,
    });
    return res.status(200).json({
      ok: true,
      dryRun,
      durationMs: Date.now() - startedAt,
      params: {
        staleOlderThanHours,
        minTotalNetR,
        minTotalTrades,
        minPositiveWeeks,
        minWorstWeekR,
      },
      result: {
        promoted: result.promoted,
        // funnel tells operator WHERE rows are getting filtered out:
        //   candidates      = rows that passed v5_enabled + freshness + lease
        //   failedTotalNetR = of those, how many fell short on totalNetR
        //   failedTotalTrades / failedPositiveWeeks / failedWorstWeek = same
        //     idea for the other v5-side thresholds
        //   failedV2Eligible    = passed v5 strict but promotion_gate.eligible
        //                         was FALSE (v2 didn't endorse)
        //   failedV2Freshness   = passed v5 + v2 eligible but stage-C window
        //                         was stale (v2 research lag)
        //   qualified           = passed everything → promoted (or would be
        //                         in dry-run)
        funnel: result.funnel,
        // Cap sample IDs to keep cron log small.
        sampleDeploymentIds: result.deploymentIds.slice(0, 50),
      },
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "v5_promote_failed",
      message: (err as Error)?.message || String(err),
      durationMs: Date.now() - startedAt,
    });
  }
}
