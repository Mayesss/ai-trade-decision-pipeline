// /api/scalp/research/cron/promote — v5-authoritative strict promote.
//
// v5-side: fresh evidence + totalNetR/trades/positive-weeks/worst-week
// thresholds (configurable via query params). v2 promotion_gate is
// informational only; v5 writes a v5Promotion marker when it enables a row.
//
// Only promotion direction is automated. We never auto-demote here. The
// existing v2 execute/reconcile engine remains responsible for live order
// handling, while the v5 live entry gate blocks new entries on stale or
// failing v5 evidence. v5-owned live rows bypass SCALP_COMPOSER_LIVE_ENABLED;
// explicit execute dryRun=true still prevents live orders.
//
// Honors SCALP_RESEARCH_AUTO_PROMOTE_ENABLED — set to "0"/"false"/"off" to
// disable without redeploy.
//
// Query params for tuning the strict criteria (defaults shown):
//   ?minTotalNetR=4        minimum sum-of-cell-netR across 12w
//   ?minTotalTrades=60     minimum sum-of-cell-trades
//   ?minPositiveWeeks=8    minimum weeks-with-positive-cross-cell-netR
//   ?minWorstWeekR=3       worst single week must be >= -minWorstWeekR
//   ?minTrailing4wNetR=4   trailing 4-week cross-cell netR must be >= this
//   Low-sample consistency exception defaults:
//   ?minConsistencyTrades=30
//   ?minConsistencyTotalNetR=12
//   ?minConsistencyPositiveWeeks=11
//   ?minConsistencyWorstWeekR=0
//   ?minConsistencyTrailing4wNetR=4
//   ?minConsistencyActiveCells=2
//   ?maxPromotions=12      optional cap (defaults to remaining runtime slots)
//   ?staleOlderThanHours=336 v5 evidence freshness (default 14 days)
//   ?dryRun=true           preview without writes; emits the funnel

export const config = { runtime: "nodejs", maxDuration: 60 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp/composer/http";
import { autoPromoteScalpResearchWinnersToEnabled } from "../../../../../lib/scalp/research/pg";

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
  const raw = String(process.env.SCALP_RESEARCH_AUTO_PROMOTE_ENABLED ?? "").trim().toLowerCase();
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
  const minTrailing4wNetR = parseFloatBounded(req.query.minTrailing4wNetR, 4, -100, 1_000);
  const minConsistencyTrades = parseIntBounded(req.query.minConsistencyTrades, 30, 0, 10_000);
  const minConsistencyTotalNetR = parseFloatBounded(req.query.minConsistencyTotalNetR, 12, -100, 1_000);
  const minConsistencyPositiveWeeks = parseIntBounded(req.query.minConsistencyPositiveWeeks, 11, 0, 52);
  const minConsistencyWorstWeekR = parseFloatBounded(req.query.minConsistencyWorstWeekR, 0, -100, 100);
  const minConsistencyTrailing4wNetR = parseFloatBounded(req.query.minConsistencyTrailing4wNetR, 4, -100, 1_000);
  const minConsistencyActiveCells = parseIntBounded(req.query.minConsistencyActiveCells, 2, 1, 100);
  const maxPromotionsRaw = firstQueryValue(req.query.maxPromotions).trim();
  const maxPromotions = maxPromotionsRaw
    ? parseIntBounded(req.query.maxPromotions, 12, 0, 500)
    : undefined;

  // Env kill switch — overrides query unless explicitly dryRun.
  const enabledByEnv = isAutoPromoteEnabledByEnv();
  if (!enabledByEnv && !dryRun) {
    return res.status(200).json({
      ok: true,
      skipped: true,
      reason: "SCALP_RESEARCH_AUTO_PROMOTE_ENABLED is off",
      durationMs: Date.now() - startedAt,
    });
  }

  try {
    const result = await autoPromoteScalpResearchWinnersToEnabled({
      staleOlderThanMs: staleOlderThanHours * 60 * 60_000,
      dryRun,
      minTotalNetR,
      minTotalTrades,
      minPositiveWeeks,
      minWorstWeekR,
      minTrailing4wNetR,
      minConsistencyTrades,
      minConsistencyTotalNetR,
      minConsistencyPositiveWeeks,
      minConsistencyWorstWeekR,
      minConsistencyTrailing4wNetR,
      minConsistencyActiveCells,
      maxPromotions,
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
        minTrailing4wNetR,
        minConsistencyTrades,
        minConsistencyTotalNetR,
        minConsistencyPositiveWeeks,
        minConsistencyWorstWeekR,
        minConsistencyTrailing4wNetR,
        minConsistencyActiveCells,
        ...(maxPromotions !== undefined && { maxPromotions }),
      },
      result: {
        promoted: result.promoted,
        liveMode: result.liveMode,
        runtimeLiveEnabled: result.runtimeLiveEnabled,
        v5LiveBypassesV2LiveEnabled: result.v5LiveBypassesV2LiveEnabled,
        // funnel tells operator WHERE rows are getting filtered out:
        //   candidates      = rows that passed v5_enabled + freshness + lease
        //   failedTotalNetR = of those, how many fell short on totalNetR
        //   failedTotalTrades / failedPositiveWeeks / failedWorstWeek /
        //     failedTrailing4wNetR = same idea for v5-side thresholds
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
