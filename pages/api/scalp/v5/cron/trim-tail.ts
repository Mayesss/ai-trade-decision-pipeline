// /api/scalp/v5/cron/trim-tail — weekly self-cleaning of the candidate pool.
//
// Retires deployments that v5 has consistently failed to qualify over a
// stalenessDays window (default 28 days = ~4 Sunday rollovers). Only touches
// rows that are NOT currently live (enabled = FALSE) and have NEVER been
// promoted (last_promoted_at IS NULL) — live trading is never disrupted.
//
// Schedule: Sunday 06:00 UTC, after the rollover wave at 02:00 has finished
// re-evaluating the pool (gives a 4h buffer for ~2k incremental evals to
// complete). Skipping the day-of-week check is supported via ?force=true for
// manual one-off runs.
//
// ?dryRun=true previews counts + deployment IDs without writing.
// ?stalenessDays=N overrides the 28-day default (min 7).

export const config = { runtime: "nodejs", maxDuration: 60 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import { retireConsistentlyFailingScalpV5Deployments } from "../../../../../lib/scalp-v5/pg";

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
  // Absent query param → "" → Number("") = 0, not NaN. Short-circuit so the
  // fallback fires correctly when the param is missing.
  const raw = firstQueryValue(value).trim();
  if (!raw) return fallback;
  const parsed = Math.floor(Number(raw));
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
  const stalenessDays = parseIntBounded(req.query.stalenessDays, 28, 7, 365);
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
    const result = await retireConsistentlyFailingScalpV5Deployments({
      stalenessDays,
      dryRun,
    });
    return res.status(200).json({
      ok: true,
      durationMs: Date.now() - startedAt,
      utcDay: nowDay,
      forced: force,
      dryRun: result.dryRun,
      stalenessDays,
      retired: result.retired,
      // Cap the response payload — at 2k+ retirements, embedding every ID
      // bloats the cron log. First 50 is enough to spot-check; the full set
      // is retrievable via a follow-up SQL query if needed.
      sampleDeploymentIds: result.deploymentIds.slice(0, 50),
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "v5_trim_tail_failed",
      message: (err as Error)?.message || String(err),
      durationMs: Date.now() - startedAt,
    });
  }
}
