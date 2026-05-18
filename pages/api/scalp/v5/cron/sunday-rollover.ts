// /api/scalp/v5/cron/sunday-rollover — weekly v5 re-evaluation trigger.
//
// Schedule: Sunday 02:00 UTC (see vercel.json). At this point Saturday's
// week is complete and the v4 classifier has the data it needs to write
// the new weekly regime snapshot. This handler clears every row's
// v5_evaluated_at so the existing hourly evaluate cron picks them all up
// over the rest of Sunday — by Monday 00:00 UTC every deployment has been
// re-validated against the just-completed week and any new winners have
// been auto-promoted.
//
// Idempotent. Defensive day-of-week check: refuses to run on non-Sunday
// unless ?force=true is passed (lets you trigger it manually for a one-off
// re-eval without waiting for the next Sunday).

export const config = { runtime: "nodejs", maxDuration: 60 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import { invalidateAllScalpV5Evidence } from "../../../../../lib/scalp-v5/pg";

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

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const startedAt = Date.now();
  const force = parseBool(req.query.force, false);
  const nowDay = new Date().getUTCDay(); // 0 = Sunday UTC

  if (nowDay !== 0 && !force) {
    return res.status(200).json({
      ok: true,
      skipped: true,
      reason: `not_sunday_utc (utc_day=${nowDay}); pass ?force=true to override`,
      durationMs: Date.now() - startedAt,
    });
  }

  try {
    const result = await invalidateAllScalpV5Evidence({ onlyEnabled: false });
    return res.status(200).json({
      ok: true,
      durationMs: Date.now() - startedAt,
      utcDay: nowDay,
      forced: force,
      invalidated: result.invalidated,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "v5_sunday_rollover_failed",
      message: (err as Error)?.message || String(err),
      durationMs: Date.now() - startedAt,
    });
  }
}
