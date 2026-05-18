// /api/scalp/v5/cron/promote — auto-flip enabled=TRUE on every deployment
// that v5 confirmed as a winner (v5_enabled=TRUE) with fresh evidence.
//
// This is the C wire-up: v5 starts driving who trades live, not just gating
// entries on rows already promoted. Only the promotion direction is
// automated. We never auto-demote here — the v2 promote cron at
// /api/scalp/v2/cron/promote handles scope-based demotion every 5 min, and
// the live entry gate (resolveScalpV5EntryBlock) already soft-blocks new
// entries on rows v5 no longer passes.
//
// Honors SCALP_V5_AUTO_PROMOTE_ENABLED — set to "0"/"false"/"off" to disable.

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
  const parsed = Math.floor(Number(firstQueryValue(value)));
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
    });
    return res.status(200).json({
      ok: true,
      dryRun,
      durationMs: Date.now() - startedAt,
      params: { staleOlderThanHours },
      result,
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
