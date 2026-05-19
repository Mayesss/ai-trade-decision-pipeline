// /api/scalp/v4/cron/build-regimes — refresh current-week regime snapshots.
//
// v4/v5 live entry gates fail closed when the current regime snapshot is older
// than SCALP_V4_FAIL_CLOSED_STALE_MS (default 2h). Keep this cron tighter than
// that TTL so otherwise-good v5 deployments do not all block as stale.

export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { scalpPrisma } from "../../../../../lib/scalp/pg/client";
import { sql } from "../../../../../lib/scalp/pg/sql";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import {
  runScalpV4WeeklyRegimeBuild,
  SCALP_V4_CLASSIFIER_VERSION,
  type ScalpV4Venue,
} from "../../../../../lib/scalp-v4";

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

function normalizeVenue(value: unknown): ScalpV4Venue {
  return String(value || "").trim().toLowerCase() === "capital" ? "capital" : "bitget";
}

async function loadLiveDeploymentSymbols(): Promise<Array<{ venue: ScalpV4Venue; symbol: string }>> {
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ venue: string; symbol: string }>>(sql`
    SELECT DISTINCT venue, symbol
    FROM scalp_v2_deployments
    WHERE enabled = TRUE
      AND live_mode = 'live'
      AND symbol IS NOT NULL
      AND symbol <> ''
    ORDER BY venue, symbol;
  `);
  return rows.map((row) => ({
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
  })).filter((row) => row.symbol.length > 0);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const startedAt = Date.now();
  try {
    const liveOnly = parseBool(req.query.liveOnly, true);
    const forceValidity = parseBool(req.query.forceValidity, true);
    const classifierVersion =
      firstQueryValue(req.query.classifierVersion).trim() || SCALP_V4_CLASSIFIER_VERSION;
    const symbols = liveOnly ? await loadLiveDeploymentSymbols() : undefined;

    const result = await runScalpV4WeeklyRegimeBuild({
      symbols,
      classifierVersion,
      forceValidity,
    });

    return res.status(200).json({
      ok: true,
      durationMs: Date.now() - startedAt,
      params: {
        liveOnly,
        forceValidity,
        classifierVersion,
        symbolsRequested: symbols?.length ?? null,
      },
      result,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "v4_regime_build_failed",
      message: (err as Error)?.message || String(err),
      durationMs: Date.now() - startedAt,
    });
  }
}
