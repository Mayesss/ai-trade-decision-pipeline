// /api/scalp/v5/cron/load-live-candles — keep enabled deployment candles fresh.
//
// The broader v2 load-candles cron maintains the research universe. This route
// is intentionally narrower: refresh only enabled live symbols so live v5 gates
// do not go stale behind research backlog or self-chain failures.

export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { scalpPrisma } from "../../../../../lib/scalp/pg/client";
import { sql } from "../../../../../lib/scalp/pg/sql";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import { runScalpV2LoadCandlesPipelineJob } from "../../../../../lib/scalp-v2/pipelineJobsAdapter";
import type { ScalpV2Venue } from "../../../../../lib/scalp-v2/types";

function firstQueryValue(value: string | string[] | undefined): string {
  return Array.isArray(value) ? String(value[0] || "") : String(value || "");
}

function parseIntBounded(
  value: string | string[] | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const raw = firstQueryValue(value).trim();
  if (!raw) return fallback;
  const parsed = Math.floor(Number(raw));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

function normalizeVenue(value: unknown): ScalpV2Venue {
  return String(value || "").trim().toLowerCase() === "capital" ? "capital" : "bitget";
}

async function loadEnabledLiveScopes(): Promise<Array<{ venue: ScalpV2Venue; symbol: string }>> {
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
    const scopes = await loadEnabledLiveScopes();
    const batchSize = parseIntBounded(req.query.batchSize, 20, 1, 100);
    const maxAttempts = parseIntBounded(req.query.maxAttempts, 10, 1, 30);

    const result = await runScalpV2LoadCandlesPipelineJob({
      batchSize,
      maxAttempts,
      scopes,
    });

    return res.status(200).json({
      ok: result.ok,
      durationMs: Date.now() - startedAt,
      params: {
        batchSize,
        maxAttempts,
        scopeCount: scopes.length,
      },
      result,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "v5_live_candle_load_failed",
      message: (err as Error)?.message || String(err),
      durationMs: Date.now() - startedAt,
    });
  }
}
