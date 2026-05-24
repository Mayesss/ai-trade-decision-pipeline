// /api/scalp/v5/cron/preflight-candles — ensure the newly completed v5
// incremental week is loaded before Sunday evaluation starts.

export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import { runScalpV5CandlePreflight } from "../../../../../lib/scalp-v5/candlePreflight";

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

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const startedAt = Date.now();
  try {
    const batchSize = parseIntBounded(req.query.batchSize, 200, 1, 200);
    const maxAttempts = parseIntBounded(req.query.maxAttempts, 10, 1, 30);
    const result = await runScalpV5CandlePreflight({
      batchSize,
      maxAttempts,
      auditTrigger: "v5_preflight_cron",
    });
    return res.status(200).json({
      ok: result.ready,
      durationMs: Date.now() - startedAt,
      params: { batchSize, maxAttempts },
      result,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "v5_candle_preflight_failed",
      message: (err as Error)?.message || String(err),
      durationMs: Date.now() - startedAt,
    });
  }
}
