export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { runScalpRegimeResearchJob } from "../../../../../lib/scalp/regimes";

function firstQueryValue(value: string | string[] | undefined): string {
  return Array.isArray(value) ? String(value[0] || "") : String(value || "");
}

function parseBool(value: string | string[] | undefined, fallback = false): boolean {
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

function setNoStoreHeaders(res: NextApiResponse): void {
  res.setHeader("Cache-Control", "no-store, max-age=0");
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

  try {
    const maxCandidatesPerCall = parseIntBounded(
      req.query.maxCandidatesPerCall ?? req.query.batchSize,
      5,
      0,
      500,
    );
    const candidateFetchLimit = parseIntBounded(
      req.query.candidateFetchLimit,
      Math.max(maxCandidatesPerCall * 4, 50),
      1,
      5_000,
    );
    const effectiveTrials = parseIntBounded(
      req.query.effectiveTrials,
      Math.max(1, Math.floor(Number(process.env.SCALP_V4_EFFECTIVE_TRIALS || 2_500_000))),
      1,
      100_000_000,
    );
    const forceValidity = parseBool(req.query.forceValidity, false);
    const autoBackfillCandles = parseBool(req.query.backfillCandles, true);
    const minCandleCoverageRatioValue = firstQueryValue(req.query.minCandleCoverageRatio).trim();
    const minCandleCoverageRatioRaw = Number(minCandleCoverageRatioValue);
    const minCandleCoverageRatio = minCandleCoverageRatioValue && Number.isFinite(minCandleCoverageRatioRaw)
      ? Math.max(0.1, Math.min(1, minCandleCoverageRatioRaw))
      : undefined;
    const candleBackfillChunkWeeks = parseIntBounded(
      req.query.candleBackfillChunkWeeks,
      Math.max(1, Math.floor(Number(process.env.SCALP_V4_CANDLE_BACKFILL_CHUNK_WEEKS || 8))),
      1,
      26,
    );
    const candleBackfillMaxRequestsPerChunk = parseIntBounded(
      req.query.candleBackfillMaxRequestsPerChunk,
      Math.max(40, Math.floor(Number(process.env.SCALP_V4_CANDLE_BACKFILL_MAX_REQUESTS_PER_CHUNK || 1200))),
      40,
      5000,
    );
    const workClaimLeaseMinutes = parseIntBounded(
      req.query.workClaimLeaseMinutes,
      Math.max(5, Math.floor(Number(process.env.SCALP_V4_WORK_LEASE_MINUTES || 120))),
      5,
      24 * 60,
    );
    const classifierVersion =
      firstQueryValue(req.query.classifierVersion).trim() || undefined;

    const job = await runScalpRegimeResearchJob({
      classifierVersion,
      forceValidity,
      maxCandidatesPerCall,
      candidateFetchLimit,
      effectiveTrials,
      autoBackfillCandles,
      minCandleCoverageRatio,
      candleBackfillChunkWeeks,
      candleBackfillMaxRequestsPerChunk,
      workClaimLeaseMs: workClaimLeaseMinutes * 60 * 1000,
    });

    return res.status(200).json({
      ok: job.ok,
      busy: job.busy,
      job,
      version: "v4",
      chaining: {
        maxCandidatesPerCall,
        candidateFetchLimit,
        effectiveTrials,
        forceValidity,
        autoBackfillCandles,
        minCandleCoverageRatio,
        candleBackfillChunkWeeks,
        candleBackfillMaxRequestsPerChunk,
        workClaimLeaseMinutes,
      },
    });
  } catch (err: any) {
    return res.status(500).json({
      ok: false,
      error: "v4_research_failed",
      message: err?.message || String(err),
    });
  }
}
