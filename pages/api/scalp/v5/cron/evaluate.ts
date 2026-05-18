// /api/scalp/v5/cron/evaluate — scheduled v5 cell-evidence refresh.
//
// Hourly (or whatever cadence vercel.json sets) bulk eval. Replaces the
// requirement to keep `npm exec tsx scripts/research-local-bulk.ts` running
// locally — Vercel runs this server-side and keeps `v5_cell_evidence` fresh.
//
// Safe to coexist with local bulk runs: the SELECT in
// loadScalpV5DeploymentsForEvaluation orders by staleness, so a local
// process and the cron will mostly pick disjoint rows, and any overlap just
// recomputes the same evidence (idempotent upsert).

export const config = { runtime: "nodejs", maxDuration: 800 };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../../lib/scalp-v2/http";
import { runScalpV5EvaluationBatch } from "../../../../../lib/scalp-v5/evaluator";

function firstQueryValue(value: string | string[] | undefined): string {
  return Array.isArray(value) ? String(value[0] || "") : String(value || "");
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

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") {
    return res.status(405).json({ error: "Method Not Allowed", message: "Use GET" });
  }
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  const startedAt = Date.now();
  try {
    // Defaults sized for an hourly cron on a Pro-tier serverless function
    // (800s timeout). 50 deployments × ~5s each ≈ 4min; well under budget.
    const limit = parseIntBounded(req.query.limit, 50, 1, 500);
    const staleOlderThanHours = parseIntBounded(
      req.query.staleOlderThanHours,
      24 * 6, // default 6 days, matches the bulk script
      1,
      24 * 14,
    );

    const result = await runScalpV5EvaluationBatch({
      limit,
      staleOlderThanMs: staleOlderThanHours * 60 * 60_000,
    });

    return res.status(200).json({
      ok: true,
      durationMs: Date.now() - startedAt,
      params: { limit, staleOlderThanHours },
      result: {
        processed: result.processed,
        succeeded: result.succeeded,
        failed: result.failed,
        enabled: result.enabled,
        disabled: result.disabled,
        config: result.details,
      },
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: "v5_evaluate_failed",
      message: (err as Error)?.message || String(err),
      durationMs: Date.now() - startedAt,
    });
  }
}
