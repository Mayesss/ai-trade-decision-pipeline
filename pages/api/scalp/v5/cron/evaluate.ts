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
import { setNoStoreHeaders } from "../../../../../lib/scalp/composer/http";
import { runScalpV5EvaluationBatch } from "../../../../../lib/scalp/research/evaluator";

function firstQueryValue(value: string | string[] | undefined): string {
  return Array.isArray(value) ? String(value[0] || "") : String(value || "");
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
    const preflightCandles = parseBool(req.query.preflightCandles, true);
    const forcePreflight = parseBool(req.query.forcePreflight, false);
    const preflightBatchSize = parseIntBounded(req.query.preflightBatchSize, 200, 1, 200);
    const preflightMaxAttempts = parseIntBounded(req.query.preflightMaxAttempts, 10, 1, 30);

    const result = await runScalpV5EvaluationBatch({
      limit,
      staleOlderThanMs: staleOlderThanHours * 60 * 60_000,
      preflightCandles,
      forcePreflight,
      preflightBatchSize,
      preflightMaxAttempts,
    });

    return res.status(200).json({
      ok: true,
      durationMs: Date.now() - startedAt,
      params: { limit, staleOlderThanHours, preflightCandles, forcePreflight, preflightBatchSize, preflightMaxAttempts },
      result: {
        processed: result.processed,
        succeeded: result.succeeded,
        failed: result.failed,
        enabled: result.enabled,
        disabled: result.disabled,
        // Mode split: how the successful evaluations were produced. In
        // steady state (after the first Sunday wave) most outcomes should
        // be `incremental`. Persistent `full` dominance means checkpoints
        // are not being preserved across runs — usually configHash drift
        // (deployment DSL changing) or a missed Sunday rollover that left
        // evidence stale for >1 week.
        fullCount: result.fullCount,
        incrementalCount: result.incrementalCount,
        skippedReason: result.skippedReason,
        preflight: result.preflight,
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
