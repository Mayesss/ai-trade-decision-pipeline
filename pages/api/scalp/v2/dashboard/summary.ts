export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../../lib/admin";
import {
  listScalpV2Candidates,
  listScalpV2Deployments,
  listScalpV2ExecutionEvents,
  listScalpV2Jobs,
  listScalpV2RecentLedger,
  listScalpV2ResearchCursors,
  listScalpV2ResearchHighlights,
  loadScalpV2RuntimeConfig,
  loadScalpV2Summary,
} from "../../../../../lib/scalp-v2/db";
import {
  parseSession,
  parseVenue,
  parseIntBounded,
  setNoStoreHeaders,
} from "../../../../../lib/scalp-v2/http";

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
    const eventLimit = parseIntBounded(req.query.eventLimit, 120, 10, 2_000);
    const ledgerLimit = parseIntBounded(req.query.ledgerLimit, 120, 10, 2_000);
    const deploymentLimit = parseIntBounded(req.query.deploymentLimit, 300, 10, 5_000);
    const jobLimit = parseIntBounded(req.query.jobLimit, 20, 5, 100);
    const candidateLimit = parseIntBounded(
      req.query.candidateLimit,
      2_000,
      100,
      10_000,
    );
    const session = parseSession(req.query.session);
    const venue = parseVenue(req.query.venue);

    const [runtime, summary, deployments, events, ledger, jobs, candidates, researchCursors, researchHighlights] = await Promise.all([
      loadScalpV2RuntimeConfig(),
      loadScalpV2Summary(),
      listScalpV2Deployments({ limit: deploymentLimit, session, venue }),
      listScalpV2ExecutionEvents({ limit: eventLimit }),
      listScalpV2RecentLedger({ limit: ledgerLimit }),
      listScalpV2Jobs({ limit: jobLimit }),
      listScalpV2Candidates({ limit: candidateLimit, session, venue }),
      listScalpV2ResearchCursors({ venue, entrySessionProfile: session }),
      listScalpV2ResearchHighlights({ venue, entrySessionProfile: session }),
    ]);

    // Build workerRows from v2 candidate metadata so the Deployment
    // Coverage grid can display stage-C weekly backtest results.
    type WorkerRow = {
      deploymentId: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      weekStartMs: number;
      weekEndMs: number;
      status: string;
      trades: number | null;
      netR: number | null;
      expectancyR: number | null;
      profitFactor: number | null;
      maxDrawdownR: number | null;
    };
    const workerRows: WorkerRow[] = [];
    const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;
    for (const candidate of candidates) {
      const meta = (candidate.metadata || {}) as Record<string, any>;
      const worker = meta.worker || {};
      const stageC = worker.stageC || {};
      const weeklyNetR = stageC.weeklyNetR as Record<string, number> | undefined;
      if (!weeklyNetR || typeof weeklyNetR !== "object") continue;

      const deploymentId = meta.deploymentId
        || `${candidate.venue}:${candidate.symbol}~${candidate.strategyId}~${candidate.tuneId}__sp_${candidate.entrySessionProfile}`;

      for (const [weekStartStr, netR] of Object.entries(weeklyNetR)) {
        const weekStartMs = Number(weekStartStr);
        if (!Number.isFinite(weekStartMs)) continue;
        workerRows.push({
          deploymentId,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          weekStartMs,
          weekEndMs: weekStartMs + ONE_WEEK_MS,
          status: "succeeded",
          trades: null,
          netR: typeof netR === "number" && Number.isFinite(netR) ? netR : null,
          expectancyR: null,
          profitFactor: null,
          maxDrawdownR: null,
        });
      }

      // Also inject stage-level aggregates if available
      for (const stageKey of ["stageA", "stageB", "stageC"] as const) {
        const stage = worker[stageKey];
        if (!stage || !stage.executed) continue;
        const fromTs = Number(stage.fromTs);
        const toTs = Number(stage.toTs);
        if (!Number.isFinite(fromTs) || !Number.isFinite(toTs)) continue;
        // Only add the aggregate if we didn't already add weekly rows for it
        if (stageKey === "stageC" && weeklyNetR && Object.keys(weeklyNetR).length > 0) continue;
        workerRows.push({
          deploymentId,
          symbol: candidate.symbol,
          strategyId: candidate.strategyId,
          tuneId: candidate.tuneId,
          weekStartMs: fromTs,
          weekEndMs: toTs,
          status: stage.passed ? "succeeded" : "failed",
          trades: typeof stage.trades === "number" ? stage.trades : null,
          netR: typeof stage.netR === "number" ? stage.netR : null,
          expectancyR: typeof stage.expectancyR === "number" ? stage.expectancyR : null,
          profitFactor: typeof stage.profitFactor === "number" ? stage.profitFactor : null,
          maxDrawdownR: typeof stage.maxDrawdownR === "number" ? stage.maxDrawdownR : null,
        });
      }
    }

    return res.status(200).json({
      ok: true,
      mode: "scalp_v2",
      runtime,
      summary,
      deployments,
      events,
      ledger,
      jobs,
      candidates,
      researchCursors,
      researchHighlights,
      workerRows,
    });
  } catch (err: any) {
    return res.status(500).json({
      error: "scalp_v2_dashboard_summary_failed",
      message: err?.message || String(err),
    });
  }
}
