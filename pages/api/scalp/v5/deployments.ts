// /api/scalp/v5/deployments — section 3: per-deployment cell evidence with
// the full weekly-NetR arrays the UI renders as bar tracks. Heaviest payload
// of the four endpoints; isolated so the lighter sections (coverage,
// gate-state, regimes) render even if this one runs long.

export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import {
  loadV5DashboardData,
  shapeCellsForDeployment,
} from "../../../../lib/scalp-v5/dashboardLoader";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const data = await loadV5DashboardData();
    // Sort: enabled rows first (kept visible at the top), then totalNetR
    // descending so the winners surface within each group.
    const sortedRows = [...data.rows].sort((a, b) => {
      if (a.enabled !== b.enabled) return a.enabled ? -1 : 1;
      return b.totalNetR - a.totalNetR;
    });
    const deployments = sortedRows.map((row) => {
      const cells = shapeCellsForDeployment(row);
      const totalTrades = cells.reduce((acc, c) => acc + c.trades, 0);
      return {
        deploymentId: row.deploymentId,
        venue: row.venue,
        symbol: row.symbol,
        session: row.session,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        enabled: row.enabled,
        v5Enabled: row.v5Enabled,
        v5EvaluatedAtMs: row.v5EvaluatedAtMs,
        currentCell: row.currentCell,
        gate: {
          decision: row.decision,
          currentCellStat: row.currentCellStat
            ? {
                trades: row.currentCellStat.trades,
                expectancyR: row.currentCellStat.expectancyR,
                netR: row.currentCellStat.netR,
                wins: row.currentCellStat.wins,
                losses: row.currentCellStat.losses,
              }
            : null,
          eligibleCells: row.evidence?.eligibleCells ?? [],
        },
        holdoutWindow: row.evidence
          ? { fromMs: row.evidence.holdoutFromMs, toMs: row.evidence.holdoutToMs }
          : null,
        totalTrades,
        totalNetR: row.totalNetR,
        cells,
      };
    });
    return res.status(200).json({
      ok: true,
      classifierVersion: data.cfg.classifierVersion,
      nowMs: data.nowMs,
      deployments,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
