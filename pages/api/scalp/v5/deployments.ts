// /api/scalp/v5/deployments — section 3: per-deployment cell evidence with
// the full weekly-NetR arrays the UI renders as bar tracks. Heaviest payload
// of the four endpoints; isolated so the lighter sections (coverage,
// gate-state, regimes) render even if this one runs long.

export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp/composer/http";
import {
  loadV5DashboardData,
  shapeCellsForDeployment,
  type V5DashboardScope,
} from "../../../../lib/scalp/research/dashboardLoader";

function firstQueryValue(value: string | string[] | undefined): string {
  return Array.isArray(value) ? String(value[0] || "") : String(value || "");
}

function parseScope(value: string | string[] | undefined): V5DashboardScope {
  const raw = firstQueryValue(value).trim().toLowerCase();
  if (raw === "enabled" || raw === "inactive" || raw === "all") return raw;
  return "live";
}

function parseIntBounded(value: string | string[] | undefined, fallback: number, min: number, max: number): number {
  const raw = firstQueryValue(value).trim();
  if (!raw) return fallback;
  const parsed = Math.floor(Number(raw));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(max, parsed));
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const scope = parseScope(req.query.scope);
    const limit = parseIntBounded(req.query.limit, scope === "live" ? 100 : 50, 1, 250);
    const offset = parseIntBounded(req.query.offset, 0, 0, 100_000);
    const data = await loadV5DashboardData({ scope, limit, offset });
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
        liveMode: row.liveMode,
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
      page: data.page,
      diagnostics: {
        payloadClass: scope === "live" ? "live_deployments" : "paged_deployments",
      },
      deployments,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
