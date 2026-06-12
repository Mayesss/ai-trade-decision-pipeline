// /api/scalp/v5/regimes — section 4: per-(venue, symbol) gate-decision
// aggregate. For each symbol the dashboard wants: the current cell, whether
// any deployment is stale, and counts of allow/block/pending across enabled
// deployments on that symbol.

export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp/composer/http";
import { loadV5DashboardData } from "../../../../lib/scalp/research/dashboardLoader";

interface SymbolRegimeBucket {
  venue: string;
  symbol: string;
  cellId: string | null;
  stale: boolean;
  allowCount: number;
  blockCount: number;
  pendingCount: number;
  totalEnabled: number;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const data = await loadV5DashboardData();
    const buckets = new Map<string, SymbolRegimeBucket>();
    for (const row of data.rows) {
      if (!row.enabled) continue;
      const key = `${row.venue}:${row.symbol}`;
      const isAllow = row.decision === "allow";
      const isPending = row.decision === "block_evaluator_pending";
      const isBlock = !isAllow && !isPending;
      const existing = buckets.get(key);
      if (existing) {
        existing.allowCount += isAllow ? 1 : 0;
        existing.blockCount += isBlock ? 1 : 0;
        existing.pendingCount += isPending ? 1 : 0;
        existing.totalEnabled += 1;
        existing.stale = existing.stale || row.currentCell.stale;
      } else {
        buckets.set(key, {
          venue: row.venue,
          symbol: row.symbol,
          cellId: row.currentCell.cellId,
          stale: row.currentCell.stale,
          allowCount: isAllow ? 1 : 0,
          blockCount: isBlock ? 1 : 0,
          pendingCount: isPending ? 1 : 0,
          totalEnabled: 1,
        });
      }
    }
    const regimes = Array.from(buckets.values()).sort((a, b) => {
      if (a.venue !== b.venue) return a.venue.localeCompare(b.venue);
      return a.symbol.localeCompare(b.symbol);
    });
    return res.status(200).json({
      ok: true,
      classifierVersion: data.cfg.classifierVersion,
      nowMs: data.nowMs,
      regimes,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
