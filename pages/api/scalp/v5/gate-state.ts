// /api/scalp/v5/gate-state — section 1: per-decision histograms across all
// deployments (all + enabled-only). Uses the shared dashboard loader so the
// regime snapshot fetch is one bulk SQL.

export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import {
  loadV5DashboardData,
  type V5GateDecision,
} from "../../../../lib/scalp-v5/dashboardLoader";

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const data = await loadV5DashboardData();
    const stateNow: Record<V5GateDecision, number> = {
      allow: 0,
      block_negative: 0,
      block_unseen: 0,
      block_stale: 0,
      block_evaluator_pending: 0,
      block_insufficient_trades: 0,
    };
    const stateNowEnabled: Record<V5GateDecision, number> = { ...stateNow };
    for (const row of data.rows) {
      stateNow[row.decision] += 1;
      if (row.enabled) stateNowEnabled[row.decision] += 1;
    }
    return res.status(200).json({
      ok: true,
      classifierVersion: data.cfg.classifierVersion,
      nowMs: data.nowMs,
      stateNow,
      stateNowEnabled,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
