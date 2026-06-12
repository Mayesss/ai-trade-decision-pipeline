// /api/scalp/research/gate-state — compact per-decision histograms.
//
// This endpoint intentionally does not use loadV5DashboardData(): the dashboard
// auto-refreshes gate-state every 30s, so selecting full v5 evidence JSON for
// every deployment would create avoidable Neon egress.

export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { scalpPrisma } from "../../../../lib/scalp/pg/client";
import { sql } from "../../../../lib/scalp/pg/sql";
import { resolveScalpRegimeFailClosedStaleMs } from "../../../../lib/scalp/regimes/pg";
import { startOfUtcWeekMondayMs } from "../../../../lib/scalp/regimes/week";
import { setNoStoreHeaders } from "../../../../lib/scalp/composer/http";
import { resolveScalpResearchConfig, type ScalpResearchCellStat } from "../../../../lib/scalp/research";
import { decideV5Gate, type V5GateDecision } from "../../../../lib/scalp/research/dashboardLoader";

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function parseCurrentCellStat(value: unknown): ScalpResearchCellStat | null {
  const rec = asRecord(value);
  if (!rec) return null;
  return {
    trades: Math.max(0, Math.floor(Number(rec.trades) || 0)),
    netR: Number(rec.netR) || 0,
    expectancyR: Number(rec.expectancyR) || 0,
    wins: Math.max(0, Math.floor(Number(rec.wins) || 0)),
    losses: Math.max(0, Math.floor(Number(rec.losses) || 0)),
    weeklyNetR: [],
    weeklyTrades: [],
    weeklyWins: [],
    weeklyLosses: [],
  };
}

function isConsistencyException(gateReason: unknown, promotionReason: unknown): boolean {
  const reason = String(gateReason || promotionReason || "").trim().toLowerCase();
  return reason === "v5_consistency_exception_passed";
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const cfg = resolveScalpResearchConfig();
    const nowMs = Date.now();
    const weekStartMs = startOfUtcWeekMondayMs(nowMs);
    const staleBefore = new Date(nowMs - resolveScalpRegimeFailClosedStaleMs());
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{
      enabled: boolean;
      liveMode: string | null;
      hasEvidence: boolean;
      currentCellId: string | null;
      snapshotUpdatedAt: Date | null;
      currentCellStat: unknown;
      gateReason: string | null;
      promotionReason: string | null;
    }>>(sql`
      SELECT
        d.enabled,
        d.live_mode AS "liveMode",
        d.v5_cell_evidence IS NOT NULL AS "hasEvidence",
        s.cell_id AS "currentCellId",
        s.updated_at AS "snapshotUpdatedAt",
        CASE
          WHEN s.cell_id IS NULL THEN NULL
          ELSE d.v5_cell_evidence->'cells'->s.cell_id
        END AS "currentCellStat",
        d.promotion_gate->>'reason' AS "gateReason",
        d.promotion_gate->'v5Promotion'->>'passReason' AS "promotionReason"
      FROM scalp_v2_deployments d
      LEFT JOIN LATERAL (
        SELECT cell_id, updated_at
        FROM scalp_regime_snapshots s
        WHERE s.venue = d.venue
          AND s.symbol = d.symbol
          AND s.granularity = 'week'
          AND s.week_start = ${new Date(weekStartMs)}
          AND s.classifier_version = ${cfg.classifierVersion}
        ORDER BY updated_at DESC
        LIMIT 1
      ) s ON TRUE
      WHERE d.candidate_id IS NOT NULL
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_candidates c
          WHERE c.id = d.candidate_id
            AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
        );
    `);
    const stateNow: Record<V5GateDecision, number> = {
      allow: 0,
      block_negative: 0,
      block_unseen: 0,
      block_stale: 0,
      block_evaluator_pending: 0,
      block_insufficient_trades: 0,
    };
    const stateNowEnabled: Record<V5GateDecision, number> = { ...stateNow };
    const stateNowLive: Record<V5GateDecision, number> = { ...stateNow };
    for (const row of rows) {
      const currentCellId = row.currentCellId ? String(row.currentCellId) : null;
      const currentCellStat = parseCurrentCellStat(row.currentCellStat);
      const evidence = row.hasEvidence
        ? {
            version: "scalp_v5_cell_evidence_r3" as const,
            classifierVersion: cfg.classifierVersion,
            evaluatedAtMs: 0,
            holdoutFromMs: 0,
            holdoutToMs: 0,
            minTradesPerCell: cfg.minTradesPerCell,
            eligibleCells: [],
            cells: currentCellId && currentCellStat ? { [currentCellId]: currentCellStat } : {},
          }
        : null;
      const stale = !row.snapshotUpdatedAt || row.snapshotUpdatedAt < staleBefore;
      const decision = decideV5Gate({
        evidence,
        consistencyException: isConsistencyException(row.gateReason, row.promotionReason),
        currentCellId,
        stale,
        minTradesPerCell: cfg.minTradesPerCell,
      });
      stateNow[decision] += 1;
      if (row.enabled) stateNowEnabled[decision] += 1;
      if (row.enabled && String(row.liveMode || "") === "live") stateNowLive[decision] += 1;
    }
    return res.status(200).json({
      ok: true,
      classifierVersion: cfg.classifierVersion,
      nowMs,
      stateNow,
      stateNowEnabled,
      stateNowLive,
      diagnostics: {
        rowsScanned: rows.length,
        payloadClass: "compact_no_full_v5_evidence",
      },
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
