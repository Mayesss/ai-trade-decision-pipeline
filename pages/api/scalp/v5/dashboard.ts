export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import {
  loadScalpV4CurrentRegimeSnapshot,
  resolveScalpV4FailClosedStaleMs,
} from "../../../../lib/scalp-v4";
import {
  isScalpV5Enabled,
  isScalpV5HardGateEnabled,
  resolveScalpV5Config,
  type ScalpV5CellEvidence,
  type ScalpV5CellStat,
} from "../../../../lib/scalp-v5";
import { scalpPrisma } from "../../../../lib/scalp/pg/client";
import { sql } from "../../../../lib/scalp/pg/sql";

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function parseCellStat(value: unknown): ScalpV5CellStat | null {
  const rec = asRecord(value);
  if (!rec) return null;
  const weekly = Array.isArray(rec.weeklyNetR)
    ? (rec.weeklyNetR as unknown[]).map((v) => Number(v) || 0)
    : [];
  return {
    trades: Math.max(0, Math.floor(Number(rec.trades) || 0)),
    netR: Number(rec.netR) || 0,
    expectancyR: Number(rec.expectancyR) || 0,
    wins: Math.max(0, Math.floor(Number(rec.wins) || 0)),
    losses: Math.max(0, Math.floor(Number(rec.losses) || 0)),
    weeklyNetR: weekly,
  };
}

function parseEvidence(value: unknown): ScalpV5CellEvidence | null {
  const rec = asRecord(value);
  if (!rec) return null;
  const cellsRec = asRecord(rec.cells) || {};
  const cells: Record<string, ScalpV5CellStat> = {};
  for (const [cellId, raw] of Object.entries(cellsRec)) {
    const stat = parseCellStat(raw);
    if (stat) cells[cellId] = stat;
  }
  const eligibleCells = Array.isArray(rec.eligibleCells)
    ? (rec.eligibleCells as unknown[]).map((v) => String(v))
    : [];
  return {
    version: (rec.version as ScalpV5CellEvidence["version"]) ?? "scalp_v5_cell_evidence_r2",
    classifierVersion: String(rec.classifierVersion || ""),
    evaluatedAtMs: Number(rec.evaluatedAtMs) || 0,
    holdoutFromMs: Number(rec.holdoutFromMs) || 0,
    holdoutToMs: Number(rec.holdoutToMs) || 0,
    minTradesPerCell: Math.max(0, Math.floor(Number(rec.minTradesPerCell) || 0)),
    cells,
    eligibleCells,
  };
}

type GateDecision =
  | "allow"
  | "block_negative"
  | "block_unseen"
  | "block_stale"
  | "block_evaluator_pending"
  | "block_insufficient_trades";

function decideGate(params: {
  evidence: ScalpV5CellEvidence | null;
  currentCellId: string | null;
  stale: boolean;
  minTradesPerCell: number;
}): GateDecision {
  if (!params.evidence) return "block_evaluator_pending";
  if (params.stale || !params.currentCellId) return "block_stale";
  const cellStat = params.evidence.cells[params.currentCellId];
  if (!cellStat) return "block_unseen";
  if (cellStat.trades < params.minTradesPerCell) return "block_insufficient_trades";
  if (cellStat.expectancyR <= 0) return "block_negative";
  return "allow";
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const db = scalpPrisma();
    const cfg = resolveScalpV5Config();
    const nowMs = Date.now();
    const staleThresholdMs = Math.max(
      60 * 60_000,
      Math.floor(
        Number(process.env.BULK_V5_STALE_OLDER_THAN_HOURS) > 0
          ? Number(process.env.BULK_V5_STALE_OLDER_THAN_HOURS) * 60 * 60_000
          : 6 * 24 * 60 * 60_000,
      ),
    );

    type DeploymentDbRow = {
      deploymentId: string;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
      enabled: boolean;
      liveMode: string | null;
      v5Enabled: boolean;
      v5EvaluatedAt: Date | null;
      v5CellEvidence: unknown;
    };
    const rows = await db.$queryRaw<DeploymentDbRow[]>(sql`
      SELECT
        deployment_id AS "deploymentId",
        venue,
        symbol,
        strategy_id AS "strategyId",
        tune_id AS "tuneId",
        entry_session_profile AS "entrySessionProfile",
        enabled,
        live_mode AS "liveMode",
        COALESCE(v5_enabled, FALSE) AS "v5Enabled",
        v5_evaluated_at AS "v5EvaluatedAt",
        v5_cell_evidence AS "v5CellEvidence"
      FROM scalp_v2_deployments
      WHERE enabled = TRUE
      ORDER BY symbol ASC, entry_session_profile ASC;
    `);

    let latestEvaluationMs: number | null = null;
    let oldestEvaluationMs: number | null = null;
    let evaluatedCount = 0;
    let missingCount = 0;
    let staleCount = 0;
    const stateNow: Record<GateDecision, number> = {
      allow: 0,
      block_negative: 0,
      block_unseen: 0,
      block_stale: 0,
      block_evaluator_pending: 0,
      block_insufficient_trades: 0,
    };

    // Per-deployment regime lookups. Done sequentially with .catch fallback
    // because the snapshot loader is cheap (single row per call), and doing
    // it in parallel risks fanning Neon connections.
    const failClosedStaleMs = resolveScalpV4FailClosedStaleMs();
    const enrichedDeployments: Array<Record<string, unknown>> = [];
    for (const row of rows) {
      const venue = String(row.venue || "").toLowerCase() === "capital" ? "capital" : "bitget";
      const evaluatedAtMs = row.v5EvaluatedAt ? row.v5EvaluatedAt.getTime() : null;
      const evidence = parseEvidence(row.v5CellEvidence);
      if (evaluatedAtMs) {
        evaluatedCount += 1;
        latestEvaluationMs = Math.max(latestEvaluationMs ?? 0, evaluatedAtMs);
        oldestEvaluationMs = Math.min(oldestEvaluationMs ?? evaluatedAtMs, evaluatedAtMs);
        if (nowMs - evaluatedAtMs > staleThresholdMs) staleCount += 1;
      } else {
        missingCount += 1;
      }
      const snap = await loadScalpV4CurrentRegimeSnapshot({
        venue: venue as "bitget" | "capital",
        symbol: row.symbol,
        nowMs,
      }).catch(() => ({ cellId: null, stale: true, snapshot: null }));
      const currentCellId = snap.cellId;
      const snapUpdatedAtMs = snap.snapshot && typeof snap.snapshot.updatedAtMs === "number"
        ? (snap.snapshot.updatedAtMs as number)
        : null;
      const regimeStale = Boolean(snap.stale) || (
        snapUpdatedAtMs === null || nowMs - snapUpdatedAtMs > failClosedStaleMs
      );
      const decision = decideGate({
        evidence,
        currentCellId,
        stale: regimeStale,
        minTradesPerCell: cfg.minTradesPerCell,
      });
      stateNow[decision] += 1;

      // Ordered cells: current cell first (if present), then by trade count
      // descending. Caps the payload at 12 cells per deployment — beyond
      // that, the UI summary is already noise.
      const cellsArr = Object.entries(evidence?.cells || {}).map(([cellId, stat]) => ({
        cellId,
        trades: stat.trades,
        netR: stat.netR,
        expectancyR: stat.expectancyR,
        wins: stat.wins,
        losses: stat.losses,
        weeklyNetR: stat.weeklyNetR,
        isCurrent: currentCellId === cellId,
      }));
      cellsArr.sort((a, b) => {
        if (a.isCurrent && !b.isCurrent) return -1;
        if (!a.isCurrent && b.isCurrent) return 1;
        return b.trades - a.trades;
      });
      const cells = cellsArr.slice(0, 12);

      const currentEvidence = currentCellId ? evidence?.cells[currentCellId] : null;
      enrichedDeployments.push({
        deploymentId: row.deploymentId,
        venue,
        symbol: row.symbol,
        session: row.entrySessionProfile,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        v5Enabled: Boolean(row.v5Enabled),
        v5EvaluatedAtMs: evaluatedAtMs,
        currentCell: {
          cellId: currentCellId,
          stale: regimeStale,
          updatedAtMs: snapUpdatedAtMs,
        },
        gate: {
          decision,
          currentCellStat: currentEvidence
            ? {
                trades: currentEvidence.trades,
                expectancyR: currentEvidence.expectancyR,
                netR: currentEvidence.netR,
                wins: currentEvidence.wins,
                losses: currentEvidence.losses,
              }
            : null,
          eligibleCells: evidence?.eligibleCells ?? [],
        },
        holdoutWindow: evidence
          ? { fromMs: evidence.holdoutFromMs, toMs: evidence.holdoutToMs }
          : null,
        totalTrades: cellsArr.reduce((acc, c) => acc + c.trades, 0),
        cells,
      });
    }

    return res.status(200).json({
      ok: true,
      classifierVersion: cfg.classifierVersion,
      v5Enabled: isScalpV5Enabled(),
      v5HardGateEnabled: isScalpV5HardGateEnabled(),
      config: {
        holdoutWeeks: cfg.holdoutWeeks,
        minTradesPerCell: cfg.minTradesPerCell,
      },
      nowMs,
      evaluator: {
        latestEvaluationMs,
        oldestEvaluationMs,
      },
      coverage: {
        enabledDeployments: rows.length,
        evaluated: evaluatedCount,
        missingEvidence: missingCount,
        stale: staleCount,
        staleThresholdMs,
      },
      stateNow,
      deployments: enrichedDeployments,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
