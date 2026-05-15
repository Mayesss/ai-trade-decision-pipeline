export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { listScalpV2Deployments } from "../../../../lib/scalp-v2/db";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import {
  classifyScalpV4DeploymentStatus,
  resolveScalpV4FailClosedStaleMs,
  SCALP_V4_CLASSIFIER_VERSION,
  startOfUtcWeekMondayMs,
  type ScalpV4DeploymentStatus,
} from "../../../../lib/scalp-v4";
import { scalpPrisma } from "../../../../lib/scalp/pg/client";
import { sql } from "../../../../lib/scalp/pg/sql";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const classifierVersion = SCALP_V4_CLASSIFIER_VERSION;
    const nowMs = Date.now();

    // Deployment list (compact). Filter to enabled OR envelope-bearing.
    const deployments = await listScalpV2Deployments({
      limit: 1000,
      compactPromotionGate: true,
    });
    const interesting = deployments.filter((row) => {
      if (row.enabled) return true;
      const envelope = asRecord(asRecord(row.promotionGate).regimeEnvelope);
      return Object.keys(envelope).length > 0;
    });

    // One bulk SELECT for current-week regime cells.
    const db = scalpPrisma();
    const currentWeekStartMs = startOfUtcWeekMondayMs(nowMs);
    const failClosedStaleMs = resolveScalpV4FailClosedStaleMs();
    const currentRegimeByKey = new Map<string, { cellId: string | null; stale: boolean; updatedAtMs: number | null }>();
    const venueSymbolPairs = interesting.map((row) => `${row.venue}:${row.symbol}`);
    if (venueSymbolPairs.length > 0) {
      const snaps = await db.$queryRaw<Array<{ venue: string; symbol: string; cellId: string | null; updatedAt: Date }>>(sql`
        SELECT DISTINCT ON (venue, symbol)
          venue, symbol,
          cell_id AS "cellId",
          updated_at AS "updatedAt"
        FROM scalp_regime_snapshots
        WHERE classifier_version = ${classifierVersion}
          AND granularity = 'week'
          AND week_start = ${new Date(currentWeekStartMs)}
          AND (venue || ':' || symbol) = ANY(${venueSymbolPairs}::text[])
        ORDER BY venue, symbol, updated_at DESC;
      `);
      for (const snap of snaps) {
        currentRegimeByKey.set(`${snap.venue}:${snap.symbol}`, {
          cellId: snap.cellId,
          stale: Math.max(0, nowMs - snap.updatedAt.getTime()) > failClosedStaleMs,
          updatedAtMs: snap.updatedAt.getTime(),
        });
      }
    }

    const rows = interesting.map((row) => {
      const gate = asRecord(row.promotionGate);
      const envelope = asRecord(gate.regimeEnvelope);
      const current = currentRegimeByKey.get(`${row.venue}:${row.symbol}`) || {
        cellId: null,
        stale: true,
        updatedAtMs: null,
      };
      const v4Status = classifyScalpV4DeploymentStatus({
        enabled: row.enabled,
        envelope: Object.keys(envelope).length > 0 ? envelope : null,
        currentCellId: current.cellId,
      });
      const allowedCells = Array.isArray(envelope.allowedCells)
        ? (envelope.allowedCells as unknown[]).map(String).filter(Boolean)
        : [];
      return {
        deploymentId: row.deploymentId,
        venue: row.venue,
        symbol: row.symbol,
        session: row.entrySessionProfile,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        enabled: row.enabled,
        liveMode: row.liveMode,
        v4Status,
        envelope: {
          eligible: Boolean(envelope.eligible),
          status: envelope.status || null,
          allowedCells,
          occupiedCells: Number(envelope.occupiedCells) || 0,
          strictPassingCells: Number(envelope.strictPassingCells) || 0,
        },
        currentRegime: current,
        reason: gate.reason || null,
        score: Number(gate.score) || null,
      };
    });

    const histogram: Record<ScalpV4DeploymentStatus, number> = {
      trading: 0,
      dormant_wrong_regime: 0,
      dormant_no_regime: 0,
      pending_walkforward: 0,
      eligible_not_promoted: 0,
      failed_walkforward: 0,
      disabled: 0,
    };
    for (const row of rows) histogram[row.v4Status] += 1;

    return res.status(200).json({ ok: true, classifierVersion, deployments: rows, statusHistogram: histogram });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
