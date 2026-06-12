export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { listScalpComposerDeployments } from "../../../../lib/scalp/composer/db";
import { setNoStoreHeaders } from "../../../../lib/scalp/composer/http";
import {
  classifyScalpRegimeDeploymentStatus,
  resolveScalpRegimeFailClosedStaleMs,
  SCALP_REGIME_CLASSIFIER_VERSION,
  startOfUtcWeekMondayMs,
  type ScalpRegimeDeploymentStatus,
} from "../../../../lib/scalp/regimes";
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
    const classifierVersion = SCALP_REGIME_CLASSIFIER_VERSION;
    const nowMs = Date.now();

    // Deployment list (compact). Filter to enabled OR envelope-bearing.
    const deployments = await listScalpComposerDeployments({
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
    const failClosedStaleMs = resolveScalpRegimeFailClosedStaleMs();
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

    // Enrich with live trade signals + regime cell age. Three bulk queries.
    const deploymentIds = interesting.map((row) => row.deploymentId);
    const venueSymbolUniquePairs = Array.from(
      new Set(interesting.map((row) => `${row.venue}:${row.symbol}`)),
    );
    const lastEntryByDeployment = new Map<string, number>();
    const openPositionByDeployment = new Map<string, number>();
    const weeksInCellByVenueSymbol = new Map<string, number>();
    if (deploymentIds.length > 0) {
      const [journalRows, positionRows] = await Promise.all([
        db.$queryRaw<Array<{ deploymentId: string; lastTs: Date }>>(sql`
          SELECT deployment_id AS "deploymentId", MAX(ts) AS "lastTs"
          FROM scalp_v2_journal
          WHERE deployment_id = ANY(${deploymentIds}::text[])
            AND type = 'execution'
          GROUP BY deployment_id;
        `),
        db.$queryRaw<Array<{ deploymentId: string; n: bigint }>>(sql`
          SELECT deployment_id AS "deploymentId", COUNT(*)::bigint AS n
          FROM scalp_v2_positions
          WHERE deployment_id = ANY(${deploymentIds}::text[])
            AND status <> 'flat'
            AND status <> 'closed'
          GROUP BY deployment_id;
        `),
      ]);
      for (const row of journalRows) lastEntryByDeployment.set(row.deploymentId, row.lastTs.getTime());
      for (const row of positionRows) openPositionByDeployment.set(row.deploymentId, Number(row.n));
    }
    if (venueSymbolUniquePairs.length > 0) {
      const transitionRows = await db.$queryRaw<Array<{
        venue: string;
        symbol: string;
        lastTransition: Date | null;
      }>>(sql`
        SELECT venue, symbol, MAX(transition_week_start) AS "lastTransition"
        FROM scalp_regime_transitions
        WHERE classifier_version = ${classifierVersion}
          AND from_cell_id IS NOT NULL
          AND (lower(venue) || ':' || upper(symbol)) = ANY(${venueSymbolUniquePairs.map((s) => s.toLowerCase().split(':')[0] + ':' + s.split(':')[1]!.toUpperCase())}::text[])
        GROUP BY venue, symbol;
      `);
      for (const row of transitionRows) {
        if (!row.lastTransition) continue;
        const weeks = Math.max(
          1,
          Math.floor((currentWeekStartMs - row.lastTransition.getTime()) / (7 * 24 * 60 * 60_000)) + 1,
        );
        weeksInCellByVenueSymbol.set(`${row.venue}:${row.symbol}`, weeks);
      }
    }

    // Load per-cell window arrays from walkforward_results.incremental_state_json
    // for eligible deployments. Needed for the per-cell sparklines in the UI.
    const eligibleDeploymentIds = interesting
      .filter((row) => {
        const env = asRecord(asRecord(row.promotionGate).regimeEnvelope);
        return Boolean(env.eligible);
      })
      .map((row) => row.deploymentId);
    const windowExpectancyByDepCell = new Map<string, Map<string, number[]>>();
    if (eligibleDeploymentIds.length > 0) {
      const wfRows = await db.$queryRaw<Array<{
        deploymentId: string;
        incrementalStateJson: unknown;
      }>>(sql`
        SELECT DISTINCT ON (deployment_id)
          deployment_id AS "deploymentId",
          incremental_state_json AS "incrementalStateJson"
        FROM scalp_regime_walkforward_results
        WHERE classifier_version = ${classifierVersion}
          AND deployment_id = ANY(${eligibleDeploymentIds}::text[])
          AND incremental_state_json IS NOT NULL
        ORDER BY deployment_id, evaluated_at DESC;
      `);
      for (const row of wfRows) {
        const state = asRecord(row.incrementalStateJson);
        const cells = asRecord(state.cells);
        const byCell = new Map<string, number[]>();
        for (const [cellId, value] of Object.entries(cells)) {
          const stat = asRecord(value);
          const arr = Array.isArray(stat.windowExpectancyR)
            ? (stat.windowExpectancyR as unknown[]).map((v) => Number(v)).filter(Number.isFinite)
            : [];
          if (arr.length > 0) byCell.set(cellId, arr);
        }
        windowExpectancyByDepCell.set(row.deploymentId, byCell);
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
      const lastEntryAtMs = lastEntryByDeployment.get(row.deploymentId) ?? null;
      const openPositionCount = openPositionByDeployment.get(row.deploymentId) ?? 0;
      const weeksInCurrentCell = weeksInCellByVenueSymbol.get(`${row.venue}:${row.symbol}`) ?? null;
      const v4Status = classifyScalpRegimeDeploymentStatus({
        enabled: row.enabled,
        envelope: Object.keys(envelope).length > 0 ? envelope : null,
        currentCellId: current.cellId,
      });
      const allowedCells = Array.isArray(envelope.allowedCells)
        ? (envelope.allowedCells as unknown[]).map(String).filter(Boolean)
        : [];
      // Pass through full cell aggregates so the UI can show per-cell KPI
      // details on demand. Stripped to a small subset of useful fields per
      // cell, plus the per-window expectancy array from incremental state.
      const cellsRaw: Array<Record<string, unknown>> = Array.isArray(envelope.cells)
        ? (envelope.cells as Array<Record<string, unknown>>)
        : [];
      const winExpByCell = windowExpectancyByDepCell.get(row.deploymentId) || new Map<string, number[]>();
      const cells = cellsRaw.map((cell) => {
        const cellId = String(cell.cellId || "");
        const deflated = asRecord(cell.deflatedSharpe);
        return {
          cellId,
          windows: Number(cell.windows) || 0,
          trades: Number(cell.trades) || 0,
          distinctEpochCount: Number(cell.distinctEpochCount) || 0,
          netR: Number(cell.netR) || 0,
          expectancyR: Number(cell.expectancyR) || 0,
          positiveWindowPct: Number(cell.positiveWindowPct) || 0,
          p25ExpectancyR: Number(cell.p25ExpectancyR) || 0,
          maxDrawdownR: Number(cell.maxDrawdownR) || 0,
          crossRegimeTradePct: Number(cell.crossRegimeTradePct) || 0,
          bootstrapP05ExpectancyR:
            cell.bootstrapP05ExpectancyR === null || cell.bootstrapP05ExpectancyR === undefined
              ? null
              : Number(cell.bootstrapP05ExpectancyR),
          sharpe: deflated.sharpe === null || deflated.sharpe === undefined ? null : Number(deflated.sharpe),
          deflatedScore: deflated.diagnosticScore === null || deflated.diagnosticScore === undefined ? null : Number(deflated.diagnosticScore),
          strictPassed: Boolean(cell.strictPassed),
          relaxedPassed: Boolean(cell.relaxedPassed),
          reason: cell.reason ? String(cell.reason) : null,
          windowExpectancyR: winExpByCell.get(cellId) || null,
        };
      });
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
          cells,
        },
        currentRegime: { ...current, weeksInCell: weeksInCurrentCell },
        lastEntryAtMs,
        openPositionCount,
        reason: gate.reason || null,
        score: Number(gate.score) || null,
      };
    });

    const histogram: Record<ScalpRegimeDeploymentStatus, number> = {
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
