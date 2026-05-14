export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { listScalpV2Deployments } from "../../../../lib/scalp-v2/db";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import {
  isScalpV4Enabled,
  isScalpV4HardGateEnabled,
  loadScalpV4CurrentRegimeSnapshot,
  SCALP_V4_CLASSIFIER_VERSION,
} from "../../../../lib/scalp-v4";
import { scalpPrisma } from "../../../../lib/scalp/pg/client";
import { sql } from "../../../../lib/scalp/pg/sql";

type V4Status =
  | "trading"
  | "dormant_wrong_regime"
  | "dormant_no_regime"
  | "pending_walkforward"
  | "eligible_not_promoted"
  | "failed_walkforward"
  | "disabled";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function classifyV4Status(args: {
  enabled: boolean;
  liveMode: string;
  envelope: Record<string, unknown> | null;
  currentCellId: string | null;
}): V4Status {
  const eligible = Boolean(args.envelope?.eligible);
  const status = String(args.envelope?.status || "");
  const allowed = Array.isArray(args.envelope?.allowedCells)
    ? (args.envelope!.allowedCells as unknown[]).map(String).filter(Boolean)
    : [];
  if (!args.enabled) {
    if (eligible) return "eligible_not_promoted";
    return "disabled";
  }
  if (!args.envelope || Object.keys(args.envelope).length === 0) return "pending_walkforward";
  if (status === "no_passing_cells" || status === "regime_overbroad_auto_rejected" || status === "regime_overbroad_pending_review") {
    return "failed_walkforward";
  }
  if (!eligible) return "failed_walkforward";
  if (!args.currentCellId) return "dormant_no_regime";
  if (!allowed.includes(args.currentCellId)) return "dormant_wrong_regime";
  return "trading";
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const db = scalpPrisma();
    const classifierVersion = SCALP_V4_CLASSIFIER_VERSION;
    const nowMs = Date.now();

    // --- Aggregated counts ---
    const [stageCRow] = await db.$queryRaw<Array<{ total: bigint }>>(sql`
      SELECT COUNT(*)::bigint AS total
      FROM scalp_v2_candidates
      WHERE (metadata_json->'worker'->'stageC'->>'passed')::boolean = true
         OR (metadata_json->'worker'->>'finalPass')::boolean = true;
    `);
    const stageCSurvivors = Number(stageCRow?.total || 0);

    const walkforwardCountRows = await db.$queryRaw<Array<{ status: string; count: bigint }>>(sql`
      SELECT status, COUNT(*)::bigint AS count
      FROM scalp_regime_walkforward_results
      WHERE classifier_version = ${classifierVersion}
      GROUP BY status;
    `);
    const walkforwardCounts: Record<string, number> = {};
    for (const row of walkforwardCountRows) walkforwardCounts[row.status] = Number(row.count);
    const walkforwardTotal = Object.values(walkforwardCounts).reduce((a, b) => a + b, 0);
    const pendingWalkforward = Math.max(0, stageCSurvivors - walkforwardTotal);

    const [regimeBuildRow] = await db.$queryRaw<Array<{ symbols: bigint; weekstart: Date | null }>>(sql`
      SELECT COUNT(DISTINCT venue || ':' || symbol)::bigint AS symbols, MAX(week_start) AS weekstart
      FROM scalp_regime_snapshots
      WHERE classifier_version = ${classifierVersion};
    `);

    // --- Recent walkforward results ---
    const recentWalkforward = await db.$queryRaw<Array<{
      deploymentId: string;
      venue: string;
      symbol: string;
      status: string;
      evaluatedAt: Date;
      durationMs: bigint | null;
      envelopeJson: unknown;
    }>>(sql`
      SELECT
        deployment_id AS "deploymentId",
        venue, symbol, status,
        evaluated_at AS "evaluatedAt",
        (details_json->>'durationMs')::bigint AS "durationMs",
        envelope_json AS "envelopeJson"
      FROM scalp_regime_walkforward_results
      WHERE classifier_version = ${classifierVersion}
      ORDER BY evaluated_at DESC
      LIMIT 60;
    `);

    // --- Recent regime transitions ---
    const recentTransitions = await db.$queryRaw<Array<{
      venue: string;
      symbol: string;
      transitionWeekStart: Date;
      fromCellId: string | null;
      toCellId: string;
    }>>(sql`
      SELECT venue, symbol,
        transition_week_start AS "transitionWeekStart",
        from_cell_id AS "fromCellId",
        to_cell_id AS "toCellId"
      FROM scalp_regime_transitions
      WHERE classifier_version = ${classifierVersion}
      ORDER BY transition_week_start DESC, created_at DESC
      LIMIT 40;
    `);

    // --- Deployments + per-deployment v4 status + current regime ---
    const deployments = await listScalpV2Deployments({
      limit: 1000,
      compactPromotionGate: true,
    });

    const enabledOrEnvelopeBearing = deployments.filter((row) => {
      if (row.enabled) return true;
      const envelope = asRecord(asRecord(row.promotionGate).regimeEnvelope);
      return Object.keys(envelope).length > 0;
    });

    const currentRegimeByKey = new Map<string, { cellId: string | null; stale: boolean; updatedAtMs: number | null }>();
    await Promise.all(
      enabledOrEnvelopeBearing.slice(0, 80).map(async (row) => {
        const snap = await loadScalpV4CurrentRegimeSnapshot({
          venue: row.venue,
          symbol: row.symbol,
          nowMs,
        }).catch(() => ({ cellId: null, stale: true, snapshot: null }));
        currentRegimeByKey.set(`${row.venue}:${row.symbol}`, {
          cellId: snap.cellId,
          stale: snap.stale,
          updatedAtMs: Number((snap.snapshot as any)?.updatedAtMs) || null,
        });
      }),
    );

    const deploymentRows = enabledOrEnvelopeBearing.map((row) => {
      const gate = asRecord(row.promotionGate);
      const envelope = asRecord(gate.regimeEnvelope);
      const regimeKey = `${row.venue}:${row.symbol}`;
      const current = currentRegimeByKey.get(regimeKey) || { cellId: null, stale: true, updatedAtMs: null };
      const v4Status = classifyV4Status({
        enabled: row.enabled,
        liveMode: row.liveMode,
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
        updatedAtMs: row.updatedAtMs,
      };
    });

    // --- v4-status histogram for the system health card ---
    const statusHistogram: Record<V4Status, number> = {
      trading: 0,
      dormant_wrong_regime: 0,
      dormant_no_regime: 0,
      pending_walkforward: 0,
      eligible_not_promoted: 0,
      failed_walkforward: 0,
      disabled: 0,
    };
    for (const row of deploymentRows) statusHistogram[row.v4Status] += 1;

    return res.status(200).json({
      ok: true,
      classifierVersion,
      v4Enabled: isScalpV4Enabled(),
      v4HardGateEnabled: isScalpV4HardGateEnabled(),
      stageCSurvivors,
      walkforwardCounts,
      walkforwardTotal,
      pendingWalkforward,
      regimeBuild: {
        symbolsCovered: Number(regimeBuildRow?.symbols || 0),
        latestWeekStartMs: regimeBuildRow?.weekstart ? regimeBuildRow.weekstart.getTime() : null,
      },
      statusHistogram,
      deployments: deploymentRows,
      recentWalkforward: recentWalkforward.map((row) => {
        const env = asRecord(row.envelopeJson);
        return {
          deploymentId: row.deploymentId,
          venue: row.venue,
          symbol: row.symbol,
          status: row.status,
          eligible: Boolean(env.eligible),
          allowedCells: Array.isArray(env.allowedCells)
            ? (env.allowedCells as unknown[]).map(String).filter(Boolean)
            : [],
          strictPassingCells: Number(env.strictPassingCells) || 0,
          occupiedCells: Number(env.occupiedCells) || 0,
          evaluatedAtMs: row.evaluatedAt.getTime(),
          durationMs: row.durationMs ? Number(row.durationMs) : null,
        };
      }),
      recentTransitions: recentTransitions.map((row) => ({
        venue: row.venue,
        symbol: row.symbol,
        transitionWeekStartMs: row.transitionWeekStart.getTime(),
        fromCellId: row.fromCellId,
        toCellId: row.toCellId,
      })),
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: (err as Error)?.message || String(err),
    });
  }
}
