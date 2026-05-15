export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp-v2/http";
import { SCALP_V4_CLASSIFIER_VERSION } from "../../../../lib/scalp-v4";
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
    const db = scalpPrisma();

    const [walkforwardRows, transitionRows, tradeRows] = await Promise.all([
      db.$queryRaw<Array<{
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
      `),
      db.$queryRaw<Array<{
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
      `),
      db.$queryRaw<Array<{
        ts: Date;
        deploymentId: string | null;
        venue: string | null;
        symbol: string | null;
        type: string;
        reasonCodes: string[];
        payload: unknown;
      }>>(sql`
        SELECT ts, deployment_id AS "deploymentId", venue, symbol, type, reason_codes AS "reasonCodes", payload
        FROM scalp_v2_journal
        WHERE type = 'execution'
          AND ts > NOW() - INTERVAL '30 day'
          AND (
            (payload->>'tradeEventOccurred')::boolean = true
            OR (payload->>'stateChanged')::boolean = true
            OR 'ENTRY_PLAN_READY' = ANY(reason_codes)
            OR 'ENTRY_EXECUTION_ERROR' = ANY(reason_codes)
          )
        ORDER BY ts DESC
        LIMIT 30;
      `),
    ]);

    return res.status(200).json({
      ok: true,
      classifierVersion,
      recentWalkforward: walkforwardRows.map((row) => {
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
      recentTransitions: transitionRows.map((row) => ({
        venue: row.venue,
        symbol: row.symbol,
        transitionWeekStartMs: row.transitionWeekStart.getTime(),
        fromCellId: row.fromCellId,
        toCellId: row.toCellId,
      })),
      recentTrades: tradeRows.map((row) => {
        const payload = asRecord(row.payload);
        const reasonCodes = Array.isArray(row.reasonCodes) ? row.reasonCodes : [];
        const tradeOccurred = Boolean(payload.tradeEventOccurred);
        const stateChanged = Boolean(payload.stateChanged);
        const state = String(payload.state || "");
        const tradePayload = asRecord(payload.trade);
        const eventKind = tradeOccurred
          ? "trade"
          : reasonCodes.includes("ENTRY_EXECUTION_ERROR")
            ? "entry_error"
            : reasonCodes.includes("ENTRY_PLAN_READY") && reasonCodes.includes("ENTRY_NOT_PLACED")
              ? "entry_skipped"
              : "state_change";
        const rMultiple =
          tradePayload.rMultiple !== undefined && Number.isFinite(Number(tradePayload.rMultiple))
            ? Number(tradePayload.rMultiple)
            : payload.rMultiple !== undefined && Number.isFinite(Number(payload.rMultiple))
              ? Number(payload.rMultiple)
              : null;
        return {
          tsMs: row.ts.getTime(),
          deploymentId: row.deploymentId,
          venue: row.venue,
          symbol: row.symbol,
          reasonCodes,
          eventKind,
          state,
          stateChanged,
          tradeEventOccurred: tradeOccurred,
          rMultiple,
          summary: String(payload.summary || payload.message || payload.event || state || "execution"),
        };
      }),
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
