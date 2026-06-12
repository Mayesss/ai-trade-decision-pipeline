export const config = { runtime: "nodejs" };

import type { NextApiRequest, NextApiResponse } from "next";

import { requireAdminAccess } from "../../../../lib/admin";
import { setNoStoreHeaders } from "../../../../lib/scalp/composer/http";
import { SCALP_REGIME_CLASSIFIER_VERSION } from "../../../../lib/scalp/regimes";
import { scalpPrisma } from "../../../../lib/scalp/pg/client";
import { sql } from "../../../../lib/scalp/pg/sql";

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function hasAnyReason(reasonCodes: string[], values: string[]): boolean {
  const set = new Set(reasonCodes);
  return values.some((value) => set.has(value));
}

function isCloseReason(reasonCodes: string[]): boolean {
  return hasAnyReason(reasonCodes, [
    "TRADE_CLOSE_CONFIRMED",
    "TRADE_CLOSE_OWNED_POSITION_NOT_FOUND",
    "TRADE_EXIT_ASSUMED_BROKER_CLOSED",
    "TRADE_EXITED_READY_NEXT_SETUP",
    "TRADE_EXIT_STOP_HIT",
    "TRADE_EXIT_TP_HIT",
    "TRADE_EXIT_TIME_STOP",
    "SCALP_COMPOSER_RECONCILE_CLOSE",
    "BROKER_OWNED_POSITION_NOT_FOUND_MARK_DONE",
  ]);
}

function labelCloseType(value: unknown): string {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/_/g, " ");
  return normalized || "closed";
}

function labelLedgerClose(row: { closeType: string; reasonCodes: string[] }): string {
  const reasonCodes = Array.isArray(row.reasonCodes) ? row.reasonCodes : [];
  if (hasAnyReason(reasonCodes, ["TRADE_EXIT_TIME_STOP"])) {
    return "time stop";
  }
  if (hasAnyReason(reasonCodes, ["TRADE_EXIT_TP_HIT", "SCALP_COMPOSER_RECONCILE_TP"])) {
    return "take profit";
  }
  if (
    String(row.closeType || "").toLowerCase() === "stop_loss" &&
    hasAnyReason(reasonCodes, ["TRAIL_STOP_UPDATED"])
  ) {
    return "trailing stop";
  }
  return labelCloseType(row.closeType);
}

function brokerPayloadSummary(value: unknown): Record<string, unknown> | null {
  const payload = asRecord(value);
  const close = asRecord(payload.bitgetBrokerClose);
  if (Object.keys(close).length === 0) return null;
  return {
    position: close.position ?? null,
    entryOrder: close.entryOrder ?? null,
    closeOrders: close.closeOrders ?? null,
  };
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== "GET") return res.status(405).json({ error: "Method Not Allowed" });
  if (!requireAdminAccess(req, res)) return;
  setNoStoreHeaders(res);

  try {
    const classifierVersion = SCALP_REGIME_CLASSIFIER_VERSION;
    const db = scalpPrisma();

    const [walkforwardRows, transitionRows, journalRows, ledgerRows, dailyRows] = await Promise.all([
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
        LIMIT 80;
      `),
      db.$queryRaw<Array<{
        tsExit: Date;
        deploymentId: string;
        venue: string;
        symbol: string;
        closeType: string;
        rMultiple: number | null;
        pnlUsd: number | null;
        sourceOfTruth: string;
        reasonCodes: string[];
        rawPayload: unknown;
      }>>(sql`
        SELECT
          ts_exit AS "tsExit",
          deployment_id AS "deploymentId",
          venue,
          symbol,
          close_type AS "closeType",
          r_multiple::double precision AS "rMultiple",
          pnl_usd::double precision AS "pnlUsd",
          source_of_truth AS "sourceOfTruth",
          reason_codes AS "reasonCodes",
          raw_payload AS "rawPayload"
        FROM scalp_v2_ledger
        WHERE ts_exit > NOW() - INTERVAL '30 day'
        ORDER BY ts_exit DESC
        LIMIT 80;
      `),
      db.$queryRaw<Array<{
        dayKey: Date | string;
        trades: number | bigint;
        wins: number | bigint;
        losses: number | bigint;
        netR: number | null;
        pnlUsd: number | null;
      }>>(sql`
        SELECT
          day_key AS "dayKey",
          SUM(trades)::int AS trades,
          SUM(wins)::int AS wins,
          SUM(losses)::int AS losses,
          COALESCE(SUM(net_r), 0)::double precision AS "netR",
          COALESCE(SUM(net_pnl_usd), 0)::double precision AS "pnlUsd"
        FROM scalp_v2_metrics_daily
        WHERE day_key >= ((NOW() AT TIME ZONE 'UTC')::date - INTERVAL '35 day')
        GROUP BY day_key
        ORDER BY day_key ASC;
      `),
    ]);

    const recentTrades = [
      ...ledgerRows.map((row) => {
        const reasonCodes = Array.isArray(row.reasonCodes) ? row.reasonCodes : [];
        return {
          tsMs: row.tsExit.getTime(),
          deploymentId: row.deploymentId,
          venue: row.venue,
          symbol: row.symbol,
          reasonCodes,
          eventKind: "trade_close" as const,
          state: "CLOSED",
          stateChanged: true,
          tradeEventOccurred: true,
          rMultiple:
            row.rMultiple !== null && Number.isFinite(Number(row.rMultiple))
              ? Number(row.rMultiple)
              : null,
          pnlUsd:
            row.pnlUsd !== null && Number.isFinite(Number(row.pnlUsd))
              ? Number(row.pnlUsd)
              : null,
          sourceOfTruth: row.sourceOfTruth,
          broker: brokerPayloadSummary(row.rawPayload),
          summary: labelLedgerClose(row),
        };
      }),
      ...journalRows
        .filter((row) => {
          const reasonCodes = Array.isArray(row.reasonCodes) ? row.reasonCodes : [];
          return !isCloseReason(reasonCodes);
        })
        .map((row) => {
          const payload = asRecord(row.payload);
          const reasonCodes = Array.isArray(row.reasonCodes) ? row.reasonCodes : [];
          const tradeOccurred = Boolean(payload.tradeEventOccurred);
          const stateChanged = Boolean(payload.stateChanged);
          const state = String(payload.state || "");
          const tradePayload = asRecord(payload.trade);
          const eventKind = tradeOccurred
            ? hasAnyReason(reasonCodes, ["ENTRY_PLACED"])
              ? "trade_open"
              : "trade"
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
    ]
      .sort((a, b) => b.tsMs - a.tsMs)
      .slice(0, 40);

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
      dailyNetR: dailyRows.map((row) => {
        const dayKey =
          row.dayKey instanceof Date
            ? row.dayKey.toISOString().slice(0, 10)
            : String(row.dayKey || "").slice(0, 10);
        return {
          dayKey,
          dayStartMs: Date.parse(`${dayKey}T00:00:00.000Z`),
          trades: Number(row.trades || 0),
          wins: Number(row.wins || 0),
          losses: Number(row.losses || 0),
          netR: Number.isFinite(Number(row.netR)) ? Number(row.netR) : 0,
          pnlUsd: Number.isFinite(Number(row.pnlUsd)) ? Number(row.pnlUsd) : 0,
        };
      }),
      recentTrades,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: (err as Error)?.message || String(err) });
  }
}
