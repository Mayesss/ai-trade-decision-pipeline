#!/usr/bin/env node
// Read-only: measure the impact of charging the round-trip bid/ask SPREAD on
// the allowed forex (Capital, embedded-spread) deployments. Replays each
// deployment's holdout window once through the patched harness — which now
// records both grossRMultiple (fee-free) and rMultiple (net of spread) per
// trade — and rebuilds v5 cell evidence BOTH ways over the SAME window. This
// isolates the spread cost cleanly (no holdout-window shift, no persistence).
//
// Why this isn't a flat SQL haircut like bitget: bitget's taker fee was a
// stable ~0.30 R/trade measured from the live ledger. Forex spread-in-R =
// spread / stop-distance, which varies per trade, and Capital's live payloads
// carry no fill detail — so the only honest number comes from re-replaying.
//
// Usage:
//   node scripts/with-db-env.mjs node --import tsx scripts/scalp-rescore-forex-spread.ts

import nextEnv from "@next/env";

import { loadScalpCandleHistoryInRange } from "../lib/scalp/candleHistory";
import { pipSizeForScalpSymbol } from "../lib/scalp/marketData";
import { isScalpPgConfigured, scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";
import { runScalpReplay } from "../lib/scalp/replay/harness";
import type { ScalpReplayCandle, ScalpReplayTrade } from "../lib/scalp/replay/types";
import type { ScalpCandle } from "../lib/scalp/types";
import { ensureScalpSymbolMarketMetadata } from "../lib/scalp/symbolMarketMetadataSync";
import { loadScalpRegimeSnapshotsBulk } from "../lib/scalp/regimes/pg";
import type { ScalpRegimeCellId, ScalpRegimeVenue } from "../lib/scalp/regimes/types";
import { buildDeploymentRuntime, resolveHoldoutWindow } from "../lib/scalp/research/evaluator";
import {
  buildScalpResearchCellEvidence,
  resolveScalpResearchConfig,
  tagTradesWithCells,
} from "../lib/scalp/research";
import type { ScalpResearchDeploymentRow } from "../lib/scalp/research/pg";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

if (process.env.SCALP_PG_USE_HTTP === undefined) {
  process.env.SCALP_PG_USE_HTTP = "1";
}

function toReplayCandles(rows: ScalpCandle[], spreadPips: number): ScalpReplayCandle[] {
  return rows.map((row) => ({
    ts: Number(row[0]),
    open: Number(row[1]),
    high: Number(row[2]),
    low: Number(row[3]),
    close: Number(row[4]),
    volume: Number(row[5] || 0),
    spreadPips,
  }));
}

function evidenceFrom(
  trades: ScalpReplayTrade[],
  snapshotsByWeekStart: Map<number, ScalpRegimeCellId>,
  cfg: { classifierVersion: string; minTradesPerCell: number },
  holdoutFromMs: number,
  holdoutToMs: number,
  nowMs: number,
) {
  const tagged = tagTradesWithCells({ trades, snapshotsByWeekStart });
  return buildScalpResearchCellEvidence({
    tagged,
    classifierVersion: cfg.classifierVersion,
    evaluatedAtMs: nowMs,
    holdoutFromMs,
    holdoutToMs,
    minTradesPerCell: cfg.minTradesPerCell,
  });
}

async function main() {
  if (!isScalpPgConfigured()) throw new Error("scalp_pg_not_configured");
  const db = scalpPrisma();
  const cfg = resolveScalpResearchConfig();
  const nowMs = Date.now();
  const { holdoutFromMs, holdoutToMs } = resolveHoldoutWindow(nowMs, cfg.holdoutWeeks);

  const rows = await db.$queryRaw<Array<ScalpResearchDeploymentRow & { riskProfile: unknown }>>(sql`
    SELECT deployment_id AS "deploymentId", venue, symbol,
           strategy_id AS "strategyId", tune_id AS "tuneId",
           entry_session_profile AS "entrySessionProfile",
           enabled, live_mode AS "liveMode", v5_enabled AS "v5Enabled",
           NULL::bigint AS "v5EvaluatedAtMs",
           COALESCE(promotion_gate, '{}'::jsonb) AS "promotionGate",
           COALESCE(risk_profile, '{}'::jsonb) AS "riskProfile"
    FROM scalp_v2_deployments
    WHERE venue = 'capital' AND live_mode = 'live' AND retired_at IS NULL
    ORDER BY symbol
  `);

  console.log("");
  console.log(`Forex spread re-score (read-only) — ${new Date().toISOString()}`);
  console.log(
    `  holdout: ${new Date(holdoutFromMs).toISOString().slice(0, 10)} .. ${new Date(holdoutToMs).toISOString().slice(0, 10)} (${cfg.holdoutWeeks}w)`,
  );
  console.log(
    `  default spread assumption: harness defaultSpreadPips (candles carry no spread)`,
  );
  console.log("");
  console.log(
    "  SYMBOL    TRADES  AVG_SPREAD_R   GROSS_NETR  NET_NETR   GROSS_ELIG  NET_ELIG  GATE",
  );
  console.log("  " + "-".repeat(86));

  const results: Array<{ symbol: string; willEnable: boolean }> = [];

  for (const row of rows) {
    const { runtime } = buildDeploymentRuntime(row as ScalpResearchDeploymentRow);
    let history;
    try {
      history = await loadScalpCandleHistoryInRange(row.symbol, "1m", holdoutFromMs, holdoutToMs);
    } catch (err) {
      console.log(`  ${row.symbol.padEnd(9)} candle_load_failed: ${String(err)}`);
      continue;
    }
    const rawCandles = (history?.record?.candles || []) as ScalpCandle[];
    if (rawCandles.length === 0) {
      console.log(`  ${row.symbol.padEnd(9)} no_candles`);
      continue;
    }
    await ensureScalpSymbolMarketMetadata(row.symbol, { fetchIfMissing: true }).catch(() => null);

    const pipSize = pipSizeForScalpSymbol(row.symbol);
    const replay = await runScalpReplay({
      candles: toReplayCandles(rawCandles, runtime.defaultSpreadPips),
      pipSize,
      config: runtime,
      captureTimeline: false,
    });

    const snapshotMap = await loadScalpRegimeSnapshotsBulk({
      pairs: [{ venue: row.venue as ScalpRegimeVenue, symbol: row.symbol }],
      classifierVersion: cfg.classifierVersion,
      fromMs: holdoutFromMs,
      toMs: holdoutToMs,
    });
    const snaps = snapshotMap.get(`${row.venue}:${row.symbol}`) || [];
    const snapshotsByWeekStart = new Map<number, ScalpRegimeCellId>();
    for (const snap of snaps) snapshotsByWeekStart.set(snap.weekStartMs, snap.cellId);

    // Net evidence (as the patched harness scores it) vs gross (fee-free).
    const netEv = evidenceFrom(replay.trades, snapshotsByWeekStart, cfg, holdoutFromMs, holdoutToMs, nowMs);
    const grossTrades = replay.trades.map((t) => ({
      ...t,
      rMultiple: Number.isFinite(t.grossRMultiple) ? (t.grossRMultiple as number) : t.rMultiple,
    }));
    const grossEv = evidenceFrom(grossTrades, snapshotsByWeekStart, cfg, holdoutFromMs, holdoutToMs, nowMs);

    const n = replay.trades.length;
    const avgSpreadR =
      n > 0 ? replay.trades.reduce((a, t) => a + (Number(t.feeR) || 0), 0) / n : 0;
    const grossNetR = Object.values(grossEv.cells).reduce((a, c) => a + Number(c.netR || 0), 0);
    const netNetR = Object.values(netEv.cells).reduce((a, c) => a + Number(c.netR || 0), 0);
    const willEnable = netEv.eligibleCells.length > 0;
    results.push({ symbol: row.symbol, willEnable });

    console.log(
      "  " +
        row.symbol.padEnd(9) +
        String(n).padStart(6) +
        avgSpreadR.toFixed(4).padStart(13) +
        grossNetR.toFixed(2).padStart(13) +
        netNetR.toFixed(2).padStart(10) +
        String(grossEv.eligibleCells.length).padStart(11) +
        String(netEv.eligibleCells.length).padStart(10) +
        "  " +
        (willEnable ? "SURVIVES" : "CUT"),
    );
  }

  console.log("  " + "-".repeat(86));
  const cut = results.filter((r) => !r.willEnable).map((r) => r.symbol);
  console.log(
    `  Survivors: ${results.filter((r) => r.willEnable).length}/${results.length}` +
      (cut.length ? `   Cut: ${cut.join(", ")}` : ""),
  );
  console.log("");

  await db.$disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
