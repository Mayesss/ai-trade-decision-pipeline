#!/usr/bin/env node
// Measure a representative round-trip spread cost (in R) per forex symbol, for
// the universe-wide fee bulk-subtract. Forex spread-in-R = spread / stop, which
// varies by symbol (pip size), so a flat number won't do — but a per-symbol
// average from ONE representative replay each is plenty for a stopgap repair
// (the deployed harness refines exactly on the next pipeline eval).
//
// Picks one candidate deployment per capital symbol, replays its holdout window
// through the patched harness (which records trade.feeR = spreadAbs/riskAbs),
// and prints symbol -> avg feeR as a JSON map.
//
// Usage:
//   node scripts/with-db-env.mjs node --import tsx scripts/scalp-measure-forex-feer.ts

import nextEnv from "@next/env";

import { loadScalpCandleHistoryInRange } from "../lib/scalp/candleHistory";
import { pipSizeForScalpSymbol } from "../lib/scalp/marketData";
import { isScalpPgConfigured, scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";
import { runScalpReplay } from "../lib/scalp/replay/harness";
import type { ScalpReplayCandle } from "../lib/scalp/replay/types";
import type { ScalpCandle } from "../lib/scalp/types";
import { ensureScalpSymbolMarketMetadata } from "../lib/scalp/symbolMarketMetadataSync";
import { buildDeploymentRuntime, resolveHoldoutWindow } from "../lib/scalp/research/evaluator";
import { resolveScalpResearchConfig } from "../lib/scalp/research";
import type { ScalpResearchDeploymentRow } from "../lib/scalp/research/pg";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());
if (process.env.SCALP_PG_USE_HTTP === undefined) process.env.SCALP_PG_USE_HTTP = "1";

function toReplayCandles(rows: ScalpCandle[], spreadPips: number): ScalpReplayCandle[] {
  return rows.map((r) => ({
    ts: Number(r[0]), open: Number(r[1]), high: Number(r[2]),
    low: Number(r[3]), close: Number(r[4]), volume: Number(r[5] || 0), spreadPips,
  }));
}

async function main() {
  if (!isScalpPgConfigured()) throw new Error("scalp_pg_not_configured");
  const db = scalpPrisma();
  const cfg = resolveScalpResearchConfig();
  const nowMs = Date.now();
  const { holdoutFromMs, holdoutToMs } = resolveHoldoutWindow(nowMs, cfg.holdoutWeeks);

  // One representative deployment per capital symbol (prefer enabled).
  const rows = await db.$queryRaw<Array<ScalpResearchDeploymentRow & { riskProfile: unknown }>>(sql`
    SELECT DISTINCT ON (symbol)
      deployment_id AS "deploymentId", venue, symbol,
      strategy_id AS "strategyId", tune_id AS "tuneId",
      entry_session_profile AS "entrySessionProfile",
      enabled, live_mode AS "liveMode", v5_enabled AS "v5Enabled",
      NULL::bigint AS "v5EvaluatedAtMs",
      COALESCE(promotion_gate, '{}'::jsonb) AS "promotionGate",
      COALESCE(risk_profile, '{}'::jsonb) AS "riskProfile"
    FROM scalp_v2_deployments
    WHERE venue = 'capital' AND candidate_id IS NOT NULL AND retired_at IS NULL
    ORDER BY symbol, enabled DESC, deployment_id
  `);

  const feeRBySymbol: Record<string, number> = {};
  console.log(`Measuring per-symbol forex feeR over ${rows.length} symbols...`);
  for (const row of rows) {
    try {
      const { runtime } = buildDeploymentRuntime(row as ScalpResearchDeploymentRow);
      const history = await loadScalpCandleHistoryInRange(row.symbol, "1m", holdoutFromMs, holdoutToMs);
      const raw = (history?.record?.candles || []) as ScalpCandle[];
      if (!raw.length) { console.log(`  ${row.symbol}: no_candles`); continue; }
      await ensureScalpSymbolMarketMetadata(row.symbol, { fetchIfMissing: true }).catch(() => null);
      const replay = await runScalpReplay({
        candles: toReplayCandles(raw, runtime.defaultSpreadPips),
        pipSize: pipSizeForScalpSymbol(row.symbol),
        config: runtime,
        captureTimeline: false,
      });
      const n = replay.trades.length;
      const feeR = n > 0 ? replay.trades.reduce((a, t) => a + (Number(t.feeR) || 0), 0) / n : 0;
      feeRBySymbol[row.symbol] = Number(feeR.toFixed(5));
      console.log(`  ${row.symbol.padEnd(8)} trades=${String(n).padStart(4)}  feeR=${feeR.toFixed(4)}`);
    } catch (err) {
      console.log(`  ${row.symbol}: error ${String(err).slice(0, 80)}`);
    }
  }

  console.log("");
  console.log("FOREX_FEER_JSON=" + JSON.stringify(feeRBySymbol));
  await db.$disconnect();
}

main().catch((err) => { console.error(err); process.exit(1); });
