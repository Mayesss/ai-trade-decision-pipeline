#!/usr/bin/env node
// Apples-to-apples comparison of currently-live deployments vs top not-deployed
// candidates, using the patched (fee/spread-aware) harness. Rebuilds NET v5
// evidence for each (so win-rate, NetR, weekly consistency are all net of cost)
// and aggregates over the regime-eligible cells the deployment actually trades.
//
// Read-only. Metals (XAU/XAG) are flagged: the harness models a flat 1.1-pip
// spread that is near-zero in R for their wide stops, so their cost is
// under-modeled and their NetR is optimistic.
//
// Usage: node scripts/with-db-env.mjs node --import tsx scripts/scalp-compare-live-vs-candidates.ts

import nextEnv from "@next/env";

import { loadScalpCandleHistoryInRange } from "../lib/scalp/candleHistory";
import { pipSizeForScalpSymbol } from "../lib/scalp/marketData";
import { isScalpPgConfigured, scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";
import { runScalpReplay } from "../lib/scalp/replay/harness";
import type { ScalpReplayCandle } from "../lib/scalp/replay/types";
import type { ScalpCandle } from "../lib/scalp/types";
import { ensureScalpSymbolMarketMetadata } from "../lib/scalp/symbolMarketMetadataSync";
import { loadScalpV4RegimeSnapshotsBulk } from "../lib/scalp/regimes/pg";
import type { ScalpV4CellId, ScalpV4Venue } from "../lib/scalp/regimes/types";
import { buildDeploymentRuntime, resolveHoldoutWindow } from "../lib/scalp/research/evaluator";
import { buildScalpV5CellEvidence, resolveScalpV5Config, tagTradesWithCells } from "../lib/scalp/research";
import type { ScalpV5DeploymentRow } from "../lib/scalp/research/pg";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());
if (process.env.SCALP_PG_USE_HTTP === undefined) process.env.SCALP_PG_USE_HTTP = "1";

const CANDIDATE_IDS = [
  "capital:XAUUSD~model_guided_composer_v2~mdl_season_m5_m1_xatr_de05027f97__sp_pacific",
  "capital:XAUUSD~model_guided_composer_v2~mdl_basis_m15_m3_xts_49d641a876__sp_tokyo",
  "capital:XAUUSD~model_guided_composer_v2~mdl_basis_m15_m3_xftp_6dff5e41ae__sp_newyork",
  "capital:GBPUSD~model_guided_composer_v2~mdl_basis_m15_m3_xts_f59d08b107__sp_newyork",
  "capital:XAUUSD~model_guided_composer_v2~mdl_season_m5_m1_xftp_196f725332__sp_berlin",
  "capital:GBPJPY~model_guided_composer_v2~mdl_basis_m5_m1_xtt_9af98de592__sp_sydney",
  "capital:GBPJPY~model_guided_composer_v2~mdl_basis_m15_m3_xts_d8769c6714__sp_tokyo",
  "capital:XAUUSD~model_guided_composer_v2~mdl_relative_m5_m1_xtt_gre_9b2ab3281c__sp_sydney",
  "capital:XAGUSD~model_guided_composer_v2~mdl_basis_m5_m3_xftp_eeb6d9f000__sp_newyork",
  "capital:USDJPY~model_guided_composer_v2~mdl_basis_m15_m3_xftp_1bb02efb88__sp_newyork",
];

function toReplayCandles(rows: ScalpCandle[], spreadPips: number): ScalpReplayCandle[] {
  return rows.map((r) => ({
    ts: Number(r[0]), open: Number(r[1]), high: Number(r[2]),
    low: Number(r[3]), close: Number(r[4]), volume: Number(r[5] || 0), spreadPips,
  }));
}

async function analyze(
  row: ScalpV5DeploymentRow,
  cfg: ReturnType<typeof resolveScalpV5Config>,
  nowMs: number,
  holdoutFromMs: number,
  holdoutToMs: number,
) {
  const { runtime } = buildDeploymentRuntime(row);
  const history = await loadScalpCandleHistoryInRange(row.symbol, "1m", holdoutFromMs, holdoutToMs);
  const raw = (history?.record?.candles || []) as ScalpCandle[];
  if (!raw.length) return null;
  await ensureScalpSymbolMarketMetadata(row.symbol, { fetchIfMissing: true }).catch(() => null);
  const replay = await runScalpReplay({
    candles: toReplayCandles(raw, runtime.defaultSpreadPips),
    pipSize: pipSizeForScalpSymbol(row.symbol),
    config: runtime,
    captureTimeline: false,
  });
  const snapshotMap = await loadScalpV4RegimeSnapshotsBulk({
    pairs: [{ venue: row.venue as ScalpV4Venue, symbol: row.symbol }],
    classifierVersion: cfg.classifierVersion,
    fromMs: holdoutFromMs,
    toMs: holdoutToMs,
  });
  const snaps = snapshotMap.get(`${row.venue}:${row.symbol}`) || [];
  const byWeek = new Map<number, ScalpV4CellId>();
  for (const s of snaps) byWeek.set(s.weekStartMs, s.cellId);
  const ev = buildScalpV5CellEvidence({
    tagged: tagTradesWithCells({ trades: replay.trades, snapshotsByWeekStart: byWeek }),
    classifierVersion: cfg.classifierVersion,
    evaluatedAtMs: nowMs,
    holdoutFromMs, holdoutToMs,
    minTradesPerCell: cfg.minTradesPerCell,
  });
  // Aggregate over the NET-recomputed eligible cells.
  let netR = 0, trades = 0, wins = 0, losses = 0;
  const weeks: number[] = [];
  for (const ck of ev.eligibleCells) {
    const c = ev.cells[ck];
    if (!c) continue;
    netR += Number(c.netR) || 0;
    trades += Number(c.trades) || 0;
    wins += Number(c.wins) || 0;
    losses += Number(c.losses) || 0;
    (c.weeklyNetR || []).forEach((v, i) => { weeks[i] = (weeks[i] || 0) + (Number(v) || 0); });
  }
  const positiveWeeks = weeks.filter((w) => w > 0).length;
  const worstWeek = weeks.length ? Math.min(...weeks) : 0;
  return {
    symbol: row.symbol,
    eligibleCells: ev.eligibleCells.length,
    netR, trades, wins, losses,
    winRate: wins + losses > 0 ? (100 * wins) / (wins + losses) : 0,
    expR: trades > 0 ? netR / trades : 0,
    positiveWeeks, weeks: weeks.length, worstWeek,
    metal: /^(XAU|XAG)/.test(row.symbol),
  };
}

async function main() {
  if (!isScalpPgConfigured()) throw new Error("scalp_pg_not_configured");
  const db = scalpPrisma();
  const cfg = resolveScalpV5Config();
  const nowMs = Date.now();
  const { holdoutFromMs, holdoutToMs } = resolveHoldoutWindow(nowMs, cfg.holdoutWeeks);

  const rows = await db.$queryRaw<Array<ScalpV5DeploymentRow & { riskProfile: unknown }>>(sql`
    SELECT deployment_id AS "deploymentId", venue, symbol,
           strategy_id AS "strategyId", tune_id AS "tuneId",
           entry_session_profile AS "entrySessionProfile", enabled,
           live_mode AS "liveMode", v5_enabled AS "v5Enabled",
           NULL::bigint AS "v5EvaluatedAtMs",
           COALESCE(promotion_gate,'{}'::jsonb) AS "promotionGate",
           COALESCE(risk_profile,'{}'::jsonb) AS "riskProfile"
    FROM scalp_v2_deployments
    WHERE (enabled = true AND live_mode='live' AND retired_at IS NULL)
       OR deployment_id = ANY(${CANDIDATE_IDS}::text[])
  `);
  const liveIds = new Set(rows.filter((r) => r.enabled).map((r) => r.deploymentId));

  const out: Array<{ group: string; r: NonNullable<Awaited<ReturnType<typeof analyze>>> }> = [];
  for (const row of rows) {
    const r = await analyze(row as ScalpV5DeploymentRow, cfg, nowMs, holdoutFromMs, holdoutToMs).catch(() => null);
    if (r) out.push({ group: liveIds.has(row.deploymentId) ? "LIVE" : "CAND", r });
  }

  const fmtRow = (g: string, r: NonNullable<Awaited<ReturnType<typeof analyze>>>) =>
    "  " + g.padEnd(5) + (r.metal ? "⚠ " : "  ") + r.symbol.padEnd(9) +
    r.netR.toFixed(2).padStart(8) + String(r.trades).padStart(8) +
    (r.winRate.toFixed(1) + "%").padStart(9) + r.expR.toFixed(3).padStart(9) +
    `${r.positiveWeeks}/${r.weeks}`.padStart(8) + r.worstWeek.toFixed(2).padStart(9);

  console.log(`\nNET (fee-aware) comparison — holdout ${new Date(holdoutFromMs).toISOString().slice(0,10)}..${new Date(holdoutToMs).toISOString().slice(0,10)}`);
  console.log("  GROUP  SYMBOL      NETR   TRADES  WINRATE    EXP_R  POS_WKS WORST_WK");
  console.log("  " + "-".repeat(72));
  console.log("  -- LIVE (currently enabled) --");
  for (const o of out.filter((x) => x.group === "LIVE").sort((a, b) => b.r.netR - a.r.netR)) console.log(fmtRow(o.group, o.r));
  console.log("  -- CANDIDATES (not deployed) --");
  for (const o of out.filter((x) => x.group === "CAND").sort((a, b) => b.r.netR - a.r.netR)) console.log(fmtRow(o.group, o.r));
  console.log("  " + "-".repeat(72));
  console.log("  ⚠ = metal (XAU/XAG): harness models flat 1.1-pip spread -> cost under-modeled, NetR optimistic.");

  await db.$disconnect();
}

main().catch((err) => { console.error(err); process.exit(1); });
