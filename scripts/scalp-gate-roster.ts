#!/usr/bin/env node
// Apply a quality gate to build the live roster. Re-runs the patched (fee-aware)
// harness for the currently-live set + a Stage-1 candidate shortlist, computes
// NET metrics (incl. net win-rate), applies the gate, dedupes candidates to the
// best tune per symbol, and prints KEEP / DEMOTE / PROMOTE. With --apply it
// demotes failing live rows and promotes the deduped passers to live.
//
// Gate: winRate>=58% AND expR>=0.05 AND trades>=30 AND positiveWeeks>=7/12.
//
// Stage-1 candidate ids are read from --ids <file> (one per line).
// Usage:
//   ... scalp-gate-roster.ts --ids /tmp/stage1_ids.txt            (report only)
//   ... scalp-gate-roster.ts --ids /tmp/stage1_ids.txt --apply    (execute)

import { readFileSync } from "node:fs";

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

const GATE = { winRate: 58, expR: 0.05, trades: 30, positiveWeeks: 7 };

function argVal(name: string): string | undefined {
  const i = process.argv.indexOf(name);
  return i === -1 ? undefined : process.argv[i + 1];
}
const APPLY = process.argv.includes("--apply");

function toReplayCandles(rows: ScalpCandle[], spreadPips: number): ScalpReplayCandle[] {
  return rows.map((r) => ({
    ts: Number(r[0]), open: Number(r[1]), high: Number(r[2]),
    low: Number(r[3]), close: Number(r[4]), volume: Number(r[5] || 0), spreadPips,
  }));
}

type Metrics = {
  deploymentId: string; symbol: string; enabled: boolean;
  netR: number; trades: number; winRate: number; expR: number;
  positiveWeeks: number; weeks: number; worstWeek: number; pass: boolean;
};

async function analyze(row: ScalpV5DeploymentRow, cfg: ReturnType<typeof resolveScalpV5Config>, nowMs: number, from: number, to: number): Promise<Metrics | null> {
  const { runtime } = buildDeploymentRuntime(row);
  const history = await loadScalpCandleHistoryInRange(row.symbol, "1m", from, to);
  const raw = (history?.record?.candles || []) as ScalpCandle[];
  if (!raw.length) return null;
  await ensureScalpSymbolMarketMetadata(row.symbol, { fetchIfMissing: true }).catch(() => null);
  const replay = await runScalpReplay({ candles: toReplayCandles(raw, runtime.defaultSpreadPips), pipSize: pipSizeForScalpSymbol(row.symbol), config: runtime, captureTimeline: false });
  const snaps = (await loadScalpV4RegimeSnapshotsBulk({ pairs: [{ venue: row.venue as ScalpV4Venue, symbol: row.symbol }], classifierVersion: cfg.classifierVersion, fromMs: from, toMs: to })).get(`${row.venue}:${row.symbol}`) || [];
  const byWeek = new Map<number, ScalpV4CellId>();
  for (const s of snaps) byWeek.set(s.weekStartMs, s.cellId);
  const ev = buildScalpV5CellEvidence({ tagged: tagTradesWithCells({ trades: replay.trades, snapshotsByWeekStart: byWeek }), classifierVersion: cfg.classifierVersion, evaluatedAtMs: nowMs, holdoutFromMs: from, holdoutToMs: to, minTradesPerCell: cfg.minTradesPerCell });
  let netR = 0, trades = 0, wins = 0, losses = 0; const weeks: number[] = [];
  for (const ck of ev.eligibleCells) {
    const c = ev.cells[ck]; if (!c) continue;
    netR += +c.netR || 0; trades += +c.trades || 0; wins += +c.wins || 0; losses += +c.losses || 0;
    (c.weeklyNetR || []).forEach((v, i) => { weeks[i] = (weeks[i] || 0) + (+v || 0); });
  }
  const winRate = wins + losses > 0 ? (100 * wins) / (wins + losses) : 0;
  const expR = trades > 0 ? netR / trades : 0;
  const positiveWeeks = weeks.filter((w) => w > 0).length;
  const pass = winRate >= GATE.winRate && expR >= GATE.expR && trades >= GATE.trades && positiveWeeks >= GATE.positiveWeeks;
  return { deploymentId: row.deploymentId, symbol: row.symbol, enabled: row.enabled, netR, trades, winRate, expR, positiveWeeks, weeks: weeks.length, worstWeek: weeks.length ? Math.min(...weeks) : 0, pass };
}

async function main() {
  if (!isScalpPgConfigured()) throw new Error("scalp_pg_not_configured");
  const db = scalpPrisma();
  const cfg = resolveScalpV5Config();
  const nowMs = Date.now();
  const { holdoutFromMs, holdoutToMs } = resolveHoldoutWindow(nowMs, cfg.holdoutWeeks);

  const idsFile = argVal("--ids");
  const candidateIds = idsFile ? readFileSync(idsFile, "utf8").split("\n").map((s) => s.trim()).filter(Boolean) : [];

  const rows = await db.$queryRaw<Array<ScalpV5DeploymentRow & { riskProfile: unknown }>>(sql`
    SELECT deployment_id AS "deploymentId", venue, symbol, strategy_id AS "strategyId", tune_id AS "tuneId",
           entry_session_profile AS "entrySessionProfile", enabled, live_mode AS "liveMode", v5_enabled AS "v5Enabled",
           NULL::bigint AS "v5EvaluatedAtMs", COALESCE(promotion_gate,'{}'::jsonb) AS "promotionGate", COALESCE(risk_profile,'{}'::jsonb) AS "riskProfile"
    FROM scalp_v2_deployments
    WHERE (enabled = true AND live_mode='live' AND retired_at IS NULL) OR deployment_id = ANY(${candidateIds}::text[])
  `);

  const results: Metrics[] = [];
  for (const row of rows) {
    const m = await analyze(row as ScalpV5DeploymentRow, cfg, nowMs, holdoutFromMs, holdoutToMs).catch(() => null);
    if (m) results.push(m);
  }

  const liveKeep = results.filter((m) => m.enabled && m.pass);
  const liveDemote = results.filter((m) => m.enabled && !m.pass);
  // candidate passers, dedupe to best (highest netR) tune per symbol
  const candPass = results.filter((m) => !m.enabled && m.pass).sort((a, b) => b.netR - a.netR);
  const bestPerSymbol = new Map<string, Metrics>();
  for (const m of candPass) if (!bestPerSymbol.has(m.symbol)) bestPerSymbol.set(m.symbol, m);
  const promote = [...bestPerSymbol.values()].sort((a, b) => b.netR - a.netR);

  const fmt = (m: Metrics) => "  " + m.symbol.padEnd(9) + m.netR.toFixed(2).padStart(8) + String(m.trades).padStart(7) + (m.winRate.toFixed(1) + "%").padStart(8) + m.expR.toFixed(3).padStart(8) + `${m.positiveWeeks}/${m.weeks}`.padStart(7) + m.worstWeek.toFixed(2).padStart(8);
  const hdr = "  SYMBOL       NETR  TRADES WINRATE   EXP_R  +WKS  WORST";
  console.log(`\nGate: winRate>=${GATE.winRate}% expR>=${GATE.expR} trades>=${GATE.trades} +weeks>=${GATE.positiveWeeks}/12  (net, holdout ${new Date(holdoutFromMs).toISOString().slice(0,10)}..${new Date(holdoutToMs).toISOString().slice(0,10)})`);
  console.log(`\nKEEP (live, passes):\n${hdr}`); liveKeep.sort((a,b)=>b.netR-a.netR).forEach((m)=>console.log(fmt(m)));
  console.log(`\nDEMOTE (live, fails):\n${hdr}`); liveDemote.sort((a,b)=>b.netR-a.netR).forEach((m)=>console.log(fmt(m)));
  console.log(`\nPROMOTE (candidate passes, best tune/symbol):\n${hdr}`); promote.forEach((m)=>console.log(fmt(m)));
  console.log(`\nResulting live roster size: ${liveKeep.length + promote.length} (keep ${liveKeep.length} + promote ${promote.length}); demote ${liveDemote.length}`);

  if (APPLY) {
    const demoteIds = liveDemote.map((m) => m.deploymentId);
    const promoteIds = promote.map((m) => m.deploymentId);
    await db.$transaction(async (tx) => {
      if (demoteIds.length) await tx.$executeRaw(sql`UPDATE scalp_v2_deployments SET enabled=false, updated_at=now() WHERE deployment_id = ANY(${demoteIds}::text[])`);
      if (promoteIds.length) await tx.$executeRaw(sql`UPDATE scalp_v2_deployments SET enabled=true, v5_enabled=true, live_mode='live', last_promoted_at=now(), updated_at=now() WHERE deployment_id = ANY(${promoteIds}::text[])`);
    });
    console.log(`\nAPPLIED: demoted ${demoteIds.length}, promoted ${promoteIds.length}.`);
  } else {
    console.log(`\n(report only — re-run with --apply to execute)`);
  }

  await db.$disconnect();
}

main().catch((err) => { console.error(err); process.exit(1); });
