// Shared loader for the v5 dashboard endpoints. Loads every deployment row +
// the current-week regime snapshot for each (venue, symbol) in one bulk query,
// then computes the v5 gate decision per row. Used by /gate-state, /regimes,
// and /deployments — collapses what was an N+1 per-row snapshot loop into one
// bulk SELECT.
//
// The old combined /dashboard endpoint did the per-row loop sequentially,
// which timed out on Vercel once the deployment count grew. This helper
// returns everything callers need to render a section without redoing the DB
// work.

import { loadScalpV4RegimeSnapshotsBulk, resolveScalpV4FailClosedStaleMs } from "../scalp-v4/pg";
import type { ScalpV4Venue } from "../scalp-v4/types";
import { startOfUtcWeekMondayMs } from "../scalp-v4/week";
import { scalpPrisma } from "../scalp/pg/client";
import { sql } from "../scalp/pg/sql";
import {
  resolveScalpV5Config,
  type ScalpV5CellEvidence,
  type ScalpV5CellStat,
  type ScalpV5Config,
} from "./index";

const ONE_WEEK_MS = 7 * 24 * 60 * 60_000;

export type V5GateDecision =
  | "allow"
  | "block_negative"
  | "block_unseen"
  | "block_stale"
  | "block_evaluator_pending"
  | "block_insufficient_trades";

export interface V5DashboardDeploymentRow {
  deploymentId: string;
  venue: ScalpV4Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  session: string;
  enabled: boolean;
  liveMode: string | null;
  v5Enabled: boolean;
  v5EvaluatedAtMs: number | null;
  evidence: ScalpV5CellEvidence | null;
  currentCell: { cellId: string | null; stale: boolean; updatedAtMs: number | null };
  decision: V5GateDecision;
  currentCellStat: ScalpV5CellStat | null;
  // Sum of netR across every cell in the evidence (12w holdout total). 0 when
  // evidence is missing. Used as the deployment-list sort key.
  totalNetR: number;
}

export interface V5DashboardLoad {
  cfg: ScalpV5Config;
  nowMs: number;
  staleThresholdMs: number;
  rows: V5DashboardDeploymentRow[];
  // Aggregate stats over the loaded set, derived once to avoid each endpoint
  // re-iterating the rows.
  totals: {
    total: number;
    enabled: number;
    evaluated: number;
    missingEvidence: number;
    stale: number;
    latestEvaluationMs: number | null;
    oldestEvaluationMs: number | null;
  };
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function parseNumberArray(value: unknown, length: number): number[] {
  if (!Array.isArray(value)) return new Array(length).fill(0);
  const arr = (value as unknown[]).map((v) => Number(v) || 0);
  // Pad/truncate to match weeklyNetR length so callers can assume parallel arrays.
  if (arr.length === length) return arr;
  if (arr.length > length) return arr.slice(0, length);
  return arr.concat(new Array(length - arr.length).fill(0));
}

function parseCellStat(value: unknown): ScalpV5CellStat | null {
  const rec = asRecord(value);
  if (!rec) return null;
  const weeklyNetR = Array.isArray(rec.weeklyNetR)
    ? (rec.weeklyNetR as unknown[]).map((v) => Number(v) || 0)
    : [];
  const len = weeklyNetR.length;
  // r2 evidence didn't carry weeklyTrades/Wins/Losses — fall back to zero
  // arrays of matching length. The incremental evaluator detects this case
  // by inspecting evidence.version (r2 → full replay).
  return {
    trades: Math.max(0, Math.floor(Number(rec.trades) || 0)),
    netR: Number(rec.netR) || 0,
    expectancyR: Number(rec.expectancyR) || 0,
    wins: Math.max(0, Math.floor(Number(rec.wins) || 0)),
    losses: Math.max(0, Math.floor(Number(rec.losses) || 0)),
    weeklyNetR,
    weeklyTrades: parseNumberArray(rec.weeklyTrades, len),
    weeklyWins: parseNumberArray(rec.weeklyWins, len),
    weeklyLosses: parseNumberArray(rec.weeklyLosses, len),
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
    // Preserve whichever version was written; the type accepts the current
    // version constant but at runtime we coerce. r2 rows show up here too
    // and the incremental evaluator checks `version !== SCALP_V5_VERSION`
    // to know to fall back to full replay.
    version: (rec.version as ScalpV5CellEvidence["version"]) ?? "scalp_v5_cell_evidence_r3",
    classifierVersion: String(rec.classifierVersion || ""),
    evaluatedAtMs: Number(rec.evaluatedAtMs) || 0,
    holdoutFromMs: Number(rec.holdoutFromMs) || 0,
    holdoutToMs: Number(rec.holdoutToMs) || 0,
    minTradesPerCell: Math.max(0, Math.floor(Number(rec.minTradesPerCell) || 0)),
    cells,
    eligibleCells,
  };
}

function decideGate(params: {
  evidence: ScalpV5CellEvidence | null;
  currentCellId: string | null;
  stale: boolean;
  minTradesPerCell: number;
}): V5GateDecision {
  if (!params.evidence) return "block_evaluator_pending";
  if (params.stale || !params.currentCellId) return "block_stale";
  const cellStat = params.evidence.cells[params.currentCellId];
  if (!cellStat) return "block_unseen";
  if (cellStat.trades < params.minTradesPerCell) return "block_insufficient_trades";
  if (cellStat.expectancyR <= 0) return "block_negative";
  return "allow";
}

function resolveStaleThresholdMs(): number {
  const envHours = Number(process.env.BULK_V5_STALE_OLDER_THAN_HOURS);
  const ms = Number.isFinite(envHours) && envHours > 0 ? envHours * 60 * 60_000 : 6 * 24 * 60 * 60_000;
  return Math.max(60 * 60_000, Math.floor(ms));
}

// Load every promoted deployment row + its current-week regime snapshot in two
// queries total. The bulk snapshot fetch uses `= ANY(keys::text[])`, so cost is
// independent of deployment count.
export async function loadV5DashboardData(): Promise<V5DashboardLoad> {
  const cfg = resolveScalpV5Config();
  const nowMs = Date.now();
  const staleThresholdMs = resolveStaleThresholdMs();
  const db = scalpPrisma();

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
  const deployRows = await db.$queryRaw<DeploymentDbRow[]>(sql`
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
    WHERE candidate_id IS NOT NULL
    ORDER BY enabled DESC, symbol ASC, entry_session_profile ASC;
  `);

  // Build the dedup'd (venue, symbol) pair set for the bulk snapshot load.
  const currentWeekStartMs = startOfUtcWeekMondayMs(nowMs);
  const pairs: Array<{ venue: ScalpV4Venue; symbol: string }> = [];
  const seenPair = new Set<string>();
  for (const row of deployRows) {
    const venue = (String(row.venue || "").toLowerCase() === "capital" ? "capital" : "bitget") as ScalpV4Venue;
    const symbol = String(row.symbol || "").trim().toUpperCase();
    const key = `${venue}:${symbol}`;
    if (seenPair.has(key)) continue;
    seenPair.add(key);
    pairs.push({ venue, symbol });
  }
  const snapshotMap = pairs.length
    ? await loadScalpV4RegimeSnapshotsBulk({
        pairs,
        classifierVersion: cfg.classifierVersion,
        fromMs: currentWeekStartMs,
        toMs: currentWeekStartMs + ONE_WEEK_MS,
      })
    : new Map<string, Awaited<ReturnType<typeof loadScalpV4RegimeSnapshotsBulk>> extends Map<string, infer V> ? V : never>();
  const failClosedStaleMs = resolveScalpV4FailClosedStaleMs();

  const rows: V5DashboardDeploymentRow[] = [];
  let enabled = 0;
  let evaluated = 0;
  let missingEvidence = 0;
  let stale = 0;
  let latestEvaluationMs: number | null = null;
  let oldestEvaluationMs: number | null = null;

  for (const r of deployRows) {
    const venue = (String(r.venue || "").toLowerCase() === "capital" ? "capital" : "bitget") as ScalpV4Venue;
    const symbol = String(r.symbol || "").trim().toUpperCase();
    const evidence = parseEvidence(r.v5CellEvidence);
    const v5EvaluatedAtMs = r.v5EvaluatedAt ? r.v5EvaluatedAt.getTime() : null;
    const isEnabled = Boolean(r.enabled);
    if (isEnabled) enabled += 1;
    if (v5EvaluatedAtMs) {
      evaluated += 1;
      latestEvaluationMs = Math.max(latestEvaluationMs ?? 0, v5EvaluatedAtMs);
      oldestEvaluationMs = Math.min(oldestEvaluationMs ?? v5EvaluatedAtMs, v5EvaluatedAtMs);
      if (nowMs - v5EvaluatedAtMs > staleThresholdMs) stale += 1;
    } else {
      missingEvidence += 1;
    }

    // Resolve current-week snapshot from the bulk map.
    const snaps = snapshotMap.get(`${venue}:${symbol}`) || [];
    // Bulk query was filtered to a single week; if any row came back it's THE
    // current snapshot for this pair.
    const snap = snaps[0] || null;
    const currentCellId = snap ? snap.cellId : null;
    const snapUpdatedAtMs = snap ? snap.updatedAtMs : null;
    const regimeStale =
      snapUpdatedAtMs === null || nowMs - snapUpdatedAtMs > failClosedStaleMs;

    const decision = decideGate({
      evidence,
      currentCellId,
      stale: regimeStale,
      minTradesPerCell: cfg.minTradesPerCell,
    });
    const currentCellStat = currentCellId && evidence ? evidence.cells[currentCellId] ?? null : null;
    let totalNetR = 0;
    if (evidence) {
      for (const stat of Object.values(evidence.cells)) {
        totalNetR += Number(stat.netR) || 0;
      }
    }

    rows.push({
      deploymentId: r.deploymentId,
      venue,
      symbol,
      strategyId: r.strategyId,
      tuneId: r.tuneId,
      session: r.entrySessionProfile,
      enabled: isEnabled,
      liveMode: r.liveMode,
      v5Enabled: Boolean(r.v5Enabled),
      v5EvaluatedAtMs,
      evidence,
      currentCell: { cellId: currentCellId, stale: regimeStale, updatedAtMs: snapUpdatedAtMs },
      decision,
      currentCellStat,
      totalNetR,
    });
  }

  return {
    cfg,
    nowMs,
    staleThresholdMs,
    rows,
    totals: {
      total: deployRows.length,
      enabled,
      evaluated,
      missingEvidence,
      stale,
      latestEvaluationMs,
      oldestEvaluationMs,
    },
  };
}

// Compact-payload version of a single deployment's cells, used by the
// /deployments endpoint. Caps at 12 cells and puts the current one first.
export function shapeCellsForDeployment(row: V5DashboardDeploymentRow): Array<{
  cellId: string;
  trades: number;
  netR: number;
  expectancyR: number;
  wins: number;
  losses: number;
  weeklyNetR: number[];
  isCurrent: boolean;
}> {
  if (!row.evidence) return [];
  const currentCellId = row.currentCell.cellId;
  const cellsArr = Object.entries(row.evidence.cells).map(([cellId, stat]) => ({
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
  return cellsArr.slice(0, 12);
}
