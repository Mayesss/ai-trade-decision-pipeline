// Shared loader for scoped v5 dashboard deployment endpoints. Loads a bounded
// page of deployment rows + the current-week regime snapshot for each
// (venue, symbol) in one bulk query, then computes the v5 gate decision per row.
//
// The old combined /dashboard endpoint did the per-row loop sequentially,
// which timed out on Vercel once the deployment count grew. This helper
// returns everything callers need to render a section without redoing the DB
// work.

import { loadScalpRegimeSnapshotsBulk, resolveScalpRegimeFailClosedStaleMs } from "../regimes/pg";
import type { ScalpRegimeVenue } from "../regimes/types";
import { startOfUtcWeekMondayMs } from "../regimes/week";
import { scalpPrisma } from "../pg/client";
import { sql } from "../pg/sql";
import {
  resolveScalpResearchConfig,
  type ScalpResearchCellEvidence,
  type ScalpResearchCellStat,
  type ScalpResearchConfig,
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
  venue: ScalpRegimeVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  session: string;
  enabled: boolean;
  liveMode: string | null;
  v5Enabled: boolean;
  v5EvaluatedAtMs: number | null;
  evidence: ScalpResearchCellEvidence | null;
  currentCell: { cellId: string | null; stale: boolean; updatedAtMs: number | null };
  decision: V5GateDecision;
  currentCellStat: ScalpResearchCellStat | null;
  // Sum of netR across every cell in the evidence (12w holdout total). 0 when
  // evidence is missing. Used as the deployment-list sort key.
  totalNetR: number;
}

export interface V5DashboardLoad {
  cfg: ScalpResearchConfig;
  nowMs: number;
  staleThresholdMs: number;
  rows: V5DashboardDeploymentRow[];
  page: {
    scope: V5DashboardScope;
    limit: number;
    offset: number;
    returned: number;
    totalMatching: number;
    hasMore: boolean;
    includeInactive: boolean;
  };
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

export type V5DashboardScope = "live" | "enabled" | "inactive" | "all";

export interface V5DashboardLoadOptions {
  scope?: V5DashboardScope;
  limit?: number;
  offset?: number;
  includeInactive?: boolean;
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

function parseCellStat(value: unknown): ScalpResearchCellStat | null {
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

function parseEvidence(value: unknown): ScalpResearchCellEvidence | null {
  const rec = asRecord(value);
  if (!rec) return null;
  const cellsRec = asRecord(rec.cells) || {};
  const cells: Record<string, ScalpResearchCellStat> = {};
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
    // and the incremental evaluator checks `version !== SCALP_RESEARCH_VERSION`
    // to know to fall back to full replay.
    version: (rec.version as ScalpResearchCellEvidence["version"]) ?? "scalp_v5_cell_evidence_r3",
    classifierVersion: String(rec.classifierVersion || ""),
    evaluatedAtMs: Number(rec.evaluatedAtMs) || 0,
    holdoutFromMs: Number(rec.holdoutFromMs) || 0,
    holdoutToMs: Number(rec.holdoutToMs) || 0,
    minTradesPerCell: Math.max(0, Math.floor(Number(rec.minTradesPerCell) || 0)),
    cells,
    eligibleCells,
  };
}

export function decideV5Gate(params: {
  evidence: ScalpResearchCellEvidence | null;
  consistencyException: boolean;
  currentCellId: string | null;
  stale: boolean;
  minTradesPerCell: number;
}): V5GateDecision {
  if (!params.evidence) return "block_evaluator_pending";
  if (params.stale || !params.currentCellId) return "block_stale";
  const cellStat = params.evidence.cells[params.currentCellId];
  if (!cellStat) return "block_unseen";
  if (cellStat.trades < params.minTradesPerCell) {
    const thinPositiveAllowed =
      params.consistencyException &&
      cellStat.trades >= 3 &&
      cellStat.netR > 0 &&
      cellStat.expectancyR > 0;
    if (!thinPositiveAllowed) return "block_insufficient_trades";
  }
  if (cellStat.expectancyR <= 0) return "block_negative";
  return "allow";
}

function isConsistencyExceptionPromotionGate(value: unknown): boolean {
  const gate = asRecord(value) || {};
  const promotion = asRecord(gate.v5Promotion) || {};
  const reason = String(gate.reason || promotion.passReason || "")
    .trim()
    .toLowerCase();
  return reason === "v5_consistency_exception_passed";
}

function resolveStaleThresholdMs(): number {
  const envHours = Number(process.env.BULK_V5_STALE_OLDER_THAN_HOURS);
  const ms = Number.isFinite(envHours) && envHours > 0 ? envHours * 60 * 60_000 : 6 * 24 * 60 * 60_000;
  return Math.max(60 * 60_000, Math.floor(ms));
}

function normalizeDashboardScope(value: unknown): V5DashboardScope {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "enabled" || raw === "inactive" || raw === "all") return raw;
  return "live";
}

function normalizeLimit(value: unknown, fallback: number, max: number): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

// Load a scoped page of promoted deployment rows + its current-week regime
// snapshots. The bulk snapshot fetch uses `= ANY(keys::text[])`, so cost is
// bounded by the number of symbols in the requested page.
export async function loadV5DashboardData(options: V5DashboardLoadOptions = {}): Promise<V5DashboardLoad> {
  const cfg = resolveScalpResearchConfig();
  const nowMs = Date.now();
  const staleThresholdMs = resolveStaleThresholdMs();
  const db = scalpPrisma();
  const scope = normalizeDashboardScope(options.scope);
  const limit = normalizeLimit(options.limit, scope === "live" ? 100 : 50, 250);
  const offset = Math.max(0, Math.floor(Number(options.offset) || 0));
  const includeInactive = options.includeInactive ?? scope !== "live";
  const scopeWhere =
    scope === "live"
      ? sql`AND d.enabled = TRUE AND d.live_mode = 'live'`
      : scope === "enabled"
        ? sql`AND d.enabled = TRUE`
        : scope === "inactive"
          ? sql`AND d.enabled = FALSE`
          : sql``;

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
    promotionGate: unknown;
  };
  const [countRows, deployRows] = await Promise.all([
    db.$queryRaw<Array<{ count: bigint }>>(sql`
      SELECT COUNT(*) AS count
      FROM scalp_v2_deployments d
      WHERE d.candidate_id IS NOT NULL
        ${scopeWhere}
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_candidates c
          WHERE c.id = d.candidate_id
            AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
        );
    `),
    db.$queryRaw<DeploymentDbRow[]>(sql`
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
        v5_cell_evidence AS "v5CellEvidence",
        promotion_gate AS "promotionGate"
      FROM scalp_v2_deployments d
      WHERE d.candidate_id IS NOT NULL
        ${scopeWhere}
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_candidates c
          WHERE c.id = d.candidate_id
            AND c.metadata_json->'scopeRemoval'->>'reason' = 'bitget_symbol_removed_no_candles'
        )
      ORDER BY enabled DESC, live_mode ASC NULLS LAST, symbol ASC, entry_session_profile ASC, deployment_id ASC
      LIMIT ${limit}
      OFFSET ${offset};
    `),
  ]);
  const totalMatching = Math.max(0, Number(countRows[0]?.count || 0));

  // Build the dedup'd (venue, symbol) pair set for the bulk snapshot load.
  const currentWeekStartMs = startOfUtcWeekMondayMs(nowMs);
  const pairs: Array<{ venue: ScalpRegimeVenue; symbol: string }> = [];
  const seenPair = new Set<string>();
  for (const row of deployRows) {
    const venue = (String(row.venue || "").toLowerCase() === "capital" ? "capital" : "bitget") as ScalpRegimeVenue;
    const symbol = String(row.symbol || "").trim().toUpperCase();
    const key = `${venue}:${symbol}`;
    if (seenPair.has(key)) continue;
    seenPair.add(key);
    pairs.push({ venue, symbol });
  }
  const snapshotMap = pairs.length
    ? await loadScalpRegimeSnapshotsBulk({
        pairs,
        classifierVersion: cfg.classifierVersion,
        fromMs: currentWeekStartMs,
        toMs: currentWeekStartMs + ONE_WEEK_MS,
      })
    : new Map<string, Awaited<ReturnType<typeof loadScalpRegimeSnapshotsBulk>> extends Map<string, infer V> ? V : never>();
  const failClosedStaleMs = resolveScalpRegimeFailClosedStaleMs();

  const rows: V5DashboardDeploymentRow[] = [];
  let enabled = 0;
  let evaluated = 0;
  let missingEvidence = 0;
  let stale = 0;
  let latestEvaluationMs: number | null = null;
  let oldestEvaluationMs: number | null = null;

  for (const r of deployRows) {
    const venue = (String(r.venue || "").toLowerCase() === "capital" ? "capital" : "bitget") as ScalpRegimeVenue;
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

    const decision = decideV5Gate({
      evidence,
      consistencyException: isConsistencyExceptionPromotionGate(r.promotionGate),
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
    page: {
      scope,
      limit,
      offset,
      returned: rows.length,
      totalMatching,
      hasMore: offset + rows.length < totalMatching,
      includeInactive,
    },
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
