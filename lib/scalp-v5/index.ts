// scalp-v5 = v3's 12-week recency replay + a per-regime-cell entry gate.
//
// Design intent (see conversation 2026-05-18): v4's 104-week walk-forward
// returned only ~52% positive prediction and was structurally a kill-switch.
// v5 replaces the weekly walk-forward with a cheaper signal: bucket the
// existing 12-week recency replay by the regime cell that was active at
// each trade's timestamp, persist per-cell expectancy on the deployment,
// and gate live entries on "current cell matches a profitable historic
// cell." Same negative-screen behavior, fresher data, no 2-year compute.

import type { ScalpReplayTrade } from "../scalp/replay/types";
import type { ScalpV4CellId } from "../scalp-v4/types";
import { startOfUtcWeekMondayMs } from "../scalp-v4/week";

// r3 added the parallel per-week trade-count / win / loss arrays so the
// incremental evaluator can subtract the oldest week's contribution from
// each cell when the holdout slides forward by one week. r2 evidence is
// still readable (the new arrays default to zeros) but any deployment
// loaded with r2 evidence falls back to a full replay on its next
// evaluation — the incremental path needs r3 to operate. Bumping to r4+
// requires another invalidation pass.
export const SCALP_V5_VERSION = "scalp_v5_cell_evidence_r3" as const;
export type ScalpV5EvidenceVersion =
  | "scalp_v5_cell_evidence_r2"
  | typeof SCALP_V5_VERSION;

export interface ScalpV5CellStat {
  trades: number;
  netR: number;
  expectancyR: number;
  wins: number;
  losses: number;
  // Per-week netR / trades / wins / losses for this cell, in chronological
  // order over the holdout window. Weeks with no trades in this cell are 0
  // in every array. Arrays must be the same length (= holdoutWeeks).
  weeklyNetR: number[];
  weeklyTrades: number[];
  weeklyWins: number[];
  weeklyLosses: number[];
}

export interface ScalpV5CellEvidence {
  version: ScalpV5EvidenceVersion;
  classifierVersion: string;
  evaluatedAtMs: number;
  holdoutFromMs: number;
  holdoutToMs: number;
  minTradesPerCell: number;
  cells: Record<string, ScalpV5CellStat>;
  // Cells in `cells` that meet the trade-count threshold AND have
  // expectancyR > 0. The live entry gate checks against this set.
  eligibleCells: string[];
}

export interface ScalpV5Config {
  classifierVersion: string;
  holdoutWeeks: number;
  minTradesPerCell: number;
}

export interface ScalpV5GateResult {
  blocked: boolean;
  shadowOnly: boolean;
  reasonCodes: string[];
  matchedCellId: string | null;
  evidence: ScalpV5CellStat | null;
}

export interface ScalpV5PromotionThresholds {
  minTotalNetR: number;
  minTotalTrades: number;
  minPositiveWeeks: number;
  minWorstWeekR: number;
  minTrailing4wNetR: number;
}

export interface ScalpV5PromotionMetrics {
  totalNetR: number;
  totalTrades: number;
  positiveWeeks: number;
  worstWeekR: number;
  trailing4wNetR: number;
  expectancyR: number;
  wins: number;
  losses: number;
  winRatePct: number | null;
  activeCells: number;
}

export interface ScalpV5PromotionEvaluation {
  qualified: boolean;
  reason:
    | "v5_strict_passed"
    | "v5_total_net_r_below_threshold"
    | "v5_total_trades_below_threshold"
    | "v5_positive_weeks_below_threshold"
    | "v5_worst_week_below_threshold"
    | "v5_trailing_4w_net_r_below_threshold";
  metrics: ScalpV5PromotionMetrics;
}

export function isScalpV5Enabled(): boolean {
  // Default ON. Set SCALP_V5_ENABLED=0 to disable the gate entirely (e.g.
  // before evidence has been evaluated on a fresh deployment).
  const raw = String(process.env.SCALP_V5_ENABLED ?? "true").trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(raw);
}

export function isScalpV5HardGateEnabled(): boolean {
  // Default ON — v5 blocks at the live entry path, no shadow mode.
  // Set SCALP_V5_HARD_GATE=0 to downgrade to shadow logging only.
  const raw = String(process.env.SCALP_V5_HARD_GATE ?? "true").trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(raw);
}

export function resolveScalpV5Config(): ScalpV5Config {
  const holdoutWeeksRaw = Number(process.env.SCALP_V5_HOLDOUT_WEEKS);
  const minTradesRaw = Number(process.env.SCALP_V5_MIN_TRADES_PER_CELL);
  return {
    classifierVersion:
      String(process.env.SCALP_V5_CLASSIFIER_VERSION || "").trim() ||
      "scalp_v4_macro_weekly_r1",
    holdoutWeeks: Number.isFinite(holdoutWeeksRaw) && holdoutWeeksRaw > 0
      ? Math.max(1, Math.min(52, Math.floor(holdoutWeeksRaw)))
      : 12,
    minTradesPerCell: Number.isFinite(minTradesRaw) && minTradesRaw > 0
      ? Math.max(1, Math.min(200, Math.floor(minTradesRaw)))
      : 8,
  };
}

export function resolveScalpV5EvidenceFreshness(params: {
  evaluatedAtMs: number | null | undefined;
  nowMs: number;
  staleOlderThanMs: number;
}): { stale: boolean; ageMs: number | null; staleBeforeMs: number } {
  const nowMs = Math.floor(Number(params.nowMs) || Date.now());
  const staleOlderThanMs = Math.max(
    60_000,
    Math.floor(Number(params.staleOlderThanMs) || 14 * 24 * 60 * 60_000),
  );
  const staleBeforeMs = nowMs - staleOlderThanMs;
  const evaluatedAtMs = Math.floor(Number(params.evaluatedAtMs) || 0);
  if (evaluatedAtMs <= 0) {
    return { stale: true, ageMs: null, staleBeforeMs };
  }
  return {
    stale: evaluatedAtMs < staleBeforeMs,
    ageMs: Math.max(0, nowMs - evaluatedAtMs),
    staleBeforeMs,
  };
}

export function evaluateScalpV5PromotionEvidence(params: {
  evidence: ScalpV5CellEvidence | null;
  thresholds: ScalpV5PromotionThresholds;
}): ScalpV5PromotionEvaluation {
  const cells = params.evidence?.cells || {};
  let totalNetR = 0;
  let totalTrades = 0;
  let wins = 0;
  let losses = 0;
  let activeCells = 0;
  const weeklySums: number[] = [];
  for (const cellValue of Object.values(cells)) {
    const cell = cellValue && typeof cellValue === "object" && !Array.isArray(cellValue)
      ? (cellValue as ScalpV5CellStat)
      : null;
    if (!cell) continue;
    totalNetR += Number(cell.netR) || 0;
    const cellTrades = Math.max(0, Math.floor(Number(cell.trades) || 0));
    totalTrades += cellTrades;
    wins += Math.max(0, Math.floor(Number(cell.wins) || 0));
    losses += Math.max(0, Math.floor(Number(cell.losses) || 0));
    if (cellTrades > 0) activeCells += 1;
    const weekly = Array.isArray(cell.weeklyNetR) ? cell.weeklyNetR : [];
    for (let idx = 0; idx < weekly.length; idx += 1) {
      weeklySums[idx] = (weeklySums[idx] ?? 0) + (Number(weekly[idx]) || 0);
    }
  }
  const metrics: ScalpV5PromotionMetrics = {
    totalNetR,
    totalTrades,
    positiveWeeks: weeklySums.filter((value) => value > 0).length,
    worstWeekR: weeklySums.length > 0 ? Math.min(...weeklySums) : 0,
    trailing4wNetR: weeklySums.slice(-4).reduce((acc, value) => acc + value, 0),
    expectancyR: totalTrades > 0 ? totalNetR / totalTrades : 0,
    wins,
    losses,
    winRatePct: wins + losses > 0 ? (wins / (wins + losses)) * 100 : null,
    activeCells,
  };
  if (metrics.totalNetR < params.thresholds.minTotalNetR) {
    return { qualified: false, reason: "v5_total_net_r_below_threshold", metrics };
  }
  if (metrics.totalTrades < params.thresholds.minTotalTrades) {
    return { qualified: false, reason: "v5_total_trades_below_threshold", metrics };
  }
  if (metrics.positiveWeeks < params.thresholds.minPositiveWeeks) {
    return { qualified: false, reason: "v5_positive_weeks_below_threshold", metrics };
  }
  if (metrics.worstWeekR < -Math.abs(params.thresholds.minWorstWeekR)) {
    return { qualified: false, reason: "v5_worst_week_below_threshold", metrics };
  }
  if (metrics.trailing4wNetR < params.thresholds.minTrailing4wNetR) {
    return { qualified: false, reason: "v5_trailing_4w_net_r_below_threshold", metrics };
  }
  return { qualified: true, reason: "v5_strict_passed", metrics };
}

// -----------------------------------------------------------------------------
// Pure helpers
// -----------------------------------------------------------------------------

export interface TaggedReplayTrade {
  trade: ScalpReplayTrade;
  cellId: ScalpV4CellId | null;
  weekStartMs: number;
}

export function tagTradesWithCells(params: {
  trades: ScalpReplayTrade[];
  snapshotsByWeekStart: Map<number, ScalpV4CellId>;
}): TaggedReplayTrade[] {
  const out: TaggedReplayTrade[] = [];
  for (const trade of params.trades) {
    const weekStartMs = startOfUtcWeekMondayMs(trade.entryTs);
    const cellId = params.snapshotsByWeekStart.get(weekStartMs) ?? null;
    out.push({ trade, cellId, weekStartMs });
  }
  return out;
}

const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

export function buildScalpV5CellEvidence(params: {
  tagged: TaggedReplayTrade[];
  classifierVersion: string;
  evaluatedAtMs: number;
  holdoutFromMs: number;
  holdoutToMs: number;
  minTradesPerCell: number;
}): ScalpV5CellEvidence {
  // The holdout window is week-aligned (caller responsibility); compute the
  // count of weeks once so weeklyNetR arrays are all the same length and
  // align by index across cells.
  const holdoutStartMs = startOfUtcWeekMondayMs(params.holdoutFromMs);
  const totalWeeks = Math.max(
    1,
    Math.round((startOfUtcWeekMondayMs(params.holdoutToMs) - holdoutStartMs) / ONE_WEEK_MS),
  );

  type Bucket = {
    netR: number;
    trades: number;
    wins: number;
    losses: number;
    weeklyNetR: number[];
    weeklyTrades: number[];
    weeklyWins: number[];
    weeklyLosses: number[];
  };
  const buckets = new Map<string, Bucket>();
  for (const { trade, cellId, weekStartMs } of params.tagged) {
    if (!cellId) continue;
    const r = Number(trade.rMultiple) || 0;
    let bucket = buckets.get(cellId);
    if (!bucket) {
      bucket = {
        netR: 0,
        trades: 0,
        wins: 0,
        losses: 0,
        weeklyNetR: new Array(totalWeeks).fill(0),
        weeklyTrades: new Array(totalWeeks).fill(0),
        weeklyWins: new Array(totalWeeks).fill(0),
        weeklyLosses: new Array(totalWeeks).fill(0),
      };
      buckets.set(cellId, bucket);
    }
    bucket.netR += r;
    bucket.trades += 1;
    if (r > 0) bucket.wins += 1;
    else if (r < 0) bucket.losses += 1;
    const weekIdx = Math.max(
      0,
      Math.min(totalWeeks - 1, Math.round((weekStartMs - holdoutStartMs) / ONE_WEEK_MS)),
    );
    bucket.weeklyNetR[weekIdx] = (bucket.weeklyNetR[weekIdx] ?? 0) + r;
    bucket.weeklyTrades[weekIdx] = (bucket.weeklyTrades[weekIdx] ?? 0) + 1;
    if (r > 0) bucket.weeklyWins[weekIdx] = (bucket.weeklyWins[weekIdx] ?? 0) + 1;
    else if (r < 0) bucket.weeklyLosses[weekIdx] = (bucket.weeklyLosses[weekIdx] ?? 0) + 1;
  }
  const cells: Record<string, ScalpV5CellStat> = {};
  const eligibleCells: string[] = [];
  for (const [cellId, b] of buckets.entries()) {
    const expectancyR = b.trades > 0 ? b.netR / b.trades : 0;
    cells[cellId] = {
      trades: b.trades,
      netR: b.netR,
      expectancyR,
      wins: b.wins,
      losses: b.losses,
      weeklyNetR: b.weeklyNetR,
      weeklyTrades: b.weeklyTrades,
      weeklyWins: b.weeklyWins,
      weeklyLosses: b.weeklyLosses,
    };
    if (b.trades >= params.minTradesPerCell && expectancyR > 0) {
      eligibleCells.push(cellId);
    }
  }
  return {
    version: SCALP_V5_VERSION,
    classifierVersion: params.classifierVersion,
    evaluatedAtMs: params.evaluatedAtMs,
    holdoutFromMs: params.holdoutFromMs,
    holdoutToMs: params.holdoutToMs,
    minTradesPerCell: params.minTradesPerCell,
    cells,
    eligibleCells,
  };
}

export function resolveScalpV5EntryBlock(params: {
  enabled: boolean;
  hardGate: boolean;
  evidence: ScalpV5CellEvidence | null;
  currentCellId: ScalpV4CellId | null;
  stale: boolean;
  minTradesPerCell: number;
}): ScalpV5GateResult {
  if (!params.enabled) {
    return { blocked: false, shadowOnly: false, reasonCodes: [], matchedCellId: null, evidence: null };
  }
  // Missing evidence means the v5 evaluator hasn't run yet on this
  // deployment. Don't block — falls through to upstream gates. The
  // evaluator will populate evidence on the next bulk pass and the gate
  // takes effect from the next entry.
  if (!params.evidence) {
    return {
      blocked: false,
      shadowOnly: false,
      reasonCodes: ["V5_CELL_EVIDENCE_MISSING"],
      matchedCellId: params.currentCellId ?? null,
      evidence: null,
    };
  }
  const reasonCodes: string[] = [];
  if (params.stale || !params.currentCellId) {
    reasonCodes.push("V5_CELL_DATA_STALE");
  }
  let cellStat: ScalpV5CellStat | null = null;
  if (params.currentCellId) {
    cellStat = params.evidence.cells[params.currentCellId] ?? null;
    if (!cellStat) {
      reasonCodes.push("V5_CELL_NOT_IN_EVIDENCE");
    } else if (cellStat.trades < params.minTradesPerCell) {
      reasonCodes.push("V5_CELL_INSUFFICIENT_TRADES");
    } else if (cellStat.expectancyR <= 0) {
      reasonCodes.push("V5_CELL_NEGATIVE_EXPECTANCY");
    }
  }
  const wouldBlock = reasonCodes.length > 0;
  return {
    blocked: wouldBlock && params.hardGate,
    shadowOnly: wouldBlock && !params.hardGate,
    reasonCodes: wouldBlock && !params.hardGate
      ? reasonCodes.map((code) => `${code}_SHADOW`)
      : reasonCodes,
    matchedCellId: params.currentCellId ?? null,
    evidence: cellStat,
  };
}

// Slides an existing 12-week evidence forward by exactly one week:
// subtracts the OLDEST week's per-cell contributions, appends the NEW
// week's contributions. Pure function — no IO. Used by the incremental
// v5 evaluator to update a deployment's evidence without re-replaying
// the full 12 weeks of candles.
//
// Contract: existing.holdoutToMs MUST equal newHoldoutToMs - ONE_WEEK_MS,
// and existing.version MUST be the current SCALP_V5_VERSION. Callers are
// responsible for validating these (else the per-week arrays don't align).
export function mergeIncrementalCellEvidence(params: {
  existing: ScalpV5CellEvidence;
  newWeekTagged: TaggedReplayTrade[];
  newHoldoutFromMs: number;
  newHoldoutToMs: number;
  classifierVersion: string;
  evaluatedAtMs: number;
  minTradesPerCell: number;
}): ScalpV5CellEvidence {
  const totalWeeks = Math.max(
    1,
    Math.round((params.newHoldoutToMs - params.newHoldoutFromMs) / ONE_WEEK_MS),
  );

  // Bucket the new week's trades by cell (single bucket per cell — all
  // trades in this batch share the same week, the just-completed one).
  type WeekContribution = { netR: number; trades: number; wins: number; losses: number };
  const newWeekStats = new Map<string, WeekContribution>();
  for (const { trade, cellId } of params.newWeekTagged) {
    if (!cellId) continue;
    const r = Number(trade.rMultiple) || 0;
    let stat = newWeekStats.get(cellId);
    if (!stat) {
      stat = { netR: 0, trades: 0, wins: 0, losses: 0 };
      newWeekStats.set(cellId, stat);
    }
    stat.netR += r;
    stat.trades += 1;
    if (r > 0) stat.wins += 1;
    else if (r < 0) stat.losses += 1;
  }

  // Drop oldest week + push new week per existing cell.
  const updatedCells: Record<string, ScalpV5CellStat> = {};
  for (const [cellId, oldStat] of Object.entries(params.existing.cells)) {
    const droppedTrades = oldStat.weeklyTrades[0] ?? 0;
    const droppedNetR = oldStat.weeklyNetR[0] ?? 0;
    const droppedWins = oldStat.weeklyWins[0] ?? 0;
    const droppedLosses = oldStat.weeklyLosses[0] ?? 0;
    const incoming = newWeekStats.get(cellId);
    const addedTrades = incoming?.trades ?? 0;
    const addedNetR = incoming?.netR ?? 0;
    const addedWins = incoming?.wins ?? 0;
    const addedLosses = incoming?.losses ?? 0;
    const trades = Math.max(0, oldStat.trades - droppedTrades + addedTrades);
    const netR = oldStat.netR - droppedNetR + addedNetR;
    const wins = Math.max(0, oldStat.wins - droppedWins + addedWins);
    const losses = Math.max(0, oldStat.losses - droppedLosses + addedLosses);
    if (trades === 0) {
      // Cell vacated the holdout window entirely — drop it from evidence.
      continue;
    }
    const weeklyNetR = [...oldStat.weeklyNetR.slice(1), addedNetR];
    const weeklyTrades = [...oldStat.weeklyTrades.slice(1), addedTrades];
    const weeklyWins = [...oldStat.weeklyWins.slice(1), addedWins];
    const weeklyLosses = [...oldStat.weeklyLosses.slice(1), addedLosses];
    updatedCells[cellId] = {
      trades,
      netR,
      expectancyR: trades > 0 ? netR / trades : 0,
      wins,
      losses,
      weeklyNetR,
      weeklyTrades,
      weeklyWins,
      weeklyLosses,
    };
  }

  // New cells that appear for the first time in this week. They get
  // zero-filled history for the prior 11 weeks plus the new week's data.
  for (const [cellId, incoming] of newWeekStats.entries()) {
    if (updatedCells[cellId]) continue;
    if (incoming.trades === 0) continue;
    const zeros = new Array(totalWeeks - 1).fill(0);
    updatedCells[cellId] = {
      trades: incoming.trades,
      netR: incoming.netR,
      expectancyR: incoming.trades > 0 ? incoming.netR / incoming.trades : 0,
      wins: incoming.wins,
      losses: incoming.losses,
      weeklyNetR: [...zeros, incoming.netR],
      weeklyTrades: [...zeros, incoming.trades],
      weeklyWins: [...zeros, incoming.wins],
      weeklyLosses: [...zeros, incoming.losses],
    };
  }

  const eligibleCells: string[] = [];
  for (const [cellId, stat] of Object.entries(updatedCells)) {
    if (stat.trades >= params.minTradesPerCell && stat.expectancyR > 0) {
      eligibleCells.push(cellId);
    }
  }

  return {
    version: SCALP_V5_VERSION,
    classifierVersion: params.classifierVersion,
    evaluatedAtMs: params.evaluatedAtMs,
    holdoutFromMs: params.newHoldoutFromMs,
    holdoutToMs: params.newHoldoutToMs,
    minTradesPerCell: params.minTradesPerCell,
    cells: updatedCells,
    eligibleCells,
  };
}

// Sunday is the v5 evaluation/promotion day — fresh weekly snapshot lands at
// Monday 00:00 UTC and the v5 evaluator re-runs the 12-week holdout against
// it overnight. We block new entries throughout Sunday UTC so trading uses
// only freshly-validated evidence starting Monday. Existing positions are
// not affected (this is an entry-side block only). Defaults to ON; set
// SCALP_V5_SUNDAY_NO_TRADE=0 to disable.
export function resolveScalpV5SundayBlock(nowMs: number): {
  blocked: boolean;
  reasonCodes: string[];
} {
  const raw = String(process.env.SCALP_V5_SUNDAY_NO_TRADE ?? "").trim().toLowerCase();
  if (["0", "false", "no", "off"].includes(raw)) {
    return { blocked: false, reasonCodes: [] };
  }
  const day = new Date(nowMs).getUTCDay(); // 0 = Sunday UTC
  if (day === 0) {
    return { blocked: true, reasonCodes: ["SUNDAY_EVALUATION_WINDOW"] };
  }
  return { blocked: false, reasonCodes: [] };
}
