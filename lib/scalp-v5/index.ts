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

export const SCALP_V5_VERSION = "scalp_v5_cell_evidence_r1" as const;

export interface ScalpV5CellStat {
  trades: number;
  netR: number;
  expectancyR: number;
  wins: number;
  losses: number;
}

export interface ScalpV5CellEvidence {
  version: typeof SCALP_V5_VERSION;
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

export function buildScalpV5CellEvidence(params: {
  tagged: TaggedReplayTrade[];
  classifierVersion: string;
  evaluatedAtMs: number;
  holdoutFromMs: number;
  holdoutToMs: number;
  minTradesPerCell: number;
}): ScalpV5CellEvidence {
  const buckets = new Map<string, { netR: number; trades: number; wins: number; losses: number }>();
  for (const { trade, cellId } of params.tagged) {
    if (!cellId) continue;
    const r = Number(trade.rMultiple) || 0;
    const bucket = buckets.get(cellId) ?? { netR: 0, trades: 0, wins: 0, losses: 0 };
    bucket.netR += r;
    bucket.trades += 1;
    if (r > 0) bucket.wins += 1;
    else if (r < 0) bucket.losses += 1;
    buckets.set(cellId, bucket);
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
