import crypto from "crypto";

import { weekStartForEntryMs } from "./week";
import type {
  ScalpV4CellAggregate,
  ScalpV4CellId,
  ScalpV4EnvelopeThresholds,
  ScalpV4RegimeEnvelope,
  ScalpV4RegimeSnapshot,
  ScalpV4TradeLike,
  ScalpV4WindowResult,
} from "./types";

const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

export const DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS: ScalpV4EnvelopeThresholds = {
  minCellWindows: 4,
  minCellTrades: 15,
  minDistinctEpochs: 2,
  minPositiveWindowPct: 70,
  minBootstrapP05ExpectancyR: 0,
  relaxedPositiveWindowPct: 55,
  relaxedBootstrapP05ExpectancyR: -0.02,
  overbroadCellPassPct: 70,
  overbroadMinCells: 6,
  bootstrapBlockWeeks: 4,
  bootstrapResamples: 2_000,
};

function finite(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function mean(values: number[]): number {
  return values.length ? values.reduce((acc, row) => acc + row, 0) / values.length : 0;
}

function percentile(values: number[], pct: number): number | null {
  const rows = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (!rows.length) return null;
  const idx = Math.max(0, Math.min(rows.length - 1, Math.floor((pct / 100) * rows.length)));
  return rows[idx]!;
}

function maxDrawdown(values: number[]): number {
  let equity = 0;
  let peak = 0;
  let dd = 0;
  for (const value of values) {
    equity += value;
    peak = Math.max(peak, equity);
    dd = Math.max(dd, peak - equity);
  }
  return dd;
}

function std(values: number[]): number {
  if (values.length < 2) return 0;
  const m = mean(values);
  const variance = values.reduce((acc, row) => acc + (row - m) * (row - m), 0) / (values.length - 1);
  return variance > 0 ? Math.sqrt(variance) : 0;
}

function seededRand(seed: string): () => number {
  let s = crypto.createHash("sha1").update(seed).digest().readUInt32BE(0);
  return () => {
    s = (1664525 * s + 1013904223) >>> 0;
    return s / 0x100000000;
  };
}

export function bootstrapP05Expectancy(params: {
  windowExpectancyR: number[];
  blockWeeks?: number;
  resamples?: number;
  seed?: string;
}): number | null {
  const values = (params.windowExpectancyR || []).filter(Number.isFinite);
  if (values.length <= 0) return null;
  const blockWeeks = Math.max(1, Math.floor(params.blockWeeks || DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.bootstrapBlockWeeks));
  const resamples = Math.max(1, Math.floor(params.resamples || DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.bootstrapResamples));
  const rand = seededRand(params.seed || JSON.stringify(values));
  const sampledMeans: number[] = [];
  for (let i = 0; i < resamples; i += 1) {
    const sample: number[] = [];
    while (sample.length < values.length) {
      // Rolling 12-week walk-forward windows overlap heavily, so use monthly
      // blocks to avoid treating adjacent windows as independent samples.
      const start = Math.floor(rand() * values.length);
      for (let j = 0; j < blockWeeks && sample.length < values.length; j += 1) {
        sample.push(values[(start + j) % values.length]!);
      }
    }
    sampledMeans.push(mean(sample));
  }
  return percentile(sampledMeans, 5);
}

function diagnosticDeflatedSharpe(windowExpectancyR: number[], effectiveTrials: number): ScalpV4CellAggregate["deflatedSharpe"] {
  const m = mean(windowExpectancyR);
  const s = std(windowExpectancyR);
  if (windowExpectancyR.length < 2 || s <= 1e-9) {
    return { sharpe: null, effectiveTrials, diagnosticScore: null };
  }
  const sharpe = m / s;
  const trialPenalty = Math.sqrt(Math.max(0, 2 * Math.log(Math.max(1, effectiveTrials))));
  return {
    sharpe,
    effectiveTrials,
    diagnosticScore: sharpe - trialPenalty,
  };
}

export function buildScalpV4SnapshotLookup(snapshots: ScalpV4RegimeSnapshot[]): Map<number, ScalpV4RegimeSnapshot> {
  return new Map((snapshots || []).map((row) => [row.weekStartMs, row]));
}

function epochIdByWeek(snapshots: ScalpV4RegimeSnapshot[]): Map<number, number> {
  const out = new Map<number, number>();
  let epoch = 0;
  let prev: ScalpV4CellId | null = null;
  for (const row of snapshots.slice().sort((a, b) => a.weekStartMs - b.weekStartMs)) {
    if (row.cellId !== "unknown" && row.cellId !== prev) {
      epoch += 1;
      prev = row.cellId;
    }
    out.set(row.weekStartMs, epoch);
  }
  return out;
}

export function buildScalpV4RegimeEnvelope(params: {
  classifierVersion: string;
  snapshots: ScalpV4RegimeSnapshot[];
  windows: ScalpV4WindowResult[];
  effectiveTrials: number;
  evaluatedAtMs?: number;
  thresholds?: Partial<ScalpV4EnvelopeThresholds>;
}): ScalpV4RegimeEnvelope {
  const thresholds = { ...DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS, ...(params.thresholds || {}) };
  const byWeek = buildScalpV4SnapshotLookup(params.snapshots);
  const epochByWeek = epochIdByWeek(params.snapshots);
  const byCell = new Map<
    ScalpV4CellId,
    {
      windowExpectancyR: number[];
      windowNetR: number[];
      trades: ScalpV4TradeLike[];
      crossed: number;
      epochs: Set<number>;
    }
  >();

  for (const window of params.windows || []) {
    const tradesByCell = new Map<ScalpV4CellId, ScalpV4TradeLike[]>();
    for (const trade of window.trades || []) {
      const entryWeek = weekStartForEntryMs(trade.entryTs);
      const exitWeek = weekStartForEntryMs(trade.exitTs);
      const entryCell = byWeek.get(entryWeek)?.cellId || "unknown";
      if (entryCell === "unknown") continue;
      const exitCell = byWeek.get(exitWeek)?.cellId || entryCell;
      const bucket = tradesByCell.get(entryCell) || [];
      bucket.push(trade);
      tradesByCell.set(entryCell, bucket);
      const current = byCell.get(entryCell) || {
        windowExpectancyR: [],
        windowNetR: [],
        trades: [],
        crossed: 0,
        epochs: new Set<number>(),
      };
      current.trades.push(trade);
      if (exitCell !== entryCell) current.crossed += 1;
      const epoch = epochByWeek.get(entryWeek);
      if (epoch) current.epochs.add(epoch);
      byCell.set(entryCell, current);
    }
    for (const [cell, trades] of tradesByCell.entries()) {
      const netR = trades.reduce((acc, row) => acc + finite(row.rMultiple), 0);
      const current = byCell.get(cell);
      if (!current) continue;
      current.windowNetR.push(netR);
      current.windowExpectancyR.push(trades.length ? netR / trades.length : 0);
    }
  }

  const cells: ScalpV4CellAggregate[] = Array.from(byCell.entries())
    .map(([cellId, row]) => {
      const tradesR = row.trades.map((trade) => finite(trade.rMultiple));
      const trades = tradesR.length;
      const netR = tradesR.reduce((acc, value) => acc + value, 0);
      const positiveWindowPct =
        row.windowNetR.length > 0
          ? (row.windowNetR.filter((value) => value > 0).length / row.windowNetR.length) * 100
          : 0;
      const boot = bootstrapP05Expectancy({
        windowExpectancyR: row.windowExpectancyR,
        blockWeeks: thresholds.bootstrapBlockWeeks,
        resamples: thresholds.bootstrapResamples,
        seed: `${params.classifierVersion}:${cellId}:${row.windowExpectancyR.join(",")}`,
      });
      const strictPassed =
        row.windowExpectancyR.length >= thresholds.minCellWindows &&
        trades >= thresholds.minCellTrades &&
        row.epochs.size >= thresholds.minDistinctEpochs &&
        positiveWindowPct >= thresholds.minPositiveWindowPct &&
        boot !== null &&
        boot > thresholds.minBootstrapP05ExpectancyR;
      const relaxedPassed =
        positiveWindowPct >= thresholds.relaxedPositiveWindowPct &&
        boot !== null &&
        boot >= thresholds.relaxedBootstrapP05ExpectancyR;
      let reason: string | null = null;
      if (!strictPassed) {
        if (row.windowExpectancyR.length < thresholds.minCellWindows) reason = "min_cell_windows_not_met";
        else if (trades < thresholds.minCellTrades) reason = "min_cell_trades_not_met";
        else if (row.epochs.size < thresholds.minDistinctEpochs) reason = "min_distinct_epochs_not_met";
        else if (positiveWindowPct < thresholds.minPositiveWindowPct) reason = "positive_window_pct_below_threshold";
        else if (boot === null || boot <= thresholds.minBootstrapP05ExpectancyR) reason = "bootstrap_p05_expectancy_below_threshold";
      }
      return {
        cellId,
        windows: row.windowExpectancyR.length,
        trades,
        distinctEpochCount: row.epochs.size,
        netR,
        expectancyR: trades ? netR / trades : 0,
        positiveWindowPct,
        p25ExpectancyR: percentile(row.windowExpectancyR, 25) ?? 0,
        maxDrawdownR: maxDrawdown(tradesR),
        crossRegimeTradePct: trades > 0 ? (row.crossed / trades) * 100 : 0,
        bootstrapP05ExpectancyR: boot,
        bootstrapResamples: thresholds.bootstrapResamples,
        deflatedSharpe: diagnosticDeflatedSharpe(row.windowExpectancyR, params.effectiveTrials),
        strictPassed,
        relaxedPassed,
        reason,
      };
    })
    .sort((a, b) => b.expectancyR - a.expectancyR);

  const occupiedCells = cells.length;
  const strictPassingCells = cells.filter((row) => row.strictPassed).length;
  const relaxedPassingCells = cells.filter((row) => row.relaxedPassed).length;
  const overbroad =
    occupiedCells >= thresholds.overbroadMinCells &&
    strictPassingCells > 0 &&
    (strictPassingCells / occupiedCells) * 100 >= thresholds.overbroadCellPassPct;
  const evaluatedAtMs = Math.floor(params.evaluatedAtMs || Date.now());
  const eligible = strictPassingCells > 0 && !overbroad;
  return {
    version: "scalp_v4_regime_envelope_r1",
    classifierVersion: params.classifierVersion,
    evaluatedAtMs,
    eligible,
    status: eligible ? "eligible" : overbroad ? "regime_overbroad_pending_review" : "no_passing_cells",
    allowedCells: overbroad ? [] : cells.filter((row) => row.strictPassed).map((row) => row.cellId),
    occupiedCells,
    strictPassingCells,
    relaxedPassingCells,
    overbroad,
    overbroadReviewUntilMs: overbroad ? evaluatedAtMs + SEVEN_DAYS_MS : null,
    thresholds,
    cells,
  };
}

export function resolveScalpV4EnvelopeBlock(params: {
  enabled: boolean;
  hardGate: boolean;
  envelope: unknown;
  currentCellId: ScalpV4CellId | null;
  stale: boolean;
}): { blocked: boolean; shadowOnly: boolean; reasonCodes: string[] } {
  if (!params.enabled) return { blocked: false, shadowOnly: false, reasonCodes: [] };
  const reasonCodes: string[] = [];
  if (params.stale || !params.currentCellId) {
    reasonCodes.push("V4_REGIME_DATA_STALE");
  }
  const envelope = params.envelope && typeof params.envelope === "object" ? (params.envelope as Record<string, unknown>) : {};
  const allowed = Array.isArray(envelope.allowedCells)
    ? envelope.allowedCells.map((row) => String(row || "")).filter(Boolean)
    : [];
  const eligible = Boolean(envelope.eligible);
  if (!eligible || !params.currentCellId || !allowed.includes(params.currentCellId)) {
    reasonCodes.push("V4_REGIME_ENVELOPE_BLOCKED");
  }
  const wouldBlock = reasonCodes.length > 0;
  return {
    blocked: wouldBlock && params.hardGate,
    shadowOnly: wouldBlock && !params.hardGate,
    reasonCodes: wouldBlock && !params.hardGate ? reasonCodes.map((code) => `${code}_SHADOW`) : reasonCodes,
  };
}
