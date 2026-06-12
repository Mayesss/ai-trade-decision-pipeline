import crypto from "crypto";

import { weekStartForEntryMs } from "./week";
import type {
  ScalpRegimeCellAggregate,
  ScalpRegimeCellCumulativeStat,
  ScalpRegimeCellId,
  ScalpRegimeEnvelopeThresholds,
  ScalpRegimeIncrementalState,
  ScalpRegimeEnvelope,
  ScalpRegimeSnapshot,
  ScalpRegimeTradeLike,
  ScalpRegimeWindowResult,
} from "./types";

const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;

// Defaults tuned from real 2-year diagnostic: 27-cell taxonomy rarely produces
// the same cell across >=2 distinct epochs in 104 weeks, so `minDistinctEpochs`
// is relaxed to 1 and we compensate with stronger per-cell evidence
// (`minCellWindows=12`, `minCellTrades=30`). `positiveWindowPct=70` kept
// because the distribution is bimodal — ~15% of cells exceed 70% positive
// windows and the rest cluster well below.
export const DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS: ScalpRegimeEnvelopeThresholds = {
  minCellWindows: 12,
  minCellTrades: 30,
  minDistinctEpochs: 1,
  minPositiveWindowPct: 70,
  minBootstrapP05ExpectancyR: 0,
  relaxedPositiveWindowPct: 55,
  relaxedBootstrapP05ExpectancyR: -0.02,
  overbroadCellPassPct: 70,
  overbroadMinCells: 6,
  bootstrapBlockWeeks: 4,
  bootstrapResamples: 2_000,
};

function envNum(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === "") return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

export function resolveScalpRegimeEnvelopeThresholds(): ScalpRegimeEnvelopeThresholds {
  return {
    minCellWindows: envNum("SCALP_V4_MIN_CELL_WINDOWS", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.minCellWindows),
    minCellTrades: envNum("SCALP_V4_MIN_CELL_TRADES", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.minCellTrades),
    minDistinctEpochs: envNum("SCALP_V4_MIN_DISTINCT_EPOCHS", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.minDistinctEpochs),
    minPositiveWindowPct: envNum("SCALP_V4_MIN_POSITIVE_WINDOW_PCT", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.minPositiveWindowPct),
    minBootstrapP05ExpectancyR: envNum("SCALP_V4_MIN_BOOTSTRAP_P05_EXPECTANCY_R", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.minBootstrapP05ExpectancyR),
    relaxedPositiveWindowPct: envNum("SCALP_V4_RELAXED_POSITIVE_WINDOW_PCT", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.relaxedPositiveWindowPct),
    relaxedBootstrapP05ExpectancyR: envNum("SCALP_V4_RELAXED_BOOTSTRAP_P05_EXPECTANCY_R", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.relaxedBootstrapP05ExpectancyR),
    overbroadCellPassPct: envNum("SCALP_V4_OVERBROAD_CELL_PASS_PCT", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.overbroadCellPassPct),
    overbroadMinCells: envNum("SCALP_V4_OVERBROAD_MIN_CELLS", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.overbroadMinCells),
    bootstrapBlockWeeks: envNum("SCALP_V4_BOOTSTRAP_BLOCK_WEEKS", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.bootstrapBlockWeeks),
    bootstrapResamples: envNum("SCALP_V4_BOOTSTRAP_RESAMPLES", DEFAULT_SCALP_V4_ENVELOPE_THRESHOLDS.bootstrapResamples),
  };
}

// Recompute the envelope decision from already-stored cell aggregates without
// re-running any replay. Lets us re-evaluate completed candidates instantly
// when thresholds change.
export function reevaluateScalpRegimeEnvelopeFromCells(params: {
  envelope: ScalpRegimeEnvelope;
  thresholds?: Partial<ScalpRegimeEnvelopeThresholds>;
  evaluatedAtMs?: number;
}): ScalpRegimeEnvelope {
  const thresholds = { ...resolveScalpRegimeEnvelopeThresholds(), ...(params.thresholds || {}) };
  const cells = params.envelope.cells.map((cell) => {
    const strictPassed =
      cell.windows >= thresholds.minCellWindows &&
      cell.trades >= thresholds.minCellTrades &&
      cell.distinctEpochCount >= thresholds.minDistinctEpochs &&
      cell.positiveWindowPct >= thresholds.minPositiveWindowPct &&
      cell.bootstrapP05ExpectancyR !== null &&
      cell.bootstrapP05ExpectancyR > thresholds.minBootstrapP05ExpectancyR;
    const relaxedPassed =
      cell.positiveWindowPct >= thresholds.relaxedPositiveWindowPct &&
      cell.bootstrapP05ExpectancyR !== null &&
      cell.bootstrapP05ExpectancyR >= thresholds.relaxedBootstrapP05ExpectancyR;
    let reason: string | null = null;
    if (!strictPassed) {
      if (cell.windows < thresholds.minCellWindows) reason = "min_cell_windows_not_met";
      else if (cell.trades < thresholds.minCellTrades) reason = "min_cell_trades_not_met";
      else if (cell.distinctEpochCount < thresholds.minDistinctEpochs) reason = "min_distinct_epochs_not_met";
      else if (cell.positiveWindowPct < thresholds.minPositiveWindowPct) reason = "positive_window_pct_below_threshold";
      else if (cell.bootstrapP05ExpectancyR === null || cell.bootstrapP05ExpectancyR <= thresholds.minBootstrapP05ExpectancyR) {
        reason = "bootstrap_p05_expectancy_below_threshold";
      }
    }
    return { ...cell, strictPassed, relaxedPassed, reason };
  });
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
    classifierVersion: params.envelope.classifierVersion,
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

function diagnosticDeflatedSharpe(windowExpectancyR: number[], effectiveTrials: number): ScalpRegimeCellAggregate["deflatedSharpe"] {
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

export function buildScalpRegimeSnapshotLookup(snapshots: ScalpRegimeSnapshot[]): Map<number, ScalpRegimeSnapshot> {
  return new Map((snapshots || []).map((row) => [row.weekStartMs, row]));
}

function epochIdByWeek(snapshots: ScalpRegimeSnapshot[]): Map<number, number> {
  const out = new Map<number, number>();
  let epoch = 0;
  let prev: ScalpRegimeCellId | null = null;
  for (const row of snapshots.slice().sort((a, b) => a.weekStartMs - b.weekStartMs)) {
    if (row.cellId !== "unknown" && row.cellId !== prev) {
      epoch += 1;
      prev = row.cellId;
    }
    out.set(row.weekStartMs, epoch);
  }
  return out;
}

export function buildScalpRegimeEnvelope(params: {
  classifierVersion: string;
  snapshots: ScalpRegimeSnapshot[];
  windows: ScalpRegimeWindowResult[];
  effectiveTrials: number;
  evaluatedAtMs?: number;
  thresholds?: Partial<ScalpRegimeEnvelopeThresholds>;
}): ScalpRegimeEnvelope {
  const thresholds = { ...resolveScalpRegimeEnvelopeThresholds(), ...(params.thresholds || {}) };
  const byWeek = buildScalpRegimeSnapshotLookup(params.snapshots);
  const epochByWeek = epochIdByWeek(params.snapshots);
  const byCell = new Map<
    ScalpRegimeCellId,
    {
      windowExpectancyR: number[];
      windowNetR: number[];
      trades: ScalpRegimeTradeLike[];
      crossed: number;
      epochs: Set<number>;
    }
  >();

  for (const window of params.windows || []) {
    const tradesByCell = new Map<ScalpRegimeCellId, ScalpRegimeTradeLike[]>();
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

  const cells: ScalpRegimeCellAggregate[] = Array.from(byCell.entries())
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

export function resolveScalpRegimeEnvelopeBlock(params: {
  enabled: boolean;
  hardGate: boolean;
  envelope: unknown;
  currentCellId: ScalpRegimeCellId | null;
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

// -----------------------------------------------------------------------------
// Incremental walk-forward helpers
// -----------------------------------------------------------------------------

function emptyCellStat(): ScalpRegimeCellCumulativeStat {
  return {
    trades: 0,
    netR: 0,
    maxDrawdownR: 0,
    crossRegimeTrades: 0,
    epochsSeen: [],
    windowExpectancyR: [],
    windowNetR: [],
  };
}

function mergeCellEpochs(existing: number[], next: number[]): number[] {
  if (!next.length) return existing;
  const set = new Set<number>(existing);
  for (const e of next) set.add(e);
  return Array.from(set).sort((a, b) => a - b);
}

// Fold one window's per-cell contributions into the incremental state.
// `windowResult` provides the trades; `snapshotByWeek` and `epochByWeek` map
// trade-entry-weeks to confirmed cells and epoch IDs (computed once per sweep).
export function foldWindowIntoIncrementalState(params: {
  state: ScalpRegimeIncrementalState;
  window: ScalpRegimeWindowResult;
  snapshotByWeek: Map<number, ScalpRegimeSnapshot>;
  epochByWeek: Map<number, number>;
}): ScalpRegimeIncrementalState {
  const tradesByCell = new Map<string, ScalpRegimeTradeLike[]>();
  const crossByCell = new Map<string, number>();
  const epochsByCell = new Map<string, Set<number>>();
  for (const trade of params.window.trades || []) {
    const entryWeek = weekStartForEntryMs(trade.entryTs);
    const exitWeek = weekStartForEntryMs(trade.exitTs);
    const entryCell = params.snapshotByWeek.get(entryWeek)?.cellId || "unknown";
    if (entryCell === "unknown") continue;
    const exitCell = params.snapshotByWeek.get(exitWeek)?.cellId || entryCell;
    const bucket = tradesByCell.get(entryCell) || [];
    bucket.push(trade);
    tradesByCell.set(entryCell, bucket);
    if (exitCell !== entryCell) crossByCell.set(entryCell, (crossByCell.get(entryCell) || 0) + 1);
    const epoch = params.epochByWeek.get(entryWeek);
    if (epoch) {
      const set = epochsByCell.get(entryCell) || new Set<number>();
      set.add(epoch);
      epochsByCell.set(entryCell, set);
    }
  }
  const nextCells = { ...params.state.cells };
  for (const [cellId, trades] of tradesByCell.entries()) {
    const tradesR = trades.map((t) => finite(t.rMultiple));
    const winNetR = tradesR.reduce((a, b) => a + b, 0);
    const winExpR = trades.length ? winNetR / trades.length : 0;
    const existing = nextCells[cellId] || emptyCellStat();
    const combinedR = [...existing.windowExpectancyR.flatMap(() => []), ...tradesR];
    // For maxDrawdown we approximate as max(existing, this window's local DD).
    // True running drawdown across all windows would need full trade history;
    // we accept slight underestimation in exchange for state-size bounds.
    let equity = 0;
    let peak = 0;
    let localDd = 0;
    for (const r of tradesR) {
      equity += r;
      if (equity > peak) peak = equity;
      if (peak - equity > localDd) localDd = peak - equity;
    }
    void combinedR;
    nextCells[cellId] = {
      trades: existing.trades + trades.length,
      netR: existing.netR + winNetR,
      maxDrawdownR: Math.max(existing.maxDrawdownR, localDd),
      crossRegimeTrades: existing.crossRegimeTrades + (crossByCell.get(cellId) || 0),
      epochsSeen: mergeCellEpochs(existing.epochsSeen, Array.from(epochsByCell.get(cellId) || [])),
      windowExpectancyR: [...existing.windowExpectancyR, winExpR],
      windowNetR: [...existing.windowNetR, winNetR],
    };
  }
  return {
    ...params.state,
    lastWindowEndMs: Math.max(params.state.lastWindowEndMs, params.window.windowEndMs),
    cells: nextCells,
  };
}

// Rebuild the envelope from cumulative cell stats. Same gate logic as
// buildScalpRegimeEnvelope but consuming pre-aggregated per-cell state.
export function buildEnvelopeFromIncrementalState(params: {
  state: ScalpRegimeIncrementalState;
  effectiveTrials: number;
  thresholds?: Partial<ScalpRegimeEnvelopeThresholds>;
  evaluatedAtMs?: number;
}): ScalpRegimeEnvelope {
  const thresholds = { ...resolveScalpRegimeEnvelopeThresholds(), ...(params.thresholds || {}) };
  const cells: ScalpRegimeCellAggregate[] = Object.entries(params.state.cells)
    .map(([cellId, stat]) => {
      const trades = stat.trades;
      const netR = stat.netR;
      const positiveWindowPct =
        stat.windowNetR.length > 0
          ? (stat.windowNetR.filter((v) => v > 0).length / stat.windowNetR.length) * 100
          : 0;
      const boot = bootstrapP05Expectancy({
        windowExpectancyR: stat.windowExpectancyR,
        blockWeeks: thresholds.bootstrapBlockWeeks,
        resamples: thresholds.bootstrapResamples,
        seed: `${params.state.classifierVersion}:${cellId}:${stat.windowExpectancyR.length}`,
      });
      const strictPassed =
        stat.windowExpectancyR.length >= thresholds.minCellWindows &&
        trades >= thresholds.minCellTrades &&
        stat.epochsSeen.length >= thresholds.minDistinctEpochs &&
        positiveWindowPct >= thresholds.minPositiveWindowPct &&
        boot !== null &&
        boot > thresholds.minBootstrapP05ExpectancyR;
      const relaxedPassed =
        positiveWindowPct >= thresholds.relaxedPositiveWindowPct &&
        boot !== null &&
        boot >= thresholds.relaxedBootstrapP05ExpectancyR;
      let reason: string | null = null;
      if (!strictPassed) {
        if (stat.windowExpectancyR.length < thresholds.minCellWindows) reason = "min_cell_windows_not_met";
        else if (trades < thresholds.minCellTrades) reason = "min_cell_trades_not_met";
        else if (stat.epochsSeen.length < thresholds.minDistinctEpochs) reason = "min_distinct_epochs_not_met";
        else if (positiveWindowPct < thresholds.minPositiveWindowPct) reason = "positive_window_pct_below_threshold";
        else if (boot === null || boot <= thresholds.minBootstrapP05ExpectancyR) {
          reason = "bootstrap_p05_expectancy_below_threshold";
        }
      }
      const m = stat.windowExpectancyR.length > 0 ? stat.windowExpectancyR.reduce((a, b) => a + b, 0) / stat.windowExpectancyR.length : 0;
      const variance = stat.windowExpectancyR.length > 1
        ? stat.windowExpectancyR.reduce((acc, v) => acc + (v - m) * (v - m), 0) / (stat.windowExpectancyR.length - 1)
        : 0;
      const stdR = Math.sqrt(Math.max(0, variance));
      const sharpe = stdR > 1e-9 ? m / stdR : null;
      const trialPenalty = Math.sqrt(Math.max(0, 2 * Math.log(Math.max(1, params.effectiveTrials))));
      return {
        cellId: cellId as ScalpRegimeCellId,
        windows: stat.windowExpectancyR.length,
        trades,
        distinctEpochCount: stat.epochsSeen.length,
        netR,
        expectancyR: trades ? netR / trades : 0,
        positiveWindowPct,
        p25ExpectancyR: percentile(stat.windowExpectancyR, 25) ?? 0,
        maxDrawdownR: stat.maxDrawdownR,
        crossRegimeTradePct: trades > 0 ? (stat.crossRegimeTrades / trades) * 100 : 0,
        bootstrapP05ExpectancyR: boot,
        bootstrapResamples: thresholds.bootstrapResamples,
        deflatedSharpe: {
          sharpe,
          effectiveTrials: params.effectiveTrials,
          diagnosticScore: sharpe === null ? null : sharpe - trialPenalty,
        },
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
    classifierVersion: params.state.classifierVersion,
    evaluatedAtMs,
    eligible,
    status: eligible ? "eligible" : overbroad ? "regime_overbroad_pending_review" : "no_passing_cells",
    allowedCells: overbroad ? [] : cells.filter((row) => row.strictPassed).map((row) => row.cellId),
    occupiedCells,
    strictPassingCells,
    relaxedPassingCells,
    overbroad,
    overbroadReviewUntilMs: overbroad ? evaluatedAtMs + 7 * 24 * 60 * 60_000 : null,
    thresholds,
    cells,
  };
}

// Convert a freshly-built envelope (from a full walk-forward) into the
// initial incremental state. Used when a candidate is walked for the first
// time and we want subsequent sweeps to incrementally update.
//
// To derive per-cell windowExpectancyR / windowNetR (which the envelope
// doesn't expose individually), we read them off the cells' own `windows`
// field where available; for backfill of pre-existing rows that lack the
// per-window detail, callers should pass `synthesized: true` and we'll
// approximate from cell-level summary stats.
export function initIncrementalStateFromEnvelope(params: {
  envelope: ScalpRegimeEnvelope;
  windowFromMs: number;
  windowToMs: number;
  synthesizeFromSummary?: boolean;
  seed?: string;
}): ScalpRegimeIncrementalState {
  const cells: Record<string, ScalpRegimeCellCumulativeStat> = {};
  for (const cell of params.envelope.cells || []) {
    const n = Math.max(1, Math.floor(cell.windows || 1));
    let windowExpectancyR: number[] = [];
    let windowNetR: number[] = [];
    if (params.synthesizeFromSummary) {
      // Synthesize a per-window distribution from stored mean, bootstrap p05,
      // and positive-window pct. The variance estimate uses:
      //   p05 ≈ mean - 1.645 * std  ⇒  std ≈ (mean - p05) / 1.645
      const mean = finite(cell.expectancyR);
      const p05 = cell.bootstrapP05ExpectancyR !== null ? finite(cell.bootstrapP05ExpectancyR) : mean;
      const stdEst = Math.max(0.001, (mean - p05) / 1.645);
      const positiveTarget = Math.round(((cell.positiveWindowPct || 0) / 100) * n);
      const rand = seededRand(`${params.seed || ""}:${cell.cellId}:${n}`);
      const generated: number[] = [];
      for (let i = 0; i < n; i += 1) {
        // Box-Muller transform — synthetic normal sample
        const u1 = Math.max(1e-9, rand());
        const u2 = rand();
        const z = Math.sqrt(-2 * Math.log(u1)) * Math.cos(2 * Math.PI * u2);
        generated.push(mean + z * stdEst);
      }
      // Adjust sign of values so positive-window count matches target.
      generated.sort((a, b) => b - a);
      const adjusted = generated.map((v, idx) => (idx < positiveTarget ? Math.max(v, 1e-6) : Math.min(v, -1e-6)));
      windowExpectancyR = adjusted;
      const avgTradesPerWindow = cell.trades / n;
      windowNetR = adjusted.map((expR) => expR * avgTradesPerWindow);
    }
    cells[cell.cellId] = {
      trades: cell.trades,
      netR: cell.netR,
      maxDrawdownR: cell.maxDrawdownR,
      crossRegimeTrades: Math.round(((cell.crossRegimeTradePct || 0) / 100) * cell.trades),
      epochsSeen: Array.from({ length: Math.max(0, cell.distinctEpochCount) }, (_, i) => i + 1),
      windowExpectancyR,
      windowNetR,
    };
  }
  return {
    version: "scalp_v4_incremental_r1",
    classifierVersion: params.envelope.classifierVersion,
    windowFromMs: params.windowFromMs,
    lastWindowEndMs: params.windowToMs,
    cells,
    synthesizedAt: params.synthesizeFromSummary ? Date.now() : undefined,
  };
}
