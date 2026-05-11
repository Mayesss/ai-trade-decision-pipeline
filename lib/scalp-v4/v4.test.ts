import assert from "node:assert/strict";
import test from "node:test";

import {
  applyScalpV4Hysteresis,
  assessScalpV4CandleCoverage,
  bootstrapP05Expectancy,
  buildScalpV4ClassifierValidityReport,
  buildScalpV4RegimeEnvelope,
  buildScalpV4WeeklyBars,
  classifyScalpV4RawRegimes,
  resolveScalpV4EnvelopeBlock,
  startOfUtcWeekMondayMs,
  validityWeekStartFromCompletedWeekMs,
  type ScalpV4RawRegimeLabel,
  type ScalpV4RegimeSnapshot,
  type ScalpV4CellId,
  type ScalpV4WeeklyBar,
} from "./index";

const WEEK = 7 * 24 * 60 * 60 * 1000;
const MINUTE = 60_000;

function weeklyBar(idx: number, close: number, range = 1): ScalpV4WeeklyBar {
  const weekStartMs = Date.UTC(2024, 0, 1) + idx * WEEK;
  return {
    weekStartMs,
    open: close - 0.2,
    high: close + range,
    low: close - range,
    close,
    volume: 1,
  };
}

function rawLabel(idx: number, rawCellId: ScalpV4RawRegimeLabel["rawCellId"]): ScalpV4RawRegimeLabel {
  return {
    weekStartMs: Date.UTC(2026, 0, 5) + idx * WEEK,
    classifierVersion: "test",
    venue: "capital",
    symbol: "EURUSD",
    volAxis: rawCellId === "unknown" ? "unknown" : "mid",
    trendAxis: rawCellId === "unknown" ? "unknown" : "choppy",
    riskAxis: rawCellId === "unknown" ? "unknown" : "neutral",
    rawCellId,
    confidence: { volDistancePct: null, trendStrength: null, riskStrength: null },
    sourceCoverage: { symbolWeeks: 1, riskWeeks: 1, warmupComplete: rawCellId !== "unknown" },
    details: {},
  };
}

test("weekly regime label is valid for the week after completed data", () => {
  const completed = startOfUtcWeekMondayMs(Date.UTC(2026, 4, 3, 23, 59));
  assert.equal(completed, Date.UTC(2026, 3, 27));
  assert.equal(validityWeekStartFromCompletedWeekMs(completed), Date.UTC(2026, 4, 4));
});

test("hysteresis requires strict consecutive raw-cell observations before flipping", () => {
  const a = "vol=mid|trend=choppy|risk=neutral";
  const b = "vol=high|trend=choppy|risk=neutral";
  const rows = applyScalpV4Hysteresis(
    [
      rawLabel(0, a),
      rawLabel(1, b),
      rawLabel(2, b),
      rawLabel(3, a),
      rawLabel(4, b),
      rawLabel(5, b),
      rawLabel(6, b),
      rawLabel(7, b),
    ],
    { hysteresisWeeks: 4 },
  );
  assert.equal(rows[0]!.cellId, a);
  assert.equal(rows[2]!.cellId, a);
  assert.equal(rows[3]!.cellId, a);
  assert.equal(rows[6]!.cellId, a);
  assert.equal(rows[7]!.cellId, b);
  assert.deepEqual(rows[7]!.transition, { fromCellId: a, toCellId: b });
});

test("classifier labels unknown during warmup and does not let future bars change prior labels", () => {
  const bars = Array.from({ length: 18 }, (_, idx) => weeklyBar(idx, 100 + idx, 1 + (idx % 3)));
  const market = {
    usdJpy: Array.from({ length: 18 }, (_, idx) => weeklyBar(idx, 140 + idx * 0.3)),
    audJpy: Array.from({ length: 18 }, (_, idx) => weeklyBar(idx, 90 + idx * 0.2)),
  };
  const opts = {
    classifierVersion: "test",
    minVolLookbackWeeks: 4,
    preferredVolLookbackWeeks: 6,
    trendFastWeeks: 3,
    trendSlowWeeks: 6,
    adxWeeks: 3,
  };
  const first = classifyScalpV4RawRegimes({
    venue: "capital",
    symbol: "EURUSD",
    weeklyBars: bars,
    marketContext: market,
    options: opts,
  });
  const extended = classifyScalpV4RawRegimes({
    venue: "capital",
    symbol: "EURUSD",
    weeklyBars: [...bars, weeklyBar(18, 80, 6), weeklyBar(19, 82, 7)],
    marketContext: {
      usdJpy: [...market.usdJpy, weeklyBar(18, 120), weeklyBar(19, 121)],
      audJpy: [...market.audJpy, weeklyBar(18, 70), weeklyBar(19, 71)],
    },
    options: opts,
  });
  assert.equal(first[0]!.rawCellId, "unknown");
  assert.deepEqual(
    first.map((row) => row.rawCellId),
    extended.slice(0, first.length).map((row) => row.rawCellId),
  );
});

test("synthetic envelope identifies the one profitable cell and reports cross-regime trades", () => {
  const cellA = "vol=mid|trend=choppy|risk=neutral";
  const cellB = "vol=high|trend=trending_up|risk=risk_on";
  const snapshots: ScalpV4RegimeSnapshot[] = Array.from({ length: 24 }, (_, idx) => {
    const cellId = idx < 8 || (idx >= 16 && idx < 20) ? cellA : cellB;
    return {
      ...rawLabel(idx, cellId),
      cellId,
      pendingCellId: null,
      pendingWeeks: 0,
      transition: null,
    };
  });
  const windowIndexes = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 16, 17, 18, 19];
  const windows = windowIndexes.map((idx) => {
    const weekStart = snapshots[idx]!.weekStartMs;
    const cell = snapshots[idx]!.cellId;
    return {
      windowStartMs: weekStart,
      windowEndMs: weekStart + 12 * WEEK,
      trades: Array.from({ length: 2 }, (_row, tradeIdx) => ({
        entryTs: weekStart + tradeIdx * 60_000,
        exitTs: weekStart + (idx === 7 && tradeIdx === 0 ? 8 * WEEK : 2 * 60_000),
        rMultiple: cell === cellA ? 0.4 : -0.2,
      })),
    };
  });
  const envelope = buildScalpV4RegimeEnvelope({
    classifierVersion: "test",
    snapshots,
    windows,
    effectiveTrials: 100,
    thresholds: {
      minCellWindows: 4,
      minCellTrades: 8,
      minDistinctEpochs: 2,
      bootstrapResamples: 200,
    },
    evaluatedAtMs: Date.UTC(2026, 4, 1),
  });
  assert.equal(envelope.eligible, true);
  assert.deepEqual(envelope.allowedCells, [cellA]);
  const passed = envelope.cells.find((row) => row.cellId === cellA);
  assert.ok(passed);
  assert.ok(Number(passed!.crossRegimeTradePct) > 0);
});

test("bootstrap is deterministic for the same seed and block size", () => {
  const values = [0.2, 0.1, -0.05, 0.3, 0.15, -0.1, 0.25, 0.05];
  const a = bootstrapP05Expectancy({ windowExpectancyR: values, blockWeeks: 4, resamples: 200, seed: "same" });
  const b = bootstrapP05Expectancy({ windowExpectancyR: values, blockWeeks: 4, resamples: 200, seed: "same" });
  assert.equal(a, b);
});

test("classifier validity fails for too few epochs", () => {
  const cell = "vol=mid|trend=choppy|risk=neutral" as ScalpV4CellId;
  const snapshots = Array.from({ length: 20 }, (_, idx) => ({
    ...rawLabel(idx, cell),
    cellId: cell,
    pendingCellId: null,
    pendingWeeks: 0,
    transition: null,
  }));
  const report = buildScalpV4ClassifierValidityReport({
    snapshots,
    minEpochs: 3,
    maxEpochs: 12,
  });
  assert.equal(report.passed, false);
  assert.equal(report.reason, "too_few_regime_epochs");
});

test("classifier validity fails for too many epochs", () => {
  const a = "vol=mid|trend=choppy|risk=neutral" as ScalpV4CellId;
  const b = "vol=high|trend=trending_up|risk=risk_on" as ScalpV4CellId;
  const snapshots = Array.from({ length: 14 }, (_, idx) => {
    const cell = idx % 2 === 0 ? a : b;
    return {
      ...rawLabel(idx, cell),
      cellId: cell,
      pendingCellId: null,
      pendingWeeks: 0,
      transition: null,
    };
  });
  const report = buildScalpV4ClassifierValidityReport({
    snapshots,
    minEpochs: 3,
    maxEpochs: 12,
  });
  assert.equal(report.passed, false);
  assert.equal(report.reason, "too_many_regime_epochs");
});

test("classifier validity fails when market behavior summaries are empty", () => {
  const a = "vol=mid|trend=choppy|risk=neutral" as ScalpV4CellId;
  const b = "vol=high|trend=trending_up|risk=risk_on" as ScalpV4CellId;
  const sequence = [a, a, b, b, a, a];
  const snapshots = sequence.map((cell, idx) => ({
    ...rawLabel(idx, cell),
    cellId: cell,
    pendingCellId: null,
    pendingWeeks: 0,
    transition: null,
  }));
  const report = buildScalpV4ClassifierValidityReport({
    snapshots,
    marketBarsByName: { EURUSD: [] },
    minEpochs: 3,
    maxEpochs: 12,
  });
  assert.equal(report.passed, false);
  assert.equal(report.reason, "market_behavior_summary_empty");
});

test("shadow mode records v4 reasons without hard blocking", () => {
  const gate = resolveScalpV4EnvelopeBlock({
    enabled: true,
    hardGate: false,
    envelope: { eligible: true, allowedCells: ["vol=mid|trend=choppy|risk=neutral"] },
    currentCellId: "vol=high|trend=choppy|risk=neutral",
    stale: false,
  });
  assert.equal(gate.blocked, false);
  assert.equal(gate.shadowOnly, true);
  assert.ok(gate.reasonCodes.includes("V4_REGIME_ENVELOPE_BLOCKED_SHADOW"));

  const hard = resolveScalpV4EnvelopeBlock({
    enabled: true,
    hardGate: true,
    envelope: { eligible: true, allowedCells: ["vol=mid|trend=choppy|risk=neutral"] },
    currentCellId: "vol=high|trend=choppy|risk=neutral",
    stale: false,
  });
  assert.equal(hard.blocked, true);
  assert.ok(hard.reasonCodes.includes("V4_REGIME_ENVELOPE_BLOCKED"));
});

test("candle coverage requires enough candles and range edges", () => {
  const fromMs = Date.UTC(2024, 0, 1);
  const toMs = fromMs + 14 * 24 * 60 * MINUTE;
  const dense = Array.from({ length: 14 * 24 * 60 }, (_, idx) => {
    const price = 100 + idx * 0.0001;
    return [fromMs + idx * MINUTE, price, price, price, price, 1] as const;
  });
  const covered = assessScalpV4CandleCoverage({
    candles: dense.map((row) => [...row] as [number, number, number, number, number, number]),
    fromMs,
    toMs,
    minCoverageRatio: 0.9,
  });
  assert.equal(covered.ok, true);

  const late = dense.slice(8 * 24 * 60);
  const missingStart = assessScalpV4CandleCoverage({
    candles: late.map((row) => [...row] as [number, number, number, number, number, number]),
    fromMs,
    toMs,
    minCoverageRatio: 0.1,
  });
  assert.equal(missingStart.ok, false);
  assert.equal(missingStart.reason, "missing_window_start");
});
