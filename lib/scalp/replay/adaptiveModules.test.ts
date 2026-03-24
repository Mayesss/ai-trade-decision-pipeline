import assert from "node:assert/strict";
import test from "node:test";

import {
  buildAdaptiveTrainingRowsFromMinuteCandles,
  deriveAdaptiveFeatureContext,
} from "../adaptive/features";
import { mineAdaptivePatternStats } from "../adaptive/mining";
import { computeEdge, computeHybridPriorScore } from "../adaptive/priors";
import {
  evaluateAdaptivePromotionDelta,
  shouldBreakAdaptivePromotionLock,
} from "../pipelineJobs";
import type { ScalpCandle } from "../types";

function buildSyntheticMinuteCandles(params: {
  startTsMs: number;
  minutes: number;
  startPrice: number;
}): ScalpCandle[] {
  const out: ScalpCandle[] = [];
  let price = params.startPrice;
  for (let i = 0; i < params.minutes; i += 1) {
    const tsMs = params.startTsMs + i * 60_000;
    const drift = Math.sin(i / 60) * 0.0004 + (i % 37 === 0 ? 0.0006 : 0);
    const open = price;
    const close = Math.max(0.01, open + drift);
    const hi = Math.max(open, close) + 0.0005;
    const lo = Math.min(open, close) - 0.0005;
    const volume = 100 + (i % 20);
    out.push([tsMs, open, hi, lo, close, volume]);
    price = close;
  }
  return out;
}

test("adaptive tokenization and mining are deterministic", () => {
  const candles = buildSyntheticMinuteCandles({
    startTsMs: Date.UTC(2026, 0, 1, 0, 0, 0),
    minutes: 8_000,
    startPrice: 1.1,
  });
  const rowsA = buildAdaptiveTrainingRowsFromMinuteCandles({
    candles1m: candles,
    symbol: "EURUSD",
    entrySessionProfile: "berlin",
    lookaheadBars: 4,
  });
  const rowsB = buildAdaptiveTrainingRowsFromMinuteCandles({
    candles1m: candles,
    symbol: "EURUSD",
    entrySessionProfile: "berlin",
    lookaheadBars: 4,
  });

  assert.equal(rowsA.length, rowsB.length);
  assert.ok(rowsA.length > 50);
  assert.deepEqual(
    rowsA.slice(0, 25).map((row) => row.featureHash),
    rowsB.slice(0, 25).map((row) => row.featureHash),
  );

  const minedA = mineAdaptivePatternStats({
    rows: rowsA,
    minSupport: 30,
    edgeScoreThreshold: 0.08,
    minLen: 2,
    maxLen: 3,
    maxPatterns: 20,
  });
  const minedB = mineAdaptivePatternStats({
    rows: rowsB,
    minSupport: 30,
    edgeScoreThreshold: 0.08,
    minLen: 2,
    maxLen: 3,
    maxPatterns: 20,
  });

  assert.deepEqual(minedA, minedB);
});

test("adaptive priors follow configured formula", () => {
  const edge = computeEdge(0.62, 0.8);
  assert.ok(edge > 0);

  const hybrid = computeHybridPriorScore({
    nLocal: 80,
    edgeLocal: 0.12,
    nSession: 300,
    edgeSession: 0.08,
    edgeGlobal: 0.02,
  });

  assert.ok(hybrid.wLocal > 0.6);
  assert.ok(hybrid.wSession >= 0);
  assert.ok(hybrid.wGlobal >= 0);
  assert.ok(hybrid.confidence > 0.5);
});

test("adaptive lock-break and delta gate predicates behave as expected", () => {
  assert.equal(
    shouldBreakAdaptivePromotionLock({
      latestWeeklyNetR: -1.6,
      latestWeeklyMaxDrawdownR: 1.1,
      baselineMaxDrawdownR: 1.2,
    }),
    true,
  );
  assert.equal(
    shouldBreakAdaptivePromotionLock({
      latestWeeklyNetR: -0.2,
      latestWeeklyMaxDrawdownR: 1.6,
      baselineMaxDrawdownR: 1.0,
    }),
    true,
  );
  assert.equal(
    shouldBreakAdaptivePromotionLock({
      latestWeeklyNetR: 0.2,
      latestWeeklyMaxDrawdownR: 1.2,
      baselineMaxDrawdownR: 1.1,
    }),
    false,
  );

  const pass = evaluateAdaptivePromotionDelta({
    candidateExpectancyR: 0.12,
    incumbentExpectancyR: 0.08,
    candidateMaxDrawdownR: 1.1,
    incumbentMaxDrawdownR: 1.2,
    minExpectancyDeltaR: 0.02,
  });
  assert.equal(pass.passed, true);

  const failExpectancy = evaluateAdaptivePromotionDelta({
    candidateExpectancyR: 0.09,
    incumbentExpectancyR: 0.08,
    candidateMaxDrawdownR: 1.0,
    incumbentMaxDrawdownR: 1.1,
    minExpectancyDeltaR: 0.02,
  });
  assert.equal(failExpectancy.passed, false);
  assert.equal(failExpectancy.reason, "adaptive_expectancy_delta_not_met");

  const failDrawdown = evaluateAdaptivePromotionDelta({
    candidateExpectancyR: 0.13,
    incumbentExpectancyR: 0.08,
    candidateMaxDrawdownR: 1.4,
    incumbentMaxDrawdownR: 1.2,
    minExpectancyDeltaR: 0.02,
  });
  assert.equal(failDrawdown.passed, false);
  assert.equal(failDrawdown.reason, "adaptive_drawdown_worse_than_incumbent");
});

test("adaptive feature context is stable for identical candles", () => {
  const candles = buildSyntheticMinuteCandles({
    startTsMs: Date.UTC(2026, 1, 1, 0, 0, 0),
    minutes: 1_000,
    startPrice: 1.2,
  });
  const baseCandles = candles.filter((_, idx) => idx % 15 === 0).slice(-280);
  const confirmCandles = candles.filter((_, idx) => idx % 3 === 0).slice(-360);
  const nowMs = baseCandles[baseCandles.length - 1]![0] + 15 * 60_000;

  const a = deriveAdaptiveFeatureContext({
    baseCandles,
    confirmCandles,
    nowMs,
    entrySessionProfile: "berlin",
  });
  const b = deriveAdaptiveFeatureContext({
    baseCandles,
    confirmCandles,
    nowMs,
    entrySessionProfile: "berlin",
  });

  assert.deepEqual(a.tokens, b.tokens);
  assert.equal(a.featureHash, b.featureHash);
});
