import test from "node:test";
import assert from "node:assert/strict";

import {
  evaluateFreshCompletedDeploymentWeeks,
  evaluateWeeklyRobustnessGate,
  type ScalpWeeklyRobustnessMetrics,
  type SyncResearchWeeklyPolicy,
} from "../promotionPolicy";

function basePolicy(): SyncResearchWeeklyPolicy {
  return {
    enabled: true,
    topKPerSymbol: 2,
    globalMaxSymbols: 6,
    globalMaxDeployments: 12,
    lookbackDays: 91,
    minCandlesPerSlice: 180,
    requireWinnerShortlist: true,
    minSlices: 8,
    minProfitablePct: 55,
    minMedianExpectancyR: 0.02,
    minP25ExpectancyR: -0.02,
    minWorstNetR: -1.5,
    maxTopWeekPnlConcentrationPct: 55,
    minFourWeekNetR: 8,
  };
}

function baseMetrics(): ScalpWeeklyRobustnessMetrics {
  return {
    slices: 12,
    profitableSlices: 8,
    profitablePct: (8 / 12) * 100,
    meanExpectancyR: 0.08,
    trimmedMeanExpectancyR: 0.07,
    p25ExpectancyR: 0.01,
    medianExpectancyR: 0.06,
    worstNetR: -1.0,
    worstMaxDrawdownR: 1.8,
    topWeekPnlConcentrationPct: 48,
    totalNetR: 30,
    fourWeekGroupNetR: [8, 9.5, 12.5],
    fourWeekGroupsEvaluated: 3,
    fourWeekMinNetR: 8,
    evaluatedAtMs: Date.now(),
  };
}

test("weekly robustness gate accepts candidates when each 4-week block netR meets threshold", () => {
  const out = evaluateWeeklyRobustnessGate(baseMetrics(), basePolicy());
  assert.equal(out.passed, true);
  assert.equal(out.reason, null);
});

test("weekly robustness gate rejects candidates when any 4-week block netR is below threshold", () => {
  const metrics = {
    ...baseMetrics(),
    fourWeekGroupNetR: [8, 7.99, 12],
    fourWeekMinNetR: 7.99,
  };
  const out = evaluateWeeklyRobustnessGate(metrics, basePolicy());
  assert.equal(out.passed, false);
  assert.equal(out.reason, "weekly_four_week_net_r_below_threshold");
});

test("weekly robustness gate rejects candidates when 3 full 4-week groups are not available", () => {
  const metrics = {
    ...baseMetrics(),
    slices: 11,
    fourWeekGroupNetR: [9, 11],
    fourWeekGroupsEvaluated: 2,
    fourWeekMinNetR: 9,
  };
  const out = evaluateWeeklyRobustnessGate(metrics, basePolicy());
  assert.equal(out.passed, false);
  assert.equal(out.reason, "weekly_four_week_groups_missing");
});

test("fresh completed week evaluation includes the just-finished week on Sunday rollover", () => {
  const weekMs = 7 * 24 * 60 * 60_000;
  const windowToTs = Date.UTC(2026, 2, 23, 0, 0, 0); // Monday after Sunday rollover
  const windowFromTs = windowToTs - 12 * weekMs;
  const tasks = Array.from({ length: 12 }, (_, idx) => {
    const fromTs = windowFromTs + idx * weekMs;
    return {
      deploymentId: "dep_1",
      symbol: "BTCUSDT",
      strategyId: "regime_pullback_m15_m3",
      tuneId: "base",
      status: "completed",
      result: { netR: 1 },
      windowFromTs: fromTs,
      windowToTs: fromTs + weekMs,
    };
  });

  const out = evaluateFreshCompletedDeploymentWeeks({
    tasks,
    nowMs: Date.UTC(2026, 2, 22, 12, 0, 0), // Sunday
  });
  assert.equal(out.ready, true);
  assert.equal(out.completedWeeks, 12);
  assert.equal(out.windowToTs, windowToTs);
});
