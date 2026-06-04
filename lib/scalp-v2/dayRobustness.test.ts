import assert from "node:assert/strict";
import test from "node:test";

import {
  DAY_ROBUSTNESS_VERSION,
  buildDayRobustnessEvidence,
  evaluateDayRobustnessForPromotion,
  type DayRobustnessPolicy,
} from "./dayRobustness";
import { DAY_MODEL_GUIDED_COMPOSER_V1_STRATEGY_ID } from "./dayComposer";
import { SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID } from "./sessionStructureComposer";
import type { ScalpReplayTrade } from "../scalp/replay/types";

const policy: DayRobustnessPolicy = {
  enabled: true,
  weeks: 26,
  extendedWeeks: 52,
  windowWeeks: 13,
  maxCandidates: 100,
  leaseMs: 60_000,
  maxAgeMs: 14 * 24 * 60 * 60_000,
  minTotalTrades: 2,
  minTotalNetR: 1,
  minProfitFactor: 1.05,
  minPositiveWindowPct: 50,
  minWorstWindowNetR: -2,
  maxDrawdownR: 10,
  maxStageCDrawdownMultiple: 2,
};

function trade(exitTs: number, rMultiple: number): ScalpReplayTrade {
  return {
    id: `${exitTs}:${rMultiple}`,
    dayKey: "2026-01-01",
    side: "BUY",
    entryTs: exitTs - 60_000,
    exitTs,
    holdMinutes: 1,
    entryPrice: 100,
    stopPrice: 99,
    takeProfitPrice: 102,
    exitPrice: 101,
    exitReason: rMultiple >= 0 ? "TP" : "STOP",
    riskAbs: 1,
    riskUsd: 1,
    notionalUsd: 100,
    rMultiple,
    pnlUsd: rMultiple,
  };
}

test("finalist robustness promotion gate requires evidence for session composer only", () => {
  const missing = evaluateDayRobustnessForPromotion({
    strategyId: SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
    metadata: {},
    policy,
    nowMs: 1000,
  });
  assert.equal(missing.required, true);
  assert.equal(missing.passed, false);
  assert.equal(missing.reason, "DAY_ROBUSTNESS_MISSING");

  const retiredDay = evaluateDayRobustnessForPromotion({
    strategyId: DAY_MODEL_GUIDED_COMPOSER_V1_STRATEGY_ID,
    metadata: {},
    policy,
    nowMs: 1000,
  });
  assert.equal(retiredDay.required, false);
  assert.equal(retiredDay.passed, true);
});

test("day robustness evidence passes and round-trips through promotion check", () => {
  const fromTs = 0;
  const week = 7 * 24 * 60 * 60_000;
  const toTs = 26 * week;
  const trades = [
    trade(2 * week, 1.2),
    trade(8 * week, -0.2),
    trade(15 * week, 1.3),
    trade(24 * week, 0.4),
  ];
  const evidence = buildDayRobustnessEvidence({
    trades,
    summary: {
      trades: trades.length,
      netR: 2.7,
      expectancyR: 0.675,
      winRatePct: 75,
      profitFactor: 14.5,
      maxDrawdownR: 0.2,
    },
    fromTs,
    toTs,
    windowToTs: toTs,
    weeks: 26,
    policy,
    stageCMaxDrawdownR: 1,
    nowMs: toTs + 1000,
  });
  assert.equal(evidence.version, DAY_ROBUSTNESS_VERSION);
  assert.equal(evidence.passed, true);
  assert.equal(evidence.totalWindows, 2);

  const check = evaluateDayRobustnessForPromotion({
    strategyId: SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
    metadata: { worker: { robustness: evidence } },
    policy,
    nowMs: toTs + 2_000,
    windowToTs: toTs,
  });
  assert.equal(check.passed, true);
  assert.equal(check.reason, "day_robustness_passed");
});

test("day robustness failed or stale evidence blocks promotion", () => {
  const evidence = buildDayRobustnessEvidence({
    trades: [trade(1_000, -1), trade(2_000, -1)],
    summary: {
      trades: 2,
      netR: -2,
      expectancyR: -1,
      winRatePct: 0,
      profitFactor: 0,
      maxDrawdownR: 2,
    },
    fromTs: 0,
    toTs: 26 * 7 * 24 * 60 * 60_000,
    windowToTs: 26 * 7 * 24 * 60 * 60_000,
    weeks: 26,
    policy,
    nowMs: 10_000,
  });
  assert.equal(evidence.passed, false);

  const failed = evaluateDayRobustnessForPromotion({
    strategyId: SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
    metadata: { worker: { robustness: evidence } },
    policy,
    nowMs: 11_000,
    windowToTs: evidence.windowToTs,
  });
  assert.equal(failed.passed, false);
  assert.equal(failed.reason, "DAY_ROBUSTNESS_FAILED");

  const stale = evaluateDayRobustnessForPromotion({
    strategyId: SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
    metadata: { worker: { robustness: { ...evidence, passed: true, reasonCodes: [] } } },
    policy,
    nowMs: 30 * 24 * 60 * 60_000,
    windowToTs: evidence.windowToTs,
  });
  assert.equal(stale.passed, false);
  assert.equal(stale.reason, "DAY_ROBUSTNESS_FAILED");
});
