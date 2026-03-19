import assert from "node:assert/strict";
import test from "node:test";

import {
  applyPromotionHysteresis,
  buildDiscoverSymbolSyncPlan,
  selectPromotionWinnerRowsWithExploration,
  type PromotionSelectionRow,
} from "../pipelineJobs";
import type { ScalpPromotionForwardValidationCandidate } from "../promotionPolicy";

function makeCandidate(params: {
  deploymentId: string;
  symbol: string;
  strategyId?: string;
  tuneId?: string;
  selectionScore: number;
}): ScalpPromotionForwardValidationCandidate {
  const strategyId = params.strategyId || "regime_pullback_m15_m3";
  const tuneId = params.tuneId || "base";
  return {
    deploymentId: params.deploymentId,
    symbol: params.symbol,
    strategyId,
    tuneId,
    rollCount: 14,
    profitableWindowPct: 62,
    profitableWindows: 9,
    meanExpectancyR: 0.11,
    trimmedMeanExpectancyR: 0.1,
    medianExpectancyR: 0.1,
    meanProfitFactor: 1.25,
    maxDrawdownR: 1.2,
    topWindowPnlConcentrationPct: 42,
    selectionScore: params.selectionScore,
    minTradesPerWindow: 2,
    totalTrades: 38,
    selectionWindowDays: 91,
    forwardWindowDays: 7,
    forwardValidation: {
      rollCount: 14,
      profitableWindowPct: 62,
      meanExpectancyR: 0.11,
      meanProfitFactor: 1.25,
      maxDrawdownR: 1.2,
      minTradesPerWindow: 2,
      selectionWindowDays: 91,
      forwardWindowDays: 7,
    },
  };
}

test("buildDiscoverSymbolSyncPlan computes active/catalog adds and removals", () => {
  const plan = buildDiscoverSymbolSyncPlan({
    existingRows: [
      { symbol: "btcusdt", active: true },
      { symbol: "ethusdt", active: true },
      { symbol: "xrpusdt", active: false },
    ],
    activeSymbols: ["ETHUSDT", "SOLUSDT"],
    catalogSymbols: ["ETHUSDT", "SOLUSDT", "ADAUSDT"],
  });

  assert.deepEqual(plan.addedActiveSymbols, ["SOLUSDT"]);
  assert.deepEqual(plan.removedActiveSymbols, ["BTCUSDT"]);
  assert.deepEqual(plan.catalogAddedSymbols, ["SOLUSDT", "ADAUSDT"]);
});

test("applyPromotionHysteresis requires 2 fails to disable and 2 passes to re-enable", () => {
  const nowMs = Date.UTC(2026, 2, 19, 20, 0, 0);

  const firstFail = applyPromotionHysteresis({
    currentlyEnabled: true,
    shouldEnableNow: false,
    previous: null,
    passThreshold: 2,
    failThreshold: 2,
    nowMs,
  });
  assert.equal(firstFail.enabled, true);
  assert.equal(firstFail.transition, "held");
  assert.equal(firstFail.hysteresis.failStreak, 1);

  const secondFail = applyPromotionHysteresis({
    currentlyEnabled: firstFail.enabled,
    shouldEnableNow: false,
    previous: firstFail.hysteresis,
    passThreshold: 2,
    failThreshold: 2,
    nowMs: nowMs + 60_000,
  });
  assert.equal(secondFail.enabled, false);
  assert.equal(secondFail.transition, "disabled");
  assert.equal(secondFail.hysteresis.failStreak, 2);

  const firstPass = applyPromotionHysteresis({
    currentlyEnabled: secondFail.enabled,
    shouldEnableNow: true,
    previous: secondFail.hysteresis,
    passThreshold: 2,
    failThreshold: 2,
    nowMs: nowMs + 120_000,
  });
  assert.equal(firstPass.enabled, false);
  assert.equal(firstPass.transition, "held");
  assert.equal(firstPass.hysteresis.passStreak, 1);

  const secondPass = applyPromotionHysteresis({
    currentlyEnabled: firstPass.enabled,
    shouldEnableNow: true,
    previous: firstPass.hysteresis,
    passThreshold: 2,
    failThreshold: 2,
    nowMs: nowMs + 180_000,
  });
  assert.equal(secondPass.enabled, true);
  assert.equal(secondPass.transition, "enabled");
  assert.equal(secondPass.hysteresis.passStreak, 2);
});

test("applyPromotionHysteresis keeps currently enabled deployment on when lockEnabled is true", () => {
  const nowMs = Date.UTC(2026, 2, 19, 21, 0, 0);
  const out = applyPromotionHysteresis({
    currentlyEnabled: true,
    shouldEnableNow: false,
    previous: {
      passStreak: 0,
      failStreak: 1,
      lastStateChangeAtMs: nowMs - 60_000,
      lastDecision: "hold",
    },
    passThreshold: 2,
    failThreshold: 2,
    nowMs,
    lockEnabled: true,
  });
  assert.equal(out.enabled, true);
  assert.equal(out.transition, "held");
  assert.equal(out.hysteresis.failStreak, 2);
});

test("selectPromotionWinnerRowsWithExploration enforces 40% exploration split when possible", () => {
  const rows: PromotionSelectionRow[] = [
    {
      deploymentId: "inc_1",
      symbol: "BTCUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "inc_1",
        symbol: "BTCUSDT",
        selectionScore: 10,
      }),
    },
    {
      deploymentId: "inc_2",
      symbol: "ETHUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "inc_2",
        symbol: "ETHUSDT",
        selectionScore: 9,
      }),
    },
    {
      deploymentId: "inc_3",
      symbol: "XAUUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "inc_3",
        symbol: "XAUUSDT",
        selectionScore: 8,
      }),
    },
    {
      deploymentId: "chal_1",
      symbol: "SOLUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "chal_1",
        symbol: "SOLUSDT",
        selectionScore: 7,
      }),
    },
    {
      deploymentId: "chal_2",
      symbol: "ADAUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "chal_2",
        symbol: "ADAUSDT",
        selectionScore: 6,
      }),
    },
    {
      deploymentId: "chal_3",
      symbol: "UNIUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "chal_3",
        symbol: "UNIUSDT",
        selectionScore: 5,
      }),
    },
  ];

  const out = selectPromotionWinnerRowsWithExploration({
    rows,
    explorationShare: 0.4,
    maxSymbols: 10,
    maxPerSymbol: 2,
    maxDeployments: 5,
  });

  assert.equal(out.exploitSlots, 3);
  assert.equal(out.explorationSlots, 2);
  assert.equal(out.exploitSelected, 3);
  assert.equal(out.explorationSelected, 2);
  assert.equal(out.selectedRows.length, 5);
  assert.equal(out.winnerIds.has("inc_1"), true);
  assert.equal(out.winnerIds.has("inc_2"), true);
  assert.equal(out.winnerIds.has("inc_3"), true);
  assert.equal(out.winnerIds.has("chal_1"), true);
  assert.equal(out.winnerIds.has("chal_2"), true);
});

test("selectPromotionWinnerRowsWithExploration respects per-symbol cap", () => {
  const rows: PromotionSelectionRow[] = [
    {
      deploymentId: "btc_inc",
      symbol: "BTCUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "btc_inc",
        symbol: "BTCUSDT",
        selectionScore: 10,
      }),
    },
    {
      deploymentId: "btc_chal",
      symbol: "BTCUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "btc_chal",
        symbol: "BTCUSDT",
        selectionScore: 9,
      }),
    },
    {
      deploymentId: "eth_inc",
      symbol: "ETHUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "eth_inc",
        symbol: "ETHUSDT",
        selectionScore: 8,
      }),
    },
  ];

  const out = selectPromotionWinnerRowsWithExploration({
    rows,
    explorationShare: 0.4,
    maxSymbols: 5,
    maxPerSymbol: 1,
    maxDeployments: 3,
  });

  const btcRows = out.selectedRows.filter((row) => row.symbol === "BTCUSDT");
  assert.equal(btcRows.length, 1);
});

test("selectPromotionWinnerRowsWithExploration keeps one row per incumbent symbol when capacity allows", () => {
  const rows: PromotionSelectionRow[] = [
    {
      deploymentId: "btc_inc_low",
      symbol: "BTCUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "btc_inc_low",
        symbol: "BTCUSDT",
        selectionScore: 2,
      }),
    },
    {
      deploymentId: "eth_inc_low",
      symbol: "ETHUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "eth_inc_low",
        symbol: "ETHUSDT",
        selectionScore: 1.5,
      }),
    },
    {
      deploymentId: "sol_chal_high",
      symbol: "SOLUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "sol_chal_high",
        symbol: "SOLUSDT",
        selectionScore: 10,
      }),
    },
    {
      deploymentId: "ada_chal_high",
      symbol: "ADAUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "ada_chal_high",
        symbol: "ADAUSDT",
        selectionScore: 9,
      }),
    },
  ];

  const out = selectPromotionWinnerRowsWithExploration({
    rows,
    explorationShare: 0.4,
    maxSymbols: 10,
    maxPerSymbol: 2,
    maxDeployments: 3,
  });

  const selectedSymbols = new Set(out.selectedRows.map((row) => row.symbol));
  assert.equal(selectedSymbols.has("BTCUSDT"), true);
  assert.equal(selectedSymbols.has("ETHUSDT"), true);
});
