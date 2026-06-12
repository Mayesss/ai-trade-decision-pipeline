import assert from "node:assert/strict";
import test from "node:test";

import {
  applyScalpStrategyConfigOverride,
  getScalpStrategyConfig,
} from "../config";
import { buildScalpEntryPlan } from "../execution";
import { createInitialScalpSessionState } from "../stateMachine";
import type { ScalpMarketSnapshot } from "../types";

function makeMarket(nowMs: number): ScalpMarketSnapshot {
  return {
    symbol: "BTCUSDT",
    epic: "BTCUSDT",
    nowMs,
    quote: {
      price: 100,
      bid: 99.99,
      offer: 100.01,
      spreadAbs: 0.02,
      spreadPips: 2,
      tsMs: nowMs,
    },
    baseTf: "M15",
    confirmTf: "M3",
    baseCandles: [],
    confirmCandles: [],
    symbolMeta: {
      version: 1,
      symbol: "BTCUSDT",
      epic: "BTCUSDT",
      source: "heuristic",
      assetCategory: "crypto",
      instrumentType: null,
      marketStatus: "TRADEABLE",
      pipSize: 0.01,
      pipPosition: 2,
      tickSize: 0.01,
      decimalPlacesFactor: 2,
      scalingFactor: null,
      minDealSize: null,
      sizeDecimals: null,
      maxLeverage: 50,
      openingHours: {
        zone: "UTC",
        alwaysOpen: true,
        windows: [
          { day: "mon", openTime: "00:00", closeTime: "23:59" },
          { day: "tue", openTime: "00:00", closeTime: "23:59" },
          { day: "wed", openTime: "00:00", closeTime: "23:59" },
          { day: "thu", openTime: "00:00", closeTime: "23:59" },
          { day: "fri", openTime: "00:00", closeTime: "23:59" },
          { day: "sat", openTime: "00:00", closeTime: "23:59" },
          { day: "sun", openTime: "00:00", closeTime: "23:59" },
        ],
      },
      fetchedAtMs: nowMs,
    },
  };
}

function withBitgetSizingEnv<T>(env: Record<string, string>, fn: () => T): T {
  const previous = new Map<string, string | undefined>();
  for (const key of Object.keys(env)) {
    previous.set(key, process.env[key]);
    process.env[key] = env[key];
  }
  try {
    return fn();
  } finally {
    for (const [key, value] of previous.entries()) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

function assertAlmostEqual(actual: number | undefined, expected: number, tolerance = 1e-9) {
  if (typeof actual !== "number") {
    assert.fail(`expected a number but received ${typeof actual}`);
  }
  assert.ok(
    Math.abs(actual - expected) <= tolerance,
    `${actual} not within ${tolerance} of ${expected}`,
  );
}

function makeReadyState(params: {
  venue: "bitget";
  nowMs: number;
  direction?: "BULLISH" | "BEARISH";
  sweepPrice?: number;
}) {
  const state = createInitialScalpSessionState({
    venue: params.venue,
    symbol: "BTCUSDT",
    dayKey: "2026-03-17",
    nowMs: params.nowMs,
    killSwitchActive: false,
  });
  state.state = "WAITING_RETRACE";
  state.sweep = {
    side: params.direction === "BEARISH" ? "SELL_SIDE" : "BUY_SIDE",
    sweepTsMs: params.nowMs - 60_000,
    sweepPrice: params.sweepPrice ?? 99,
    bufferAbs: 0,
    rejected: true,
    rejectedTsMs: params.nowMs - 30_000,
    reasonCodes: [],
  };
  state.ifvg = {
    direction: params.direction ?? "BULLISH",
    low: 99.8,
    high: 100.2,
    createdTsMs: params.nowMs - 60_000,
    expiresAtMs: params.nowMs + 60 * 60_000,
    entryMode: "midline_touch",
    touched: true,
  };
  return state;
}

test("Bitget margin-aware sizing reaches target risk under 20x and 5 percent margin", () => {
  const nowMs = Date.UTC(2026, 2, 17, 10, 0, 0, 0);
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    risk: {
      referenceEquityUsd: 10_000,
      riskPerTradePct: 0.35,
      minNotionalUsd: 1,
      maxNotionalUsd: 100_000,
      stopBufferPips: 0,
      stopBufferSpreadMult: 0,
      minStopDistancePips: 0.01,
    },
  });
  const market = makeMarket(nowMs);

  const bitgetPlan = withBitgetSizingEnv(
    {
      SCALP_BITGET_POLICY_MAX_LEVERAGE: "20",
      SCALP_BITGET_MARGIN_PER_TRADE_PCT: "5",
      SCALP_BITGET_MIN_RISK_TARGET_FILL_PCT: "25",
    },
    () =>
      buildScalpEntryPlan({
        state: makeReadyState({ venue: "bitget", nowMs }),
        market,
        cfg,
        entryIntent: { model: "ifvg_touch" },
      }),
  );

  assert.ok(bitgetPlan.plan);
  assertAlmostEqual(bitgetPlan.plan?.targetRiskUsd, 35);
  assertAlmostEqual(bitgetPlan.plan?.actualRiskUsd, 35);
  assertAlmostEqual(bitgetPlan.plan?.riskUsd, 35);
  assert.equal(bitgetPlan.plan?.marginBudgetUsd, 500);
  assert.equal(bitgetPlan.plan?.leverage, 7);
  assertAlmostEqual(bitgetPlan.plan?.riskTargetFillPct, 100);
  assert.ok(bitgetPlan.reasonCodes.includes("BITGET_MARGIN_AWARE_SIZING"));
  assert.ok(bitgetPlan.reasonCodes.includes("ENTRY_PLAN_FEE_AWARE_RISK_SIZING"));
});

test("Bitget margin-aware sizing downsizes without moving stop or take profit", () => {
  const nowMs = Date.UTC(2026, 2, 17, 10, 0, 0, 0);
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    risk: {
      referenceEquityUsd: 10_000,
      riskPerTradePct: 0.35,
      minNotionalUsd: 1,
      maxNotionalUsd: 100_000,
      stopBufferPips: 0,
      stopBufferSpreadMult: 0,
      minStopDistancePips: 0.01,
    },
  });
  const market = makeMarket(nowMs);

  const bitgetPlan = withBitgetSizingEnv(
    {
      SCALP_BITGET_POLICY_MAX_LEVERAGE: "20",
      SCALP_BITGET_MARGIN_PER_TRADE_PCT: "5",
      SCALP_BITGET_MIN_RISK_TARGET_FILL_PCT: "25",
    },
    () =>
      buildScalpEntryPlan({
        state: makeReadyState({ venue: "bitget", nowMs, sweepPrice: 99.99 }),
        market,
        cfg,
        entryIntent: { model: "ifvg_touch" },
      }),
  );

  assert.ok(bitgetPlan.plan);
  assert.equal(bitgetPlan.plan?.entryReferencePrice, 100);
  assert.equal(bitgetPlan.plan?.stopPrice, 99.99);
  assertAlmostEqual(bitgetPlan.plan?.takeProfitPrice, 100.02);
  assert.equal(bitgetPlan.plan?.notionalUsd, 10_000);
  assert.equal(bitgetPlan.plan?.leverage, 20);
  assert.ok((bitgetPlan.plan?.actualRiskUsd ?? 0) < 35);
  assert.ok((bitgetPlan.plan?.riskTargetFillPct ?? 0) > 25);
  assert.ok(bitgetPlan.reasonCodes.includes("ENTRY_PLAN_RISK_TARGET_DOWNSIZED"));
});

test("Bitget margin-aware sizing skips when downsized risk is too small", () => {
  const nowMs = Date.UTC(2026, 2, 17, 10, 0, 0, 0);
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    risk: {
      referenceEquityUsd: 10_000,
      riskPerTradePct: 0.35,
      minNotionalUsd: 1,
      maxNotionalUsd: 100_000,
      stopBufferPips: 0,
      stopBufferSpreadMult: 0,
      minStopDistancePips: 0.01,
    },
  });
  const market = makeMarket(nowMs);

  const bitgetPlan = withBitgetSizingEnv(
    {
      SCALP_BITGET_POLICY_MAX_LEVERAGE: "20",
      SCALP_BITGET_MARGIN_PER_TRADE_PCT: "0.1",
      SCALP_BITGET_MIN_RISK_TARGET_FILL_PCT: "25",
    },
    () =>
      buildScalpEntryPlan({
        state: makeReadyState({ venue: "bitget", nowMs, sweepPrice: 99.99 }),
        market,
        cfg,
        entryIntent: { model: "ifvg_touch" },
      }),
  );

  assert.equal(bitgetPlan.plan, null);
  assert.ok(bitgetPlan.reasonCodes.includes("BITGET_MARGIN_AWARE_SIZING"));
  assert.ok(bitgetPlan.reasonCodes.includes("ENTRY_PLAN_RISK_TARGET_TOO_SMALL"));
});

test("entry plan rejects BUY stop that is not protective", () => {
  const nowMs = Date.UTC(2026, 2, 17, 10, 0, 0, 0);
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    risk: {
      stopBufferPips: 0,
      stopBufferSpreadMult: 0,
      minStopDistancePips: 0.01,
    },
  });
  const market = makeMarket(nowMs);
  const plan = buildScalpEntryPlan({
    state: makeReadyState({
      venue: "bitget",
      nowMs,
      direction: "BULLISH",
      sweepPrice: 101,
    }),
    market,
    cfg,
    entryIntent: { model: "ifvg_touch" },
  });
  assert.equal(plan.plan, null);
  assert.ok(plan.reasonCodes.includes("ENTRY_PLAN_STOP_NOT_PROTECTIVE"));
});

test("entry plan rejects expired IFVG setup", () => {
  const nowMs = Date.UTC(2026, 2, 17, 10, 0, 0, 0);
  const cfg = getScalpStrategyConfig();
  const market = makeMarket(nowMs);
  const state = makeReadyState({ venue: "bitget", nowMs });
  state.ifvg!.expiresAtMs = nowMs;

  const plan = buildScalpEntryPlan({
    state,
    market,
    cfg,
    entryIntent: { model: "ifvg_touch" },
  });

  assert.equal(plan.plan, null);
  assert.ok(plan.reasonCodes.includes("ENTRY_PLAN_IFVG_EXPIRED"));
});

test("entry plan rejects SELL stop that is not protective", () => {
  const nowMs = Date.UTC(2026, 2, 17, 10, 0, 0, 0);
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    risk: {
      stopBufferPips: 0,
      stopBufferSpreadMult: 0,
      minStopDistancePips: 0.01,
    },
  });
  const market = makeMarket(nowMs);
  const plan = buildScalpEntryPlan({
    state: makeReadyState({
      venue: "bitget",
      nowMs,
      direction: "BEARISH",
      sweepPrice: 99,
    }),
    market,
    cfg,
    entryIntent: { model: "ifvg_touch" },
  });
  assert.equal(plan.plan, null);
  assert.ok(plan.reasonCodes.includes("ENTRY_PLAN_STOP_NOT_PROTECTIVE"));
});
