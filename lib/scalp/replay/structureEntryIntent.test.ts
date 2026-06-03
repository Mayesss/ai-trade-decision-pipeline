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
    symbol: "EURUSD",
    epic: "EURUSD",
    nowMs,
    quote: {
      price: 1.1,
      bid: 1.0999,
      offer: 1.1001,
      spreadAbs: 0.0002,
      spreadPips: 2,
      tsMs: nowMs,
    },
    baseTf: "M15",
    confirmTf: "M3",
    baseCandles: [],
    confirmCandles: [],
    symbolMeta: {
      version: 1,
      symbol: "EURUSD",
      epic: "EURUSD",
      source: "heuristic",
      assetCategory: "forex",
      instrumentType: null,
      marketStatus: "TRADEABLE",
      pipSize: 0.0001,
      pipPosition: 4,
      tickSize: 0.0001,
      decimalPlacesFactor: 4,
      scalingFactor: null,
      minDealSize: null,
      sizeDecimals: null,
      maxLeverage: 30,
      openingHours: {
        zone: "UTC",
        alwaysOpen: true,
        windows: [],
      },
      fetchedAtMs: nowMs,
    },
  };
}

test("structure_level entry intent builds a valid entry plan", () => {
  const nowMs = Date.UTC(2026, 2, 17, 10, 0, 0, 0);
  const state = createInitialScalpSessionState({
    venue: "capital",
    symbol: "EURUSD",
    dayKey: "2026-03-17",
    nowMs,
    killSwitchActive: false,
  });
  state.strategyId = "day_model_guided_composer_v1";
  state.tuneId = "dtc_h1td1_pdhpdl_sweep_m15acc_fix2r6h_abcdef1234";
  state.deploymentId = "capital:EURUSD~day_model_guided_composer_v1~test__sp_berlin";
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    risk: {
      referenceEquityUsd: 10_000,
      riskPerTradePct: 0.25,
      minNotionalUsd: 1,
      maxNotionalUsd: 100_000,
      minStopDistancePips: 1,
    },
  });
  const res = buildScalpEntryPlan({
    state,
    market: makeMarket(nowMs),
    cfg,
    entryIntent: {
      model: "structure_level",
      side: "BUY",
      entryMode: "market",
      entryReferencePrice: 1.1,
      stopPrice: 1.098,
      takeProfitPrice: 1.104,
      setupKey: "day:test:buy",
      reasonCodes: ["DAY_COMPOSER_STRUCTURE_LEVEL_READY"],
    },
  });
  assert.ok(res.plan);
  assert.equal(res.plan!.side, "BUY");
  assert.equal(res.plan!.orderType, "MARKET");
  assert.equal(res.plan!.stopPrice, 1.098);
  assert.equal(res.plan!.takeProfitPrice, 1.104);
  assert.ok(res.plan!.setupId.startsWith("structure:"));
  assert.ok(res.reasonCodes.includes("ENTRY_INTENT_STRUCTURE_LEVEL"));
  assert.ok(res.reasonCodes.includes("ENTRY_PLAN_READY"));
});

test("structure_level entry intent rejects invalid target geometry", () => {
  const nowMs = Date.UTC(2026, 2, 17, 10, 0, 0, 0);
  const state = createInitialScalpSessionState({
    venue: "capital",
    symbol: "EURUSD",
    dayKey: "2026-03-17",
    nowMs,
    killSwitchActive: false,
  });
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    risk: { minStopDistancePips: 1 },
  });
  const res = buildScalpEntryPlan({
    state,
    market: makeMarket(nowMs),
    cfg,
    entryIntent: {
      model: "structure_level",
      side: "BUY",
      entryMode: "market",
      entryReferencePrice: 1.1,
      stopPrice: 1.098,
      takeProfitPrice: 1.099,
      setupKey: "day:test:bad_target",
    },
  });
  assert.equal(res.plan, null);
  assert.ok(res.reasonCodes.includes("ENTRY_PLAN_TP_NOT_FAVORABLE"));
});
