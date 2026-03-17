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

function makeReadyState(params: { venue: "capital" | "bitget"; nowMs: number }) {
  const state = createInitialScalpSessionState({
    venue: params.venue,
    symbol: "BTCUSDT",
    dayKey: "2026-03-17",
    nowMs: params.nowMs,
    killSwitchActive: false,
  });
  state.state = "WAITING_RETRACE";
  state.sweep = {
    side: "BUY_SIDE",
    sweepTsMs: params.nowMs - 60_000,
    sweepPrice: 99,
    bufferAbs: 0,
    rejected: true,
    rejectedTsMs: params.nowMs - 30_000,
    reasonCodes: [],
  };
  state.ifvg = {
    direction: "BULLISH",
    low: 99.8,
    high: 100.2,
    createdTsMs: params.nowMs - 60_000,
    expiresAtMs: params.nowMs + 60 * 60_000,
    entryMode: "midline_touch",
    touched: true,
  };
  return state;
}

test("entry sizing is venue-aware (capital caps tighter than bitget for crypto)", () => {
  const nowMs = Date.UTC(2026, 2, 17, 10, 0, 0, 0);
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    risk: {
      referenceEquityUsd: 100,
      riskPerTradePct: 20,
      minNotionalUsd: 1,
      maxNotionalUsd: 100_000,
      stopBufferPips: 0,
      stopBufferSpreadMult: 0,
      minStopDistancePips: 0.01,
    },
  });
  const market = makeMarket(nowMs);

  const capitalPlan = buildScalpEntryPlan({
    state: makeReadyState({ venue: "capital", nowMs }),
    market,
    cfg,
    entryIntent: { model: "ifvg_touch" },
  });
  const bitgetPlan = buildScalpEntryPlan({
    state: makeReadyState({ venue: "bitget", nowMs }),
    market,
    cfg,
    entryIntent: { model: "ifvg_touch" },
  });

  assert.ok(capitalPlan.plan);
  assert.ok(bitgetPlan.plan);
  assert.ok(capitalPlan.reasonCodes.includes("ENTRY_PLAN_ASSET_LEVERAGE_CAP_ACTIVE"));
  assert.ok(bitgetPlan.reasonCodes.includes("ENTRY_PLAN_FEE_AWARE_RISK_SIZING"));
  assert.ok((bitgetPlan.plan?.notionalUsd ?? 0) > (capitalPlan.plan?.notionalUsd ?? 0));
});
