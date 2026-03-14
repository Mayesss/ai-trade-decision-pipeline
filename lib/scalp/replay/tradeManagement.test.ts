import assert from "node:assert/strict";
import test from "node:test";

import {
  applyScalpStrategyConfigOverride,
  getScalpStrategyConfig,
} from "../config";
import { manageScalpOpenTrade } from "../execution";
import { createInitialScalpSessionState } from "../stateMachine";
import type { ScalpCandle, ScalpMarketSnapshot } from "../types";

function candle(
  ts: number,
  open: number,
  high: number,
  low: number,
  close: number,
): ScalpCandle {
  return [ts, open, high, low, close, 100];
}

function marketSnapshot(params: {
  nowMs: number;
  price: number;
  confirmCandles: ScalpCandle[];
}): ScalpMarketSnapshot {
  return {
    symbol: "EURUSD",
    epic: "REPLAY:EURUSD",
    nowMs: params.nowMs,
    quote: {
      price: params.price,
      bid: params.price - 0.00005,
      offer: params.price + 0.00005,
      spreadAbs: 0.0001,
      spreadPips: 1,
      tsMs: params.nowMs,
    },
    baseTf: "M15",
    confirmTf: "M3",
    baseCandles: params.confirmCandles.slice(-4),
    confirmCandles: params.confirmCandles,
  };
}

test("manageScalpOpenTrade closes position on stop hit and records loss stats", async () => {
  const nowMs = Date.UTC(2026, 0, 5, 10, 0, 0, 0);
  const state = createInitialScalpSessionState({
    symbol: "EURUSD",
    dayKey: "2026-01-05",
    nowMs,
    killSwitchActive: false,
  });
  state.state = "IN_TRADE";
  state.trade = {
    setupId: "test-setup",
    dealReference: "test-ref",
    side: "BUY",
    entryPrice: 1,
    stopPrice: 0.99,
    takeProfitPrice: 1.02,
    riskR: 1,
    riskAbs: 0.01,
    initialStopPrice: 0.99,
    remainingSizePct: 1,
    realizedR: 0,
    tp1Done: false,
    tp1Price: null,
    trailActive: false,
    trailStopPrice: null,
    favorableExtremePrice: 1,
    barsHeld: 0,
    openedAtMs: nowMs - 9 * 60_000,
    brokerOrderId: null,
    dryRun: false,
  };

  const cfg = getScalpStrategyConfig();
  const confirmCandles = [
    candle(nowMs - 9 * 60_000, 1.0, 1.0006, 0.9996, 1.0003),
    candle(nowMs - 6 * 60_000, 1.0003, 1.0007, 0.9998, 1.0002),
    candle(nowMs - 3 * 60_000, 1.0002, 1.0004, 0.989, 0.9892),
  ];
  const managed = await manageScalpOpenTrade({
    state,
    market: marketSnapshot({
      nowMs,
      price: 0.9892,
      confirmCandles,
    }),
    cfg,
    dryRun: true,
    nowMs,
  });

  assert.equal(managed.state.trade, null);
  assert.equal(managed.state.stats.losses, 1);
  assert.equal(managed.state.stats.wins, 0);
  assert.equal(managed.state.stats.consecutiveLosses, 1);
  assert.ok(Math.abs((managed.state.stats.realizedR ?? 0) + 1) < 1e-9);
  assert.equal(managed.state.state, "IDLE");
  assert.ok(managed.reasonCodes.includes("TRADE_EXIT_STOP_HIT"));
  assert.equal(managed.closedTrade?.exitReason, "STOP");
  assert.equal(managed.closedTrade?.exitPrice, 0.99);
  assert.ok(Math.abs((managed.closedTrade?.totalTradeR ?? 0) + 1) < 1e-9);
});

test("manageScalpOpenTrade does not overstate stop loss when market price overshoots stop", async () => {
  const nowMs = Date.UTC(2026, 0, 5, 11, 0, 0, 0);
  const state = createInitialScalpSessionState({
    symbol: "BTCUSDT",
    dayKey: "2026-01-05",
    nowMs,
    killSwitchActive: false,
  });
  state.state = "IN_TRADE";
  state.trade = {
    setupId: "btc-stop-overshoot",
    dealReference: "btc-stop-overshoot",
    side: "BUY",
    entryPrice: 100,
    stopPrice: 99,
    takeProfitPrice: 102,
    riskR: 1,
    riskAbs: 1,
    initialStopPrice: 99,
    remainingSizePct: 1,
    realizedR: 0,
    tp1Done: false,
    tp1Price: null,
    trailActive: false,
    trailStopPrice: null,
    favorableExtremePrice: 100,
    barsHeld: 0,
    openedAtMs: nowMs - 9 * 60_000,
    brokerOrderId: null,
    dryRun: false,
  };

  const managed = await manageScalpOpenTrade({
    state,
    market: marketSnapshot({
      nowMs,
      price: 35,
      confirmCandles: [
        candle(nowMs - 9 * 60_000, 100, 100.3, 99.8, 100.1),
        candle(nowMs - 6 * 60_000, 100.1, 100.2, 99.6, 99.9),
        candle(nowMs - 3 * 60_000, 99.9, 100, 35, 35),
      ],
    }),
    cfg: getScalpStrategyConfig(),
    dryRun: true,
    nowMs,
  });

  assert.equal(managed.state.trade, null);
  assert.equal(managed.state.stats.realizedR, -1);
  assert.equal(managed.closedTrade?.exitReason, "STOP");
  assert.equal(managed.closedTrade?.exitPrice, 99);
  assert.equal(managed.closedTrade?.totalTradeR, -1);
});

test("manageScalpOpenTrade executes TP1 partial and keeps runner open", async () => {
  const nowMs = Date.UTC(2026, 0, 5, 10, 30, 0, 0);
  const state = createInitialScalpSessionState({
    symbol: "EURUSD",
    dayKey: "2026-01-05",
    nowMs,
    killSwitchActive: false,
  });
  state.state = "IN_TRADE";
  state.trade = {
    setupId: "test-setup-tp1",
    dealReference: "test-ref-tp1",
    side: "BUY",
    entryPrice: 1,
    stopPrice: 0.99,
    takeProfitPrice: 1.02,
    riskR: 1,
    riskAbs: 0.01,
    initialStopPrice: 0.99,
    remainingSizePct: 1,
    realizedR: 0,
    tp1Done: false,
    tp1Price: null,
    trailActive: false,
    trailStopPrice: null,
    favorableExtremePrice: 1,
    barsHeld: 0,
    openedAtMs: nowMs - 6 * 60_000,
    brokerOrderId: null,
    dryRun: false,
  };

  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    risk: {
      tp1R: 1,
      tp1ClosePct: 50,
      trailStartR: 99,
      timeStopBars: 9999,
      breakEvenOffsetR: 0,
    },
  });

  const confirmCandles = [
    candle(nowMs - 6 * 60_000, 1.0, 1.0005, 0.9998, 1.0003),
    candle(nowMs - 3 * 60_000, 1.0003, 1.0124, 1.0001, 1.012),
  ];

  const managed = await manageScalpOpenTrade({
    state,
    market: marketSnapshot({
      nowMs,
      price: 1.012,
      confirmCandles,
    }),
    cfg,
    dryRun: true,
    nowMs,
  });

  assert.ok(managed.state.trade);
  assert.equal(managed.state.state, "IN_TRADE");
  assert.equal(managed.state.trade!.tp1Done, true);
  assert.ok((managed.state.trade!.remainingSizePct ?? 0) < 1);
  assert.ok((managed.state.trade!.remainingSizePct ?? 0) > 0);
  assert.ok((managed.state.trade!.realizedR ?? 0) > 0);
  assert.ok(managed.reasonCodes.includes("TP1_PARTIAL_EXECUTED"));
});
