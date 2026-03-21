import assert from "node:assert/strict";
import test from "node:test";

import {
  evaluateScalpReplayMarketGate,
  evaluateScalpReplayWeekendGate,
} from "./marketHours";
import type { ScalpSymbolMarketMetadata } from "./symbolMarketMetadata";

function ts(isoUtc: string): number {
  return Date.parse(isoUtc);
}

function sessionMetadata(): ScalpSymbolMarketMetadata {
  return {
    version: 1,
    symbol: "XAUUSDT",
    epic: "CS.D.XAUUSD.TODAY.IP",
    source: "bitget",
    assetCategory: "commodity",
    instrumentType: "COMMODITIES",
    marketStatus: "TRADEABLE",
    pipSize: 0.01,
    pipPosition: 2,
    tickSize: 0.01,
    decimalPlacesFactor: 100,
    scalingFactor: 1,
    minDealSize: 0.1,
    sizeDecimals: 1,
    openingHours: {
      zone: "UTC",
      alwaysOpen: false,
      windows: [
        { day: "mon", openTime: "00:00", closeTime: "21:59" },
        { day: "tue", openTime: "00:00", closeTime: "21:59" },
        { day: "wed", openTime: "00:00", closeTime: "21:59" },
        { day: "thu", openTime: "00:00", closeTime: "21:59" },
        { day: "fri", openTime: "00:00", closeTime: "21:59" },
      ],
    },
    fetchedAtMs: ts("2026-03-14T00:00:00.000Z"),
  };
}

test("session gate uses broker opening hours for pre-close entry block", () => {
  const out = evaluateScalpReplayMarketGate({
    symbol: "XAUUSDT",
    nowMs: ts("2026-02-20T21:10:00.000Z"),
    metadata: sessionMetadata(),
  });
  assert.equal(out.marketClosed, false);
  assert.equal(out.entryBlocked, true);
  assert.equal(out.forceCloseNow, false);
  assert.equal(out.reasonCode, "SESSION_ENTRY_BLOCK");
});

test("session gate uses broker opening hours for force close", () => {
  const out = evaluateScalpReplayMarketGate({
    symbol: "XAUUSDT",
    nowMs: ts("2026-02-20T21:50:00.000Z"),
    metadata: sessionMetadata(),
  });
  assert.equal(out.marketClosed, false);
  assert.equal(out.entryBlocked, true);
  assert.equal(out.forceCloseNow, true);
  assert.equal(out.reasonCode, "SESSION_FORCE_CLOSE");
});

test("session gate keeps market closed outside broker opening hours", () => {
  const out = evaluateScalpReplayMarketGate({
    symbol: "XAUUSDT",
    nowMs: ts("2026-02-21T12:00:00.000Z"),
    metadata: sessionMetadata(),
  });
  assert.equal(out.marketClosed, true);
  assert.equal(out.entryBlocked, true);
  assert.equal(out.forceCloseNow, false);
  assert.equal(out.reasonCode, "MARKET_CLOSED_SESSION");
  assert.ok(Number.isFinite(out.reopensAtMs as number));
});

test("fallback weekend gate is disabled for crypto without session metadata", () => {
  const out = evaluateScalpReplayWeekendGate(
    "BTCUSDT",
    ts("2026-02-20T21:50:00.000Z"),
  );
  assert.equal(out.marketClosed, false);
  assert.equal(out.entryBlocked, false);
  assert.equal(out.forceCloseNow, false);
  assert.equal(out.reasonCode, "WEEKEND_POLICY_DISABLED");
});
