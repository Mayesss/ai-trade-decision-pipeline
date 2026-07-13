import assert from "node:assert/strict";
import test from "node:test";

import {
  evaluateScalpReplayMarketGate,
  evaluateScalpReplayWeekendGate,
  resolveOpeningHoursState,
} from "./marketHours";
import { buildScalpOpeningHoursSchedule } from "./symbolMarketMetadata";
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

// TLT-like Capital schedule: overnight sessions crossing midnight plus a
// morning re-open. Raw windows are split at midnight during normalization;
// resolveOpeningHoursState must merge the contiguous pieces back into one
// continuous session so closesAtMs is the real session end.
const tltSchedule = buildScalpOpeningHoursSchedule({
  zone: "UTC",
  days: {
    mon: ["10:00 - 00:00"],
    tue: ["00:00 - 02:00", "10:00 - 00:00"],
    wed: ["00:00 - 02:00", "10:00 - 00:00"],
    thu: ["00:00 - 02:00", "10:00 - 00:00"],
    fri: ["00:00 - 02:00", "10:00 - 23:00"],
  },
});

// 2026-07-13 is a Monday.
test("opening-hours state: mid-session close is the merged session end past midnight", () => {
  const state = resolveOpeningHoursState(
    tltSchedule,
    ts("2026-07-13T20:00:00.000Z"),
  );
  assert.equal(state.isOpen, true);
  // Mon 10:00 → Tue 02:00 is one continuous session. Close carries the
  // schedule's inclusive-minute grain, so allow Tue 02:00–02:01.
  const closeMin = ts("2026-07-14T02:00:00.000Z");
  const closeMax = ts("2026-07-14T02:01:00.000Z");
  assert.ok(
    (state.closesAtMs as number) >= closeMin &&
      (state.closesAtMs as number) <= closeMax,
    `close ${new Date(state.closesAtMs as number).toISOString()} outside Tue 02:00-02:01`,
  );
  assert.equal(state.nextOpenAtMs, ts("2026-07-14T10:00:00.000Z"));
});

test("opening-hours state: closed between sessions reports the next open", () => {
  const state = resolveOpeningHoursState(
    tltSchedule,
    ts("2026-07-14T05:00:00.000Z"),
  );
  assert.equal(state.isOpen, false);
  assert.equal(state.nextOpenAtMs, ts("2026-07-14T10:00:00.000Z"));
});

test("opening-hours state: non-UTC zone and missing schedule are unknown, never a guess", () => {
  // Built literally: buildScalpOpeningHoursSchedule normalizes every zone to
  // "UTC" (Capital reports UTC schedules), so a non-UTC zone can only reach
  // the resolver from a schedule constructed elsewhere.
  const londonSchedule = {
    zone: "Europe/London",
    alwaysOpen: false,
    windows: [{ day: "mon" as const, openTime: "10:00", closeTime: "22:00" }],
  };
  const unknown = { isOpen: null, closesAtMs: null, nextOpenAtMs: null };
  assert.deepEqual(
    resolveOpeningHoursState(londonSchedule, ts("2026-07-13T20:00:00.000Z")),
    unknown,
  );
  assert.deepEqual(
    resolveOpeningHoursState(null, ts("2026-07-13T20:00:00.000Z")),
    unknown,
  );
});

test("opening-hours state: alwaysOpen is open with no boundaries", () => {
  const schedule = buildScalpOpeningHoursSchedule({
    zone: "UTC",
    alwaysOpen: true,
    days: { mon: ["00:00 - 23:59"] },
  });
  assert.deepEqual(
    resolveOpeningHoursState(schedule, ts("2026-07-13T20:00:00.000Z")),
    { isOpen: true, closesAtMs: null, nextOpenAtMs: null },
  );
});
