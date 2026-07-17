import assert from "node:assert/strict";
import test from "node:test";

import { buildForexEventContext } from "./forexEvents";
import type { ForexEconomicEvent, ForexEventsState } from "./forexEvents";

const NOW_MS = Date.UTC(2026, 6, 14, 14, 0); // 2026-07-14T14:00Z

function event(minutesFromNow: number, overrides: Partial<ForexEconomicEvent> = {}): ForexEconomicEvent {
  const ts = new Date(NOW_MS + minutesFromNow * 60_000).toISOString();
  return {
    id: `ev${minutesFromNow}`,
    timestamp_utc: ts,
    currency: "USD",
    impact: "HIGH",
    event_name: "Core CPI m/m",
    source: "forexfactory",
    ...overrides,
  };
}

function state(events: ForexEconomicEvent[]): ForexEventsState {
  return {
    snapshot: {
      source: "forexfactory",
      fetchedAtMs: NOW_MS - 5 * 60_000,
      fromDate: "2026-07-13",
      toDate: "2026-07-21",
      events,
    },
    meta: {
      lastFetchAttemptAtMs: NOW_MS - 5 * 60_000,
      lastSuccessAtMs: NOW_MS - 5 * 60_000,
      lastFailureAtMs: null,
      lastError: null,
    },
    stale: false,
    staleMinutes: 45,
    refreshMinutes: 15,
  };
}

function build(events: ForexEconomicEvent[]) {
  // XAUUSD resolves to the USD macro calendar via the commodity category.
  return buildForexEventContext({ symbol: "XAUUSD", category: "commodity", state: state(events), nowMs: NOW_MS });
}

test("a released HIGH event past its blackout lands in recentEvents, status stays clear", () => {
  const ctx = build([event(-90)]);
  assert.equal(ctx.status, "clear");
  assert.equal(ctx.activeEvents.length, 0);
  assert.equal(ctx.recentEvents.length, 1);
  assert.equal(ctx.recentEvents[0].minutesToEvent, -90);
  assert.equal(ctx.recentEvents[0].activeWindow, false);
});

test("an event inside the post-release blackout stays active-only — never doubles into recentEvents", () => {
  // Default post-block = 15min; released 10min ago is still in the window.
  const ctx = build([event(-10)]);
  assert.equal(ctx.status, "active");
  assert.equal(ctx.activeEvents.length, 1);
  assert.equal(ctx.recentEvents.length, 0);
});

test("recentEvents never flips status to active (blackout gate invariant)", () => {
  const ctx = build([event(-90), event(-120)]);
  assert.equal(ctx.status, "clear");
  assert.equal(ctx.recentEvents.length, 2);
});

test("MEDIUM releases are excluded by the default recent-impacts filter but still blackout while active", () => {
  const released = build([event(-90, { impact: "MEDIUM", id: "med1" })]);
  assert.equal(released.recentEvents.length, 0);
  const active = build([event(-10, { impact: "MEDIUM", id: "med2" })]);
  assert.equal(active.status, "active"); // blockImpacts default HIGH+MEDIUM unchanged
});

test("releases older than the lookback are dropped; upcoming events stay in nextEvents only", () => {
  const ctx = build([event(-240), event(120)]);
  assert.equal(ctx.recentEvents.length, 0);
  assert.equal(ctx.nextEvents.length, 1);
  assert.equal(ctx.nextEvents[0].minutesToEvent, 120);
});
