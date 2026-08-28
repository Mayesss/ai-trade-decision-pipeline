// Characterization tests for the market-metadata kernel that lib/capital.ts
// depends on (leverage split, market-hours gating).
import test from "node:test";
import assert from "node:assert/strict";

import {
  inferScalpAssetCategory,
  isPreciousMetalFamilySymbol,
  isWeekendClosedScalpSymbol,
} from "./market/symbolInfo";
import {
  buildScalpOpeningHoursSchedule,
  scalpAssetCategoryFromInstrumentType,
} from "./market/symbolMarketMetadata";
import { resolveOpeningHoursState } from "./market/marketHours";

test("precious-metal family split (drives Capital leverage floor)", () => {
  for (const sym of ["GOLD", "SILVER", "XAUUSD", "XAGUSD", "xauusd", "XPTUSD"]) {
    assert.equal(isPreciousMetalFamilySymbol(sym), true, sym);
  }
  // Energy/base commodities must NOT get the metals leverage floor.
  for (const sym of ["COPPER", "OIL_CRUDE", "NATURALGAS", "USOIL", "EURUSD", "US100", ""]) {
    assert.equal(isPreciousMetalFamilySymbol(sym), false, sym);
  }
});

test("asset category inference for the cron universe", () => {
  const expectations: Record<string, string> = {
    EURUSD: "forex",
    USDJPY: "forex",
    GBPUSD: "forex",
    BTCUSDT: "crypto",
    ETHUSDT: "crypto",
    US100: "index",
    DE40: "index",
    UK100: "index",
    HK50: "index",
    GOLD: "commodity",
    SILVER: "commodity",
    COPPER: "commodity",
    OIL_CRUDE: "commodity",
    NATURALGAS: "commodity",
    "": "other",
  };
  for (const [sym, expected] of Object.entries(expectations)) {
    assert.equal(inferScalpAssetCategory(sym), expected, sym || "<empty>");
  }
});

test("weekend-closed classification: only crypto trades weekends", () => {
  assert.equal(isWeekendClosedScalpSymbol("BTCUSDT"), false);
  assert.equal(isWeekendClosedScalpSymbol("GOLD"), true);
  assert.equal(isWeekendClosedScalpSymbol("EURUSD"), true);
});

test("instrument-type mapping wins over symbol heuristics", () => {
  assert.equal(scalpAssetCategoryFromInstrumentType("GOLD", "COMMODITIES"), "commodity");
  assert.equal(scalpAssetCategoryFromInstrumentType("EURUSD", "CURRENCIES"), "forex");
  assert.equal(scalpAssetCategoryFromInstrumentType("ANY", "SHARES"), "equity");
  assert.equal(scalpAssetCategoryFromInstrumentType("ANY", "INDICES"), "index");
  // Unknown instrument type falls back to symbol inference.
  assert.equal(scalpAssetCategoryFromInstrumentType("BTCUSDT", ""), "crypto");
});

test("opening-hours state: no schedule means unknown, never closed", () => {
  const state = resolveOpeningHoursState(null, Date.now());
  assert.deepEqual(state, {
    isOpen: null,
    openedAtMs: null,
    closesAtMs: null,
    nextOpenAtMs: null,
  });
});

test("opening-hours state: alwaysOpen schedule is open", () => {
  const schedule = buildScalpOpeningHoursSchedule({ zone: "UTC", alwaysOpen: true });
  assert.ok(schedule, "alwaysOpen schedule should build");
  const state = resolveOpeningHoursState(schedule, Date.now());
  assert.equal(state.isOpen, true);
});

test("opening-hours state: UTC windowed schedule open/closed transitions", () => {
  const schedule = buildScalpOpeningHoursSchedule({
    zone: "UTC",
    windows: [{ day: "mon", openTime: "08:00", closeTime: "16:00" }],
  });
  assert.ok(schedule, "windowed schedule should build");

  // 2026-08-24 is a Monday.
  const mondayNoon = Date.UTC(2026, 7, 24, 12, 0, 0);
  const open = resolveOpeningHoursState(schedule, mondayNoon);
  assert.equal(open.isOpen, true);
  assert.equal(open.openedAtMs, Date.UTC(2026, 7, 24, 8, 0, 0));
  // The close minute is inclusive: a "16:00" close means the window ends 16:01.
  assert.equal(open.closesAtMs, Date.UTC(2026, 7, 24, 16, 1, 0));

  const mondayEvening = Date.UTC(2026, 7, 24, 18, 0, 0);
  const closed = resolveOpeningHoursState(schedule, mondayEvening);
  assert.equal(closed.isOpen, false);
  assert.equal(closed.openedAtMs, null);
  assert.equal(closed.nextOpenAtMs, Date.UTC(2026, 7, 31, 8, 0, 0));
});
