import assert from "node:assert/strict";
import test from "node:test";

import {
  buildHeuristicScalpSymbolMarketMetadata,
  buildScalpOpeningHoursSchedule,
  normalizeScalpSymbolMarketMetadata,
  scalpAssetCategoryFromInstrumentType,
} from "./symbolMarketMetadata";

test("buildScalpOpeningHoursSchedule expands cross-midnight windows", () => {
  const schedule = buildScalpOpeningHoursSchedule({
    zone: "UTC",
    days: {
      sun: ["23:00 - 02:00"],
    },
  });
  assert.ok(schedule);
  assert.equal(schedule?.zone, "UTC");
  assert.deepEqual(schedule?.windows, [
    { day: "mon", openTime: "00:00", closeTime: "02:00" },
    { day: "sun", openTime: "23:00", closeTime: "23:59" },
  ]);
});

test("normalizeScalpSymbolMarketMetadata preserves broker pip sizes", () => {
  const metadata = normalizeScalpSymbolMarketMetadata({
    symbol: "BTCUSDT",
    epic: "CS.D.BITCOIN.TODAY.IP",
    source: "capital",
    assetCategory: "crypto",
    pipSize: 1,
    pipPosition: 0,
    tickSize: 1,
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
    fetchedAtMs: 1,
  });
  assert.equal(metadata.pipSize, 1);
  assert.equal(metadata.pipPosition, 0);
  assert.equal(metadata.tickSize, 1);
});

test("normalizeScalpSymbolMarketMetadata normalizes bitget crypto precision", () => {
  const metadata = normalizeScalpSymbolMarketMetadata({
    symbol: "XANUSDT",
    epic: "XANUSDT",
    source: "bitget",
    assetCategory: "equity",
    instrumentType: "PERPETUAL",
    pipSize: 0.0000001,
    tickSize: 0.000001,
    decimalPlacesFactor: 6,
    maxLeverage: 50.9,
    fetchedAtMs: 1,
  });
  assert.equal(metadata.assetCategory, "crypto");
  assert.equal(metadata.pipSize, 0.000001);
  assert.equal(metadata.maxLeverage, 50);
});

test("scalpAssetCategoryFromInstrumentType maps Capital categories", () => {
  assert.equal(
    scalpAssetCategoryFromInstrumentType("BTCUSDT", "CRYPTOCURRENCIES"),
    "crypto",
  );
  assert.equal(
    scalpAssetCategoryFromInstrumentType("NAS100", "INDICES"),
    "index",
  );
  assert.equal(
    scalpAssetCategoryFromInstrumentType("XAUUSDT", "COMMODITIES"),
    "commodity",
  );
});

test("buildHeuristicScalpSymbolMarketMetadata keeps crypto always open", () => {
  const metadata = buildHeuristicScalpSymbolMarketMetadata("BTCUSDT");
  assert.equal(metadata.assetCategory, "crypto");
  assert.equal(metadata.openingHours?.alwaysOpen, true);
});
