import assert from "node:assert/strict";
import test from "node:test";

import {
  resolveCapitalEpic,
  resolveCapitalForexBaseUnitUsdFromQuotes,
  resolveCapitalScalpOrderSizing,
} from "./capital";

test("resolveCapitalEpic maps internal gold symbols to classic Capital gold CFD", () => {
  assert.deepEqual(resolveCapitalEpic("XAUUSDT"), {
    ticker: "XAUUSDT",
    epic: "GOLD",
    source: "default",
  });
  assert.deepEqual(resolveCapitalEpic("XAUUSD"), {
    ticker: "XAUUSD",
    epic: "GOLD",
    source: "default",
  });
  assert.deepEqual(resolveCapitalEpic("GOLD"), {
    ticker: "GOLD",
    epic: "GOLD",
    source: "default",
  });
});

test("resolveCapitalEpic maps internal silver symbols to classic Capital silver CFD", () => {
  assert.deepEqual(resolveCapitalEpic("XAGUSDT"), {
    ticker: "XAGUSDT",
    epic: "SILVER",
    source: "default",
  });
  assert.deepEqual(resolveCapitalEpic("XAGUSD"), {
    ticker: "XAGUSD",
    epic: "SILVER",
    source: "default",
  });
  assert.deepEqual(resolveCapitalEpic("SILVER"), {
    ticker: "SILVER",
    epic: "SILVER",
    source: "default",
  });
});

test("resolveCapitalEpic preserves existing crypto CFD mapping", () => {
  assert.deepEqual(resolveCapitalEpic("BTCUSDT"), {
    ticker: "BTCUSDT",
    epic: "BTCUSD",
    source: "default",
  });
});

test("Capital forex conversion resolves USD value for direct and inverse USD legs", () => {
  assert.deepEqual(
    resolveCapitalForexBaseUnitUsdFromQuotes({
      symbol: "EURUSD",
      pairPrice: 1.08,
      quotes: {},
    }),
    {
      baseUnitUsd: 1.08,
      reasonCodes: ["CAPITAL_FX_USD_CONVERSION"],
    },
  );
  assert.deepEqual(
    resolveCapitalForexBaseUnitUsdFromQuotes({
      symbol: "USDJPY",
      pairPrice: 156,
      quotes: {},
    }),
    {
      baseUnitUsd: 1,
      reasonCodes: ["CAPITAL_FX_USD_CONVERSION"],
    },
  );
});

test("Capital forex conversion resolves cross-pair base USD value", () => {
  assert.deepEqual(
    resolveCapitalForexBaseUnitUsdFromQuotes({
      symbol: "AUDNZD",
      pairPrice: 1.225,
      quotes: {
        AUDUSD: 0.66,
        NZDUSD: 0.61,
      },
    }),
    {
      baseUnitUsd: 0.66,
      reasonCodes: ["CAPITAL_FX_USD_CONVERSION"],
    },
  );
  const eurJpy = resolveCapitalForexBaseUnitUsdFromQuotes({
    symbol: "EURJPY",
    pairPrice: 185,
    quotes: {
      USDJPY: 156,
    },
  });
  assert.equal(eurJpy.reasonCodes.includes("CAPITAL_FX_USD_CONVERSION"), true);
  assert.ok(Math.abs((eurJpy.baseUnitUsd ?? 0) - 185 / 156) < 1e-12);
});

test("Capital forex conversion reports unavailable cross-pair conversion", () => {
  assert.deepEqual(
    resolveCapitalForexBaseUnitUsdFromQuotes({
      symbol: "EURJPY",
      pairPrice: 185,
      quotes: {},
    }),
    {
      baseUnitUsd: null,
      reasonCodes: ["CAPITAL_FX_CONVERSION_UNAVAILABLE"],
    },
  );
});

test("Capital EURJPY sizing uses base USD value instead of JPY quote price", () => {
  const sizing = resolveCapitalScalpOrderSizing({
    symbol: "EURJPY",
    assetCategory: "forex",
    requestedNotionalUsd: 500,
    referencePrice: 185,
    minDealSize: 100,
    sizeDecimals: 0,
    availableMarginUsd: 127,
    leverageCap: 30,
    forexBaseUnitUsd: 1.08,
  });

  assert.equal(sizing.accepted, true);
  assert.equal(sizing.size, 400);
  assert.equal(sizing.minNotionalUsd, 108);
  assert.equal(sizing.orderNotionalUsd, 432);
  assert.equal(sizing.rejectReason, null);
  assert.ok(sizing.reasonCodes.includes("CAPITAL_FX_USD_CONVERSION"));
  assert.ok(sizing.reasonCodes.includes("CAPITAL_MIN_SIZE_APPLIED"));
});

test("Capital forex sizing rejects only when available margin cannot cover quantized minimum", () => {
  const sizing = resolveCapitalScalpOrderSizing({
    symbol: "EURJPY",
    assetCategory: "forex",
    requestedNotionalUsd: 500,
    referencePrice: 185,
    minDealSize: 100,
    sizeDecimals: 0,
    availableMarginUsd: 2,
    leverageCap: 30,
    forexBaseUnitUsd: 1.08,
  });

  assert.equal(sizing.accepted, false);
  assert.equal(sizing.size, 100);
  assert.equal(sizing.minNotionalUsd, 108);
  assert.equal(sizing.maxNotionalUsd, 60);
  assert.equal(sizing.rejectReason, "INSUFFICIENT_AVAILABLE_MARGIN");
});

test("Capital forex sizing blocks cross pairs when USD conversion is unavailable", () => {
  const sizing = resolveCapitalScalpOrderSizing({
    symbol: "EURJPY",
    assetCategory: "forex",
    requestedNotionalUsd: 500,
    referencePrice: 185,
    minDealSize: 100,
    sizeDecimals: 0,
    availableMarginUsd: 127,
    leverageCap: 30,
    forexBaseUnitUsd: null,
  });

  assert.equal(sizing.accepted, false);
  assert.equal(sizing.rejectReason, "CAPITAL_FX_CONVERSION_UNAVAILABLE");
  assert.ok(sizing.reasonCodes.includes("CAPITAL_FX_CONVERSION_UNAVAILABLE"));
});
