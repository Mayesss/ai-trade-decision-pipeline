import assert from "node:assert/strict";
import test from "node:test";

import { inferScalpAssetCategory, pipSizeForScalpSymbol } from "./symbolInfo";

test("inferScalpAssetCategory classifies common scalp symbols", () => {
  assert.equal(inferScalpAssetCategory("EURUSD"), "forex");
  assert.equal(inferScalpAssetCategory("USDJPY"), "forex");
  assert.equal(inferScalpAssetCategory("BTCUSDT"), "crypto");
  assert.equal(inferScalpAssetCategory("XAUUSDT"), "commodity");
  assert.equal(inferScalpAssetCategory("QQQUSDT"), "index");
});

test("pipSizeForScalpSymbol preserves FX sizing and uses metal sizing for gold", () => {
  assert.equal(pipSizeForScalpSymbol("EURUSD"), 0.0001);
  assert.equal(pipSizeForScalpSymbol("USDJPY"), 0.01);
  assert.equal(pipSizeForScalpSymbol("XAUUSDT"), 0.01);
  assert.equal(pipSizeForScalpSymbol("XAUUSD"), 0.01);
});

test("pipSizeForScalpSymbol supports explicit env overrides for non-fx symbols", () => {
  const prev = process.env.SCALP_SYMBOL_PIP_SIZE_MAP;
  process.env.SCALP_SYMBOL_PIP_SIZE_MAP = JSON.stringify({ BTCUSDT: 1 });
  try {
    assert.equal(pipSizeForScalpSymbol("BTCUSDT"), 1);
  } finally {
    if (prev === undefined) delete process.env.SCALP_SYMBOL_PIP_SIZE_MAP;
    else process.env.SCALP_SYMBOL_PIP_SIZE_MAP = prev;
  }
});

test("pipSizeForScalpSymbol prefers broker metadata when provided", () => {
  assert.equal(pipSizeForScalpSymbol("BTCUSDT", { pipSize: 1 }), 1);
  assert.equal(pipSizeForScalpSymbol("NAS100", { pipSize: 0.1 }), 0.1);
});
