// Characterization tests for getCronSymbolConfigs (parsing vercel.json crons),
// written before trimming the dead scalp half out of symbolRegistry.ts.
import { test } from "vitest";
import assert from "node:assert/strict";

import { getCronSymbolConfigs, getCronSymbols } from "../../lib/symbolRegistry";

test("parses the swing analyze crons from vercel.json", () => {
  const configs = getCronSymbolConfigs();
  // 24 -> 8 symbols on 2026-09-15 (docs/alpha-lab-spec.md §10): two per asset
  // class, because the portfolio cap is 4 with one position per class.
  assert.ok(configs.length >= 8, `expected the trimmed symbol universe, got ${configs.length}`);

  const bySymbol = new Map(configs.map((c) => [c.symbol, c]));
  // No duplicate symbols.
  assert.equal(bySymbol.size, configs.length);

  const btc = bySymbol.get("BTCUSDT");
  assert.ok(btc);
  assert.equal(btc.platform, "bitget");
  assert.equal(btc.category, "crypto");
  assert.equal(btc.decisionPolicy, "balanced");

  // No Capital commodity is openable at 1% risk with a 3-ATR stop on the
  // current account (docs/alpha-lab-spec.md §12), so the class is absent.
  const us500 = bySymbol.get("US500");
  assert.ok(us500);
  assert.equal(us500.platform, "capital");
  assert.equal(us500.category, "index");

  const eurusd = bySymbol.get("EURUSD");
  assert.ok(eurusd);
  assert.equal(eurusd.platform, "capital");
  assert.equal(eurusd.category, "forex");

  for (const config of configs) {
    assert.ok(["bitget", "capital"].includes(config.platform), config.symbol);
    assert.ok(config.path.includes("/api/swing/analyze"), config.symbol);
  }
});

test("getCronSymbols mirrors the configs", () => {
  assert.deepEqual(
    getCronSymbols(),
    getCronSymbolConfigs().map((c) => c.symbol),
  );
});
