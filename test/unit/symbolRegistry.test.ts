// Characterization tests for getCronSymbolConfigs (parsing vercel.json crons),
// written before trimming the dead scalp half out of symbolRegistry.ts.
import { test } from "vitest";
import assert from "node:assert/strict";

import { getCronSymbolConfigs, getCronSymbols } from "../../lib/symbolRegistry";

test("parses the swing analyze crons from vercel.json", () => {
  const configs = getCronSymbolConfigs();
  assert.ok(configs.length >= 20, `expected a full symbol universe, got ${configs.length}`);

  const bySymbol = new Map(configs.map((c) => [c.symbol, c]));
  // No duplicate symbols.
  assert.equal(bySymbol.size, configs.length);

  const btc = bySymbol.get("BTCUSDT");
  assert.ok(btc);
  assert.equal(btc.platform, "bitget");
  assert.equal(btc.category, "crypto");
  assert.equal(btc.decisionPolicy, "balanced");

  const gold = bySymbol.get("GOLD");
  assert.ok(gold);
  assert.equal(gold.platform, "capital");
  assert.equal(gold.category, "commodity");

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
