import assert from "node:assert/strict";
import { test } from "node:test";

import { combineStageStats, composerCohortKey } from "./pooledSignificance";

// Reference: compute exact stats directly from a concatenated per-trade array.
function exactFromTrades(arr: number[]) {
  const n = arr.length;
  const mean = arr.reduce((a, b) => a + b, 0) / n;
  const variance = arr.reduce((a, b) => a + (b - mean) * (b - mean), 0) / (n - 1);
  const sd = Math.sqrt(variance);
  const stderr = sd / Math.sqrt(n);
  return { n, mean, sd, stderr, lowerBoundR: mean - 1.64 * stderr };
}

function groupFrom(arr: number[]) {
  const ref = exactFromTrades(arr);
  return { n: ref.n, meanR: ref.mean, stdR: ref.sd };
}

test("combineStageStats reconstructs the exact pooled population statistics", () => {
  const a = [1, -1, 0.5, -0.5, 2, -1, 1, -1];
  const b = [0.2, 0.3, -0.4, 1.1, -1, 0.5];
  const c = [-0.5, -0.5, 2, -1, 0.8];

  const pooled = combineStageStats([groupFrom(a), groupFrom(b), groupFrom(c)]);
  const ref = exactFromTrades([...a, ...b, ...c]);

  assert.ok(pooled);
  assert.equal(pooled!.n, ref.n);
  assert.ok(Math.abs(pooled!.meanR - ref.mean) < 1e-9, "pooled mean matches");
  assert.ok(Math.abs(pooled!.stdR - ref.sd) < 1e-9, "pooled std matches");
  assert.ok(Math.abs(pooled!.stderrR - ref.stderr) < 1e-9, "pooled stderr matches");
  assert.ok(Math.abs(pooled!.lowerBoundR - ref.lowerBoundR) < 1e-9, "pooled lowerBoundR matches");
  assert.equal(pooled!.symbols, 3);
});

test("pooling tightens lowerBoundR for a consistent cross-symbol edge", () => {
  // Three symbols, same modest positive edge, identical spread. Pooling more
  // trades shrinks the standard error => higher (tighter) lowerBoundR than any
  // single symbol, so a real edge clears the gate it would miss per-symbol.
  const oneSymbol = groupFrom([0.3, -0.2, 0.4, -0.1, 0.2, -0.1, 0.3, -0.2]);
  const single = combineStageStats([oneSymbol])!;
  const pooled = combineStageStats([oneSymbol, oneSymbol, oneSymbol])!;
  assert.equal(pooled.n, single.n * 3);
  assert.ok(Math.abs(pooled.meanR - single.meanR) < 1e-9, "mean unchanged");
  assert.ok(pooled.lowerBoundR > single.lowerBoundR, "pooled lower bound is tighter");
});

test("pooling dilutes a one-symbol fluke with weak siblings", () => {
  const winner = groupFrom([1, 1, 0.8, 1.2, 0.9, 1.1]); // looks great alone
  const weak1 = groupFrom([-0.5, -0.4, -0.6, 0.1, -0.3, -0.2]);
  const weak2 = groupFrom([-0.3, -0.5, 0.0, -0.4, -0.2, -0.1]);
  const alone = combineStageStats([winner])!;
  const pooled = combineStageStats([winner, weak1, weak2])!;
  assert.ok(alone.lowerBoundR > 0, "fluke passes alone");
  assert.ok(pooled.lowerBoundR < alone.lowerBoundR, "pooled edge is diluted by weak siblings");
});

test("combineStageStats handles empty / invalid groups", () => {
  assert.equal(combineStageStats([]), null);
  assert.equal(combineStageStats([{ n: 0, meanR: 0, stdR: 0 }]), null);
  assert.equal(combineStageStats([{ n: NaN, meanR: 1, stdR: 1 }]), null);
  const single = combineStageStats([{ n: 5, meanR: 0.1, stdR: 0.5 }]);
  assert.ok(single);
  assert.equal(single!.n, 5);
});

test("composerCohortKey is symbol-less and case-insensitive", () => {
  const k1 = composerCohortKey({ venue: "bitget", session: "tokyo", armId: "regime_h1_m15", modelVersion: "v1" });
  const k2 = composerCohortKey({ venue: "BITGET", session: "Tokyo", armId: "REGIME_H1_M15", modelVersion: "V1" });
  assert.equal(k1, k2);
  assert.equal(k1, "bitget:tokyo:regime_h1_m15:v1");
});
