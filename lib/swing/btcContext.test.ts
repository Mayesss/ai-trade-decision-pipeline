import assert from "node:assert/strict";
import test from "node:test";

import { computeBtcContext, loadBtcContext, swingBtcContextEnabled } from "./btcContext";

const DAY = 86_400_000;
const HOUR = 3_600_000;
const T0 = Date.UTC(2026, 0, 1);

// Bitget-shaped candle rows: [ts, open, high, low, close, ...] as strings.
function candlesFromCloses(closes: number[], stepMs: number): string[][] {
  return closes.map((c, i) => [String(T0 + i * stepMs), String(c), String(c), String(c), String(c), "0", "0"]);
}

function closesFromReturns(returns: number[], start = 100): number[] {
  const out = [start];
  for (const r of returns) out.push(out[out.length - 1] * (1 + r));
  return out;
}

// Alternating ±1% BTC daily returns; alt moves at exactly 2x.
const BTC_RETS = Array.from({ length: 94 }, (_, i) => (i % 2 === 0 ? 0.01 : -0.01));
const ALT_RETS = BTC_RETS.map((r) => r * 2);
const FLAT_HOURLY = candlesFromCloses(Array(172).fill(100), HOUR);

test("perfectly coupled alt: corr 1, beta 2, residual near zero", () => {
  const ctx = computeBtcContext({
    altDaily: candlesFromCloses(closesFromReturns(ALT_RETS), DAY),
    btcDaily: candlesFromCloses(closesFromReturns(BTC_RETS), DAY),
    btcHourly: FLAT_HOURLY,
  });
  assert.ok(ctx);
  assert.equal(ctx.corr_30d, 1);
  assert.equal(ctx.corr_90d, 1);
  assert.equal(ctx.beta_90d, 2);
  // Compounding makes the 7d residual small but not exactly zero.
  assert.ok(Math.abs(ctx.alt_vs_btc_residual_7d_bp as number) < 10);
});

test("anti-correlated alt: corr -1, beta -1", () => {
  const ctx = computeBtcContext({
    altDaily: candlesFromCloses(closesFromReturns(BTC_RETS.map((r) => -r)), DAY),
    btcDaily: candlesFromCloses(closesFromReturns(BTC_RETS), DAY),
    btcHourly: FLAT_HOURLY,
  });
  assert.ok(ctx);
  assert.equal(ctx.corr_90d, -1);
  assert.equal(ctx.beta_90d, -1);
});

test("BTC hourly returns are measured close-to-close in bp", () => {
  const closes = Array(172).fill(100);
  closes[171] = 101; // +1% on the latest bar only
  const ctx = computeBtcContext({
    altDaily: candlesFromCloses(closesFromReturns(ALT_RETS), DAY),
    btcDaily: candlesFromCloses(closesFromReturns(BTC_RETS), DAY),
    btcHourly: candlesFromCloses(closes, HOUR),
  });
  assert.ok(ctx);
  assert.equal(ctx.btc.ret_1h_bp, 100);
  assert.equal(ctx.btc.ret_4h_bp, 100);
  assert.equal(ctx.btc.ret_24h_bp, 100);
  assert.equal(ctx.btc.ret_7d_bp, 100);
});

test("freshly listed alt (20 daily bars): correlation fields null, BTC state still present", () => {
  const ctx = computeBtcContext({
    altDaily: candlesFromCloses(closesFromReturns(ALT_RETS.slice(0, 19)), DAY),
    btcDaily: candlesFromCloses(closesFromReturns(BTC_RETS), DAY),
    btcHourly: FLAT_HOURLY,
  });
  assert.ok(ctx);
  assert.equal(ctx.corr_30d, null);
  assert.equal(ctx.corr_90d, null);
  assert.equal(ctx.beta_90d, null);
  assert.equal(ctx.alt_vs_btc_residual_7d_bp, null);
  assert.equal(ctx.btc.ret_24h_bp, 0);
});

test("misaligned daily bars are joined by timestamp, not index", () => {
  // Drop one mid-series alt bar; an index join would shift every later pair
  // and destroy the correlation, a ts join keeps corr at 1.
  const alt = candlesFromCloses(closesFromReturns(ALT_RETS), DAY);
  alt.splice(40, 1);
  const ctx = computeBtcContext({
    altDaily: alt,
    btcDaily: candlesFromCloses(closesFromReturns(BTC_RETS), DAY),
    btcHourly: FLAT_HOURLY,
  });
  assert.ok(ctx);
  assert.equal(ctx.corr_30d, 1);
});

test("no usable data returns null (prompt block absent)", () => {
  assert.equal(computeBtcContext({ altDaily: [], btcDaily: [], btcHourly: [] }), null);
  assert.equal(computeBtcContext({ altDaily: null, btcDaily: undefined, btcHourly: "junk" }), null);
});

test("loader no-ops without network on BTCUSDT and when disabled", async () => {
  assert.equal(await loadBtcContext("BTCUSDT"), null);
  const prev = process.env.SWING_BTC_CONTEXT_ENABLED;
  process.env.SWING_BTC_CONTEXT_ENABLED = "false";
  try {
    assert.equal(swingBtcContextEnabled(), false);
    assert.equal(await loadBtcContext("ETHUSDT"), null);
  } finally {
    if (prev === undefined) delete process.env.SWING_BTC_CONTEXT_ENABLED;
    else process.env.SWING_BTC_CONTEXT_ENABLED = prev;
  }
});
