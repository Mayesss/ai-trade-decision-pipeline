import assert from "node:assert/strict";
import { test } from "vitest";

import { buildEventReactionContext, measureEventReaction } from "../../../lib/swing/eventReaction";
import type { ForexCompactEvent } from "../../../lib/swing/forexEvents";

const RELEASE_MS = Date.UTC(2026, 6, 14, 12, 30); // 2026-07-14T12:30Z (CPI-style)
const BAR_MS = 15 * 60_000;

function compactEvent(overrides: Partial<ForexCompactEvent> = {}): ForexCompactEvent {
  return {
    id: "abc123",
    timestamp_utc: new Date(RELEASE_MS).toISOString(),
    currency: "USD",
    impact: "HIGH",
    event_name: "Core CPI m/m",
    minutesToEvent: -60,
    activeWindow: false,
    ...overrides,
  };
}

// 15m bars as [ts, o, h, l, c, v] rows around the release. Flat at 100 before,
// then an up impulse to 101 that partially retraces to 100.6.
// Release at 12:30 falls ON a bar boundary: the 12:15 bar (closes 12:30) is the
// last fully-closed pre-release bar → anchor close = 100.
function fixtureBars(): number[][] {
  const bars: number[][] = [];
  let ts = RELEASE_MS - 8 * BAR_MS;
  for (; ts < RELEASE_MS; ts += BAR_MS) bars.push([ts, 100, 100.05, 99.95, 100, 10]);
  bars.push([ts, 100, 101, 99.9, 100.9, 50]); ts += BAR_MS;   // release bar: spike to 101
  bars.push([ts, 100.9, 100.95, 100.5, 100.6, 30]); ts += BAR_MS; // partial give-back
  bars.push([ts, 100.6, 100.7, 100.55, 100.6, 20]);
  return bars;
}

test("measureEventReaction quantifies an up reaction with partial retrace", () => {
  const bars = fixtureBars();
  const nowMs = RELEASE_MS + 45 * 60_000;
  const m = measureEventReaction({
    event: compactEvent(),
    bars: bars.map((b) => ({ ts: b[0], open: b[1], high: b[2], low: b[3], close: b[4] })),
    barMs: BAR_MS,
    nowMs,
  });
  assert.ok(m);
  assert.equal(m.minutes_since_release, 45);
  // (100.6/100 − 1) × 1e4 = 60bp
  assert.equal(m.ret_since_release_bp, 60);
  // range over post bars: high 101, low 99.9 → 110bp vs anchor 100
  assert.equal(m.range_since_release_bp, 110);
  // push up = 1.0, price gave back 0.4 → 0.4
  assert.equal(m.retrace_pct, 0.4);
});

test("anchor excludes the bar containing the release", () => {
  // Release mid-bar: 12:37 → the 12:30 bar contains the release and must NOT
  // be the anchor; the 12:15 bar (closes 12:30) is.
  const bars = fixtureBars();
  const event = compactEvent({ timestamp_utc: new Date(RELEASE_MS + 7 * 60_000).toISOString() });
  const m = measureEventReaction({
    event,
    bars: bars.map((b) => ({ ts: b[0], open: b[1], high: b[2], low: b[3], close: b[4] })),
    barMs: BAR_MS,
    nowMs: RELEASE_MS + 45 * 60_000,
  });
  assert.ok(m);
  // Anchor stays the flat 100 close → same 60bp, not measured off the spike bar.
  assert.equal(m.ret_since_release_bp, 60);
});

test("returns null when candles do not reach back to the release", () => {
  const bars = fixtureBars().slice(-2); // only post-release bars
  const m = measureEventReaction({
    event: compactEvent(),
    bars: bars.map((b) => ({ ts: b[0], open: b[1], high: b[2], low: b[3], close: b[4] })),
    barMs: BAR_MS,
    nowMs: RELEASE_MS + 45 * 60_000,
  });
  assert.equal(m, null);
});

test("buildEventReactionContext handles array rows, second-scale timestamps, and empty input", () => {
  const secondsRows = fixtureBars().map((b) => [b[0] / 1000, b[1], b[2], b[3], b[4], b[5]]);
  const out = buildEventReactionContext({
    recentEvents: [compactEvent()],
    candles: secondsRows,
    nowMs: RELEASE_MS + 45 * 60_000,
  });
  assert.ok(out);
  assert.equal(out.length, 1);
  assert.equal(out[0].ret_since_release_bp, 60);
  assert.equal(out[0].event_name, "Core CPI m/m");

  assert.equal(buildEventReactionContext({ recentEvents: [], candles: secondsRows }), null);
  assert.equal(buildEventReactionContext({ recentEvents: null, candles: secondsRows }), null);
  assert.equal(buildEventReactionContext({ recentEvents: [compactEvent()], candles: [] }), null);
});

test("retrace_pct is null when the reaction is too small to measure", () => {
  // Post-release bars barely move: push < 2bp → retrace ratio meaningless.
  const bars: number[][] = [];
  let ts = RELEASE_MS - 4 * BAR_MS;
  for (; ts < RELEASE_MS; ts += BAR_MS) bars.push([ts, 100, 100.005, 99.995, 100, 10]);
  bars.push([ts, 100, 100.01, 99.995, 100.005, 10]);
  const out = buildEventReactionContext({
    recentEvents: [compactEvent()],
    candles: bars,
    nowMs: RELEASE_MS + 20 * 60_000,
  });
  assert.ok(out);
  assert.equal(out[0].retrace_pct, null);
  assert.equal(out[0].ret_since_release_bp, 0.5);
});

// Since 2026-10-06 /api/analyze feeds the micro (1H) candles — the 15m nano
// fetch is gone. On hourly bars a 12:30 release sits INSIDE the 12:00 bar, so
// the anchor is the 11:00 bar (closes 12:00): half an hour of pre-release
// drift lands in the measurement, which is the stated cost of the coarser grain.
test("buildEventReactionContext on hourly bars: bar length inferred, anchor is the last hour closed before release", () => {
  const H = 60 * 60_000;
  const start = Date.UTC(2026, 6, 14, 8, 0);
  const rows = [
    [start, 100, 100.05, 99.95, 100, 10], // 08:00
    [start + H, 100, 100.05, 99.95, 100, 10], // 09:00
    [start + 2 * H, 100, 100.05, 99.95, 100, 10], // 10:00
    [start + 3 * H, 100, 100.25, 99.95, 100.2, 10], // 11:00 — closes 12:00 at 100.2 (pre-release drift)
    [start + 4 * H, 100.2, 101.2, 100.1, 100.9, 50], // 12:00 — contains the 12:30 release
    [start + 5 * H, 100.9, 101, 100.7, 100.8, 30], // 13:00 — forming
  ];
  const out = buildEventReactionContext({
    recentEvents: [compactEvent()],
    candles: rows,
    nowMs: RELEASE_MS + 75 * 60_000,
  });
  assert.ok(out);
  assert.equal(out[0].minutes_since_release, 75);
  // (100.8 / 100.2 − 1) × 1e4 ≈ 59.9bp, anchored on the 11:00 bar, not the 12:00 one.
  assert.equal(out[0].ret_since_release_bp, 59.9);
});
