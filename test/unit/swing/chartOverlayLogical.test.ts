// components/ChartPanel logicalIndexForTime — where an exact moment sits in
// bar-index space, so the chart can draw a position's walls at the minute the
// venue actually filled instead of rounding them to a bar.
//
// The case that forced it: XRPUSDT 2026-09-06, a stop filled at 07:13:20. On
// the ranges drawn with 4H bars — where a bar close IS a decision tick — the
// nearest-bar snap moved that wall to 08:00, so an exchange stop-out read as
// "the position closed on the tick".

import assert from 'node:assert/strict';
import { test } from 'vitest';

import { logicalIndexForTime } from '../../../components/ChartPanel';

const H4 = 4 * 3600;
const FIRST = Date.UTC(2026, 8, 5, 12, 0, 0) / 1000;
const bars = Array.from({ length: 12 }, (_, i) => FIRST + i * H4);

test('a bar time is its own index', () => {
    assert.equal(logicalIndexForTime(bars, FIRST), 0);
    assert.equal(logicalIndexForTime(bars, FIRST + 3 * H4), 3);
    assert.equal(logicalIndexForTime(bars, bars[bars.length - 1]), bars.length - 1);
});

test('a mid-bar moment lands between the two bars, not on one', () => {
    // The real stop-out: 07:13:20 on the 6th, between the 04:00 and 08:00 bars.
    const exit = Date.UTC(2026, 8, 6, 7, 13, 20) / 1000;
    const logical = logicalIndexForTime(bars, exit) as number;
    const before = Math.floor(logical);
    assert.equal(bars[before], Date.UTC(2026, 8, 6, 4, 0, 0) / 1000);
    assert.ok(logical > before && logical < before + 1, 'strictly inside the bar');
    // 3h13m20s into a 4h bar.
    assert.ok(Math.abs(logical - before - 0.8056) < 0.001, `got ${logical - before}`);
});

test('times outside the loaded bars clamp to the ends', () => {
    assert.equal(logicalIndexForTime(bars, FIRST - 10 * H4), 0);
    assert.equal(logicalIndexForTime(bars, FIRST + 100 * H4), bars.length - 1);
});

test('no bars, no position', () => {
    assert.equal(logicalIndexForTime([], FIRST), null);
});

test('a single bar cannot be interpolated across', () => {
    assert.equal(logicalIndexForTime([FIRST], FIRST + 60), 0);
});
