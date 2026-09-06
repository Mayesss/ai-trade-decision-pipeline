// components/ChartPanel — placing a position's walls at the minute the venue
// actually filled instead of rounding them to a bar.
//
// The case that forced it: XRPUSDT 2026-09-06, a stop filled at 07:13:20. On
// the ranges drawn with 4H bars — where a bar close IS a decision tick — the
// nearest-bar snap moved that wall to 08:00, so an exchange stop-out read as
// "the position closed on the tick".
//
// The first attempt handed the chart a FRACTIONAL logical index. Its
// logical→x mapping starts `if (!isInteger(logical)) return 0`, so every box
// collapsed to a sliver at the left edge and the overlays looked deleted.
// Hence coordinateForAnchor: interpolate between two real bar coordinates.

import assert from 'node:assert/strict';
import { test } from 'vitest';

import { barAnchorForTime, coordinateForAnchor } from '../../../components/ChartPanel';

const H4 = 4 * 3600;
const FIRST = Date.UTC(2026, 8, 5, 12, 0, 0) / 1000;
const bars = Array.from({ length: 12 }, (_, i) => FIRST + i * H4);

test('a bar time anchors to that bar with no fraction', () => {
    assert.deepEqual(barAnchorForTime(bars, FIRST + 3 * H4), {
        before: FIRST + 3 * H4,
        after: FIRST + 4 * H4,
        frac: 0,
    });
});

test('a mid-bar moment carries the fraction between its two bars', () => {
    // The real stop-out: 07:13:20 on the 6th, between the 04:00 and 08:00 bars.
    const exit = Date.UTC(2026, 8, 6, 7, 13, 20) / 1000;
    const anchor = barAnchorForTime(bars, exit);
    assert.equal(anchor?.before, Date.UTC(2026, 8, 6, 4, 0, 0) / 1000);
    assert.equal(anchor?.after, Date.UTC(2026, 8, 6, 8, 0, 0) / 1000);
    // 3h13m20s into a 4h bar.
    assert.ok(Math.abs((anchor?.frac ?? 0) - 0.8056) < 0.001, `got ${anchor?.frac}`);
});

test('times outside the loaded bars clamp to the ends', () => {
    assert.equal(barAnchorForTime(bars, FIRST - 10 * H4)?.before, FIRST);
    const past = barAnchorForTime(bars, FIRST + 100 * H4);
    assert.equal(past?.before, bars[bars.length - 1]);
    assert.equal(past?.after, null, 'nothing to interpolate towards at the last bar');
});

test('no bars, no anchor', () => {
    assert.equal(barAnchorForTime([], FIRST), null);
});

// --- anchor → x -----------------------------------------------------------
// A stand-in for the chart: bars are 10px apart, and — like the real one — it
// only answers for times that are actually bars.
const toX = (time: number) => {
    const idx = bars.indexOf(time);
    return idx === -1 ? null : idx * 10;
};

test('a mid-bar anchor lands between the two bar coordinates', () => {
    const anchor = { before: bars[4], after: bars[5], frac: 0.8056 };
    assert.ok(Math.abs((coordinateForAnchor(anchor, toX) as number) - 48.056) < 0.001);
});

test('a bar-aligned anchor lands exactly on the bar', () => {
    assert.equal(coordinateForAnchor({ before: bars[4], after: bars[5], frac: 0 }, toX), 40);
    assert.equal(coordinateForAnchor({ before: bars[4], after: null, frac: 0 }, toX), 40);
});

test('no next-bar coordinate falls back to the bar itself, never to zero', () => {
    // The failure mode that shipped: an unanswerable position must not become 0.
    const anchor = { before: bars[4], after: FIRST - H4, frac: 0.5 };
    assert.equal(coordinateForAnchor(anchor, toX), 40);
});

test('an unplaceable anchor is null, so the caller can fall back', () => {
    assert.equal(coordinateForAnchor({ before: FIRST - H4, after: null, frac: 0 }, toX), null);
    assert.equal(coordinateForAnchor(null, toX), null);
    assert.equal(coordinateForAnchor({ before: bars[0], after: null, frac: 0 }, () => Number.NaN), null);
});

test('a mid-bar event projects BEFORE the next bar, not onto it', () => {
    // The post-mortem dot: a win evaluation for a 09:13:20 close, on a 15m
    // chart zoomed to ~246px per bar. Snapping it to the nearest bar put the
    // trophy on the 09:15 tick — 27px to the right of the position wall that
    // marks the very close it is evaluating.
    const M15 = 15 * 60;
    const first = Date.UTC(2026, 8, 6, 0, 0, 0) / 1000;
    const bars15 = Array.from({ length: 96 }, (_, i) => first + i * M15);
    const spacing = 246;
    const barX = (time: number) => {
        const i = bars15.indexOf(time);
        return i === -1 ? null : i * spacing;
    };
    const closeSec = Date.UTC(2026, 8, 6, 7, 13, 20) / 1000;

    const exact = coordinateForAnchor(barAnchorForTime(bars15, closeSec), barX) as number;
    const nextBarX = barX(Date.UTC(2026, 8, 6, 7, 15, 0) / 1000) as number;
    const prevBarX = barX(Date.UTC(2026, 8, 6, 7, 0, 0) / 1000) as number;

    assert.ok(exact > prevBarX && exact < nextBarX, 'lands inside the bar it happened in');
    assert.ok(nextBarX - exact > 25, `the old snap was ${(nextBarX - exact).toFixed(1)}px late`);
});
