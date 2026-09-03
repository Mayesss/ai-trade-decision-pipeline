// components/ChartPanel buildLimitOrderItems — how a resting-entry window is
// projected onto the visible candles. The rule that matters: a window issued
// BEFORE the visible range still draws, clipped to the left edge. On the 4H
// range (48 x 5m bars) almost every resting entry predates the window, so
// dropping those is the difference between a line and nothing at all.

import assert from 'node:assert/strict';
import { test } from 'vitest';

import { buildLimitOrderItems } from '../../../components/ChartPanel';

const STEP = 5 * 60; // 5m bars, the 4H preset
const FIRST = 1_788_400_000; // arbitrary epoch second, bar-aligned below
const bars = Array.from({ length: 48 }, (_, i) => ({ time: FIRST + i * STEP, value: 77_000 + i }));
const LAST = bars[bars.length - 1].time;

test('a window issued before the visible range is clipped to the left edge, not dropped', () => {
    const items = buildLimitOrderItems(bars, [
        // Issued 2h before the window opens, filled 30 bars in.
        { side: 'buy', price: 77_500, fromTime: FIRST - 2 * 3600, toTime: FIRST + 30 * STEP, filled: true },
    ]);
    assert.equal(items.length, 1);
    assert.equal(items[0].fromTime, FIRST, 'clamped to the first visible bar');
    assert.equal(items[0].toTime, FIRST + 30 * STEP);
    assert.equal(items[0].filled, true, 'the fill dot survives the clip');
});

test('a window that ends before the visible range is dropped', () => {
    const items = buildLimitOrderItems(bars, [
        { side: 'sell', price: 77_530, fromTime: FIRST - 3 * 3600, toTime: FIRST - 2 * 3600, filled: true },
    ]);
    assert.deepEqual(items, []);
});

test('a still-resting window is clamped to the last visible bar', () => {
    const items = buildLimitOrderItems(bars, [
        { side: 'buy', price: 77_100, fromTime: FIRST + 10 * STEP, toTime: LAST + 6 * 3600, filled: false },
    ]);
    assert.equal(items.length, 1);
    assert.equal(items[0].toTime, LAST);
    assert.equal(items[0].filled, false);
});

test('edges snap to real bar times so the renderer can place them', () => {
    const offBar = FIRST + 12 * STEP + 137; // mid-bar
    const items = buildLimitOrderItems(bars, [
        { side: 'sell', price: 77_400, fromTime: offBar, toTime: offBar + 4 * STEP, filled: false },
    ]);
    assert.equal(items.length, 1);
    assert.ok(
        bars.some((bar) => bar.time === items[0].fromTime) && bars.some((bar) => bar.time === items[0].toTime),
        'both edges land on candle times',
    );
});

test('nonsense segments are ignored', () => {
    assert.deepEqual(
        buildLimitOrderItems(bars, [
            { side: 'buy', price: 0, fromTime: FIRST, toTime: FIRST + STEP, filled: false },
            { side: 'buy', price: 77_000, fromTime: FIRST + 5 * STEP, toTime: FIRST + STEP, filled: false },
        ]),
        [],
    );
    assert.deepEqual(buildLimitOrderItems([], [{ side: 'buy', price: 1, fromTime: 1, toTime: 2, filled: false }]), []);
});
