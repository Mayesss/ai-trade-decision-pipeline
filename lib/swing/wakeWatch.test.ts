import assert from 'node:assert/strict';
import test from 'node:test';

import {
    breakTriggerFailed,
    emergencyMoveAtr,
    lastClosedBar,
    minutesSinceBarBoundary,
    timeframeToMs,
    wakeBandCrossed,
} from './wakeWatch';

test('wakeBandCrossed: at/beyond a band wakes, inside stays quiet', () => {
    assert.equal(wakeBandCrossed(105, 105, 95), 'above');
    assert.equal(wakeBandCrossed(106, 105, 95), 'above');
    assert.equal(wakeBandCrossed(95, 105, 95), 'below');
    assert.equal(wakeBandCrossed(94, 105, 95), 'below');
    assert.equal(wakeBandCrossed(100, 105, 95), null);
});

test('wakeBandCrossed: single-sided bands and missing data', () => {
    assert.equal(wakeBandCrossed(106, 105, null), 'above');
    assert.equal(wakeBandCrossed(94, 105, null), null);
    assert.equal(wakeBandCrossed(94, null, 95), 'below');
    assert.equal(wakeBandCrossed(null, 105, 95), null);
    assert.equal(wakeBandCrossed(NaN, 105, 95), null);
    assert.equal(wakeBandCrossed(100, null, null), null);
});

test('emergencyMoveAtr: absolute move in ATR units, either direction', () => {
    const ref = { price: 100, atr: 2, ts: 0 };
    assert.equal(emergencyMoveAtr(103, ref), 1.5);
    assert.equal(emergencyMoveAtr(97, ref), 1.5);
    assert.equal(emergencyMoveAtr(100, ref), 0);
});

test('emergencyMoveAtr: unusable ref or price fails QUIET (null)', () => {
    assert.equal(emergencyMoveAtr(103, null), null);
    assert.equal(emergencyMoveAtr(103, { price: 100, atr: null, ts: 0 }), null);
    assert.equal(emergencyMoveAtr(103, { price: 100, atr: 0, ts: 0 }), null);
    assert.equal(emergencyMoveAtr(103, { price: NaN, atr: 2, ts: 0 }), null);
    assert.equal(emergencyMoveAtr(null, { price: 100, atr: 2, ts: 0 }), null);
});

// ---- failed-break watch helpers ----

const HOUR = 60 * 60_000;

test('timeframeToMs: parses the timeframe strings both venues use', () => {
    assert.equal(timeframeToMs('4H'), 4 * HOUR);
    assert.equal(timeframeToMs('1h'), HOUR);
    assert.equal(timeframeToMs('15m'), 15 * 60_000);
    assert.equal(timeframeToMs('1D'), 24 * HOUR);
    assert.equal(timeframeToMs('1W'), 7 * 24 * HOUR);
    assert.equal(timeframeToMs('4D'), 4 * 24 * HOUR);
});

test('timeframeToMs: garbage fails quiet (null)', () => {
    for (const tf of ['', null, undefined, 'utc4', '0h', '-1h', 'h', '4x']) {
        assert.equal(timeframeToMs(tf as any), null);
    }
});

test('lastClosedBar: skips the forming bar, coerces string cells, ignores junk rows', () => {
    const tfMs = 4 * HOUR;
    const t0 = 1_784_700_000_000 - (1_784_700_000_000 % tfMs); // aligned boundary
    const candles = [
        [String(t0 - tfMs), '1', '2', '0.5', '99'],
        ['not-a-ts', '1', '2', '0.5', '1'],
        [String(t0), '1', '2', '0.5', '101'], // closed at t0+tfMs
        [String(t0 + tfMs), '1', '2', '0.5', '105'], // forming
    ];
    const bar = lastClosedBar(candles, tfMs, t0 + tfMs + 60_000);
    assert.deepEqual(bar, { closeTs: t0 + tfMs, close: 101 });
});

test('lastClosedBar: nothing closed / unusable input → null', () => {
    const tfMs = 4 * HOUR;
    assert.equal(lastClosedBar([[String(Date.now()), '1', '2', '0.5', '100']], tfMs, Date.now()), null);
    assert.equal(lastClosedBar([], tfMs, Date.now()), null);
    assert.equal(lastClosedBar(null, tfMs, Date.now()), null);
    assert.equal(lastClosedBar([[String(Date.now() - 2 * tfMs), '1', '2', '0.5', '100']], 0, Date.now()), null);
});

test('breakTriggerFailed: close back through the trigger, per side', () => {
    assert.equal(breakTriggerFailed('long', 100, 99.5), true); // breakout long, closed below trigger
    assert.equal(breakTriggerFailed('long', 100, 100), false); // at the level is not through it
    assert.equal(breakTriggerFailed('long', 100, 101), false);
    assert.equal(breakTriggerFailed('short', 100, 100.5), true); // breakdown short, closed back above
    assert.equal(breakTriggerFailed('short', 100, 99), false);
    assert.equal(breakTriggerFailed('long', null, 99), false);
    assert.equal(breakTriggerFailed('flat' as any, 100, 99), false);
});

test('minutesSinceBarBoundary: minutes into the current bar', () => {
    const tfMs = 4 * HOUR;
    const boundary = 1_784_700_000_000 - (1_784_700_000_000 % tfMs);
    assert.equal(minutesSinceBarBoundary(tfMs, boundary + 3 * 60_000), 3);
    assert.equal(minutesSinceBarBoundary(tfMs, boundary), 0);
    assert.equal(minutesSinceBarBoundary(0, boundary), null);
});
