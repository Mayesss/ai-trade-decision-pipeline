import assert from 'node:assert/strict';
import test from 'node:test';

import {
    breakTriggerFailed,
    clampWakeSustainMinutes,
    emergencyMoveAtr,
    flatWakePlanStale,
    lastClosedBar,
    minutesSinceBarBoundary,
    sustainedWakeStep,
    timeframeToMs,
    wakeBandCrossed,
    WAKE_PLAN_GRACE_MINUTES_DEFAULT,
    WAKE_REF_MAX_AGE_MINUTES_DEFAULT,
    WAKE_SUSTAIN_MAX_MINUTES,
    WAKE_SUSTAIN_MIN_MINUTES,
} from './wakeWatch';

const GRACE_MS = WAKE_PLAN_GRACE_MINUTES_DEFAULT * 60_000;

test('flatWakePlanStale: fresh during the cooldown and within grace after it', () => {
    const until = 1_750_000_000_000;
    // Mid-cooldown: the scheduled scenario, never stale.
    assert.equal(flatWakePlanStale(until, until - 6 * 3_600_000, until - 3_600_000), false);
    // Just past cooldown end, inside grace: still the planned horizon.
    assert.equal(flatWakePlanStale(until, null, until + GRACE_MS - 60_000), false);
});

test('flatWakePlanStale: stale past cooldown end + grace (the GOLD outage shape)', () => {
    const until = 1_750_000_000_000;
    assert.equal(flatWakePlanStale(until, null, until + GRACE_MS + 60_000), true);
});

test('flatWakePlanStale: no until_ms falls back to set_at + grace; neither → never stale', () => {
    const setAt = 1_750_000_000_000;
    assert.equal(flatWakePlanStale(null, setAt, setAt + GRACE_MS - 60_000), false);
    assert.equal(flatWakePlanStale(0, setAt, setAt + GRACE_MS + 60_000), true);
    // Fail open: with no usable timestamps the band keeps working.
    assert.equal(flatWakePlanStale(null, 0, Number.MAX_SAFE_INTEGER), false);
});

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

test('emergencyMoveAtr: with nowMs a stale ref fails QUIET, a fresh one measures', () => {
    const now = 1_750_000_000_000;
    const maxAgeMs = WAKE_REF_MAX_AGE_MINUTES_DEFAULT * 60_000;
    const fresh = { price: 100, atr: 2, ts: now - maxAgeMs + 60_000 };
    const stale = { price: 100, atr: 2, ts: now - maxAgeMs - 60_000 };
    assert.equal(emergencyMoveAtr(103, fresh, now), 1.5);
    // Frozen anchor (outage / venue closure): quiet, the regular cadence owns it.
    assert.equal(emergencyMoveAtr(103, stale, now), null);
    // A ref with no usable timestamp cannot prove freshness → quiet too.
    assert.equal(emergencyMoveAtr(103, { price: 100, atr: 2, ts: 0 }, now), null);
    // Without nowMs the raw measurement is preserved (age unchecked).
    assert.equal(emergencyMoveAtr(103, stale), 1.5);
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

// ---------------------------------------------------------------------------
// Sustained wake confirmation
// ---------------------------------------------------------------------------

// Band above 105 / below 95, 30-min confirmation window.
const NOW = 1_750_000_000_000;
const sustainedBase = {
    wakeAbove: 105,
    wakeBelow: 95,
    sustainMinutes: 30,
    touchSide: null as 'above' | 'below' | null,
    touchStartedMs: null as number | null,
    touchExtreme: null as number | null,
    nowMs: NOW,
};

test('clampWakeSustainMinutes: clamps into the bounded window, null for non-positive', () => {
    assert.equal(clampWakeSustainMinutes(30), 30);
    assert.equal(clampWakeSustainMinutes(1), WAKE_SUSTAIN_MIN_MINUTES);
    assert.equal(clampWakeSustainMinutes(500), WAKE_SUSTAIN_MAX_MINUTES);
    assert.equal(clampWakeSustainMinutes(0), null);
    assert.equal(clampWakeSustainMinutes(-5), null);
    assert.equal(clampWakeSustainMinutes(null), null);
    assert.equal(clampWakeSustainMinutes('abc'), null);
});

test('sustainedWakeStep: no cross, no touch → idle', () => {
    assert.deepEqual(sustainedWakeStep({ ...sustainedBase, price: 100 }), { kind: 'idle' });
});

test('sustainedWakeStep: first minute beyond the band arms the touch, no fire', () => {
    const step = sustainedWakeStep({ ...sustainedBase, price: 106 });
    assert.deepEqual(step, { kind: 'arm', side: 'above', sweep: null });
});

test('sustainedWakeStep: touch held but window not yet elapsed → extend on new extreme, hold otherwise', () => {
    const touched = { ...sustainedBase, touchSide: 'above' as const, touchStartedMs: NOW - 10 * 60_000 };
    // No stored extreme yet → any valid price persists one.
    assert.deepEqual(sustainedWakeStep({ ...touched, price: 106 }), { kind: 'extend', side: 'above', extreme: 106 });
    // Beyond the stored extreme → push it.
    assert.deepEqual(
        sustainedWakeStep({ ...touched, price: 108, touchExtreme: 107 }),
        { kind: 'extend', side: 'above', extreme: 108 },
    );
    // Inside the stored extreme → nothing to write.
    assert.deepEqual(sustainedWakeStep({ ...touched, price: 106, touchExtreme: 107 }), { kind: 'hold' });
});

test('sustainedWakeStep: window elapsed while still beyond → fire with held minutes', () => {
    const step = sustainedWakeStep({
        ...sustainedBase,
        price: 106,
        touchSide: 'above',
        touchStartedMs: NOW - 31 * 60_000,
        touchExtreme: 108,
    });
    assert.deepEqual(step, { kind: 'fire', side: 'above', heldMinutes: 31, via: 'time', extensionAtr: null });
});

test('sustainedWakeStep: extension ≥0.5 ATR confirms by force — even on the first minute', () => {
    // No touch state at all: a violent first-minute break fires immediately.
    const step = sustainedWakeStep({ ...sustainedBase, price: 106.5, atr: 3 });
    assert.deepEqual(step, { kind: 'fire', side: 'above', heldMinutes: 0, via: 'extension', extensionAtr: 0.5 });
    // Below-side mirror mid-touch: extension beats the clock.
    const below = sustainedWakeStep({
        ...sustainedBase,
        price: 93,
        atr: 4,
        touchSide: 'below',
        touchStartedMs: NOW - 4 * 60_000,
    });
    assert.deepEqual(below, { kind: 'fire', side: 'below', heldMinutes: 4, via: 'extension', extensionAtr: 0.5 });
});

test('sustainedWakeStep: extension uses CURRENT price, not the stored extreme; sub-threshold arms/extends', () => {
    // Spiked to 0.9 ATR earlier (extreme) but sits at 0.13 ATR now → not force.
    const step = sustainedWakeStep({
        ...sustainedBase,
        price: 105.4,
        atr: 3,
        touchSide: 'above',
        touchStartedMs: NOW - 4 * 60_000,
        touchExtreme: 107.7,
    });
    assert.deepEqual(step, { kind: 'hold' });
    // First minute, sub-threshold excursion, no ATR → plain arm.
    assert.deepEqual(sustainedWakeStep({ ...sustainedBase, price: 105.4, atr: 3 }), {
        kind: 'arm',
        side: 'above',
        sweep: null,
    });
});

test('sustainedWakeStep: reclaimed before the window → sweep with touch evidence', () => {
    const step = sustainedWakeStep({
        ...sustainedBase,
        price: 100,
        touchSide: 'above',
        touchStartedMs: NOW - 12 * 60_000,
        touchExtreme: 107.5,
    });
    assert.deepEqual(step, {
        kind: 'sweep',
        sweep: { side: 'above', level: 105, touchedAtMs: NOW - 12 * 60_000, reclaimedAtMs: NOW, extreme: 107.5 },
    });
});

test('sustainedWakeStep: side flip re-arms the new side and sweeps the failed old touch', () => {
    const step = sustainedWakeStep({
        ...sustainedBase,
        price: 94,
        touchSide: 'above',
        touchStartedMs: NOW - 8 * 60_000,
        touchExtreme: 106,
    });
    assert.deepEqual(step, {
        kind: 'arm',
        side: 'below',
        sweep: { side: 'above', level: 105, touchedAtMs: NOW - 8 * 60_000, reclaimedAtMs: NOW, extreme: 106 },
    });
});

test('sustainedWakeStep: below-side mirror — arm, fire, sweep', () => {
    assert.deepEqual(sustainedWakeStep({ ...sustainedBase, price: 94 }), { kind: 'arm', side: 'below', sweep: null });
    assert.deepEqual(
        sustainedWakeStep({
            ...sustainedBase,
            price: 94,
            touchSide: 'below',
            touchStartedMs: NOW - 45 * 60_000,
            touchExtreme: 93,
        }),
        { kind: 'fire', side: 'below', heldMinutes: 45, via: 'time', extensionAtr: null },
    );
    const sweep = sustainedWakeStep({
        ...sustainedBase,
        price: 96,
        touchSide: 'below',
        touchStartedMs: NOW - 5 * 60_000,
        touchExtreme: 93.4,
    });
    assert.equal(sweep.kind, 'sweep');
    assert.equal((sweep as any).sweep.level, 95);
});

test('sustainedWakeStep: unusable price with a live touch stays put (no false sweep)', () => {
    // A failed price fetch must not read as "price reclaimed the band".
    const step = sustainedWakeStep({
        ...sustainedBase,
        price: null,
        touchSide: 'above',
        touchStartedMs: NOW - 10 * 60_000,
        touchExtreme: 107,
    });
    assert.deepEqual(step, { kind: 'hold' });
    // And with no touch either, it is just an idle minute.
    assert.deepEqual(sustainedWakeStep({ ...sustainedBase, price: null }), { kind: 'idle' });
});
