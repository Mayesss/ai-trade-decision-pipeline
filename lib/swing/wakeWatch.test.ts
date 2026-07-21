import assert from 'node:assert/strict';
import test from 'node:test';

import { wakeBandCrossed, emergencyMoveAtr } from './wakeWatch';

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
