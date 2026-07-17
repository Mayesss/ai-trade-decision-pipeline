import assert from 'node:assert/strict';
import test from 'node:test';

import { sanitizeHoldCooldown, HOLD_COOLDOWN_MAX_MINUTES, HOLD_COOLDOWN_MIN_MINUTES } from './ai';

const base = { action: 'HOLD', positionOpen: false, price: 100 };

test('sanitizeHoldCooldown: valid flat HOLD request passes with bands intact', () => {
    const out = sanitizeHoldCooldown({ ...base, cooldownMinutes: 120, wakeAbove: 105, wakeBelow: 95 });
    assert.equal(out.cooldownMinutes, 120);
    assert.equal(out.wakeAbove, 105);
    assert.equal(out.wakeBelow, 95);
    assert.deepEqual(out.notes, []);
});

test('sanitizeHoldCooldown: only flat HOLD is eligible', () => {
    for (const params of [
        { ...base, action: 'BUY' },
        { ...base, action: 'CLOSE' },
        { ...base, positionOpen: true },
    ]) {
        const out = sanitizeHoldCooldown({ ...params, cooldownMinutes: 60, wakeAbove: 105, wakeBelow: 95 });
        assert.equal(out.cooldownMinutes, null);
        assert.equal(out.wakeAbove, null);
        assert.equal(out.wakeBelow, null);
    }
});

test('sanitizeHoldCooldown: minutes clamp to [min, max]', () => {
    assert.equal(sanitizeHoldCooldown({ ...base, cooldownMinutes: 1, wakeAbove: null, wakeBelow: null }).cooldownMinutes, HOLD_COOLDOWN_MIN_MINUTES);
    assert.equal(
        sanitizeHoldCooldown({ ...base, cooldownMinutes: 100000, wakeAbove: null, wakeBelow: null }).cooldownMinutes,
        HOLD_COOLDOWN_MAX_MINUTES,
    );
    assert.equal(sanitizeHoldCooldown({ ...base, cooldownMinutes: null, wakeAbove: 105, wakeBelow: null }).cooldownMinutes, null);
});

test('sanitizeHoldCooldown: wrong-side wake bands are dropped, cooldown survives', () => {
    const out = sanitizeHoldCooldown({ ...base, cooldownMinutes: 60, wakeAbove: 99, wakeBelow: 101 });
    assert.equal(out.cooldownMinutes, 60);
    assert.equal(out.wakeAbove, null);
    assert.equal(out.wakeBelow, null);
    assert.ok(out.notes.includes('wake_above_dropped_not_above_price'));
    assert.ok(out.notes.includes('wake_below_dropped_not_below_price'));
});

test('sanitizeHoldCooldown: unknown price drops bands but keeps the cooldown', () => {
    const out = sanitizeHoldCooldown({ ...base, price: null, cooldownMinutes: 60, wakeAbove: 105, wakeBelow: 95 });
    assert.equal(out.cooldownMinutes, 60);
    assert.equal(out.wakeAbove, null);
    assert.equal(out.wakeBelow, null);
    assert.ok(out.notes.includes('wake_bands_dropped_price_unknown'));
});
