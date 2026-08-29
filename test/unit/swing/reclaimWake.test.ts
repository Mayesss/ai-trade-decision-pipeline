import assert from 'node:assert/strict';
import { test } from 'vitest';

import {
    reclaimWakeEligible,
    RECLAIM_WAKE_FRESH_MINUTES,
    RECLAIM_WAKE_MIN_DEPTH_ATR,
} from '../../../lib/swing/wakeWatch';

const NOW = 1_800_000_000_000;
const base = {
    sweep: { level: 100, extreme: 100.5, reclaimedAtMs: NOW - 2 * 60_000 },
    atr: 2, // depth = 0.5/2 = 0.25 ATR >= 0.2 floor
    reclaimLookedAtMs: null,
    nowMs: NOW,
};

test('eligible: fresh, deep enough, budget unspent', () => {
    assert.equal(reclaimWakeEligible(base), true);
});

test('depth floor: a shallow poke is bar noise, not a liquidity grab', () => {
    const shallow = { ...base, sweep: { ...base.sweep, extreme: 100 + RECLAIM_WAKE_MIN_DEPTH_ATR * 2 * 0.9 } };
    assert.equal(reclaimWakeEligible(shallow), false);
    // exactly at the floor passes
    const atFloor = { ...base, sweep: { ...base.sweep, extreme: 100 + RECLAIM_WAKE_MIN_DEPTH_ATR * 2 } };
    assert.equal(reclaimWakeEligible(atFloor), true);
});

test('one-shot: a spent budget never re-fires', () => {
    assert.equal(reclaimWakeEligible({ ...base, reclaimLookedAtMs: NOW - 3600_000 }), false);
});

test('freshness: a stale sweep is evidence, not an event', () => {
    const stale = {
        ...base,
        sweep: { ...base.sweep, reclaimedAtMs: NOW - (RECLAIM_WAKE_FRESH_MINUTES + 1) * 60_000 },
    };
    assert.equal(reclaimWakeEligible(stale), false);
});

test('unusable inputs fail quiet: no ATR, no extreme, no sweep', () => {
    assert.equal(reclaimWakeEligible({ ...base, atr: null }), false);
    assert.equal(reclaimWakeEligible({ ...base, atr: 0 }), false);
    assert.equal(reclaimWakeEligible({ ...base, sweep: { ...base.sweep, extreme: null } }), false);
    assert.equal(reclaimWakeEligible({ ...base, sweep: null }), false);
});
