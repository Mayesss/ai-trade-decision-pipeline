import assert from 'node:assert/strict';
import test from 'node:test';

import { pickTighterStop, recycleLeverageCeiling } from './trading';

// After the margin-recycle maneuver rests a breakeven stop, the AI's same-tick
// stop amend may only TIGHTEN past that trigger — long: higher, short: lower.

test('long: AI stop above the BE trigger tightens (kept), at/below stands down', () => {
    assert.equal(pickTighterStop('long', 100.08, 101), 101);
    assert.equal(pickTighterStop('long', 100.08, 100.08), null);
    assert.equal(pickTighterStop('long', 100.08, 99), null);
});

test('short: AI stop below the BE trigger tightens (kept), at/above stands down', () => {
    assert.equal(pickTighterStop('short', 99.92, 99), 99);
    assert.equal(pickTighterStop('short', 99.92, 99.92), null);
    assert.equal(pickTighterStop('short', 99.92, 101), null);
});

test('no BE trigger (maneuver did not move the stop): AI stop passes through', () => {
    assert.equal(pickTighterStop('long', null, 98), 98);
    assert.equal(pickTighterStop('short', undefined, 103), 103);
    assert.equal(pickTighterStop('long', NaN, 98), 98);
});

test('no usable AI stop: nothing to amend regardless of trigger', () => {
    assert.equal(pickTighterStop('long', 100.08, null), null);
    assert.equal(pickTighterStop('long', null, null), null);
    assert.equal(pickTighterStop('short', 99.92, 0), null);
    assert.equal(pickTighterStop('short', 99.92, NaN), null);
});

// Liq-safe ceiling for recycle raises: liq distance ≈ 1/lev − mmr must stay ≥
// the buffer. Defaults (400bps buffer + 100bps assumed mmr) → floor(10000/500).
test('recycleLeverageCeiling: defaults land at 20x', () => {
    assert.equal(recycleLeverageCeiling(), 20);
});
