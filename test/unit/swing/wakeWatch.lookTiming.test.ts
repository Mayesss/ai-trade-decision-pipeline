import assert from 'node:assert/strict';
import { test } from 'vitest';

import { wakeLookTiming } from '../../../lib/swing/wakeWatch';

const NOW = Date.UTC(2026, 8, 11, 7, 31);
const minAgo = (m: number) => NOW - m * 60_000;

test('unimpeded sustained wake: sustained = the whole hold, no gate figure', () => {
    assert.deepEqual(wakeLookTiming({ touchStartedMs: minAgo(12), gateHeldAtMs: null, nowMs: NOW }), {
        crossedMinutesAgo: 12,
        sustainedMinutes: 12,
        gateHeldMinutes: null,
    });
});

test('DE40 2026-09-11: touch 05:31, fire refused 05:41, look 07:31 → sustained 10, held 110', () => {
    // Before the split this look reported "sustained 119 min" and the model
    // read a two-hour pre-open hold as a stronger break.
    assert.deepEqual(wakeLookTiming({ touchStartedMs: minAgo(120), gateHeldAtMs: minAgo(110), nowMs: NOW }), {
        crossedMinutesAgo: 120,
        sustainedMinutes: 10,
        gateHeldMinutes: 110,
    });
});

test('instant band (no touch state) parked by a gate: only the hold is known', () => {
    assert.deepEqual(wakeLookTiming({ touchStartedMs: null, gateHeldAtMs: minAgo(30), nowMs: NOW }), {
        crossedMinutesAgo: null,
        sustainedMinutes: null,
        gateHeldMinutes: 30,
    });
});

test('a stamp older than the touch belongs to an earlier crossing and is ignored', () => {
    assert.deepEqual(wakeLookTiming({ touchStartedMs: minAgo(20), gateHeldAtMs: minAgo(200), nowMs: NOW }), {
        crossedMinutesAgo: 20,
        sustainedMinutes: 20,
        gateHeldMinutes: null,
    });
});

test('sustained never reports 0 for a confirmed touch; nothing known → all null', () => {
    assert.equal(wakeLookTiming({ touchStartedMs: NOW - 10_000, gateHeldAtMs: null, nowMs: NOW }).sustainedMinutes, 1);
    assert.deepEqual(wakeLookTiming({ touchStartedMs: null, gateHeldAtMs: null, nowMs: NOW }), {
        crossedMinutesAgo: null,
        sustainedMinutes: null,
        gateHeldMinutes: null,
    });
});
