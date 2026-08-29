import assert from 'node:assert/strict';
import { test } from 'vitest';

import {
    sessionSweepStep,
    SESSION_SWEEP_WINDOW_MINUTES,
    type SessionLevelsRef,
    type SessionSweepState,
} from '../../../lib/swing/wakeWatch';

const NOW = 1_800_000_000_000;
const ref: SessionLevelsRef = {
    levels: { last_session_high: 110, prior_day_high: 105, last_session_low: 95, prior_day_low: 90 },
    atr: 2,
    ts: NOW - 10 * 60_000,
    lastSessionValidUntilMs: NOW + 3 * 3600_000,
    priorDayValidUntilMs: NOW + 6 * 3600_000,
};

test('idle inside the pools; arm picks the NEAREST violated pool', () => {
    assert.equal(sessionSweepStep({ price: 100, ref, state: null, nowMs: NOW }).kind, 'idle');
    // Below both lows (89): nearest pool above price is prior_day_low (90).
    const arm = sessionSweepStep({ price: 89, ref, state: null, nowMs: NOW });
    assert.equal(arm.kind, 'arm');
    assert.equal((arm as { state: SessionSweepState }).state.kind, 'prior_day_low');
    assert.equal((arm as { state: SessionSweepState }).state.side, 'below');
    // Between the lows (93): only last_session_low (95) is violated.
    const arm2 = sessionSweepStep({ price: 93, ref, state: null, nowMs: NOW });
    assert.equal((arm2 as { state: SessionSweepState }).state.kind, 'last_session_low');
});

test('touch lifecycle: extend deeper, reclaim back inside, abandon past the window', () => {
    const state: SessionSweepState = { kind: 'last_session_low', side: 'below', level: 95, touchedAtMs: NOW - 5 * 60_000, extreme: 94.5 };
    const ext = sessionSweepStep({ price: 94.2, ref, state, nowMs: NOW });
    assert.equal(ext.kind, 'extend');
    assert.equal((ext as { state: SessionSweepState }).state.extreme, 94.2);
    assert.equal(sessionSweepStep({ price: 94.8, ref, state, nowMs: NOW }).kind, 'hold');
    const rec = sessionSweepStep({ price: 95.4, ref, state, nowMs: NOW });
    assert.equal(rec.kind, 'reclaim');
    const ev = (rec as { event: { level: number; extreme: number; atr: number | null } }).event;
    assert.equal(ev.level, 95);
    assert.equal(ev.extreme, 94.5);
    assert.equal(ev.atr, 2);
    const stale: SessionSweepState = { ...state, touchedAtMs: NOW - (SESSION_SWEEP_WINDOW_MINUTES + 1) * 60_000 };
    assert.equal(sessionSweepStep({ price: 94.8, ref, state: stale, nowMs: NOW }).kind, 'abandon');
});

test('validity horizons: expired pools never arm; an expired touched pool abandons', () => {
    const expiredRef = { ...ref, lastSessionValidUntilMs: NOW - 60_000 };
    // 93 violates only last_session_low, which is expired -> idle.
    assert.equal(sessionSweepStep({ price: 93, ref: expiredRef, state: null, nowMs: NOW }).kind, 'idle');
    const state: SessionSweepState = { kind: 'last_session_low', side: 'below', level: 95, touchedAtMs: NOW - 5 * 60_000, extreme: 94 };
    assert.equal(sessionSweepStep({ price: 94, ref: expiredRef, state, nowMs: NOW }).kind, 'abandon');
});

test('fail-quiet: no ref abandons a touch; unusable price holds it', () => {
    const state: SessionSweepState = { kind: 'prior_day_high', side: 'above', level: 105, touchedAtMs: NOW - 2 * 60_000, extreme: 106 };
    assert.equal(sessionSweepStep({ price: 106, ref: null, state, nowMs: NOW }).kind, 'abandon');
    assert.equal(sessionSweepStep({ price: null, ref, state, nowMs: NOW }).kind, 'hold');
    assert.equal(sessionSweepStep({ price: null, ref, state: null, nowMs: NOW }).kind, 'idle');
});
