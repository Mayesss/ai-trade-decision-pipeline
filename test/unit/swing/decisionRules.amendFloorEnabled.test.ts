// The amend stop floor is pinned OFF for the test run (test/harness/setup-env.ts
// sets SWING_AMEND_SL_MIN_ATR=0; prod default 1 from 2026-09-15, = the entry floor (3) from 2026-09-23), so
// decisionRules.exchangeTpSl.test.ts pins the DISABLED amend behaviour. This
// file pins the mechanism as it behaves when ON — the production state.
//
// AMEND_SL_MIN_ATR is captured into a module-level const at import time, so the
// env has to be set BEFORE the modules load: a dedicated file and a dynamic
// import after the stub (the same module-state trap as reentryEnabled).

import assert from 'node:assert/strict';
import { beforeAll, test, vi } from 'vitest';

const FLOOR_ATR = 1;
const PRICE = 100;
const ATR = 2;

let sanitizeExchangeTpSl: typeof import('../../../lib/swing/decisionRules').sanitizeExchangeTpSl;
let AMEND_SL_MIN_ATR: number;

beforeAll(async () => {
    vi.stubEnv('SWING_AMEND_SL_MIN_ATR', String(FLOOR_ATR));
    vi.resetModules();
    ({ sanitizeExchangeTpSl } = await import('../../../lib/swing/decisionRules'));
    ({ AMEND_SL_MIN_ATR } = await import('../../../lib/swing/decisionConfig'));
    // Guard the whole file: if the stub ever stops reaching the module constant
    // these tests would silently pass against the disabled path instead.
    assert.equal(AMEND_SL_MIN_ATR, FLOOR_ATR);
});

const amend = (side: 'long' | 'short', sl: number | null, standingStopLossPrice: number) =>
    sanitizeExchangeTpSl({
        action: 'HOLD',
        positionOpen: true,
        side,
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: null,
        stopLossPrice: sl,
        standingStopLossPrice,
    });

const entry = (action: 'BUY' | 'SELL', sl: number) =>
    sanitizeExchangeTpSl({
        action,
        positionOpen: false,
        side: null,
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: null,
        stopLossPrice: sl,
    });

test('an amended stop inside the floor is DROPPED (standing stop stays), never widened, never flagged for refusal', () => {
    const noise = amend('long', PRICE - 0.3 * ATR, PRICE - 2 * ATR);
    assert.equal(noise.stopLossPrice, null);
    assert.equal(noise.entryStopBelowFloor, false);
    assert.ok(noise.notes.includes('sl_below_amend_floor_dropped'));
    assert.ok(!noise.notes.includes('sl_below_entry_floor'));

    const noiseShort = amend('short', PRICE + 0.3 * ATR, PRICE + 2 * ATR);
    assert.equal(noiseShort.stopLossPrice, null);
    assert.ok(noiseShort.notes.includes('sl_below_amend_floor_dropped'));
});

test('an amended stop at or beyond the floor passes unchanged', () => {
    const trail = amend('long', PRICE - 1.2 * ATR, PRICE - 2 * ATR);
    assert.equal(trail.stopLossPrice, PRICE - 1.2 * ATR);
    assert.ok(!trail.notes.includes('sl_below_amend_floor_dropped'));
    // Exactly at the floor passes: the check is strict-less-than.
    const edge = amend('long', PRICE - FLOOR_ATR * ATR, PRICE - 2 * ATR);
    assert.equal(edge.stopLossPrice, PRICE - FLOOR_ATR * ATR);
});

test('the amend floor does not reach entries — those are the ENTRY floor’s business', () => {
    // 1.5 ATR clears the entry floor (default 1) and would clear the amend floor
    // too; what matters is that no amend note is ever written on an entry.
    const ok = entry('BUY', PRICE - 1.5 * ATR);
    assert.equal(ok.stopLossPrice, PRICE - 1.5 * ATR);
    assert.ok(!ok.notes.includes('sl_below_amend_floor_dropped'));
    // 0.5 ATR: entry floor flags for refusal; still no amend note.
    const tight = entry('BUY', PRICE - 0.5 * ATR);
    assert.equal(tight.entryStopBelowFloor, true);
    assert.ok(!tight.notes.includes('sl_below_amend_floor_dropped'));
});

test('the tighten-only guard still runs when the floor is cleared', () => {
    const loosen = amend('long', PRICE - 1.5 * ATR, PRICE - 1 * ATR);
    assert.equal(loosen.stopLossPrice, null);
    assert.ok(loosen.notes.includes('sl_loosened_dropped'));
});
