// The entry target floor is pinned OFF for the test run (test/harness/setup-env.ts
// sets SWING_ENTRY_TP_MIN_R=0; the prod default is 1 since 2026-09-23). This file
// pins the mechanism as it behaves when ON — the production state.
//
// ENTRY_TP_MIN_R is captured into a module-level const at import time, so the
// env has to be set BEFORE the modules load: a dedicated file and a dynamic
// import after the stub (the same module-state trap as amendFloorEnabled).

import assert from 'node:assert/strict';
import { beforeAll, test, vi } from 'vitest';

const PRICE = 100;
const ATR = 1;

let sanitizeExchangeTpSl: typeof import('../../../lib/swing/decisionRules').sanitizeExchangeTpSl;

beforeAll(async () => {
    vi.stubEnv('SWING_ENTRY_TP_MIN_R', '1');
    vi.stubEnv('SWING_ENTRY_SL_MIN_ATR', '3');
    vi.resetModules();
    ({ sanitizeExchangeTpSl } = await import('../../../lib/swing/decisionRules'));
    const cfg = await import('../../../lib/swing/decisionConfig');
    assert.equal(cfg.ENTRY_TP_MIN_R, 1);
});

const entry = (action: 'BUY' | 'SELL', tp: number | null, sl: number | null) =>
    sanitizeExchangeTpSl({ action, positionOpen: false, side: null, price: PRICE, primaryAtr: ATR, takeProfitPrice: tp, stopLossPrice: sl });

test('a target closer than the entry stop is flagged for refusal, both legs returned as asked', () => {
    // The BTCUSDT 09-20 shape: 3.2-ATR stop, 1.5-ATR target (TP/SL 0.47).
    const out = entry('BUY', PRICE + 1.5, PRICE - 3.2);
    assert.equal(out.entryTargetBelowStopRatio, true);
    assert.equal(out.takeProfitPrice, PRICE + 1.5);
    assert.equal(out.stopLossPrice, PRICE - 3.2);
    assert.ok(out.notes.includes('tp_below_entry_stop_ratio'));

    const short = entry('SELL', PRICE - 2, PRICE + 3.5);
    assert.equal(short.entryTargetBelowStopRatio, true);
});

test('a target at or beyond 1x the stop passes', () => {
    assert.equal(entry('BUY', PRICE + 3.2, PRICE - 3.2).entryTargetBelowStopRatio, false);
    assert.equal(entry('SELL', PRICE - 6, PRICE + 3).entryTargetBelowStopRatio, false);
});

test('one refusal reason at a time: a stop already under its floor is not also flagged on the target', () => {
    const out = entry('BUY', PRICE + 0.5, PRICE - 1);
    assert.equal(out.entryStopBelowFloor, true);
    assert.equal(out.entryTargetBelowStopRatio, false);
});

test('no model stop (caller attaches the default) -> nothing to measure against', () => {
    assert.equal(entry('BUY', PRICE + 0.5, null).entryTargetBelowStopRatio, false);
});

test('amends are never flagged — pulling a target in on a live position is management, not an entry', () => {
    const out = sanitizeExchangeTpSl({
        action: 'HOLD',
        positionOpen: true,
        side: 'long',
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: PRICE + 0.5,
        stopLossPrice: null,
        standingStopLossPrice: PRICE - 3,
    });
    assert.equal(out.entryTargetBelowStopRatio, false);
    assert.equal(out.takeProfitPrice, PRICE + 0.5);
});
