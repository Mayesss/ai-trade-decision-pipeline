import assert from 'node:assert/strict';
import test from 'node:test';

import { sanitizeEntryLimit } from './ai';

// Anchor: price 100, primary ATR 2. Usable pullback window: 0.1–1.5 ATR.
const PRICE = 100;
const ATR = 2;

const run = (action: 'BUY' | 'SELL' | 'HOLD', limit: number | null, overrides: Record<string, unknown> = {}) =>
    sanitizeEntryLimit({
        action,
        positionOpen: false,
        price: PRICE,
        primaryAtr: ATR,
        entryLimitPrice: limit,
        ...(overrides as any),
    });

test('valid pullback limits pass through unchanged', () => {
    assert.equal(run('BUY', 99).entryLimitPrice, 99); // 0.5 ATR below
    assert.equal(run('SELL', 101.5).entryLimitPrice, 101.5); // 0.75 ATR above
});

test('null limit means market entry, no notes', () => {
    const out = run('BUY', null);
    assert.equal(out.entryLimitPrice, null);
    assert.deepEqual(out.notes, []);
});

test('wrong-side or too-close limits fall back to market entry', () => {
    // BUY limit above price = marketable, not a pullback → market
    const above = run('BUY', 101);
    assert.equal(above.entryLimitPrice, null);
    assert.ok(above.notes.includes('limit_too_close_market_entry'));
    // Within 0.1 ATR of price → effectively market
    const close = run('SELL', 100.1);
    assert.equal(close.entryLimitPrice, null);
});

test('limits beyond 1.5 ATR clamp to the max pullback distance', () => {
    const buy = run('BUY', 90); // 5 ATR below
    assert.equal(buy.entryLimitPrice, PRICE - 1.5 * ATR);
    assert.ok(buy.notes.includes('limit_clamped_max_atr'));
    const sell = run('SELL', 110);
    assert.equal(sell.entryLimitPrice, PRICE + 1.5 * ATR);
});

test('in-position, non-entry actions, and missing ATR never emit a limit', () => {
    assert.equal(run('HOLD', 99).entryLimitPrice, null);
    assert.equal(
        sanitizeEntryLimit({ action: 'BUY', positionOpen: true, price: PRICE, primaryAtr: ATR, entryLimitPrice: 99 })
            .entryLimitPrice,
        null,
    );
    const noAtr = run('BUY', 99, { primaryAtr: null });
    assert.equal(noAtr.entryLimitPrice, null);
    assert.ok(noAtr.notes.includes('limit_dropped_no_atr'));
});
