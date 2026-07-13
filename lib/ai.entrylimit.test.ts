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

test('null limit means market entry as requested, no notes, entry kept', () => {
    const out = run('BUY', null);
    assert.equal(out.entryLimitPrice, null);
    assert.equal(out.dropEntry, false);
    assert.deepEqual(out.notes, []);
});

test('wrong-side or too-close limits DROP the entry (no market fallback)', () => {
    // BUY limit above price = wrong side (marketable, not a pullback) → entry dropped
    const above = run('BUY', 101);
    assert.equal(above.entryLimitPrice, null);
    assert.equal(above.dropEntry, true);
    assert.ok(above.notes.includes('limit_wrong_side_entry_dropped'));
    // Within 0.1 ATR of price → inside the noise band → entry dropped
    const close = run('SELL', 100.1);
    assert.equal(close.entryLimitPrice, null);
    assert.equal(close.dropEntry, true);
    assert.ok(close.notes.includes('limit_too_close_entry_dropped'));
});

test('limits beyond 1.5 ATR clamp to the max pullback distance, entry kept', () => {
    const buy = run('BUY', 90); // 5 ATR below
    assert.equal(buy.entryLimitPrice, PRICE - 1.5 * ATR);
    assert.equal(buy.dropEntry, false);
    assert.ok(buy.notes.includes('limit_clamped_max_atr'));
    const sell = run('SELL', 110);
    assert.equal(sell.entryLimitPrice, PRICE + 1.5 * ATR);
});

test('valid limits keep the entry', () => {
    const out = run('BUY', 99);
    assert.equal(out.dropEntry, false);
});

test('in-position and non-entry actions never emit a limit and never drop', () => {
    const hold = run('HOLD', 99);
    assert.equal(hold.entryLimitPrice, null);
    assert.equal(hold.dropEntry, false);
    const inPos = sanitizeEntryLimit({
        action: 'BUY',
        positionOpen: true,
        price: PRICE,
        primaryAtr: ATR,
        entryLimitPrice: 99,
    });
    assert.equal(inPos.entryLimitPrice, null);
    assert.equal(inPos.dropEntry, false);
});

test('missing ATR cannot validate the limit → entry dropped', () => {
    const noAtr = run('BUY', 99, { primaryAtr: null });
    assert.equal(noAtr.entryLimitPrice, null);
    assert.equal(noAtr.dropEntry, true);
    assert.ok(noAtr.notes.includes('limit_no_atr_entry_dropped'));
});
