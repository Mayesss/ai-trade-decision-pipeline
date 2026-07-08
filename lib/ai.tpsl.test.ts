import assert from 'node:assert/strict';
import test from 'node:test';

import { sanitizeExchangeTpSl } from './ai';

// Anchor: price 100, primary ATR 2. Entry fallback TP = 3×ATR = ±6.
const PRICE = 100;
const ATR = 2;

const entry = (action: 'BUY' | 'SELL', tp: number | null, sl: number | null = null) =>
    sanitizeExchangeTpSl({
        action,
        positionOpen: false,
        side: null,
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: tp,
        stopLossPrice: sl,
    });

const amend = (
    side: 'long' | 'short',
    tp: number | null,
    sl: number | null,
    overrides: Partial<Parameters<typeof sanitizeExchangeTpSl>[0]> = {},
) =>
    sanitizeExchangeTpSl({
        action: 'HOLD',
        positionOpen: true,
        side,
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: tp,
        stopLossPrice: sl,
        ...overrides,
    });

test('entry BUY keeps a valid structural TP and nulls the SL (catastrophe stop is code-owned)', () => {
    const out = entry('BUY', 104, 97);
    assert.equal(out.takeProfitPrice, 104);
    assert.equal(out.stopLossPrice, null);
});

test('entry without a TP falls back to 3×ATR on the profit side', () => {
    assert.equal(entry('BUY', null).takeProfitPrice, PRICE + 3 * ATR);
    assert.equal(entry('SELL', null).takeProfitPrice, PRICE - 3 * ATR);
});

test('entry TP on the wrong side or inside 0.5 ATR is replaced by the fallback', () => {
    const wrongSide = entry('BUY', 95);
    assert.ok(wrongSide.notes.includes('tp_wrong_side_dropped'));
    assert.equal(wrongSide.takeProfitPrice, PRICE + 3 * ATR);
    const tooClose = entry('SELL', 99.5); // 0.25 ATR away
    assert.ok(tooClose.notes.includes('tp_too_close_dropped'));
    assert.equal(tooClose.takeProfitPrice, PRICE - 3 * ATR);
});

test('entry TP beyond 10 ATR is clamped', () => {
    const out = entry('BUY', 200);
    assert.equal(out.takeProfitPrice, PRICE + 10 * ATR);
    assert.ok(out.notes.includes('tp_clamped_max_atr'));
});

test('in-position amend keeps valid TP/SL on the correct sides', () => {
    const long = amend('long', 105, 96);
    assert.equal(long.takeProfitPrice, 105);
    assert.equal(long.stopLossPrice, 96);
    const short = amend('short', 95, 104);
    assert.equal(short.takeProfitPrice, 95);
    assert.equal(short.stopLossPrice, 104);
});

test('in-position amend drops wrong-side legs independently', () => {
    const out = amend('long', 95, 105); // TP below price, SL above price — both invalid for a long
    assert.equal(out.takeProfitPrice, null);
    assert.equal(out.stopLossPrice, null);
    assert.ok(out.notes.includes('tp_wrong_side_dropped'));
    assert.ok(out.notes.includes('sl_wrong_side_dropped'));
});

test('stop amendments are clamped to the 3×ATR catastrophe distance from current price', () => {
    const long = amend('long', null, 80); // 10 ATR away → clamp to 100 - 6
    assert.equal(long.stopLossPrice, PRICE - 3 * ATR);
    assert.ok(long.notes.includes('sl_clamped_max_atr'));
    const short = amend('short', null, 120);
    assert.equal(short.stopLossPrice, PRICE + 3 * ATR);
});

test('amend applies on a partial CLOSE but not on a full CLOSE', () => {
    const partial = amend('long', 105, 96, { action: 'CLOSE', exitSizePct: 40 });
    assert.equal(partial.takeProfitPrice, 105);
    assert.equal(partial.stopLossPrice, 96);
    const full = amend('long', 105, 96, { action: 'CLOSE', exitSizePct: 100 });
    assert.equal(full.takeProfitPrice, null);
    assert.equal(full.stopLossPrice, null);
});

test('REVERSE is an entry for the opposite side: TP validated for the NEW side, SL stays code-owned', () => {
    // Reversing a LONG → new SHORT: TP must be below price.
    const good = sanitizeExchangeTpSl({
        action: 'REVERSE',
        positionOpen: true,
        side: 'long',
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: 95,
        stopLossPrice: 104, // ignored on entries — catastrophe stop is code-owned
    });
    assert.equal(good.takeProfitPrice, 95);
    assert.equal(good.stopLossPrice, null);
    // TP on the wrong side for the NEW short → fallback below price.
    const wrong = sanitizeExchangeTpSl({
        action: 'REVERSE',
        positionOpen: true,
        side: 'long',
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: 105,
        stopLossPrice: null,
    });
    assert.ok(wrong.notes.includes('tp_wrong_side_dropped'));
    assert.equal(wrong.takeProfitPrice, PRICE - 3 * ATR);
    // Reversing a SHORT → new LONG: no TP given → fallback above price.
    const fromShort = sanitizeExchangeTpSl({
        action: 'REVERSE',
        positionOpen: true,
        side: 'short',
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: null,
        stopLossPrice: null,
    });
    assert.equal(fromShort.takeProfitPrice, PRICE + 3 * ATR);
});

test('flat HOLD and unusable anchors produce no bracket', () => {
    const flatHold = sanitizeExchangeTpSl({
        action: 'HOLD',
        positionOpen: false,
        side: null,
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: 105,
        stopLossPrice: 95,
    });
    assert.deepEqual([flatHold.takeProfitPrice, flatHold.stopLossPrice], [null, null]);
    const noPrice = sanitizeExchangeTpSl({
        action: 'BUY',
        positionOpen: false,
        side: null,
        price: NaN,
        primaryAtr: ATR,
        takeProfitPrice: 105,
        stopLossPrice: null,
    });
    assert.deepEqual([noPrice.takeProfitPrice, noPrice.stopLossPrice], [null, null]);
});

test('entry without ATR keeps a side-valid TP but has no fallback', () => {
    const withTp = sanitizeExchangeTpSl({
        action: 'BUY',
        positionOpen: false,
        side: null,
        price: PRICE,
        primaryAtr: null,
        takeProfitPrice: 104,
        stopLossPrice: null,
    });
    assert.equal(withTp.takeProfitPrice, 104);
    const withoutTp = sanitizeExchangeTpSl({
        action: 'BUY',
        positionOpen: false,
        side: null,
        price: PRICE,
        primaryAtr: null,
        takeProfitPrice: null,
        stopLossPrice: null,
    });
    assert.equal(withoutTp.takeProfitPrice, null);
});
