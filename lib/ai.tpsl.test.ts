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

test('entry BUY keeps a valid structural TP and a valid structural SL', () => {
    const out = entry('BUY', 104, 97);
    assert.equal(out.takeProfitPrice, 104);
    assert.equal(out.stopLossPrice, 97);
});

test('entry SL on the wrong side or inside 1 ATR is dropped (caller falls back to catastrophe stop)', () => {
    const wrongSide = entry('BUY', 104, 103); // SL above price on a long
    assert.equal(wrongSide.stopLossPrice, null);
    assert.ok(wrongSide.notes.includes('sl_wrong_side_dropped'));
    const tooClose = entry('BUY', 104, 98.5); // 0.75 ATR below — inside swing noise
    assert.equal(tooClose.stopLossPrice, null);
    assert.ok(tooClose.notes.includes('sl_too_close_dropped'));
    const atFloor = entry('BUY', 104, 98); // exactly 1 ATR — kept
    assert.equal(atFloor.stopLossPrice, 98);
});

test('entry SL beyond 3 ATR is clamped to the catastrophe distance', () => {
    const out = entry('SELL', 95, 110); // 5 ATR above on a short
    assert.equal(out.stopLossPrice, PRICE + 3 * ATR);
    assert.ok(out.notes.includes('sl_clamped_max_atr'));
});

test('entry without a TP falls back to 3×ATR on the profit side', () => {
    assert.equal(entry('BUY', null).takeProfitPrice, PRICE + 3 * ATR);
    assert.equal(entry('SELL', null).takeProfitPrice, PRICE - 3 * ATR);
});

test('entry TP on the wrong side or inside 2 ATR is replaced by the fallback', () => {
    const wrongSide = entry('BUY', 95);
    assert.ok(wrongSide.notes.includes('tp_wrong_side_dropped'));
    assert.equal(wrongSide.takeProfitPrice, PRICE + 3 * ATR);
    const tooClose = entry('SELL', 97); // 1.5 ATR away — not a swing target
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

test('stop amendments only tighten vs the standing stop', () => {
    // Long with standing stop 96: 97 tightens (kept), 94 loosens (dropped).
    const tighten = amend('long', null, 97, { standingStopLossPrice: 96 });
    assert.equal(tighten.stopLossPrice, 97);
    const loosen = amend('long', null, 94.5, { standingStopLossPrice: 96 });
    assert.equal(loosen.stopLossPrice, null);
    assert.ok(loosen.notes.includes('sl_loosened_dropped'));
    // Short with standing stop 104: 103 tightens, 105 loosens.
    const tightenShort = amend('short', null, 103, { standingStopLossPrice: 104 });
    assert.equal(tightenShort.stopLossPrice, 103);
    const loosenShort = amend('short', null, 105, { standingStopLossPrice: 104 });
    assert.equal(loosenShort.stopLossPrice, null);
    assert.ok(loosenShort.notes.includes('sl_loosened_dropped'));
    // No standing stop → any protective level within the clamp is allowed.
    const noStanding = amend('long', null, 94.5);
    assert.equal(noStanding.stopLossPrice, 94.5);
});

test('amend applies on a partial CLOSE but not on a full CLOSE', () => {
    const partial = amend('long', 105, 96, { action: 'CLOSE', exitSizePct: 40 });
    assert.equal(partial.takeProfitPrice, 105);
    assert.equal(partial.stopLossPrice, 96);
    const full = amend('long', 105, 96, { action: 'CLOSE', exitSizePct: 100 });
    assert.equal(full.takeProfitPrice, null);
    assert.equal(full.stopLossPrice, null);
});

test('REVERSE is an entry for the opposite side: TP and SL validated for the NEW side', () => {
    // Reversing a LONG → new SHORT: TP below price, SL above price.
    const good = sanitizeExchangeTpSl({
        action: 'REVERSE',
        positionOpen: true,
        side: 'long',
        price: PRICE,
        primaryAtr: ATR,
        takeProfitPrice: 95,
        stopLossPrice: 104, // 2 ATR above — valid structural stop for the new short
    });
    assert.equal(good.takeProfitPrice, 95);
    assert.equal(good.stopLossPrice, 104);
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
