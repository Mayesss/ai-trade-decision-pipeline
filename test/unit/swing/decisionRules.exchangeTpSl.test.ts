import assert from 'node:assert/strict';
import { test } from 'vitest';

import { sanitizeExchangeTpSl } from '../../../lib/swing/decisionRules';

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

test('entry SL on the wrong side is dropped; a sub-1-ATR stop is now kept', () => {
    const wrongSide = entry('BUY', 104, 101);
    assert.ok(wrongSide.notes.includes('sl_wrong_side_dropped'));
    assert.equal(wrongSide.stopLossPrice, null);
    const tight = entry('BUY', 104, 99); // 0.5 ATR — previously dropped
    assert.equal(tight.stopLossPrice, 99);
    assert.ok(!tight.notes.includes('sl_too_close_dropped'));
    const noise = entry('BUY', 104, 99.95); // 0.025 ATR — under BRACKET_MIN_GAP_ATR
    assert.ok(noise.notes.includes('sl_too_close_dropped'));
    assert.equal(noise.stopLossPrice, null);
});

test('entry without a TP falls back to 3×ATR on the profit side', () => {
    assert.equal(entry('BUY', null).takeProfitPrice, PRICE + 3 * ATR);
    assert.equal(entry('SELL', null).takeProfitPrice, PRICE - 3 * ATR);
});

test('entry TP on the wrong side is replaced by the fallback', () => {
    const wrongSide = entry('BUY', 95);
    assert.ok(wrongSide.notes.includes('tp_wrong_side_dropped'));
    assert.equal(wrongSide.takeProfitPrice, PRICE + 3 * ATR);
});

// Floors removed 2026-09-02 (ENTRY_TP_MIN_ATR = 2, ENTRY_SL_MIN_ATR = 1): they
// made any trade inside a sub-2-ATR range inexpressible, and a violation
// silently WIDENED the target to the 3×ATR fallback. Bracket geometry is the
// model's now. Anchor for every case below: PRICE 100, ATR 2.
test('the tight-stop range trade is expressible — both legs kept as asked', () => {
    const out = entry('BUY', 102.4, 99.2); // stop 0.4 ATR, target 1.2 ATR = 3R
    assert.equal(out.stopLossPrice, 99.2);
    assert.equal(out.takeProfitPrice, 102.4);
    assert.equal(out.notes.length, 0);
});

test('a sub-2-ATR target is kept — no minimum distance', () => {
    const near = entry('SELL', 97, 102.5); // 1.5 ATR target, 1.25 ATR stop
    assert.equal(near.takeProfitPrice, 97);
    assert.equal(near.stopLossPrice, 102.5);
    assert.equal(near.notes.length, 0);
});

test('a target shorter than its stop is the model\'s call, not an error', () => {
    // 0.2 ATR target on a 1.5 ATR stop = 0.13R. Sub-1R is not a rejected shape:
    // no reward:risk ratio is enforced anywhere, so both legs must survive
    // untouched and the sanitizer must stay silent about it.
    const out = entry('BUY', 100.4, 97);
    assert.equal(out.takeProfitPrice, 100.4);
    assert.equal(out.stopLossPrice, 97);
    assert.equal(out.notes.length, 0);
});

test('entry TP inside the noise gap is still dropped to the fallback', () => {
    const noise = entry('BUY', 100.1); // 0.05 ATR — under BRACKET_MIN_GAP_ATR
    assert.ok(noise.notes.includes('tp_too_close_dropped'));
    assert.equal(noise.takeProfitPrice, PRICE + 3 * ATR);
});

// The anchor above (ATR = 2% of price) is a HIGH-ATR instrument, where the ATR
// floor is the binding one. These cases use a low-ATR anchor, where it is not.
test('on a low-ATR instrument the floor is the sizing threshold, not 0.1 ATR', () => {
    // EURUSD-shaped: price 1.0850, 4H ATR ≈ 0.355% of price. 0.1 ATR ≈ 0.036%
    // of price — under MIN_SIZEABLE_STOP_PCT (0.05%). A stop in that band used
    // to survive here and then make resolveRiskBasedSizing return null, which
    // dropped execution to the legacy stop-blind notional.
    const price = 1.085;
    const atr = 0.00385; // 0.355% of price
    const sanitize = (sl: number) =>
        sanitizeExchangeTpSl({
            action: 'BUY',
            positionOpen: false,
            side: null,
            price,
            primaryAtr: atr,
            takeProfitPrice: null,
            stopLossPrice: sl,
        });

    // 0.12 ATR — clears the ATR floor, still unsizeable (0.043% of entry).
    const unsizeable = sanitize(price - 0.12 * atr);
    assert.equal(unsizeable.stopLossPrice, null);
    assert.ok(unsizeable.notes.includes('sl_below_sizeable_dropped'));

    // 0.2 ATR = 0.071% of entry — over both floors, kept as asked.
    const ok = price - 0.2 * atr;
    assert.equal(sanitize(ok).stopLossPrice, ok);
});

test('on a high-ATR instrument the ATR floor still binds and keeps its own note', () => {
    // Anchor case: 0.05 ATR = 0.1% of price, sizeable but under 0.1 ATR.
    const noise = entry('BUY', 104, PRICE - 0.05 * ATR);
    assert.equal(noise.stopLossPrice, null);
    assert.ok(noise.notes.includes('sl_too_close_dropped'));
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
