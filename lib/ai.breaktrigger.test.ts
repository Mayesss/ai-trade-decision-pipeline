import assert from 'node:assert/strict';
import test from 'node:test';

import { computeSwingState, sanitizeEntryTrigger } from './ai';

// ---- entry_trigger_price sanitation ----

test('sanitizeEntryTrigger: BUY keeps a trigger below price, SELL above', () => {
    assert.equal(
        sanitizeEntryTrigger({ action: 'BUY', positionOpen: false, price: 100, triggerPrice: 98 }).triggerPrice,
        98,
    );
    assert.equal(
        sanitizeEntryTrigger({ action: 'SELL', positionOpen: false, price: 100, triggerPrice: 102 }).triggerPrice,
        102,
    );
});

test('sanitizeEntryTrigger: wrong-side triggers are dropped with a note', () => {
    const buy = sanitizeEntryTrigger({ action: 'BUY', positionOpen: false, price: 100, triggerPrice: 101 });
    assert.equal(buy.triggerPrice, null);
    assert.ok(buy.notes.includes('entry_trigger_dropped_not_below_price'));
    const sell = sanitizeEntryTrigger({ action: 'SELL', positionOpen: false, price: 100, triggerPrice: 99 });
    assert.equal(sell.triggerPrice, null);
    assert.ok(sell.notes.includes('entry_trigger_dropped_not_above_price'));
});

test('sanitizeEntryTrigger: only flat BUY/SELL are eligible', () => {
    for (const params of [
        { action: 'HOLD', positionOpen: false },
        { action: 'REVERSE', positionOpen: true },
        { action: 'CLOSE', positionOpen: true },
        { action: 'BUY', positionOpen: true },
    ]) {
        const out = sanitizeEntryTrigger({ ...params, price: 100, triggerPrice: 98 });
        assert.equal(out.triggerPrice, null);
        assert.deepEqual(out.notes, []);
    }
});

test('sanitizeEntryTrigger: unknown price drops the trigger, junk values coerce to null', () => {
    const out = sanitizeEntryTrigger({ action: 'BUY', positionOpen: false, price: null, triggerPrice: 98 });
    assert.equal(out.triggerPrice, null);
    assert.ok(out.notes.includes('entry_trigger_dropped_price_unknown'));
    for (const triggerPrice of [null, undefined, 'the level', -5, 0, NaN]) {
        assert.equal(
            sanitizeEntryTrigger({ action: 'BUY', positionOpen: false, price: 100, triggerPrice }).triggerPrice,
            null,
        );
    }
});

// ---- market.failed_break prompt surfacing ----

const NOW_MS = 1_750_000_000_000;
const bundle = { ticker: [{ lastPr: '100', change24h: '0' }], candles: [] };
const indicators: any = {
    micro: '',
    macro: '',
    primary: { summary: '', timeframe: '4h' },
    context: { summary: '', timeframe: '1d' },
    microTimeFrame: '1h',
    macroTimeFrame: '1d',
    sr: {},
    rawCandles: {},
};
const momentum: any = { microExtensionInAtr: 0, info: { microEntryOk: false } };

const buildUserPrompt = (failedBreak: any) => {
    const state = computeSwingState(
        'BTCUSDT',
        '4h',
        bundle,
        {},
        'none',
        null,
        null,
        indicators,
        {},
        null,
        momentum,
        [],
        null,
        true,
        5,
        undefined,
        'crypto',
        'bitget',
        null,
        NOW_MS,
        null,
        null,
        failedBreak,
    );
    return state.assemble(null, []).user;
};

test('computeSwingState: a failed break surfaces as market.failed_break with bar age', () => {
    const user = buildUserPrompt({
        side: 'long',
        triggerPrice: 100,
        barClose: 99.2,
        barClosedAtMs: NOW_MS - 4 * 60_000,
    });
    const market = JSON.parse(
        user.slice(user.indexOf('MARKET (raw inputs):') + 'MARKET (raw inputs):'.length).split('\n\nTASKS:')[0],
    );
    assert.deepEqual(market.failed_break, {
        side: 'long',
        trigger_price: 100,
        bar_close: 99.2,
        bar_closed_minutes_ago: 4,
    });
});

test('computeSwingState: no failed break → market.failed_break absent', () => {
    const user = buildUserPrompt(null);
    assert.ok(!user.includes('failed_break'));
});
