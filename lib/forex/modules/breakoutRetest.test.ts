import assert from 'node:assert/strict';
import test from 'node:test';

import { evaluateBreakoutRetestModule } from './breakoutRetest';
import type { ForexPairMetrics, ForexRegimePacket } from '../types';

function makeCandle(index: number, open: number, high: number, low: number, close: number) {
    return [index, open, high, low, close, 1000];
}

function baseCandles() {
    const candles: any[] = [];
    for (let i = 0; i < 70; i += 1) {
        const open = i % 2 === 0 ? 1.1002 : 1.0998;
        const close = i % 2 === 0 ? 1.1000 : 1.1001;
        candles.push(makeCandle(i, open, 1.1010, 1.0990, close));
    }
    return candles;
}

function makePacket(overrides: Record<string, any> = {}): ForexRegimePacket {
    return {
        pair: 'EURUSD',
        generatedAtMs: Date.now(),
        regime: 'trend_up',
        permission: 'both',
        allowed_modules: ['breakout_retest'],
        risk_state: 'normal',
        confidence: 0.7,
        htf_context: {
            nearest_support: null,
            nearest_resistance: null,
            distance_to_support_atr1h: null,
            distance_to_resistance_atr1h: null,
        },
        notes_codes: [],
        ...overrides,
    };
}

function makeMetrics(overrides: Record<string, any> = {}): ForexPairMetrics {
    return {
        pair: 'EURUSD',
        epic: 'EURUSD',
        sessionTag: 'LONDON',
        price: 1.1,
        spreadAbs: 0.0001,
        spreadPips: 1,
        spreadToAtr1h: 0.05,
        atr1h: 0.001,
        atr4h: 0.002,
        atr1hPercent: 0.001,
        trendStrength: 0.8,
        chopScore: 0.25,
        shockFlag: false,
        timestampMs: Date.now(),
        ...overrides,
    };
}

function makeMarket(candles: any[], overrides: Record<string, any> = {}) {
    return {
        pair: 'EURUSD',
        epic: 'EURUSD',
        nowMs: Date.now(),
        sessionTag: 'LONDON',
        price: 1.1,
        bid: 1.0999,
        offer: 1.1,
        spreadAbs: 0.0001,
        spreadPips: 1,
        atr5m: 0.0002,
        atr1h: 0.001,
        atr4h: 0.002,
        atr1hPercent: 0.001,
        spreadToAtr1h: 0.05,
        trendDirection1h: 'up',
        trendStrength1h: 0.8,
        chopScore1h: 0.25,
        shockFlag: false,
        nearestSupport: null,
        nearestResistance: null,
        distanceToSupportAtr1h: null,
        distanceToResistanceAtr1h: null,
        candles: {
            m5: candles,
            m15: candles,
            h1: candles,
            h4: candles,
            d1: candles,
        },
        ...overrides,
    } as any;
}

test('breakout-retest returns long signal only after breakout + retest + confirmation', () => {
    const candles = baseCandles();
    candles[candles.length - 3] = makeCandle(67, 1.1009, 1.10145, 1.10095, 1.1014);
    candles[candles.length - 2] = makeCandle(68, 1.10135, 1.10138, 1.10102, 1.10122);
    candles[candles.length - 1] = makeCandle(69, 1.1012, 1.10155, 1.10108, 1.1014);

    const out = evaluateBreakoutRetestModule({
        pair: 'EURUSD',
        packet: makePacket({ regime: 'trend_up', permission: 'both' }),
        market: makeMarket(candles, { trendDirection1h: 'up' }),
        metrics: makeMetrics(),
    });

    assert.ok(out);
    assert.equal(out?.side, 'BUY');
    assert.ok(out?.reasonCodes.includes('MODULE_BREAKOUT_RETEST_LONG_TWO_STEP_CONFIRMED'));
});

test('breakout-retest does not signal when continuation confirmation is missing', () => {
    const candles = baseCandles();
    candles[candles.length - 3] = makeCandle(67, 1.1009, 1.10145, 1.10095, 1.1014);
    candles[candles.length - 2] = makeCandle(68, 1.10135, 1.10138, 1.10102, 1.10122);
    candles[candles.length - 1] = makeCandle(69, 1.1012, 1.10128, 1.10098, 1.1011);

    const out = evaluateBreakoutRetestModule({
        pair: 'EURUSD',
        packet: makePacket({ regime: 'trend_up', permission: 'both' }),
        market: makeMarket(candles, { trendDirection1h: 'up' }),
        metrics: makeMetrics(),
    });

    assert.equal(out, null);
});

test('breakout-retest returns short signal only after breakout + retest + confirmation', () => {
    const candles = baseCandles();
    candles[candles.length - 3] = makeCandle(67, 1.0991, 1.09915, 1.09855, 1.0986);
    candles[candles.length - 2] = makeCandle(68, 1.0987, 1.09898, 1.09862, 1.0988);
    candles[candles.length - 1] = makeCandle(69, 1.09882, 1.0989, 1.0984, 1.0986);

    const out = evaluateBreakoutRetestModule({
        pair: 'EURUSD',
        packet: makePacket({ regime: 'trend_down', permission: 'both' }),
        market: makeMarket(candles, { trendDirection1h: 'down' }),
        metrics: makeMetrics(),
    });

    assert.ok(out);
    assert.equal(out?.side, 'SELL');
    assert.ok(out?.reasonCodes.includes('MODULE_BREAKOUT_RETEST_SHORT_TWO_STEP_CONFIRMED'));
});
