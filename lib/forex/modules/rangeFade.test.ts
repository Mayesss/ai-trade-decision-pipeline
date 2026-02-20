import assert from 'node:assert/strict';
import test from 'node:test';

import { evaluateRangeFadeModule } from './rangeFade';
import type { ForexPairMetrics, ForexRegimePacket } from '../types';

function makeCandle(index: number, open: number, high: number, low: number, close: number) {
    return [index, open, high, low, close, 1000];
}

function baseCandles() {
    const candles: any[] = [];
    for (let i = 0; i < 60; i += 1) {
        const open = 1.1 + (i % 2 === 0 ? 0.0002 : -0.0002);
        const close = 1.1 + (i % 2 === 0 ? -0.0001 : 0.0001);
        const high = 1.101;
        const low = 1.099;
        candles.push(makeCandle(i, open, high, low, close));
    }
    return candles;
}

function makePacket(overrides: Record<string, any> = {}): ForexRegimePacket {
    return {
        pair: 'EURUSD',
        generatedAtMs: Date.now(),
        regime: 'range',
        permission: 'both',
        allowed_modules: ['range_fade'],
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
        trendStrength: 0.5,
        chopScore: 0.5,
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
        trendDirection1h: 'neutral',
        trendStrength1h: 0.5,
        chopScore1h: 0.5,
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

test('range fade returns short signal when upper boundary rejects', () => {
    const candles = baseCandles();
    candles[candles.length - 2] = makeCandle(58, 1.1008, 1.101, 1.099, 1.101);
    candles[candles.length - 1] = makeCandle(59, 1.101, 1.10103, 1.1008, 1.1009);

    const out = evaluateRangeFadeModule({
        pair: 'EURUSD',
        packet: makePacket(),
        market: makeMarket(candles),
        metrics: makeMetrics(),
    });

    assert.equal(out.killSwitchTriggered, false);
    assert.ok(out.signal);
    assert.equal(out.signal?.module, 'range_fade');
    assert.equal(out.signal?.side, 'SELL');
});

test('range fade triggers kill switch on breakout candle', () => {
    const candles = baseCandles();
    candles[candles.length - 1] = makeCandle(59, 1.1012, 1.102, 1.1008, 1.1018);

    const out = evaluateRangeFadeModule({
        pair: 'EURUSD',
        packet: makePacket(),
        market: makeMarket(candles),
        metrics: makeMetrics(),
    });

    assert.equal(out.signal, null);
    assert.equal(out.killSwitchTriggered, true);
    assert.ok(out.reasonCodes.includes('MODULE_RANGE_FADE_BREAKOUT_KILL_SWITCH'));
});
