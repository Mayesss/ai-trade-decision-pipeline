import assert from 'node:assert/strict';
import test from 'node:test';

import {
    hasActiveEventWindow,
    isOppositePermission,
    shouldInvalidateByStop,
    shouldInvalidateFallback,
} from './engine';
import type { ForexPositionContext, ForexRegimePacket } from './types';

const basePacket: ForexRegimePacket = {
    pair: 'EURUSD',
    generatedAtMs: 1,
    regime: 'trend_up',
    permission: 'both',
    allowed_modules: ['pullback'],
    risk_state: 'normal',
    confidence: 0.7,
    htf_context: {
        nearest_support: 1.09,
        nearest_resistance: 1.12,
        distance_to_support_atr1h: 0.8,
        distance_to_resistance_atr1h: 1.1,
    },
    notes_codes: [],
};

const baseContext: ForexPositionContext = {
    pair: 'EURUSD',
    side: 'BUY',
    entryModule: 'pullback',
    module: 'pullback',
    entryPrice: 1.1,
    initialStopPrice: 1.095,
    currentStopPrice: 1.095,
    initialRiskPrice: 0.005,
    partialTakenPct: 0,
    trailingActive: false,
    trailingMode: 'none',
    tp1Price: null,
    tp2Price: null,
    stopPrice: 1.095,
    openedAtMs: 1,
    lastManagedAtMs: 1,
    lastCloseAtMs: null,
    updatedAtMs: 1,
    packet: basePacket,
};

test('shouldInvalidateByStop invalidates long when mid price is below stop', () => {
    const out = shouldInvalidateByStop({
        context: baseContext,
        openSide: 'BUY',
        midPrice: 1.0949,
    });

    assert.equal(out.invalidated, true);
    assert.equal(out.reasonCode, 'STOP_INVALIDATED_LONG');
});

test('shouldInvalidateByStop invalidates short when mid price is above stop', () => {
    const out = shouldInvalidateByStop({
        context: { ...baseContext, side: 'SELL', stopPrice: 1.105 },
        openSide: 'SELL',
        midPrice: 1.106,
    });

    assert.equal(out.invalidated, true);
    assert.equal(out.reasonCode, 'STOP_INVALIDATED_SHORT');
});

test('shouldInvalidateFallback invalidates long on down trend with short/flat permission', () => {
    const out = shouldInvalidateFallback({
        openSide: 'BUY',
        trendDirection1h: 'down',
        packet: { ...basePacket, permission: 'short_only' },
    });

    assert.equal(out.invalidated, true);
    assert.equal(out.reasonCode, 'STRUCTURE_INVALIDATION_FALLBACK_LONG');
});

test('isOppositePermission matches long-vs-short and short-vs-long', () => {
    assert.equal(isOppositePermission('BUY', 'short_only'), true);
    assert.equal(isOppositePermission('SELL', 'long_only'), true);
    assert.equal(isOppositePermission('BUY', 'both'), false);
});

test('hasActiveEventWindow is true only for active gate reason code', () => {
    assert.equal(hasActiveEventWindow(['EVENT_WINDOW_CLEAR']), false);
    assert.equal(hasActiveEventWindow(['EVENT_WINDOW_ACTIVE_BLOCK']), true);
});
