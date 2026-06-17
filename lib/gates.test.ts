import assert from 'node:assert/strict';
import test from 'node:test';

import { computeAdaptiveGates, parseAtr1hAbs } from './gates';

test('parseAtr1hAbs prefers macro metrics ATR over rounded summary text', () => {
    const atr = parseAtr1hAbs({
        macro: 'VWAP=1.17, RSI=49.8, trend=up, ATR=0.00, EMA20=1.18',
        macroTimeFrame: '1D',
        metrics: {
            '1D': {
                atr: 0.0049275,
            },
        },
    } as any);
    assert.equal(atr, 0.0049275);
});

test('parseAtr1hAbs falls back to macro summary when metrics ATR is unavailable', () => {
    const atr = parseAtr1hAbs({
        macro: 'VWAP=1.17, RSI=49.8, trend=up, ATR=0.0049, EMA20=1.18',
        macroTimeFrame: '1D',
    } as any);
    assert.equal(atr, 0.0049);
});

test('computeAdaptiveGates supports atrFloorScale to avoid over-strict ATR floor for low-vol markets', () => {
    const baseInput = {
        symbol: 'EURUSD',
        last: 1.18,
        orderbook: {
            bids: [[1.1799, 5_000_000]] as [number, number][],
            asks: [[1.1801, 5_000_000]] as [number, number][],
        },
        notionalUSDT: 100,
        atrAbsMacro: 0.0048,
        macroTimeframeMinutes: 1440,
        regime: 'up' as const,
        positionOpen: false,
        disableSymbolExclusions: true,
    };

    const strict = computeAdaptiveGates(baseInput);
    const relaxed = computeAdaptiveGates({
        ...baseInput,
        atrFloorScale: 0.8,
    });

    assert.equal(strict.gates.atr_ok, false);
    assert.equal(relaxed.gates.atr_ok, true);
    assert.ok(relaxed.metrics.atrPctFloor < strict.metrics.atrPctFloor);
});

test('computeAdaptiveGates uses explicit forex ATR thresholds', () => {
    const baseInput = {
        symbol: 'USDJPY',
        last: 160.3,
        orderbook: {
            bids: [[160.29, 5_000_000]] as [number, number][],
            asks: [[160.33, 5_000_000]] as [number, number][],
        },
        notionalUSDT: 100,
        atrAbsMacro: 0.48,
        macroTimeframeMinutes: 1440,
        regime: 'up' as const,
        positionOpen: false,
        disableSymbolExclusions: true,
    };

    const generic = computeAdaptiveGates(baseInput);
    const forex = computeAdaptiveGates({
        ...baseInput,
        marketCategory: 'forex',
    });

    assert.equal(generic.gates.atr_ok, false);
    assert.equal(forex.gates.atr_ok, true);
    assert.equal(forex.gates.spread_ok, true);
    assert.equal(forex.gates.slippage_ok, true);
    assert.ok(forex.metrics.atrPctFloor < generic.metrics.atrPctFloor);
});
