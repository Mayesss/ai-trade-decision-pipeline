import assert from 'node:assert/strict';
import test from 'node:test';

import { REGIME_PULLBACK_M15_M3_STRATEGY_ID } from './regimePullbackM15M3';
import { REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID } from './regimePullbackM15M3XauusdGuarded';
import {
    DEFAULT_SCALP_STRATEGY_ID,
    getDefaultScalpStrategy,
    getScalpStrategyById,
    listScalpStrategies,
    resolveScalpStrategyIdForSymbol,
} from './registry';

test('default scalp strategy is registered and stable', () => {
    const strategy = getDefaultScalpStrategy();
    assert.equal(DEFAULT_SCALP_STRATEGY_ID, REGIME_PULLBACK_M15_M3_STRATEGY_ID);
    assert.equal(strategy.id, REGIME_PULLBACK_M15_M3_STRATEGY_ID);
    assert.equal(strategy.shortName, 'Regime Pullback');
});

test('strategy registry exposes unique ids and supports lookup', () => {
    const all = listScalpStrategies();
    assert.ok(all.length >= 2, 'expected at least two registered strategies');
    const ids = all.map((row) => row.id);
    assert.ok(ids.includes(REGIME_PULLBACK_M15_M3_STRATEGY_ID), 'expected default strategy in registry');
    assert.ok(ids.includes(REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID), 'expected XAUUSD guarded regime strategy in registry');
    const uniqueIds = new Set(ids);
    assert.equal(ids.length, uniqueIds.size, 'strategy IDs must be unique');
    for (const id of ids) {
        const resolved = getScalpStrategyById(id);
        assert.ok(resolved, `expected strategy lookup for ${id}`);
        assert.equal(resolved!.id, id);
    }
    assert.equal(getScalpStrategyById('does_not_exist'), null);
});

test('symbol strategy resolution uses explicit fallback and defaults otherwise', () => {
    assert.equal(
        resolveScalpStrategyIdForSymbol({
            symbol: 'XAUUSD',
            fallbackStrategyId: REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID,
        }),
        REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID,
    );
    assert.equal(
        resolveScalpStrategyIdForSymbol({
            symbol: 'EURUSD',
            fallbackStrategyId: REGIME_PULLBACK_M15_M3_STRATEGY_ID,
        }),
        REGIME_PULLBACK_M15_M3_STRATEGY_ID,
    );
    assert.equal(
        resolveScalpStrategyIdForSymbol({
            symbol: 'EURUSD',
            fallbackStrategyId: 'not_registered',
        }),
        REGIME_PULLBACK_M15_M3_STRATEGY_ID,
    );
});
