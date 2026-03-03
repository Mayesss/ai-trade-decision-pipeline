import assert from 'node:assert/strict';
import test from 'node:test';

import { HSS_ICT_M15_M3_GUARDED_STRATEGY_ID } from './hssIctM15M3Guarded';
import { HSS_ICT_M15_M3_STRATEGY_ID } from './hssIctM15M3';
import { DEFAULT_SCALP_STRATEGY_ID, getDefaultScalpStrategy, getScalpStrategyById, listScalpStrategies } from './registry';

test('default scalp strategy is registered and stable', () => {
    const strategy = getDefaultScalpStrategy();
    assert.equal(DEFAULT_SCALP_STRATEGY_ID, HSS_ICT_M15_M3_STRATEGY_ID);
    assert.equal(strategy.id, HSS_ICT_M15_M3_STRATEGY_ID);
    assert.equal(strategy.shortName, 'HSS-ICT M15/M3');
    assert.equal(strategy.longName, 'Hybrid Session-Scoped ICT Scalp (M15/M3)');
});

test('strategy registry exposes unique ids and supports lookup', () => {
    const all = listScalpStrategies();
    assert.ok(all.length >= 2, 'expected at least two registered strategies');
    const ids = all.map((row) => row.id);
    assert.ok(ids.includes(HSS_ICT_M15_M3_STRATEGY_ID), 'expected default strategy in registry');
    assert.ok(ids.includes(HSS_ICT_M15_M3_GUARDED_STRATEGY_ID), 'expected guarded strategy in registry');
    const uniqueIds = new Set(ids);
    assert.equal(ids.length, uniqueIds.size, 'strategy IDs must be unique');
    for (const id of ids) {
        const resolved = getScalpStrategyById(id);
        assert.ok(resolved, `expected strategy lookup for ${id}`);
        assert.equal(resolved!.id, id);
    }
    assert.equal(getScalpStrategyById('does_not_exist'), null);
});
