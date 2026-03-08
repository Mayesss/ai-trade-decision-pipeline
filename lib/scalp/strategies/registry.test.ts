import assert from 'node:assert/strict';
import test from 'node:test';

import { COMPRESSION_BREAKOUT_PULLBACK_M15_M3_STRATEGY_ID } from './compressionBreakoutPullbackM15M3';
import { FAILED_AUCTION_EXTREME_REVERSAL_M15_M1_STRATEGY_ID } from './failedAuctionExtremeReversalM15M1';
import { HSS_ICT_M15_M3_GUARDED_STRATEGY_ID } from './hssIctM15M3Guarded';
import { OPENING_RANGE_BREAKOUT_RETEST_M5_M1_STRATEGY_ID } from './openingRangeBreakoutRetestM5M1';
import { PDH_PDL_RECLAIM_M15_M3_STRATEGY_ID } from './pdhPdlReclaimM15M3';
import { REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID } from './regimePullbackM15M3BtcusdtGuarded';
import { REGIME_PULLBACK_M15_M3_STRATEGY_ID } from './regimePullbackM15M3';
import { REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID } from './regimePullbackM15M3XauusdGuarded';
import { TREND_DAY_REACCELERATION_M15_M3_STRATEGY_ID } from './trendDayReaccelerationM15M3';
import {
    DEFAULT_SCALP_STRATEGY_ID,
    getDefaultScalpStrategy,
    getScalpStrategyById,
    listScalpStrategies,
    resolveScalpStrategyIdAlias,
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
    assert.ok(all.length >= 7, 'expected at least seven registered strategies');
    const ids = all.map((row) => row.id);
    assert.ok(ids.includes(REGIME_PULLBACK_M15_M3_STRATEGY_ID), 'expected default strategy in registry');
    assert.ok(!ids.includes(REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID), 'expected BTCUSDT guarded alias removed from registry');
    assert.ok(!ids.includes(REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID), 'expected XAUUSD guarded alias removed from registry');
    assert.ok(ids.includes(HSS_ICT_M15_M3_GUARDED_STRATEGY_ID), 'expected guarded HSS-ICT strategy in registry');
    assert.ok(ids.includes(OPENING_RANGE_BREAKOUT_RETEST_M5_M1_STRATEGY_ID), 'expected opening range breakout strategy in registry');
    assert.ok(ids.includes(PDH_PDL_RECLAIM_M15_M3_STRATEGY_ID), 'expected PDH/PDL reclaim strategy in registry');
    assert.ok(ids.includes(COMPRESSION_BREAKOUT_PULLBACK_M15_M3_STRATEGY_ID), 'expected compression breakout strategy in registry');
    assert.ok(ids.includes(FAILED_AUCTION_EXTREME_REVERSAL_M15_M1_STRATEGY_ID), 'expected failed auction reversal strategy in registry');
    assert.ok(ids.includes(TREND_DAY_REACCELERATION_M15_M3_STRATEGY_ID), 'expected trend-day reacceleration strategy in registry');
    const uniqueIds = new Set(ids);
    assert.equal(ids.length, uniqueIds.size, 'strategy IDs must be unique');
    for (const id of ids) {
        const resolved = getScalpStrategyById(id);
        assert.ok(resolved, `expected strategy lookup for ${id}`);
        assert.equal(resolved!.id, id);
    }
    assert.equal(getScalpStrategyById('does_not_exist'), null);
    assert.equal(resolveScalpStrategyIdAlias(REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID), REGIME_PULLBACK_M15_M3_STRATEGY_ID);
    assert.equal(resolveScalpStrategyIdAlias(REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID), REGIME_PULLBACK_M15_M3_STRATEGY_ID);
    assert.equal(getScalpStrategyById(REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID)?.id, REGIME_PULLBACK_M15_M3_STRATEGY_ID);
    assert.equal(getScalpStrategyById(REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID)?.id, REGIME_PULLBACK_M15_M3_STRATEGY_ID);
});

test('symbol strategy resolution uses explicit fallback and defaults otherwise', () => {
    assert.equal(
        resolveScalpStrategyIdForSymbol({
            symbol: 'XAUUSD',
            fallbackStrategyId: REGIME_PULLBACK_M15_M3_XAUUSD_STRATEGY_ID,
        }),
        REGIME_PULLBACK_M15_M3_STRATEGY_ID,
    );
    assert.equal(
        resolveScalpStrategyIdForSymbol({
            symbol: 'BTCUSDT',
            fallbackStrategyId: REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID,
        }),
        REGIME_PULLBACK_M15_M3_STRATEGY_ID,
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
