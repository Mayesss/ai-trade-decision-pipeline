import assert from 'node:assert/strict';
import test from 'node:test';

import { buildNextUniverseWithChurnCaps, resolveRecommendedStrategiesForSymbol } from '../symbolDiscovery';

test('resolveRecommendedStrategiesForSymbol maps BTC/XAU/FX symbols to expected strategy families', () => {
    const allowlist = [
        'compression_breakout_pullback_m15_m3',
        'regime_pullback_m15_m3',
        'trend_day_reacceleration_m15_m3',
        'pdh_pdl_reclaim_m15_m3',
    ];

    assert.deepEqual(resolveRecommendedStrategiesForSymbol('BTCUSDT', allowlist), [
        'regime_pullback_m15_m3',
        'compression_breakout_pullback_m15_m3',
    ]);
    assert.deepEqual(resolveRecommendedStrategiesForSymbol('XAUUSDT', allowlist), [
        'regime_pullback_m15_m3',
        'trend_day_reacceleration_m15_m3',
    ]);
    assert.deepEqual(resolveRecommendedStrategiesForSymbol('EURUSD', allowlist), [
        'regime_pullback_m15_m3',
        'pdh_pdl_reclaim_m15_m3',
    ]);
});

test('buildNextUniverseWithChurnCaps limits weekly adds/removes and preserves pinned symbols', () => {
    const policy = {
        version: 1 as const,
        updatedAt: null,
        notes: null,
        limits: {
            maxUniverseSymbols: 4,
            minUniverseSymbols: 2,
            maxWeeklyAdds: 2,
            maxWeeklyRemoves: 1,
            maxCandidates: 10,
        },
        criteria: {
            minHistoryDays: 1,
            minHistoryCoveragePct: 1,
            minAvgBarsPerDay: 1,
            minRecentBars7d: 1,
            minMedianRangePct: 0,
            maxSpreadPips: null,
            requireTradableQuote: false,
        },
        sources: {
            includeCapitalMarketsApi: true,
            includeCapitalTickerMap: true,
            includeDeploymentSymbols: true,
            includeHistorySymbols: true,
            requireHistoryPresence: true,
            explicitSymbols: [],
            excludedSymbols: [],
        },
        pinnedSymbols: ['XAUUSDT'],
        strategyAllowlist: [],
    };

    const candidateRows = [
        { symbol: 'E', eligible: true, score: 99, reasons: [], recommendedStrategyIds: [], metrics: {} as any },
        { symbol: 'F', eligible: true, score: 98, reasons: [], recommendedStrategyIds: [], metrics: {} as any },
        { symbol: 'B', eligible: true, score: 97, reasons: [], recommendedStrategyIds: [], metrics: {} as any },
        { symbol: 'A', eligible: true, score: 96, reasons: [], recommendedStrategyIds: [], metrics: {} as any },
    ];

    const out = buildNextUniverseWithChurnCaps({
        previousSymbols: ['A', 'B', 'C', 'D'],
        candidateRows: candidateRows as any,
        policy,
        pinnedSymbols: ['XAUUSDT'],
    });

    assert.deepEqual(out.selectedSymbols, ['A', 'B', 'E', 'XAUUSDT']);
    assert.deepEqual(out.addedSymbols, ['E', 'XAUUSDT']);
    assert.deepEqual(out.removedSymbols, ['C', 'D']);
});
