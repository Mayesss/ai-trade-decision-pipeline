import assert from 'node:assert/strict';
import test from 'node:test';

import {
    buildNextUniverseWithChurnCaps,
    resolveCompletedWeekCoverageStartMs,
    resolveRecommendedStrategiesForSymbol,
    resolveRequiredHistoryDaysForCompletedWeeks,
    resolveSeedSymbolEligibility,
    summarizeSeedHistoryQuality,
} from '../symbolDiscovery';

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

test('resolveRecommendedStrategiesForSymbol supports broader asset classes when allowlist includes them', () => {
    const allowlist = [
        'compression_breakout_pullback_m15_m3',
        'regime_pullback_m15_m3',
        'hss_ict_m15_m3_guarded',
        'failed_auction_extreme_reversal_m15_m1',
        'trend_day_reacceleration_m15_m3',
        'pdh_pdl_reclaim_m15_m3',
    ];

    assert.deepEqual(resolveRecommendedStrategiesForSymbol('ETHUSDT', allowlist), [
        'regime_pullback_m15_m3',
        'compression_breakout_pullback_m15_m3',
        'failed_auction_extreme_reversal_m15_m1',
        'hss_ict_m15_m3_guarded',
    ]);
    assert.deepEqual(resolveRecommendedStrategiesForSymbol('US500USD', allowlist), [
        'trend_day_reacceleration_m15_m3',
        'regime_pullback_m15_m3',
        'failed_auction_extreme_reversal_m15_m1',
    ]);
    assert.deepEqual(resolveRecommendedStrategiesForSymbol('AAPL', allowlist), [
        'trend_day_reacceleration_m15_m3',
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

    assert.deepEqual(out.selectedSymbols, ['XAUUSDT', 'E', 'B', 'A']);
    assert.deepEqual(out.addedSymbols, ['XAUUSDT', 'E']);
    assert.deepEqual(out.removedSymbols, ['C', 'D']);
});

test('resolveSeedSymbolEligibility blocks bootstrap symbols when bootstrap seeding is disabled', () => {
    const nowMs = Date.UTC(2026, 2, 10, 12, 0, 0);
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
            minHistoryDays: 45,
            minHistoryCoveragePct: 80,
            minAvgBarsPerDay: 900,
            minRecentBars7d: 4000,
            minMedianRangePct: 0.025,
            maxSpreadPips: 35,
            requireTradableQuote: true,
        },
        sources: {
            includeCapitalMarketsApi: true,
            includeCapitalTickerMap: false,
            includeDeploymentSymbols: true,
            includeHistorySymbols: true,
            requireHistoryPresence: true,
            explicitSymbols: [],
            excludedSymbols: [],
        },
        pinnedSymbols: ['BTCUSDT'],
        strategyAllowlist: ['regime_pullback_m15_m3'],
    };

    const out = resolveSeedSymbolEligibility({
        policy,
        nowMs,
        candles: [],
        hasStrategyFit: true,
        allowBootstrapSymbols: false,
    });

    assert.equal(out.eligible, false);
    assert.equal(out.reason, 'seed_bootstrap_disabled');
});

test('resolveSeedSymbolEligibility enforces bars/day and recent-bars gates for seed stage', () => {
    const nowMs = Date.UTC(2026, 2, 10, 12, 0, 0);
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
            minHistoryDays: 45,
            minHistoryCoveragePct: 80,
            minAvgBarsPerDay: 900,
            minRecentBars7d: 4000,
            minMedianRangePct: 0.025,
            maxSpreadPips: 35,
            requireTradableQuote: true,
        },
        sources: {
            includeCapitalMarketsApi: true,
            includeCapitalTickerMap: false,
            includeDeploymentSymbols: true,
            includeHistorySymbols: true,
            requireHistoryPresence: true,
            explicitSymbols: [],
            excludedSymbols: [],
        },
        pinnedSymbols: ['BTCUSDT'],
        strategyAllowlist: ['regime_pullback_m15_m3'],
    };

    const sparseCandles: Array<[number, number, number, number, number, number]> = [];
    const twoDaysAgo = nowMs - 2 * 24 * 60 * 60 * 1000;
    for (let i = 0; i < 120; i += 1) {
        const ts = twoDaysAgo + i * 30 * 60 * 1000; // sparse: 2 bars/hour
        sparseCandles.push([ts, 1, 1.01, 0.99, 1, 0]);
    }

    const sparseOut = resolveSeedSymbolEligibility({
        policy,
        nowMs,
        candles: sparseCandles,
        hasStrategyFit: true,
        allowBootstrapSymbols: false,
    });
    assert.equal(sparseOut.eligible, false);
    assert.equal(sparseOut.reason, 'seed_avg_bars_per_day_below_min');

    const sparseBootstrapOut = resolveSeedSymbolEligibility({
        policy,
        nowMs,
        candles: sparseCandles,
        hasStrategyFit: true,
        allowBootstrapSymbols: true,
    });
    assert.equal(sparseBootstrapOut.eligible, true);
    assert.equal(sparseBootstrapOut.reason, null);

    const denseCandles: Array<[number, number, number, number, number, number]> = [];
    const fiveDaysAgo = nowMs - 5 * 24 * 60 * 60 * 1000;
    for (let i = 0; i < 5 * 24 * 60; i += 1) {
        const ts = fiveDaysAgo + i * 60 * 1000;
        denseCandles.push([ts, 1, 1.01, 0.99, 1, 0]);
    }
    const denseOut = resolveSeedSymbolEligibility({
        policy,
        nowMs,
        candles: denseCandles,
        hasStrategyFit: true,
        allowBootstrapSymbols: false,
    });
    assert.equal(denseOut.eligible, true);
    assert.equal(denseOut.reason, null);
});

test('summarizeSeedHistoryQuality reports span, lag, and activity metrics', () => {
    const nowMs = Date.UTC(2026, 2, 10, 12, 0, 0);
    const startMs = nowMs - 3 * 24 * 60 * 60 * 1000;
    const candles: Array<[number, number, number, number, number, number]> = [];
    for (let i = 0; i < 3 * 24 * 60; i += 1) {
        const ts = startMs + i * 60 * 1000;
        candles.push([ts, 1, 1.01, 0.99, 1, 0]);
    }

    const quality = summarizeSeedHistoryQuality(candles, nowMs);
    assert.ok((quality.spanDays ?? 0) > 2.9);
    assert.ok((quality.avgBarsPerDay ?? 0) > 1300);
    assert.ok((quality.recentBars7d ?? 0) >= candles.length);
    assert.ok((quality.lagHours ?? 999) < 1);
});

test('completed-week coverage helpers extend seed history beyond raw lookback days when needed', () => {
    const nowMs = Date.UTC(2026, 2, 10, 12, 0, 0); // Tuesday
    const coverageStartMs = resolveCompletedWeekCoverageStartMs(nowMs, 13);
    assert.equal(coverageStartMs, Date.UTC(2025, 11, 8, 0, 0, 0));

    const effectiveDays = resolveRequiredHistoryDaysForCompletedWeeks({
        nowMs,
        targetHistoryDays: 90,
        requiredSuccessiveWeeks: 13,
    });
    assert.equal(effectiveDays, 93);
});
