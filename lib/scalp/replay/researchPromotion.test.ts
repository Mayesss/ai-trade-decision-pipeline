import assert from 'node:assert/strict';
import test from 'node:test';

import {
    buildCandidateMaterializationShortlist,
    buildBestEligibleTuneDeploymentIdSet,
    buildForwardValidationByCandidate,
    buildForwardValidationByCandidateFromTasks,
    buildWinnerCandidateKeySet,
    filterMaterializationCandidatesByQuality,
    shouldEnforceWinnerShortlistForDeployment,
} from '../researchPromotion';
import type { ScalpResearchCycleSnapshot, ScalpResearchTask } from '../researchCycle';

function makeCycle(cycleId = 'rc_promote'): ScalpResearchCycleSnapshot {
    return {
        version: 1,
        cycleId,
        status: 'completed',
        createdAtMs: 0,
        updatedAtMs: 0,
        startedBy: 'test',
        dryRun: false,
        sourceUniverseGeneratedAt: null,
        params: {
            symbols: ['BTCUSDT'],
            lookbackDays: 90,
            chunkDays: 14,
            minCandlesPerTask: 180,
            maxTasks: 120,
            maxAttempts: 2,
            runningStaleAfterMs: 20 * 60 * 1000,
        },
        symbols: ['BTCUSDT'],
        taskIds: [],
        latestSummary: null,
    };
}

function makeCompletedTask(params: {
    cycleId: string;
    taskId: string;
    symbol: string;
    strategyId: string;
    trades: number;
    netR: number;
    expectancyR: number;
    profitFactor: number | null;
    maxDrawdownR: number;
}): ScalpResearchTask {
    return {
        version: 1,
        cycleId: params.cycleId,
        taskId: params.taskId,
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: 'research_test',
        deploymentId: `${params.symbol}~${params.strategyId}~research_test`,
        windowFromTs: 1,
        windowToTs: 2,
        status: 'completed',
        attempts: 1,
        createdAtMs: 0,
        updatedAtMs: 0,
        workerId: 'w1',
        startedAtMs: 0,
        finishedAtMs: 0,
        errorCode: null,
        errorMessage: null,
        result: {
            symbol: params.symbol,
            strategyId: params.strategyId,
            tuneId: 'research_test',
            deploymentId: `${params.symbol}~${params.strategyId}~research_test`,
            windowFromTs: 1,
            windowToTs: 2,
            trades: params.trades,
            winRatePct: 50,
            netR: params.netR,
            expectancyR: params.expectancyR,
            profitFactor: params.profitFactor,
            maxDrawdownR: params.maxDrawdownR,
            avgHoldMinutes: 30,
            netPnlUsd: 10,
            grossProfitR: 2,
            grossLossR: -1,
        },
    };
}

test('buildForwardValidationByCandidate computes roll-level gate metrics from completed tasks', () => {
    const cycle = makeCycle();
    const tasks: ScalpResearchTask[] = [
        makeCompletedTask({
            cycleId: cycle.cycleId,
            taskId: 't1',
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            trades: 10,
            netR: 2,
            expectancyR: 0.2,
            profitFactor: 1.4,
            maxDrawdownR: 3,
        }),
        makeCompletedTask({
            cycleId: cycle.cycleId,
            taskId: 't2',
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            trades: 5,
            netR: -1,
            expectancyR: -0.2,
            profitFactor: 0.7,
            maxDrawdownR: 5,
        }),
        makeCompletedTask({
            cycleId: cycle.cycleId,
            taskId: 't3',
            symbol: 'BTCUSDT',
            strategyId: 'regime_pullback_m15_m3',
            trades: 8,
            netR: 3,
            expectancyR: 0.375,
            profitFactor: null,
            maxDrawdownR: 2,
        }),
        {
            ...makeCompletedTask({
                cycleId: cycle.cycleId,
                taskId: 't4',
                symbol: 'BTCUSDT',
                strategyId: 'regime_pullback_m15_m3',
                trades: 6,
                netR: 1,
                expectancyR: 0.16,
                profitFactor: 1.2,
                maxDrawdownR: 4,
            }),
            status: 'failed',
            result: null,
        },
    ];

    const rows = buildForwardValidationByCandidate(cycle, tasks);

    const cbp = rows.find((row) => row.symbol === 'BTCUSDT' && row.strategyId === 'compression_breakout_pullback_m15_m3');
    assert.ok(cbp);
    assert.equal(cbp.rollCount, 2);
    assert.equal(cbp.profitableWindows, 1);
    assert.equal(cbp.profitableWindowPct, 50);
    assert.equal(cbp.meanExpectancyR, 0);
    assert.ok(Math.abs((cbp.meanProfitFactor || 0) - 1.05) < 1e-9);
    assert.equal(cbp.maxDrawdownR, 5);
    assert.equal(cbp.minTradesPerWindow, 5);
    assert.equal(cbp.totalTrades, 15);
    assert.equal(cbp.forwardValidation.selectionWindowDays, 90);
    assert.equal(cbp.forwardValidation.forwardWindowDays, 14);

    const guarded = rows.find((row) => row.symbol === 'BTCUSDT' && row.strategyId === 'regime_pullback_m15_m3');
    assert.ok(guarded);
    assert.equal(guarded.rollCount, 1);
    assert.equal(guarded.profitableWindowPct, 100);
    assert.equal(guarded.meanExpectancyR, 0.375);
    assert.equal(guarded.meanProfitFactor, null);
    assert.equal(guarded.minTradesPerWindow, 8);
});

test('buildForwardValidationByCandidateFromTasks supports longer confirmation horizons without changing ranking inputs', () => {
    const tasks: ScalpResearchTask[] = [
        makeCompletedTask({
            cycleId: 'rc_old',
            taskId: 'c1',
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            trades: 4,
            netR: 1.2,
            expectancyR: 0.3,
            profitFactor: 1.4,
            maxDrawdownR: 2,
        }),
        makeCompletedTask({
            cycleId: 'rc_new',
            taskId: 'c2',
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            trades: 6,
            netR: -0.6,
            expectancyR: -0.1,
            profitFactor: 0.8,
            maxDrawdownR: 3,
        }),
    ];

    const rows = buildForwardValidationByCandidateFromTasks({
        tasks,
        selectionWindowDays: 364,
        forwardWindowDays: 7,
    });

    assert.equal(rows.length, 1);
    assert.equal(rows[0]?.rollCount, 2);
    assert.equal(rows[0]?.totalTrades, 10);
    assert.equal(rows[0]?.profitableWindowPct, 50);
    assert.equal(rows[0]?.forwardValidation.selectionWindowDays, 364);
    assert.equal(rows[0]?.forwardValidation.forwardWindowDays, 7);
    assert.ok(Math.abs((rows[0]?.medianExpectancyR || 0) - 0.1) < 1e-9);
    assert.equal(rows[0]?.topWindowPnlConcentrationPct, 100);
    assert.ok(Math.abs((rows[0]?.selectionScore || 0) - 0.05) < 1e-9);
});

test('buildWinnerCandidateKeySet prefers smoother strategies over outlier-heavy higher-mean candidates', () => {
    const winners = buildWinnerCandidateKeySet(
        [
            {
                symbol: 'BTCUSDT',
                strategyId: 'trend_day_reacceleration_m15_m3',
                tuneId: 'lucky',
                deploymentId: 'BTCUSDT~trend_day_reacceleration_m15_m3~lucky',
                rollCount: 12,
                profitableWindowPct: 58,
                profitableWindows: 7,
                meanExpectancyR: 0.65,
                medianExpectancyR: 0.08,
                meanProfitFactor: 1.2,
                maxDrawdownR: 4.5,
                topWindowPnlConcentrationPct: 88,
                selectionScore: null,
                minTradesPerWindow: 3,
                totalTrades: 72,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
                forwardValidation: {
                    rollCount: 12,
                    profitableWindowPct: 58,
                    meanExpectancyR: 0.65,
                    meanProfitFactor: 1.2,
                    maxDrawdownR: 4.5,
                    minTradesPerWindow: 3,
                    selectionWindowDays: 90,
                    forwardWindowDays: 7,
                },
            },
            {
                symbol: 'BTCUSDT',
                strategyId: 'compression_breakout_pullback_m15_m3',
                tuneId: 'steady',
                deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~steady',
                rollCount: 12,
                profitableWindowPct: 83,
                profitableWindows: 10,
                meanExpectancyR: 0.46,
                medianExpectancyR: 0.42,
                meanProfitFactor: 2.8,
                maxDrawdownR: 1.7,
                topWindowPnlConcentrationPct: 36,
                selectionScore: null,
                minTradesPerWindow: 6,
                totalTrades: 98,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
                forwardValidation: {
                    rollCount: 12,
                    profitableWindowPct: 83,
                    meanExpectancyR: 0.46,
                    meanProfitFactor: 2.8,
                    maxDrawdownR: 1.7,
                    minTradesPerWindow: 6,
                    selectionWindowDays: 90,
                    forwardWindowDays: 7,
                },
            },
        ],
        1,
    );

    assert.deepEqual(
        Array.from(winners),
        ['BTCUSDT::compression_breakout_pullback_m15_m3::steady'],
    );
});

test('buildWinnerCandidateKeySet keeps only the best tune per strategy before symbol shortlist', () => {
    const winners = buildWinnerCandidateKeySet(
        [
            {
                symbol: 'BTCUSDT',
                strategyId: 'compression_breakout_pullback_m15_m3',
                tuneId: 'best',
                deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~best',
                rollCount: 12,
                profitableWindowPct: 78,
                profitableWindows: 9,
                meanExpectancyR: 0.31,
                medianExpectancyR: 0.29,
                meanProfitFactor: 1.9,
                maxDrawdownR: 2.1,
                topWindowPnlConcentrationPct: 42,
                selectionScore: null,
                minTradesPerWindow: 4,
                totalTrades: 80,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
                forwardValidation: {
                    rollCount: 12,
                    profitableWindowPct: 78,
                    meanExpectancyR: 0.31,
                    meanProfitFactor: 1.9,
                    maxDrawdownR: 2.1,
                    minTradesPerWindow: 4,
                    selectionWindowDays: 90,
                    forwardWindowDays: 7,
                },
            },
            {
                symbol: 'BTCUSDT',
                strategyId: 'compression_breakout_pullback_m15_m3',
                tuneId: 'second',
                deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~second',
                rollCount: 12,
                profitableWindowPct: 76,
                profitableWindows: 9,
                meanExpectancyR: 0.29,
                medianExpectancyR: 0.27,
                meanProfitFactor: 1.8,
                maxDrawdownR: 2.2,
                topWindowPnlConcentrationPct: 44,
                selectionScore: null,
                minTradesPerWindow: 4,
                totalTrades: 80,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
                forwardValidation: {
                    rollCount: 12,
                    profitableWindowPct: 76,
                    meanExpectancyR: 0.29,
                    meanProfitFactor: 1.8,
                    maxDrawdownR: 2.2,
                    minTradesPerWindow: 4,
                    selectionWindowDays: 90,
                    forwardWindowDays: 7,
                },
            },
            {
                symbol: 'BTCUSDT',
                strategyId: 'regime_pullback_m15_m3',
                tuneId: 'default',
                deploymentId: 'BTCUSDT~regime_pullback_m15_m3~default',
                rollCount: 12,
                profitableWindowPct: 74,
                profitableWindows: 9,
                meanExpectancyR: 0.28,
                medianExpectancyR: 0.26,
                meanProfitFactor: 2.3,
                maxDrawdownR: 1.8,
                topWindowPnlConcentrationPct: 40,
                selectionScore: null,
                minTradesPerWindow: 4,
                totalTrades: 76,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
                forwardValidation: {
                    rollCount: 12,
                    profitableWindowPct: 74,
                    meanExpectancyR: 0.28,
                    meanProfitFactor: 2.3,
                    maxDrawdownR: 1.8,
                    minTradesPerWindow: 4,
                    selectionWindowDays: 90,
                    forwardWindowDays: 7,
                },
            },
        ],
        2,
    );

    assert.deepEqual(
        Array.from(winners).sort(),
        [
            'BTCUSDT::compression_breakout_pullback_m15_m3::best',
            'BTCUSDT::regime_pullback_m15_m3::default',
        ],
    );
});

test('buildCandidateMaterializationShortlist returns top-k per symbol across tuned candidates', () => {
    const rows = [
        {
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'default',
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
            rollCount: 8,
            profitableWindowPct: 60,
            profitableWindows: 5,
            meanExpectancyR: 0.12,
            meanProfitFactor: 1.3,
            maxDrawdownR: 4,
            minTradesPerWindow: 5,
            totalTrades: 60,
            selectionWindowDays: 90,
            forwardWindowDays: 14,
            forwardValidation: {
                rollCount: 8,
                profitableWindowPct: 60,
                meanExpectancyR: 0.12,
                meanProfitFactor: 1.3,
                maxDrawdownR: 4,
                minTradesPerWindow: 5,
                selectionWindowDays: 90,
                forwardWindowDays: 14,
            },
        },
        {
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'auto_tp15',
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~auto_tp15',
            rollCount: 8,
            profitableWindowPct: 70,
            profitableWindows: 6,
            meanExpectancyR: 0.2,
            meanProfitFactor: 1.5,
            maxDrawdownR: 3,
            minTradesPerWindow: 5,
            totalTrades: 62,
            selectionWindowDays: 90,
            forwardWindowDays: 14,
            forwardValidation: {
                rollCount: 8,
                profitableWindowPct: 70,
                meanExpectancyR: 0.2,
                meanProfitFactor: 1.5,
                maxDrawdownR: 3,
                minTradesPerWindow: 5,
                selectionWindowDays: 90,
                forwardWindowDays: 14,
            },
        },
        {
            symbol: 'XAUUSDT',
            strategyId: 'regime_pullback_m15_m3',
            tuneId: 'default',
            deploymentId: 'XAUUSDT~regime_pullback_m15_m3~default',
            rollCount: 8,
            profitableWindowPct: 55,
            profitableWindows: 4,
            meanExpectancyR: 0.08,
            meanProfitFactor: 1.1,
            maxDrawdownR: 2,
            minTradesPerWindow: 4,
            totalTrades: 48,
            selectionWindowDays: 90,
            forwardWindowDays: 14,
            forwardValidation: {
                rollCount: 8,
                profitableWindowPct: 55,
                meanExpectancyR: 0.08,
                meanProfitFactor: 1.1,
                maxDrawdownR: 2,
                minTradesPerWindow: 4,
                selectionWindowDays: 90,
                forwardWindowDays: 14,
            },
        },
    ];

    const shortlist = buildCandidateMaterializationShortlist(rows, 1);
    assert.equal(shortlist.length, 2);
    assert.ok(shortlist.some((row) => row.symbol === 'BTCUSDT' && row.tuneId === 'auto_tp15'));
    assert.ok(shortlist.some((row) => row.symbol === 'XAUUSDT' && row.tuneId === 'default'));
});

test('filterMaterializationCandidatesByQuality keeps only candidates with sufficient trades and expectancy', () => {
    const rows = [
        {
            symbol: 'BTCUSDT',
            strategyId: 'regime_pullback_m15_m3',
            tuneId: 'a',
            deploymentId: 'BTCUSDT~regime_pullback_m15_m3~a',
            rollCount: 7,
            profitableWindowPct: 70,
            profitableWindows: 5,
            meanExpectancyR: 0.12,
            meanProfitFactor: 1.3,
            maxDrawdownR: 2.5,
            minTradesPerWindow: 3,
            totalTrades: 80,
            selectionWindowDays: 90,
            forwardWindowDays: 14,
            forwardValidation: {
                rollCount: 7,
                profitableWindowPct: 70,
                meanExpectancyR: 0.12,
                meanProfitFactor: 1.3,
                maxDrawdownR: 2.5,
                minTradesPerWindow: 3,
                selectionWindowDays: 90,
                forwardWindowDays: 14,
            },
        },
        {
            symbol: 'BTCUSDT',
            strategyId: 'regime_pullback_m15_m3',
            tuneId: 'b',
            deploymentId: 'BTCUSDT~regime_pullback_m15_m3~b',
            rollCount: 7,
            profitableWindowPct: 60,
            profitableWindows: 4,
            meanExpectancyR: 0.2,
            meanProfitFactor: 1.2,
            maxDrawdownR: 3,
            minTradesPerWindow: 0,
            totalTrades: 40,
            selectionWindowDays: 90,
            forwardWindowDays: 14,
            forwardValidation: {
                rollCount: 7,
                profitableWindowPct: 60,
                meanExpectancyR: 0.2,
                meanProfitFactor: 1.2,
                maxDrawdownR: 3,
                minTradesPerWindow: 0,
                selectionWindowDays: 90,
                forwardWindowDays: 14,
            },
        },
        {
            symbol: 'XAUUSDT',
            strategyId: 'trend_day_reacceleration_m15_m3',
            tuneId: 'c',
            deploymentId: 'XAUUSDT~trend_day_reacceleration_m15_m3~c',
            rollCount: 7,
            profitableWindowPct: 55,
            profitableWindows: 4,
            meanExpectancyR: -0.02,
            meanProfitFactor: 0.95,
            maxDrawdownR: 4,
            minTradesPerWindow: 8,
            totalTrades: 120,
            selectionWindowDays: 90,
            forwardWindowDays: 14,
            forwardValidation: {
                rollCount: 7,
                profitableWindowPct: 55,
                meanExpectancyR: -0.02,
                meanProfitFactor: 0.95,
                maxDrawdownR: 4,
                minTradesPerWindow: 8,
                selectionWindowDays: 90,
                forwardWindowDays: 14,
            },
        },
    ];

    const filtered = filterMaterializationCandidatesByQuality(rows, {
        minTradesPerWindow: 2,
        minMeanExpectancyR: 0,
    });

    assert.equal(filtered.length, 1);
    assert.equal(filtered[0]?.deploymentId, 'BTCUSDT~regime_pullback_m15_m3~a');
});

test('buildBestEligibleTuneDeploymentIdSet keeps only the best eligible tune per symbol strategy', () => {
    const candidates = [
        {
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'default',
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
            rollCount: 12,
            profitableWindowPct: 91.67,
            profitableWindows: 11,
            meanExpectancyR: 0.42,
            meanProfitFactor: 2.1,
            maxDrawdownR: 1.8,
            minTradesPerWindow: 6,
            totalTrades: 96,
            selectionWindowDays: 90,
            forwardWindowDays: 7,
            forwardValidation: {
                rollCount: 12,
                profitableWindowPct: 91.67,
                meanExpectancyR: 0.42,
                meanProfitFactor: 2.1,
                maxDrawdownR: 1.8,
                minTradesPerWindow: 6,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
            },
        },
        {
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'auto_tr1p7',
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~auto_tr1p7',
            rollCount: 12,
            profitableWindowPct: 92,
            profitableWindows: 11,
            meanExpectancyR: 0.43,
            meanProfitFactor: 2.2,
            maxDrawdownR: 1.7,
            minTradesPerWindow: 6,
            totalTrades: 98,
            selectionWindowDays: 90,
            forwardWindowDays: 7,
            forwardValidation: {
                rollCount: 12,
                profitableWindowPct: 92,
                meanExpectancyR: 0.43,
                meanProfitFactor: 2.2,
                maxDrawdownR: 1.7,
                minTradesPerWindow: 6,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
            },
        },
        {
            symbol: 'XAUUSDT',
            strategyId: 'trend_day_reacceleration_m15_m3',
            tuneId: 'default',
            deploymentId: 'XAUUSDT~trend_day_reacceleration_m15_m3~default',
            rollCount: 12,
            profitableWindowPct: 91.67,
            profitableWindows: 11,
            meanExpectancyR: 0.63,
            meanProfitFactor: 5.0,
            maxDrawdownR: 3.65,
            minTradesPerWindow: 8,
            totalTrades: 112,
            selectionWindowDays: 90,
            forwardWindowDays: 7,
            forwardValidation: {
                rollCount: 12,
                profitableWindowPct: 91.67,
                meanExpectancyR: 0.63,
                meanProfitFactor: 5.0,
                maxDrawdownR: 3.65,
                minTradesPerWindow: 8,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
            },
        },
    ];

    const winners = buildBestEligibleTuneDeploymentIdSet({
        candidates,
        deployments: [
            {
                deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
                symbol: 'BTCUSDT',
                strategyId: 'compression_breakout_pullback_m15_m3',
                tuneId: 'default',
                enabled: false,
                promotionGate: { eligible: true, reason: null, source: 'walk_forward', evaluatedAtMs: 1, forwardValidation: candidates[0].forwardValidation, thresholds: null },
            },
            {
                deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~auto_tr1p7',
                symbol: 'BTCUSDT',
                strategyId: 'compression_breakout_pullback_m15_m3',
                tuneId: 'auto_tr1p7',
                enabled: false,
                promotionGate: { eligible: true, reason: null, source: 'walk_forward', evaluatedAtMs: 1, forwardValidation: candidates[1].forwardValidation, thresholds: null },
            },
            {
                deploymentId: 'XAUUSDT~trend_day_reacceleration_m15_m3~default',
                symbol: 'XAUUSDT',
                strategyId: 'trend_day_reacceleration_m15_m3',
                tuneId: 'default',
                enabled: false,
                promotionGate: { eligible: true, reason: null, source: 'walk_forward', evaluatedAtMs: 1, forwardValidation: candidates[2].forwardValidation, thresholds: null },
            },
            {
                deploymentId: 'XAUUSDT~regime_pullback_m15_m3~default',
                symbol: 'XAUUSDT',
                strategyId: 'regime_pullback_m15_m3',
                tuneId: 'default',
                enabled: false,
                promotionGate: { eligible: false, reason: 'missing_cycle_candidate', source: 'walk_forward', evaluatedAtMs: 1, forwardValidation: candidates[2].forwardValidation, thresholds: null },
            },
        ],
    });

    assert.deepEqual(
        Array.from(winners).sort(),
        [
            'BTCUSDT~compression_breakout_pullback_m15_m3~auto_tr1p7',
            'XAUUSDT~trend_day_reacceleration_m15_m3~default',
        ],
    );
});

test('buildBestEligibleTuneDeploymentIdSet keeps an enabled incumbent when the challenger edge is marginal', () => {
    const candidates = [
        {
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'default',
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
            rollCount: 12,
            profitableWindowPct: 91.67,
            profitableWindows: 11,
            meanExpectancyR: 0.42,
            meanProfitFactor: 2.1,
            maxDrawdownR: 1.8,
            minTradesPerWindow: 6,
            totalTrades: 96,
            selectionWindowDays: 90,
            forwardWindowDays: 7,
            forwardValidation: {
                rollCount: 12,
                profitableWindowPct: 91.67,
                meanExpectancyR: 0.42,
                meanProfitFactor: 2.1,
                maxDrawdownR: 1.8,
                minTradesPerWindow: 6,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
            },
        },
        {
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'auto_tr1p7',
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~auto_tr1p7',
            rollCount: 12,
            profitableWindowPct: 92,
            profitableWindows: 11,
            meanExpectancyR: 0.43,
            meanProfitFactor: 2.2,
            maxDrawdownR: 1.7,
            minTradesPerWindow: 6,
            totalTrades: 98,
            selectionWindowDays: 90,
            forwardWindowDays: 7,
            forwardValidation: {
                rollCount: 12,
                profitableWindowPct: 92,
                meanExpectancyR: 0.43,
                meanProfitFactor: 2.2,
                maxDrawdownR: 1.7,
                minTradesPerWindow: 6,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
            },
        },
    ];

    const winners = buildBestEligibleTuneDeploymentIdSet({
        candidates,
        deployments: [
            {
                deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
                symbol: 'BTCUSDT',
                strategyId: 'compression_breakout_pullback_m15_m3',
                tuneId: 'default',
                enabled: true,
                promotionGate: { eligible: true, reason: null, source: 'walk_forward', evaluatedAtMs: 1, forwardValidation: candidates[0].forwardValidation, thresholds: null },
            },
            {
                deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~auto_tr1p7',
                symbol: 'BTCUSDT',
                strategyId: 'compression_breakout_pullback_m15_m3',
                tuneId: 'auto_tr1p7',
                enabled: false,
                promotionGate: { eligible: true, reason: null, source: 'walk_forward', evaluatedAtMs: 1, forwardValidation: candidates[1].forwardValidation, thresholds: null },
            },
        ],
    });

    assert.deepEqual(Array.from(winners), ['BTCUSDT~compression_breakout_pullback_m15_m3~default']);
});

test('buildBestEligibleTuneDeploymentIdSet replaces an enabled incumbent when the challenger is materially better', () => {
    const candidates = [
        {
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'default',
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
            rollCount: 12,
            profitableWindowPct: 58,
            profitableWindows: 7,
            meanExpectancyR: 0.18,
            medianExpectancyR: 0.1,
            meanProfitFactor: 1.2,
            maxDrawdownR: 3.4,
            topWindowPnlConcentrationPct: 72,
            minTradesPerWindow: 4,
            totalTrades: 80,
            selectionWindowDays: 90,
            forwardWindowDays: 7,
            forwardValidation: {
                rollCount: 12,
                profitableWindowPct: 58,
                meanExpectancyR: 0.18,
                meanProfitFactor: 1.2,
                maxDrawdownR: 3.4,
                minTradesPerWindow: 4,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
            },
        },
        {
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'challenger',
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~challenger',
            rollCount: 12,
            profitableWindowPct: 83,
            profitableWindows: 10,
            meanExpectancyR: 0.46,
            medianExpectancyR: 0.41,
            meanProfitFactor: 2.8,
            maxDrawdownR: 1.7,
            topWindowPnlConcentrationPct: 34,
            minTradesPerWindow: 6,
            totalTrades: 98,
            selectionWindowDays: 90,
            forwardWindowDays: 7,
            forwardValidation: {
                rollCount: 12,
                profitableWindowPct: 83,
                meanExpectancyR: 0.46,
                meanProfitFactor: 2.8,
                maxDrawdownR: 1.7,
                minTradesPerWindow: 6,
                selectionWindowDays: 90,
                forwardWindowDays: 7,
            },
        },
    ];

    const winners = buildBestEligibleTuneDeploymentIdSet({
        candidates,
        deployments: [
            {
                deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
                symbol: 'BTCUSDT',
                strategyId: 'compression_breakout_pullback_m15_m3',
                tuneId: 'default',
                enabled: true,
                promotionGate: { eligible: true, reason: null, source: 'walk_forward', evaluatedAtMs: 1, forwardValidation: candidates[0].forwardValidation, thresholds: null },
            },
            {
                deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~challenger',
                symbol: 'BTCUSDT',
                strategyId: 'compression_breakout_pullback_m15_m3',
                tuneId: 'challenger',
                enabled: false,
                promotionGate: { eligible: true, reason: null, source: 'walk_forward', evaluatedAtMs: 1, forwardValidation: candidates[1].forwardValidation, thresholds: null },
            },
        ],
    });

    assert.deepEqual(Array.from(winners), ['BTCUSDT~compression_breakout_pullback_m15_m3~challenger']);
});

test('shouldEnforceWinnerShortlistForDeployment exempts incumbents from shortlist-only disqualification', () => {
    assert.equal(
        shouldEnforceWinnerShortlistForDeployment({
            deployment: {
                enabled: true,
                promotionGate: {
                    eligible: true,
                    reason: null,
                    source: 'walk_forward',
                    evaluatedAtMs: 1,
                    forwardValidation: null,
                    thresholds: null,
                },
            },
            inWinnerShortlist: false,
            requireWinnerShortlist: true,
        }),
        false,
    );

    assert.equal(
        shouldEnforceWinnerShortlistForDeployment({
            deployment: {
                enabled: false,
                promotionGate: {
                    eligible: false,
                    reason: 'missing_cycle_candidate',
                    source: 'walk_forward',
                    evaluatedAtMs: 1,
                    forwardValidation: null,
                    thresholds: null,
                },
            },
            inWinnerShortlist: false,
            requireWinnerShortlist: true,
        }),
        true,
    );
});
