import assert from 'node:assert/strict';
import test from 'node:test';

import { buildForwardValidationByCandidate } from '../researchPromotion';
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
            strategyId: 'regime_pullback_m15_m3_btcusdt',
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
                strategyId: 'regime_pullback_m15_m3_btcusdt',
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

    const guarded = rows.find((row) => row.symbol === 'BTCUSDT' && row.strategyId === 'regime_pullback_m15_m3_btcusdt');
    assert.ok(guarded);
    assert.equal(guarded.rollCount, 1);
    assert.equal(guarded.profitableWindowPct, 100);
    assert.equal(guarded.meanExpectancyR, 0.375);
    assert.equal(guarded.meanProfitFactor, null);
    assert.equal(guarded.minTradesPerWindow, 8);
});
