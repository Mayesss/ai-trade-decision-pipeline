import assert from 'node:assert/strict';
import test from 'node:test';

import type { ScalpResearchCycleSnapshot, ScalpResearchTask } from '../researchCycle';
import { buildResearchCycleTasks, summarizeResearchTasks } from '../researchCycle';

const DAY_MS = 24 * 60 * 60_000;

function makeCycle(cycleId = 'rc_test'): ScalpResearchCycleSnapshot {
    return {
        version: 1,
        cycleId,
        status: 'running',
        createdAtMs: 0,
        updatedAtMs: 0,
        startedBy: null,
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

function makeTask(params: {
    cycleId: string;
    taskId: string;
    symbol: string;
    strategyId: string;
    status: ScalpResearchTask['status'];
    result?: ScalpResearchTask['result'];
}): ScalpResearchTask {
    return {
        version: 1,
        cycleId: params.cycleId,
        taskId: params.taskId,
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: 'unit_tune',
        deploymentId: `${params.symbol.toLowerCase()}_${params.strategyId}`,
        windowFromTs: 1,
        windowToTs: 2,
        status: params.status,
        attempts: 1,
        createdAtMs: 0,
        updatedAtMs: 0,
        workerId: null,
        startedAtMs: null,
        finishedAtMs: null,
        errorCode: params.status === 'failed' ? 'unit_error' : null,
        errorMessage: params.status === 'failed' ? 'unit failure' : null,
        result: params.result || null,
    };
}

test('buildResearchCycleTasks chunks lookback windows across allowed strategies', () => {
    const nowMs = Date.UTC(2026, 2, 1);
    const tasks = buildResearchCycleTasks({
        cycleId: 'rc_unit',
        nowMs,
        symbols: ['BTCUSDT'],
        lookbackDays: 28,
        chunkDays: 14,
        maxTasks: 20,
        strategyAllowlist: ['compression_breakout_pullback_m15_m3', 'regime_pullback_m15_m3'],
        tunerEnabled: false,
        maxTuneVariantsPerStrategy: 1,
    });

    assert.equal(tasks.length, 4);

    const uniqueTaskIds = new Set(tasks.map((task) => task.taskId));
    assert.equal(uniqueTaskIds.size, tasks.length);

    const strategyIds = Array.from(new Set(tasks.map((task) => task.strategyId))).sort();
    assert.deepEqual(strategyIds, ['compression_breakout_pullback_m15_m3', 'regime_pullback_m15_m3']);

    const byStrategy = new Map<string, number>();
    for (const task of tasks) {
        byStrategy.set(task.strategyId, (byStrategy.get(task.strategyId) || 0) + 1);
        assert.equal(task.status, 'pending');
        assert.equal(task.attempts, 0);
        assert.ok(task.windowFromTs < task.windowToTs);
        assert.ok(task.windowToTs - task.windowFromTs <= 14 * DAY_MS);
    }
    assert.equal(byStrategy.get('compression_breakout_pullback_m15_m3'), 2);
    assert.equal(byStrategy.get('regime_pullback_m15_m3'), 2);
});

test('buildResearchCycleTasks enforces maxTasks cap', () => {
    const nowMs = Date.UTC(2026, 2, 1);
    const tasks = buildResearchCycleTasks({
        cycleId: 'rc_cap',
        nowMs,
        symbols: ['BTCUSDT', 'XAUUSDT', 'EURUSD'],
        lookbackDays: 90,
        chunkDays: 14,
        maxTasks: 3,
        strategyAllowlist: [],
        tunerEnabled: false,
        maxTuneVariantsPerStrategy: 1,
    });

    assert.equal(tasks.length, 3);
});

test('buildResearchCycleTasks expands symbol+strategy into capped tune variants when tuner is enabled', () => {
    const nowMs = Date.UTC(2026, 2, 1);
    const tasks = buildResearchCycleTasks({
        cycleId: 'rc_tuned',
        nowMs,
        symbols: ['BTCUSDT'],
        lookbackDays: 28,
        chunkDays: 14,
        maxTasks: 200,
        strategyAllowlist: ['compression_breakout_pullback_m15_m3', 'regime_pullback_m15_m3'],
        tunerEnabled: true,
        maxTuneVariantsPerStrategy: 3,
    });

    assert.equal(tasks.length, 12);
    const tuneIds = new Set(tasks.map((row) => row.tuneId));
    assert.ok(tuneIds.has('default'));
    assert.ok(Array.from(tuneIds).some((row) => row.startsWith('auto_')));
    assert.ok(tasks.some((row) => row.configOverride && Object.keys(row.configOverride).length > 0));
});

test('summarizeResearchTasks aggregates candidate metrics and keeps running status with pending tasks', () => {
    const cycle = makeCycle('rc_sum_running');

    const completed = makeTask({
        cycleId: cycle.cycleId,
        taskId: 't1',
        symbol: 'BTCUSDT',
        strategyId: 'regime_pullback_m15_m3',
        status: 'completed',
        result: {
            symbol: 'BTCUSDT',
            strategyId: 'regime_pullback_m15_m3',
            tuneId: 'unit_tune',
            deploymentId: 'btcusdt_guarded',
            windowFromTs: 1,
            windowToTs: 2,
            trades: 10,
            winRatePct: 60,
            netR: 2,
            expectancyR: 0.2,
            profitFactor: 1.5,
            maxDrawdownR: 1.25,
            avgHoldMinutes: 24,
            netPnlUsd: 120,
            grossProfitR: 5,
            grossLossR: -3,
        },
    });

    const failed = makeTask({
        cycleId: cycle.cycleId,
        taskId: 't2',
        symbol: 'BTCUSDT',
        strategyId: 'regime_pullback_m15_m3',
        status: 'failed',
    });

    const pending = makeTask({
        cycleId: cycle.cycleId,
        taskId: 't3',
        symbol: 'BTCUSDT',
        strategyId: 'compression_breakout_pullback_m15_m3',
        status: 'pending',
    });

    const summary = summarizeResearchTasks(cycle, [completed, failed, pending]);

    assert.equal(summary.status, 'running');
    assert.equal(summary.totals.tasks, 3);
    assert.equal(summary.totals.completed, 1);
    assert.equal(summary.totals.failed, 1);
    assert.equal(summary.totals.pending, 1);
    assert.equal(summary.totals.running, 0);
    assert.equal(summary.progressPct, (2 / 3) * 100);

    const guarded = summary.candidateAggregates.find(
        (row) => row.symbol === 'BTCUSDT' && row.strategyId === 'regime_pullback_m15_m3',
    );
    assert.ok(guarded);
    assert.equal(guarded.completedTasks, 1);
    assert.equal(guarded.failedTasks, 1);
    assert.equal(guarded.trades, 10);
    assert.equal(guarded.netR, 2);
    assert.equal(guarded.expectancyR, 0.2);
    assert.equal(guarded.maxDrawdownR, 1.25);
    assert.equal(guarded.profitFactor, 5 / 3);
});

test('summarizeResearchTasks marks cycle failed when all tasks fail', () => {
    const cycle = makeCycle('rc_sum_failed');

    const failedA = makeTask({
        cycleId: cycle.cycleId,
        taskId: 't1',
        symbol: 'BTCUSDT',
        strategyId: 'regime_pullback_m15_m3',
        status: 'failed',
    });
    const failedB = makeTask({
        cycleId: cycle.cycleId,
        taskId: 't2',
        symbol: 'XAUUSDT',
        strategyId: 'regime_pullback_m15_m3',
        status: 'failed',
    });

    const summary = summarizeResearchTasks(cycle, [failedA, failedB]);

    assert.equal(summary.status, 'failed');
    assert.equal(summary.totals.failed, 2);
    assert.equal(summary.totals.completed, 0);
    assert.equal(summary.progressPct, 100);
});

test('summarizeResearchTasks keeps tune variants as separate candidates', () => {
    const cycle = makeCycle('rc_sum_tunes');
    const taskA: ScalpResearchTask = {
        ...makeTask({
            cycleId: cycle.cycleId,
            taskId: 't_a',
            symbol: 'BTCUSDT',
            strategyId: 'regime_pullback_m15_m3',
            status: 'completed',
            result: {
                symbol: 'BTCUSDT',
                strategyId: 'regime_pullback_m15_m3',
                tuneId: 'default',
                deploymentId: 'BTCUSDT~regime_pullback_m15_m3~default',
                windowFromTs: 1,
                windowToTs: 2,
                trades: 8,
                winRatePct: 50,
                netR: 1,
                expectancyR: 0.125,
                profitFactor: 1.2,
                maxDrawdownR: 1,
                avgHoldMinutes: 20,
                netPnlUsd: 10,
                grossProfitR: 2,
                grossLossR: -1,
            },
        }),
        tuneId: 'default',
        deploymentId: 'BTCUSDT~regime_pullback_m15_m3~default',
    };
    const taskB: ScalpResearchTask = {
        ...taskA,
        taskId: 't_b',
        tuneId: 'auto_tp30',
        deploymentId: 'BTCUSDT~regime_pullback_m15_m3~auto_tp30',
        result: {
            ...(taskA.result as NonNullable<ScalpResearchTask['result']>),
            tuneId: 'auto_tp30',
            deploymentId: 'BTCUSDT~regime_pullback_m15_m3~auto_tp30',
            netR: 3,
            expectancyR: 0.375,
        },
    };

    const summary = summarizeResearchTasks(cycle, [taskA, taskB]);
    assert.equal(summary.candidateAggregates.length, 2);
    assert.ok(summary.candidateAggregates.some((row) => row.tuneId === 'default'));
    assert.ok(summary.candidateAggregates.some((row) => row.tuneId === 'auto_tp30'));
});
