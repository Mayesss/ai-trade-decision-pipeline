import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import test from 'node:test';

import type { ScalpResearchCycleSnapshot, ScalpResearchTask } from '../researchCycle';
import {
    buildResearchCycleTasks,
    createEmptyResearchSymbolCooldownSnapshot,
    evaluateResearchCyclePreflight,
    evaluateResearchTaskClaimability,
    isResearchSymbolCooldownActive,
    isResearchTaskFailureEligibleForSymbolCooldown,
    registerResearchSymbolFailure,
    resolveResearchWorkerRuntimeConfig,
    summarizeResearchTasks,
} from '../researchCycle';
import { saveScalpCandleHistory } from '../candleHistory';

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

test('evaluateResearchTaskClaimability blocks stale running reclaim when max attempts are exhausted', () => {
    const nowMs = Date.UTC(2026, 2, 10, 8, 0, 0);
    const startedAtMs = nowMs - 30 * 60 * 1000;
    const out = evaluateResearchTaskClaimability({
        status: 'running',
        attempts: 2,
        maxAttempts: 2,
        startedAtMs,
        runningStaleAfterMs: 20 * 60 * 1000,
        nowMs,
    });

    assert.equal(out.runningStale, true);
    assert.equal(out.maxAttemptsReached, true);
    assert.equal(out.claimable, false);
    assert.equal(out.shouldMarkFailedForAttempts, true);
});

test('evaluateResearchTaskClaimability allows reclaim for stale running with attempts remaining', () => {
    const nowMs = Date.UTC(2026, 2, 10, 8, 0, 0);
    const startedAtMs = nowMs - 30 * 60 * 1000;
    const out = evaluateResearchTaskClaimability({
        status: 'running',
        attempts: 1,
        maxAttempts: 2,
        startedAtMs,
        runningStaleAfterMs: 20 * 60 * 1000,
        nowMs,
    });

    assert.equal(out.runningStale, true);
    assert.equal(out.maxAttemptsReached, false);
    assert.equal(out.claimable, true);
    assert.equal(out.shouldMarkFailedForAttempts, false);
});

test('evaluateResearchTaskClaimability treats pending tasks as claimable when attempts remain', () => {
    const nowMs = Date.UTC(2026, 2, 10, 8, 0, 0);
    const out = evaluateResearchTaskClaimability({
        status: 'pending',
        attempts: 1,
        maxAttempts: 2,
        startedAtMs: null,
        runningStaleAfterMs: 20 * 60 * 1000,
        nowMs,
    });

    assert.equal(out.maxAttemptsReached, false);
    assert.equal(out.claimable, true);
    assert.equal(out.shouldMarkFailedForAttempts, false);
});

test('summarizeResearchTasks treats cooldown-deferred pending tasks as pending (not failed)', () => {
    const cycle = makeCycle('rc_sum_pending_cooldown');

    const deferred: ScalpResearchTask = {
        ...makeTask({
            cycleId: cycle.cycleId,
            taskId: 't_pending_cooldown',
            symbol: 'EURUSD',
            strategyId: 'regime_pullback_m15_m3',
            status: 'pending',
        }),
        attempts: 0,
        errorCode: 'symbol_cooldown_active',
        errorMessage: 'symbol_cooldown_active_until:2026-03-10T10:00:00.000Z',
    };

    const summary = summarizeResearchTasks(cycle, [deferred]);

    assert.equal(summary.status, 'running');
    assert.equal(summary.totals.pending, 1);
    assert.equal(summary.totals.failed, 0);
    assert.equal(summary.progressPct, 0);
});

test('symbol cooldown activates after repeated eligible failures in window', () => {
    const nowMs = Date.UTC(2026, 2, 10, 8, 0, 0);
    const snapshot = createEmptyResearchSymbolCooldownSnapshot(nowMs);
    const cfg = {
        enabled: true,
        failureThreshold: 3,
        failureWindowMs: 30 * 60 * 1000,
        cooldownMs: 2 * 60 * 60 * 1000,
        maxTrackedSymbols: 50,
    };

    const a = registerResearchSymbolFailure(snapshot, {
        symbol: 'EURUSD',
        errorCode: 'task_failed',
        errorMessage: 'fetch failed',
        nowMs: nowMs + 1_000,
        config: cfg,
    });
    const b = registerResearchSymbolFailure(snapshot, {
        symbol: 'EURUSD',
        errorCode: 'task_failed',
        errorMessage: 'fetch failed',
        nowMs: nowMs + 2_000,
        config: cfg,
    });
    const c = registerResearchSymbolFailure(snapshot, {
        symbol: 'EURUSD',
        errorCode: 'task_failed',
        errorMessage: 'fetch failed',
        nowMs: nowMs + 3_000,
        config: cfg,
    });

    assert.equal(a.blockedNow, false);
    assert.equal(b.blockedNow, false);
    assert.equal(c.blockedNow, true);
    assert.ok(c.blockedUntilMs > nowMs);
    assert.equal(isResearchSymbolCooldownActive(snapshot, 'EURUSD', nowMs + 10_000), true);
    assert.equal(isResearchSymbolCooldownActive(snapshot, 'EURUSD', c.blockedUntilMs + 1), false);
});

test('failure classifier only tracks hard/network-like task failures', () => {
    assert.equal(isResearchTaskFailureEligibleForSymbolCooldown('task_timeout', 'task_timeout:60000'), true);
    assert.equal(isResearchTaskFailureEligibleForSymbolCooldown('insufficient_candles', 'insufficient_candles:12'), true);
    assert.equal(isResearchTaskFailureEligibleForSymbolCooldown('task_failed', 'fetch failed'), true);
    assert.equal(isResearchTaskFailureEligibleForSymbolCooldown('task_failed', 'random_logic_error'), false);
    assert.equal(isResearchTaskFailureEligibleForSymbolCooldown('validation_error', 'bad payload'), false);
});

test('resolveResearchWorkerRuntimeConfig clamps requested maxRuns and concurrency to env caps', () => {
    const out = resolveResearchWorkerRuntimeConfig(
        {
            maxRuns: 300,
            concurrency: 64,
            maxDurationMs: 250_000,
        },
        {
            SCALP_RESEARCH_WORKER_MAX_RUNS_CAP: '50',
            SCALP_RESEARCH_WORKER_MAX_CONCURRENCY: '8',
            SCALP_RESEARCH_WORKER_CONCURRENCY: '4',
            SCALP_RESEARCH_WORKER_MAX_DURATION_MS: '200000',
            SCALP_RESEARCH_WORKER_MAX_DURATION_CAP_MS: '90000',
        },
    );

    assert.equal(out.maxRunsCap, 50);
    assert.equal(out.maxConcurrency, 8);
    assert.equal(out.maxRuns, 50);
    assert.equal(out.concurrency, 8);
    assert.equal(out.maxDurationCapMs, 90_000);
    assert.equal(out.maxDurationMs, 90_000);
});

test('resolveResearchWorkerRuntimeConfig uses default concurrency but does not exceed maxRuns', () => {
    const out = resolveResearchWorkerRuntimeConfig(
        {
            maxRuns: 3,
        },
        {
            SCALP_RESEARCH_WORKER_MAX_RUNS_CAP: '200',
            SCALP_RESEARCH_WORKER_MAX_CONCURRENCY: '16',
            SCALP_RESEARCH_WORKER_CONCURRENCY: '10',
            SCALP_RESEARCH_WORKER_MAX_DURATION_MS: '45000',
            SCALP_RESEARCH_WORKER_MAX_DURATION_CAP_MS: '200000',
        },
    );

    assert.equal(out.maxRuns, 3);
    assert.equal(out.concurrency, 3);
    assert.equal(out.maxDurationMs, 45_000);
});

test('evaluateResearchCyclePreflight excludes underfilled symbols but allows ready symbols', async () => {
    const tmpRoot = await mkdtemp(path.join(os.tmpdir(), 'scalp-preflight-'));
    const prevEnv = {
        CANDLE_HISTORY_DIR: process.env.CANDLE_HISTORY_DIR,
        SCALP_SYMBOL_UNIVERSE_PATH: process.env.SCALP_SYMBOL_UNIVERSE_PATH,
        SCALP_RESEARCH_REPORT_PATH: process.env.SCALP_RESEARCH_REPORT_PATH,
        ALLOW_SCALP_FILE_BACKEND: process.env.ALLOW_SCALP_FILE_BACKEND,
    };

    try {
        process.env.CANDLE_HISTORY_DIR = path.join(tmpRoot, 'candles');
        process.env.SCALP_SYMBOL_UNIVERSE_PATH = path.join(tmpRoot, 'universe.json');
        process.env.SCALP_RESEARCH_REPORT_PATH = path.join(tmpRoot, 'report.json');
        process.env.ALLOW_SCALP_FILE_BACKEND = '1';

        await writeFile(
            process.env.SCALP_SYMBOL_UNIVERSE_PATH,
            JSON.stringify({
                version: 1,
                generatedAtIso: '2026-03-11T00:00:00.000Z',
                policy: {},
                source: 'weekly_discovery_v1',
                dryRun: false,
                previousSymbols: [],
                selectedSymbols: ['EURUSD', 'GBPUSD'],
                addedSymbols: ['EURUSD', 'GBPUSD'],
                removedSymbols: [],
                candidatesEvaluated: 2,
                selectedRows: [],
                topRejectedRows: [],
            }),
            'utf8',
        );
        await writeFile(
            process.env.SCALP_RESEARCH_REPORT_PATH,
            JSON.stringify({
                generatedAtIso: '2026-03-11T00:05:00.000Z',
            }),
            'utf8',
        );

        const preflightNowMs = Date.UTC(2026, 2, 11, 12, 0, 0); // Wednesday
        const oneDayMs = 24 * 60 * 60 * 1000;
        const oneWeekMs = 7 * oneDayMs;
        const dayOfWeek = new Date(Date.UTC(2026, 2, 11, 0, 0, 0)).getUTCDay(); // 0=Sun..6=Sat
        const daysSinceMonday = (dayOfWeek + 6) % 7;
        const startCurrentWeekMondayMs = Date.UTC(2026, 2, 11, 0, 0, 0) - daysSinceMonday * oneDayMs;
        const firstRequiredWeekStartMs = startCurrentWeekMondayMs - 12 * oneWeekMs;
        const makeWeeklyCandles = (weeks: number, perWeek: number) => {
            const out: Array<[number, number, number, number, number, number]> = [];
            for (let week = 0; week < weeks; week += 1) {
                const weekStart = firstRequiredWeekStartMs + week * oneWeekMs;
                for (let slot = 0; slot < perWeek; slot += 1) {
                    const ts = weekStart + (slot + 1) * 12 * 60 * 60 * 1000;
                    out.push([ts, 1.1, 1.11, 1.09, 1.105, 10]);
                }
            }
            return out;
        };

        await saveScalpCandleHistory({
            symbol: 'EURUSD',
            timeframe: '1m',
            epic: 'CS.D.EURUSD.TODAY.IP',
            source: 'capital',
            candles: makeWeeklyCandles(12, 2),
        });
        await saveScalpCandleHistory({
            symbol: 'GBPUSD',
            timeframe: '1m',
            epic: 'CS.D.GBPUSD.TODAY.IP',
            source: 'capital',
            candles: makeWeeklyCandles(5, 2),
        });

        const out = await evaluateResearchCyclePreflight({ minCandlesPerTask: 20, nowMs: preflightNowMs });
        assert.equal(out.ready, true);
        assert.deepEqual(out.resolvedSymbols, ['EURUSD']);
        assert.ok(out.candleChecks.some((row) => row.symbol === 'GBPUSD' && row.ok === false));
        assert.equal(out.failures.some((row) => row.code === 'insufficient_candles'), false);
    } finally {
        if (prevEnv.CANDLE_HISTORY_DIR === undefined) delete process.env.CANDLE_HISTORY_DIR;
        else process.env.CANDLE_HISTORY_DIR = prevEnv.CANDLE_HISTORY_DIR;
        if (prevEnv.SCALP_SYMBOL_UNIVERSE_PATH === undefined) delete process.env.SCALP_SYMBOL_UNIVERSE_PATH;
        else process.env.SCALP_SYMBOL_UNIVERSE_PATH = prevEnv.SCALP_SYMBOL_UNIVERSE_PATH;
        if (prevEnv.SCALP_RESEARCH_REPORT_PATH === undefined) delete process.env.SCALP_RESEARCH_REPORT_PATH;
        else process.env.SCALP_RESEARCH_REPORT_PATH = prevEnv.SCALP_RESEARCH_REPORT_PATH;
        if (prevEnv.ALLOW_SCALP_FILE_BACKEND === undefined) delete process.env.ALLOW_SCALP_FILE_BACKEND;
        else process.env.ALLOW_SCALP_FILE_BACKEND = prevEnv.ALLOW_SCALP_FILE_BACKEND;
        await rm(tmpRoot, { recursive: true, force: true });
    }
});

test('evaluateResearchCyclePreflight filters symbols missing 12 successive completed weeks', async () => {
    const tmpRoot = await mkdtemp(path.join(os.tmpdir(), 'scalp-preflight-weeks-'));
    const prevEnv = {
        CANDLE_HISTORY_DIR: process.env.CANDLE_HISTORY_DIR,
        SCALP_SYMBOL_UNIVERSE_PATH: process.env.SCALP_SYMBOL_UNIVERSE_PATH,
        SCALP_RESEARCH_REPORT_PATH: process.env.SCALP_RESEARCH_REPORT_PATH,
        SCALP_RESEARCH_PREFLIGHT_REQUIRED_SUCCESSIVE_WEEKS: process.env.SCALP_RESEARCH_PREFLIGHT_REQUIRED_SUCCESSIVE_WEEKS,
        ALLOW_SCALP_FILE_BACKEND: process.env.ALLOW_SCALP_FILE_BACKEND,
    };

    try {
        process.env.CANDLE_HISTORY_DIR = path.join(tmpRoot, 'candles');
        process.env.SCALP_SYMBOL_UNIVERSE_PATH = path.join(tmpRoot, 'universe.json');
        process.env.SCALP_RESEARCH_REPORT_PATH = path.join(tmpRoot, 'report.json');
        process.env.SCALP_RESEARCH_PREFLIGHT_REQUIRED_SUCCESSIVE_WEEKS = '12';
        process.env.ALLOW_SCALP_FILE_BACKEND = '1';

        await writeFile(
            process.env.SCALP_SYMBOL_UNIVERSE_PATH,
            JSON.stringify({
                version: 1,
                generatedAtIso: '2026-03-11T00:00:00.000Z',
                policy: {},
                source: 'weekly_discovery_v1',
                dryRun: false,
                previousSymbols: [],
                selectedSymbols: ['EURUSD', 'GBPUSD'],
                addedSymbols: ['EURUSD', 'GBPUSD'],
                removedSymbols: [],
                candidatesEvaluated: 2,
                selectedRows: [],
                topRejectedRows: [],
            }),
            'utf8',
        );
        await writeFile(
            process.env.SCALP_RESEARCH_REPORT_PATH,
            JSON.stringify({
                generatedAtIso: '2026-03-11T00:05:00.000Z',
            }),
            'utf8',
        );

        const preflightNowMs = Date.UTC(2026, 2, 11, 12, 0, 0);
        const oneDayMs = 24 * 60 * 60 * 1000;
        const oneWeekMs = 7 * oneDayMs;
        const dayStartMs = Date.UTC(2026, 2, 11, 0, 0, 0);
        const dayOfWeek = new Date(dayStartMs).getUTCDay();
        const daysSinceMonday = (dayOfWeek + 6) % 7;
        const startCurrentWeekMondayMs = dayStartMs - daysSinceMonday * oneDayMs;
        const firstRequiredWeekStartMs = startCurrentWeekMondayMs - 12 * oneWeekMs;
        const makeWeeklyCandles = (weekIndexes: number[]) =>
            weekIndexes.map((week) => {
                const ts = firstRequiredWeekStartMs + week * oneWeekMs + 12 * 60 * 60 * 1000;
                return [ts, 1.1, 1.11, 1.09, 1.105, 10] as [number, number, number, number, number, number];
            });

        await saveScalpCandleHistory({
            symbol: 'EURUSD',
            timeframe: '1m',
            epic: 'CS.D.EURUSD.TODAY.IP',
            source: 'capital',
            candles: makeWeeklyCandles(Array.from({ length: 12 }, (_, idx) => idx)),
        });
        await saveScalpCandleHistory({
            symbol: 'GBPUSD',
            timeframe: '1m',
            epic: 'CS.D.GBPUSD.TODAY.IP',
            source: 'capital',
            candles: makeWeeklyCandles([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11]), // missing week 10
        });

        const out = await evaluateResearchCyclePreflight({ minCandlesPerTask: 1, nowMs: preflightNowMs, maxCandleChecks: 2 });
        assert.equal(out.ready, true);
        assert.deepEqual(out.resolvedSymbols, ['EURUSD']);
        assert.equal(out.weeklySuccessiveRequirement?.requiredWeeks, 12);
        const gbpExcluded = (out.weeklySuccessiveRequirement?.excludedSymbols || []).find((row) => row.symbol === 'GBPUSD');
        if (gbpExcluded) {
            assert.ok(gbpExcluded.missingWeeks > 0);
        }
    } finally {
        if (prevEnv.CANDLE_HISTORY_DIR === undefined) delete process.env.CANDLE_HISTORY_DIR;
        else process.env.CANDLE_HISTORY_DIR = prevEnv.CANDLE_HISTORY_DIR;
        if (prevEnv.SCALP_SYMBOL_UNIVERSE_PATH === undefined) delete process.env.SCALP_SYMBOL_UNIVERSE_PATH;
        else process.env.SCALP_SYMBOL_UNIVERSE_PATH = prevEnv.SCALP_SYMBOL_UNIVERSE_PATH;
        if (prevEnv.SCALP_RESEARCH_REPORT_PATH === undefined) delete process.env.SCALP_RESEARCH_REPORT_PATH;
        else process.env.SCALP_RESEARCH_REPORT_PATH = prevEnv.SCALP_RESEARCH_REPORT_PATH;
        if (prevEnv.SCALP_RESEARCH_PREFLIGHT_REQUIRED_SUCCESSIVE_WEEKS === undefined)
            delete process.env.SCALP_RESEARCH_PREFLIGHT_REQUIRED_SUCCESSIVE_WEEKS;
        else
            process.env.SCALP_RESEARCH_PREFLIGHT_REQUIRED_SUCCESSIVE_WEEKS =
                prevEnv.SCALP_RESEARCH_PREFLIGHT_REQUIRED_SUCCESSIVE_WEEKS;
        if (prevEnv.ALLOW_SCALP_FILE_BACKEND === undefined) delete process.env.ALLOW_SCALP_FILE_BACKEND;
        else process.env.ALLOW_SCALP_FILE_BACKEND = prevEnv.ALLOW_SCALP_FILE_BACKEND;
        await rm(tmpRoot, { recursive: true, force: true });
    }
});
