import assert from 'node:assert/strict';
import test from 'node:test';

import { buildScalpResearchPlan } from '../researchPlanner';
import type { ScalpResearchCandidateAggregate, ScalpResearchCycleSummary } from '../researchCycle';
import { buildResearchCycleTasks } from '../researchCycle';

function makeCandidate(params: {
    symbol: string;
    strategyId: string;
    tuneId: string;
    completedTasks?: number;
    failedTasks?: number;
    trades?: number;
    netR?: number;
    expectancyR?: number;
    profitFactor?: number | null;
    maxDrawdownR?: number;
    configOverride?: ScalpResearchCandidateAggregate['configOverride'];
}): ScalpResearchCandidateAggregate {
    return {
        symbol: params.symbol,
        strategyId: params.strategyId,
        tuneId: params.tuneId,
        deploymentId: `${params.symbol}~${params.strategyId}~${params.tuneId}`,
        configOverride: params.configOverride || null,
        completedTasks: params.completedTasks ?? 12,
        failedTasks: params.failedTasks ?? 0,
        trades: params.trades ?? 40,
        winRatePct: 55,
        netR: params.netR ?? 4,
        expectancyR: params.expectancyR ?? 0.1,
        profitFactor: params.profitFactor ?? 1.2,
        maxDrawdownR: params.maxDrawdownR ?? 3,
        avgHoldMinutes: 20,
        netPnlUsd: 100,
        grossProfitR: 8,
        grossLossAbsR: 4,
    };
}

function makeSummary(candidates: ScalpResearchCandidateAggregate[]): ScalpResearchCycleSummary {
    return {
        cycleId: 'rc_prev',
        status: 'completed',
        totals: {
            tasks: candidates.length * 12,
            pending: 0,
            running: 0,
            completed: candidates.length * 12,
            failed: 0,
        },
        progressPct: 100,
        candidateAggregates: candidates,
        generatedAtMs: 0,
    };
}

test('buildScalpResearchPlan promotes champions then nearby neighbors and challengers', () => {
    const previousSummary = makeSummary([
        makeCandidate({
            symbol: 'BTCUSDT',
            strategyId: 'regime_pullback_m15_m3',
            tuneId: 'auto_tp30',
            netR: 12,
            expectancyR: 0.25,
            profitFactor: 1.8,
            configOverride: { risk: { tp1ClosePct: 30 } },
        }),
        makeCandidate({
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'default',
            netR: 5,
            expectancyR: 0.12,
            profitFactor: 1.3,
            configOverride: null,
        }),
    ]);

    const plan = buildScalpResearchPlan({
        symbols: ['BTCUSDT'],
        strategyAllowlist: ['regime_pullback_m15_m3', 'compression_breakout_pullback_m15_m3'],
        tunerEnabled: true,
        maxTuneVariantsPerStrategy: 6,
        previousSummary,
        policy: {
            enabled: true,
            championCandidatesPerSymbol: 1,
            neighborVariantsPerCandidate: 1,
            challengerStrategiesPerSymbol: 1,
            challengerTunesPerStrategy: 1,
            incubatorStrategiesPerSymbol: 1,
            incubatorTunesPerStrategy: 1,
            fallbackStrategiesPerSymbol: 1,
            fallbackTunesPerStrategy: 1,
            minChampionTrades: 2,
        },
    });

    assert.equal(plan[0]?.tier, 'champion');
    assert.equal(plan[0]?.strategyId, 'regime_pullback_m15_m3');
    assert.equal(plan[0]?.tuneId, 'auto_tp30');
    assert.ok(plan.some((row) => row.tier === 'neighbor' && row.strategyId === 'regime_pullback_m15_m3' && row.tuneId !== 'auto_tp30'));
    assert.ok(plan.some((row) => row.tier === 'challenger' && row.strategyId === 'compression_breakout_pullback_m15_m3'));
});

test('buildScalpResearchPlan bootstraps all symbols when there is no prior cycle summary', () => {
    const plan = buildScalpResearchPlan({
        symbols: ['BTCUSDT', 'XAUUSDT'],
        strategyAllowlist: ['regime_pullback_m15_m3', 'compression_breakout_pullback_m15_m3'],
        tunerEnabled: false,
        maxTuneVariantsPerStrategy: 1,
        previousSummary: null,
        policy: {
            enabled: true,
            incubatorSymbolsPerCycle: 1,
            incubatorStrategiesPerSymbol: 1,
            incubatorTunesPerStrategy: 1,
        },
    });

    assert.ok(plan.some((row) => row.symbol === 'BTCUSDT'));
    assert.ok(plan.some((row) => row.symbol === 'XAUUSDT'));
    assert.ok(plan.every((row) => row.tier === 'incubator'));
});

test('buildScalpResearchPlan caps genuinely new symbols once a prior cycle exists', () => {
    const previousSummary = makeSummary([
        makeCandidate({
            symbol: 'BTCUSDT',
            strategyId: 'regime_pullback_m15_m3',
            tuneId: 'default',
            netR: 8,
            expectancyR: 0.2,
        }),
    ]);

    const plan = buildScalpResearchPlan({
        symbols: ['BTCUSDT', 'XAUUSDT', 'EURUSD'],
        strategyAllowlist: ['regime_pullback_m15_m3', 'compression_breakout_pullback_m15_m3'],
        tunerEnabled: false,
        maxTuneVariantsPerStrategy: 1,
        previousSummary,
        policy: {
            enabled: true,
            championCandidatesPerSymbol: 1,
            neighborVariantsPerCandidate: 0,
            challengerStrategiesPerSymbol: 0,
            incubatorSymbolsPerCycle: 1,
            incubatorStrategiesPerSymbol: 1,
            incubatorTunesPerStrategy: 1,
            fallbackStrategiesPerSymbol: 1,
            fallbackTunesPerStrategy: 1,
        },
    });

    assert.ok(plan.some((row) => row.symbol === 'BTCUSDT'));
    assert.ok(plan.some((row) => row.symbol === 'XAUUSDT'));
    assert.equal(plan.some((row) => row.symbol === 'EURUSD'), false);
});

test('buildResearchCycleTasks uses planner-selected combos when planner mode is enabled', () => {
    const nowMs = Date.UTC(2026, 2, 1);
    const previousSummary = makeSummary([
        makeCandidate({
            symbol: 'BTCUSDT',
            strategyId: 'regime_pullback_m15_m3',
            tuneId: 'auto_tp30',
            netR: 12,
            expectancyR: 0.25,
            profitFactor: 1.8,
            configOverride: { risk: { tp1ClosePct: 30 } },
        }),
        makeCandidate({
            symbol: 'BTCUSDT',
            strategyId: 'compression_breakout_pullback_m15_m3',
            tuneId: 'default',
            netR: 5,
            expectancyR: 0.12,
            profitFactor: 1.3,
            configOverride: null,
        }),
    ]);

    const tasks = buildResearchCycleTasks({
        cycleId: 'rc_planned',
        nowMs,
        symbols: ['BTCUSDT'],
        lookbackDays: 28,
        chunkDays: 7,
        maxTasks: 200,
        strategyAllowlist: ['regime_pullback_m15_m3', 'compression_breakout_pullback_m15_m3'],
        tunerEnabled: true,
        maxTuneVariantsPerStrategy: 6,
        plannerEnabled: true,
        plannerPolicy: {
            championCandidatesPerSymbol: 1,
            neighborVariantsPerCandidate: 1,
            challengerStrategiesPerSymbol: 1,
            challengerTunesPerStrategy: 1,
            incubatorStrategiesPerSymbol: 1,
            incubatorTunesPerStrategy: 1,
            fallbackStrategiesPerSymbol: 1,
            fallbackTunesPerStrategy: 1,
            minChampionTrades: 2,
        },
        previousSummary,
    });

    assert.equal(tasks.length, 12);
    const combos = new Set(tasks.map((row) => `${row.symbol}::${row.strategyId}::${row.tuneId}`));
    assert.equal(combos.size, 3);
    assert.ok(combos.has('BTCUSDT::regime_pullback_m15_m3::auto_tp30'));
});
