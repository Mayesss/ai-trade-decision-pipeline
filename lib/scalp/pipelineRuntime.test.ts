import assert from 'node:assert/strict';
import test from 'node:test';

import {
    mergeScalpPipelineRuntimeSnapshot,
    normalizeScalpPipelineRuntimeSnapshot,
} from './pipelineRuntime';

test('mergeScalpPipelineRuntimeSnapshot preserves unrelated sections', () => {
    const current = normalizeScalpPipelineRuntimeSnapshot({
        version: 1,
        updatedAtMs: 100,
        orchestrator: {
            runId: 'orch_1',
            stage: 'worker',
            cycleId: 'cycle_1',
            startedAtMs: 10,
            updatedAtMs: 90,
            completedAtMs: null,
            isRunning: true,
            progressPct: 70,
            progressLabel: 'running cycle worker',
            lastError: null,
        },
        promotionSync: {
            status: 'running',
            cycleId: 'cycle_1',
            phase: 'matching',
            startedAtMs: 50,
            updatedAtMs: 95,
            finishedAtMs: null,
            totalDeployments: 20,
            processedDeployments: 5,
            matchedDeployments: 3,
            updatedDeployments: 2,
            currentSymbol: 'BTCUSDT',
            currentStrategyId: 'regime_pullback_m15_m3',
            currentTuneId: 'auto',
            reason: null,
            lastError: null,
            lastCompletedCycleId: 'cycle_0',
            lastCompletedAtMs: 40,
        },
    });
    const merged = mergeScalpPipelineRuntimeSnapshot(current, {
        updatedAtMs: 120,
        promotionSync: {
            processedDeployments: 6,
            updatedAtMs: 120,
        },
    });
    assert.ok(merged);
    assert.equal(merged?.orchestrator?.runId, 'orch_1');
    assert.equal(merged?.orchestrator?.stage, 'worker');
    assert.equal(merged?.promotionSync?.processedDeployments, 6);
    assert.equal(merged?.promotionSync?.matchedDeployments, 3);
    assert.equal(merged?.promotionSync?.lastCompletedCycleId, 'cycle_0');
});

test('mergeScalpPipelineRuntimeSnapshot preserves last completed promotion data across progress patches', () => {
    const current = normalizeScalpPipelineRuntimeSnapshot({
        version: 1,
        updatedAtMs: 200,
        promotionSync: {
            status: 'succeeded',
            cycleId: 'cycle_2',
            phase: null,
            startedAtMs: 150,
            updatedAtMs: 200,
            finishedAtMs: 200,
            totalDeployments: 10,
            processedDeployments: 10,
            matchedDeployments: 4,
            updatedDeployments: 4,
            currentSymbol: null,
            currentStrategyId: null,
            currentTuneId: null,
            reason: null,
            lastError: null,
            lastCompletedCycleId: 'cycle_2',
            lastCompletedAtMs: 200,
        },
    });
    const merged = mergeScalpPipelineRuntimeSnapshot(current, {
        updatedAtMs: 300,
        promotionSync: {
            status: 'running',
            cycleId: 'cycle_3',
            phase: 'matching',
            startedAtMs: 250,
            updatedAtMs: 300,
            processedDeployments: 1,
            matchedDeployments: 0,
            updatedDeployments: 0,
            lastError: null,
        },
    });
    assert.ok(merged);
    assert.equal(merged?.promotionSync?.status, 'running');
    assert.equal(merged?.promotionSync?.cycleId, 'cycle_3');
    assert.equal(merged?.promotionSync?.lastCompletedCycleId, 'cycle_2');
    assert.equal(merged?.promotionSync?.lastCompletedAtMs, 200);
});

test('normalizeScalpPipelineRuntimeSnapshot drops empty payloads', () => {
    assert.equal(normalizeScalpPipelineRuntimeSnapshot({}), null);
    assert.equal(normalizeScalpPipelineRuntimeSnapshot(null), null);
});
