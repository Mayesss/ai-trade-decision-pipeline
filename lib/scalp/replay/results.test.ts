import assert from 'node:assert/strict';
import test from 'node:test';

import { buildScalpDeploymentId } from '../deployments';
import { defaultScalpReplayConfig } from './harness';
import { toScalpBacktestLeaderboardEntry } from './results';
import type { ScalpReplayResult } from './types';

test('toScalpBacktestLeaderboardEntry emits a symbol x strategy x tune row with ranking metrics', () => {
    const baseConfig = defaultScalpReplayConfig('EURUSD');
    const deploymentId = buildScalpDeploymentId({
        symbol: 'EURUSD',
        strategyId: baseConfig.strategyId,
        tuneId: 'return_a',
    });
    const config = {
        ...baseConfig,
        tuneId: 'return_a',
        deploymentId,
        tuneLabel: 'return_a',
    };
    const result: ScalpReplayResult = {
        config,
        summary: {
            symbol: 'EURUSD',
            strategyId: config.strategyId,
            tuneId: config.tuneId,
            deploymentId: config.deploymentId,
            tuneLabel: config.tuneLabel,
            startTs: 1,
            endTs: 2,
            runs: 100,
            trades: 12,
            wins: 7,
            losses: 5,
            winRatePct: 58.3333,
            avgR: 0.42,
            expectancyR: 0.42,
            netR: 5.04,
            grossProfitR: 9.5,
            grossLossR: -4.46,
            profitFactor: 2.13,
            netPnlUsd: 504,
            maxDrawdownR: 1.8,
            avgHoldMinutes: 26,
            exitsByReason: { TP: 7, STOP: 5 },
        },
        trades: [],
        timeline: [],
    };

    const entry = toScalpBacktestLeaderboardEntry(result);

    assert.deepEqual(entry, {
        symbol: 'EURUSD',
        strategyId: config.strategyId,
        tuneId: 'return_a',
        deploymentId,
        tuneLabel: 'return_a',
        netR: 5.04,
        profitFactor: 2.13,
        maxDrawdownR: 1.8,
        trades: 12,
        winRatePct: 58.3333,
        avgHoldMinutes: 26,
        expectancyR: 0.42,
    });
});
