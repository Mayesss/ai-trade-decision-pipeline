import assert from 'node:assert/strict';
import test from 'node:test';

import { buildScalpDeploymentId, parseScalpDeploymentId, resolveScalpDeployment } from '../deployments';
import { getDefaultScalpStrategy } from '../strategies/registry';

test('resolveScalpDeployment normalizes explicit symbol, strategy, and tune inputs', () => {
    const deployment = resolveScalpDeployment({
        symbol: 'eurusd',
        strategyId: 'REGIME_PULLBACK_M15_M3',
        tuneId: 'London Return V1!!',
    });

    assert.equal(deployment.symbol, 'EURUSD');
    assert.equal(deployment.strategyId, 'regime_pullback_m15_m3');
    assert.equal(deployment.tuneId, 'london-return-v1');
    assert.equal(deployment.deploymentId, 'EURUSD~regime_pullback_m15_m3~london-return-v1');
    assert.equal(deployment.tuneLabel, 'london-return-v1');
});

test('parseScalpDeploymentId round-trips a deployment key for later live ownership tracking', () => {
    const strategyId = getDefaultScalpStrategy().id;
    const deploymentId = buildScalpDeploymentId({
        symbol: 'GBPUSD',
        strategyId,
        tuneId: 'session_a',
    });

    const parsed = parseScalpDeploymentId(deploymentId);

    assert.ok(parsed);
    assert.equal(parsed?.symbol, 'GBPUSD');
    assert.equal(parsed?.strategyId, strategyId);
    assert.equal(parsed?.tuneId, 'session_a');
    assert.equal(parsed?.deploymentId, deploymentId);
});
