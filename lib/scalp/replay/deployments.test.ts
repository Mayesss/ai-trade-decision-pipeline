import assert from 'node:assert/strict';
import test from 'node:test';

import { buildScalpDeploymentId, parseScalpDeploymentId, resolveScalpDeployment } from '../deployments';
import { getDefaultScalpStrategy } from '../strategies/registry';

test('resolveScalpDeployment normalizes explicit symbol, strategy, and tune inputs', () => {
    const deployment = resolveScalpDeployment({
        venue: 'bitget',
        symbol: 'eurusd',
        strategyId: 'REGIME_PULLBACK_M15_M3',
        tuneId: 'London Return V1!!',
    });

    assert.equal(deployment.symbol, 'EURUSD');
    assert.equal(deployment.venue, 'bitget');
    assert.equal(deployment.strategyId, 'regime_pullback_m15_m3');
    assert.equal(deployment.tuneId, 'london-return-v1');
    assert.equal(deployment.deploymentId, 'bitget:EURUSD~regime_pullback_m15_m3~london-return-v1');
    assert.equal(deployment.tuneLabel, 'london-return-v1');
});

test('parseScalpDeploymentId round-trips a deployment key for later live ownership tracking', () => {
    const strategyId = getDefaultScalpStrategy().id;
    const deploymentId = buildScalpDeploymentId({
        venue: 'bitget',
        symbol: 'GBPUSD',
        strategyId,
        tuneId: 'session_a',
    });

    const parsed = parseScalpDeploymentId(deploymentId);

    assert.ok(parsed);
    assert.equal(parsed?.venue, 'bitget');
    assert.equal(parsed?.symbol, 'GBPUSD');
    assert.equal(parsed?.strategyId, strategyId);
    assert.equal(parsed?.tuneId, 'session_a');
    assert.equal(parsed?.deploymentId, deploymentId);
});

test('buildScalpDeploymentId always prefixes venue to avoid legacy collisions', () => {
    const strategyId = getDefaultScalpStrategy().id;
    const deploymentId = buildScalpDeploymentId({
        venue: 'bitget',
        symbol: 'BTCUSDT',
        strategyId,
        tuneId: 'default',
    });

    assert.equal(deploymentId, `bitget:BTCUSDT~${strategyId}~default`);
    const parsed = parseScalpDeploymentId(deploymentId);
    assert.ok(parsed);
    assert.equal(parsed?.venue, 'bitget');
    assert.equal(parsed?.symbol, 'BTCUSDT');
});

test('resolveScalpDeployment keeps deployment-id tune suffix when tuneId also provided', () => {
    const strategyId = getDefaultScalpStrategy().id;
    const deploymentId = `bitget:LINKUSDT~${strategyId}~mdl_basis_m5_m3_xftp_2a87b3bea4__sp_sydney`;

    const resolved = resolveScalpDeployment({
        deploymentId,
        symbol: 'LINKUSDT',
        strategyId,
        tuneId: 'mdl_basis_m5_m3_xftp_2a87b3bea4',
    });

    assert.equal(resolved.deploymentId, deploymentId);
    assert.equal(resolved.tuneId, 'mdl_basis_m5_m3_xftp_2a87b3bea4__sp_sydney');
});

test('parseScalpDeploymentId treats unprefixed ids as default venue ids', () => {
    const strategyId = getDefaultScalpStrategy().id;
    const parsed = parseScalpDeploymentId(`EURUSD~${strategyId}~default`);
    assert.ok(parsed);
    assert.equal(parsed?.venue, 'bitget');
    assert.equal(parsed?.symbol, 'EURUSD');
});
