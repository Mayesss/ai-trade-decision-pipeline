import assert from 'node:assert/strict';
import test from 'node:test';

import { getScalpCronDeploymentConfigs, getScalpCronSymbolConfigs } from '../../symbolRegistry';

test('scalp cron registry parses deployment execution routes from vercel scheduling', () => {
    const deploymentConfigs = getScalpCronDeploymentConfigs();
    const symbolConfigs = getScalpCronSymbolConfigs();

    const xau = deploymentConfigs.find((row) => row.symbol === 'XAUUSDT');
    const btc = deploymentConfigs.find((row) => row.symbol === 'BTCUSDT');

    assert.ok(xau);
    assert.ok(btc);
    assert.equal(xau!.route, 'execute-deployments');
    assert.equal(btc!.route, 'execute-deployments');
    assert.match(xau!.path, /\/api\/scalp\/cron\/execute-deployments\?symbol=XAUUSDT/);
    assert.match(btc!.path, /\/api\/scalp\/cron\/execute-deployments\?symbol=BTCUSDT/);

    const symbolRoutes = new Map(symbolConfigs.map((row) => [row.symbol, row.route]));
    assert.equal(symbolRoutes.get('XAUUSDT'), 'execute-deployments');
    assert.equal(symbolRoutes.get('BTCUSDT'), 'execute-deployments');
});
