import assert from 'node:assert/strict';
import test from 'node:test';

import { getScalpCronDeploymentConfigs, getScalpCronSymbolConfigs } from '../../symbolRegistry';

test('scalp cron registry parses deployment execution routes from vercel scheduling', () => {
    const deploymentConfigs = getScalpCronDeploymentConfigs();
    const symbolConfigs = getScalpCronSymbolConfigs();

    const allDeployments = deploymentConfigs.find((row) => row.symbol === '*');

    assert.ok(allDeployments);
    assert.equal(allDeployments!.route, 'execute-deployments');
    assert.match(allDeployments!.path, /\/api\/scalp\/cron\/execute-deployments\?all=true/);

    const symbolRoutes = new Map(symbolConfigs.map((row) => [row.symbol, row.route]));
    assert.equal(symbolRoutes.get('*'), 'execute-deployments');
});
