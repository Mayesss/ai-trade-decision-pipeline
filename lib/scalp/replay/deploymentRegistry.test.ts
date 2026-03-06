import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
    filterScalpDeploymentRegistry,
    loadScalpDeploymentRegistry,
    removeScalpDeploymentRegistryEntry,
    upsertScalpDeploymentRegistryEntry,
} from '../deploymentRegistry';

test('deployment registry upserts, filters, and removes tuned deployments via file storage', async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), 'scalp-deployments-'));
    const prevPath = process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH;
    process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH = path.join(dir, 'registry.json');

    try {
        const upserted = await upsertScalpDeploymentRegistryEntry({
            symbol: 'eurusd',
            strategyId: 'regime_pullback_m15_m3',
            tuneId: 'return_a',
            source: 'matrix',
            updatedBy: 'test',
            leaderboardEntry: {
                symbol: 'EURUSD',
                strategyId: 'regime_pullback_m15_m3',
                tuneId: 'return_a',
                deploymentId: 'EURUSD~regime_pullback_m15_m3~return_a',
                tuneLabel: 'return_a',
                netR: 3.2,
                profitFactor: 1.8,
                maxDrawdownR: 1.1,
                trades: 10,
                winRatePct: 55,
                avgHoldMinutes: 22,
                expectancyR: 0.32,
            },
            configOverride: {
                risk: {
                    takeProfitR: 1.4,
                },
            },
        });

        assert.equal(upserted.entry.symbol, 'EURUSD');
        assert.equal(upserted.entry.tuneId, 'return_a');
        assert.equal(upserted.snapshot.deployments.length, 1);

        const loaded = await loadScalpDeploymentRegistry();
        const filtered = filterScalpDeploymentRegistry(loaded, { symbol: 'EURUSD', enabled: 'true' });
        assert.equal(filtered.length, 1);
        assert.equal(filtered[0]?.deploymentId, 'EURUSD~regime_pullback_m15_m3~return_a');
        assert.deepEqual(filtered[0]?.configOverride, { risk: { takeProfitR: 1.4 } });

        const removed = await removeScalpDeploymentRegistryEntry({
            deploymentId: 'EURUSD~regime_pullback_m15_m3~return_a',
        });
        assert.equal(removed.removed, true);
        assert.equal(removed.snapshot.deployments.length, 0);
    } finally {
        if (prevPath === undefined) {
            delete process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH;
        } else {
            process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH = prevPath;
        }
    }
});
