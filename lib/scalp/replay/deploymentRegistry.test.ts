import assert from 'node:assert/strict';
import { mkdtemp, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
    canonicalizeScalpDeploymentRegistry,
    filterScalpDeploymentRegistry,
    loadScalpDeploymentRegistry,
    removeScalpDeploymentRegistryEntry,
    upsertScalpDeploymentRegistryEntriesBulk,
    upsertScalpDeploymentRegistryEntry,
} from '../deploymentRegistry';

test('deployment registry upserts, filters, and removes tuned deployments via file storage', async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), 'scalp-deployments-'));
    const prevPath = process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH;
    const prevStore = process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE;
    const prevAllowFile = process.env.ALLOW_SCALP_FILE_BACKEND;
    process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH = path.join(dir, 'registry.json');
    process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE = 'file';
    process.env.ALLOW_SCALP_FILE_BACKEND = '1';

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
                deploymentId: 'bitget:EURUSD~regime_pullback_m15_m3~return_a',
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
        assert.equal(upserted.entry.enabled, false);
        assert.equal(upserted.entry.promotionGate?.eligible, false);
        assert.equal(upserted.entry.promotionGate?.reason, 'missing_forward_validation');
        assert.equal(upserted.snapshot.deployments.length, 1);

        const promoted = await upsertScalpDeploymentRegistryEntry({
            deploymentId: 'bitget:EURUSD~regime_pullback_m15_m3~return_a',
            enabled: true,
            source: 'matrix',
            updatedBy: 'test',
            forwardValidation: {
                rollCount: 18,
                profitableWindowPct: 61,
                meanExpectancyR: 0.08,
                meanProfitFactor: 1.11,
                maxDrawdownR: 6.4,
                minTradesPerWindow: 3,
                selectionWindowDays: 90,
                forwardWindowDays: 28,
                confirmationWindowDays: 364,
                confirmationRollCount: 18,
                confirmationProfitableWindowPct: 61,
                confirmationMeanExpectancyR: 0.08,
                confirmationMeanProfitFactor: 1.11,
                confirmationMaxDrawdownR: 6.4,
                confirmationMinTradesPerWindow: 3,
                confirmationTotalTrades: 54,
                confirmationEvaluatedAtMs: 1234567890,
            },
        });
        assert.equal(promoted.entry.enabled, true);
        assert.equal(promoted.entry.promotionGate?.eligible, true);
        assert.equal(promoted.entry.promotionGate?.source, 'walk_forward');
        assert.equal(promoted.entry.promotionGate?.forwardValidation?.confirmationWindowDays, 364);
        assert.equal(promoted.entry.promotionGate?.forwardValidation?.confirmationTotalTrades, 54);

        const loaded = await loadScalpDeploymentRegistry();
        const filtered = filterScalpDeploymentRegistry(loaded, {
            symbol: 'EURUSD',
            enabled: 'true',
            promotionEligible: 'true',
        });
        assert.equal(filtered.length, 1);
        assert.equal(filtered[0]?.deploymentId, 'bitget:EURUSD~regime_pullback_m15_m3~return_a');
        assert.deepEqual(filtered[0]?.configOverride, { risk: { takeProfitR: 1.4 } });

        const removed = await removeScalpDeploymentRegistryEntry({
            deploymentId: 'bitget:EURUSD~regime_pullback_m15_m3~return_a',
        });
        assert.equal(removed.removed, true);
        assert.equal(removed.snapshot.deployments.length, 0);
    } finally {
        if (prevPath === undefined) {
            delete process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH;
        } else {
            process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH = prevPath;
        }
        if (prevStore === undefined) {
            delete process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE;
        } else {
            process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE = prevStore;
        }
        if (prevAllowFile === undefined) {
            delete process.env.ALLOW_SCALP_FILE_BACKEND;
        } else {
            process.env.ALLOW_SCALP_FILE_BACKEND = prevAllowFile;
        }
    }
});

test('canonicalizeScalpDeploymentRegistry rewrites legacy guarded strategy rows to root strategy ids', async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), 'scalp-deployments-canonicalize-'));
    const filePath = path.join(dir, 'registry.json');
    const prevPath = process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH;
    const prevStore = process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE;
    const prevAllowFile = process.env.ALLOW_SCALP_FILE_BACKEND;
    const prevVariant = process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_VARIANT;
    const prevHours = process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_BERLIN;
    const prevExperiment = process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_EXPERIMENT;
    const prevTp1 = process.env.SCALP_BTCUSDT_GUARD_TP1_CLOSE_PCT;
    const prevTrail = process.env.SCALP_BTCUSDT_GUARD_TRAIL_ATR_MULT;
    const prevTimeStop = process.env.SCALP_BTCUSDT_GUARD_TIME_STOP_BARS;

    process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH = filePath;
    process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE = 'file';
    process.env.ALLOW_SCALP_FILE_BACKEND = '1';
    delete process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_VARIANT;
    delete process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_BERLIN;
    delete process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_EXPERIMENT;
    delete process.env.SCALP_BTCUSDT_GUARD_TP1_CLOSE_PCT;
    delete process.env.SCALP_BTCUSDT_GUARD_TRAIL_ATR_MULT;
    delete process.env.SCALP_BTCUSDT_GUARD_TIME_STOP_BARS;

    try {
        await writeFile(
            filePath,
            `${JSON.stringify(
                {
                    version: 1,
                    updatedAt: '2026-03-01T00:00:00.000Z',
                    deployments: [
                        {
                            symbol: 'BTCUSDT',
                            strategyId: 'regime_pullback_m15_m3_btcusdt',
                            tuneId: 'guarded_high_pf_default',
                            deploymentId: 'BTCUSDT~regime_pullback_m15_m3_btcusdt~guarded_high_pf_default',
                            enabled: true,
                            source: 'backtest',
                            configOverride: null,
                            leaderboardEntry: {
                                symbol: 'BTCUSDT',
                                strategyId: 'regime_pullback_m15_m3_btcusdt',
                                tuneId: 'guarded_high_pf_default',
                                deploymentId: 'BTCUSDT~regime_pullback_m15_m3_btcusdt~guarded_high_pf_default',
                                tuneLabel: 'guarded_high_pf_default',
                                netR: 1.5,
                                profitFactor: 1.1,
                                maxDrawdownR: 2.2,
                                trades: 11,
                                winRatePct: 50,
                                avgHoldMinutes: 34,
                                expectancyR: 0.14,
                            },
                            createdAtMs: 1000,
                            updatedAtMs: 3000,
                            updatedBy: 'legacy',
                        },
                        {
                            symbol: 'BTCUSDT',
                            strategyId: 'regime_pullback_m15_m3',
                            tuneId: 'guarded_high_pf_default',
                            deploymentId: 'BTCUSDT~regime_pullback_m15_m3~guarded_high_pf_default',
                            enabled: true,
                            source: 'backtest',
                            configOverride: {
                                risk: {
                                    tp1ClosePct: 99,
                                },
                            },
                            createdAtMs: 1000,
                            updatedAtMs: 2000,
                            updatedBy: 'newer',
                        },
                    ],
                },
                null,
                2,
            )}\n`,
            'utf8',
        );

        const dryRun = await canonicalizeScalpDeploymentRegistry({ dryRun: true });
        assert.equal(dryRun.wrote, false);
        assert.equal(dryRun.beforeCount, 2);
        assert.equal(dryRun.afterCount, 1);
        assert.equal(dryRun.dedupedCount, 1);
        assert.equal(dryRun.legacyStrategyRows, 1);
        assert.equal(dryRun.legacyDeploymentIdRows, 1);

        const applied = await canonicalizeScalpDeploymentRegistry({ dryRun: false });
        assert.equal(applied.wrote, true);
        assert.equal(applied.afterCount, 1);

        const loaded = await loadScalpDeploymentRegistry();
        assert.equal(loaded.deployments.length, 1);
        const row = loaded.deployments[0];
        assert.ok(row);
        assert.equal(row.strategyId, 'regime_pullback_m15_m3');
        assert.equal(row.deploymentId, 'bitget:BTCUSDT~regime_pullback_m15_m3~guarded_high_pf_default');
        assert.equal(row.leaderboardEntry?.strategyId, 'regime_pullback_m15_m3');
        assert.equal(
            row.leaderboardEntry?.deploymentId,
            'bitget:BTCUSDT~regime_pullback_m15_m3~guarded_high_pf_default',
        );
        assert.equal(row.configOverride?.risk?.tp1ClosePct, 20);
        assert.equal(row.configOverride?.risk?.trailAtrMult, 1.4);
        assert.equal(row.configOverride?.risk?.timeStopBars, 15);
        assert.deepEqual(row.configOverride?.sessions?.blockedBerlinEntryHours, [10, 11]);
        assert.equal(row.configOverride?.confirm?.allowPullbackSwingBreakTrigger, true);
    } finally {
        if (prevPath === undefined) {
            delete process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH;
        } else {
            process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH = prevPath;
        }
        if (prevStore === undefined) {
            delete process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE;
        } else {
            process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE = prevStore;
        }
        if (prevAllowFile === undefined) delete process.env.ALLOW_SCALP_FILE_BACKEND;
        else process.env.ALLOW_SCALP_FILE_BACKEND = prevAllowFile;
        if (prevVariant === undefined) delete process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_VARIANT;
        else process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_VARIANT = prevVariant;
        if (prevHours === undefined) delete process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_BERLIN;
        else process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_BERLIN = prevHours;
        if (prevExperiment === undefined) delete process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_EXPERIMENT;
        else process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_EXPERIMENT = prevExperiment;
        if (prevTp1 === undefined) delete process.env.SCALP_BTCUSDT_GUARD_TP1_CLOSE_PCT;
        else process.env.SCALP_BTCUSDT_GUARD_TP1_CLOSE_PCT = prevTp1;
        if (prevTrail === undefined) delete process.env.SCALP_BTCUSDT_GUARD_TRAIL_ATR_MULT;
        else process.env.SCALP_BTCUSDT_GUARD_TRAIL_ATR_MULT = prevTrail;
        if (prevTimeStop === undefined) delete process.env.SCALP_BTCUSDT_GUARD_TIME_STOP_BARS;
        else process.env.SCALP_BTCUSDT_GUARD_TIME_STOP_BARS = prevTimeStop;
    }
});

test('deployment registry bulk upsert applies multiple rows and resolves duplicate deployment ids by last write', async () => {
    const dir = await mkdtemp(path.join(os.tmpdir(), 'scalp-deployments-bulk-'));
    const prevPath = process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH;
    const prevStore = process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE;
    const prevAllowFile = process.env.ALLOW_SCALP_FILE_BACKEND;
    process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH = path.join(dir, 'registry.json');
    process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE = 'file';
    process.env.ALLOW_SCALP_FILE_BACKEND = '1';

    try {
        const out = await upsertScalpDeploymentRegistryEntriesBulk([
            {
                symbol: 'eurusd',
                strategyId: 'regime_pullback_m15_m3',
                tuneId: 'return_a',
                source: 'matrix',
                updatedBy: 'test-bulk',
            },
            {
                symbol: 'eurusd',
                strategyId: 'regime_pullback_m15_m3',
                tuneId: 'return_b',
                source: 'matrix',
                updatedBy: 'test-bulk',
            },
            {
                deploymentId: 'bitget:EURUSD~regime_pullback_m15_m3~return_a',
                source: 'matrix',
                enabled: true,
                updatedBy: 'test-bulk',
                forwardValidation: {
                    rollCount: 12,
                    profitableWindowPct: 58,
                    meanExpectancyR: 0.04,
                    meanProfitFactor: 1.06,
                    maxDrawdownR: 4.2,
                    minTradesPerWindow: 2,
                    selectionWindowDays: 90,
                    forwardWindowDays: 28,
                },
            },
        ]);

        assert.equal(out.entries.length, 3);
        const loaded = await loadScalpDeploymentRegistry();
        assert.equal(loaded.deployments.length, 2);

        const returnA = loaded.deployments.find(
            (row) => row.deploymentId === 'bitget:EURUSD~regime_pullback_m15_m3~return_a',
        );
        assert.ok(returnA);
        assert.equal(returnA.enabled, true);
        assert.equal(returnA.promotionGate?.eligible, true);
    } finally {
        if (prevPath === undefined) {
            delete process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH;
        } else {
            process.env.SCALP_DEPLOYMENTS_REGISTRY_PATH = prevPath;
        }
        if (prevStore === undefined) {
            delete process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE;
        } else {
            process.env.SCALP_DEPLOYMENTS_REGISTRY_STORE = prevStore;
        }
        if (prevAllowFile === undefined) {
            delete process.env.ALLOW_SCALP_FILE_BACKEND;
        } else {
            process.env.ALLOW_SCALP_FILE_BACKEND = prevAllowFile;
        }
    }
});
