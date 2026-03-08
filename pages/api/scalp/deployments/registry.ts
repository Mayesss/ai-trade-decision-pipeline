export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import {
    filterScalpDeploymentRegistry,
    loadScalpDeploymentRegistry,
    removeScalpDeploymentRegistryEntry,
    scalpDeploymentRegistryKvKey,
    scalpDeploymentRegistryPath,
    scalpDeploymentRegistryStoreMode,
    upsertScalpDeploymentRegistryEntry,
} from '../../../../lib/scalp/deploymentRegistry';
import {
    buildScalpConfigOverrideFromEffectiveConfig,
    compactScalpStrategyConfigOverride,
} from '../../../../lib/scalp/tuning';

type RegistryBody = {
    action?: 'upsert' | 'delete' | string;
    symbol?: string;
    strategyId?: string;
    tuneId?: string;
    deploymentId?: string;
    enabled?: boolean | string;
    source?: 'manual' | 'backtest' | 'matrix' | string;
    notes?: string;
    updatedBy?: string;
    includeTimeframes?: boolean | string;
    configOverride?: unknown;
    effectiveConfig?: unknown;
    leaderboardEntry?: unknown;
    forwardValidation?: unknown;
    promotionGate?: unknown;
};

function parseBool(value: unknown, fallback: boolean): boolean {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'string') {
        const normalized = value.trim().toLowerCase();
        if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
        if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    }
    return fallback;
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value.trim() || undefined;
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
    return undefined;
}

function setNoStoreHeaders(res: NextApiResponse): void {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    if (req.method === 'GET') {
        const snapshot = await loadScalpDeploymentRegistry();
        const deployments = filterScalpDeploymentRegistry(snapshot, {
            symbol: firstQueryValue(req.query.symbol),
            strategyId: firstQueryValue(req.query.strategyId),
            tuneId: firstQueryValue(req.query.tuneId),
            enabled: firstQueryValue(req.query.enabled),
            promotionEligible: firstQueryValue(req.query.promotionEligible),
        });
        return res.status(200).json({
            ok: true,
            registryStore: scalpDeploymentRegistryStoreMode(),
            registryPath: scalpDeploymentRegistryPath(),
            registryKvKey: scalpDeploymentRegistryKvKey(),
            updatedAt: snapshot.updatedAt,
            count: deployments.length,
            deployments,
        });
    }

    if (req.method !== 'POST') {
        return res.status(405).json({ error: 'method_not_allowed', message: 'Use GET or POST' });
    }

    try {
        const body = (req.body || {}) as RegistryBody;
        const action = String(body.action || 'upsert').trim().toLowerCase();

        if (action === 'delete') {
            const removed = await removeScalpDeploymentRegistryEntry({
                symbol: body.symbol,
                strategyId: body.strategyId,
                tuneId: body.tuneId,
                deploymentId: body.deploymentId,
            });
            return res.status(200).json({
                ok: true,
                action,
                removed: removed.removed,
                deploymentId: removed.deploymentId,
                updatedAt: removed.snapshot.updatedAt,
                count: removed.snapshot.deployments.length,
                deployments: removed.snapshot.deployments,
            });
        }

        const configOverride =
            compactScalpStrategyConfigOverride(body.configOverride) ||
            buildScalpConfigOverrideFromEffectiveConfig(body.effectiveConfig, {
                includeTimeframes: parseBool(body.includeTimeframes, false),
            });
        const upserted = await upsertScalpDeploymentRegistryEntry({
            symbol: body.symbol,
            strategyId: body.strategyId,
            tuneId: body.tuneId,
            deploymentId: body.deploymentId,
            enabled: body.enabled,
            source: body.source,
            notes: body.notes,
            updatedBy: body.updatedBy,
            leaderboardEntry: body.leaderboardEntry,
            forwardValidation: body.forwardValidation,
            promotionGate: body.promotionGate,
            configOverride,
        });
        return res.status(200).json({
            ok: true,
            action: 'upsert',
            updatedAt: upserted.snapshot.updatedAt,
            count: upserted.snapshot.deployments.length,
            entry: upserted.entry,
            deployments: upserted.snapshot.deployments,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'scalp_deployment_registry_failed',
            message: err?.message || String(err),
        });
    }
}
