export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { getScalpStrategyConfig } from '../../../../lib/scalp/config';
import {
    loadScalpStrategyRuntimeSnapshot,
    setScalpDefaultStrategy,
    setScalpStrategyKvEnabled,
} from '../../../../lib/scalp/store';

function setNoStoreHeaders(res: NextApiResponse): void {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

function parseBool(value: unknown): boolean | null {
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') {
        if (value === 1) return true;
        if (value === 0) return false;
        return null;
    }
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return null;
}

function parseUpdatedBy(value: unknown): string | null {
    const normalized = String(value || '').trim();
    if (!normalized) return null;
    return normalized.slice(0, 120);
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value.trim() || undefined;
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
    return undefined;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    const cfg = getScalpStrategyConfig();

    try {
        if (req.method === 'GET') {
            const strategyId = firstQueryValue(req.query.strategyId);
            const runtime = await loadScalpStrategyRuntimeSnapshot(cfg.enabled, strategyId);
            return res.status(200).json({
                mode: 'scalp',
                strategyId: runtime.strategyId,
                defaultStrategyId: runtime.defaultStrategyId,
                strategy: runtime.strategy,
                strategies: runtime.strategies,
            });
        }

        if (req.method === 'POST') {
            const body = req.body && typeof req.body === 'object' ? (req.body as Record<string, unknown>) : {};
            const enabled = parseBool(body.enabled);
            const requestedStrategyId = String(body.strategyId || '').trim() || undefined;
            const defaultStrategyId = String(body.defaultStrategyId || '').trim() || undefined;
            const updatedBy = parseUpdatedBy(body.updatedBy) || 'dashboard-ui';
            let runtime = await loadScalpStrategyRuntimeSnapshot(cfg.enabled, requestedStrategyId);

            if (enabled !== null) {
                runtime = await setScalpStrategyKvEnabled({
                    strategyId: requestedStrategyId || runtime.strategyId,
                    enabled,
                    envEnabled: cfg.enabled,
                    updatedBy,
                });
            }

            if (defaultStrategyId) {
                runtime = await setScalpDefaultStrategy({
                    strategyId: defaultStrategyId,
                    envEnabled: cfg.enabled,
                });
            }

            if (enabled === null && !defaultStrategyId) {
                return res.status(400).json({
                    error: 'invalid_payload',
                    message: 'Provide `enabled` (boolean) and/or `defaultStrategyId`.',
                });
            }

            return res.status(200).json({
                ok: true,
                strategyId: runtime.strategyId,
                defaultStrategyId: runtime.defaultStrategyId,
                strategy: runtime.strategy,
                strategies: runtime.strategies,
            });
        }

        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET or POST' });
    } catch (err: any) {
        return res.status(500).json({
            error: 'scalp_strategy_control_failed',
            message: err?.message || String(err),
        });
    }
}
