export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { getScalpStrategyConfig } from '../../../../lib/scalp/config';
import { loadScalpStrategyControlSnapshot, setScalpStrategyKvEnabled } from '../../../../lib/scalp/store';

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

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    const cfg = getScalpStrategyConfig();

    try {
        if (req.method === 'GET') {
            const strategy = await loadScalpStrategyControlSnapshot(cfg.enabled);
            return res.status(200).json({
                mode: 'scalp',
                strategy,
            });
        }

        if (req.method === 'POST') {
            const body = req.body && typeof req.body === 'object' ? (req.body as Record<string, unknown>) : {};
            const enabled = parseBool(body.enabled);
            if (enabled === null) {
                return res.status(400).json({
                    error: 'invalid_enabled',
                    message: 'Provide `enabled` as a boolean value.',
                });
            }

            const strategy = await setScalpStrategyKvEnabled({
                enabled,
                envEnabled: cfg.enabled,
                updatedBy: parseUpdatedBy(body.updatedBy) || 'dashboard-ui',
            });

            return res.status(200).json({
                ok: true,
                strategy,
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
