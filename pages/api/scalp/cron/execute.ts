export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { getScalpStrategyConfig, normalizeScalpSymbol } from '../../../../lib/scalp/config';
import { runScalpExecuteCycle } from '../../../../lib/scalp/engine';
import { loadScalpStrategyRuntimeSnapshot } from '../../../../lib/scalp/store';

function parseBoolParam(value: string | string[] | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const first = Array.isArray(value) ? value[0] : value;
    if (first === undefined) return fallback;
    const normalized = String(first).trim().toLowerCase();
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    return fallback;
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value.trim() || undefined;
    if (Array.isArray(value) && value.length > 0) {
        return String(value[0] || '').trim() || undefined;
    }
    return undefined;
}

function parseNowMs(value: string | undefined): number | undefined {
    const num = Number(value);
    if (Number.isFinite(num) && num > 0) return num;
    return undefined;
}

function setNoStoreHeaders(res: NextApiResponse): void {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;

    setNoStoreHeaders(res);

    const dryRun = parseBoolParam(req.query.dryRun, true);
    const symbol = firstQueryValue(req.query.symbol);
    const nowMs = parseNowMs(firstQueryValue(req.query.nowMs));
    const strategyId = firstQueryValue(req.query.strategyId);

    try {
        const cfg = getScalpStrategyConfig();
        const normalizedSymbol = normalizeScalpSymbol(symbol || cfg.defaultSymbol);
        const runtime = await loadScalpStrategyRuntimeSnapshot(cfg.enabled, strategyId);
        const strategy = runtime.strategy;
        if (!strategy.enabled) {
            const reasonCodes = strategy.envEnabled
                ? ['SCALP_ENGINE_DISABLED', 'SCALP_STRATEGY_DISABLED_BY_KV']
                : ['SCALP_ENGINE_DISABLED', 'SCALP_ENGINE_DISABLED_BY_ENV'];
            return res.status(200).json({
                ok: true,
                skipped: true,
                symbol: normalizedSymbol,
                dryRun,
                strategyId: strategy.strategyId,
                defaultStrategyId: runtime.defaultStrategyId,
                strategy,
                strategies: runtime.strategies,
                reasonCodes,
            });
        }
        const result = await runScalpExecuteCycle({
            symbol: normalizedSymbol,
            dryRun,
            nowMs,
            strategyId: strategy.strategyId,
        });
        return res.status(200).json({
            ok: true,
            defaultStrategyId: runtime.defaultStrategyId,
            strategy,
            strategies: runtime.strategies,
            ...result,
        });
    } catch (err: any) {
        console.error('Error in /api/scalp/cron/execute:', err);
        return res.status(500).json({ error: err?.message || String(err) });
    }
}
