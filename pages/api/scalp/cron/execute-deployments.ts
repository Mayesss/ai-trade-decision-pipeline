export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { listScalpDeploymentRegistryEntries } from '../../../../lib/scalp/deploymentRegistry';
import { runScalpExecuteCycle } from '../../../../lib/scalp/engine';
import type { ScalpMarketSnapshot } from '../../../../lib/scalp/types';

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
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
    return undefined;
}

function parseNowMs(value: string | undefined): number | undefined {
    const num = Number(value);
    if (Number.isFinite(num) && num > 0) return Math.floor(num);
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
    const debug = parseBoolParam(req.query.debug, false);
    const nowMs = parseNowMs(firstQueryValue(req.query.nowMs));
    const symbol = firstQueryValue(req.query.symbol);
    const all = parseBoolParam(req.query.all, false);
    const requirePromotionEligible = parseBoolParam(
        req.query.requirePromotionEligible,
        parseBoolParam(process.env.SCALP_REQUIRE_PROMOTION_ELIGIBLE, false),
    );

    try {
        if (!symbol && !all) {
            return res.status(400).json({
                error: 'symbol_required',
                message: 'Provide ?symbol=... or ?all=true.',
            });
        }

        const deployments = await listScalpDeploymentRegistryEntries({
            symbol,
            enabled: true,
            promotionEligible: requirePromotionEligible ? 'true' : undefined,
        });
        if (!deployments.length) {
            return res.status(200).json({
                ok: true,
                dryRun,
                count: 0,
                results: [],
                message: requirePromotionEligible
                    ? 'No enabled + promotion-eligible deployments matched the request.'
                    : 'No enabled deployments matched the request.',
            });
        }

        const results: Array<Record<string, unknown>> = [];
        const errors: Array<Record<string, unknown>> = [];
        const marketSnapshotCache = new Map<string, ScalpMarketSnapshot>();
        const effectiveNowMs = nowMs ?? Date.now();
        for (const deployment of deployments) {
            try {
                const cycle = await runScalpExecuteCycle({
                    symbol: deployment.symbol,
                    dryRun,
                    debug,
                    nowMs: effectiveNowMs,
                    strategyId: deployment.strategyId,
                    tuneId: deployment.tuneId,
                    deploymentId: deployment.deploymentId,
                    configOverride: deployment.configOverride || undefined,
                    marketSnapshotCache,
                });
                results.push({
                    ...cycle,
                    enabled: deployment.enabled,
                    source: deployment.source,
                    notes: deployment.notes,
                });
            } catch (err: any) {
                errors.push({
                    symbol: deployment.symbol,
                    strategyId: deployment.strategyId,
                    tuneId: deployment.tuneId,
                    deploymentId: deployment.deploymentId,
                    error: err?.message || String(err),
                });
            }
        }

        return res.status(errors.length ? 207 : 200).json({
            ok: errors.length === 0,
            dryRun,
            debug,
            requestedSymbol: symbol || null,
            requestedAll: all,
            requirePromotionEligible,
            count: deployments.length,
            results,
            errors,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'execute_deployments_failed',
            message: err?.message || String(err),
        });
    }
}
