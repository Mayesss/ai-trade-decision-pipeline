export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { runScalpExecuteCycle } from '../../../../lib/scalp/engine';
import { getScalpHybridPolicy, listScalpHybridSymbols, resolveScalpHybridSelection } from '../../../../lib/scalp/hybridPolicy';

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
    const nowMs = parseNowMs(firstQueryValue(req.query.nowMs));
    const symbol = firstQueryValue(req.query.symbol);
    const forceProfile = firstQueryValue(req.query.profile);
    const runAll = parseBoolParam(req.query.all, false);

    try {
        const policy = getScalpHybridPolicy();
        if (forceProfile && !policy.profiles[forceProfile]) {
            return res.status(400).json({
                error: 'invalid_profile',
                message: `Unknown profile: ${forceProfile}`,
                availableProfiles: Object.keys(policy.profiles),
            });
        }
        const symbols = symbol ? [symbol] : runAll ? listScalpHybridSymbols(policy) : [];
        if (!symbols.length) {
            return res.status(400).json({
                error: 'symbol_required',
                message: 'Provide ?symbol=... or ?all=true.',
            });
        }

        const results: any[] = [];
        const errors: Array<{ symbol: string; profile: string; error: string }> = [];
        for (const symbolValue of symbols) {
            let selectedProfile = policy.defaultProfile;
            try {
                const selection = resolveScalpHybridSelection(symbolValue, policy, forceProfile);
                selectedProfile = selection.profile;
                const cycle = await runScalpExecuteCycle({
                    symbol: selection.symbol,
                    dryRun,
                    nowMs,
                    configOverride: selection.configOverride,
                });
                results.push({
                    ...cycle,
                    profile: selection.profile,
                });
            } catch (err: any) {
                errors.push({
                    symbol: String(symbolValue || '').toUpperCase(),
                    profile: selectedProfile,
                    error: err?.message || String(err),
                });
            }
        }

        const ok = errors.length === 0;
        return res.status(ok || results.length > 0 ? 200 : 500).json({
            ok,
            dryRun,
            requestedSymbol: symbol || null,
            requestedAll: runAll,
            forceProfile: forceProfile || null,
            policy: {
                version: policy.version,
                defaultProfile: policy.defaultProfile,
                symbolProfileCount: Object.keys(policy.symbolProfiles).length,
            },
            results,
            errors,
        });
    } catch (err: any) {
        console.error('Error in /api/scalp/cron/execute-hybrid:', err);
        return res.status(500).json({
            error: 'execute_hybrid_failed',
            message: err?.message || String(err),
        });
    }
}
