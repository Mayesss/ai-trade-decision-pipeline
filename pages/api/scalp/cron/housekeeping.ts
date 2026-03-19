export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { runScalpHousekeeping } from '../../../../lib/scalp/housekeeping';

function firstQueryValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value.trim() || undefined;
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
    return undefined;
}

function parseBoolParam(value: string | string[] | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const first = Array.isArray(value) ? value[0] : value;
    if (first === undefined) return fallback;
    const normalized = String(first).trim().toLowerCase();
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    return fallback;
}

function parsePositiveInt(value: string | undefined): number | undefined {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return undefined;
    return Math.floor(n);
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

    const dryRun = parseBoolParam(req.query.dryRun, false);
    const cycleRetentionDays = parsePositiveInt(firstQueryValue(req.query.cycleRetentionDays));
    const inactiveSymbolRetentionDays = parsePositiveInt(
        firstQueryValue(req.query.inactiveSymbolRetentionDays),
    );
    const lockMaxAgeMinutes = parsePositiveInt(firstQueryValue(req.query.lockMaxAgeMinutes));
    const maxScanKeys = parsePositiveInt(firstQueryValue(req.query.maxScanKeys));
    const refreshReport = parseBoolParam(req.query.refreshReport, false);
    const cleanupOrphanDeployments = parseBoolParam(req.query.cleanupOrphanDeployments, true);
    const candleHistoryKeepWeeks = parsePositiveInt(firstQueryValue(req.query.candleHistoryKeepWeeks));
    const candleHistoryTimeframe = firstQueryValue(req.query.candleHistoryTimeframe);

    try {
        const out = await runScalpHousekeeping({
            dryRun,
            cycleRetentionDays,
            inactiveSymbolRetentionDays,
            lockMaxAgeMinutes,
            maxScanKeys,
            refreshReport,
            cleanupOrphanDeployments,
            candleHistoryKeepWeeks,
            candleHistoryTimeframe,
        });

        return res.status(200).json({
            ok: out.ok,
            dryRun: out.dryRun,
            generatedAtMs: out.generatedAtMs,
            generatedAtIso: out.generatedAtIso,
            config: out.config,
            summary: out.summary,
            details: out.details,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'scalp_housekeeping_failed',
            message: err?.message || String(err),
        });
    }
}
