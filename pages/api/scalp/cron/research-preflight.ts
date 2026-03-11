export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { evaluateResearchCyclePreflight } from '../../../../lib/scalp/researchCycle';

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

function parsePositiveInt(value: string | undefined): number | undefined {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return undefined;
    return Math.floor(n);
}

function parseSymbolsCsv(value: string | undefined): string[] {
    if (!value) return [];
    return value
        .split(',')
        .map((row) =>
            String(row || '')
                .trim()
                .toUpperCase()
                .replace(/[^A-Z0-9._-]/g, ''),
        )
        .filter((row) => Boolean(row));
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

    try {
        const preflight = await evaluateResearchCyclePreflight({
            symbols: parseSymbolsCsv(firstQueryValue(req.query.symbols)),
            minCandlesPerTask: parsePositiveInt(firstQueryValue(req.query.minCandlesPerTask)),
            requireUniverseSnapshot: req.query.requireUniverseSnapshot === undefined
                ? undefined
                : parseBoolParam(req.query.requireUniverseSnapshot, true),
            requireReportSnapshot: req.query.requireReportSnapshot === undefined
                ? undefined
                : parseBoolParam(req.query.requireReportSnapshot, true),
            maxCandleChecks: parsePositiveInt(firstQueryValue(req.query.maxCandleChecks)),
        });

        const status = preflight.ready ? 200 : 409;
        return res.status(status).json({
            ok: preflight.ready,
            preflight,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'research_preflight_failed',
            message: err?.message || String(err),
        });
    }
}
