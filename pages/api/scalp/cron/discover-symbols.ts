export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { runScalpSymbolDiscoveryCycle } from '../../../../lib/scalp/symbolDiscovery';

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
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return undefined;
    return Math.floor(n);
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
    const includeLiveQuotes = parseBoolParam(req.query.includeLiveQuotes, true);
    const nowMs = parseNowMs(firstQueryValue(req.query.nowMs));
    const maxCandidates = parsePositiveInt(firstQueryValue(req.query.maxCandidates));

    try {
        const snapshot = await runScalpSymbolDiscoveryCycle({
            dryRun,
            includeLiveQuotes,
            nowMs,
            maxCandidatesOverride: maxCandidates,
        });

        return res.status(200).json({
            ok: true,
            dryRun,
            includeLiveQuotes,
            generatedAtIso: snapshot.generatedAtIso,
            selectedCount: snapshot.selectedSymbols.length,
            candidatesEvaluated: snapshot.candidatesEvaluated,
            selectedSymbols: snapshot.selectedSymbols,
            addedSymbols: snapshot.addedSymbols,
            removedSymbols: snapshot.removedSymbols,
            topSelectedRows: snapshot.selectedRows.slice(0, 10),
            topRejectedRows: snapshot.topRejectedRows.slice(0, 10),
            snapshot,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'scalp_symbol_discovery_failed',
            message: err?.message || String(err),
        });
    }
}
