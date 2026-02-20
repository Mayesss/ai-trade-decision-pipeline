export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { loadForexJournal } from '../../../../lib/forex/store';

function setNoStoreHeaders(res: NextApiResponse) {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

function parseIntParam(value: string | string[] | undefined, fallback: number) {
    const raw = Array.isArray(value) ? value[0] : value;
    const n = Number(raw);
    if (!Number.isFinite(n)) return fallback;
    return Math.floor(n);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;

    setNoStoreHeaders(res);

    const limit = Math.min(1000, Math.max(1, parseIntParam(req.query.limit as string | string[] | undefined, 200)));
    const pair = String(req.query.pair || '')
        .trim()
        .toUpperCase();

    try {
        const rows = await loadForexJournal(limit * 2);
        const filtered = pair ? rows.filter((row) => row.pair === pair) : rows;
        return res.status(200).json({
            mode: 'forex',
            count: Math.min(limit, filtered.length),
            journal: filtered.slice(0, limit),
        });
    } catch (err: any) {
        console.error('Error in /api/forex/dashboard/journal:', err);
        return res.status(500).json({ error: err?.message || String(err) });
    }
}
