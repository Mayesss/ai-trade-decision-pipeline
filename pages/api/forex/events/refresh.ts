export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { refreshForexEvents } from '../../../../lib/forex/events/fmp';

function parseBoolParam(value: string | string[] | undefined, fallback: boolean) {
    if (value === undefined) return fallback;
    const v = Array.isArray(value) ? value[0] : value;
    if (v === undefined) return fallback;
    const normalized = String(v).toLowerCase();
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    return fallback;
}

function setNoStoreHeaders(res: NextApiResponse) {
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

    const force = parseBoolParam(req.query.force as string | string[] | undefined, false);

    try {
        const result = await refreshForexEvents({ force });
        if (!result.ok) {
            return res.status(502).json(result);
        }
        return res.status(200).json(result);
    } catch (err: any) {
        console.error('Error in /api/forex/events/refresh:', err);
        return res.status(500).json({
            error: err?.message || String(err),
        });
    }
}
