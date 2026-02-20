export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { runForexRegimeCycle } from '../../../../lib/forex/engine';

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

    try {
        const snapshot = await runForexRegimeCycle();
        return res.status(200).json({ ok: true, snapshot });
    } catch (err: any) {
        console.error('Error in /api/forex/cron/regime:', err);
        return res.status(500).json({ error: err?.message || String(err) });
    }
}
