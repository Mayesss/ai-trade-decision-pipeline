export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { loadForexPacketSnapshot } from '../../../../lib/forex/store';

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
        const snapshot = await loadForexPacketSnapshot();
        return res.status(200).json({
            mode: 'forex',
            generatedAtMs: snapshot?.generatedAtMs ?? null,
            packets: snapshot?.packets ?? [],
        });
    } catch (err: any) {
        console.error('Error in /api/forex/dashboard/packets:', err);
        return res.status(500).json({ error: err?.message || String(err) });
    }
}
