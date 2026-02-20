// api/bitget-ping.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { requireAdminAccess } from '../../lib/admin';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    try {
        if (req.method !== 'GET') {
            return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
        }
        if (!requireAdminAccess(req, res)) return;

        const ping = await fetch('https://api.bitget.com/api/v2/public/time');
        const data = await ping.json().catch(() => ({}));

        return res.status(200).json({
            status: ping.status,
            ok: ping.ok,
            serverTime: data?.data?.serverTime || null,
        });
    } catch (err: any) {
        console.error('Error in /bitget-ping:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
