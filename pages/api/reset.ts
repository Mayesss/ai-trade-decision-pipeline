// api/reset.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { clearAll } from '../../lib/kvstore';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    try {
        if (req.method !== 'POST') {
            return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST' });
        }

        await clearAll()
        return res.status(200).json({ deleted: 'all' });
    } catch (err: any) {
        console.error('Error in /reset:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
