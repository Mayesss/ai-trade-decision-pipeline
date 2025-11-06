// api/lastDecision.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { loadLastDecision } from '../../lib/kvstore';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    try {
        if (req.method !== 'GET') {
            return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
        }

        const symbol = (req.query.symbol as string) || 'ETHUSDT';
        const decision = await loadLastDecision({}, symbol);

        if (!decision) {
            return res.status(404).json({ message: `No decisions found for ${symbol}` });
        }

        return res.status(200).json(decision);
    } catch (err: any) {
        console.error('Error in /lastDecision:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
