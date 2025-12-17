import type { NextApiRequest, NextApiResponse } from 'next';

import { loadExecutionLogs } from '../../lib/execLog';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });

    const symbolRaw = req.query.symbol;
    const symbol = String(Array.isArray(symbolRaw) ? symbolRaw[0] : symbolRaw || '').toUpperCase();
    const limit = Math.min(60 * 24, Math.max(1, Number(req.query.limit ?? 10)));
    if (!symbol) return res.status(400).json({ error: 'symbol_required' });

    const logs = await loadExecutionLogs(symbol, limit);
    return res.status(200).json({ symbol, logs });
}
