import type { NextApiRequest, NextApiResponse } from 'next';

import { clearPlanLogs, loadPlanLogs } from '../../lib/planLog';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method === 'GET') {
        const symbolRaw = req.query.symbol;
        const symbol = String(Array.isArray(symbolRaw) ? symbolRaw[0] : symbolRaw || '').toUpperCase();
        const limit = Math.min(60, Math.max(1, Number(req.query.limit ?? 20)));
        if (!symbol) return res.status(400).json({ error: 'symbol_required' });
        const history = await loadPlanLogs(symbol, limit);
        return res.status(200).json({ history });
    }

    if (req.method === 'DELETE') {
        const symbolRaw = req.query.symbol;
        const symbol = symbolRaw ? String(Array.isArray(symbolRaw) ? symbolRaw[0] : symbolRaw || '').toUpperCase() : '';
        const result = await clearPlanLogs(symbol || undefined);
        if (!result.cleared) return res.status(400).json({ error: result.error });
        return res.status(200).json({ cleared: true, symbol: symbol || null });
    }

    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET or DELETE' });
}
