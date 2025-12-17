import type { NextApiRequest, NextApiResponse } from 'next';

import { readPlan } from '../../lib/planStore';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });

    const symbolRaw = req.query.symbol;
    const symbol = String(Array.isArray(symbolRaw) ? symbolRaw[0] : symbolRaw || '').toUpperCase();
    if (!symbol) return res.status(400).json({ error: 'symbol_required' });

    const planRecord = await readPlan(symbol);
    if (!planRecord?.plan) return res.status(404).json({ error: 'plan_not_found', symbol });

    return res.status(200).json({ symbol, plan: planRecord.plan, savedAt: planRecord.savedAt ?? null, prompt: planRecord.prompt ?? null });
}
