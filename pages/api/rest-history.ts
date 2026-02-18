import type { NextApiRequest, NextApiResponse } from 'next';

import { clearDecisionHistory, loadDecisionHistory } from '../../lib/history';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method == 'POST') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use DELETE or GET' });
    }

    if (req.method === 'DELETE') {
    await clearDecisionHistory();
    return res.status(200).json({ cleared: true });
    }
    if (req.method === 'GET') {
        const symbol = req.query.symbol as string;
        const platform = req.query.platform as string | undefined;
        if (!symbol) {
            return res.status(400).json({ error: 'Bad Request', message: 'Missing symbol parameter' });
        }
        const history = await loadDecisionHistory(symbol, 20, platform);
        return res.status(200).json({ history });
    }

    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use DELETE to clear history and GET to read it' });
}
