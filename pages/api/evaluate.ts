import type { NextApiRequest, NextApiResponse } from 'next';

import { loadDecisionHistory } from '../../lib/history';
import { callAI } from '../../lib/ai';
import { AI_MODEL } from '../../lib/constants';

function actionStats(items: any[]) {
    return items.reduce(
        (acc, item) => {
            const action = (item.aiDecision?.action || 'UNKNOWN').toUpperCase();
            acc[action] = (acc[action] || 0) + 1;
            return acc;
        },
        {} as Record<string, number>,
    );
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'POST') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST' });
    }

    const symbol = String(req.body?.symbol || '').toUpperCase();
    const limit = Math.min(30, Math.max(5, Number(req.body?.limit ?? 50)));

    if (!symbol) {
        return res.status(400).json({ error: 'symbol_required' });
    }

    const history = await loadDecisionHistory(symbol, limit);
    if (!history.length) {
        return res.status(404).json({ error: 'no_history', symbol });
    }

    const condensed = history.map((item) => ({
        timestamp: item.timestamp,
        timeFrame: item.timeFrame,
        prompt: item.prompt,
        action: item.aiDecision?.action,
        signal_strength: item.aiDecision?.signal_strength,
        bias: item.aiDecision?.bias,
        summary: item.aiDecision?.summary,
        reason: item.aiDecision?.reason,
        dryRun: item.dryRun,
        snapshot: item.snapshot,
        execResult: item.execResult,
        AI_MODEL: AI_MODEL,
    }));

    const stats = {
        totalSamples: condensed.length,
        actionDistribution: actionStats(history),
    };

    const system = `You are an expert trading performance evaluator. Review historical AI trading decisions and provide constructive feedback. Respond in JSON only with the shape {"performance_rating:"1_to_10_performance_rating"overview":"string","what_went_well":["..."],"issues":["..."],"improvements":["..."],"confidence":"LOW|MEDIUM|HIGH"}. Focus on how well the prompt/market snapshot aligns with the action taken and note concrete improvements. Be concise and specific.`;
    const user = `Symbol: ${symbol}. Recent decisions (most recent first): ${JSON.stringify(condensed)}. Stats: ${JSON.stringify(stats)}.`;

    const evaluation = await callAI(system, user);

    return res.status(200).json({ symbol, stats, samples: condensed, evaluation });
}
