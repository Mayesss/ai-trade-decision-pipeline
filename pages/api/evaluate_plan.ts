import type { NextApiRequest, NextApiResponse } from 'next';

import { callAI } from '../../lib/ai';
import { readPlan } from '../../lib/planStore';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'POST' && req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST or GET' });
    }

    const symbolRaw = req.method === 'GET' ? req.query.symbol : req.body?.symbol;
    const symbol = String(Array.isArray(symbolRaw) ? symbolRaw[0] : symbolRaw || '').toUpperCase();
    if (!symbol) {
        return res.status(400).json({ error: 'symbol_required' });
    }

    const planRecord = await readPlan(symbol);
    if (!planRecord?.plan) {
        return res.status(404).json({ error: 'plan_not_found', symbol });
    }

    const plan = planRecord.plan;

    const system = `You are a plan quality auditor. Review the provided trading plan_v1 JSON for correctness, completeness, stability, and guardrail adherence. Focus on:
- Schema fidelity: exact keys/types, no extras.
- Direction/risk consistency: allowed_directions vs biases; stability vs prior plan if provided.
- Key levels: reasonable prices/strengths/states; opposing levels not both "at_level".
- No-trade rules: buffers sensible vs horizon; extension rule present.
- Exit urgency: invalidation_notes grammar correctness, LVL relevance (1H support for longs, 1H resistance for shorts, fallback 4H), FAST tighter in CONSERVATIVE, present even in AGGRESSIVE, ACTION valid.
- Cooldown: sensible if losses/chop; OFF vs NONE alignment.
- Safety: risk_mode/allowed_directions/entry_mode when base gates fail.
- Chop handling: if location_confluence_score high or both 1H levels close, prefer NONE/CONSERVATIVE.
- Leverage caps: within 1-4, consistent with risk_mode.
Respond in strict JSON parseable by JSON.parse:
{
  "overall_rating": 0-10,
  "issues": ["..."],
  "improvements": ["..."],
  "findings": ["..."],  // critical only
  "confidence": "LOW|MEDIUM|HIGH"
}`;

    const prevRaw = req.method === 'GET' ? req.query.prev_plan : req.body?.prev_plan;
    const prevPlan = prevRaw ? JSON.parse(String(Array.isArray(prevRaw) ? prevRaw[0] : prevRaw)) : planRecord.plan?.prev_plan ?? null;

    const user = `Symbol: ${symbol}. Plan: ${JSON.stringify(plan)}. Prev plan (optional): ${JSON.stringify(prevPlan)}.`;

    const evaluation = await callAI(system, user);

    return res.status(200).json({ symbol, plan, evaluation });
}
