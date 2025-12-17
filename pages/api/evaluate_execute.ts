import type { NextApiRequest, NextApiResponse } from 'next';

import { callAI } from '../../lib/ai';
import { readPlan } from '../../lib/planStore';
import { readExecState } from '../../lib/execState';

type ExecRun = {
    ts?: string | number;
    decision?: string;
    reason?: string;
    position_state?: string;
    plan_ts?: string;
    plan_allowed_directions?: string;
    plan_risk_mode?: string;
    plan_entry_mode?: string;
    gatesNow?: Record<string, any>;
};

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'POST' && req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST or GET' });
    }

    const symbolRaw = req.method === 'GET' ? req.query.symbol : req.body?.symbol;
    const symbol = String(Array.isArray(symbolRaw) ? symbolRaw[0] : symbolRaw || '').toUpperCase();
    if (!symbol) {
        return res.status(400).json({ error: 'symbol_required' });
    }

    const runsRaw = req.method === 'GET' ? req.query.runs : req.body?.runs;
    let runs: ExecRun[] = [];
    if (runsRaw) {
        try {
            const parsed = typeof runsRaw === 'string' ? JSON.parse(runsRaw) : runsRaw;
            if (Array.isArray(parsed)) runs = parsed as ExecRun[];
        } catch {
            return res.status(400).json({ error: 'invalid_runs_json' });
        }
    }
    if (!runs.length) {
        return res.status(400).json({ error: 'runs_required', message: 'Provide recent executor runs array' });
    }

    const planRecord = await readPlan(symbol);
    const plan = planRecord?.plan ?? null;
    const execState = await readExecState(symbol);

    const system = `You are an execution auditor. Evaluate whether executor decisions respect the latest plan and risk constraints. Check:
- Plan freshness: plan_ts vs run ts.
- Direction alignment: never enter against allowed_directions; BOTH should bias to macro/primary.
- Risk mode: leverage/size hints vs risk_mode and max_leverage.
- Entry/exit discipline: honoring entry_mode, no-trade buffers, extension filters.
- Invalidation handling: TRIM/CLOSE when invalidation triggers; exits allowed despite gates.
- Cooldown: no entries during cooldown; anti-churn respected.
- Gate enforcement: spread/liquidity/slippage gating for entries; exits always allowed.
- Trim near levels: trims happen only when in profit near opposite level.
Return strict JSON parseable by JSON.parse:
{
  "overall_rating": 0-10,
  "issues": ["..."],
  "improvements": ["..."],
  "findings": ["..."], // critical only
  "confidence": "LOW|MEDIUM|HIGH"
}`;

    const user = `Symbol: ${symbol}. Plan: ${JSON.stringify(plan)}. Exec state: ${JSON.stringify(execState)}. Recent runs (most recent first preferred): ${JSON.stringify(runs)}.`;

    const evaluation = await callAI(system, user);

    return res.status(200).json({ symbol, plan, execState, runs, evaluation });
}
