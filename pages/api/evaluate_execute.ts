import type { NextApiRequest, NextApiResponse } from 'next';

import { callAI } from '../../lib/ai';
import { fetchPositionInfo, fetchRealizedRoi, fetchRecentPositionWindows } from '../../lib/analytics';
import { readPlan } from '../../lib/planStore';
import { readExecState } from '../../lib/execState';
import { setExecEvaluation } from '../../lib/utils';
import { loadExecutionLogs } from '../../lib/execLog';

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

function distribution<T extends string>(items: T[]) {
    return items.reduce(
        (acc, item) => {
            acc[item] = (acc[item] || 0) + 1;
            return acc;
        },
        {} as Record<T, number>,
    );
}

function formatDistributionSummary(dist: Record<string, number>, sampleSize: number, maxItems = 6) {
    const parts = Object.entries(dist)
        .sort((a, b) => b[1] - a[1])
        .slice(0, maxItems)
        .map(([k, v]) => `${k}:${v}`);
    return `sample_size=${sampleSize}${parts.length ? `, ${parts.join(', ')}` : ''}`;
}

function safeNumber(n: any): number | null {
    const v = typeof n === 'string' ? Number(n.replace('%', '')) : Number(n);
    return Number.isFinite(v) ? v : null;
}

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
    const plan = planRecord?.plan ?? null;
    const execState = await readExecState(symbol);
    const logs = await loadExecutionLogs(symbol, 60);
    const runs: ExecRun[] = logs
        .map((l) => (l?.payload && typeof l.payload === 'object' ? l.payload : l))
        .filter(Boolean) as ExecRun[];

    if (!runs.length) {
        return res.status(404).json({ error: 'no_execution_history', symbol });
    }

    const system = `You are an execution quality evaluator for an algorithmic executor (NOT a chatty AI trader). The executor is deterministic logic based on an hourly AI plan. Rate execution quality using objective outcomes and rule adherence:
	- Trade quality: entries near intended levels/zones, exits/trims near opposite levels or invalidations, avoiding churn, not trading into costs.
	- Profitability: realized ROI, win-rate/avg win/loss, drawdowns (if available), and whether results are consistent with the plan's risk_mode.
	- Risk & discipline: never violate allowed_directions, respect cooldowns and base gates for entries, allow exits when needed.
	- Robustness: behaves sensibly in chop (WAIT more), avoids over-trading when signals are weak, uses trims prudently.
	- Data quality: if execution uses noisy/insufficient data, call it out.

	Return strict JSON parseable by JSON.parse (no markdown, no trailing commas):
	{
	  "aspects": {
	    "data_quality": {"rating": 0-10, "comment": "market data freshness/quality for execution", "improvements": ["optional"], "findings": ["critical only"], "checks": ["orderbook/ATR/RSI availability", "spread/depth sanity"]},
	    "data_quantity": {"rating": 0-10, "comment": "enough bars/history to justify signals", "improvements": ["optional"], "findings": ["critical only"], "checks": ["candle depth", "sample size sufficient"]},
	    "ai_performance": {"rating": 0-10, "comment": "plan adherence quality (not 'AI')", "improvements": ["optional"], "findings": ["critical only"], "checks": ["allowed_directions compliance", "risk_mode sizing/leverage", "cooldown respect"]},
	    "strategy_performance": {"rating": 0-10, "comment": "trade quality (entries/exits/trims) + profitability", "improvements": ["optional"], "findings": ["critical only"], "checks": ["entry timing vs levels", "exit discipline", "churn vs costs", "ROI/win-rate"]},
	    "signal_strength_clarity": {"rating": 0-10, "comment": "reasons/labels are meaningful for monitoring", "improvements": ["optional"], "findings": ["critical only"], "checks": ["reason codes", "decision labels"]},
	    "risk_management": {"rating": 0-10, "comment": "risk containment and loss control", "improvements": ["optional"], "findings": ["critical only"], "checks": ["loss containment", "no entries when gates fail", "invalidation action"]},
	    "consistency": {"rating": 0-10, "comment": "stable behavior across runs and regimes", "improvements": ["optional"], "findings": ["critical only"], "checks": ["no flip-flop", "repeatable entry rules"]},
	    "explainability": {"rating": 0-10, "comment": "is it easy to understand why trades happened", "improvements": ["optional"], "findings": ["critical only"], "checks": ["reason is specific", "plan fields referenced"]},
	    "responsiveness": {"rating": 0-10, "comment": "reacts quickly to invalidations/plan changes", "improvements": ["optional"], "findings": ["critical only"], "checks": ["stale plan handling", "invalidation speed"]},
	    "prompt_engineering": {"rating": 0-10, "comment": "rule engineering quality (deterministic logic)", "improvements": ["optional"], "findings": ["critical only"], "checks": ["logic simplicity", "edge vs cost awareness"]},
	    "prompt_consistency": {"rating": 0-10, "comment": "rule consistency vs plan intent", "improvements": ["optional"], "findings": ["critical only"], "checks": ["entry_mode alignment", "buffer alignment"]},
	    "action_logic": {"rating": 0-10, "comment": "decision logic correctness", "improvements": ["optional"], "findings": ["critical only"], "checks": ["TRIM only in profit", "gates block entries only", "no forbidden entries"]},
	    "ai_freedom": {"rating": 0-10, "comment": "not applicable; treat as 'flexibility' of executor", "improvements": ["optional"], "findings": ["critical only"], "checks": ["handles edge cases without breaking rules"]},
	    "guardrail_coverage": {"rating": 0-10, "comment": "coverage of critical guardrails in execution", "improvements": ["optional"], "findings": ["critical only"], "checks": ["stale plan", "cooldown", "gate checks", "direction mismatch exits"]}
	  },
	  "overall_rating": 0-10,
	  "overview": "string",
	  "what_went_well": ["..."],
	  "issues": ["..."],
	  "improvements": ["..."],
	  "confidence": "LOW|MEDIUM|HIGH"
	}

	Keep the overview concise (<= 3 sentences).`;

    const [roi24h, recentWindows, posInfo] = await Promise.all([
        fetchRealizedRoi(symbol, 24).catch(() => null),
        fetchRecentPositionWindows(symbol, 24).catch(() => [] as any[]),
        fetchPositionInfo(symbol).catch(() => ({ status: 'none' as const })),
    ]);
    const lastNetPct = roi24h ? safeNumber((roi24h as any).lastNetPct) : null;
    const roiSumPct = roi24h ? safeNumber((roi24h as any).sumPct) : null;
    const roiNetPct = roi24h ? safeNumber((roi24h as any).roi) : null;
    const closed = Array.isArray(recentWindows) ? recentWindows : [];
    const wins = closed.filter((w: any) => typeof w?.pnlPct === 'number' && w.pnlPct > 0);
    const losses = closed.filter((w: any) => typeof w?.pnlPct === 'number' && w.pnlPct < 0);
    const realizedCount = closed.filter((w: any) => typeof w?.pnlPct === 'number').length;
    const winRate = realizedCount ? (wins.length / realizedCount) * 100 : null;
    const avgWinPct = wins.length ? wins.reduce((acc: number, w: any) => acc + (w.pnlPct as number), 0) / wins.length : null;
    const avgLossPct = losses.length ? losses.reduce((acc: number, w: any) => acc + (w.pnlPct as number), 0) / losses.length : null;
    const tradeStats = {
        realized_count_24h: realizedCount,
        win_rate_24h: winRate,
        avg_win_pct_24h: avgWinPct,
        avg_loss_pct_24h: avgLossPct,
        pnl_sum_pct_24h: roiSumPct,
        pnl_net_pct_24h: roiNetPct,
        last_closed_pnl_pct: lastNetPct,
        open_position: posInfo,
    };

    const user = `Symbol: ${symbol}.
This executor is deterministic logic (not an AI agent). Evaluate the EXECUTOR based on logs + objective performance stats.
Latest plan (may be null): ${JSON.stringify(plan)}.
Exec state: ${JSON.stringify(execState)}.
Recent execution runs (most recent first): ${JSON.stringify(runs)}.
Performance stats (24h): ${JSON.stringify(tradeStats)}.`;

    const evaluation = await callAI(system, user);
    const decisions = runs.map((r) => String(r?.decision || 'UNKNOWN').toUpperCase());
    const decisionDistribution = distribution(decisions);
    const sampleSize = runs.length;
    const redundanceSummary = formatDistributionSummary(decisionDistribution, sampleSize);
    const evaluationWithMeta = {
        ...(evaluation && typeof evaluation === 'object' ? evaluation : {}),
        sample_size: sampleSize,
        decision_distribution: decisionDistribution,
        redundance_summary: redundanceSummary,
    };

    try {
        await setExecEvaluation(symbol, evaluationWithMeta);
    } catch (err) {
        console.warn('Failed to persist exec evaluation:', err);
    }

    return res.status(200).json({ symbol, plan, execState, runs, evaluation: evaluationWithMeta });
}
