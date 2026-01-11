import type { NextApiRequest, NextApiResponse } from 'next';

import { loadDecisionHistory } from '../../lib/history';
import { callAI } from '../../lib/ai';
import { AI_MODEL } from '../../lib/constants';
import { setEvaluation } from '../../lib/utils';

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
    if (req.method !== 'POST' && req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST or GET' });
    }

    const body = req.method === 'GET' ? req.query : req.body ?? {};
    const symbol = String(body?.symbol || '').toUpperCase();
    const limit = Math.min(30, Math.max(5, Number(body?.limit ?? 50)));

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

        const system = `You are an expert trading performance evaluator AND prompt auditor. Review historical AI trading decisions and the prompts/system instructions used to produce them. Find anomalies, contradictions, confusing naming, missing guards, and cost/logic mismatches. Respond in strict JSON parseable by JSON.parse with no trailing commas:
{
    "aspects": {
        "data_quality": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["missing/NaN metrics", "stale candles vs analytics", "timeframe mismatches", "S/R state coherence", "gates reflect metrics"]},
        "data_quantity": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["sample size", "candle depth", "enough bars to compute indicators", "decision count sufficiency"]},
        "ai_performance": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["actions align with biases/signals", "edge vs costs", "macro/context use"]},
        "strategy_performance": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["entries/exits vs stated rules", "churn vs costs", "HTF alignment"]},
        "signal_strength_clarity": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["MEDIUM/HIGH vs aligned_driver_count/flow", "directional counts used", "threshold clarity"]},
        "risk_management": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["stop/TP vs ATR", "base gates vs exits", "PnL handling", "cost-aware churn"]},
        "consistency": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["system text vs actions", "bias definitions vs implementation", "metric/rule coherence"]},
        "explainability": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["reason matches action", "biases and drivers cited", "missing rationale fields"]},
        "responsiveness": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["timeliness vs timeframe", "time_stop vs call cadence", "reaction to regime shifts"]},
        "prompt_engineering": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["verbosity", "ambiguity", "JSON strictness", "naming clarity"]},
        "prompt_consistency": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["list contradictions, naming confusion, missing gates/guards, cost mismatches"], "checks": ["JSON strictness, naming and guardrails coherence"]},
        "action_logic": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["gaps in entry/exit rules, base gates on open positions, reversal discipline, PnL sign conventions"], "checks": ["entry/exit edge vs costs, base gates, reversal handling"]},
        "ai_freedom": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["reasoning latitude vs hard filters", "over-constrained if/else patterns", "room for nuanced exits/entries without breaking guardrails"]},
        "guardrail_coverage": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["coverage of base gates and costs in prompts", "reversal and exit guardrails present", "risk-off bias when uncertain", "hedging/ambiguity avoidance"]}
    },
    "overall_rating": 0-10,
    "overview": "string",
    "what_went_well": ["..."],
    "issues": ["..."],
    "improvements": ["..."],
    "confidence": "LOW|MEDIUM|HIGH"
}
Rate each aspect 0-10 (0=poor, 10=excellent) with a short comment. Provide per-aspect improvements only when you have concrete, actionable suggestions (keep each entry brief) and only list them when they exist. Findings are optional—include them only when something is truly critical and only list them when they exist. In prompt_consistency/action_logic, explicitly call out: macro gating vs HIGH signals, price_vs_breakeven sign for shorts, cost mismatches (fees vs taker_fee_rate vs heuristic bps), base gates blocking exits, macro neutral handling, S/R dual at_level, JSON strictness. For ai_freedom, flag over-constrained logic that kills nuanced entries/exits; for guardrail_coverage, flag missing base gate/cost coverage, weak reversal/exit rails, lack of risk-off bias, or hedging ambiguity. Be concise.`;
    const user = `Symbol: ${symbol}. Recent decisions (most recent first): ${JSON.stringify(condensed)}. Stats: ${JSON.stringify(stats)}. Include prompt/system text when highlighting prompt issues: ${JSON.stringify(condensed.map((c) => c.prompt))}.`;

    const evaluation = await callAI(system, user);

    // Persist the latest evaluation for this symbol (single entry per symbol).
    try {
        await setEvaluation(symbol, evaluation);
    } catch (err) {
        console.error('Failed to persist evaluation:', err);
    }

    return res.status(200).json({ symbol, stats, samples: condensed, evaluation });
}
