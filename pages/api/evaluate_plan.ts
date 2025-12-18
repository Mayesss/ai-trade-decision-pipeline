import type { NextApiRequest, NextApiResponse } from 'next';

import { callAI } from '../../lib/ai';
import { fetchPositionInfo, fetchRealizedRoi, fetchRecentPositionWindows } from '../../lib/analytics';
import { loadExecutionLogs } from '../../lib/execLog';
import { readPlan } from '../../lib/planStore';
import { setPlanEvaluation } from '../../lib/utils';
import { loadPlanLogs } from '../../lib/planLog';

function distribution(items: string[]) {
    return items.reduce(
        (acc, item) => {
            const key = String(item || 'UNKNOWN').toUpperCase();
            acc[key] = (acc[key] || 0) + 1;
            return acc;
        },
        {} as Record<string, number>,
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

function countFlips<T>(items: T[], keyFn: (item: T) => string) {
    let flips = 0;
    let prev: string | null = null;
    for (const item of items) {
        const cur = keyFn(item);
        if (prev !== null && cur !== prev) flips += 1;
        prev = cur;
    }
    return flips;
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
    if (!planRecord?.plan) {
        return res.status(404).json({ error: 'plan_not_found', symbol });
    }

    const plan = planRecord.plan;
    const prompt = planRecord.prompt ?? null;
    const limit = Math.min(60, Math.max(1, Number(req.method === 'GET' ? req.query.limit ?? 24 : req.body?.limit ?? 24)));
    const planHistory = await loadPlanLogs(symbol, limit);
    const planSamples = planHistory.length ? planHistory : [{ symbol, timestamp: planRecord.savedAt ?? Date.now(), plan, prompt }];

    const allowedDirectionsDist = distribution(planSamples.map((p: any) => String(p?.plan?.allowed_directions || 'UNKNOWN')));
    const riskModeDist = distribution(planSamples.map((p: any) => String(p?.plan?.risk_mode || 'UNKNOWN')));
    const entryModeDist = distribution(planSamples.map((p: any) => String(p?.plan?.entry_mode || 'UNKNOWN')));
    const sampleSize = planSamples.length;
    const stability = {
        allowed_directions_flips: countFlips(planSamples, (p: any) => String(p?.plan?.allowed_directions || 'UNKNOWN')),
        risk_mode_flips: countFlips(planSamples, (p: any) => String(p?.plan?.risk_mode || 'UNKNOWN')),
        entry_mode_flips: countFlips(planSamples, (p: any) => String(p?.plan?.entry_mode || 'UNKNOWN')),
        max_leverage_flips: countFlips(planSamples, (p: any) => String(p?.plan?.max_leverage ?? 'UNKNOWN')),
    };
    const stabilityRate = sampleSize > 1 ? stability.allowed_directions_flips / (sampleSize - 1) : 0;
    const [roi24h, recentWindows, posInfo, execLogs] = await Promise.all([
        fetchRealizedRoi(symbol, 24).catch(() => null),
        fetchRecentPositionWindows(symbol, 24).catch(() => [] as any[]),
        fetchPositionInfo(symbol).catch(() => ({ status: 'none' as const })),
        loadExecutionLogs(symbol, 60).catch(() => [] as any[]),
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
    const execDecisionDist = distribution(
        (Array.isArray(execLogs) ? execLogs : [])
            .map((l: any) => (l?.payload && typeof l.payload === 'object' ? l.payload : l))
            .map((p: any) => String(p?.decision || 'UNKNOWN')),
    );
    const tradeStats = {
        realized_count_24h: realizedCount,
        win_rate_24h: winRate,
        avg_win_pct_24h: avgWinPct,
        avg_loss_pct_24h: avgLossPct,
        pnl_sum_pct_24h: roiSumPct,
        pnl_net_pct_24h: roiNetPct,
        last_closed_pnl_pct: lastNetPct,
        open_position: posInfo,
        exec_decision_distribution_last60: execDecisionDist,
    };

    const system = `You are an expert trading plan evaluator AND prompt auditor. The plan is generated by an AI planner, but the EXECUTION is deterministic algorithmic logic that follows this plan for ~1 hour. Evaluate plan quality for a deterministic executor:
- Schema fidelity: exact keys/types, no extras.
- Executor compatibility: fields must be practical + unambiguous (allowed_directions, risk_mode, max_leverage, entry_mode, key_levels, no_trade_rules, exit_urgency).
- Stability: avoid frequent flips in allowed_directions/risk_mode/entry_mode unless regime truly changes.
- Direction/risk consistency: allowed_directions vs biases; risk_mode/max_leverage must match confidence and gates.
- Key levels: reasonable prices/strengths/states; opposing levels not both "at_level".
- No-trade rules: buffers sensible vs horizon; extension rule present.
- Exit urgency: invalidation_notes grammar correctness, LVL relevance (1H support for longs, 1H resistance for shorts, fallback 4H), FAST tighter in CONSERVATIVE, present even in AGGRESSIVE, ACTION valid.
- Cooldown: sensible if losses/chop; OFF vs NONE alignment.
- Safety: risk_mode/allowed_directions/entry_mode when base gates fail.
- Chop handling: if location_confluence_score high or both 1H levels close, prefer NONE/CONSERVATIVE.
- Leverage caps: consistent with risk_mode.
- Prompt quality: clarity, JSON strictness, alignment with expected fields/constraints, no contradictions, includes invalidation grammar and LVL selection rules.

Respond in strict JSON parseable by JSON.parse (no markdown, no trailing commas):
{
  "aspects": {
    "data_quality": {"rating": 0-10, "comment": "inputs/levels appear coherent and realistic", "improvements": ["optional"], "findings": ["critical only"], "checks": ["key_levels sanity", "strength/state coherence"]},
    "data_quantity": {"rating": 0-10, "comment": "sufficient context captured in plan", "improvements": ["optional"], "findings": ["critical only"], "checks": ["coverage of required fields"]},
    "ai_performance": {"rating": 0-10, "comment": "planner quality (structure, stability, adherence to schema)", "improvements": ["optional"], "findings": ["critical only"], "checks": ["schema fidelity", "no hallucinated keys", "stable outputs"]},
    "strategy_performance": {"rating": 0-10, "comment": "plan quality as a trading blueprint and observed outcomes", "improvements": ["optional"], "findings": ["critical only"], "checks": ["profitability stats context", "executor decision distribution sanity"]},
    "signal_strength_clarity": {"rating": 0-10, "comment": "risk_mode/max_leverage/allowed_directions are clearly justified", "improvements": ["optional"], "findings": ["critical only"], "checks": ["summary/reason match plan fields"]},
    "risk_management": {"rating": 0-10, "comment": "safety rails and risk throttling in plan", "improvements": ["optional"], "findings": ["critical only"], "checks": ["risk_mode vs allowed_directions", "cooldown usage", "max_leverage sanity"]},
    "consistency": {"rating": 0-10, "comment": "stability across consecutive plans", "improvements": ["optional"], "findings": ["critical only"], "checks": ["flip rate", "no unnecessary direction changes"]},
    "explainability": {"rating": 0-10, "comment": "plan is easy for humans + executor to follow", "improvements": ["optional"], "findings": ["critical only"], "checks": ["summary <=2 lines", "reason actionable"]},
    "responsiveness": {"rating": 0-10, "comment": "updates when regime changes, otherwise stays stable", "improvements": ["optional"], "findings": ["critical only"], "checks": ["changes align with sample stats"]},
    "prompt_engineering": {"rating": 0-10, "comment": "prompt quality for strict JSON and correct guardrails", "improvements": ["optional"], "findings": ["critical only"], "checks": ["JSON strictness", "invalidation grammar included"]},
    "prompt_consistency": {"rating": 0-10, "comment": "prompt and output are not contradictory", "improvements": ["optional"], "findings": ["critical only"], "checks": ["constraints match schema"]},
    "action_logic": {"rating": 0-10, "comment": "plan is executable and unambiguous for deterministic executor", "improvements": ["optional"], "findings": ["critical only"], "checks": ["entry_mode not NONE when allowed", "no_trade_rules usable"]},
    "ai_freedom": {"rating": 0-10, "comment": "planner has enough flexibility without breaking rules", "improvements": ["optional"], "findings": ["critical only"], "checks": ["avoids overfitting micro noise"]},
    "guardrail_coverage": {"rating": 0-10, "comment": "guardrails are present and coherent", "improvements": ["optional"], "findings": ["critical only"], "checks": ["cooldown", "no-trade buffers", "exit invalidation"]} 
  },
  "overall_rating": 0-10,
  "overview": "string",
  "what_went_well": ["..."],
  "issues": ["..."],
  "improvements": ["..."],
  "confidence": "LOW|MEDIUM|HIGH"
}

Rate each aspect 0-10 (0=poor, 10=excellent) with a short comment. Provide per-aspect improvements only when concrete. Findings only when truly critical. Keep the overview concise (<= 3 sentences).`;

    const user = `Symbol: ${symbol}.
Latest plan: ${JSON.stringify(plan)}.
Latest prompt (system/user): ${JSON.stringify(prompt)}.
Recent plan samples (newest first, condensed): ${JSON.stringify(
        planSamples.map((p: any) => ({
            timestamp: p.timestamp,
            plan_ts: p?.plan?.plan_ts,
            allowed_directions: p?.plan?.allowed_directions,
            risk_mode: p?.plan?.risk_mode,
            max_leverage: p?.plan?.max_leverage,
            entry_mode: p?.plan?.entry_mode,
            summary: p?.plan?.summary,
        })),
    )}.
Stats: ${JSON.stringify({
        sample_size: sampleSize,
        allowed_directions: allowedDirectionsDist,
        risk_mode: riskModeDist,
        entry_mode: entryModeDist,
        stability,
        stability_rate_allowed_directions: stabilityRate,
        performance_24h: tradeStats,
    })}.`;

    const evaluation = await callAI(system, user);
    const evaluationWithMeta = {
        ...(evaluation && typeof evaluation === 'object' ? evaluation : {}),
        sample_size: sampleSize,
        allowed_directions_distribution: allowedDirectionsDist,
        risk_mode_distribution: riskModeDist,
        entry_mode_distribution: entryModeDist,
        stability,
        stability_rate_allowed_directions: stabilityRate,
        redundance_summary: [
            `sample_size=${sampleSize}`,
            `dir_flips=${stability.allowed_directions_flips}`,
            ...Object.entries(allowedDirectionsDist)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 4)
                .map(([k, v]) => `allowed_directions:${k}:${v}`),
            ...Object.entries(riskModeDist)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 3)
                .map(([k, v]) => `risk_mode:${k}:${v}`),
        ].join(', '),
    };

    try {
        await setPlanEvaluation(symbol, evaluationWithMeta);
    } catch (err) {
        console.warn('Failed to persist plan evaluation:', err);
    }

    return res.status(200).json({ symbol, plan, evaluation: evaluationWithMeta });
}
