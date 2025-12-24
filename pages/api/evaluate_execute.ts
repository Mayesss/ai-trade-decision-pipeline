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
    entries_disabled?: boolean;
    entry_blockers?: string[];
    market?: { last?: number; spreadBps?: number; depthUSD?: number } | null;
    indicators?: Record<string, any> | null;
    levels?: Record<string, any> | null;
    entry_eval?: Record<string, any> | null;
    invalidation_eval?: Record<string, any> | null;
    order_details?: Record<string, any> | null;
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

function safeTsMs(ts: unknown): number | null {
    if (typeof ts === 'number' && Number.isFinite(ts)) return ts;
    if (typeof ts === 'string' && ts) {
        const ms = Date.parse(ts);
        return Number.isFinite(ms) ? ms : null;
    }
    return null;
}

function decisionSide(decision: string): 'long' | 'short' | null {
    const d = String(decision || '').toUpperCase();
    if (d.includes('ENTER_LONG')) return 'long';
    if (d.includes('ENTER_SHORT')) return 'short';
    return null;
}

function truthyBoolean(x: any): boolean | null {
    if (x === true) return true;
    if (x === false) return false;
    return null;
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
    const logs = await loadExecutionLogs(symbol, 1440); // last 24 hours
    const runs: ExecRun[] = logs
        .map((l) => (l?.payload && typeof l.payload === 'object' ? l.payload : l))
        .filter(Boolean) as ExecRun[];

    if (!runs.length) {
        return res.status(404).json({ error: 'no_execution_history', symbol });
    }

    const system = `You are an execution quality evaluator for a deterministic trade executor (NOT a chatty AI trader).
Your goal is to produce an evaluation that is measurable from the execution logs' structured fields:
- Answer "why didn't it trade?" using entries_disabled, entry_blockers, gatesNow, reason, and key filters.
- Answer "which confirmations predict profit?" using entry_eval flags and realized PnL windows (if present).

Return strict JSON parseable by JSON.parse (no markdown, no trailing commas):
{
  "aspects": {
    "why_no_trade": {
      "rating": 0-10,
      "comment": "How well the logs/logic explain WAIT / no-entry outcomes",
      "checks": ["uses entries_disabled + entry_blockers", "reasons are specific and consistent", "gate failures are visible"],
      "findings": ["critical only"],
      "improvements": ["optional"]
    },
    "blockers_and_filters": {
      "rating": 0-10,
      "comment": "Correctness/impact of gates, cooldowns, extension and level-distance filters",
      "checks": ["gates_fail frequency vs conditions", "cooldown behavior", "extension_block rate", "too_close_support/resistance behavior"],
      "findings": ["critical only"],
      "improvements": ["optional"]
    },
    "confirmation_signals": {
      "rating": 0-10,
      "comment": "Quality of confirmation logic + whether confirmations are meaningfully selective",
      "checks": ["rsiOk/emaOk/obImbOk hit-rates", "confirmationCount distribution", "pullback vs breakout mix"],
      "findings": ["critical only"],
      "improvements": ["optional"]
    },
    "predictive_power": {
      "rating": 0-10,
      "comment": "Do logged confirmations correlate with better realized PnL?",
      "checks": ["feature→outcome stats (if any matches)", "separate by side (long/short) if available"],
      "findings": ["critical only"],
      "improvements": ["optional"]
    },
    "exit_and_invalidation": {
      "rating": 0-10,
      "comment": "Responsiveness/correctness of trims and forced closes (direction mismatch, invalidations)",
      "checks": ["invalidation_eval.triggered vs action", "close_if_invalidation respected", "forced close is full size"],
      "findings": ["critical only"],
      "improvements": ["optional"]
    },
    "logging_quality": {
      "rating": 0-10,
      "comment": "Are the logged fields sufficient for analysis without being huge?",
      "checks": ["market/indicators/levels present", "entry_eval present", "entry_blockers present", "order_details compact on trades only"],
      "findings": ["critical only"],
      "improvements": ["optional"]
    }
  },
  "overall_rating": 0-10,
  "overview": "string",
  "diagnostics": {
    "top_no_trade_reasons": ["..."],
    "top_entry_blockers": ["..."],
    "confirmation_insights": ["..."],
    "profitability_summary": "string"
  },
  "issues": ["..."],
  "improvements": ["..."],
  "confidence": "LOW|MEDIUM|HIGH"
}

Keep the overview concise (<= 3 sentences). Use the provided Run aggregates when possible.`;

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

    // ------------------------------
    // Lightweight analytics from run logs
    // ------------------------------
    const decisions = runs.map((r) => String(r?.decision || 'UNKNOWN').toUpperCase());
    const decisionDistribution = distribution(decisions);
    const reasons = runs.map((r) => String(r?.reason || 'UNKNOWN'));
    const reasonDistribution = distribution(reasons);
    const entryBlockers = runs.flatMap((r) => (Array.isArray(r?.entry_blockers) ? r.entry_blockers : []));
    const entryBlockerDistribution = distribution(entryBlockers.map((b) => String(b || 'UNKNOWN')));
    const entriesDisabledCount = runs.filter((r) => r?.entries_disabled === true).length;

    const confirmationKeys = ['rsiOk', 'emaOk', 'obImbOk', 'inPullbackZone', 'breakout2x5m'] as const;
    const confirmationHitCounts: Record<(typeof confirmationKeys)[number], { true: number; false: number; null: number }> =
        Object.fromEntries(confirmationKeys.map((k) => [k, { true: 0, false: 0, null: 0 }])) as any;
    for (const r of runs) {
        const ev = (r as any)?.entry_eval;
        for (const k of confirmationKeys) {
            const v = truthyBoolean(ev?.[k]);
            if (v === true) confirmationHitCounts[k].true += 1;
            else if (v === false) confirmationHitCounts[k].false += 1;
            else confirmationHitCounts[k].null += 1;
        }
    }

    // Attempt to align entry runs to realized position windows for basic feature→outcome signal checks.
    const entryRuns = runs
        .map((r) => ({ r, tsMs: safeTsMs(r?.ts), side: decisionSide(String(r?.decision || '')) }))
        .filter((x) => x.tsMs && x.side) as { r: ExecRun; tsMs: number; side: 'long' | 'short' }[];

    const windows = closed
        .map((w: any) => ({
            ...w,
            entryTimestamp: typeof w?.entryTimestamp === 'number' ? w.entryTimestamp : null,
            pnlPct: typeof w?.pnlPct === 'number' ? w.pnlPct : null,
            side: w?.side === 'long' || w?.side === 'short' ? (w.side as 'long' | 'short') : null,
        }))
        .filter((w: any) => w.entryTimestamp && w.pnlPct !== null) as any[];

    const usedWindowIds = new Set<string>();
    const matchedOutcomes: Array<{
        entry_ts: number;
        side: 'long' | 'short';
        pnlPct: number;
        features: Record<string, boolean | null>;
    }> = [];

    const maxMatchMs = 20 * 60 * 1000;
    for (const e of entryRuns) {
        let best: any = null;
        let bestDt = Infinity;
        for (const w of windows) {
            if (usedWindowIds.has(String(w.id))) continue;
            if (w.side && w.side !== e.side) continue;
            const dt = Math.abs(Number(w.entryTimestamp) - e.tsMs);
            if (dt < bestDt && dt <= maxMatchMs) {
                best = w;
                bestDt = dt;
            }
        }
        if (!best) continue;
        usedWindowIds.add(String(best.id));
        const ev = (e.r as any)?.entry_eval || {};
        const features: Record<string, boolean | null> = {};
        for (const k of confirmationKeys) features[k] = truthyBoolean(ev?.[k]);
        matchedOutcomes.push({
            entry_ts: e.tsMs,
            side: e.side,
            pnlPct: Number(best.pnlPct),
            features,
        });
    }

    const confirmationOutcomeStats: Record<
        string,
        { true_count: number; false_count: number; true_avg_pnl_pct: number | null; false_avg_pnl_pct: number | null }
    > = {};
    for (const k of confirmationKeys) {
        const trues = matchedOutcomes.filter((m) => m.features[k] === true).map((m) => m.pnlPct);
        const falses = matchedOutcomes.filter((m) => m.features[k] === false).map((m) => m.pnlPct);
        const avg = (xs: number[]) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
        confirmationOutcomeStats[k] = {
            true_count: trues.length,
            false_count: falses.length,
            true_avg_pnl_pct: avg(trues),
            false_avg_pnl_pct: avg(falses),
        };
    }

    const user = `Symbol: ${symbol}.
This executor is deterministic logic (not an AI agent). Evaluate the EXECUTOR based on logs + objective performance stats.
Latest plan (may be null): ${JSON.stringify(plan)}.
Exec state: ${JSON.stringify(execState)}.
Recent execution runs (most recent first): ${JSON.stringify(runs)}.
Run aggregates: ${JSON.stringify({
        sample_size: runs.length,
        decisions: decisionDistribution,
        reasons: reasonDistribution,
        entries_disabled_count: entriesDisabledCount,
        entry_blockers: entryBlockerDistribution,
        confirmation_hit_counts: confirmationHitCounts,
        matched_entry_outcomes_count: matchedOutcomes.length,
        confirmation_outcome_stats: confirmationOutcomeStats,
    })}.
Performance stats (24h): ${JSON.stringify(tradeStats)}.`;

    const evaluation = await callAI(system, user);
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
