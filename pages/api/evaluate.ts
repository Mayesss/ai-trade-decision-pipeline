import type { NextApiRequest, NextApiResponse } from 'next';
import crypto from 'crypto';

import { loadDecisionHistory } from '../../lib/history';
import { callAI } from '../../lib/ai';
import { AI_MODEL } from '../../lib/constants';
import { setEvaluation } from '../../lib/utils';
import { kvGetJson, kvSetJson } from '../../lib/kv';

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

function parseBoolParam(value: string | string[] | undefined, fallback: boolean) {
    if (value === undefined) return fallback;
    const v = Array.isArray(value) ? value[0] : value;
    if (v === undefined) return fallback;
    const normalized = String(v).toLowerCase();
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    return fallback;
}

function chunkArray<T>(arr: T[], size: number): T[][] {
    if (size <= 0) return [arr];
    const out: T[][] = [];
    for (let i = 0; i < arr.length; i += size) {
        out.push(arr.slice(i, i + size));
    }
    return out;
}

const EVALUATE_JOB_KEY_PREFIX = 'evaluate:job';
const EVALUATE_JOB_TTL_SECONDS = 24 * 60 * 60; // 24h

type EvaluateResult = {
    symbol: string;
    stats: {
        totalSamples: number;
        actionDistribution: Record<string, number>;
    };
    samples: any[];
    batch: {
        batchSize: number;
        batchCount: number;
        sampleCounts: number[];
    };
    batchEvaluations?: any[];
    evaluation: any;
};

type EvaluateJobRecord = {
    id: string;
    symbol: string;
    status: 'queued' | 'running' | 'succeeded' | 'failed';
    createdAt: number;
    updatedAt: number;
    params: {
        limit: number;
        batchSize: number;
        includeBatchEvaluations: boolean;
    };
    result?: EvaluateResult;
    error?: string;
};

function jobKey(jobId: string) {
    return `${EVALUATE_JOB_KEY_PREFIX}:${jobId}`;
}

function firstHeaderValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value;
    if (Array.isArray(value) && value.length) return value[0];
    return undefined;
}

function setNoStoreHeaders(res: NextApiResponse) {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

const EVALUATION_SYSTEM_PROMPT = `You are an expert trading performance evaluator AND prompt auditor. Review historical AI trading decisions and the prompts/system instructions used to produce them. Find anomalies, contradictions, confusing naming, missing guards, and cost/logic mismatches. Respond in strict JSON parseable by JSON.parse with no trailing commas:
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

const AGGREGATION_SYSTEM_PROMPT = `You are consolidating multiple partial trading evaluations into one final evaluation.
Preserve the exact output schema and JSON strictness below, and merge evidence across chunks.
Do not drop critical findings repeated across chunks; deduplicate and prioritize by severity and frequency.
If chunk scores differ, produce a balanced final score with clear rationale in overview/comment fields.
Return strict JSON parseable by JSON.parse, with no markdown and no extra keys.
{
    "aspects": {
        "data_quality": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "data_quantity": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "ai_performance": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "strategy_performance": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "signal_strength_clarity": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "risk_management": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "consistency": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "explainability": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "responsiveness": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "prompt_engineering": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "prompt_consistency": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["..."], "checks": ["..."]},
        "action_logic": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["..."], "checks": ["..."]},
        "ai_freedom": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]},
        "guardrail_coverage": {"rating": 0-10, "comment": "string", "improvements": ["optional improvements"], "findings": ["critical only"], "checks": ["..."]}
    },
    "overall_rating": 0-10,
    "overview": "string",
    "what_went_well": ["..."],
    "issues": ["..."],
    "improvements": ["..."],
    "confidence": "LOW|MEDIUM|HIGH"
}`;

async function runEvaluation(params: {
    symbol: string;
    limit: number;
    batchSize: number;
    includeBatchEvaluations: boolean;
}): Promise<EvaluateResult> {
    const { symbol, limit, batchSize, includeBatchEvaluations } = params;
    const history = await loadDecisionHistory(symbol, limit);
    if (!history.length) {
        throw new Error('no_history');
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

    const chunks = chunkArray(condensed, batchSize);
    const batchEvaluations: any[] = [];

    for (let i = 0; i < chunks.length; i += 1) {
        const chunk = chunks[i];
        const chunkStats = {
            chunkIndex: i + 1,
            chunkCount: chunks.length,
            chunkSize: chunk.length,
            totalSamples: condensed.length,
            actionDistribution: actionStats(chunk.map((item) => ({ aiDecision: { action: item.action } }))),
        };

        const chunkUser = `Symbol: ${symbol}. This is evaluation batch ${i + 1}/${chunks.length}. Decisions in this batch (most recent first): ${JSON.stringify(chunk)}. Global stats: ${JSON.stringify(
            stats,
        )}. Batch stats: ${JSON.stringify(chunkStats)}. Include prompt/system text when highlighting prompt issues: ${JSON.stringify(
            chunk.map((c) => c.prompt),
        )}.`;
        try {
            const chunkEvaluation = await callAI(EVALUATION_SYSTEM_PROMPT, chunkUser);
            batchEvaluations.push({
                batchIndex: i + 1,
                sampleCount: chunk.length,
                evaluation: chunkEvaluation,
            });
        } catch (err: any) {
            throw new Error(`Batch ${i + 1}/${chunks.length} failed: ${err?.message || String(err)}`);
        }
    }

    let evaluation: any;
    if (batchEvaluations.length === 1) {
        evaluation = batchEvaluations[0].evaluation;
    } else {
        const aggregateUser = `Symbol: ${symbol}. Merge these partial evaluations into one final output with the exact schema. Global stats: ${JSON.stringify(
            stats,
        )}. Partial evaluations: ${JSON.stringify(batchEvaluations)}.`;
        evaluation = await callAI(AGGREGATION_SYSTEM_PROMPT, aggregateUser);
    }

    // Persist the latest evaluation for this symbol (single entry per symbol).
    try {
        await setEvaluation(symbol, evaluation);
    } catch (err) {
        console.error('Failed to persist evaluation:', err);
    }

    return {
        symbol,
        stats,
        samples: condensed,
        batch: {
            batchSize,
            batchCount: chunks.length,
            sampleCounts: chunks.map((c) => c.length),
        },
        ...(includeBatchEvaluations ? { batchEvaluations } : {}),
        evaluation,
    };
}

async function runEvaluationJob(record: EvaluateJobRecord) {
    const key = jobKey(record.id);
    const now = Date.now();
    await kvSetJson<EvaluateJobRecord>(
        key,
        {
            ...record,
            status: 'running',
            updatedAt: now,
        },
        EVALUATE_JOB_TTL_SECONDS,
    );
    try {
        const result = await runEvaluation({
            symbol: record.symbol,
            limit: record.params.limit,
            batchSize: record.params.batchSize,
            includeBatchEvaluations: record.params.includeBatchEvaluations,
        });
        await kvSetJson<EvaluateJobRecord>(
            key,
            {
                ...record,
                status: 'succeeded',
                updatedAt: Date.now(),
                result,
            },
            EVALUATE_JOB_TTL_SECONDS,
        );
    } catch (err: any) {
        await kvSetJson<EvaluateJobRecord>(
            key,
            {
                ...record,
                status: 'failed',
                updatedAt: Date.now(),
                error: err?.message || String(err),
            },
            EVALUATE_JOB_TTL_SECONDS,
        );
    }
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    setNoStoreHeaders(res);

    const body = req.query ?? {};
    const jobId = String(body?.jobId || '').trim();
    const executeMode = parseBoolParam(body?.execute as string | string[] | undefined, false);
    if (jobId) {
        const record = await kvGetJson<EvaluateJobRecord>(jobKey(jobId));
        if (!record) return res.status(404).json({ error: 'job_not_found', jobId });
        if (executeMode) {
            if (record.status === 'queued' || record.status === 'running') {
                await runEvaluationJob(record);
            }
            const updated = await kvGetJson<EvaluateJobRecord>(jobKey(jobId));
            return res.status(200).json(updated ?? record);
        }
        return res.status(200).json(record);
    }

    const symbol = String(body?.symbol || '').toUpperCase();
    const limit = Math.min(30, Math.max(5, Number(body?.limit ?? 50)));
    const batchSize = Math.min(10, Math.max(2, Number(body?.batchSize ?? 5)));
    const includeBatchEvaluations = parseBoolParam(body?.includeBatchEvaluations as string | string[] | undefined, false);
    const asyncMode = parseBoolParam(body?.async as string | string[] | undefined, false);

    if (!symbol) {
        return res.status(400).json({ error: 'symbol_required' });
    }

    if (asyncMode) {
        const id = crypto.randomUUID();
        const now = Date.now();
        const record: EvaluateJobRecord = {
            id,
            symbol,
            status: 'queued',
            createdAt: now,
            updatedAt: now,
            params: {
                limit,
                batchSize,
                includeBatchEvaluations,
            },
        };
        await kvSetJson<EvaluateJobRecord>(jobKey(id), record, EVALUATE_JOB_TTL_SECONDS);
        // Trigger detached worker request so execution isn't tied to this response lifecycle.
        const host = req.headers.host;
        const xfp = req.headers['x-forwarded-proto'];
        const proto =
            typeof xfp === 'string' && xfp
                ? xfp
                : Array.isArray(xfp) && xfp.length
                ? xfp[0]
                : host?.includes('localhost')
                ? 'http'
                : 'https';
        if (host) {
            const workerUrl = `${proto}://${host}/api/evaluate?jobId=${encodeURIComponent(id)}&execute=true`;
            const workerHeaders: Record<string, string> = {};
            const adminHeader = firstHeaderValue(req.headers['x-admin-access-secret']);
            const cookieHeader = firstHeaderValue(req.headers.cookie);
            const vercelBypass = process.env.VERCEL_AUTOMATION_BYPASS_SECRET;
            if (adminHeader) workerHeaders['x-admin-access-secret'] = adminHeader;
            if (cookieHeader) workerHeaders.cookie = cookieHeader;
            if (vercelBypass) workerHeaders['x-vercel-protection-bypass'] = vercelBypass;

            void fetch(workerUrl, {
                method: 'GET',
                headers: Object.keys(workerHeaders).length ? workerHeaders : undefined,
                cache: 'no-store',
            })
                .then(async (workerRes) => {
                    if (workerRes.ok) return;
                    await kvSetJson<EvaluateJobRecord>(
                        jobKey(id),
                        {
                            ...record,
                            status: 'failed',
                            updatedAt: Date.now(),
                            error: `worker_trigger_failed_status_${workerRes.status}`,
                        },
                        EVALUATE_JOB_TTL_SECONDS,
                    );
                    console.error(`Failed to trigger async evaluate worker: HTTP ${workerRes.status}`);
                })
                .catch((err) => {
                    console.error('Failed to trigger async evaluate worker:', err);
                });
        } else {
            // Fallback for unusual runtime where host header is unavailable.
            void runEvaluationJob(record).catch((err) => {
                console.error('Failed to run fallback async evaluate job:', err);
            });
        }
        return res.status(202).json({
            jobId: id,
            status: 'queued',
            poll: `/api/evaluate?jobId=${id}`,
        });
    }

    try {
        const result = await runEvaluation({
            symbol,
            limit,
            batchSize,
            includeBatchEvaluations,
        });
        return res.status(200).json(result);
    } catch (err: any) {
        if (String(err?.message || '').includes('no_history')) {
            return res.status(404).json({ error: 'no_history', symbol });
        }
        throw err;
    }
}
