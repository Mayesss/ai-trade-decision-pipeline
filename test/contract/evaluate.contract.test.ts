// Contract: /api/swing/evaluate — the third AI prompt surface (the decision-
// quality evaluator / prompt auditor). Pure KV + PG + AI: history entries come
// from the KV index (seeded through the real appendDecisionHistory write
// path), their prompts are hydrated from swing.decisions (KV stores them with
// prompt:null by design), one evaluator call goes to the gateway, and the
// result lands back in KV under evaluation:<SYMBOL>.

import { expect, test } from 'vitest';

import handler from '../../pages/api/swing/evaluate';
import { appendDecisionHistory } from '../../lib/history';
import { conversation, FIXED_NOW_MS, startBoundary } from '../harness';
import { createApiRequest, createApiResponse } from '../harness/next';
import { resetEntries } from '../harness/recorder';
import { openAiDecides } from '../harness/worlds/aiGateway';
import { kvWorld } from '../harness/worlds/kv';

import type { PgResponder } from '../harness/pg';
import type { DecisionHistoryEntry } from '../../lib/history';

const TS_1 = FIXED_NOW_MS - 2 * 3600_000;
const TS_2 = FIXED_NOW_MS - 1 * 3600_000;

function historyEntry(timestamp: number, action: string, summary: string): DecisionHistoryEntry {
    return {
        timestamp,
        symbol: 'BTCUSDT',
        platform: 'bitget',
        timeFrame: '4H',
        dryRun: false,
        prompt: { system: 'SWING SYSTEM PROMPT v1', user: `STATE at ${new Date(timestamp).toISOString()}` },
        aiDecision: { action, summary, reason: 'test decision', signal_strength: 'MEDIUM' } as never,
        execResult: { placed: action !== 'HOLD', orderId: action !== 'HOLD' ? 'ord-1' : null },
        snapshot: { price: 77000, gates: { spread_ok: true } } as never,
    };
}

// Prompt hydration source: KV history rows carry prompt:null, so the route
// pulls prompt_json from swing.decisions for the window.
const evaluatePg: PgResponder = (text) => {
    const kind = text.split(' ')[0].toUpperCase();
    if (!['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH'].includes(kind)) return 0; // schema bootstrap
    if (text.includes('FROM swing.decisions')) {
        return [TS_1, TS_2].map((ts, i) => ({
            id: 900 + i,
            decided_at_ms: String(ts),
            symbol: 'BTCUSDT',
            platform: 'bitget',
            action: i === 0 ? 'BUY' : 'HOLD',
            dry_run: false,
            prompt_json: { system: 'SWING SYSTEM PROMPT v1', user: `STATE at ${new Date(ts).toISOString()}` },
            ai_decision_json: {},
            exec_result_json: {},
            snapshot_json: {},
        }));
    }
    throw new Error(`evaluate pg world: unexpected query: ${text}`);
};

const EVALUATION = {
    overall_grade: 'B',
    prompt_quality: 'STATE blocks are complete; funding context occasionally missing.',
    decision_quality: 'Entries respect structure; HOLD reasoning is consistent.',
    recommendations: ['surface funding-rate deltas in the prompt'],
};

startBoundary(() => ({
    http: [...kvWorld(), openAiDecides(EVALUATION)],
    db: evaluatePg,
}));

test('history read, prompt hydration, evaluator call, KV persist', async () => {
    // Seed the KV history through the real write path, then reset the
    // recorder so the snapshot holds only the evaluation's own conversation.
    await appendDecisionHistory(historyEntry(TS_1, 'BUY', 'entry'));
    await appendDecisionHistory(historyEntry(TS_2, 'HOLD', 'manage'));
    resetEntries();

    const req = createApiRequest({
        path: '/api/swing/evaluate',
        query: { symbol: 'BTCUSDT', limit: '5', batchSize: '5' },
    });
    const { res, state } = createApiResponse();
    await handler(req as never, res as never);

    expect(state.statusCode).toBe(200);
    const body = state.body as Record<string, any>;
    expect(body.symbol).toBe('BTCUSDT');
    expect(body.stats.totalSamples).toBe(2);
    expect(body.evaluation).toEqual(EVALUATION);

    const text = await conversation();
    expect(text).toContain('POST https://ai-gateway.vercel.sh/v1/responses');
    expect(text).toContain('evaluation%3ABTCUSDT');

    await expect(text).toMatchFileSnapshot('./__snapshots__/evaluate-btcusdt.txt');
});
