// Contract: one postmortem execution end-to-end — the SECOND prompt surface
// (the forensic-analyst system prompt + dossier user message go out to the
// gateway and into the snapshot), plus the queue choreography around it:
// claim UPDATE, decision-window / tick-log / lesson-library reads, the
// post-exit bitget candle read (window derived from the row's exit_ts_ms),
// the ai-health KV read, the complete UPDATE and the lesson INSERT.

import { expect, test } from 'vitest';

import handler from '../../pages/api/swing/postmortem';
import { createApiRequest, createApiResponse } from '../harness/next';
import { conversation, FIXED_NOW_MS, startBoundary } from '../harness';
import { responsesDecides } from '../harness/worlds/aiGateway';
import { bitgetGet } from '../harness/worlds/bitget';
import { kvWorld } from '../harness/worlds/kv';

import type { PgResponder } from '../harness/pg';

// Exit >12h before the frozen clock, so the candle window's Date.now() clamp
// never bites and endTime = exit + 12h is a pure function of the row.
const EXIT_MS = FIXED_NOW_MS - 24 * 3600_000;
const ENTRY_MS = EXIT_MS - 8 * 3600_000;

// The claimed row (claim UPDATE already flipped status to running).
const POSTMORTEM_ROW = {
    id: 42,
    platform: 'bitget',
    symbol: 'BTCUSDT',
    position_key: `BTCUSDT-${ENTRY_MS}`,
    status: 'running',
    trigger_source: 'close',
    side: 'long',
    entry_ts_ms: String(ENTRY_MS),
    exit_ts_ms: String(EXIT_MS),
    entry_price: '62000',
    exit_price: '60760',
    pnl_pct: '-2',
    pnl_net: '-124', // loss → the loss-analyst prompt + POSTMORTEM_SCHEMA
    verdict: null,
    lesson: null,
    lesson_scope: null,
    report_json: null,
    dossier_json: null,
    model: null,
    usage_json: null,
    error: null,
    attempts: 1,
    created_at: new Date(EXIT_MS + 5 * 60_000).toISOString(),
    updated_at: new Date(EXIT_MS + 5 * 60_000).toISOString(),
};

const DECISION_ROWS = [
    {
        id: 900,
        decided_at_ms: String(ENTRY_MS),
        symbol: 'BTCUSDT',
        platform: 'bitget',
        action: 'BUY',
        dry_run: false,
        prompt_json: { system: 'SWING SYSTEM PROMPT v1', user: 'STATE: entry tick user prompt' },
        ai_decision_json: {
            action: 'BUY',
            summary: 'trend continuation',
            reason: 'HTF up, pullback held',
            take_profit_price: 64000,
            stop_loss_price: 61000,
            ai_model: 'openai/gpt-5.6-sol',
        },
        exec_result_json: { placed: true, orderId: 'ord-1', leverage: 5 },
        snapshot_json: {},
    },
    {
        id: 901,
        decided_at_ms: String(EXIT_MS),
        symbol: 'BTCUSDT',
        platform: 'bitget',
        action: 'CLOSE',
        dry_run: false,
        prompt_json: { system: 'SWING SYSTEM PROMPT v1', user: 'STATE: close tick user prompt' },
        ai_decision_json: { action: 'CLOSE', summary: 'thesis invalidated', reason: 'lost the level', exit_size_pct: 100 },
        exec_result_json: { closed: true },
        snapshot_json: {},
    },
];

const postmortemPg: PgResponder = (text) => {
    const kind = text.split(' ')[0].toUpperCase();
    if (!['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH'].includes(kind)) return 0; // schema bootstrap
    if (text.startsWith('UPDATE swing.postmortems') && text.includes("'running'")) return [POSTMORTEM_ROW];
    if (text.startsWith('UPDATE swing.postmortems')) return 1; // complete / fail / requeue
    if (text.startsWith('INSERT INTO swing.lessons')) return [{ id: 7 }];
    if (text.includes('FROM swing.decisions')) return DECISION_ROWS;
    if (text.includes('FROM swing.tick_log')) return [];
    if (text.includes('FROM swing.lessons')) return [];
    throw new Error(`postmortem pg world: unexpected query: ${text}`);
};

// The forensic report the canned model returns — verdict is the only hard
// requirement; lesson_action 'new' drives the lesson INSERT.
const REPORT = {
    verdict: 'stop_placement',
    confidence: 0.72,
    timeline_analysis: 'Stop sat inside ordinary 4H noise; the level itself held.',
    what_went_wrong: ['stop placed 0.6 ATR from entry on a 2 ATR structure'],
    missed_signals: [],
    gate_impact: null,
    suggestions: ['anchor stops beyond the swing low, not a fixed distance'],
    lesson_adherence: null,
    lesson_action: 'new',
    reinforce_lesson_id: null,
    lesson: 'Place BTC continuation stops at least 1.2 primary-ATR beyond the swing low.',
    lesson_scope: 'symbol',
};

// One 15m candle inside the post-exit window.
const POST_EXIT_CANDLES = [[String(EXIT_MS + 15 * 60_000), '60760', '60900', '60700', '60850', '120', '7290000']];

startBoundary({
    http: [...kvWorld(), bitgetGet('/api/v2/mix/market/candles', POST_EXIT_CANDLES), responsesDecides(REPORT)],
    db: postmortemPg,
});

test('claim, dossier assembly, forensic prompt, complete + lesson insert', async () => {
    const req = createApiRequest({
        path: '/api/swing/postmortem',
        query: { id: '42', execute: 'true' },
    });
    const { res, state } = createApiResponse();
    await handler(req as never, res as never);

    expect(state.statusCode).toBe(200);
    expect(state.body).toMatchObject({
        id: 42,
        status: 'succeeded',
        verdict: 'stop_placement',
        lesson: REPORT.lesson,
    });

    const text = await conversation();
    // The dossier prompt carries the pivotal tick prompts verbatim.
    expect(text).toContain('STATE: entry tick user prompt');
    expect(text).toContain('INSERT INTO swing.lessons');

    await expect(text).toMatchFileSnapshot('./__snapshots__/postmortem-loss.txt');
});
