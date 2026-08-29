// lib/swing/aiBouncer.ts — verdict parsing, flag gating, the json_schema →
// instruct-JSON fallback, and every fail-open path (a bouncer failure must
// never block the full decision call).

import assert from 'node:assert/strict';
import { afterEach, test, vi } from 'vitest';

import { parseBouncerVerdict, runAiBouncer, swingAiBouncerEnabled } from '../../../lib/swing/aiBouncer';

import type { AiBouncerInput } from '../../../lib/swing/aiBouncer';

afterEach(() => {
    vi.unstubAllEnvs();
});

const INPUT: AiBouncerInput = {
    symbol: 'BTCUSDT',
    platform: 'bitget',
    category: 'crypto',
    price: 65000,
    change_24h_pct: 1.2,
    signal_strength: 'MEDIUM',
    micro_bias_calc: 'UP',
    primary_bias: 'UP',
    macro_bias: 'NEUTRAL',
    context_bias: 'UP',
    primary_trend_up: true,
    primary_trend_down: false,
    primary_breakout_confirmed: true,
    primary_breakdown_confirmed: false,
    micro_entry_ok: true,
    aligned_driver_count: 3,
    regime_alignment: 0.5,
    location_confluence_score: 2,
    micro_extension_atr: 0.8,
    primary_extension_atr: 1.1,
    breakout_retest_ok_primary: true,
    breakout_retest_dir_primary: 'up',
    actionability_branch: 'confirmed_primary_structure',
};

// ---- parseBouncerVerdict -------------------------------------------------

test('parses clean JSON', () => {
    const v = parseBouncerVerdict('{"proceed":false,"confidence":0.8,"reason":"mixed biases"}');
    assert.deepEqual(v, { proceed: false, confidence: 0.8, reason: 'mixed biases' });
});

test('parses fenced JSON with surrounding prose', () => {
    const v = parseBouncerVerdict('Sure!\n```json\n{"proceed":true,"confidence":0.9,"reason":"clean breakout"}\n```');
    assert.deepEqual(v, { proceed: true, confidence: 0.9, reason: 'clean breakout' });
});

test('clamps confidence into [0,1] and truncates reason to 140 chars', () => {
    const v = parseBouncerVerdict(`{"proceed":true,"confidence":7,"reason":"${'r'.repeat(300)}"}`);
    assert.ok(v);
    assert.equal(v.confidence, 1);
    assert.equal(v.reason.length, 140);
});

test('garbage, non-boolean proceed, and empty input all parse to null', () => {
    assert.equal(parseBouncerVerdict('the setup looks weak, skip it'), null);
    assert.equal(parseBouncerVerdict('{"proceed":"no","confidence":0.5,"reason":"x"}'), null);
    assert.equal(parseBouncerVerdict(''), null);
    assert.equal(parseBouncerVerdict(null), null);
});

// ---- runAiBouncer --------------------------------------------------------

type FetchCall = { url: string; body: Record<string, unknown> };

function stubFetch(responder: (call: FetchCall, index: number) => Response): {
    calls: FetchCall[];
    restore: () => void;
} {
    const originalFetch = globalThis.fetch;
    const calls: FetchCall[] = [];
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
        const call = { url: String(input), body: JSON.parse(String(init?.body)) as Record<string, unknown> };
        calls.push(call);
        return responder(call, calls.length - 1);
    }) as typeof fetch;
    return { calls, restore: () => (globalThis.fetch = originalFetch) };
}

function grokResponse(content: string, status = 200): Response {
    return new Response(
        JSON.stringify({
            id: 'chatcmpl-1',
            model: 'spacexai/grok-4.1-fast-reasoning',
            choices: [{ index: 0, message: { role: 'assistant', content }, finish_reason: 'stop' }],
            usage: { prompt_tokens: 400, completion_tokens: 60 },
        }),
        { status },
    );
}

test('flag off (default): returns null without any fetch', async () => {
    const { calls, restore } = stubFetch(() => grokResponse('never'));
    try {
        assert.equal(swingAiBouncerEnabled(), false);
        assert.equal(await runAiBouncer(INPUT), null);
        assert.equal(calls.length, 0);
    } finally {
        restore();
    }
});

test('flag on: sends json_schema response_format and returns the verdict with usage', async () => {
    vi.stubEnv('SWING_AI_BOUNCER_ENABLED', 'true');
    vi.stubEnv('AI_GATEWAY_API_KEY', 'test-key');
    const { calls, restore } = stubFetch(() =>
        grokResponse('{"proceed":false,"confidence":0.7,"reason":"low confluence"}'),
    );
    try {
        const verdict = await runAiBouncer(INPUT);
        assert.ok(verdict);
        assert.equal(verdict.proceed, false);
        assert.equal(verdict.confidence, 0.7);
        assert.equal(verdict.reason, 'low confluence');
        assert.equal(verdict.model, 'spacexai/grok-4.1-fast-reasoning');
        assert.deepEqual(verdict.usage, { input_tokens: 400, output_tokens: 60 });

        assert.equal(calls.length, 1);
        assert.equal(calls[0].url, 'https://ai-gateway.vercel.sh/v1/chat/completions');
        assert.equal(calls[0].body.model, 'spacexai/grok-4.1-fast-reasoning');
        const rf = calls[0].body.response_format as Record<string, unknown>;
        assert.equal(rf.type, 'json_schema');
        // The user turn is the compact technical snapshot, verbatim.
        const messages = calls[0].body.messages as Array<{ role: string; content: string }>;
        assert.deepEqual(JSON.parse(messages[1].content), INPUT);
    } finally {
        restore();
    }
});

test('response_format rejected with 400: retries once without it', async () => {
    vi.stubEnv('SWING_AI_BOUNCER_ENABLED', 'true');
    vi.stubEnv('AI_GATEWAY_API_KEY', 'test-key');
    const { calls, restore } = stubFetch((call, index) =>
        index === 0
            ? grokResponse('unsupported', 400)
            : grokResponse('{"proceed":true,"confidence":0.6,"reason":"ok"}'),
    );
    try {
        const verdict = await runAiBouncer(INPUT);
        assert.ok(verdict);
        assert.equal(verdict.proceed, true);
        assert.equal(calls.length, 2);
        assert.ok('response_format' in calls[0].body);
        assert.ok(!('response_format' in calls[1].body));
    } finally {
        restore();
    }
});

test('HTTP 500, unparseable verdict, and missing key all fail open to null', async () => {
    vi.stubEnv('SWING_AI_BOUNCER_ENABLED', 'true');
    vi.stubEnv('AI_GATEWAY_API_KEY', 'test-key');

    const server = stubFetch(() => grokResponse('boom', 500));
    try {
        assert.equal(await runAiBouncer(INPUT), null);
    } finally {
        server.restore();
    }

    const garbage = stubFetch(() => grokResponse('I would skip this one.'));
    try {
        assert.equal(await runAiBouncer(INPUT), null);
    } finally {
        garbage.restore();
    }

    vi.stubEnv('AI_GATEWAY_API_KEY', '');
    const noKey = stubFetch(() => grokResponse('never'));
    try {
        assert.equal(await runAiBouncer(INPUT), null);
        assert.equal(noKey.calls.length, 0);
    } finally {
        noKey.restore();
    }
});
