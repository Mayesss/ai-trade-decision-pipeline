// lib/swing/perplexity.ts — flag gating, gateway request shape, fail-open
// paths. KV cache behavior is covered by the contract suite (the unit project
// runs with KV unconfigured, so the module's cache try/catch fails open to a
// fetch here — itself a property worth the coverage).

import assert from 'node:assert/strict';
import { afterEach, test, vi } from 'vitest';

import { loadPerplexityContext, swingPerplexityEnabled } from '../../../lib/swing/perplexity';

afterEach(() => {
    vi.unstubAllEnvs();
});

type FetchCall = { url: string; init: RequestInit };

function stubFetch(responder: (call: FetchCall) => Response): { calls: FetchCall[]; restore: () => void } {
    const originalFetch = globalThis.fetch;
    const calls: FetchCall[] = [];
    globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
        const call = { url: String(input), init: init ?? {} };
        calls.push(call);
        return responder(call);
    }) as typeof fetch;
    return { calls, restore: () => (globalThis.fetch = originalFetch) };
}

function sonarResponse(content: unknown, status = 200): Response {
    return new Response(
        JSON.stringify({
            id: 'chatcmpl-1',
            model: 'perplexity/sonar',
            choices: [{ index: 0, message: { role: 'assistant', content }, finish_reason: 'stop' }],
            usage: { prompt_tokens: 100, completion_tokens: 50 },
        }),
        { status },
    );
}

test('flag off (default): returns null without any fetch', async () => {
    const { calls, restore } = stubFetch(() => sonarResponse('never'));
    try {
        assert.equal(swingPerplexityEnabled(), false);
        assert.equal(await loadPerplexityContext('BTCUSDT', { platform: 'bitget' }), null);
        assert.equal(calls.length, 0);
    } finally {
        restore();
    }
});

test('flag on: calls chat/completions with sonar-legal params only and returns the digest', async () => {
    vi.stubEnv('SWING_PERPLEXITY_ENABLED', 'true');
    vi.stubEnv('AI_GATEWAY_API_KEY', 'test-key');
    const { calls, restore } = stubFetch(() => sonarResponse('- 2h ago: ETF inflows steady. Mood: bullish.'));
    try {
        const ctx = await loadPerplexityContext('BTCUSDT', { platform: 'bitget', category: 'crypto' });
        assert.ok(ctx);
        assert.equal(ctx.text, '- 2h ago: ETF inflows steady. Mood: bullish.');
        assert.equal(ctx.model, 'perplexity/sonar');
        assert.equal(ctx.fromCache, false);
        assert.ok(Number.isFinite(ctx.fetchedAtMs));

        // KV is unconfigured in the unit project → the only call is the gateway.
        assert.equal(calls.length, 1);
        assert.equal(calls[0].url, 'https://ai-gateway.vercel.sh/v1/chat/completions');
        const headers = calls[0].init.headers as Record<string, string>;
        assert.equal(headers.Authorization, 'Bearer test-key');
        const body = JSON.parse(String(calls[0].init.body));
        assert.equal(body.model, 'perplexity/sonar');
        assert.equal(body.messages.length, 2);
        assert.ok(String(body.messages[1].content).includes('Asset: BTC'));
        // Whole-hour markers collapsed the whole freshest window — the reason
        // the digest exists — into an unreadable "0h ago".
        assert.ok(String(body.messages[0].content).includes('NEVER write "0h ago"'));
        // Sonar accepts ONLY max_tokens/temperature/stop — anything else 400s.
        assert.deepEqual(
            Object.keys(body).sort(),
            ['max_tokens', 'messages', 'model', 'temperature'].sort(),
        );
    } finally {
        restore();
    }
});

test('HTTP failure fails open to null', async () => {
    vi.stubEnv('SWING_PERPLEXITY_ENABLED', 'true');
    vi.stubEnv('AI_GATEWAY_API_KEY', 'test-key');
    const { restore } = stubFetch(() => sonarResponse('irrelevant', 500));
    try {
        assert.equal(await loadPerplexityContext('BTCUSDT', { platform: 'bitget' }), null);
    } finally {
        restore();
    }
});

test('empty or non-string content fails open to null', async () => {
    vi.stubEnv('SWING_PERPLEXITY_ENABLED', 'true');
    vi.stubEnv('AI_GATEWAY_API_KEY', 'test-key');
    const { restore } = stubFetch(() => sonarResponse('   '));
    try {
        assert.equal(await loadPerplexityContext('ETHUSDT', { platform: 'bitget' }), null);
    } finally {
        restore();
    }
});

test('missing gateway key fails open to null instead of throwing', async () => {
    vi.stubEnv('SWING_PERPLEXITY_ENABLED', 'true');
    const { calls, restore } = stubFetch(() => sonarResponse('never'));
    try {
        assert.equal(await loadPerplexityContext('BTCUSDT', { platform: 'bitget' }), null);
        assert.equal(calls.length, 0);
    } finally {
        restore();
    }
});

test('digest text is hard-capped at 2000 chars', async () => {
    vi.stubEnv('SWING_PERPLEXITY_ENABLED', 'true');
    vi.stubEnv('AI_GATEWAY_API_KEY', 'test-key');
    const { restore } = stubFetch(() => sonarResponse('x'.repeat(5000)));
    try {
        const ctx = await loadPerplexityContext('BTCUSDT', { platform: 'bitget' });
        assert.ok(ctx);
        assert.equal(ctx.text.length, 2000);
    } finally {
        restore();
    }
});
