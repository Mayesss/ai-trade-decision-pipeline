// Transcript compaction: a chained thread resends its whole transcript on every
// call, so each STORED turn used to carry a full STATE/MARKET tape (measured
// 14.4K avg / 33K peak input tokens on in-position ticks vs 7.4K flat). The
// live turn stays complete; only the ARCHIVED copy is slimmed to the readings
// the decision rested on plus whatever triggered the look.
//
// Two halves are tested here: computeSwingState produces the abbreviated turn,
// and the OpenAI client archives THAT instead of what it sent.

import assert from 'node:assert/strict';
import { test } from 'vitest';

import { computeSwingState } from '../../lib/ai';
import { callAIThread } from '../../lib/openAi';

const NOW_MS = 1_750_000_000_000;
const bundle: any = {
    ticker: [{ lastPr: '100', change24h: '0.01' }],
    candles: [],
};
const indicators: any = {
    micro: '',
    macro: '',
    primary: { summary: '', timeframe: '4h' },
    context: { summary: '', timeframe: '1d' },
    microTimeFrame: '1h',
    macroTimeFrame: '1d',
    sr: {},
    rawCandles: {},
};
const momentum: any = { microExtensionInAtr: 0, info: { microEntryOk: true } };
const analytics: any = {
    topWalls: { bid: [{ price: 99, size: 10 }], ask: [{ price: 101, size: 12 }] },
    volume_profile: [{ price: 100, volume: 500 }],
};

function build(opts: { positionContext?: any; cooldownWake?: any } = {}) {
    const state = computeSwingState(
        'BTCUSDT',
        '4h',
        bundle,
        analytics,
        opts.positionContext ? 'long' : 'none',
        null,
        null,
        indicators,
        {},
        opts.positionContext ?? null,
        momentum,
        [{ action: 'HOLD', timestamp: NOW_MS - 4 * 3600_000 } as any],
        null,
        true,
        5,
        undefined,
        'crypto',
        'bitget',
        null,
        NOW_MS,
        null,
        opts.cooldownWake ?? null,
    );
    return state.assemble(null, []);
}

test('the abbreviated turn keeps the readings the decision rested on', () => {
    const { userCompact } = build();
    assert.ok(userCompact.startsWith('[ABBREVIATED EARLIER TURN'), 'must be self-labelling');
    const state = JSON.parse(userCompact.slice(userCompact.indexOf('STATE: ') + 7, userCompact.indexOf('\nMARKET: ')));
    for (const key of [
        'time',
        'biases',
        'trend',
        'structure',
        'momentum',
        'extension_atr',
        'volatility',
        'location',
        'levels',
        'position',
    ]) {
        assert.ok(key in state, `abbreviated STATE should keep ${key}`);
    }
    assert.deepEqual(Object.keys(state.time), ['iso_utc'], 'only the timestamp is worth keeping from time');
    assert.ok('rsi' in state.momentum && 'micro_entry_ok' in state.momentum);
});

test('the abbreviated turn drops the stale tape and the redundant history', () => {
    const { userCompact } = build();
    for (const dropped of [
        'geometry',
        'recent_candles',
        'volume_profile',
        'bid_walls',
        'ask_walls',
        'recent_actions',
        'forex_events',
        'atr_pctile',
        'value_state_macro',
    ]) {
        assert.ok(!userCompact.includes(dropped), `abbreviated turn should not carry ${dropped}`);
    }
});

test('the abbreviated turn carries the trigger that caused the look', () => {
    const { userCompact } = build({
        cooldownWake: { crossed: 'above', level: 105, setAtMs: NOW_MS - 60_000, note: 'breakout check' },
    });
    const market = JSON.parse(userCompact.slice(userCompact.indexOf('MARKET: ') + 8));
    assert.equal(market.cooldown_wake.level, 105);
    assert.equal(market.cooldown_wake.note, 'breakout check');
    assert.ok(market.price, 'price is the anchor for every archived reading');
});

test('the LIVE turn is untouched — only the archived copy is slimmed', () => {
    const { user, userCompact } = build();
    assert.ok(user.includes('recent_candles'), 'the model still receives the full tape');
    assert.ok(user.includes('volume_profile'));
    assert.ok(!user.includes('ABBREVIATED'));
    // Smaller, necessarily. No ratio is asserted here: this fixture carries no
    // real candles, so it renders no geometry and a stub tape — the savings that
    // matter come from exactly those, and are measured on the recorded-market
    // contract fixtures instead.
    assert.ok(
        userCompact.length < user.length,
        `abbreviated turn should be smaller (was ${userCompact.length} vs ${user.length})`,
    );
});

test('callAIThread archives the abbreviated turn but SENDS the full one', async () => {
    const originalFetch = globalThis.fetch;
    let sentBody: any = null;
    globalThis.fetch = (async (_input: RequestInfo | URL, init?: RequestInit) => {
        sentBody = JSON.parse(String(init?.body));
        return new Response(
            JSON.stringify({
                id: 'resp_1',
                model: 'openai/gpt-5.6-sol',
                output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: '{"action":"HOLD"}' }] }],
                usage: { input_tokens: 10, output_tokens: 2, input_tokens_details: { cached_tokens: 0 } },
            }),
            { status: 200 },
        );
    }) as typeof fetch;
    process.env.AI_GATEWAY_API_KEY = 'test-key';
    try {
        const out = await callAIThread('SYSTEM', 'FULL USER TURN', undefined, {
            userForTranscript: 'SHORT RECORD',
        });
        assert.equal(sentBody.input, 'FULL USER TURN', 'the model must receive the full turn');
        assert.deepEqual(out.appendTurns, [
            { role: 'user', content: 'SHORT RECORD' },
            { role: 'assistant', content: '{"action":"HOLD"}' },
        ]);
    } finally {
        globalThis.fetch = originalFetch;
        delete process.env.AI_GATEWAY_API_KEY;
    }
});

test('callAIThread falls back to the sent turn when no abbreviated copy is given', async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async () =>
        new Response(
            JSON.stringify({
                id: 'resp_1',
                model: 'openai/gpt-5.6-sol',
                output: [{ type: 'message', role: 'assistant', content: [{ type: 'output_text', text: '{"action":"HOLD"}' }] }],
                usage: { input_tokens: 10, output_tokens: 2, input_tokens_details: { cached_tokens: 0 } },
            }),
            { status: 200 },
        )) as typeof fetch;
    process.env.AI_GATEWAY_API_KEY = 'test-key';
    try {
        const out = await callAIThread('SYSTEM', 'FULL USER TURN');
        assert.equal(out.appendTurns[0].content, 'FULL USER TURN');
    } finally {
        globalThis.fetch = originalFetch;
        delete process.env.AI_GATEWAY_API_KEY;
    }
});
