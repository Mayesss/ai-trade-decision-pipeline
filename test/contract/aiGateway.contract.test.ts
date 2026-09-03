// Contract: lib/aiProvider.callSwingDecision — the single choke point for all
// swing AI traffic. Both dialects ride the same gateway host; both replay
// the stored transcript (the gateway is stateless). The snapshot captures the
// FULL outgoing prompt — this is the prompt regression net: any change to
// system/user assembly or transcript replay shows up as a snapshot diff.
// Every call also touches the KV health flag (lib/swing/aiHealth.ts).

import { expect, test, vi } from 'vitest';

import { callSwingDecision } from '../../lib/aiProvider';
import { conversation, startBoundary } from '../harness';
import { messagesDecides, responsesDecides } from '../harness/worlds/aiGateway';
import { kvWorld } from '../harness/worlds/kv';

const DECISION = { action: 'HOLD', summary: 'no edge at this level', confidence: 55 };

const SCHEMA = {
    name: 'swing_decision',
    schema: {
        type: 'object',
        properties: {
            action: { type: 'string' },
            summary: { type: 'string' },
            confidence: { type: 'number' },
        },
        required: ['action', 'summary', 'confidence'],
        additionalProperties: false,
    } as Record<string, unknown>,
};

const SYSTEM = 'You are the swing trading desk.';
const USER = 'STATE: flat. MARKET: rangebound chop around VWAP.';

startBoundary(() => ({
    http: [...kvWorld(), responsesDecides(DECISION), messagesDecides(DECISION)],
}));

test('Responses dialect: forced tool call with replayed transcript', async () => {
    vi.stubEnv('SWING_AI_PROVIDER', 'openai');

    const result = await callSwingDecision({
        system: SYSTEM,
        user: USER,
        schema: SCHEMA,
        thread: {
            transcript: [
                { role: 'user', content: 'prior tick state' },
                { role: 'assistant', content: '{"action":"HOLD"}' },
            ],
        },
    });

    expect(result.json).toEqual(DECISION);
    expect(result.dialect).toBe('responses');
    expect(result.responseId).toBe('resp_test-1');
    expect(result.usage).toEqual({
        input_tokens: 1000,
        output_tokens: 200,
        cache_creation_input_tokens: null,
        cache_read_input_tokens: 0,
    });
    expect(result.appendTurns).toEqual([
        { role: 'user', content: USER },
        { role: 'assistant', content: JSON.stringify(DECISION) },
    ]);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/ai-responses-thread.txt');
});

test('Messages dialect: call with cache breakpoints and echoed thinking blocks', async () => {
    vi.stubEnv('SWING_AI_PROVIDER', 'claude');

    const result = await callSwingDecision({
        system: SYSTEM,
        user: USER,
        schema: SCHEMA,
        thread: {
            transcript: [
                { role: 'user', content: [{ type: 'text', text: 'prior tick state' }] },
                { role: 'assistant', content: [{ type: 'text', text: '{"action":"HOLD"}' }] },
            ],
        },
    });

    expect(result.json).toEqual(DECISION);
    expect(result.dialect).toBe('messages');
    expect(result.responseId).toBe('msg_test-1');
    expect(result.usage).toEqual({
        input_tokens: 1000,
        output_tokens: 200,
        cache_creation_input_tokens: 0,
        cache_read_input_tokens: 0,
    });
    // The assistant turn is stored VERBATIM — thinking block included, it must
    // be echoed back unchanged on the next tick of the same thread.
    expect(result.appendTurns?.[1]).toMatchObject({
        role: 'assistant',
        content: [
            { type: 'thinking', thinking: 'canned reasoning' },
            { type: 'text', text: JSON.stringify(DECISION) },
        ],
    });

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/ai-messages-thread.txt');
});

// Transcript compaction, at the dialect seam: the model must receive the FULL
// turn while the thread archives the abbreviated one. Asserted per dialect
// because the two clients build appendTurns differently (responses plain string
// content, messages MessageParam text blocks) — a regression in either silently
// stores the wrong text, and only shows up as an oversized thread days later.
const ABBREVIATED = '[ABBREVIATED EARLIER TURN — tape dropped]\nSTATE: {"price":1}';

test('Responses dialect: archives the abbreviated turn, sends the full one', async () => {
    vi.stubEnv('SWING_AI_PROVIDER', 'openai');

    const result = await callSwingDecision({
        system: SYSTEM,
        user: USER,
        schema: SCHEMA,
        userForTranscript: ABBREVIATED,
    });

    expect(result.appendTurns?.[0]).toEqual({ role: 'user', content: ABBREVIATED });
    // …while the request that actually went out carried the full turn.
    expect(await conversation()).toContain(USER);
});

test('Messages dialect: archives the abbreviated turn, sends the full one', async () => {
    vi.stubEnv('SWING_AI_PROVIDER', 'claude');

    const result = await callSwingDecision({
        system: SYSTEM,
        user: USER,
        schema: SCHEMA,
        userForTranscript: ABBREVIATED,
    });

    expect(result.appendTurns?.[0]).toEqual({
        role: 'user',
        content: [{ type: 'text', text: ABBREVIATED }],
    });
    expect(await conversation()).toContain(USER);
});
