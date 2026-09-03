// Contract: what the Responses client does when the model answers on the
// WRONG CHANNEL. On 2026-09-03 zai/glm-5.3 replaced an OpenAI id as
// DEFAULT_AI_MODEL and text.format json_schema stopped being enforced —
// nothing upstream honors it — so the model wrote its thinking into the
// message item and every such tick threw with no decision written. The client
// now forces a single tool call, and these are the recovery paths around it.
//
// Deliberately one file: the reshape handlers count their calls, and
// startBoundary rebuilds the world per test so each test gets a fresh counter.

import { expect, test, vi } from 'vitest';

import { AiCallError } from '../../lib/aiError';
import { callSwingDecision } from '../../lib/aiProvider';
import { conversation, startBoundary } from '../harness';
import { responsesAnswersInProse, responsesTruncates } from '../harness/worlds/aiGateway';
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

// The real thing, trimmed: reasoning that starts mid-sentence because the
// front of it went into the reasoning item.
const LEAKED_THINKING =
    ' primary bias is down. Room below ~1.1+ ATR to range low, then more. Stop above extreme 25933.05 → short entry at 25925. Hmm, modest. Wait, but is this the right read? Let me reconsider:';

const boundary = startBoundary(() => ({ http: [...kvWorld()] }));

test('prose on the first attempt: the nudged retry recovers the decision', async () => {
    vi.stubEnv('SWING_AI_PROVIDER', 'openai');
    boundary.use(responsesAnswersInProse(LEAKED_THINKING, { json: DECISION }));

    const result = await callSwingDecision({
        system: SYSTEM,
        user: USER,
        schema: SCHEMA,
    });

    expect(result.json).toEqual(DECISION);
    // The archived assistant turn is the ARGUMENTS json as plain text, so the
    // replay filter accepts it on the next tick of this thread.
    expect(result.appendTurns?.[1]).toEqual({ role: 'assistant', content: JSON.stringify(DECISION) });

    // Two outgoing calls, and the nudge rides at the TAIL of the second so the
    // cached system+transcript prefix is still byte-identical.
    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/ai-reshape-retry.txt');
});

test('prose on both attempts: throws a diagnosable transient failure', async () => {
    vi.stubEnv('SWING_AI_PROVIDER', 'openai');
    boundary.use(responsesAnswersInProse(LEAKED_THINKING));

    const err = await callSwingDecision({ system: SYSTEM, user: USER, schema: SCHEMA }).catch((e: unknown) => e);

    expect(err).toBeInstanceOf(AiCallError);
    const aiErr = err as AiCallError;
    // Transient, so the health flag latches on a STREAK rather than on one
    // stubborn tick (lib/swing/aiHealth.ts).
    expect(aiErr.kind).toBe('transient');
    expect(aiErr.dialect).toBe('responses');
    // Diagnosis first, model prose last: aiHealth keeps only 300 chars of this
    // and the prose used to eat all of it.
    expect(aiErr.message).toMatch(
        /^AI returned no usable JSON after 2 attempts \(status=completed answer_channel=message out_tokens=200\)/,
    );
    expect(aiErr.message).toContain('primary bias is down');
});

test('a reply cut off by the output ceiling is refused even though it parses', async () => {
    vi.stubEnv('SWING_AI_PROVIDER', 'openai');
    // Valid JSON, so only the status check can catch it.
    boundary.use(responsesTruncates(JSON.stringify(DECISION)));

    const err = await callSwingDecision({ system: SYSTEM, user: USER, schema: SCHEMA }).catch((e: unknown) => e);

    expect(err).toBeInstanceOf(AiCallError);
    expect((err as AiCallError).message).toContain('incomplete=max_output_tokens');
});
