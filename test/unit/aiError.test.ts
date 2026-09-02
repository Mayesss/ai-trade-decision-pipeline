import assert from 'node:assert/strict';
import { test } from 'vitest';

import { AiCallError, classifyAiFailure, coerceAiCallError } from '../../lib/aiError';

test('OpenAI lapsed subscription (429 insufficient_quota) classifies as billing', () => {
    assert.equal(
        classifyAiFailure({
            status: 429,
            code: 'insufficient_quota',
            message:
                'AI error: 429 Too Many Requests - You exceeded your current quota, please check your plan and billing details.',
        }),
        'billing',
    );
});

test('OpenAI rate limit (429 rate_limit_exceeded) stays transient', () => {
    assert.equal(
        classifyAiFailure({
            status: 429,
            code: 'rate_limit_exceeded',
            message: 'AI error: 429 Too Many Requests - Rate limit reached for gpt-5.4.',
        }),
        'transient',
    );
});

test('Anthropic out-of-credit 400 classifies as billing via message', () => {
    assert.equal(
        classifyAiFailure({
            status: 400,
            code: 'invalid_request_error',
            message: 'Claude AI error: 400 - Your credit balance is too low to access the Anthropic API.',
        }),
        'billing',
    );
});

test('401 classifies as config', () => {
    assert.equal(
        classifyAiFailure({ status: 401, code: null, message: 'AI error: 401 Unauthorized' }),
        'config',
    );
});

test('5xx and network errors stay transient', () => {
    assert.equal(
        classifyAiFailure({ status: 500, code: 'server_error', message: 'AI error: 500 Internal Server Error' }),
        'transient',
    );
    assert.equal(classifyAiFailure({ status: null, code: null, message: 'fetch failed' }), 'transient');
});

test('AiCallError self-classifies from status/code when kind omitted', () => {
    const err = new AiCallError({
        message: 'AI error: 429 - You exceeded your current quota',
        dialect: 'responses',
        status: 429,
        code: 'insufficient_quota',
    });
    assert.equal(err.kind, 'billing');
    assert.equal(err.dialect, 'responses');
});

test('coerceAiCallError wraps plain errors as transient and passes typed ones through', () => {
    const wrapped = coerceAiCallError(new TypeError('fetch failed'), 'responses');
    assert.ok(wrapped instanceof AiCallError);
    assert.equal(wrapped.kind, 'transient');

    const typed = new AiCallError({ message: 'Missing OPENAI_API_KEY', dialect: 'responses', kind: 'config' });
    assert.equal(coerceAiCallError(typed, 'responses'), typed);
});
