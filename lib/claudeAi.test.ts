import assert from 'node:assert/strict';
import test from 'node:test';

import { toClaudeSchema, truncateClaudeTranscript } from './claudeAi';
import { SWING_DECISION_SCHEMA } from './ai';

function pair(i: number): any[] {
    return [
        { role: 'user', content: [{ type: 'text', text: `tick ${i}` }] },
        { role: 'assistant', content: [{ type: 'thinking', thinking: `t${i}` }, { type: 'text', text: `{"n":${i}}` }] },
    ];
}

test('toClaudeSchema strips numeric bounds recursively, keeps structure', () => {
    const stripped: any = toClaudeSchema(SWING_DECISION_SCHEMA.schema);
    assert.equal(stripped.additionalProperties, false);
    assert.deepEqual(stripped.properties.leverage, { type: ['integer', 'null'] });
    assert.deepEqual(stripped.properties.exit_size_pct, { type: ['number', 'null'] });
    assert.ok(stripped.required.includes('action'));
    assert.deepEqual(stripped.properties.action.enum, ['BUY', 'SELL', 'HOLD', 'CLOSE', 'REVERSE']);
    // the original stays untouched
    assert.equal((SWING_DECISION_SCHEMA.schema.properties as any).leverage.minimum, 5);
});

test('truncateClaudeTranscript: under the cap is returned as-is', () => {
    const transcript = [...pair(1), ...pair(2)];
    assert.equal(truncateClaudeTranscript(transcript as any, 62), transcript);
});

test('truncateClaudeTranscript: keeps entry pair + most recent pairs, alternation intact', () => {
    const transcript: any[] = [];
    for (let i = 0; i < 50; i++) transcript.push(...pair(i)); // 100 messages
    const out = truncateClaudeTranscript(transcript as any, 10);
    assert.equal(out.length, 10);
    // entry pair survives
    assert.equal((out[0] as any).content[0].text, 'tick 0');
    assert.equal(out[1].role, 'assistant');
    // tail is the most recent pairs and starts with a user turn
    assert.equal(out[2].role, 'user');
    assert.equal((out[2] as any).content[0].text, 'tick 46');
    assert.equal((out[out.length - 1] as any).content[1].text, '{"n":49}');
    for (let i = 1; i < out.length; i++) {
        assert.notEqual(out[i].role, out[i - 1].role, `alternation broken at ${i}`);
    }
});
