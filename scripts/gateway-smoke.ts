// Live smoke test for the OpenAI path (lib/ai.ts callAIThread) through the
// Vercel AI Gateway. Makes 2 small real calls. Verifies: structured outputs
// (json_schema strict), JSON validity, and Responses-API thread chaining
// (previous_response_id) surviving the gateway hop.
// Run: node scripts/with-db-env.mjs node --import tsx scripts/gateway-smoke.ts
import { callAIThread } from '../lib/ai';

const SCHEMA = {
    name: 'smoke_decision',
    schema: {
        type: 'object',
        additionalProperties: false,
        required: ['action', 'reason'],
        properties: {
            action: { type: 'string', enum: ['BUY', 'SELL', 'HOLD'] },
            reason: { type: 'string' },
        },
    } as Record<string, unknown>,
};

const SYSTEM = 'You are a terse trading assistant. Answer only per the schema.';

async function main() {
    const turn1 = await callAIThread(
        SYSTEM,
        'BTC at 50000, support 49700, resistance 51200, mild uptrend. One-word-ish reason.',
        SCHEMA,
    );
    console.log('turn1:', JSON.stringify({ json: turn1.json, responseId: turn1.responseId, model: turn1.model, usage: turn1.usage }));
    if (!turn1.responseId) throw new Error('no responseId returned — store:true not honored?');

    const turn2 = await callAIThread(
        SYSTEM,
        'Same instrument, price now 50300. In your reason, restate the resistance level you were given last turn.',
        SCHEMA,
        { previousResponseId: turn1.responseId },
    );
    console.log('turn2:', JSON.stringify({ json: turn2.json, responseId: turn2.responseId, model: turn2.model }));
    const remembered = /51[,.]?200/.test(String(turn2.json?.reason || ''));
    console.log(`thread memory check (mentions 51200): ${remembered ? 'PASS' : 'FAIL'}`);
    if (!remembered) process.exitCode = 1;
}

main().catch((err) => {
    console.error('gateway-smoke failed:', err?.message || err);
    process.exitCode = 1;
});
