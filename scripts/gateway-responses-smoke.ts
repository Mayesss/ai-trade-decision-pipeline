// Live smoke test for the Responses dialect (lib/gatewayResponses.ts
// callResponsesDecision) through the Vercel AI Gateway. Makes 2 small real
// calls. Verifies: JSON validity and client-side transcript chaining (the
// endpoint is stateless, so memory rides on the replayed transcript).
//
// It does NOT tell you WHICH channel answered — a forced tool call and a
// prose reply rescued by the reshape retry both look like success here. The
// client logs [ai_answer_channel] / [ai_reshape_retry] when either happens,
// so watch stderr: a clean run prints neither. Shape enforcement is NOT
// json_schema (zai has no strict mode — see the client header); it is a
// forced tool call.
// Run: node scripts/with-db-env.mjs node --import tsx scripts/gateway-responses-smoke.ts
import { callResponsesDecision } from '../lib/gatewayResponses';

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
    const turn1 = await callResponsesDecision(
        SYSTEM,
        'BTC at 50000, support 49700, resistance 51200, mild uptrend. One-word-ish reason.',
        SCHEMA,
    );
    console.log('turn1:', JSON.stringify({ json: turn1.json, responseId: turn1.responseId, model: turn1.model, usage: turn1.usage }));
    if (!turn1.responseId) throw new Error('no responseId returned');
    if (!turn1.appendTurns?.length) throw new Error('no appendTurns returned — transcript chaining broken');

    const turn2 = await callResponsesDecision(
        SYSTEM,
        'Same instrument, price now 50300. In your reason, restate the resistance level you were given last turn.',
        SCHEMA,
        { transcript: turn1.appendTurns },
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
