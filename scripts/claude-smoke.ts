// Live smoke test for lib/claudeAi.ts (SWING_AI_PROVIDER=claude path).
// Makes 2 real (cheap) API calls. Run: node --import tsx scripts/claude-smoke.ts
// Verifies: stripped-schema acceptance by structured outputs, JSON validity,
// transcript chaining (thinking-block echo on turn 2), prompt-cache write/read.
import { callClaudeSwingDecision, toClaudeSchema } from '../lib/claudeAi';
import { SWING_DECISION_SCHEMA } from '../lib/ai';

// Padding pushes the system prompt past Opus 4.8's 4096-token minimum
// cacheable prefix so the cache_control breakpoint actually engages.
const PAD = Array.from({ length: 60 })
    .map(
        (_, i) =>
            `Reference note ${i}: swing structure analysis weighs breakouts, retests, regime alignment, location versus major levels, momentum slope, extension in ATR units, liquidity sweeps at session extremes, and cost-versus-expected-move asymmetry before any entry decision is made.`,
    )
    .join('\n');

const SYSTEM = `You are an expert swing-trading market-structure analyst. Decide one action and size it.
${PAD}
OUTPUT: one decision object per the enforced schema. Flat context: allowed actions BUY/SELL/HOLD.`;

const USER_1 = `You are analyzing BTC for swing trading (mode=simulation).
STATE: {"biases":{"micro":"up","primary":"up","macro":"neutral"},"price":50000,"atr_primary":600,"levels":{"resistance":{"price":51200,"dist_atr":2.0},"support":{"price":49700,"dist_atr":0.5}}}
TASKS: output one action (BUY/SELL/HOLD); leverage 5-10 on entries else null; raise_leverage_to/move_stop_to_be null; exit_size_pct null when flat; set take_profit_price and stop_loss_price on entries; entry_limit_price optional pullback limit; summary <=2 lines; reason brief.`;

const USER_2 = `Follow-up tick, same instrument. Price moved to 50250, everything else unchanged. Re-evaluate: does your previous reasoning still hold? Same output schema.`;

const REQUIRED_KEYS = [
    'action',
    'summary',
    'reason',
    'exit_size_pct',
    'leverage',
    'take_profit_price',
    'stop_loss_price',
    'entry_limit_price',
];

async function main() {
    console.log(
        'stripped leverage schema (bounds removed):',
        JSON.stringify((toClaudeSchema(SWING_DECISION_SCHEMA.schema) as any).properties.leverage),
    );

    const first = await callClaudeSwingDecision(SYSTEM, USER_1, SWING_DECISION_SCHEMA);
    console.log('--- call 1 (stateless, schema-enforced) ---');
    console.log('responseId:', `${first.responseId?.slice(0, 8)}…`);
    console.log('decision:', JSON.stringify(first.json));
    const assistantContent = (first.appendTurns[1] as any).content;
    console.log(
        'assistant turn blocks:',
        Array.isArray(assistantContent) ? assistantContent.map((b: any) => b.type).join(',') : typeof assistantContent,
    );
    console.log('usage:', JSON.stringify(first.usage));

    const second = await callClaudeSwingDecision(SYSTEM, USER_2, SWING_DECISION_SCHEMA, {
        transcript: first.appendTurns,
    });
    console.log('--- call 2 (chained transcript incl. thinking echo) ---');
    console.log('responseId:', `${second.responseId?.slice(0, 8)}…`);
    console.log('decision:', JSON.stringify(second.json));
    console.log('usage:', JSON.stringify(second.usage));

    for (const [label, result] of [
        ['call1', first],
        ['call2', second],
    ] as const) {
        const missing = REQUIRED_KEYS.filter((k) => !(k in result.json));
        if (missing.length) throw new Error(`${label}: missing schema keys: ${missing.join(',')}`);
    }
    if (!Number(second.usage.cache_read_input_tokens)) {
        console.warn('WARN: call 2 read 0 cached tokens — cache breakpoint did not engage');
    }
    console.log('OK: schema keys present on both calls');
}

main().catch((err) => {
    console.error('SMOKE FAILED:', err?.message || err);
    process.exit(1);
});
