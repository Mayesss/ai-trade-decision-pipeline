// Contract: the ROUND TRIP of transcript compaction on a live manage tick.
//
// The other tests cover the halves — computeSwingState builds an abbreviated
// turn (unit), the clients archive it (unit + aiGateway contract). What was
// untested is the loop that actually runs in production: a turn compacted on an
// EARLIER tick is stored, read back from swing.ai_threads, replayed into the
// next call, and that call's own turn is compacted in turn. The seeded
// transcript below is therefore a REAL userCompact payload, not a stub — this
// is the test that would catch compaction that survives one tick but corrupts
// the thread on the next.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePgWith, decisionBase, inPositionPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { openAiDecides } from '../../harness/worlds/aiGateway';
import { bitgetPost } from '../../harness/worlds/bitget';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

const MANAGE_HOLD = {
    ...decisionBase('HOLD', 'thesis intact', 'prior abbreviated context still reads long'),
    stop_loss_price: 76000,
};

// A genuine abbreviated turn as produced by computeSwingState on the ENTRY
// tick of this position: the marker line, the trimmed STATE, and the trigger
// block that explains why that look happened.
const PRIOR_COMPACT_TURN = [
    '[ABBREVIATED EARLIER TURN — the full STATE/MARKET tape from this evaluation has been dropped to keep this conversation small. These are the readings the decision below rested on.]',
    'STATE: {"time":{"iso_utc":"2026-08-26T18:00:00.000Z"},"biases":{"micro":"UP","micro_source":"structure_breakout_retest","primary":"UP","macro":"UP","context":"UP"},"trend":{"primary_up":true,"primary_down":false,"primary_breakout_confirmed":true,"primary_breakdown_confirmed":false,"macro_up":true,"macro_down":false},"structure":{"micro":{"state":"bull","break_state":"above","bos":true,"bos_dir":"up","choch":false,"breakout_retest_ok":true,"breakout_retest_dir":"up"},"primary":{"state":"bull","break_state":"above","bos":true,"bos_dir":"up","choch":false,"breakout_retest_ok":true,"breakout_retest_dir":"up"}},"momentum":{"rsi":{"micro":58.1,"primary":61.2,"macro":66.4},"micro_entry_ok":true},"extension_atr":{"micro":0.4,"primary":0.7},"volatility":{"atr_pct":{"primary":1.4,"macro":3.8}},"location":{"context_support_dist_atr":2.1,"context_resistance_dist_atr":1.9,"context_breakout_confirmed":false,"context_breakdown_confirmed":false,"chop_risk":false},"levels":{"primary":{"support":{"price":74500,"dist_atr":0.9,"strength":0.81,"type":"swing_pivot","state":"retesting"},"resistance":{"price":81000,"dist_atr":2.2,"strength":0.77,"type":"swing_pivot","state":"approaching"}},"context":{"support":{"price":70000,"dist_atr":3.4,"strength":0.94,"type":"swing_pivot","state":"rejected"},"resistance":{"price":84000,"dist_atr":2.8,"strength":0.99,"type":"swing_pivot","state":"approaching"}}},"position":{"open":false,"status":"none","reentry_cooldown":null}}',
    'MARKET: {"price":{"last":75000,"change_24h_pct":1.2},"cooldown_wake":{"crossed":"above","level":74800,"set_minutes_ago":42,"sustained_minutes":10,"note":"reclaim of 74.8k → long entry"}}',
].join('\n');

const THREAD_ROW = {
    status: 'in_position',
    last_response_id: 'resp_prev-9',
    turns: 3,
    provider: 'openai',
    transcript: [
        { role: 'user', content: PRIOR_COMPACT_TURN },
        {
            role: 'assistant',
            content: '{"action":"BUY","summary":"reclaim confirmed","reason":"long from 74.8k reclaim, stop under the sweep"}',
        },
    ],
    wake_above: null,
    wake_below: null,
    wake_note: null,
    wake_set_at_ms: null,
};

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...inPositionPrivateWorld({
                symbol: 'BTCUSDT',
                entryPrice: '75000',
                markPrice: '77543.7',
                openedAtMs: fixture.capturedAtMs - 2 * 24 * 3600_000,
                takeProfit: '84000',
                stopLoss: '74500',
            }),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin holds higher low', sentiment: 'NEUTRAL' }]),
            forexFactoryCalendar([]),
            openAiDecides(MANAGE_HOLD),
            bitgetPost('/api/v2/mix/order/modify-tpsl-order', { orderId: 'plan-sl-1' }),
        ],
        db: analyzePgWith((text) => (text.includes('FROM swing.ai_threads') ? [THREAD_ROW] : undefined)),
    }),
    { nowMs: fixture.capturedAtMs },
);

test('an abbreviated turn round-trips: replayed intact, and this tick archives its own', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    expect((out.body as Record<string, any>).decision.action).toBe('HOLD');

    const text = await conversation();

    // 1. The stored abbreviated turn replays VERBATIM — the entry thesis and the
    //    trigger that produced it both survive the round trip.
    expect(text).toContain('ABBREVIATED EARLIER TURN');
    expect(text).toContain('reclaim of 74.8k');
    expect(text).toContain('long from 74.8k reclaim, stop under the sweep');

    // 2. THIS tick still sends the model a complete turn (compaction must never
    //    starve the live decision of current data).
    expect(text).toContain('STATE (derived signals');
    expect(text).toContain('recent_candles');
    expect(text).toContain('geometry');

    // 3. …and the turn it writes back to the thread is abbreviated, so the
    //    thread does not regrow. The persisted transcript carries BOTH the
    //    replayed prior turn and this tick's freshly abbreviated one, and
    //    neither drags the tape along.
    //    (bounded to THIS statement's params — the decisions INSERT that follows
    //    stores the full prompt on purpose and would otherwise match).
    const threadStart = text.indexOf('INSERT INTO swing.ai_threads');
    const nextStatement = text.indexOf('"text":', threadStart + 1);
    const threadWrite = text.slice(threadStart, nextStatement > 0 ? nextStatement : undefined);
    const abbreviatedInWrite = threadWrite.split('ABBREVIATED EARLIER TURN').length - 1;
    expect(abbreviatedInWrite).toBe(2);
    expect(threadWrite).not.toContain('volume_profile');
    expect(threadWrite).not.toContain('recent_candles');

    await expect(text).toMatchFileSnapshot('./__snapshots__/live-transcript-compaction.txt');
});
