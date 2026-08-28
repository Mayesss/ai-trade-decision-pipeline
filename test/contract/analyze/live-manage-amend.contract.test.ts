// Contract: a LIVE (dryRun=false) manage tick on an open long with an
// existing AI thread. Live-only behavior under test: the stored transcript is
// read from swing.ai_threads and REPLAYED in the AI call's input (the
// conversation-memory contract), the stop amendment goes out as a
// modify-tpsl-order body (existing plan order + current position size), and
// the thread row advances with the appended turns.

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
    ...decisionBase('HOLD', 'thesis intact, protect gains', 'raise stop under reclaimed level'),
    stop_loss_price: 76000,
};

// The conversation this position's thread carries from its previous ticks —
// must reappear verbatim in the outgoing AI request.
const THREAD_ROW = {
    status: 'in_position',
    last_response_id: 'resp_prev-7',
    turns: 3,
    provider: 'openai',
    transcript: [
        { role: 'user', content: 'STATE: entry tick state (prior)' },
        { role: 'assistant', content: '{"action":"BUY","summary":"prior entry thesis"}' },
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

test('live manage: transcript replay, modify-tpsl-order body, thread advance', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('HOLD');
    expect(body.decision.previous_response_id).toBe('resp_prev-7');
    expect(body.execRes.tpsl?.stopLoss).toMatchObject({ applied: true, mode: 'modify' });

    const text = await conversation();
    // Transcript replay: the prior turns ride along in the new AI request.
    expect(text).toContain('STATE: entry tick state (prior)');
    expect(text).toContain('modify-tpsl-order');

    await expect(text).toMatchFileSnapshot('./__snapshots__/live-manage-amend.txt');
});
