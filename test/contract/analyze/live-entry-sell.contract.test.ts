// Contract: a LIVE (dryRun=false) flat tick where the model answers SELL —
// the execution conversation the dryRun scenarios stop short of. This is the
// most trade-critical contract in the repo: the snapshot pins the exact
// set-margin-mode / set-leverage / place-order request bodies (side, size,
// clientOid, preset TP/SL quantization) plus the live-only persistence — the
// thread upsert and the break-trigger bookkeeping. Order endpoints are canned;
// "live" here means the code path, never the network.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { bitgetPost } from '../../harness/worlds/bitget';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Captured lastPr was ~77,543 — short with the stop above, the target below,
// and a failed-break trigger on the level whose loss justifies the trade.
const SELL = {
    ...decisionBase('SELL', 'breakdown continuation short', 'confirmed primary breakdown with room below'),
    leverage: 5,
    stop_loss_price: 80500,
    take_profit_price: 72000,
    entry_trigger_price: 77300,
};

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin loses range support', sentiment: 'NEGATIVE' }]),
            forexFactoryCalendar([]),
            responsesDecides(SELL),
            bitgetPost('/api/v2/mix/account/set-margin-mode', {}),
            bitgetPost('/api/v2/mix/account/set-leverage', { symbol: 'BTCUSDT', leverage: '5' }),
            bitgetPost('/api/v2/mix/order/place-order', { orderId: 'order-live-1', clientOid: 'echo' }),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('live SELL: leverage setup, order body with preset bracket, thread + trigger persistence', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('SELL');
    expect(body.dryRun).toBe(false);
    expect(body.execRes.placed).toBe(true);
    expect(body.execRes.orderId).toBe('order-live-1');
    expect(body.execRes.clientOid).toMatch(/^cfw-/);

    const summary = await conversationSummary();
    expect(summary.filter((line) => line.includes('place-order'))).toHaveLength(1);
    expect(summary.filter((line) => line.includes('set-leverage'))).toHaveLength(1);
    expect(summary.filter((line) => line.includes('set-margin-mode'))).toHaveLength(1);

    const text = await conversation();
    expect(text).toContain('INSERT INTO swing.ai_threads');

    await expect(text).toMatchFileSnapshot('./__snapshots__/live-entry-sell.txt');
});
