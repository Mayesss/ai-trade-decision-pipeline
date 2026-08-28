// Contract: a manage tick on an open long with a standing exchange bracket.
// The model answers HOLD with a tightened stop; dryRun reads everything the
// amendment needs (position, contract meta, standing plan orders) but stops
// before the modify call.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, inPositionPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { openAiDecides } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Long from 75,000 (captured mark ~77,543); model tightens the stop to 76,000
// and leaves the TP untouched.
const MANAGE_HOLD = {
    ...decisionBase('HOLD', 'thesis intact, protect gains', 'raise stop under reclaimed level'),
    stop_loss_price: 76000,
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
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('in-position manage tick: HOLD with stop amendment, dryRun stops before modify', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');
    expect(body.execRes.placed).toBe(false);

    const text = await conversation();
    expect(text).toContain('orders-plan-pending');
    expect(text).not.toContain('modify-tpsl-order');
    expect(text).not.toContain('place-tpsl-order');

    await expect(text).toMatchFileSnapshot('./__snapshots__/in-position-manage.txt');
});
