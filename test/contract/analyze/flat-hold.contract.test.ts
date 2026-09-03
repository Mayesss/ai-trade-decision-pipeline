// Contract: a full flat analyze tick that PASSES the actionability gate and
// asks the model — captured BTCUSDT market data (actionable at capture time),
// canned HOLD answer. The snapshot is the tick's complete outgoing
// conversation: every market read, the KV cache traffic, the full prompt on
// the AI call, and the dryRun persistence trail (decision row, tick log).

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

const HOLD = decisionBase('HOLD', 'boxed between 4H levels', 'no directional edge at current price');

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([
                { title: 'Bitcoin consolidates below resistance', sentiment: 'NEUTRAL' },
                { title: 'ETF inflows steady', sentiment: 'POSITIVE' },
            ]),
            forexFactoryCalendar([
                {
                    title: 'Non-Farm Employment Change',
                    country: 'USD',
                    date: new Date(fixture.capturedAtMs + 26 * 3600_000).toISOString(),
                    impact: 'High',
                    forecast: '185K',
                    previous: '187K',
                },
            ]),
            responsesDecides(HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('flat actionable tick: full prompt to the model, HOLD answer, dryRun persistence', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');
    expect(body.decision.response_id).toBe('resp_test-1');
    expect(body.execRes.placed).toBe(false);
    expect(body.dryRun).toBe(true);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/flat-hold.txt');
});
