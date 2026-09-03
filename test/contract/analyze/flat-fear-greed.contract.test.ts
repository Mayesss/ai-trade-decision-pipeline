// Contract: SWING_FEAR_GREED_ENABLED on a flat crypto tick that passes the
// gates — the tick fetches the alternative.me index (KV-cached, market-wide)
// and market.fear_greed lands in the decision prompt's MARKET JSON. The
// snapshot captures the full outgoing conversation including that prompt —
// the regression net for the field's shape/placement.

import { expect, test, vi } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { fearGreedIndex } from '../../harness/worlds/fearGreed';
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
            coindeskNews([{ title: 'Bitcoin consolidates below resistance', sentiment: 'NEUTRAL' }]),
            forexFactoryCalendar([]),
            // Newest first, like the real payload: today 72 after a week-long
            // grind up from fear — the reversed oldest-first series is what
            // the prompt must carry.
            fearGreedIndex(
                [72, 65, 63, 60, 61, 58, 55, 48, 44, 41].map((value, i) => ({
                    value,
                    classification: value >= 55 ? 'Greed' : value >= 47 ? 'Neutral' : 'Fear',
                    timestampMs: fixture.capturedAtMs - i * 86_400_000,
                })),
            ),
            responsesDecides(HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('fear/greed index is fetched post-gates and rendered as market.fear_greed', async () => {
    vi.stubEnv('SWING_FEAR_GREED_ENABLED', 'true');

    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');

    const convo = await conversation();
    // The index request went to alternative.me…
    expect(convo).toContain('api.alternative.me/fng/');
    // …and the decision prompt carries the rendered block, history reversed
    // into oldest-first (the prompt sits inside the recorded request's JSON
    // body, so its quotes arrive backslash-escaped).
    expect(convo).toContain(
        '\\"fear_greed\\":{\\"value\\":72,\\"label\\":\\"Greed\\",\\"updated_utc\\":\\"2026-08-28\\",\\"daily_oldest_first\\":[41,44,48,55,58,61,60,63,65,72]}',
    );

    await expect(convo).toMatchFileSnapshot('./__snapshots__/flat-fear-greed.txt');
});
