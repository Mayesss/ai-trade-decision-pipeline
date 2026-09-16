// Contract: SWING_PERPLEXITY_ENABLED on a FLAT scan — the tick fetches the
// sonar digest via the gateway's chat/completions endpoint (KV-cached) and the
// FRESH SENTIMENT block lands in the decision prompt's USER turn. Flat scans
// skipped the digest from 2026-09-10 to 2026-09-16 (in-position only, a cost
// cut under the 4H cadence); under the daily cadence a flat entry decided once
// a day gets the same fresh read as a managed position. Same world as
// flat-hold.contract.test.ts plus the perplexity handler; the snapshot is the
// regression net for the block's wording/placement on the flat variant.

import { expect, test, vi } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { responsesDecides, perplexityReports } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

const HOLD = decisionBase('HOLD', 'boxed between 4H levels', 'no directional edge at current price');

const DIGEST = '- 3h ago: spot ETF inflows steady, no regulatory items. Social mood: cautiously bullish.';

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
            perplexityReports(DIGEST),
            responsesDecides(HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('flat scan: perplexity digest is fetched and rendered as FRESH SENTIMENT in the user turn', async () => {
    vi.stubEnv('SWING_PERPLEXITY_ENABLED', 'true');
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });
    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');

    const convo = await conversation();
    expect(convo).toContain('/v1/chat/completions');
    expect(convo).toContain('perplexity/sonar');
    expect(convo).toContain('FRESH SENTIMENT');
    expect(convo).toContain(DIGEST);
    await expect(convo).toMatchFileSnapshot('./__snapshots__/flat-perplexity.txt');
});
