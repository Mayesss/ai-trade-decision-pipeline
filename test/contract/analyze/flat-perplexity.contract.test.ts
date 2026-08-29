// Contract: SWING_PERPLEXITY_ENABLED on a flat tick that passes the gates —
// the tick fetches the sonar digest via the gateway's chat/completions
// endpoint (KV-cached) and the FRESH SENTIMENT block lands in the decision
// prompt's USER turn. The snapshot captures the full outgoing conversation
// including that prompt — the regression net for the block's wording/placement.

import { expect, test, vi } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { openAiDecides, perplexityReports } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

const HOLD = decisionBase('HOLD', 'boxed between 4H levels', 'no directional edge at current price');

const DIGEST = '- 2h ago: spot ETF inflows steady, no regulatory items. Social mood: bullish.';

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin consolidates below resistance', sentiment: 'NEUTRAL' }]),
            forexFactoryCalendar([]),
            perplexityReports(DIGEST),
            openAiDecides(HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('perplexity digest is fetched post-gates and rendered as FRESH SENTIMENT in the user turn', async () => {
    vi.stubEnv('SWING_PERPLEXITY_ENABLED', 'true');

    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');

    const convo = await conversation();
    // The digest request went to the gateway's chat/completions with sonar…
    expect(convo).toContain('/v1/chat/completions');
    expect(convo).toContain('perplexity/sonar');
    // …and the decision prompt carries the rendered block.
    expect(convo).toContain('FRESH SENTIMENT');
    expect(convo).toContain(DIGEST);

    await expect(convo).toMatchFileSnapshot('./__snapshots__/flat-perplexity.txt');
});
