// Contract: SWING_PERPLEXITY_ENABLED on an IN-POSITION manage tick — the tick
// fetches the sonar digest via the gateway's chat/completions endpoint
// (KV-cached) and the FRESH SENTIMENT block lands in the decision prompt's
// USER turn. Since 2026-09-10 this is the ONLY tick kind that fetches it: flat
// scans skip the digest (pinned in flat-hold.contract.test.ts, whose world has
// no perplexity handler). The snapshot captures the full outgoing conversation
// including that prompt — the regression net for the block's wording/placement.
import { expect, test, vi } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, inPositionPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { responsesDecides, perplexityReports } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Long from 75,000 (captured mark ~77,543); the model holds and leaves the
// bracket untouched — the digest, not the management, is under test.
const MANAGE_HOLD = decisionBase('HOLD', 'thesis intact', 'no change to the bracket');
const DIGEST = '- 2h ago: spot ETF inflows steady, no regulatory items. Social mood: bullish.';

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
            perplexityReports(DIGEST),
            responsesDecides(MANAGE_HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('perplexity digest is fetched on an in-position tick and rendered as FRESH SENTIMENT in the user turn', async () => {
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
    await expect(convo).toMatchFileSnapshot('./__snapshots__/in-position-perplexity.txt');
});
