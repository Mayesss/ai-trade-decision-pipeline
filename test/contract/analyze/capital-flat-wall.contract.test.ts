// Contract: a flat Capital.com tick whose market presses into a near, unbroken
// weekly wall now REACHES the model. This case used to be stopped before
// prompt assembly (actionability reason into_context_wall) on the measured
// grounds that the AI always HOLDed it — which was true only while a market
// entry was the sole way in. A wall is also the best-defined bounce location on
// the chart, and resting entries can now trade one, so the wall is handed over
// as data (location.context_*_dist_atr) instead of adjudicated in code.
//
// The branch is still named in the actionability reason so the decision trail
// stays queryable, and the ai-bouncer still runs after this point and can
// decline the expensive call on its own judgment.
//
// The snapshot carries the whole capital preamble — session handshake, epic
// probe, tradeability read, position/account/working-order state, the price
// ladder and the session-levels candles — and then the prompt itself.

import { expect, test } from 'vitest';

import eurusdFixtureJson from '../fixtures/capital-EURUSD.json';
import { analyzePg, capitalFlatPrivateWorld, decisionBase, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { openAiDecides } from '../../harness/worlds/aiGateway';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { marketauxNews } from '../../harness/worlds/news';
import { capitalMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = eurusdFixtureJson as RecordedMarketFixture;

const HOLD = decisionBase('HOLD', 'at the weekly wall', 'waiting for the level to resolve');

startBoundary(
    () => ({
        http: [
            ...capitalMarketWorld(fixture),
            ...capitalFlatPrivateWorld(),
            ...kvWorld(),
            marketauxNews([{ title: 'Euro steady ahead of ECB' }]),
            forexFactoryCalendar([]),
            openAiDecides(HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('EURUSD pressing into a weekly wall reaches the model instead of being gated out', async () => {
    const out = await runAnalyzeTick({
        symbol: 'EURUSD',
        platform: 'capital',
        category: 'forex',
        dryRun: 'true',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    // The gate no longer short-circuits this market.
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');
    expect(body.decision.reason).not.toContain('flat_skip_not_actionable');

    // The AI was actually consulted, and the capital preamble still ran first.
    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(true);
    expect(summary[0]).toContain('/api/v1/session');

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/capital-flat-wall.txt');
    // Longer timeout: the capital rate limiter serializes ~15 calls against a
    // frozen Date.now(), so each successive call waits one interval more.
}, 30_000);
