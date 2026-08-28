// Contract: a flat Capital.com tick the actionability gate stops before
// prompt assembly — the captured EURUSD market pressed into a near, unbroken
// weekly wall (into_context_wall; a different gate reason than bitget's
// boxed_or_unconfirmed scenario). The conversation shows the whole capital
// preamble: session handshake, epic probe, tradeability read, position/
// account/working-order state, the full price ladder and the session-levels
// candles — and then no news and no AI (neither handler is registered, so an
// unexpected call would fail via error-on-unhandled).

import { expect, test } from 'vitest';

import eurusdFixtureJson from '../fixtures/capital-EURUSD.json';
import { analyzePg, capitalFlatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { capitalMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = eurusdFixtureJson as RecordedMarketFixture;

startBoundary(
    () => ({
        http: [
            ...capitalMarketWorld(fixture),
            ...capitalFlatPrivateWorld(),
            ...kvWorld(),
            forexFactoryCalendar([]),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('EURUSD pressing into a weekly wall: the gate skips prompt, news and AI', async () => {
    const out = await runAnalyzeTick({
        symbol: 'EURUSD',
        platform: 'capital',
        category: 'forex',
        dryRun: 'true',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision).toMatchObject({
        action: 'HOLD',
        summary: 'not_actionable',
        reason: 'flat_skip_not_actionable_into_context_wall',
    });

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
    expect(summary.some((line) => line.includes('api.marketaux.com'))).toBe(false);
    expect(summary[0]).toContain('/api/v1/session');

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/capital-flat-gated.txt');
    // Longer timeout: the capital rate limiter serializes ~15 calls against a
    // frozen Date.now(), so each successive call waits one interval more.
}, 30_000);
