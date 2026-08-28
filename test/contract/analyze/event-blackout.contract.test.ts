// Contract: the forex event blackout gate. A HIGH-impact USD event inside the
// pre-event block window (default 30min before / 15min after) puts the
// EURUSD forex event context at status=active, and a FLAT tick exits before
// prompt assembly. The conversation shows the ForexFactory refresh + KV
// snapshot bookkeeping, then no news and no AI.

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
            forexFactoryCalendar([
                {
                    // 10 minutes ahead — inside the 30-minute pre-event block.
                    title: 'FOMC Press Conference',
                    country: 'USD',
                    date: new Date(fixture.capturedAtMs + 10 * 60_000).toISOString(),
                    impact: 'High',
                    forecast: '',
                    previous: '',
                },
            ]),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('imminent HIGH event: flat tick exits in blackout before prompt, news and AI', async () => {
    const out = await runAnalyzeTick({
        symbol: 'EURUSD',
        platform: 'capital',
        category: 'forex',
        dryRun: 'true',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.reason).toContain('event_blackout');
    expect(body.forexEventContext).toMatchObject({ status: 'active' });

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
    expect(summary.some((line) => line.includes('api.marketaux.com'))).toBe(false);
    expect(summary.some((line) => line.includes('nfs.faireconomy.media'))).toBe(true);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/event-blackout.txt');
}, 30_000);
