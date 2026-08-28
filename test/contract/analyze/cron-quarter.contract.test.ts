// Contract: a production-shaped cron tick — the x-vercel-cron header plus the
// exact query the vercel.json crons send (dryRun=false, decisionPolicy=
// balanced). The fixture timestamp (18:24 UTC) makes this a QUARTER tick off
// a 4H close, so the cron-only primary-close gate ends it (not_primary_close)
// — a gate no manual tick can ever hit. Also under test: the cron-control
// kill-switch read, the last-scan freshness marker, and the warm-latch INCR
// in the finally block (this tick is finisher 1 of 29, so the big summary
// warm must NOT run).

import { expect, test } from 'vitest';

import ethFixtureJson from '../fixtures/bitget-ETHUSDT.json';
import { analyzePg, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = ethFixtureJson as RecordedMarketFixture;

startBoundary(
    () => ({
        http: [...bitgetMarketWorld(fixture), ...flatPrivateWorld(), ...kvWorld(), forexFactoryCalendar([])],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('quarter cron tick: kill-switch read, last-scan marker, gated skip, warm latch', async () => {
    const out = await runAnalyzeTick(
        {
            symbol: 'ETHUSDT',
            platform: 'bitget',
            newsSource: 'coindesk',
            category: 'crypto',
            dryRun: 'false',
            decisionPolicy: 'balanced',
        },
        { 'x-vercel-cron': '1' },
    );

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.summary).toBe('not_primary_close');

    const summary = await conversationSummary();
    const kvLines = summary.filter((line) => line.includes('kv.boundary.test'));
    // Cadence bookkeeping that only exists on cron requests:
    expect((await conversation()).includes('swing:warm:latch:')).toBe(true);
    expect(kvLines.length).toBeGreaterThan(0);
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
    // Finisher 1 of 29 — the summary warm must not have started (it would show
    // up as a burst of extra KV/pg reads after the latch INCR).
    expect((await conversation()).includes('swing:warm:done:')).toBe(false);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/cron-quarter.txt');
});
