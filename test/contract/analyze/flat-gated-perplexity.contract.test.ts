// Contract: SWING_PERPLEXITY_ENABLED must not change what a SKIPPED tick talks
// to — the digest fetch lives in the post-gates bundle, so a tick stopped by a
// pre-AI gate never pays for it. The world registers no AI, news or perplexity
// handler: any leak fails via msw error-on-unhandled.
//
// The gate used here is the cron-only primary-close gate (the fixture is 18:24
// UTC, a quarter tick off the 4H close), NOT the actionability gate. Until
// 2026-09-03 this fixture was `boxed_or_unconfirmed` and the actionability gate
// supplied the skip; the anchor door now admits that market (see
// flat-anchor-door.contract.test.ts) and no captured fixture is still water, so
// the invariant is pinned on a gate that still fires. What is under test is the
// ORDERING — bundle after gates — which is gate-agnostic.

import { expect, test, vi } from 'vitest';

import ethFixtureJson from '../fixtures/bitget-ETHUSDT.json';
import { analyzePg, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversationSummary, startBoundary } from '../../harness';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = ethFixtureJson as RecordedMarketFixture;

startBoundary(
    () => ({
        http: [...bitgetMarketWorld(fixture), ...flatPrivateWorld(), ...kvWorld(), forexFactoryCalendar([])],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('gated tick with the flag on: no perplexity (or any gateway) request goes out', async () => {
    vi.stubEnv('SWING_PERPLEXITY_ENABLED', 'true');

    const out = await runAnalyzeTick(
        { symbol: 'ETHUSDT', platform: 'bitget', dryRun: 'true' },
        { 'x-vercel-cron': '1' },
    );

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.summary).toBe('not_primary_close');

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
});
