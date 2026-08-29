// Contract: SWING_PERPLEXITY_ENABLED must not change what a GATED tick talks
// to — the digest fetch lives in the post-gates bundle, so a tick the
// actionability gate stops never pays for it. Same world as flat-gated (no AI,
// no news, no perplexity handler): any leak fails via msw error-on-unhandled.

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

    const out = await runAnalyzeTick({ symbol: 'ETHUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    expect((out.body as Record<string, any>).promptSkipped).toBe(true);

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
});
