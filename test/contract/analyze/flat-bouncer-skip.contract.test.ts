// Contract: the ai-bouncer closes the door on a flat tick that passed every
// hard gate. The baseline world deliberately registers NO main-model handler
// and NO news handler: a bouncer skip must stop the tick BEFORE the pre-prompt
// bundle, so any call to either fails the test via msw error-on-unhandled.
// The bouncer handler itself is added per test (startBoundary().use).

import { expect, test, vi } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { bouncerDecides } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

const boundary = startBoundary(
    () => ({
        http: [...bitgetMarketWorld(fixture), ...flatPrivateWorld(), ...kvWorld(), forexFactoryCalendar([])],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('bouncer proceed=false: skip persisted as stage ai_bouncer, no prompt, no news', async () => {
    vi.stubEnv('SWING_AI_BOUNCER_ENABLED', 'true');
    boundary.use(bouncerDecides({ proceed: false, confidence: 0.8, reason: 'Mixed biases, low confluence' }));

    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision).toMatchObject({
        action: 'HOLD',
        summary: 'ai_bouncer_skip',
        reason: 'flat_skip_ai_bouncer_mixed_biases_low_confluence',
    });
    expect(body.execRes.reason).toBe('ai_bouncer_skip');

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/flat-bouncer-skip.txt');
});
