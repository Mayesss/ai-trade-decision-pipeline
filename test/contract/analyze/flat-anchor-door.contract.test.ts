// Contract: the ANCHOR door (evaluateActionability door (c), added 2026-09-03).
//
// This captured ETHUSDT market was `boxed_or_unconfirmed` under the old two-door
// gate and never reached the model. Look at what it actually is: price 2431.6
// sitting ON a strong primary swing low (2431.19, dist 0.002 ATR, strength
// 0.907, state at_level), at the floor of the primary regression channel
// (channel_pos 0), micro RSI 30.4, macro bias UP. A level bounce with defined
// invalidation — refused a call only because the opposite level was 0.287 ATR
// away and the bounce door demands ACTIONABILITY_ROOM_ATR = 1.5 of room.
//
// That is the misclassification the anchor door exists to fix: "no room to run"
// is not "nothing to trade", it is a symmetric range. The tick now reaches the
// model, which decides for itself — here, HOLD.
//
// Companion unit coverage for the branch logic (including the still-water skip
// that remains the gate's ONLY rejection): test/unit/swing/signals.actionability.

import { expect, test } from 'vitest';

import ethFixtureJson from '../fixtures/bitget-ETHUSDT.json';
import { analyzePg, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = ethFixtureJson as RecordedMarketFixture;

const HOLD = decisionBase('HOLD', 'at 4H swing low, range too tight to pay', 'waiting for the range to resolve');

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([]),
            forexFactoryCalendar([]),
            responsesDecides(HOLD),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('symmetric range at a primary level reaches the model instead of being skipped', async () => {
    const out = await runAnalyzeTick({ symbol: 'ETHUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    // The assertion that carries the change: this used to be
    // promptSkipped=true with reason flat_skip_not_actionable_boxed_or_unconfirmed.
    expect(body.promptSkipped).toBeUndefined();
    expect(body.decision.action).toBe('HOLD');
    expect(body.decision.response_id).toBe('resp_test-1');
    expect(body.dryRun).toBe(true);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/flat-anchor-door.txt');
});
