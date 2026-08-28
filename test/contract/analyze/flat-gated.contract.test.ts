// Contract: a flat tick the actionability gate stops BEFORE prompt assembly —
// the captured ETHUSDT market was boxed_or_unconfirmed. The world deliberately
// registers NO AI handler and NO news handler: if the gated path ever called
// either, msw's error-on-unhandled would fail the test by itself.

import { expect, test } from 'vitest';

import ethFixtureJson from '../fixtures/bitget-ETHUSDT.json';
import { analyzePg, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
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

test('boxed market: the gate skips prompt, news and AI entirely', async () => {
    const out = await runAnalyzeTick({ symbol: 'ETHUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision).toMatchObject({
        action: 'HOLD',
        summary: 'not_actionable',
        reason: 'flat_skip_not_actionable_boxed_or_unconfirmed',
    });

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
    expect(summary.some((line) => line.includes('data-api.coindesk.com'))).toBe(false);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/flat-gated.txt');
});
