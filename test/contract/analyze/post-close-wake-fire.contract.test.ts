// Contract: the wake-watcher's position_closed fire (postCloseReconcile=1)
// is code-only even when the thread is already gone — the executing CLOSE
// tick ends the thread itself, and the watcher's same-minute read can still
// race it and fire. Without this the fire reached the model whenever the
// close landed inside the 4H boundary tolerance window (ETHUSDT 18:01,
// NATURALGAS 10:01 on 2026-09-10).

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

test('position_closed wake fire with no thread left: skip recorded, no AI call', async () => {
    const out = await runAnalyzeTick({
        symbol: 'ETHUSDT',
        platform: 'bitget',
        decisionPolicy: 'balanced',
        wake: '1',
        enforcePrimaryCloseGate: '1',
        postCloseReconcile: '1',
        dryRun: 'false',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.summary).toBe('post_close_reconcile');
    expect(body.decision.reason).toBe('flat_skip_post_close_wake_fire');

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/post-close-wake-fire.txt');
});
