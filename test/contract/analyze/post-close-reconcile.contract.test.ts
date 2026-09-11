// Contract: the tick that discovers a venue-side close (AI thread says
// in_position, venue says flat) is code-only. It ends the thread and records
// the skip, and does NOT take a fresh flat look — even on a manual/boundary
// cadence where a flat tick would normally reach the model. Post-mortems own
// "what happened"; the next regular 4H close owns "what now".

import { expect, test } from 'vitest';

import ethFixtureJson from '../fixtures/bitget-ETHUSDT.json';
import { analyzePgWith, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = ethFixtureJson as RecordedMarketFixture;

// The thread row a position leaves behind: still in_position, venue now flat.
const IN_POSITION_THREAD = {
    status: 'in_position',
    last_response_id: 'resp_eth_manage_17',
    turns: 17,
    provider: 'openai',
    transcript: null,
    wake_above: null,
    wake_below: null,
    wake_note: null,
    wake_set_at_ms: null,
};

startBoundary(
    () => ({
        // No AI gateway handler on purpose: a model call here fails the test
        // via msw error-on-unhandled.
        http: [...bitgetMarketWorld(fixture), ...flatPrivateWorld(), ...kvWorld(), forexFactoryCalendar([])],
        db: analyzePgWith((text) => (text.includes('FROM swing.ai_threads') ? [IN_POSITION_THREAD] : undefined)),
    }),
    { nowMs: fixture.capturedAtMs },
);

test('venue-side close discovered by the tick: thread ended, skip recorded, no AI call', async () => {
    // dryRun=false: the thread reconcile only runs on live ticks. Nothing is
    // placed on this path — the skip returns before any order logic.
    const out = await runAnalyzeTick({
        symbol: 'ETHUSDT',
        platform: 'bitget',
        newsSource: 'coindesk',
        category: 'crypto',
        dryRun: 'false',
        decisionPolicy: 'balanced',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.summary).toBe('post_close_reconcile');
    expect(body.decision.reason).toBe('flat_skip_post_close_reconcile');
    expect(body.execRes.placed).toBe(false);

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
    const convo = await conversation();
    // The thread was ended (DELETE FROM swing.ai_threads) and the skip logged.
    expect(convo).toContain('DELETE FROM swing.ai_threads');
    expect(convo).toContain('post_close_reconcile');

    await expect(convo).toMatchFileSnapshot('./__snapshots__/post-close-reconcile.txt');
});
