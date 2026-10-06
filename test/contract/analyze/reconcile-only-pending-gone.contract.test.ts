// Contract: the wake-watcher's pending_entry_gone fire (reconcileOnly=1). The
// thread row says a resting entry is live, the venue's order books are empty
// (venue purge, manual cancel). Until 2026-10-06 the next 15-minute cron tick
// cleaned this up as a side effect; with analyze running only inside the daily
// decision windows, the watcher fires this run instead. It must end the stale
// thread — which otherwise holds an asset-class slot in the portfolio cap —
// and stop right there: no market-data fetch, no gates, no AI call.

import { expect, test } from 'vitest';

import ethFixtureJson from '../fixtures/bitget-ETHUSDT.json';
import { analyzePgWith, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = ethFixtureJson as RecordedMarketFixture;

const STALE_PENDING_THREAD = {
    platform: 'bitget',
    symbol: 'ETHUSDT',
    status: 'pending_entry',
    last_response_id: 'resp_entry-1',
    turns: 1,
    provider: 'openai',
    transcript: [],
    wake_above: null,
    wake_below: null,
    wake_note: null,
    wake_set_at_ms: null,
};

startBoundary(
    () => ({
        // flatPrivateWorld: no position, both entry order books empty.
        http: [...bitgetMarketWorld(fixture), ...flatPrivateWorld(), ...kvWorld(), forexFactoryCalendar([])],
        db: analyzePgWith((text) =>
            text.includes('FROM swing.ai_threads') && text.startsWith('SELECT') ? [STALE_PENDING_THREAD] : undefined,
        ),
    }),
    { nowMs: fixture.capturedAtMs },
);

test('pending_entry_gone fire: stale thread ended, then a code-only stop before any market read', async () => {
    const out = await runAnalyzeTick({
        symbol: 'ETHUSDT',
        platform: 'bitget',
        decisionPolicy: 'balanced',
        wake: '1',
        reconcileOnly: '1',
        dryRun: 'false',
    });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.promptSkipped).toBe(true);
    expect(body.decision.summary).toBe('reconcile_only');
    expect(body.decision.reason).toBe('reconcile_only_flat');

    const transcript = await conversation();
    const summary = await conversationSummary();
    // The reconcile: both books read, the stale row deleted.
    expect(summary.some((line) => line.includes('orders-pending'))).toBe(true);
    expect(transcript).toContain('DELETE FROM swing.ai_threads');
    // ...and nothing a look would need.
    expect(summary.some((line) => line.includes('/api/v2/mix/market/candles'))).toBe(false);
    expect(summary.some((line) => line.includes('ai-gateway.vercel.sh'))).toBe(false);
    // Look-only gates are skipped: no portfolio-occupancy query, no margin read.
    expect(transcript.includes("status IN ('pending_entry', 'in_position')")).toBe(false);
    // One account read — the equity snapshot every non-quarter tick writes;
    // the margin pre-skip would have been a second.
    expect(summary.filter((line) => line.includes('/api/v2/mix/account/accounts'))).toHaveLength(1);

    await expect(transcript).toMatchFileSnapshot('./__snapshots__/reconcile-only-pending-gone.txt');
});
