// Contract: a LIVE flat tick whose SELL loses the commit-time asset-class
// claim — a sibling (ETHUSDT) took 'class:crypto' while this tick was mid-AI,
// so the pre-AI gate saw an empty book but the entry must not go in. Pins that
// the claim is attempted before any order endpoint and that a lost claim
// places nothing and starts no thread (2026-09-23 BTCUSDT + ETHUSDT).

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePgWith, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

const SELL = {
    ...decisionBase('SELL', 'breakdown continuation short', 'confirmed primary breakdown with room below'),
    leverage: 5,
    stop_loss_price: 80500,
    take_profit_price: 72000,
    entry_trigger_price: 77300,
};

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin loses range support', sentiment: 'NEGATIVE' }]),
            forexFactoryCalendar([]),
            responsesDecides(SELL),
        ],
        db: analyzePgWith((text) => {
            // ETHUSDT's tick claimed the class moments ago; its thread is not
            // written yet, so ai_threads is still empty.
            if (text.startsWith('INSERT INTO swing.entry_claims')) return [];
            if (text.includes('FROM swing.entry_claims')) {
                return [{ claim_key: 'class:crypto', platform: 'bitget', symbol: 'ETHUSDT', claimed_at_ms: fixture.capturedAtMs }];
            }
            return undefined;
        }),
    }),
    { nowMs: fixture.capturedAtMs },
);

test('live SELL that loses the class claim drops to HOLD and places nothing', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('HOLD');
    expect(body.decision.original_action).toBe('SELL');
    expect(body.decision.entry_dropped).toBe('asset_class_claimed');
    expect(body.decision.reason).toContain('asset_class_claimed:crypto:ETHUSDT');
    expect(body.decision.stop_loss_price).toBeNull();
    expect(body.execRes.placed).not.toBe(true);

    const summary = await conversationSummary();
    expect(summary.filter((line) => line.includes('place-order'))).toHaveLength(0);
    expect(summary.filter((line) => line.includes('set-leverage'))).toHaveLength(0);

    const text = await conversation();
    expect(text).not.toContain('INSERT INTO swing.ai_threads');

    await expect(text).toMatchFileSnapshot('./__snapshots__/live-entry-claim-lost.txt');
});
