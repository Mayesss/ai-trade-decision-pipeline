// Contract: a flat tick where the model answers SELL with a bracket. The
// captured BTCUSDT market is trend-down (that is what made it actionable), so
// the with-trend entry here is a short — a BUY would be demoted to HOLD by
// postprocessDecision's counter-trend rule. dryRun short-circuits execution
// before any order HTTP — the contract is that the conversation contains NO
// place-order/set-leverage call, while the decision row (with the full
// prompt) and tick log are still persisted.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, flatPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { openAiDecides } from '../../harness/worlds/aiGateway';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Captured lastPr was ~77,543 — short with the stop above and the target below.
const SELL = {
    ...decisionBase('SELL', 'breakdown continuation short', 'confirmed primary breakdown with room below'),
    leverage: 5,
    stop_loss_price: 80500,
    take_profit_price: 72000,
};

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...flatPrivateWorld(),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin loses range support', sentiment: 'NEGATIVE' }]),
            forexFactoryCalendar([]),
            openAiDecides(SELL),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('flat tick, SELL answer: dryRun stops before any order call', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'true' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;
    expect(body.decision.action).toBe('SELL');
    expect(body.execRes.placed).toBe(false);
    expect(body.execRes.orderId).toBeNull();
    expect(body.execRes.clientOid).toMatch(/^cfw-/);

    const summary = await conversationSummary();
    expect(summary.some((line) => line.includes('place-order'))).toBe(false);
    expect(summary.some((line) => line.includes('set-leverage'))).toBe(false);

    await expect(await conversation()).toMatchFileSnapshot('./__snapshots__/entry-sell.txt');
});
