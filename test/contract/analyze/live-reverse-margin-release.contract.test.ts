// Contract: a LIVE REVERSE the account can only afford BECAUSE the position
// being closed hands its margin back. executeDecision closes (flash-close)
// before it opens the opposite side, so the closing position's margin is on
// the balance by the time the new order is sent — but the affordability gate
// used to compare the new side's margin against RAW pre-close availability
// and refused the trade. 2026-09-07 ADAUSDT: needed 20.27 against 10.01
// "available" while the short being closed held 20.40 of its own, so the
// reversal was silently downgraded to HOLD. This pins the fix: free margin
// alone is far too small here, the released margin covers it, the reverse
// executes.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, inPositionPrivateWorld, runAnalyzeTick } from './world';
import { conversationSummary, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { bitgetPost } from '../../harness/worlds/bitget';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Long from 75,000 (captured mark ~77,543) reversing to short: stop above,
// target below.
const REVERSE = {
    ...decisionBase('REVERSE', 'long thesis broke, flipping short', 'primary flipped bear at resistance'),
    leverage: 5,
    stop_loss_price: 80500,
    take_profit_price: 72000,
};

// The open long holds 775.44 of margin (0.05 × 77,543.7 ÷ 5). Free margin is
// set BELOW the reversal's own requirement (~525: 1% of 10,000 equity over a
// 3.8% stop, at 5×) so the only way this trade can pass the gate is by
// counting what the close gives back.
const POSITION_MARGIN = '775.44';
const FREE_MARGIN = '350';

startBoundary(
    () => ({
        http: [
            ...bitgetMarketWorld(fixture),
            ...inPositionPrivateWorld({
                symbol: 'BTCUSDT',
                entryPrice: '75000',
                markPrice: '77543.7',
                openedAtMs: fixture.capturedAtMs - 2 * 24 * 3600_000,
                takeProfit: '84000',
                stopLoss: '74500',
                marginSize: POSITION_MARGIN,
                availableUsd: FREE_MARGIN,
            }),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin rejects range high', sentiment: 'NEGATIVE' }]),
            forexFactoryCalendar([]),
            responsesDecides(REVERSE),
            bitgetPost('/api/v2/mix/account/set-margin-mode', {}),
            bitgetPost('/api/v2/mix/account/set-leverage', { symbol: 'BTCUSDT', leverage: '5' }),
            bitgetPost('/api/v2/mix/order/close-positions', { successList: [{ symbol: 'BTCUSDT' }], failureList: [] }),
            bitgetPost('/api/v2/mix/order/place-order', { orderId: 'order-reverse-1', clientOid: 'echo' }),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('live REVERSE: the closing position\'s released margin makes the flip affordable', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;

    // The gate must not have touched the action.
    expect(body.decision.action).toBe('REVERSE');
    expect(body.decision.original_action).toBeUndefined();
    expect(body.decision.entry_dropped).toBeUndefined();
    expect(String(body.decision.reason)).not.toContain('entry dropped');

    // The scenario is only meaningful while free margin alone is insufficient:
    // if this ever stops holding, the test has stopped testing the fix.
    const marginNeeded = Number(body.decision.risk_sizing.margin_usd);
    expect(marginNeeded).toBeGreaterThan(Number(FREE_MARGIN));
    expect(marginNeeded).toBeLessThan(Number(FREE_MARGIN) + Number(POSITION_MARGIN));

    // And it actually reversed on the venue: close first, then the flip.
    expect(body.execRes.placed).toBe(true);
    expect(body.execRes.reversed).toBe(true);
    expect(body.execRes.orderId).toBe('order-reverse-1');

    const summary = await conversationSummary();
    expect(summary.filter((line) => line.includes('close-positions'))).toHaveLength(1);
    expect(summary.filter((line) => line.includes('place-order'))).toHaveLength(1);
});
