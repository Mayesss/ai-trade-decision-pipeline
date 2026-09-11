// Contract: a LIVE REVERSE the account genuinely cannot afford, even after the
// closing position hands its margin back. Two things must hold on the way out.
//
// 1. The model's call survives the downgrade. Execution needs action=HOLD so
//    nothing is placed, but the dashboard has to report REVERSE — on
//    2026-09-07 ADAUSDT a refused reversal rendered as a plain HOLD, which
//    reads as the model choosing to sit still: the opposite of what happened.
// 2. The bracket built for the reversal is torn down. It was sized for the
//    NEW side, which on a reverse is the opposite of the position still on the
//    venue, so shipping it amends the survivor with an inverted bracket.
//    Bitget refused both legs that day (45122/45135) and the original bracket
//    survived by luck alone. No modify call may leave the process at all.

import { expect, test } from 'vitest';

import btcFixtureJson from '../fixtures/bitget-BTCUSDT.json';
import { analyzePg, decisionBase, inPositionPrivateWorld, runAnalyzeTick } from './world';
import { conversation, conversationSummary, startBoundary } from '../../harness';
import { responsesDecides } from '../../harness/worlds/aiGateway';
import { forexFactoryCalendar } from '../../harness/worlds/forexFactory';
import { kvWorld } from '../../harness/worlds/kv';
import { coindeskNews } from '../../harness/worlds/news';
import { bitgetMarketWorld } from '../../harness/worlds/recordedMarkets';

import type { RecordedMarketFixture } from '../../harness/worlds/recordedMarkets';

const fixture = btcFixtureJson as RecordedMarketFixture;

// Long from 75,000 reversing to short: the new side's stop sits ABOVE price
// and its target BELOW — both on the wrong side for the long that stays open
// when the entry is refused, which is exactly what must not be sent.
const REVERSE = {
    ...decisionBase('REVERSE', 'long thesis broke, flipping short', 'primary flipped bear at resistance'),
    leverage: 5,
    stop_loss_price: 80500,
    take_profit_price: 72000,
};

// Sizing wants ~525 of margin (1% of 10,000 equity over a 3.8% stop, at 5×);
// the long releases 400 and only 100 is free, so the reversal is unaffordable
// however generously it is measured.
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
                marginSize: '400.00',
                availableUsd: '100',
            }),
            ...kvWorld(),
            coindeskNews([{ title: 'Bitcoin rejects range high', sentiment: 'NEGATIVE' }]),
            forexFactoryCalendar([]),
            responsesDecides(REVERSE),
        ],
        db: analyzePg,
    }),
    { nowMs: fixture.capturedAtMs },
);

test('live REVERSE refused on margin: action preserved for the UI, bracket torn down', async () => {
    const out = await runAnalyzeTick({ symbol: 'BTCUSDT', platform: 'bitget', dryRun: 'false' });

    expect(out.statusCode).toBe(200);
    const body = out.body as Record<string, any>;

    // Nothing executed...
    expect(body.decision.action).toBe('HOLD');
    expect(body.execRes.placed).toBe(false);

    // ...but the model's actual call is still on the record for the dashboard.
    expect(body.decision.original_action).toBe('REVERSE');
    expect(body.decision.entry_dropped).toBe('insufficient_available_margin');
    // The note names both halves of the affordability read, so the reason
    // string alone explains the refusal.
    expect(String(body.decision.reason)).toContain('reverse_release≈400.00');

    // The reversal's bracket is gone — not merely rejected downstream.
    expect(body.decision.stop_loss_price).toBeNull();
    expect(body.decision.take_profit_price).toBeNull();
    // The persisted snapshot records the teardown too, so "what did the model
    // ask for vs what shipped" stays answerable after the fact.
    const persisted = await conversation();
    expect(persisted).toContain('bracket_dropped_with_entry');

    // The wrong-side bracket never reached the venue. The world declares no
    // order endpoints at all, so any attempt would also fail as unhandled.
    const summary = await conversationSummary();
    expect(summary.filter((line) => line.includes('modify-tpsl-order'))).toHaveLength(0);
    expect(summary.filter((line) => line.includes('place-order'))).toHaveLength(0);
    expect(summary.filter((line) => line.includes('close-positions'))).toHaveLength(0);
});
