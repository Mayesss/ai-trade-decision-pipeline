import assert from 'node:assert/strict';
import test from 'node:test';

import { collapseLimitReissues, attachRecentActionOutcomes, type RecentActionEntry } from './recentActions';

const MIN = 60_000;
const T0 = 1_750_000_000_000;
const at = (min: number) => T0 + min * MIN;

const entry = (action: string, min: number, extra: Partial<RecentActionEntry> = {}): RecentActionEntry => ({
    action,
    timestamp: at(min),
    closePct: null,
    entryLimitPrice: null,
    ...extra,
});

test('collapseLimitReissues: consecutive identical limit re-issues fold into one row', () => {
    const out = collapseLimitReissues([
        entry('SELL', 0, { entryLimitPrice: 24930.6 }),
        entry('SELL', 15, { entryLimitPrice: 24930.6 }),
        entry('SELL', 30, { entryLimitPrice: 24930.6 }),
        entry('HOLD', 45),
    ]);
    assert.equal(out.length, 2);
    assert.equal(out[0].action, 'SELL');
    assert.equal(out[0].reissueCount, 3);
    assert.equal(out[0].firstTimestamp, at(0));
    assert.equal(out[0].timestamp, at(30));
    assert.equal(out[1].action, 'HOLD');
});

test('collapseLimitReissues: different limit price, market entries, or interleaved actions break the chain', () => {
    const differentPrice = collapseLimitReissues([
        entry('SELL', 0, { entryLimitPrice: 100 }),
        entry('SELL', 15, { entryLimitPrice: 101 }),
    ]);
    assert.equal(differentPrice.length, 2);
    const marketEntries = collapseLimitReissues([entry('BUY', 0), entry('BUY', 15)]);
    assert.equal(marketEntries.length, 2);
    const interleaved = collapseLimitReissues([
        entry('SELL', 0, { entryLimitPrice: 100 }),
        entry('HOLD', 15),
        entry('SELL', 30, { entryLimitPrice: 100 }),
    ]);
    assert.equal(interleaved.length, 3);
});

test('attachRecentActionOutcomes: filled-and-closed entry gets pnl + hold time', () => {
    const [a] = attachRecentActionOutcomes([entry('BUY', 0, { entryLimitPrice: 100 })], {
        positions: [{ side: 'long', entryTimestamp: at(20), exitTimestamp: at(200), pnlPct: -0.8 }],
        nowMs: at(300),
    });
    assert.deepEqual(a.outcome, { closedPnlPctOnMargin: -0.8, heldMin: 180 });
});

test('attachRecentActionOutcomes: entry matching the open position reads still_open', () => {
    const [a] = attachRecentActionOutcomes([entry('SELL', 0, { entryLimitPrice: 100 })], {
        positions: [],
        openPosition: { side: 'short', entryTimestamp: at(10) },
        nowMs: at(30),
    });
    assert.equal(a.outcome, 'still_open');
});

test('attachRecentActionOutcomes: superseded unfilled limit reads never_filled', () => {
    const out = attachRecentActionOutcomes(
        [entry('SELL', 0, { entryLimitPrice: 100 }), entry('HOLD', 60)],
        { positions: [], nowMs: at(70) },
    );
    assert.equal(out[0].outcome, 'never_filled');
    assert.equal(out[1].outcome, null);
});

test('attachRecentActionOutcomes: a possibly still-resting limit claims nothing', () => {
    const [a] = attachRecentActionOutcomes([entry('SELL', 0, { entryLimitPrice: 100 })], {
        positions: [],
        nowMs: at(30),
    });
    assert.equal(a.outcome, null);
});

test('attachRecentActionOutcomes: aged-out lone limit with no fill reads never_filled', () => {
    const [a] = attachRecentActionOutcomes([entry('SELL', 0, { entryLimitPrice: 100 })], {
        positions: [],
        nowMs: at(120),
    });
    assert.equal(a.outcome, 'never_filled');
});

test('attachRecentActionOutcomes: unmatched market entry stays null, never never_filled', () => {
    const out = attachRecentActionOutcomes([entry('BUY', 0), entry('HOLD', 60)], {
        positions: [],
        nowMs: at(120),
    });
    assert.equal(out[0].outcome, null);
});

test('attachRecentActionOutcomes: side mismatch does not match', () => {
    const [a] = attachRecentActionOutcomes([entry('BUY', 0, { entryLimitPrice: 100 }), entry('HOLD', 60)].slice(0, 2), {
        positions: [{ side: 'short', entryTimestamp: at(20), exitTimestamp: at(50), pnlPct: 1 }],
        nowMs: at(120),
    });
    assert.equal(a.outcome, 'never_filled');
});

test('attachRecentActionOutcomes: fill window ends at the next action', () => {
    const out = attachRecentActionOutcomes(
        [entry('BUY', 0, { entryLimitPrice: 100 }), entry('HOLD', 30)],
        {
            // Entry landed AFTER the next evaluation superseded the order — not ours.
            positions: [{ side: 'long', entryTimestamp: at(45), exitTimestamp: at(90), pnlPct: 2 }],
            nowMs: at(120),
        },
    );
    assert.equal(out[0].outcome, 'never_filled');
});

test('attachRecentActionOutcomes: collapsed chain matches from its first issue', () => {
    const [a] = attachRecentActionOutcomes(
        [{ ...entry('SELL', 45, { entryLimitPrice: 100 }), firstTimestamp: at(0), reissueCount: 4 }],
        {
            positions: [{ side: 'short', entryTimestamp: at(20), exitTimestamp: at(300), pnlPct: -9.41 }],
            nowMs: at(400),
        },
    );
    assert.deepEqual(a.outcome, { closedPnlPctOnMargin: -9.41, heldMin: 280 });
});

test('attachRecentActionOutcomes: full CLOSE picks up the realized pnl, trims do not', () => {
    const positions = [{ side: 'long', entryTimestamp: at(0), exitTimestamp: at(180), pnlPct: 1.2 }];
    const [full] = attachRecentActionOutcomes([entry('CLOSE', 182)], { positions, nowMs: at(200) });
    assert.deepEqual(full.outcome, { closedPnlPctOnMargin: 1.2, heldMin: 180 });
    const [trim] = attachRecentActionOutcomes([entry('CLOSE', 182, { closePct: 30 })], {
        positions,
        nowMs: at(200),
    });
    assert.equal(trim.outcome, null);
});
