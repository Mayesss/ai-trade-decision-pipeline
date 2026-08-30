import assert from 'node:assert/strict';
import { test } from 'vitest';

import { sanitizeRestingEntry } from '../../../lib/swing/decisionRules';
import type { RestingEntryKind } from '../../../lib/swing/decisionConfig';

// Anchor: price 100, primary ATR 2.
//   limit window 0.1–1.5 ATR  -> BUY 97.0–99.8,  SELL 100.2–103.0
//   stop  window 0.25–1.5 ATR -> BUY 100.5–103.0, SELL 97.0–99.5
// Venue support is pinned here so these stay unit tests of the ENVELOPE, not of
// which venue happens to be wired (that pairing is covered separately below).
const PRICE = 100;
const ATR = 2;
const allowBoth = () => true;

type Args = {
    action?: 'BUY' | 'SELL' | 'HOLD';
    limit?: number | null;
    stop?: number | null;
    positionOpen?: boolean;
    primaryAtr?: number | null;
    allowKind?: (kind: RestingEntryKind) => boolean;
};

const run = (args: Args = {}) =>
    sanitizeRestingEntry({
        action: args.action ?? 'BUY',
        positionOpen: args.positionOpen ?? false,
        price: PRICE,
        primaryAtr: args.primaryAtr === undefined ? ATR : args.primaryAtr,
        entryLimitPrice: args.limit ?? null,
        entryStopPrice: args.stop ?? null,
        allowKind: args.allowKind ?? allowBoth,
    });

// --- the four quadrants, which is the whole point of the split ---------------

test('all four resting quadrants are valid and keep their kind', () => {
    // limit rests AGAINST the trade
    const buyLimit = run({ action: 'BUY', limit: 99 }); // 0.5 ATR below
    assert.equal(buyLimit.price, 99);
    assert.equal(buyLimit.kind, 'limit');
    assert.equal(buyLimit.dropEntry, false);

    const sellLimit = run({ action: 'SELL', limit: 101.5 }); // 0.75 ATR above
    assert.equal(sellLimit.price, 101.5);
    assert.equal(sellLimit.kind, 'limit');

    // stop rests WITH the trade — the quadrants that had no primitive before
    const buyStop = run({ action: 'BUY', stop: 101.5 }); // 0.75 ATR above
    assert.equal(buyStop.price, 101.5);
    assert.equal(buyStop.kind, 'stop');
    assert.equal(buyStop.dropEntry, false);

    const sellStop = run({ action: 'SELL', stop: 98.5 }); // 0.75 ATR below
    assert.equal(sellStop.price, 98.5);
    assert.equal(sellStop.kind, 'stop');
    assert.equal(sellStop.dropEntry, false);
});

test('the same price is valid for one kind and wrong-side for the other', () => {
    // 101.5 is a legitimate BUY stop but a wrong-side BUY limit.
    assert.equal(run({ action: 'BUY', stop: 101.5 }).kind, 'stop');
    const asLimit = run({ action: 'BUY', limit: 101.5 });
    assert.equal(asLimit.dropEntry, true);
    assert.ok(asLimit.notes.includes('resting_entry_limit_wrong_side_entry_dropped'));

    // ...and the mirror: 99 is a legitimate BUY limit but a wrong-side BUY stop.
    assert.equal(run({ action: 'BUY', limit: 99 }).kind, 'limit');
    const asStop = run({ action: 'BUY', stop: 99 });
    assert.equal(asStop.dropEntry, true);
    assert.ok(asStop.notes.includes('resting_entry_stop_wrong_side_entry_dropped'));
});

// --- market, exclusivity, applicability -------------------------------------

test('both fields null means market entry as requested, no notes, entry kept', () => {
    const out = run({ action: 'BUY' });
    assert.equal(out.price, null);
    assert.equal(out.kind, null);
    assert.equal(out.dropEntry, false);
    assert.deepEqual(out.notes, []);
});

test('setting both tools is contradictory and drops the entry', () => {
    const out = run({ action: 'BUY', limit: 99, stop: 101.5 });
    assert.equal(out.price, null);
    assert.equal(out.kind, null);
    assert.equal(out.dropEntry, true);
    assert.ok(out.notes.includes('resting_entry_ambiguous_entry_dropped'));
});

test('in-position and non-entry actions never rest an order and never drop', () => {
    const hold = run({ action: 'HOLD', limit: 99 });
    assert.equal(hold.price, null);
    assert.equal(hold.dropEntry, false);

    const inPos = run({ action: 'BUY', limit: 99, positionOpen: true });
    assert.equal(inPos.price, null);
    assert.equal(inPos.dropEntry, false);

    // A stop takes the same path.
    const inPosStop = run({ action: 'BUY', stop: 101.5, positionOpen: true });
    assert.equal(inPosStop.price, null);
    assert.equal(inPosStop.dropEntry, false);
});

// --- the envelope ------------------------------------------------------------

test('inside the per-kind noise band drops the entry, and the bands differ', () => {
    const closeLimit = run({ action: 'SELL', limit: 100.1 }); // 0.05 ATR
    assert.equal(closeLimit.dropEntry, true);
    assert.ok(closeLimit.notes.includes('resting_entry_limit_too_close_entry_dropped'));

    // 0.2 ATR clears the limit floor (0.1) but not the stop floor (0.25):
    // a stop that close is triggered by the bar it is sitting in.
    assert.equal(run({ action: 'BUY', limit: 99.6 }).dropEntry, false);
    const closeStop = run({ action: 'BUY', stop: 100.4 });
    assert.equal(closeStop.dropEntry, true);
    assert.ok(closeStop.notes.includes('resting_entry_stop_too_close_entry_dropped'));
});

test('beyond the max window the price clamps and the entry survives', () => {
    const buyLimit = run({ action: 'BUY', limit: 90 }); // 5 ATR below
    assert.equal(buyLimit.price, PRICE - 1.5 * ATR);
    assert.equal(buyLimit.kind, 'limit');
    assert.equal(buyLimit.dropEntry, false);
    assert.ok(buyLimit.notes.includes('resting_entry_limit_clamped_max_atr'));

    assert.equal(run({ action: 'SELL', limit: 110 }).price, PRICE + 1.5 * ATR);
    // Stops clamp on the opposite side of price from limits.
    assert.equal(run({ action: 'BUY', stop: 120 }).price, PRICE + 1.5 * ATR);
    assert.equal(run({ action: 'SELL', stop: 80 }).price, PRICE - 1.5 * ATR);
});

test('missing ATR cannot validate the request → entry dropped for either kind', () => {
    const noAtrLimit = run({ action: 'BUY', limit: 99, primaryAtr: null });
    assert.equal(noAtrLimit.dropEntry, true);
    assert.ok(noAtrLimit.notes.includes('resting_entry_no_atr_entry_dropped'));
    assert.equal(run({ action: 'BUY', stop: 101.5, primaryAtr: null }).dropEntry, true);
});

// --- venue support -----------------------------------------------------------

test('an unsupported kind drops the entry rather than degrading to the other', () => {
    // Bitget today: limits rest, stops are not yet wired. A stop must NOT
    // become a limit — that is the opposite trade.
    const limitOnly = (kind: RestingEntryKind) => kind === 'limit';
    const stop = run({ action: 'BUY', stop: 101.5, allowKind: limitOnly });
    assert.equal(stop.price, null);
    assert.equal(stop.kind, null);
    assert.equal(stop.dropEntry, true);
    assert.ok(stop.notes.includes('resting_entry_stop_unsupported_entry_dropped'));

    // The supported kind is unaffected, and market entries never depend on support.
    assert.equal(run({ action: 'BUY', limit: 99, allowKind: limitOnly }).kind, 'limit');
    assert.equal(run({ action: 'BUY', allowKind: () => false }).dropEntry, false);
});
