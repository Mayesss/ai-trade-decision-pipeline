import assert from 'node:assert/strict';
import test from 'node:test';

import { buildVenueSessionEvents } from './sessionEvents';

const ctx = (symbol: string, isoUtc: string, category?: string) =>
    buildVenueSessionEvents({ symbol, category: category ?? null, nowMs: Date.parse(isoUtc) });

// The dates below replay the exact boundary fills from the 2026-07-10..14 live
// bleed: each phase assertion is a moment where a resting limit got swept.

test('DE40 pre-open: 06:45 UTC in summer is 15 min before the 07:00 UTC Xetra open', () => {
    const c = ctx('DE40', '2026-07-13T06:45:00Z')!;
    assert.equal(c.liquidity_phase, 'pre_open');
    const open = c.upcoming.find((e) => e.event === 'xetra_cash_open')!;
    assert.equal(open.minutes_to, 15);
    assert.equal(open.at_utc, '2026-07-13T07:00:00.000Z');
});

test('DE40 DST: in winter the Xetra open sits at 08:00 UTC', () => {
    const c = ctx('DE40', '2026-01-13T07:45:00Z')!;
    assert.equal(c.liquidity_phase, 'pre_open');
    const open = c.upcoming.find((e) => e.event === 'xetra_cash_open')!;
    assert.equal(open.at_utc, '2026-01-13T08:00:00.000Z');
});

test('DE40 opening drive covers the first 30 min after the cash open', () => {
    assert.equal(ctx('DE40', '2026-07-13T07:10:00Z')!.liquidity_phase, 'opening_drive');
    assert.equal(ctx('DE40', '2026-07-13T10:00:00Z')!.liquidity_phase, 'normal');
});

test('HK50 into_close before the HKEX lunch, venue_break during it', () => {
    // Lunch 12:00–13:00 HKT = 04:00–05:00 UTC.
    assert.equal(ctx('HK50', '2026-07-14T03:46:00Z')!.liquidity_phase, 'into_close');
    assert.equal(ctx('HK50', '2026-07-14T04:30:00Z')!.liquidity_phase, 'venue_break');
    // Afternoon resumption drives like an open.
    assert.equal(ctx('HK50', '2026-07-14T05:10:00Z')!.liquidity_phase, 'opening_drive');
});

test('GOLD Sunday-evening reopen is thin_reopen; Saturday is off_hours', () => {
    // Globex weekly reopen Sun 18:00 ET = 22:00 UTC in July.
    assert.equal(ctx('GOLD', '2026-07-12T23:28:00Z')!.liquidity_phase, 'thin_reopen');
    assert.equal(ctx('GOLD', '2026-07-11T12:00:00Z')!.liquidity_phase, 'off_hours');
});

test('US100 into_close before the 20:00 UTC cash close; off_hours overnight', () => {
    assert.equal(ctx('US100', '2026-07-13T19:50:00Z')!.liquidity_phase, 'into_close');
    assert.equal(ctx('US100', '2026-07-13T15:00:00Z')!.liquidity_phase, 'normal');
    // 21:05 UTC Monday = 17:05 ET → inside the Globex maintenance halt.
    assert.equal(ctx('US100', '2026-07-13T21:05:00Z')!.liquidity_phase, 'venue_break');
    // 23:00 UTC = 19:00 ET → Globex reopened but the cash book is dark; the
    // overnight tape is thin relative to regular hours.
    assert.equal(ctx('US100', '2026-07-13T23:00:00Z')!.liquidity_phase, 'off_hours');
});

test('US100 opening drive right after the 13:30 UTC cash open', () => {
    assert.equal(ctx('US100', '2026-07-13T13:45:00Z')!.liquidity_phase, 'opening_drive');
});

test('FX: weekend off_hours, London-open drive, category fallback', () => {
    assert.equal(ctx('EURUSD', '2026-07-12T12:00:00Z', 'forex')!.liquidity_phase, 'off_hours');
    // London open 08:00 BST = 07:00 UTC in July.
    assert.equal(ctx('EURUSD', '2026-07-13T07:10:00Z', 'forex')!.liquidity_phase, 'opening_drive');
});

test('unknown symbol without a category yields no calendar', () => {
    assert.equal(ctx('SOMETHING', '2026-07-13T12:00:00Z'), null);
});

test('recent/upcoming windows carry signed distances as labeled fields', () => {
    const c = ctx('US100', '2026-07-13T13:45:00Z')!;
    const open = c.recent.find((e) => e.event === 'us_cash_open')!;
    assert.equal(open.minutes_ago, 15);
    assert.ok(c.upcoming.every((e) => e.minutes_to > 0));
    assert.ok(c.recent.every((e) => e.minutes_ago >= 0));
});
