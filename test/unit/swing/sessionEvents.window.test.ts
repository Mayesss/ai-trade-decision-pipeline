import assert from 'node:assert/strict';
import { test } from 'vitest';

import { evaluateSessionDecisionWindow } from '../../../lib/swing/sessionEvents';

// September 2026: Berlin is UTC+2 (Xetra 09:00 local = 07:00 UTC, close 17:30
// = 15:30 UTC), New York is UTC−4 (cash open 09:30 = 13:30 UTC), London is
// UTC+1 (LSE open 08:00 = 07:00 UTC, close 16:30 = 15:30 UTC).
const MON = (h: number, m = 0) => Date.UTC(2026, 8, 14, h, m); // Monday 2026-09-14
const SAT = (h: number) => Date.UTC(2026, 8, 12, h);
const cfg = { preOpenMin: 120, postOpenMin: 30, postCloseMin: 60 };

const win = (symbol: string, nowMs: number, category: string | null = null) =>
    evaluateSessionDecisionWindow({ symbol, category, nowMs, ...cfg });

test('the three measured buckets are inside a window', () => {
    // 06:00 UTC DE40 — one hour before the Xetra open.
    const dax = win('DE40', MON(6));
    assert.equal(dax?.active, true);
    assert.equal(dax?.kind, 'pre_open');
    assert.equal(dax?.event, 'xetra_cash_open');
    // 12:00 UTC US100 — ninety minutes before the New York open.
    const nq = win('US100', MON(12));
    assert.equal(nq?.active, true);
    assert.equal(nq?.kind, 'pre_open');
    assert.equal(nq?.event, 'us_cash_open');
    // 16:00 UTC GOLD — thirty minutes after the European cash close, which the
    // metals calendar now carries as a cross-venue influence event.
    const gold = win('GOLD', MON(16));
    assert.equal(gold?.active, true);
    assert.equal(gold?.kind, 'post_close');
    assert.equal(gold?.event, 'europe_cash_close');
    // Same for a US index and a forex pair at 16:00 UTC.
    assert.equal(win('US500', MON(16))?.event, 'europe_cash_close');
    // FX lists the London close at the same instant; either name is the same window.
    const fx = win('EURUSD', MON(16), 'forex');
    assert.equal(fx?.kind, 'post_close');
    assert.ok(['london_close', 'europe_cash_close'].includes(String(fx?.event)));
});

test('opening drive and the owed look boundary', () => {
    // 13:45 UTC US100 — 15 min into the New York open: opening_drive.
    const drive = win('US100', MON(13, 45));
    assert.equal(drive?.kind, 'opening_drive');
    assert.equal(drive?.endMs, MON(14));
    // 14:00 UTC — the window has just ended; the first tick here is the owed look.
    const after = win('US100', MON(14));
    assert.equal(after?.active, false);
    assert.equal(after?.kind, null);
    // 14:30 UTC — plain session.
    assert.equal(win('US100', MON(14, 30))?.active, false);
});

test('overlapping windows: the one ending last governs', () => {
    // 15:45 UTC DE40: Xetra's own close (15:30, home) and the US cash session
    // both matter; the post_close window runs to 16:30.
    const dax = win('DE40', MON(15, 45));
    assert.equal(dax?.kind, 'post_close');
    assert.equal(dax?.endMs, MON(16, 30));
});

test('no calendar → null; weekend → inactive; disabled minutes shrink the windows', () => {
    assert.equal(win('BTCUSDT', MON(12), 'crypto'), null);
    assert.equal(win('US100', SAT(12))?.active, false);
    // With a 30-minute pre-open the 12:00 UTC US tick is no longer inside.
    const narrow = evaluateSessionDecisionWindow({ symbol: 'US100', nowMs: MON(12), preOpenMin: 30, postOpenMin: 30, postCloseMin: 60 });
    assert.equal(narrow?.active, false);
});
