import assert from 'node:assert/strict';
import { test } from 'vitest';

import { listSessionDecisionWindows } from '../../../lib/swing/sessionEvents';

// September 2026: Berlin is UTC+2 (Xetra open 09:00 local = 07:00 UTC, close
// 17:30 = 15:30 UTC), New York is UTC−4 (cash open 09:30 = 13:30 UTC).
const MON = (h: number, m = 0) => Date.UTC(2026, 8, 14, h, m); // Monday 2026-09-14
const cfg = { preOpenMin: 120, postOpenMin: 30, postCloseMin: 60 };
const iso = (ms: number) => new Date(ms).toISOString().slice(11, 16);

test('DE40 at 04:02 UTC with a 6h horizon: one merged blind span around the Xetra open', () => {
    // The DE40 2026-09-11 case: a band armed here with a 10-min confirm was
    // unwatchable from 05:00 to 07:30 — pre_open and opening_drive merge into
    // the one stretch the model has to plan around.
    const spans = listSessionDecisionWindows({ symbol: 'DE40', category: 'index', nowMs: MON(4, 2), horizonMin: 360, ...cfg });
    assert.ok(spans);
    assert.equal(spans.length, 1);
    assert.equal(iso(spans[0].startMs), '05:00');
    assert.equal(iso(spans[0].endMs), '07:30');
    assert.deepEqual(spans[0].kinds, ['pre_open', 'opening_drive']);
    assert.deepEqual(spans[0].events, ['xetra_cash_open']);
});

test('a span already under way is listed; spans past the horizon are not', () => {
    // 06:00 UTC: inside the Xetra pre-open. Horizon 90 min ends at 07:30, so
    // the opening drive (07:00) still merges in and the US open span (11:30)
    // is out of range.
    const spans = listSessionDecisionWindows({ symbol: 'DE40', category: 'index', nowMs: MON(6), horizonMin: 90, ...cfg });
    assert.ok(spans);
    assert.equal(spans.length, 1);
    assert.equal(iso(spans[0].startMs), '05:00');
    assert.equal(iso(spans[0].endMs), '07:30');
});

test('a full-day horizon lists sorted, non-overlapping spans incl. the cross-venue US open', () => {
    const spans = listSessionDecisionWindows({ symbol: 'DE40', category: 'index', nowMs: MON(4, 2), horizonMin: 1440, ...cfg });
    assert.ok(spans && spans.length >= 3);
    for (let i = 1; i < spans.length; i++) {
        assert.ok(spans[i].startMs > spans[i - 1].endMs, `span ${i} overlaps or is out of order`);
    }
    const usOpen = spans.find((s) => s.events.includes('us_cash_open'));
    assert.ok(usOpen, 'the US cash open moves the DAX and counts as a window');
    assert.equal(iso(usOpen.startMs), '11:30');
    assert.equal(iso(usOpen.endMs), '14:00');
    const xetraClose = spans.find((s) => s.events.includes('xetra_cash_close'));
    assert.ok(xetraClose);
    assert.equal(iso(xetraClose.startMs), '15:30');
    assert.equal(iso(xetraClose.endMs), '16:30');
    assert.deepEqual(xetraClose.kinds, ['post_close']);
});

test('a window that already ended is not listed', () => {
    // 08:00 UTC: the Xetra open span ended at 07:30.
    const spans = listSessionDecisionWindows({ symbol: 'DE40', category: 'index', nowMs: MON(8), horizonMin: 120, ...cfg });
    assert.deepEqual(spans, []);
});

test('no venue calendar (crypto) → null, so the prompt payload stays absent', () => {
    assert.equal(listSessionDecisionWindows({ symbol: 'BTCUSDT', category: 'crypto', nowMs: MON(4), horizonMin: 1440, ...cfg }), null);
});
