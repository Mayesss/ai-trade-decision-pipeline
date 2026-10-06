// Constants and windows that assume a watcher tick length
// (WAKE_WATCH_TICK_MINUTES = 10 since 2026-10-06). Each one was sized for the
// one-minute watcher before; these pin that they now agree with the tick.
import assert from 'node:assert/strict';
import { test } from 'vitest';

import {
    failedBreakCheckDue,
    FAILED_BREAK_POST_CLOSE_WINDOW_MIN,
    SESSION_SWEEP_STATE_TTL_SECONDS,
    SESSION_SWEEP_WINDOW_MINUTES,
    WAKE_CONFIRM_MIN_MINUTES,
    WAKE_WATCH_FIRED_TTL_SECONDS,
    WAKE_WATCH_TICK_MINUTES,
    WAKE_WATCH_TICK_OFFSET_MINUTES,
} from '../../../lib/swing/wakeWatch';

const H4 = 4 * 3600_000;
const at = (h: number, m: number, s = 0) => Date.UTC(2026, 7, 12, h, m, s);

test('failed-break check: the first two ticks after a 4H close (:05, :15), jitter included, never the third', () => {
    assert.equal(failedBreakCheckDue(H4, at(8, 5, 5)), true);
    assert.equal(failedBreakCheckDue(H4, at(8, 15, 0)), true);
    // The tick the old 10-minute window never reached.
    assert.equal(failedBreakCheckDue(H4, at(8, 15, 30)), true);
    assert.equal(failedBreakCheckDue(H4, at(8, 19, 59)), true);
    assert.equal(failedBreakCheckDue(H4, at(8, 25, 0)), false);
    assert.equal(failedBreakCheckDue(H4, at(9, 5, 0)), false);
    const secondTick = WAKE_WATCH_TICK_OFFSET_MINUTES + WAKE_WATCH_TICK_MINUTES;
    assert.ok(FAILED_BREAK_POST_CLOSE_WINDOW_MIN >= secondTick);
    assert.ok(FAILED_BREAK_POST_CLOSE_WINDOW_MIN < secondTick + WAKE_WATCH_TICK_MINUTES);
});

test('fired marker outlives the run that set it, but not the gap to the next tick', () => {
    // pages/api/swing/wake-watch.ts maxDuration = 300.
    assert.ok(WAKE_WATCH_FIRED_TTL_SECONDS >= 300);
    assert.ok(WAKE_WATCH_FIRED_TTL_SECONDS < WAKE_WATCH_TICK_MINUTES * 60);
});

test('a confirm window can be no shorter than one tick (it is only observed at ticks)', () => {
    assert.equal(WAKE_CONFIRM_MIN_MINUTES, WAKE_WATCH_TICK_MINUTES);
});

test('session-sweep touch state survives until the tick that abandons it', () => {
    // Abandon happens on the first tick past the window: up to one tick after
    // it ends, plus jitter.
    assert.ok(SESSION_SWEEP_STATE_TTL_SECONDS >= (SESSION_SWEEP_WINDOW_MINUTES + WAKE_WATCH_TICK_MINUTES) * 60);
});
