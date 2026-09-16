// The scheduled-look cadence is pinned to 'primary' (every 4H close) for the
// test run (test/harness/setup-env.ts) so the fixtures' frozen clocks keep
// meaning what they meant. This file pins the DAILY cadence — the production
// default since 2026-09-16 — as pure functions of the clock.
//
// DECISION_CADENCE and the cooldown bounds are module-level consts captured at
// import time, so the env is stubbed BEFORE a fresh dynamic import.

import assert from 'node:assert/strict';
import { beforeAll, test, vi } from 'vitest';

let cfg: typeof import('../../../lib/swing/decisionConfig');

beforeAll(async () => {
    vi.stubEnv('SWING_DECISION_CADENCE', '1D');
    for (const k of [
        'SWING_DECISION_HOUR_UTC_BITGET',
        'SWING_DECISION_HOUR_UTC_CAPITAL',
        'SWING_DECISION_RETRY_WINDOW_MIN',
        'SWING_AI_COOLDOWN_MIN_MIN',
        'SWING_AI_COOLDOWN_MAX_MIN',
    ]) {
        vi.stubEnv(k, undefined);
    }
    vi.resetModules();
    cfg = await import('../../../lib/swing/decisionConfig');
    assert.equal(cfg.DECISION_CADENCE, '1D');
});

const at = (iso: string) => new Date(iso);

test('venue decision hours: Bitget at the daily candle close, Capital in the London morning; both are 4H closes', () => {
    assert.equal(cfg.DECISION_HOUR_UTC.bitget, 0);
    assert.equal(cfg.DECISION_HOUR_UTC.capital, 8);
    assert.equal(cfg.decisionHourUtcFor('bitget'), 0);
    assert.equal(cfg.decisionHourUtcFor('capital'), 8);
    assert.equal(cfg.decisionHourUtcFor(null), 0);
    // Indicators are read on closed primary (4H) bars, so the hour must be one.
    assert.equal(cfg.DECISION_HOUR_UTC.bitget % 4, 0);
    assert.equal(cfg.DECISION_HOUR_UTC.capital % 4, 0);
    assert.equal(cfg.DECISION_RETRY_WINDOW_MIN, 360);
});

test('decisionDayKey: the look is owed from the decision hour for the whole retry window, and nowhere else', () => {
    // Bitget, hour 0: due from 00:00 through 05:59, keyed to that day.
    assert.equal(cfg.decisionDayKey('bitget', at('2026-09-16T00:00:00Z')), '2026-09-16');
    assert.equal(cfg.decisionDayKey('bitget', at('2026-09-16T00:03:00Z')), '2026-09-16'); // jitter past the old 2-min tolerance
    assert.equal(cfg.decisionDayKey('bitget', at('2026-09-16T04:00:00Z')), '2026-09-16'); // a later 4H close still serves the owed day
    assert.equal(cfg.decisionDayKey('bitget', at('2026-09-16T05:59:00Z')), '2026-09-16');
    assert.equal(cfg.decisionDayKey('bitget', at('2026-09-16T06:00:00Z')), null); // window closed, day is over
    assert.equal(cfg.decisionDayKey('bitget', at('2026-09-16T23:59:00Z')), null); // not early for tomorrow either

    // Capital, hour 8: due 08:00 through 13:59.
    assert.equal(cfg.decisionDayKey('capital', at('2026-09-16T07:59:00Z')), null);
    assert.equal(cfg.decisionDayKey('capital', at('2026-09-16T08:00:15Z')), '2026-09-16');
    assert.equal(cfg.decisionDayKey('capital', at('2026-09-16T12:00:15Z')), '2026-09-16'); // the 12:00 miss of 2026-09-16 would have been served
    assert.equal(cfg.decisionDayKey('capital', at('2026-09-16T14:00:00Z')), null);
});

test('decisionDayKey keys a window that crosses midnight to the day of the decision hour', () => {
    vi.stubEnv('SWING_DECISION_HOUR_UTC_CAPITAL', '22');
    return (async () => {
        vi.resetModules();
        const late = await import('../../../lib/swing/decisionConfig');
        assert.equal(late.decisionDayKey('capital', at('2026-09-16T23:30:00Z')), '2026-09-16');
        assert.equal(late.decisionDayKey('capital', at('2026-09-17T02:30:00Z')), '2026-09-16');
        assert.equal(late.decisionDayKey('capital', at('2026-09-17T04:00:00Z')), null);
        vi.stubEnv('SWING_DECISION_HOUR_UTC_CAPITAL', undefined);
    })();
});

test('served marker key is per venue and symbol', () => {
    assert.equal(cfg.scheduledLookServedKey('capital', 'us500'), 'swing:decision:served:v1:capital:US500');
    assert.equal(cfg.scheduledLookServedKey(null, 'BTCUSDT'), 'swing:decision:served:v1:bitget:BTCUSDT');
});

test('flat cooldown clamp bounds scale with the cadence: at least one day plus jitter, at most a week', () => {
    assert.equal(cfg.HOLD_COOLDOWN_MIN_MINUTES, 1470);
    assert.equal(cfg.HOLD_COOLDOWN_MAX_MINUTES, 7 * 1440);
});
