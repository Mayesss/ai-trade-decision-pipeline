// The scheduled-look cadence is pinned to 'primary' (every 4H close) for the
// test run (test/harness/setup-env.ts) so the fixtures' frozen clocks keep
// meaning what they meant. This file pins the DAILY cadence — the production
// default since 2026-09-16 — as a pure function of the clock.
//
// DECISION_CADENCE and the cooldown bounds are module-level consts captured at
// import time, so the env is stubbed BEFORE a fresh dynamic import.

import assert from 'node:assert/strict';
import { beforeAll, test, vi } from 'vitest';

let cfg: typeof import('../../../lib/swing/decisionConfig');

beforeAll(async () => {
    vi.stubEnv('SWING_DECISION_CADENCE', '1D');
    vi.stubEnv('SWING_DECISION_HOUR_UTC_BITGET', undefined);
    vi.stubEnv('SWING_DECISION_HOUR_UTC_CAPITAL', undefined);
    vi.stubEnv('SWING_AI_COOLDOWN_MIN_MIN', undefined);
    vi.stubEnv('SWING_AI_COOLDOWN_MAX_MIN', undefined);
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
});

test('isDailyDecisionTime fires inside the jitter tolerance around the venue hour and nowhere else', () => {
    assert.equal(cfg.isDailyDecisionTime('bitget', at('2026-09-16T00:00:00Z')), true);
    assert.equal(cfg.isDailyDecisionTime('bitget', at('2026-09-16T00:02:00Z')), true);
    assert.equal(cfg.isDailyDecisionTime('bitget', at('2026-09-16T23:58:30Z')), true); // wrap across midnight
    assert.equal(cfg.isDailyDecisionTime('bitget', at('2026-09-16T00:03:00Z')), false);
    assert.equal(cfg.isDailyDecisionTime('bitget', at('2026-09-16T04:00:00Z')), false); // a 4H close, not THE daily one
    assert.equal(cfg.isDailyDecisionTime('bitget', at('2026-09-16T12:00:00Z')), false);

    assert.equal(cfg.isDailyDecisionTime('capital', at('2026-09-16T08:00:00Z')), true);
    assert.equal(cfg.isDailyDecisionTime('capital', at('2026-09-16T07:58:00Z')), true);
    assert.equal(cfg.isDailyDecisionTime('capital', at('2026-09-16T00:00:00Z')), false);
    assert.equal(cfg.isDailyDecisionTime('capital', at('2026-09-16T16:00:00Z')), false);
});

test('flat cooldown clamp bounds scale with the cadence: at least one day plus jitter, at most a week', () => {
    assert.equal(cfg.HOLD_COOLDOWN_MIN_MINUTES, 1470);
    assert.equal(cfg.HOLD_COOLDOWN_MAX_MINUTES, 7 * 1440);
});
