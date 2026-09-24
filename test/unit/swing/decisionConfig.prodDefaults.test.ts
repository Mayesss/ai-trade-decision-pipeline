// The test harness pins several swing thresholds at historical values so the
// fixtures keep meaning what they meant (test/harness/setup-env.ts). This file
// is the one place that asserts what PRODUCTION runs with when no env var is
// set — so a default cannot drift without a test noticing, and a reader can
// find the live numbers without opening Vercel.
//
// Module-level consts capture the env at import time, so the pins are removed
// BEFORE a fresh dynamic import (the module-state trap the swing-fixtures skill
// warns about).

import assert from 'node:assert/strict';
import { beforeAll, test, vi } from 'vitest';

let cfg: typeof import('../../../lib/swing/decisionConfig');

beforeAll(async () => {
    for (const k of [
        'SWING_ENTRY_SL_MIN_ATR',
        'SWING_AMEND_SL_MIN_ATR',
        'SWING_ENTRY_TP_MIN_R',
        'SWING_INPOS_EMERGENCY_MOVE_ATR',
        'SWING_REENTRY_COOLDOWN_MIN',
        'SWING_POSITION_WAKE_MIN_ATR',
        'SWING_DECISION_CADENCE',
        'SWING_DECISION_HOUR_UTC_BITGET',
        'SWING_DECISION_HOUR_UTC_CAPITAL',
    ]) {
        vi.stubEnv(k, undefined);
    }
    vi.resetModules();
    cfg = await import('../../../lib/swing/decisionConfig');
});

test('2026-09-16 horizon loosening: the code defaults are what production runs (docs/alpha-lab-spec.md §11)', () => {
    assert.equal(cfg.ENTRY_SL_MIN_ATR, 3);
    assert.equal(cfg.REENTRY_COOLDOWN_MIN, 1440);
    assert.equal(cfg.POSITION_WAKE_MIN_ATR, 1);
    // Scheduled look once a day per venue (§12), at a 4H close.
    assert.equal(cfg.DECISION_CADENCE, '1D');
    assert.deepEqual(cfg.DECISION_HOUR_UTC, { bitget: 0, capital: 8 });
});

test('2026-09-23 swing geometry: amend floor = entry floor, target >= 1x stop, one emergency threshold', () => {
    assert.equal(cfg.AMEND_SL_MIN_ATR, cfg.ENTRY_SL_MIN_ATR);
    assert.equal(cfg.ENTRY_TP_MIN_R, 1);
    // Read by both analyze and the wake-watcher; the watcher used to default to 1.5.
    assert.equal(cfg.IN_POSITION_EMERGENCY_MOVE_ATR, 3);
});

test('the wake-band floor stays inside the entry floor, and the amend floor inside both', () => {
    // A band must be reachable before the stop is, and an amend floor above the
    // entry floor would refuse every trail. Geometry invariants, not taste.
    assert.ok(cfg.POSITION_WAKE_MIN_ATR <= cfg.ENTRY_SL_MIN_ATR);
    assert.ok(cfg.AMEND_SL_MIN_ATR <= cfg.ENTRY_SL_MIN_ATR);
});
