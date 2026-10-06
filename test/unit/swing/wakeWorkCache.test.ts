import assert from 'node:assert/strict';
import { test } from 'vitest';

import {
    unclaimedWakeBands,
    wakeWorkSnapshotUsable,
    WAKE_WORK_SNAPSHOT_TTL_SECONDS,
} from '../../../lib/swing/wakeWorkCache';
import type { SwingAiCooldownRow } from '../../../lib/swing/pg';

const NOW = 1_700_000_000_000;

const band = (symbol: string, claimedUntilMs: number | null): SwingAiCooldownRow => ({
    platform: 'bitget',
    symbol,
    claimedUntilMs,
    untilMs: NOW + 3_600_000,
    wakeAbove: 100,
    wakeBelow: null,
    wakeNote: null,
    setAtMs: NOW - 60_000,
    confirmMinutes: null,
    touchSide: null,
    touchStartedMs: null,
    touchExtreme: null,
    atr: null,
    sweeps: [],
    reclaimLookedAtMs: null,
    gateHeldAtMs: null,
});

const snap = (over: Record<string, unknown> = {}) => ({
    v: 7,
    ts: NOW,
    bands: [],
    triggers: [],
    threads: [],
    pendingEntries: [],
    ...over,
});

// --- claim-lease filter (applied at READ time, cached rows included) --------

test('unclaimed band is work', () => {
    assert.equal(unclaimedWakeBands([band('BTCUSDT', null)], NOW).length, 1);
});

test('band under a live claim lease is withheld', () => {
    assert.equal(unclaimedWakeBands([band('BTCUSDT', NOW + 60_000)], NOW).length, 0);
});

// The whole reason the predicate moved out of SQL: a crashed analyze run's row
// must become workable again the instant its lease lapses, WITHOUT any write to
// invalidate the snapshot it is cached in.
test('a lapsed lease re-arms a cached band with no invalidation', () => {
    const rows = [band('BTCUSDT', NOW + 60_000)];
    assert.equal(unclaimedWakeBands(rows, NOW).length, 0, 'held while the lease is live');
    assert.equal(unclaimedWakeBands(rows, NOW + 60_001).length, 1, 're-armed the moment it lapses');
});

test('lease expiring exactly now is already lapsed', () => {
    assert.equal(unclaimedWakeBands([band('BTCUSDT', NOW)], NOW + 1).length, 1);
});

// --- snapshot freshness ----------------------------------------------------

test('snapshot at the current version, inside the TTL, serves', () => {
    assert.equal(wakeWorkSnapshotUsable(snap(), 7, NOW + 1_000), true);
});

test('a writer bump (version moved) invalidates the snapshot', () => {
    assert.equal(wakeWorkSnapshotUsable(snap(), 8, NOW + 1_000), false);
});

// The backstop for a bump that never landed because KV was briefly down.
test('TTL expiry invalidates even at a matching version', () => {
    const past = WAKE_WORK_SNAPSHOT_TTL_SECONDS * 1000 + 1;
    assert.equal(wakeWorkSnapshotUsable(snap(), 7, NOW + past), false);
    assert.equal(wakeWorkSnapshotUsable(snap(), 7, NOW + past - 2), true);
});

test('a missing or malformed snapshot never serves', () => {
    assert.equal(wakeWorkSnapshotUsable(null, 7, NOW), false);
    assert.equal(wakeWorkSnapshotUsable({ v: 7, ts: NOW }, 7, NOW), false, 'no lists');
    assert.equal(wakeWorkSnapshotUsable({ v: 7, ts: NOW, bands: [], triggers: [] }, 7, NOW), false);
});

// Snapshots written before pending entries joined the work list must not
// serve: the watcher would read "no resting entries" off a list that was
// never loaded and skip the fill / vanished-order / session-window upkeep.
test('a snapshot without the pending-entry list (pre-2026-10-06 shape) never serves', () => {
    const { pendingEntries: _omitted, ...legacy } = snap();
    assert.equal(wakeWorkSnapshotUsable(legacy, 7, NOW), false);
});

// The TTL is a backstop for a lost bump AND a floor on Neon wakes: every expiry
// outside an analyze window is a cold start. Against the 10-minute watcher it
// must span many ticks — the old 900s would have re-read Postgres on every
// other tick once analyze stopped refreshing the snapshot every 15 minutes.
test('TTL spans hours of watcher ticks, not one analyze cycle', () => {
    assert.ok(WAKE_WORK_SNAPSHOT_TTL_SECONDS >= 3 * 3600, String(WAKE_WORK_SNAPSHOT_TTL_SECONDS));
});

// A first-ever read sees no version key at all; kvMGetJson yields null -> 0.
test('absent version key matches a snapshot stamped 0', () => {
    assert.equal(wakeWorkSnapshotUsable(snap({ v: 0 }), 0, NOW), true);
    assert.equal(wakeWorkSnapshotUsable(snap({ v: 1 }), 0, NOW), false);
});
