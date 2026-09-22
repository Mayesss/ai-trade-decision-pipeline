// KV snapshot of the 1-minute wake-watcher's work list.
//
// The watcher (pages/api/swing/wake-watch.ts, cron `* * * * *`) used to run
// three unconditional Postgres SELECTs every minute. Nothing else in the repo
// reads those three lists, and they are effectively static config between
// analyze runs — the watcher compares the bands against a LIVE VENUE price
// (Bitget ticker / Capital quote), never against anything stored. So a
// per-minute round trip to Neon bought nothing except a compute that could
// never scale to zero: 6.00 CU-hours a day, 100% of the clock, ~all idle.
//
// Correctness model — the DB stays the source of truth, KV only skips reads:
//   * every writer that can change what the watcher would see bumps a version
//     counter (lib/swing/wakeWorkVersion.ts); a snapshot is used only while its
//     stamped version still matches;
//   * the claim-lease test is applied here, against the current clock, on
//     cached rows too — so a crashed analyze run's wake re-arms the moment its
//     lease lapses, with no cache round trip and no added latency;
//   * a TTL backstop bounds staleness from a bump that failed to land (KV
//     hiccup) to one window rather than forever;
//   * every KV failure path falls through to Postgres. KV being down degrades
//     to exactly today's behaviour, never to a missed wake.
//
// TTL: 900s, deliberately LONGER than the cache is usually allowed to live.
// Invalidation is the version bump, not the clock: every 15-minute analyze
// cycle writes cooldowns/threads and therefore bumps, so in practice the
// snapshot is rebuilt once per cycle, on the watcher minute right after
// analyze — while the compute is awake for analyze anyway, so the refresh
// costs no extra wake window. A shorter TTL would force EXTRA Postgres reads
// on an unaligned phase and wake the compute for nothing, which is the whole
// problem this module exists to fix. The TTL is only the backstop for a bump
// that never landed (KV hiccup), and 15 minutes bounds that at one analyze
// cycle — i.e. a failure degrades the watcher to the 15-minute tick it was
// built to beat, never to something worse.
import { kvMGetJson, kvSetJson } from '../kv';
import {
    listSwingAiCooldownsWithWakeBands,
    listSwingBreakTriggers,
    listSwingInPositionThreads,
    swingCooldownClaimIsLive,
    type SwingAiCooldownRow,
    type SwingBreakTriggerRow,
} from './pg';
import { WAKE_WORK_VERSION_KEY } from './wakeWorkVersion';

export const WAKE_WORK_SNAPSHOT_KEY = 'swing:wake-work:snap';
export const WAKE_WORK_SNAPSHOT_TTL_SECONDS = 900;

export type SwingInPositionThreadRow = {
    platform: string;
    symbol: string;
    wakeAbove: number | null;
    wakeBelow: number | null;
};

type WakeWorkSnapshot = {
    v: number;
    ts: number;
    bands: SwingAiCooldownRow[];
    triggers: SwingBreakTriggerRow[];
    threads: SwingInPositionThreadRow[];
};

export type WakeWork = {
    // Bands already filtered to those NOT under a live claim lease — the
    // contract the watcher had when the predicate lived in SQL.
    bands: SwingAiCooldownRow[];
    triggers: SwingBreakTriggerRow[];
    threads: SwingInPositionThreadRow[];
    source: 'kv' | 'pg';
};

function isSnapshot(value: unknown): value is WakeWorkSnapshot {
    if (!value || typeof value !== 'object') return false;
    const row = value as Record<string, unknown>;
    return (
        Number.isFinite(Number(row.v)) &&
        Number.isFinite(Number(row.ts)) &&
        Array.isArray(row.bands) &&
        Array.isArray(row.triggers) &&
        Array.isArray(row.threads)
    );
}

// Pure. The claim-lease filter that used to be a SQL predicate. Applied on
// every read, cached or fresh, so a lapsed lease re-arms a cached row with no
// invalidation and no round trip.
export function unclaimedWakeBands(bands: SwingAiCooldownRow[], nowMs: number): SwingAiCooldownRow[] {
    return bands.filter((row) => !swingCooldownClaimIsLive(row, nowMs));
}

// Pure. A snapshot may serve only while it is stamped with the current version
// AND inside the TTL backstop. Version mismatch means a writer moved something
// since; TTL expiry covers a bump that never landed.
export function wakeWorkSnapshotUsable(
    snapshot: unknown,
    version: number,
    nowMs: number,
): snapshot is WakeWorkSnapshot {
    if (!isSnapshot(snapshot)) return false;
    if (Number(snapshot.v) !== version) return false;
    return nowMs - Number(snapshot.ts) < WAKE_WORK_SNAPSHOT_TTL_SECONDS * 1000;
}

export async function loadWakeWork(nowMs: number = Date.now()): Promise<WakeWork> {
    let version = 0;
    let snapshot: WakeWorkSnapshot | null = null;

    // One MGET for both keys — the watcher runs 1440x/day and Upstash bills
    // per command (see docs/kv-cost-reduction.md).
    try {
        const [rawVersion, rawSnapshot] = await kvMGetJson<unknown>([
            WAKE_WORK_VERSION_KEY,
            WAKE_WORK_SNAPSHOT_KEY,
        ]);
        version = Number(rawVersion) || 0;
        if (isSnapshot(rawSnapshot)) snapshot = rawSnapshot;
    } catch (err) {
        console.warn('[wake-work] snapshot read failed; falling back to pg:', err);
    }

    if (snapshot && wakeWorkSnapshotUsable(snapshot, version, nowMs)) {
        return {
            bands: unclaimedWakeBands(snapshot.bands, nowMs),
            triggers: snapshot.triggers,
            threads: snapshot.threads,
            source: 'kv',
        };
    }

    // Miss. Read the version BEFORE the queries so a write landing while they
    // are in flight loses the race and invalidates the snapshot we are about
    // to store, rather than being swallowed by it.
    const versionAtRead = version;

    const [bands, triggers, threads] = await Promise.all([
        listSwingAiCooldownsWithWakeBands().catch((err) => {
            console.warn('[wake-watch] cooldown list failed:', err);
            return null;
        }),
        listSwingBreakTriggers().catch((err) => {
            console.warn('[wake-watch] break-trigger list failed:', err);
            return null;
        }),
        listSwingInPositionThreads().catch((err) => {
            console.warn('[wake-watch] in-position thread list failed:', err);
            return null;
        }),
    ]);

    // A partial read must not be cached: storing [] for a list that merely
    // failed would hide real wake work for a whole TTL. Serve what we got this
    // minute and re-read next minute.
    if (bands === null || triggers === null || threads === null) {
        return {
            bands: unclaimedWakeBands(bands || [], nowMs),
            triggers: triggers || [],
            threads: threads || [],
            source: 'pg',
        };
    }

    try {
        await kvSetJson<WakeWorkSnapshot>(
            WAKE_WORK_SNAPSHOT_KEY,
            { v: versionAtRead, ts: nowMs, bands, triggers, threads },
            WAKE_WORK_SNAPSHOT_TTL_SECONDS,
        );
    } catch (err) {
        console.warn('[wake-work] snapshot write failed:', err);
    }

    return { bands: unclaimedWakeBands(bands, nowMs), triggers, threads, source: 'pg' };
}
