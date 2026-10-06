// KV snapshot of the wake-watcher's work list.
//
// The watcher (pages/api/swing/wake-watch.ts, cron every WAKE_WATCH_TICK_MINUTES)
// used to run its Postgres SELECTs unconditionally. Nothing else in the repo
// reads these lists, and they are effectively static config between writes —
// the watcher compares the bands against a LIVE VENUE price (Bitget ticker /
// Capital quote), never against anything stored. So a round trip to Neon per
// tick bought nothing except a compute that could never scale to zero: 6.00
// CU-hours a day, 100% of the clock, ~all idle.
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
//     to exactly the uncached behaviour, never to a missed wake.
//
// TTL: 12 hours. Invalidation is the version bump, not the clock — every write
// that matters bumps, and the snapshot is rebuilt while the compute is still
// awake from that write: on the :05 watcher tick after an analyze cron firing
// (vercel.json phase), and at the end of any watcher run that fired or wrote
// (wake-watch.ts). The TTL is ONLY the backstop for a bump that never landed,
// and it sets a floor on Neon wakes: every expiry outside an analyze window
// is a cold start billed for at least five idle minutes. It was 900s while
// analyze ran every 15 minutes and refreshed the snapshot as a side effect;
// since the analyze schedule went daily (2026-10-06) a 900s TTL would itself
// have woken the compute every other watcher tick. Upkeep-only cron ticks do
// not bump, so the snapshot is normally rebuilt only after each venue's daily
// look (~00:05 and ~08:05 UTC): 12h then expires once a day outside a window
// (~20:05), where 6h would expire three times. The price: a bump lost to a KV
// hiccup can leave the watcher on a stale list for up to twelve hours — a
// band not watched, a new position's close not detected (its bracket still
// protects it). Every lost bump is logged ('[wake-work] version bump failed');
// INCR of swing:wake-work:v forces a rebuild on the next tick.
import { kvMGetJson, kvSetJson } from '../kv';
import {
    listSwingAiCooldownsWithWakeBands,
    listSwingBreakTriggers,
    listSwingInPositionThreads,
    listSwingPendingEntryThreads,
    swingCooldownClaimIsLive,
    type SwingAiCooldownRow,
    type SwingBreakTriggerRow,
} from './pg';
import { WAKE_WORK_VERSION_KEY } from './wakeWorkVersion';

export const WAKE_WORK_SNAPSHOT_KEY = 'swing:wake-work:snap';
export const WAKE_WORK_SNAPSHOT_TTL_SECONDS = 12 * 3600;

export type SwingInPositionThreadRow = {
    platform: string;
    symbol: string;
    wakeAbove: number | null;
    wakeBelow: number | null;
};

// A resting entry the pipeline believes is live on the venue. The watcher
// reconciles these against venue reality (filled / vanished) and withdraws
// Capital ones before a session decision window — upkeep the 15-minute analyze
// cron used to do as a side effect of ticking.
export type SwingPendingEntryThreadRow = {
    platform: string;
    symbol: string;
};

type WakeWorkSnapshot = {
    v: number;
    ts: number;
    bands: SwingAiCooldownRow[];
    triggers: SwingBreakTriggerRow[];
    threads: SwingInPositionThreadRow[];
    pendingEntries: SwingPendingEntryThreadRow[];
};

export type WakeWork = {
    // Bands already filtered to those NOT under a live claim lease — the
    // contract the watcher had when the predicate lived in SQL.
    bands: SwingAiCooldownRow[];
    triggers: SwingBreakTriggerRow[];
    threads: SwingInPositionThreadRow[];
    pendingEntries: SwingPendingEntryThreadRow[];
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
        Array.isArray(row.threads) &&
        // Absent on snapshots written before pending entries joined the list:
        // those are unusable, so the first tick after deploy re-reads.
        Array.isArray(row.pendingEntries)
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

    // One MGET for both keys — the watcher runs 144x/day and Upstash bills
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
            pendingEntries: snapshot.pendingEntries,
            source: 'kv',
        };
    }

    // Miss. Read the version BEFORE the queries so a write landing while they
    // are in flight loses the race and invalidates the snapshot we are about
    // to store, rather than being swallowed by it.
    const versionAtRead = version;

    const [bands, triggers, threads, pendingEntries] = await Promise.all([
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
        listSwingPendingEntryThreads().catch((err) => {
            console.warn('[wake-watch] pending-entry thread list failed:', err);
            return null;
        }),
    ]);

    // A partial read must not be cached: storing [] for a list that merely
    // failed would hide real wake work for a whole TTL. Serve what we got this
    // tick and re-read next tick.
    if (bands === null || triggers === null || threads === null || pendingEntries === null) {
        return {
            bands: unclaimedWakeBands(bands || [], nowMs),
            triggers: triggers || [],
            threads: threads || [],
            pendingEntries: pendingEntries || [],
            source: 'pg',
        };
    }

    try {
        await kvSetJson<WakeWorkSnapshot>(
            WAKE_WORK_SNAPSHOT_KEY,
            { v: versionAtRead, ts: nowMs, bands, triggers, threads, pendingEntries },
            WAKE_WORK_SNAPSHOT_TTL_SECONDS,
        );
    } catch (err) {
        console.warn('[wake-work] snapshot write failed:', err);
    }

    return { bands: unclaimedWakeBands(bands, nowMs), triggers, threads, pendingEntries, source: 'pg' };
}
