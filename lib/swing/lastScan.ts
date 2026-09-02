// Per-symbol "last analyze scan" marker. Quarter-tick scans deliberately do NOT
// persist decision rows (they'd be pure noise in the history), which left the
// dashboard unable to show that the 15m cadence is alive between hourly rows.
// This tiny KV marker records "the cron looked at this symbol at T" on EVERY
// automation tick — including ones that end in an unpersisted skip — so the UI
// can surface scan freshness without polluting the decision history.
import { kvExpire, kvGetJson, kvListPushJson, kvListRangeJson, kvListTrim, kvMGetJson, kvSetJson } from '../kv';

const KEY_PREFIX = 'swing:lastScan:v1';
// Long enough to survive weekend market closures without the marker vanishing.
const TTL_SECONDS = 7 * 24 * 60 * 60;

// Rolling per-symbol tick log backing the dashboard decision timeline. The
// lastScan marker above only keeps the LATEST tick — this list keeps the recent
// ones so quarter-tick gate skips (never persisted as decision rows) are still
// selectable on the timeline. Capped ring buffer: ~8 writes/hour worst case
// (every tick writes once bare + once staged), so 400 entries ≈ 2 days.
const TICK_LOG_KEY_PREFIX = 'swing:scanTicks:v1';
const TICK_LOG_MAX_ENTRIES = 400;

export type LastScanMarker = {
    ts: number;
    // Set when the scan ended in an UNPERSISTED quarter-tick skip — the gate
    // stage and skip reason that would have been a decision row on an hourly
    // tick. Absent when the tick proceeded to a real (persisted) decision.
    stage?: string;
    reason?: string;
};

export type ScanTickEntry = LastScanMarker;

function key(platform: string, symbol: string): string {
    return `${KEY_PREFIX}:${String(platform || 'bitget').toLowerCase()}:${symbol.toUpperCase()}`;
}

function tickLogKey(platform: string, symbol: string): string {
    return `${TICK_LOG_KEY_PREFIX}:${String(platform || 'bitget').toLowerCase()}:${symbol.toUpperCase()}`;
}

// Ring-buffer housekeeping (LTRIM + EXPIRE) is amortized to roughly once an
// hour per symbol instead of once per write, which is what a tick used to pay
// twice over. It is observably identical: readers take LRANGE 0..399, so a list
// that briefly holds 404 entries still hands back the same newest 400, and the
// TTL is 7 days against a cadence that refreshes it every hour. The rule is the
// tick's own minute-of-hour so it stays deterministic (contract snapshots record
// every KV command) — the :00 cron firing of each hour does the housekeeping.
function housekeepingDue(tsMs: number): boolean {
    return new Date(tsMs).getUTCMinutes() < 15;
}

async function writeScanMarker(
    platform: string,
    symbol: string,
    entry: LastScanMarker,
    opts: { housekeep: boolean },
): Promise<void> {
    try {
        await kvSetJson<LastScanMarker>(key(platform, symbol), entry, TTL_SECONDS);
    } catch (err) {
        console.warn(`last-scan marker write failed for ${symbol}:`, err);
    }
    // Append the same entry to the rolling tick log. A tick can land here twice
    // (bare at scan start, staged again on skip); the timeline reader dedupes by
    // tick bucket, preferring the staged entry. Separate try so a log failure
    // never costs the marker (and vice versa).
    try {
        const logKey = tickLogKey(platform, symbol);
        await kvListPushJson<ScanTickEntry>(logKey, entry);
        if (opts.housekeep) {
            await kvListTrim(logKey, 0, TICK_LOG_MAX_ENTRIES - 1);
            await kvExpire(logKey, TTL_SECONDS);
        }
    } catch (err) {
        console.warn(`scan tick log write failed for ${symbol}:`, err);
    }
}

// Tick-start stamp: "the cron picked this symbol up". Never does the ring-buffer
// housekeeping — the tick's outcome call below lands seconds later and owns it.
export async function stampSwingScanStarted(platform: string, symbol: string): Promise<void> {
    return writeScanMarker(platform, symbol, { ts: Date.now() }, { housekeep: false });
}

// Tick-outcome stamp, carrying the gate stage that would have been a decision
// row on an hourly tick. Best-effort, never throws — a lost marker only costs
// UI freshness info.
export async function recordSwingLastScan(
    platform: string,
    symbol: string,
    info?: { stage?: string; reason?: string },
): Promise<void> {
    const entry: LastScanMarker = {
        ts: Date.now(),
        ...(info?.stage ? { stage: info.stage } : {}),
        ...(info?.reason ? { reason: info.reason } : {}),
    };
    return writeScanMarker(platform, symbol, entry, { housekeep: housekeepingDue(entry.ts) });
}

// Every symbol's marker in ONE command. MGET is a single billed Upstash
// command whatever the key count, so summary builds (25 symbols, four range
// blobs) read markers here instead of one GET per symbol.
export async function readSwingLastScanMany(
    pairs: Array<{ platform: string; symbol: string }>,
): Promise<Array<LastScanMarker | null>> {
    if (!pairs.length) return [];
    return kvMGetJson<LastScanMarker>(pairs.map((pair) => key(pair.platform, pair.symbol)));
}

export async function readSwingLastScan(platform: string, symbol: string): Promise<LastScanMarker | null> {
    try {
        const marker = await kvGetJson<LastScanMarker>(key(platform, symbol));
        const ts = Number(marker?.ts);
        return Number.isFinite(ts) && ts > 0 ? { ...marker, ts } : null;
    } catch {
        return null;
    }
}

// Recent scan ticks, newest first (LPUSH order), filtered to sinceMs. Best-effort:
// returns [] on any failure — the timeline then just shows persisted decisions.
export async function readSwingScanTicks(
    platform: string,
    symbol: string,
    opts?: { sinceMs?: number },
): Promise<ScanTickEntry[]> {
    try {
        const rows = await kvListRangeJson<ScanTickEntry>(
            tickLogKey(platform, symbol),
            0,
            TICK_LOG_MAX_ENTRIES - 1,
        );
        const since = Number(opts?.sinceMs) || 0;
        return rows.filter((row) => {
            const ts = Number(row?.ts);
            return Number.isFinite(ts) && ts > 0 && ts >= since;
        });
    } catch {
        return [];
    }
}
