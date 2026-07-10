// Per-symbol "last analyze scan" marker. Quarter-tick scans deliberately do NOT
// persist decision rows (they'd be pure noise in the history), which left the
// dashboard unable to show that the 15m cadence is alive between hourly rows.
// This tiny KV marker records "the cron looked at this symbol at T" on EVERY
// automation tick — including ones that end in an unpersisted skip — so the UI
// can surface scan freshness without polluting the decision history.
import { kvGetJson, kvSetJson } from '../kv';

const KEY_PREFIX = 'swing:lastScan:v1';
// Long enough to survive weekend market closures without the marker vanishing.
const TTL_SECONDS = 7 * 24 * 60 * 60;

export type LastScanMarker = {
    ts: number;
    // Set when the scan ended in an UNPERSISTED quarter-tick skip — the gate
    // stage and skip reason that would have been a decision row on an hourly
    // tick. Absent when the tick proceeded to a real (persisted) decision.
    stage?: string;
    reason?: string;
};

function key(platform: string, symbol: string): string {
    return `${KEY_PREFIX}:${String(platform || 'bitget').toLowerCase()}:${symbol.toUpperCase()}`;
}

// Best-effort, never throws — a lost marker only costs UI freshness info.
export async function recordSwingLastScan(
    platform: string,
    symbol: string,
    info?: { stage?: string; reason?: string },
): Promise<void> {
    try {
        await kvSetJson<LastScanMarker>(
            key(platform, symbol),
            { ts: Date.now(), ...(info?.stage ? { stage: info.stage } : {}), ...(info?.reason ? { reason: info.reason } : {}) },
            TTL_SECONDS,
        );
    } catch (err) {
        console.warn(`last-scan marker write failed for ${symbol}:`, err);
    }
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
