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

type LastScanMarker = { ts: number };

function key(platform: string, symbol: string): string {
    return `${KEY_PREFIX}:${String(platform || 'bitget').toLowerCase()}:${symbol.toUpperCase()}`;
}

// Best-effort, never throws — a lost marker only costs UI freshness info.
export async function recordSwingLastScan(platform: string, symbol: string, ts = Date.now()): Promise<void> {
    try {
        await kvSetJson<LastScanMarker>(key(platform, symbol), { ts }, TTL_SECONDS);
    } catch (err) {
        console.warn(`last-scan marker write failed for ${symbol}:`, err);
    }
}

export async function readSwingLastScan(platform: string, symbol: string): Promise<number | null> {
    try {
        const marker = await kvGetJson<LastScanMarker>(key(platform, symbol));
        const ts = Number(marker?.ts);
        return Number.isFinite(ts) && ts > 0 ? ts : null;
    } catch {
        return null;
    }
}
