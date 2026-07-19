// Write-through persistence for swing positions. Closed positions are the
// authoritative, stably-keyed records (Bitget assigns a position id and final
// pnl), so we upsert them keyed by that id, attributing the leverage we
// captured at execution time (ground truth) over anything the broker reports.
import { pickCapturedLeverage, type CapturedLeverage, type PositionWindow } from '../analytics';
import { isSwingPgConfigured, upsertSwingPosition, insertSwingAccountSnapshot } from './pg';
import { maybeEnqueueSwingPostmortem } from './postmortem';
import { kvGetJson, kvSetJson } from '../kv';

// Per-symbol high-water-mark of the latest closed-position exit timestamp we've
// already mirrored to Postgres. Closed positions are immutable, so re-upserting
// the full lookback on every dashboard recompute just burns Neon transfer — we
// only write windows that closed AFTER this mark.
const syncedExitHwmKey = (platform: string, symbol: string) =>
  `swing:synced_exit_hwm:${String(platform || 'bitget').toLowerCase()}:${symbol.toUpperCase()}`;

function finiteNum(value: unknown): number | null {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

// Record a point-in-time account snapshot. Called from the hourly analyze cron
// (a bounded, non-read-path seam — NOT from dashboard reads, which would grow
// this append-only table unbounded). Captures the *current* leverage Bitget
// reports on the open position, so we retain a history of it. Best-effort.
export async function recordSwingAccountSnapshot(params: {
    platform: string;
    symbol: string;
    capturedAtMs: number;
    positionInfo?: { status?: string; leverage?: number | null; available?: unknown; total?: unknown } | null;
    leverageHint?: number | null;
}): Promise<void> {
    if (!isSwingPgConfigured()) return;
    const pos = params.positionInfo;
    const open = pos?.status === 'open';
    const leverage = (open ? finiteNum(pos?.leverage) : null) ?? finiteNum(params.leverageHint);
    try {
        await insertSwingAccountSnapshot({
            platform: params.platform,
            symbol: params.symbol,
            capturedAtMs: params.capturedAtMs,
            leverage,
            available: open ? finiteNum(pos?.available) : null,
            openPosition: open ? pos : null,
        });
    } catch (err) {
        console.error(`Failed to record swing account snapshot for ${params.symbol}:`, err);
    }
}

// Merge persisted (Neon mirror) and live (broker) closed-position windows,
// deduped by stable id — live wins on conflict since the broker is freshest.
// Shared by the dashboard-summary and chart-overlay read paths so both surface a
// just-closed Bitget position without waiting for the mirror to catch up.
export function mergePositionWindows(persisted: PositionWindow[], live: PositionWindow[]): PositionWindow[] {
    const byId = new Map<string, PositionWindow>();
    for (const w of persisted) {
        byId.set(String(w.id || `${w.symbol}-${w.entryTimestamp ?? 'nots'}`), w);
    }
    for (const w of live) {
        byId.set(String(w.id || `${w.symbol}-${w.entryTimestamp ?? 'nots'}`), w);
    }
    return Array.from(byId.values()).sort(
        (a, b) =>
            Number(a.entryTimestamp ?? a.exitTimestamp ?? 0) - Number(b.entryTimestamp ?? b.exitTimestamp ?? 0),
    );
}

// Persist a batch of closed position windows. Best-effort: never throws into
// the caller's path (a read endpoint shouldn't fail because a mirror write did).
export async function syncSwingClosedPositions(
    platform: string,
    windows: PositionWindow[],
    capturedLeverages?: CapturedLeverage[] | null,
): Promise<void> {
    if (!isSwingPgConfigured() || !windows?.length) return;

    // Group by symbol so we can throttle each against its own high-water-mark.
    // (Callers pass one symbol's windows today, but stay correct if that changes.)
    const bySymbol = new Map<string, PositionWindow[]>();
    for (const w of windows) {
        if (!w.exitTimestamp || !w.symbol) continue; // only stable, closed positions
        const arr = bySymbol.get(w.symbol) ?? [];
        arr.push(w);
        bySymbol.set(w.symbol, arr);
    }

    for (const [symbol, symbolWindows] of bySymbol) {
        const hwmKey = syncedExitHwmKey(platform, symbol);
        let hwm = 0;
        try {
            hwm = (await kvGetJson<number>(hwmKey)) ?? 0;
        } catch {
            /* treat as not-yet-synced */
        }

        let maxExit = hwm;
        for (const w of symbolWindows) {
            const exitTs = Number(w.exitTimestamp);
            if (!(exitTs > hwm)) continue; // already mirrored — skip the redundant write
            const captured = pickCapturedLeverage(w.entryTimestamp, capturedLeverages);
            const entryLeverage = captured ?? (typeof w.leverage === 'number' ? w.leverage : null);
            const leverageSource = captured != null ? 'captured' : entryLeverage != null ? 'derived' : null;
            try {
                await upsertSwingPosition(platform, {
                    ...w,
                    status: 'closed',
                    entryLeverage,
                    leverageSource,
                });
                if (exitTs > maxExit) maxExit = exitTs;
                // Freshly-mirrored close (past the HWM ⇒ first sighting) →
                // post-mortem candidate. Idempotent + best-effort inside.
                await maybeEnqueueSwingPostmortem(platform, { ...w, leverage: entryLeverage ?? w.leverage });
            } catch (err) {
                console.error(`Failed to persist swing position ${w.id}:`, err);
            }
        }

        if (maxExit > hwm) {
            try {
                await kvSetJson(hwmKey, maxExit);
            } catch {
                // HWM is best-effort: if it fails we just re-sync next time (no data loss)
            }
        }
    }
}
