// Write-through persistence for swing positions. Closed positions are the
// authoritative, stably-keyed records (Bitget assigns a position id and final
// pnl), so we upsert them keyed by that id, attributing the leverage we
// captured at execution time (ground truth) over anything the broker reports.
import { pickCapturedLeverage, type CapturedLeverage, type PositionWindow } from '../analytics';
import { isSwingPgConfigured, upsertSwingPosition, insertSwingAccountSnapshot } from './pg';

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

// Persist a batch of closed position windows. Best-effort: never throws into
// the caller's path (a read endpoint shouldn't fail because a mirror write did).
export async function syncSwingClosedPositions(
    platform: string,
    windows: PositionWindow[],
    capturedLeverages?: CapturedLeverage[] | null,
): Promise<void> {
    if (!isSwingPgConfigured() || !windows?.length) return;
    for (const w of windows) {
        // only persist actually-closed positions here (stable id + final pnl)
        if (!w.exitTimestamp) continue;
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
        } catch (err) {
            console.error(`Failed to persist swing position ${w.id}:`, err);
        }
    }
}
