function safeUtcHour(value: number | undefined): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    return Math.max(0, Math.min(23, Math.floor(n)));
}

export function minutesUntilNextUtcRollover(nowMs = Date.now(), rolloverHourUtc = 0): number {
    const ts = Number(nowMs);
    if (!Number.isFinite(ts)) return Number.POSITIVE_INFINITY;

    const hour = safeUtcHour(rolloverHourUtc);
    const now = new Date(ts);
    const nextRolloverMs = Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        hour,
        0,
        0,
        0,
    );
    const targetMs = ts <= nextRolloverMs ? nextRolloverMs : nextRolloverMs + 24 * 60 * 60 * 1000;
    return Math.max(0, (targetMs - ts) / 60_000);
}

export function isWithinPreRolloverWindow(nowMs = Date.now(), windowMinutes = 0, rolloverHourUtc = 0): boolean {
    const window = Math.max(0, Number(windowMinutes) || 0);
    if (!(window > 0)) return false;
    return minutesUntilNextUtcRollover(nowMs, rolloverHourUtc) <= window;
}
