// Decision logic for the 1-minute wake-watcher (pages/api/swing/wake-watch):
// pure functions + KV key/type contracts shared with the analyze route. The
// watcher never calls the AI itself — it only decides WHEN to fire the normal
// analyze route early, so a crossed wake band or a violent in-position move is
// acted on within ~a minute instead of waiting for the next 4H bar close.

// Last AI look on a symbol: stamped by analyze after every real AI call so the
// watcher can measure "how far has price moved since the model last saw this
// market" without fetching candles/indicators every minute.
export type WakeWatchRef = {
    price: number;
    atr: number | null;
    ts: number;
};

export const wakeWatchRefKey = (platform: string, symbol: string) =>
    `swing:wakewatch:ref:${String(platform || '').toLowerCase()}:${String(symbol || '').toUpperCase()}`;

// Fired-marker: set immediately before invoking the analyze route so two
// consecutive watcher ticks cannot double-fire the same event while the first
// (slow, AI-bearing) call is still running. TTL outlives any analyze run.
export const wakeWatchFiredKey = (platform: string, symbol: string) =>
    `swing:wakewatch:fired:${String(platform || '').toLowerCase()}:${String(symbol || '').toUpperCase()}`;
export const WAKE_WATCH_FIRED_TTL_SECONDS = 240;

// Same semantics as the analyze cooldown handler: at/beyond either band = wake.
export function wakeBandCrossed(
    price: number | null | undefined,
    wakeAbove: number | null | undefined,
    wakeBelow: number | null | undefined,
): 'above' | 'below' | null {
    const p = Number(price);
    if (!(Number.isFinite(p) && p > 0)) return null;
    if (wakeAbove != null && Number.isFinite(Number(wakeAbove)) && p >= Number(wakeAbove)) return 'above';
    if (wakeBelow != null && Number.isFinite(Number(wakeBelow)) && p <= Number(wakeBelow)) return 'below';
    return null;
}

// ---------------------------------------------------------------------------
// Failed-break watch (swing.break_triggers): on a breakout/breakdown entry the
// model declares the trigger level that justified the trade; if a LATER
// primary bar CLOSES back through it, the break has failed and the model
// should be woken to decide the exit (its own post-mortem lesson). The
// watcher checks this shortly after each primary bar close; the analyze route
// re-detects it on every in-position tick and surfaces market.failed_break.
// ---------------------------------------------------------------------------

// '4H' / '1h' / '15m' / '1D' / '1W' → milliseconds. Null on anything else so
// callers fail quiet (no watch) instead of firing on a bogus window.
export function timeframeToMs(tf: string | null | undefined): number | null {
    const m = String(tf || '')
        .trim()
        .match(/^(\d+)\s*(m|min|h|d|w)$/i);
    if (!m) return null;
    const n = Number(m[1]);
    if (!Number.isFinite(n) || n <= 0) return null;
    const unit = m[2].toLowerCase();
    const unitMs =
        unit === 'w' ? 7 * 24 * 60 * 60_000 : unit === 'd' ? 24 * 60 * 60_000 : unit === 'h' ? 60 * 60_000 : 60_000;
    return n * unitMs;
}

// Last fully CLOSED bar from an ascending [ts, o, h, l, c, ...] candle array
// (both venue fetchers produce this shape; the final row is usually the
// forming bar). Null when nothing usable — fail quiet.
export function lastClosedBar(
    candles: unknown[] | null | undefined,
    tfMs: number,
    nowMs: number,
): { closeTs: number; close: number } | null {
    if (!Array.isArray(candles) || !(Number.isFinite(tfMs) && tfMs > 0)) return null;
    for (let i = candles.length - 1; i >= 0; i--) {
        const row = candles[i] as any[];
        const ts = Number(row?.[0]);
        const close = Number(row?.[4]);
        if (!(Number.isFinite(ts) && ts > 0 && Number.isFinite(close) && close > 0)) continue;
        if (ts + tfMs <= nowMs) return { closeTs: ts + tfMs, close };
    }
    return null;
}

// The lesson's condition: a bar that CLOSED back through the entry trigger.
// long entered on a break above trigger → failed when a bar closes below it;
// short mirrored. The bar must have closed AFTER the entry ("the next primary
// bar"), which callers enforce via closeTs > entryAtMs.
export function breakTriggerFailed(
    side: 'long' | 'short' | string,
    triggerPrice: number | null | undefined,
    barClose: number | null | undefined,
): boolean {
    const trigger = Number(triggerPrice);
    const close = Number(barClose);
    if (!(Number.isFinite(trigger) && trigger > 0 && Number.isFinite(close) && close > 0)) return false;
    if (side === 'long') return close < trigger;
    if (side === 'short') return close > trigger;
    return false;
}

// Watcher throttle: the failed-break condition can only change when a primary
// bar closes, so candle fetches are limited to the first few minutes after a
// boundary instead of every minute of the day.
export function minutesSinceBarBoundary(tfMs: number, nowMs: number): number | null {
    if (!(Number.isFinite(tfMs) && tfMs > 0 && Number.isFinite(nowMs) && nowMs > 0)) return null;
    return (nowMs % tfMs) / 60_000;
}

// In-position emergency: absolute move (either direction) since the last AI
// look, in primary-ATR units. Null when the ref is unusable — the watcher then
// stays quiet and the regular cadence owns the position (fail quiet, not loud:
// a missing ref must not cause per-minute AI calls).
export function emergencyMoveAtr(
    price: number | null | undefined,
    ref: WakeWatchRef | null | undefined,
): number | null {
    const p = Number(price);
    if (!(Number.isFinite(p) && p > 0)) return null;
    const refPrice = Number(ref?.price);
    const atr = Number(ref?.atr);
    if (!(Number.isFinite(refPrice) && refPrice > 0)) return null;
    if (!(Number.isFinite(atr) && atr > 0)) return null;
    return Math.abs(p - refPrice) / atr;
}
