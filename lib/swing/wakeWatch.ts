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
