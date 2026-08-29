// Mechanical entry on CONFIRMED wake-band fires (native — always on).
// Backed by the 2026-08-29 replay of 60 days of wake fires:
// the AI converted its own sustained-confirmed breakout plans at 3.3% (4/121)
// while re-arming "retest" bands in endless HOLD chains, and the only
// wake-entry policy with positive expectancy was: enter ONLY
// sustained-confirmed fires, hold a WIDE disaster stop (tight stops and fixed
// 2-ATR targets both destroyed the cohort), let the failed-break watch (a
// primary bar CLOSING back through the level) own the exit — the win rate is
// ~15% and the profit is a fat right tail, so uncapped winners are the edge.
//
// So: when a flat cooldown wake fires already CONFIRMED (price held beyond
// the band for the AI's own sustain window, or extended >=0.5 primary-ATR by
// force), the analyze route skips the AI call and executes this synthetic
// decision through the exact same sanitize/size/execute/persist pipeline an
// AI decision takes. Instant-touch fires (no sustain window) still go to the
// AI — mechanically entering those lost money in the same replay.
//
// The AI keeps managing the position from the next tick on (stateless
// "adopted mid-life" thread), and the code-side hard constraints in
// postprocessDecision (trend guard, re-entry cooldown, base gates) still
// apply — only the micro_entry_ok timing block is bypassed, because the
// confirmation IS the timing evidence and momentum timing is routinely false
// in the first minutes of a real break.

// Sizing: mechanical wake entries use the repo's standard fixed-fractional
// risk sizing (SWING_RISK_EQUITY_PCT via resolveRiskBasedSizing), exactly like
// any AI-decided entry — no special fraction (user decision 2026-08-29).

// Disaster stop distance, in primary ATR. NOT "just inside the level": a stop
// at the level is swept by the ordinary post-break retest. The replayed
// policy survives retests and exits on the failed-break CLOSE; this stop only
// bounds the gap/crash case. Sanitizer window is 1–3 ATR, so 1.5 passes.
export const WAKE_AUTO_ENTRY_SL_ATR = 1.5;

// Resting TP backstop, in primary ATR. The edge in the replay was entirely in
// uncapped multi-day winners (a 2-ATR target flipped the cohort negative), so
// this sits far away — inside the sanitizer's 10-ATR clamp but beyond any
// 7-day outcome observed except the single best trend. The AI amends or
// closes long before this fills; it exists so the position is never
// TP-naked on the venue.
export const WAKE_AUTO_ENTRY_TP_ATR = 8;

// Chase guard: max extension beyond the broken level (in primary ATR) at
// which the mechanical entry still fires. The replay cohort entered within
// ~a minute of confirmation, near the level; a fire discovered LATE (watcher
// outage, gate-blocked retries) can sit several ATR past it. Geometry pins
// the cap: the stop is entry − WAKE_AUTO_ENTRY_SL_ATR, so any entry more
// than (SL_ATR − 0.5) beyond the level parks the stop at/inside the level —
// the sweep-vulnerable placement the same replay showed losing. Beyond the
// cap the builder refuses and the caller falls back to the normal AI call,
// whose retest protocol owns exactly this situation.
export const WAKE_AUTO_ENTRY_MAX_EXTENSION_ATR = 1.0;

export type WakeAutoEntryParams = {
    crossed: 'above' | 'below';
    level: number;
    note: string | null;
    sustainedMinutes: number | null;
    breakExtensionAtr: number | null;
    price: number;
    primaryAtr: number | null;
};

// The synthetic raw decision, shaped exactly like an AI response so it rides
// postprocessDecision and every sanitizer unchanged. Returns null when the
// inputs cannot anchor a stop (no usable ATR/price) — the caller then falls
// back to the normal AI call rather than entering unprotected.
export function buildWakeAutoEntryDecision(params: WakeAutoEntryParams): Record<string, unknown> | null {
    const price = Number(params.price);
    const atr = Number(params.primaryAtr);
    const level = Number(params.level);
    if (!(Number.isFinite(price) && price > 0)) return null;
    if (!(Number.isFinite(atr) && atr > 0)) return null;
    if (!(Number.isFinite(level) && level > 0)) return null;
    const dir = params.crossed === 'above' ? 1 : -1;
    // Chase guard (see WAKE_AUTO_ENTRY_MAX_EXTENSION_ATR): price already too
    // far past the level -> no mechanical entry, the AI decides instead.
    const extensionAtr = (dir * (price - level)) / atr;
    if (extensionAtr > WAKE_AUTO_ENTRY_MAX_EXTENSION_ATR) return null;
    const stop = price - dir * WAKE_AUTO_ENTRY_SL_ATR * atr;
    const target = price + dir * WAKE_AUTO_ENTRY_TP_ATR * atr;
    if (!(stop > 0 && target > 0)) return null;
    const confirmedVia =
        params.breakExtensionAtr != null
            ? `extension ${params.breakExtensionAtr.toFixed(2)} ATR beyond the band`
            : `held ${params.sustainedMinutes ?? '?'}m beyond the band`;
    return {
        action: params.crossed === 'above' ? 'BUY' : 'SELL',
        summary: `Mechanical entry on confirmed ${params.crossed === 'above' ? 'breakout' : 'breakdown'} of ${level} (${confirmedVia}).`,
        reason:
            `wake_auto_entry: the sustained wake band at ${level} fired confirmed (${confirmedVia}). ` +
            `Policy: enter in break direction at standard risk, disaster stop ${WAKE_AUTO_ENTRY_SL_ATR} ATR, ` +
            `far TP backstop ${WAKE_AUTO_ENTRY_TP_ATR} ATR, exit owned by the failed-break watch at the level.` +
            (params.note ? ` Band plan: ${params.note}` : ''),
        exit_size_pct: null,
        take_profit_price: target,
        stop_loss_price: stop,
        entry_limit_price: null,
        // Arms the failed-break watch: a primary bar CLOSING back through the
        // broken level wakes the AI with market.failed_break (default: CLOSE).
        entry_trigger_price: level,
        cooldown_minutes: null,
        cooldown_wake_above: null,
        cooldown_wake_below: null,
        cooldown_wake_note: null,
        cooldown_wake_sustain_minutes: null,
        leverage: null,
    };
}
