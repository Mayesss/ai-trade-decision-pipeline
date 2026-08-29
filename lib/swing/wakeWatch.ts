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

// ---------------------------------------------------------------------------
// Wake-plan staleness. A wake note is the model's PLAN for a level ("breakdown
// below X → short check") — a plan has a horizon. For a flat cooldown band the
// horizon is explicit: the AI-chosen cooldown end (until_ms) plus one primary
// candle of grace. A band crossed after that (long venue closure, AI outage —
// GOLD 2026-07-27 fired 3.5h past its cooldown end after a 14h quota blackout
// and was executed as a schedule) is a stale IDEA, not a standing order: it no
// longer grants the off-cadence wake, and if the note reaches the prompt it is
// flagged expired so the model re-derives the setup instead of executing it.
// In-position bands have no until_ms (replace-on-every-look); their staleness
// anchor is set_at + the same grace, applied at prompt-build time only (the
// early look itself stays valuable for management).
// ---------------------------------------------------------------------------

export const WAKE_PLAN_GRACE_MINUTES_DEFAULT = 240; // one 4H primary candle

export function wakePlanGraceMinutes(): number {
    const raw = Number(process.env.SWING_WAKE_PLAN_GRACE_MINUTES ?? WAKE_PLAN_GRACE_MINUTES_DEFAULT);
    return Number.isFinite(raw) && raw >= 0 ? Math.floor(raw) : WAKE_PLAN_GRACE_MINUTES_DEFAULT;
}

// Flat cooldown band: stale once now is past the plan's own horizon. A row
// with no usable until_ms falls back to set_at + grace; with neither, never
// stale (fail open — the prompt's set_minutes_ago still shows the age).
export function flatWakePlanStale(
    untilMs: number | null | undefined,
    setAtMs: number | null | undefined,
    nowMs: number,
): boolean {
    const graceMs = wakePlanGraceMinutes() * 60_000;
    const until = Number(untilMs);
    if (Number.isFinite(until) && until > 0) return nowMs > until + graceMs;
    const setAt = Number(setAtMs);
    if (Number.isFinite(setAt) && setAt > 0) return nowMs > setAt + graceMs;
    return false;
}

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
// Sustained wake confirmation. A flat wake band mostly encodes a breakout/
// breakdown plan, and the first touch of such a level is the moment of MAX
// ambiguity — a stop-run sweep and a real break look identical for the first
// minutes. The model may therefore attach cooldown_wake_sustain_minutes to a
// band: the wake fires only if price is STILL beyond the band that many
// minutes after first touch. A touch that reclaims earlier never wakes the
// model at all — it is recorded as a SWEEP on the cooldown row and handed to
// the model as market.wake_band_sweeps evidence on its next look (a failed
// poke is signal, just not an emergency). Deliberately a PERSISTENCE filter,
// not a delay: "wake me N minutes after touch regardless" would spend a look
// on reclaimed chop and hand back N minutes on real breaks without filtering
// anything. In-position bands are exempt by design — they guard a live
// position, where an instant look is right whether the break is real or fake.
// ---------------------------------------------------------------------------

export const WAKE_SUSTAIN_MIN_MINUTES = 5;
export const WAKE_SUSTAIN_MAX_MINUTES = 60;

// Distance-based early confirmation: a stop-run sweep by definition does not
// TRAVEL far beyond the level (observed sweep depths ≤ ~0.3%), so a break that
// extends this many primary-ATRs beyond the band has proven itself by force —
// fire immediately instead of letting a vertical runner drift away for the
// rest of the time window (SOL 2026-08-03 ran +2.46% during a 20m wait).
export const WAKE_BREAK_CONFIRM_ATR_DEFAULT = 0.5;

export function wakeBreakConfirmAtr(): number {
    const raw = Number(process.env.SWING_WAKE_BREAK_CONFIRM_ATR ?? WAKE_BREAK_CONFIRM_ATR_DEFAULT);
    return Number.isFinite(raw) && raw > 0 ? raw : WAKE_BREAK_CONFIRM_ATR_DEFAULT;
}

export function clampWakeSustainMinutes(raw: unknown): number | null {
    const n = Number(raw);
    if (!Number.isFinite(n) || n <= 0) return null;
    return Math.min(WAKE_SUSTAIN_MAX_MINUTES, Math.max(WAKE_SUSTAIN_MIN_MINUTES, Math.round(n)));
}

export type WakeSweepEvent = {
    side: 'above' | 'below';
    level: number;
    touchedAtMs: number;
    reclaimedAtMs: number;
    extreme: number | null;
};

// ---------------------------------------------------------------------------
// Reclaim wake: a sweep (touch + reclaim before the sustain window) of an
// AI-armed band fires an immediate AI look — the bounce moment becomes
// actionable instead of arriving hours later as stale wake_band_sweeps
// evidence. Judgment-gated ONLY: the 2026-08-29 replay of 236 sweeps showed
// mechanical fades losing (21% win, −0.73R/trade net) — the look exists so
// the model can fade WITH structural confluence, never so code enters.
// ---------------------------------------------------------------------------

// Depth floor: a poke that barely cleared the band is bar noise, not a
// liquidity grab worth an AI call (the TLT case swept one band six times in
// an hour at ~0.005 depth). Measured real sweeps ran up to ~0.3% deep.
export const RECLAIM_WAKE_MIN_DEPTH_ATR = 0.2;

// Analyze-side freshness: the look must happen at the bounce moment. A sweep
// older than this (watcher outage, fire lost) is evidence, not an event.
export const RECLAIM_WAKE_FRESH_MINUTES = 10;

// Shared by the watcher (fire?) and the analyze route (build the look?).
// One reclaim look per cooldown row lifetime (reclaimLookedAtMs one-shot;
// a fresh cooldown upsert resets it — new plan, new budget).
export function reclaimWakeEligible(params: {
    sweep: Pick<WakeSweepEvent, 'level' | 'extreme' | 'reclaimedAtMs'> | null | undefined;
    atr: number | null;
    reclaimLookedAtMs: number | null;
    nowMs: number;
}): boolean {
    const { sweep, atr, reclaimLookedAtMs, nowMs } = params;
    if (!sweep) return false;
    if (reclaimLookedAtMs !== null && reclaimLookedAtMs > 0) return false;
    if (!(Number.isFinite(Number(atr)) && Number(atr) > 0)) return false;
    const extreme = Number(sweep.extreme);
    const level = Number(sweep.level);
    if (!(Number.isFinite(extreme) && extreme > 0 && Number.isFinite(level) && level > 0)) return false;
    if (Math.abs(extreme - level) / Number(atr) < RECLAIM_WAKE_MIN_DEPTH_ATR) return false;
    const reclaimedAtMs = Number(sweep.reclaimedAtMs);
    if (!(Number.isFinite(reclaimedAtMs) && reclaimedAtMs > 0)) return false;
    return nowMs - reclaimedAtMs <= RECLAIM_WAKE_FRESH_MINUTES * 60_000;
}

// One watcher-minute of a sustained band, as a pure state transition.
// The caller persists what the step tells it to and nothing else:
//   fire   — confirmed: either the time window held (`via: 'time'`) or the
//            CURRENT price extends ≥ wakeBreakConfirmAtr() primary-ATRs beyond
//            the band (`via: 'extension'` — force proves the break before the
//            clock does; fires even on the first minute of a touch).
//   arm    — first minute beyond the band (or beyond the OTHER band after a
//            side flip): start/restart the touch. On a flip the failed old
//            touch rides along as `sweep`.
//   extend — touch continues with a new excursion extreme worth persisting.
//   hold   — touch continues, nothing to write this minute.
//   sweep  — price back inside the bands before the window elapsed: record
//            the failed touch, clear the touch state.
//   idle   — no cross, no touch.
export type SustainedWakeStep =
    | { kind: 'fire'; side: 'above' | 'below'; heldMinutes: number; via: 'time' | 'extension'; extensionAtr: number | null }
    | { kind: 'arm'; side: 'above' | 'below'; sweep: WakeSweepEvent | null }
    | { kind: 'extend'; side: 'above' | 'below'; extreme: number }
    | { kind: 'hold' }
    | { kind: 'sweep'; sweep: WakeSweepEvent }
    | { kind: 'idle' };

function bandLevel(side: 'above' | 'below', wakeAbove: number | null, wakeBelow: number | null): number | null {
    return side === 'above' ? wakeAbove : wakeBelow;
}

export function sustainedWakeStep(params: {
    price: number | null;
    wakeAbove: number | null;
    wakeBelow: number | null;
    sustainMinutes: number;
    touchSide: 'above' | 'below' | null;
    touchStartedMs: number | null;
    touchExtreme: number | null;
    nowMs: number;
    // Primary ATR captured when the band was set (swing.ai_cooldowns.wake_atr)
    // — enables the extension confirm. Null/absent = time-only confirmation.
    atr?: number | null;
}): SustainedWakeStep {
    const { price, wakeAbove, wakeBelow, sustainMinutes, touchSide, touchStartedMs, touchExtreme, nowMs, atr } = params;
    const touching = touchSide !== null && Number.isFinite(Number(touchStartedMs)) && Number(touchStartedMs) > 0;
    // Unusable price = the market is UNOBSERVABLE this minute, not "back
    // inside the bands" — a failed ticker fetch must never record a false
    // sweep (or false-fail a touch). Hold still and retry next minute.
    if (!(Number.isFinite(Number(price)) && Number(price) > 0)) {
        return touching ? { kind: 'hold' } : { kind: 'idle' };
    }
    const crossed = wakeBandCrossed(price, wakeAbove, wakeBelow);

    const sweepOf = (side: 'above' | 'below'): WakeSweepEvent | null => {
        const level = bandLevel(side, wakeAbove, wakeBelow);
        if (level === null || !touching) return null;
        return {
            side,
            level,
            touchedAtMs: Number(touchStartedMs),
            reclaimedAtMs: nowMs,
            extreme: touchExtreme,
        };
    };

    if (!crossed) {
        if (!touching) return { kind: 'idle' };
        const sweep = sweepOf(touchSide as 'above' | 'below');
        // sweep is null only if the touched side's band level vanished
        // mid-touch — unreachable via upsert (which resets touch state too);
        // fail quiet rather than record a level-less event.
        return sweep ? { kind: 'sweep', sweep } : { kind: 'idle' };
    }

    // Extension confirm — checked BEFORE touch bookkeeping so a violent break
    // fires on its very first observed minute (no touch state needed). Uses
    // the CURRENT excursion, not the stored extreme: a spike that already fell
    // back toward the level is sweep-shaped, not force.
    const p = Number(price);
    const level = bandLevel(crossed, wakeAbove, wakeBelow);
    const atrNum = Number(atr);
    const extensionAtr =
        level !== null && Number.isFinite(atrNum) && atrNum > 0
            ? ((p - level) * (crossed === 'above' ? 1 : -1)) / atrNum
            : null;
    const heldMsSoFar = touching && touchSide === crossed ? nowMs - Number(touchStartedMs) : 0;
    if (extensionAtr !== null && extensionAtr >= wakeBreakConfirmAtr()) {
        return {
            kind: 'fire',
            side: crossed,
            heldMinutes: Math.max(0, Math.round(heldMsSoFar / 60_000)),
            via: 'extension',
            extensionAtr,
        };
    }

    if (!touching || touchSide !== crossed) {
        // First minute beyond this band. If a touch on the OTHER side was
        // live, price traversed the whole range — that touch failed.
        const sweep = touching && touchSide !== crossed ? sweepOf(touchSide as 'above' | 'below') : null;
        return { kind: 'arm', side: crossed, sweep };
    }

    const heldMs = nowMs - Number(touchStartedMs);
    if (heldMs >= sustainMinutes * 60_000) {
        return {
            kind: 'fire',
            side: crossed,
            heldMinutes: Math.max(0, Math.round(heldMs / 60_000)),
            via: 'time',
            extensionAtr,
        };
    }

    const storedExtreme = Number(touchExtreme);
    const beyondStored =
        !Number.isFinite(storedExtreme) || (crossed === 'above' ? p > storedExtreme : p < storedExtreme);
    if (Number.isFinite(p) && p > 0 && beyondStored) return { kind: 'extend', side: crossed, extreme: p };
    return { kind: 'hold' };
}

// ---------------------------------------------------------------------------
// Session-level sweeps (reclaim-wake phase 2): the same touch-and-reclaim
// event class as band sweeps, but at CODE-KNOWN liquidity pools — the last
// completed session's high/low and the prior day's high/low — so a sweep
// fires an immediate AI look even when the AI armed no band there. All state
// lives in KV (there is no cooldown row to hang it on): analyze stamps a
// levels ref on every session-category look; the watcher runs a per-minute
// touch state machine against it and, on a deep-enough reclaim, writes an
// event the analyze route consumes as market.session_reclaim. Judgment-gated
// only, same as phase 1 — a fired look is never a mechanical entry.
// ---------------------------------------------------------------------------

export type SessionSweepKind = 'last_session_high' | 'last_session_low' | 'prior_day_high' | 'prior_day_low';

// Stamped by analyze whenever it builds the forex session context. Levels
// carry their own validity horizons instead of a blanket ref age: last-session
// levels are true until the CURRENT session completes (then a new "last
// completed" exists), prior-day levels until the UTC date rolls — so a symbol
// parked under a multi-hour AI cooldown keeps sweep coverage from its last
// stamped ref instead of going blind after a fixed staleness window.
export type SessionLevelsRef = {
    levels: Partial<Record<SessionSweepKind, number | null>>;
    // Primary ATR at stamp time — anchors the sweep depth floor.
    atr: number | null;
    ts: number;
    lastSessionValidUntilMs: number | null;
    priorDayValidUntilMs: number | null;
};

export function sessionSweepKindValidUntil(ref: SessionLevelsRef, kind: SessionSweepKind): number {
    const fallback = Number(ref.ts) + 90 * 60_000; // rows stamped before the horizons existed
    const horizon =
        kind === 'prior_day_high' || kind === 'prior_day_low' ? ref.priorDayValidUntilMs : ref.lastSessionValidUntilMs;
    return Number.isFinite(Number(horizon)) && Number(horizon) > 0 ? Number(horizon) : fallback;
}

// In-flight touch (one at a time per symbol) and the fired reclaim event.
export type SessionSweepState = {
    kind: SessionSweepKind;
    side: 'above' | 'below';
    level: number;
    touchedAtMs: number;
    extreme: number;
};
export type SessionSweepEvent = {
    kind: SessionSweepKind;
    side: 'above' | 'below';
    level: number;
    extreme: number;
    touchedAtMs: number;
    reclaimedAtMs: number;
    atr: number | null;
};

export const sessionLevelsRefKey = (platform: string, symbol: string) =>
    `swing:sessionsweep:ref:${String(platform || '').toLowerCase()}:${String(symbol || '').toUpperCase()}`;
export const sessionSweepStateKey = (platform: string, symbol: string) =>
    `swing:sessionsweep:state:${String(platform || '').toLowerCase()}:${String(symbol || '').toUpperCase()}`;
export const sessionSweepEventKey = (platform: string, symbol: string) =>
    `swing:sessionsweep:event:${String(platform || '').toLowerCase()}:${String(symbol || '').toUpperCase()}`;
// Per-level one-shot budget (TTL-scoped): the TLT failure mode is the same
// pool being poked many times per session — one look per pool per window.
export const sessionSweepLookedKey = (platform: string, symbol: string, kind: SessionSweepKind) =>
    `swing:sessionsweep:looked:${String(platform || '').toLowerCase()}:${String(symbol || '').toUpperCase()}:${kind}`;
export const SESSION_SWEEP_LOOKED_TTL_SECONDS = 6 * 3600;
export const SESSION_SWEEP_EVENT_TTL_SECONDS = 15 * 60;

// A touch that HOLDS beyond the level this long is a break, not a sweep —
// the state is abandoned (breakouts belong to the AI's own band machinery).
export const SESSION_SWEEP_WINDOW_MINUTES = 30;

export type SessionSweepStep =
    | { kind: 'idle' }
    | { kind: 'hold' }
    | { kind: 'arm'; state: SessionSweepState }
    | { kind: 'extend'; state: SessionSweepState }
    | { kind: 'abandon' }
    | { kind: 'reclaim'; event: SessionSweepEvent };

// One watcher-minute against the session levels, as a pure transition —
// mirrors sustainedWakeStep's contract (caller persists what the step says).
export function sessionSweepStep(params: {
    price: number | null;
    ref: SessionLevelsRef | null;
    state: SessionSweepState | null;
    nowMs: number;
}): SessionSweepStep {
    const { price, ref, state, nowMs } = params;
    if (!ref) return state ? { kind: 'abandon' } : { kind: 'idle' };
    const p = Number(price);
    // Unobservable market: hold still (never false-reclaim on a failed fetch).
    if (!(Number.isFinite(p) && p > 0)) return state ? { kind: 'hold' } : { kind: 'idle' };
    if (state && nowMs > sessionSweepKindValidUntil(ref, state.kind)) {
        // The touched pool's level expired mid-touch (session completed, day
        // rolled): abandon rather than judge against a superseded level.
        return { kind: 'abandon' };
    }

    if (state) {
        const dir = state.side === 'above' ? 1 : -1;
        const beyond = dir * (p - state.level) > 0;
        if (!beyond) {
            return {
                kind: 'reclaim',
                event: {
                    kind: state.kind,
                    side: state.side,
                    level: state.level,
                    extreme: state.extreme,
                    touchedAtMs: state.touchedAtMs,
                    reclaimedAtMs: nowMs,
                    atr: ref.atr,
                },
            };
        }
        if (nowMs - state.touchedAtMs > SESSION_SWEEP_WINDOW_MINUTES * 60_000) return { kind: 'abandon' };
        const newExtreme = state.side === 'above' ? Math.max(state.extreme, p) : Math.min(state.extreme, p);
        if (newExtreme !== state.extreme) return { kind: 'extend', state: { ...state, extreme: newExtreme } };
        return { kind: 'hold' };
    }

    // No touch in flight: is price beyond a pool right now? Highs sweep from
    // below ('above' excursion), lows from above ('below'). When several
    // pools on one side are violated, the NEAREST one to price is the pool
    // being taken. Both sides violated at once = unusable levels; stay idle.
    const highs: SessionSweepKind[] = ['last_session_high', 'prior_day_high'];
    const lows: SessionSweepKind[] = ['last_session_low', 'prior_day_low'];
    let above: { kind: SessionSweepKind; level: number } | null = null;
    let below: { kind: SessionSweepKind; level: number } | null = null;
    for (const kind of highs) {
        if (nowMs > sessionSweepKindValidUntil(ref, kind)) continue;
        const level = Number(ref.levels[kind]);
        if (Number.isFinite(level) && level > 0 && p > level && (!above || level > above.level)) {
            above = { kind, level };
        }
    }
    for (const kind of lows) {
        if (nowMs > sessionSweepKindValidUntil(ref, kind)) continue;
        const level = Number(ref.levels[kind]);
        if (Number.isFinite(level) && level > 0 && p < level && (!below || level < below.level)) {
            below = { kind, level };
        }
    }
    if (above && below) return { kind: 'idle' };
    const violated = above ? { ...above, side: 'above' as const } : below ? { ...below, side: 'below' as const } : null;
    if (!violated) return { kind: 'idle' };
    return {
        kind: 'arm',
        state: { kind: violated.kind, side: violated.side, level: violated.level, touchedAtMs: nowMs, extreme: p },
    };
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
        const row = candles[i] as ArrayLike<unknown> | null | undefined;
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

// The emergency comparison only means "emergency" while the ref is recent: it
// measures the move SINCE THE LAST AI LOOK, and its job is to beat the regular
// cadence by minutes. Once the ref is older than ~1.5x the 4H look cadence, no
// look has happened for a while (AI outage, venue closure) — price is then
// almost always ≥ the threshold away from the frozen anchor, which made the
// condition permanently true and re-fired doomed analyze calls every ~4 min
// through the whole 2026-07-27 quota outage. A stale ref fails quiet: the
// regular cadence (which resumes the moment the AI is back) owns the position.
export const WAKE_REF_MAX_AGE_MINUTES_DEFAULT = 360;

export function wakeRefMaxAgeMinutes(): number {
    const raw = Number(process.env.SWING_WAKE_REF_MAX_AGE_MINUTES ?? WAKE_REF_MAX_AGE_MINUTES_DEFAULT);
    return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : WAKE_REF_MAX_AGE_MINUTES_DEFAULT;
}

// In-position emergency: absolute move (either direction) since the last AI
// look, in primary-ATR units. Null when the ref is unusable — the watcher then
// stays quiet and the regular cadence owns the position (fail quiet, not loud:
// a missing ref must not cause per-minute AI calls). Passing nowMs enables the
// staleness guard above; omitting it preserves the raw measurement.
export function emergencyMoveAtr(
    price: number | null | undefined,
    ref: WakeWatchRef | null | undefined,
    nowMs?: number,
): number | null {
    const p = Number(price);
    if (!(Number.isFinite(p) && p > 0)) return null;
    const refPrice = Number(ref?.price);
    const atr = Number(ref?.atr);
    if (!(Number.isFinite(refPrice) && refPrice > 0)) return null;
    if (!(Number.isFinite(atr) && atr > 0)) return null;
    if (Number.isFinite(nowMs as number)) {
        const refTs = Number(ref?.ts);
        if (!(Number.isFinite(refTs) && refTs > 0)) return null;
        if ((nowMs as number) - refTs > wakeRefMaxAgeMinutes() * 60_000) return null;
    }
    return Math.abs(p - refPrice) / atr;
}
