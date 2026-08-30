// lib/swing/decisionRules.ts
//
// Everything that happens to the model's answer AFTER it parses: the hard
// constraints (postprocessDecision) and the per-field sanitizers that clamp or
// drop values the venue would reject.
//
// This is the enforcement half of the contract the prompt describes — the
// prompt's "enforced in code" claims are these functions, and both read their
// bounds from decisionConfig.ts so the two can never disagree.

import { clampWakeSustainMinutes } from './wakeWatch';
import type { RecentActionEntry } from './recentActions';
import type { TradeDecision } from '../trading';
import {
    resolveDecisionPolicy,
    flagOff,
    PULLBACK_LIMIT_ENABLED,
    SESSION_OFFENSE_ENABLED,
    POSITION_WAKE_MIN_ATR,
    HOLD_COOLDOWN_MIN_MINUTES,
    HOLD_COOLDOWN_MAX_MINUTES,
    EXCHANGE_TP_FALLBACK_ATR_MULT,
    EXCHANGE_SL_MAX_ATR_MULT,
    ENTRY_TP_MIN_ATR,
    ENTRY_SL_MIN_ATR,
    TP_MAX_ATR,
    AMEND_MIN_GAP_ATR,
    ENTRY_LIMIT_MIN_ATR,
    ENTRY_LIMIT_MAX_ATR,
} from './decisionConfig';
import type { PositionContext, DecisionPolicy, LastClosedPosition, PromptDecisionContext } from './decisionConfig';
import { resolveReentryCooldown, computeSignalStrength } from './signals';

const toBiasLabel = (value: string): 'UP' | 'DOWN' | 'NEUTRAL' => {
    const v = value.toLowerCase();
    if (v === 'up') return 'UP';
    if (v === 'down') return 'DOWN';
    return 'NEUTRAL';
};

export function postprocessDecision(params: {
    decision: Record<string, unknown> | null | undefined;
    context: PromptDecisionContext;
    gates: { spread_ok: boolean; liquidity_ok: boolean; atr_ok: boolean; slippage_ok: boolean };
    positionOpen: boolean;
    recentActions: RecentActionEntry[];
    positionContext: PositionContext | null;
    policy?: DecisionPolicy;
    lastClosedPosition?: LastClosedPosition | null;
    // Test seam for the sweep-reclaim re-entry exception; production callers
    // rely on the env-derived default.
    sessionOffenseEnabled?: boolean;
    // Mechanical entry on a CONFIRMED wake fire (lib/swing/wakeAutoEntry):
    // bypasses ONLY the micro_entry_ok timing block below — momentum timing is
    // routinely false in the first minutes of a real break, and the wake's
    // sustain/extension confirmation IS the timing evidence. Trend guard,
    // re-entry cooldown, and base gates still apply.
    confirmedWakeEntry?: boolean;
}) {
    const {
        decision,
        context,
        gates,
        positionOpen,
        recentActions,
        positionContext,
        policy,
        lastClosedPosition,
        sessionOffenseEnabled,
        confirmedWakeEntry,
    } = params;
    const resolvedDecisionPolicy = resolveDecisionPolicy(policy);
    const strictPolicy = resolvedDecisionPolicy === 'strict';
    const signalStrength = computeSignalStrength(context);
    const microBias = toBiasLabel(context.micro_bias_calc);
    const primaryBias = toBiasLabel(context.primary_bias);
    const macroBias = toBiasLabel(context.macro_bias);
    const contextBias = toBiasLabel(context.context_bias);

    const allowedActions = positionOpen ? ['HOLD', 'CLOSE', 'REVERSE'] : ['BUY', 'SELL', 'HOLD'];
    let action = String(decision?.action || 'HOLD').toUpperCase();
    if (!allowedActions.includes(action)) action = 'HOLD';

    const desiredSide =
        action === 'BUY'
            ? 'long'
            : action === 'SELL'
              ? 'short'
              : action === 'REVERSE'
                ? positionContext?.side === 'long'
                    ? 'short'
                    : positionContext?.side === 'short'
                      ? 'long'
                      : null
                : null;

    if (desiredSide === 'short' && context.primary_trend_up && microBias === 'UP') {
        const allowCounterTrend = !strictPolicy && signalStrength === 'HIGH' && context.primary_breakdown_confirmed;
        if (!allowCounterTrend) action = 'HOLD';
    }
    if (desiredSide === 'long' && context.primary_trend_down && microBias === 'DOWN') {
        const allowCounterTrend = !strictPolicy && signalStrength === 'HIGH' && context.primary_breakout_confirmed;
        if (!allowCounterTrend) action = 'HOLD';
    }

    if (!positionOpen && !context.micro_entry_ok && !confirmedWakeEntry && (action === 'BUY' || action === 'SELL')) {
        const allowException =
            (signalStrength === 'HIGH' && context.breakout_retest_ok_primary) ||
            (!strictPolicy &&
                signalStrength !== 'LOW' &&
                (context.breakout_retest_ok_primary || context.aligned_driver_count >= 4));
        if (!allowException) action = 'HOLD';
    }

    // Re-entry cooldown: when flat, block re-opening the direction that just closed.
    // Opposite-direction entries stay allowed (a reversal thesis is a new trade).
    // Sweep-reclaim exception (session offense, flag-gated): when the
    // just-stopped side's extreme was swept and RECLAIMED
    // (bullishLiquidityReclaim for a long, bearishLiquidityRejection for a
    // short), the stop-run itself was the event and the reclaim re-entry is the
    // highest-edge same-direction trade — the anti-churn block must not eat it.
    // With SESSION_OFFENSE_ENABLED off (swing default) the exception is off
    // too: the cooldown always applies.
    if (!positionOpen && (action === 'BUY' || action === 'SELL')) {
        const cooldown = resolveReentryCooldown(lastClosedPosition);
        if (cooldown && desiredSide === cooldown.blockedSide) {
            const signals = context.forex_session_context?.signals;
            const reclaimForSide =
                (sessionOffenseEnabled ?? SESSION_OFFENSE_ENABLED) &&
                (desiredSide === 'long'
                    ? Boolean(signals?.bullishLiquidityReclaim)
                    : Boolean(signals?.bearishLiquidityRejection));
            if (!reclaimForSide) action = 'HOLD';
        }
    }

    if (action === 'CLOSE' || action === 'REVERSE') {
        const antiFlipLookback = strictPolicy ? 2 : 1;
        const recent = (recentActions || [])
            .slice(-antiFlipLookback)
            .map((a) => String(a.action || '').toUpperCase())
            .filter((a) => a);
        const strongEnoughForRepeat = signalStrength === 'HIGH' || (!strictPolicy && signalStrength === 'MEDIUM');
        if (!strongEnoughForRepeat && recent.includes(action)) {
            action = 'HOLD';
        }
    }

    const baseGatesOk = Boolean(gates?.spread_ok && gates?.liquidity_ok && gates?.atr_ok && gates?.slippage_ok);
    if (!baseGatesOk) {
        if (positionOpen) {
            if (strictPolicy) {
                action = 'CLOSE';
            } else if (action === 'REVERSE') {
                action = 'CLOSE';
            } else if (action !== 'CLOSE') {
                action = 'HOLD';
            }
        } else {
            action = 'HOLD';
        }
    }

    const leverage =
        action === 'BUY' || action === 'SELL' || action === 'REVERSE'
            ? Number.isFinite(decision?.leverage as number)
                ? Number(decision?.leverage)
                : null
            : null;
    const exit_size_pct =
        action === 'CLOSE' || action === 'REVERSE'
            ? Number.isFinite(decision?.exit_size_pct as number)
                ? Number(decision?.exit_size_pct)
                : null
            : null;

    // Profit-lock margin-recycle fields (crypto only; caller strips for non-crypto).
    // Eligible on HOLD or a PARTIAL close (a full close has nothing to manage).
    // Execution owns the authoritative [current, symbol max] leverage clamp — here
    // we only gate by action + feature flag and coerce types.
    const manageEligible =
        process.env.ENABLE_CRYPTO_MARGIN_RECYCLE === 'true' &&
        (action === 'HOLD' || (action === 'CLOSE' && exit_size_pct != null && exit_size_pct < 100));
    const raise_leverage_to =
        manageEligible &&
        Number.isFinite(Number(decision?.raise_leverage_to)) &&
        Number(decision?.raise_leverage_to) > 0
            ? Math.round(Number(decision?.raise_leverage_to))
            : null;
    const move_stop_to_be = manageEligible ? decision?.move_stop_to_be === true : false;

    // Exchange-side TP/SL targets. Here we only gate by action and coerce types;
    // price-level sanity (correct side of price, min/max ATR distance, entry-TP
    // fallback, tighten-only stop amends) is enforced by sanitizeExchangeTpSl in
    // the API route, which has the live price + ATR. On entries (BUY/SELL/REVERSE
    // — REVERSE opens a fresh position bracketed for the NEW side) the SL is the
    // model's structural invalidation stop; when it is null or later dropped, the
    // code-owned 3×ATR catastrophe stop is attached instead.
    const coercePrice = (v: unknown) => {
        const n = Number(v);
        return Number.isFinite(n) && n > 0 ? n : null;
    };
    const isEntryAction = action === 'BUY' || action === 'SELL' || action === 'REVERSE';
    const tpslAmendEligible =
        positionOpen && (action === 'HOLD' || (action === 'CLOSE' && exit_size_pct != null && exit_size_pct < 100));
    const take_profit_price =
        isEntryAction || tpslAmendEligible ? coercePrice(decision?.take_profit_price) : null;
    const stop_loss_price =
        isEntryAction || tpslAmendEligible ? coercePrice(decision?.stop_loss_price) : null;
    // Pullback limit entry: flat BUY/SELL only (REVERSE stays market — it must
    // actually flip the exposure, not maybe-flip it). Price-side/distance
    // sanity is enforced by sanitizeEntryLimit in the API route.
    const entry_limit_price =
        !positionOpen && (action === 'BUY' || action === 'SELL') ? coercePrice(decision?.entry_limit_price) : null;
    // Failed-break watch trigger: fresh flat entries only (REVERSE stays
    // untracked — its side depends on the position being flipped, and the rare
    // reverse can re-declare on the next look). Side sanity vs live price is
    // enforced by sanitizeEntryTrigger in the API route.
    const entry_trigger_price =
        !positionOpen && (action === 'BUY' || action === 'SELL') ? coercePrice(decision?.entry_trigger_price) : null;
    // Flat-HOLD cooldown: type/eligibility coercion only — clamping and wake-band
    // side validation happen in sanitizeHoldCooldown in the API route (live price).
    const cooldownEligible = !positionOpen && action === 'HOLD';
    const cooldown_minutes =
        cooldownEligible && Number.isFinite(Number(decision?.cooldown_minutes)) && Number(decision?.cooldown_minutes) > 0
            ? Math.round(Number(decision?.cooldown_minutes))
            : null;
    // In-position wake bands ride the same cooldown_wake_* fields, routed by
    // position state: eligible on in-position HOLD / partial CLOSE (same gate
    // as TP/SL amends). cooldown_minutes stays flat-only. Level sanity happens
    // in sanitizePositionWake in the API route (live price + bracket). Flag is
    // read at CALL time (like ENABLE_CRYPTO_MARGIN_RECYCLE above) so tests can
    // toggle it.
    const wakeEligible = cooldownEligible || (!flagOff(process.env.ENABLE_POSITION_WAKE_BANDS) && tpslAmendEligible);
    const cooldown_wake_above = wakeEligible ? coercePrice(decision?.cooldown_wake_above) : null;
    const cooldown_wake_below = wakeEligible ? coercePrice(decision?.cooldown_wake_below) : null;
    // The band's plan, echoed back at fire time (market.cooldown_wake.note /
    // market.position_wake.note). Only meaningful alongside a band;
    // length-capped, never trusted raw.
    const rawWakeNote = decision?.cooldown_wake_note;
    const cooldown_wake_note =
        wakeEligible &&
        (cooldown_wake_above !== null || cooldown_wake_below !== null) &&
        typeof rawWakeNote === 'string' &&
        rawWakeNote.trim()
            ? rawWakeNote.trim().slice(0, 200)
            : null;
    // Sustained confirmation: FLAT bands only (an in-position band guards a
    // live position, where the instant look is right whether the break is
    // real or fake). Type coercion here; clamping in sanitizeHoldCooldown.
    const rawSustain = Number(decision?.cooldown_wake_sustain_minutes);
    const cooldown_wake_sustain_minutes =
        cooldownEligible &&
        (cooldown_wake_above !== null || cooldown_wake_below !== null) &&
        Number.isFinite(rawSustain) &&
        rawSustain > 0
            ? Math.round(rawSustain)
            : null;

    const normalized: Record<string, unknown> = {
        ...decision,
        action,
        leverage,
        exit_size_pct,
        raise_leverage_to,
        move_stop_to_be,
        take_profit_price,
        stop_loss_price,
        entry_limit_price,
        entry_trigger_price,
        cooldown_minutes,
        cooldown_wake_above,
        cooldown_wake_below,
        cooldown_wake_note,
        cooldown_wake_sustain_minutes,
        signal_strength: signalStrength,
        micro_bias: microBias,
        primary_bias: primaryBias,
        macro_bias: macroBias,
        context_bias: contextBias,
    };
    // Raw-AI-JSON boundary: action is coerced into the allowed set above and
    // summary/reason are schema-required, so the normalized shape is a decision.
    return normalized as TradeDecision & Record<string, unknown>;
}

// ------------------------------
// Flat-HOLD cooldown sanitation
// ------------------------------

export type HoldCooldown = {
    cooldownMinutes: number | null;
    wakeAbove: number | null;
    wakeBelow: number | null;
    // The model's one-line plan for the band ("retest of broken 118.4k for
    // long entry") — persisted with the cooldown row and echoed back as
    // market.cooldown_wake.note when the band fires. Only kept while at least
    // one band survives sanitation: a note is a plan attached to a band, not
    // a general memo.
    wakeNote: string | null;
    // Sustained-confirmation window (minutes, clamped to
    // [WAKE_SUSTAIN_MIN_MINUTES, WAKE_SUSTAIN_MAX_MINUTES]): the band wakes
    // only if price is still beyond it this long after first touch; failed
    // touches return as market.wake_band_sweeps. null = instant touch wake.
    // Like the note, only kept while at least one band survives.
    sustainMinutes: number | null;
    notes: string[];
};

// Flat HOLD only. Minutes clamp to [15, max]; wake bands must sit on the
// correct side of current price (above > price, below < price) — an invalid
// band is dropped (the cooldown stays, just less conditional), and an
// unverifiable price drops the bands rather than trusting them blind.
export function sanitizeHoldCooldown(params: {
    action: string;
    positionOpen: boolean;
    price: number | null;
    cooldownMinutes: unknown;
    wakeAbove: unknown;
    wakeBelow: unknown;
    wakeNote?: unknown;
    wakeSustainMinutes?: unknown;
}): HoldCooldown {
    const notes: string[] = [];
    if (params.positionOpen || String(params.action).toUpperCase() !== 'HOLD') {
        return { cooldownMinutes: null, wakeAbove: null, wakeBelow: null, wakeNote: null, sustainMinutes: null, notes };
    }
    const rawMinutes = Number(params.cooldownMinutes);
    if (!Number.isFinite(rawMinutes) || rawMinutes <= 0) {
        return { cooldownMinutes: null, wakeAbove: null, wakeBelow: null, wakeNote: null, sustainMinutes: null, notes };
    }
    const cooldownMinutes = Math.min(HOLD_COOLDOWN_MAX_MINUTES, Math.max(HOLD_COOLDOWN_MIN_MINUTES, Math.round(rawMinutes)));
    if (cooldownMinutes !== Math.round(rawMinutes)) notes.push(`clamped_${Math.round(rawMinutes)}m_to_${cooldownMinutes}m`);

    const price = Number(params.price);
    const priceKnown = Number.isFinite(price) && price > 0;
    const wakeAbove = Number(params.wakeAbove);
    const wakeBelow = Number(params.wakeBelow);
    let above: number | null = Number.isFinite(wakeAbove) && wakeAbove > 0 ? wakeAbove : null;
    let below: number | null = Number.isFinite(wakeBelow) && wakeBelow > 0 ? wakeBelow : null;
    if (!priceKnown) {
        if (above !== null || below !== null) notes.push('wake_bands_dropped_price_unknown');
        above = null;
        below = null;
    } else {
        if (above !== null && above <= price) {
            notes.push('wake_above_dropped_not_above_price');
            above = null;
        }
        if (below !== null && below >= price) {
            notes.push('wake_below_dropped_not_below_price');
            below = null;
        }
    }
    const rawNote = params.wakeNote;
    let wakeNote: string | null =
        typeof rawNote === 'string' && rawNote.trim() ? rawNote.trim().slice(0, 200) : null;
    if (wakeNote !== null && above === null && below === null) {
        notes.push('wake_note_dropped_no_band');
        wakeNote = null;
    }
    const rawSustain = Number(params.wakeSustainMinutes);
    let sustainMinutes: number | null = clampWakeSustainMinutes(params.wakeSustainMinutes);
    if (sustainMinutes !== null && above === null && below === null) {
        notes.push('wake_sustain_dropped_no_band');
        sustainMinutes = null;
    } else if (sustainMinutes !== null && sustainMinutes !== Math.round(rawSustain)) {
        notes.push(`wake_sustain_clamped_${Math.round(rawSustain)}m_to_${sustainMinutes}m`);
    }
    return { cooldownMinutes, wakeAbove: above, wakeBelow: below, wakeNote, sustainMinutes, notes };
}

// ------------------------------
// In-position wake-band sanitation
// ------------------------------

export type PositionWakeBands = {
    wakeAbove: number | null;
    wakeBelow: number | null;
    // The model's one-line plan for the band ("losing 3.42 = thesis dead →
    // decide exit") — stored on the thread and echoed back as
    // market.position_wake.note when the band fires.
    wakeNote: string | null;
    notes: string[];
};

// In-position HOLD / partial CLOSE only (the TP/SL-amend eligibility). Bands
// are early-look requests, never protection — each one must sit on the correct
// side of current price, STRICTLY inside the standing bracket (at/beyond SL or
// TP the venue closes the position first, so the band is unreachable), and at
// least POSITION_WAKE_MIN_ATR primary-ATRs from price (a band glued to price
// would re-fire a full AI call every few minutes). A violating band is dropped
// with a note; the decision itself is never affected. Callers pass the bracket
// as it stands AFTER this tick's amend sanitation (amended ?? standing), so
// bands validate against the levels that will actually rest on the venue.
// Deliberately does NOT check ENABLE_POSITION_WAKE_BANDS itself (pure,
// testable): with the flag off, normalizeDecision already nulls the fields
// and the analyze route never persists.
export function sanitizePositionWake(params: {
    action: string;
    positionOpen: boolean;
    exitSizePct: number | null;
    price: number | null;
    primaryAtr: number | null;
    takeProfitPrice: number | null;
    stopLossPrice: number | null;
    wakeAbove: unknown;
    wakeBelow: unknown;
    wakeNote?: unknown;
}): PositionWakeBands {
    const notes: string[] = [];
    const none = (): PositionWakeBands => ({ wakeAbove: null, wakeBelow: null, wakeNote: null, notes });
    const action = String(params.action).toUpperCase();
    const eligible =
        params.positionOpen &&
        (action === 'HOLD' || (action === 'CLOSE' && params.exitSizePct != null && params.exitSizePct < 100));
    if (!eligible) return none();

    const price = Number(params.price);
    if (!(Number.isFinite(price) && price > 0)) {
        const rawAbove = Number(params.wakeAbove);
        const rawBelow = Number(params.wakeBelow);
        if ((Number.isFinite(rawAbove) && rawAbove > 0) || (Number.isFinite(rawBelow) && rawBelow > 0)) {
            notes.push('wake_bands_dropped_price_unknown');
        }
        return none();
    }

    let above: number | null = Number.isFinite(Number(params.wakeAbove)) && Number(params.wakeAbove) > 0 ? Number(params.wakeAbove) : null;
    let below: number | null = Number.isFinite(Number(params.wakeBelow)) && Number(params.wakeBelow) > 0 ? Number(params.wakeBelow) : null;

    // Side of current price.
    if (above !== null && above <= price) {
        notes.push('wake_above_dropped_not_above_price');
        above = null;
    }
    if (below !== null && below >= price) {
        notes.push('wake_below_dropped_not_below_price');
        below = null;
    }

    // Strictly inside the bracket. Which bracket leg sits on which side of
    // price depends on position side, but not here: both legs are themselves
    // side-validated, so it suffices that a band never sits at/beyond ANY
    // bracket level on its own side of price.
    const tp = Number(params.takeProfitPrice);
    const sl = Number(params.stopLossPrice);
    const bracketAbove = [tp, sl].filter((v) => Number.isFinite(v) && v > price);
    const bracketBelow = [sl, tp].filter((v) => Number.isFinite(v) && v > 0 && v < price);
    if (above !== null && bracketAbove.length && above >= Math.min(...bracketAbove)) {
        notes.push('wake_above_dropped_beyond_bracket');
        above = null;
    }
    if (below !== null && bracketBelow.length && below <= Math.max(...bracketBelow)) {
        notes.push('wake_below_dropped_beyond_bracket');
        below = null;
    }

    // Min distance from price (churn guard). Unknown ATR fails open — the side
    // and bracket checks still bound the band, and a missing ATR is rare.
    const atr = Number(params.primaryAtr);
    if (Number.isFinite(atr) && atr > 0) {
        const minDist = POSITION_WAKE_MIN_ATR * atr;
        if (above !== null && above - price < minDist) {
            notes.push('wake_above_dropped_too_close');
            above = null;
        }
        if (below !== null && price - below < minDist) {
            notes.push('wake_below_dropped_too_close');
            below = null;
        }
    } else if (above !== null || below !== null) {
        notes.push('wake_min_dist_unverified');
    }

    const rawNote = params.wakeNote;
    let wakeNote: string | null =
        typeof rawNote === 'string' && rawNote.trim() ? rawNote.trim().slice(0, 200) : null;
    if (wakeNote !== null && above === null && below === null) {
        notes.push('wake_note_dropped_no_band');
        wakeNote = null;
    }
    return { wakeAbove: above, wakeBelow: below, wakeNote, notes };
}

// ------------------------------
// Failed-break trigger sanitation
// ------------------------------

// Flat BUY/SELL only. The trigger is the broken level the entry thesis stands
// on, so it must sit on the thesis side of current price: below it for a long
// (price broke UP through the trigger), above it for a short. A wrong-side or
// absurdly distant trigger is dropped (the entry itself is unaffected — the
// failed-break watch is protective bookkeeping, never an order parameter).
export function sanitizeEntryTrigger(params: {
    action: string;
    positionOpen: boolean;
    price: number | null;
    triggerPrice: unknown;
}): { triggerPrice: number | null; notes: string[] } {
    const notes: string[] = [];
    const action = String(params.action || '').toUpperCase();
    if (params.positionOpen || (action !== 'BUY' && action !== 'SELL')) {
        return { triggerPrice: null, notes };
    }
    const raw = Number(params.triggerPrice);
    if (!Number.isFinite(raw) || raw <= 0) return { triggerPrice: null, notes };
    const price = Number(params.price);
    if (!(Number.isFinite(price) && price > 0)) {
        notes.push('entry_trigger_dropped_price_unknown');
        return { triggerPrice: null, notes };
    }
    if (action === 'BUY' && raw >= price) {
        notes.push('entry_trigger_dropped_not_below_price');
        return { triggerPrice: null, notes };
    }
    if (action === 'SELL' && raw <= price) {
        notes.push('entry_trigger_dropped_not_above_price');
        return { triggerPrice: null, notes };
    }
    return { triggerPrice: raw, notes };
}

// ------------------------------
// Exchange-side TP/SL sanitation
// ------------------------------

export type ExchangeTpSl = {
    takeProfitPrice: number | null;
    stopLossPrice: number | null;
    notes: string[];
};

/**
 * Validate the model's exchange-side TP/SL price targets against the live
 * price + primary ATR. Wrong-side or too-close levels are dropped, too-far
 * levels are clamped, and entries without a usable TP fall back to a wide
 * 3×ATR target so every entry ships with a resting exchange TP.
 *
 * Entries: `side` is derived from the action. stop_loss_price MAY be a
 * structural invalidation stop (protective side, 1–3×ATR from the entry
 * anchor); when absent or dropped, the caller attaches the code-owned 3×ATR
 * catastrophe stop instead. REVERSE is an entry for the OPPOSITE of the
 * current position side — same treatment. In-position (HOLD / partial CLOSE):
 * both legs may amend the standing bracket (null = leave unchanged), and a
 * stop amendment may only TIGHTEN protection vs `standingStopLossPrice` —
 * never further from price than the stop already resting.
 */
export function sanitizeExchangeTpSl(params: {
    action: string;
    positionOpen: boolean;
    side: 'long' | 'short' | null;
    price: number;
    primaryAtr: number | null;
    takeProfitPrice: number | null;
    stopLossPrice: number | null;
    exitSizePct?: number | null;
    // The stop currently resting on the position (null = none). Amend-only
    // tighten guard; ignored on entries.
    standingStopLossPrice?: number | null;
}): ExchangeTpSl {
    const notes: string[] = [];
    const action = String(params.action || '').toUpperCase();
    const price = Number(params.price);
    const atr = Number.isFinite(params.primaryAtr as number) && (params.primaryAtr as number) > 0 ? Number(params.primaryAtr) : null;

    const isReverse =
        params.positionOpen && action === 'REVERSE' && (params.side === 'long' || params.side === 'short');
    const isEntry = (!params.positionOpen && (action === 'BUY' || action === 'SELL')) || isReverse;
    const isAmend =
        params.positionOpen &&
        (params.side === 'long' || params.side === 'short') &&
        (action === 'HOLD' ||
            (action === 'CLOSE' && params.exitSizePct != null && params.exitSizePct < 100));
    if (!(price > 0) || (!isEntry && !isAmend)) {
        return { takeProfitPrice: null, stopLossPrice: null, notes: ['tpsl_not_applicable'] };
    }

    const side: 'long' | 'short' = isReverse
        ? params.side === 'long'
            ? 'short'
            : 'long'
        : isEntry
          ? action === 'BUY'
              ? 'long'
              : 'short'
          : (params.side as 'long' | 'short');
    const dir = side === 'long' ? 1 : -1;

    // Take profit: must sit on the profit side of price; entries need real
    // swing room (≥ENTRY_TP_MIN_ATR) so the target pays for the stop, amends
    // just need to clear the current price by a noise buffer.
    let tp = Number.isFinite(params.takeProfitPrice as number) && (params.takeProfitPrice as number) > 0 ? Number(params.takeProfitPrice) : null;
    if (tp != null) {
        if (dir * (tp - price) <= 0) {
            notes.push('tp_wrong_side_dropped');
            tp = null;
        } else if (atr) {
            const distAtr = Math.abs(tp - price) / atr;
            const minAtr = isEntry ? ENTRY_TP_MIN_ATR : AMEND_MIN_GAP_ATR;
            if (distAtr < minAtr) {
                notes.push('tp_too_close_dropped');
                tp = null;
            } else if (distAtr > TP_MAX_ATR) {
                tp = price + dir * TP_MAX_ATR * atr;
                notes.push('tp_clamped_max_atr');
            }
        }
    }
    if (tp == null && isEntry && atr) {
        const fallback = price + dir * EXCHANGE_TP_FALLBACK_ATR_MULT * atr;
        if (fallback > 0) {
            tp = fallback;
            notes.push('tp_entry_fallback_3atr');
        }
    }
    if (tp != null && !(tp > 0)) tp = null;

    // Stop loss: entries may attach a structural invalidation stop (the caller
    // falls back to the code-owned 3×ATR catastrophe stop when absent/dropped);
    // amends must be protective vs current price, never wider than the
    // catastrophe distance, and never looser than the standing stop.
    let sl = Number.isFinite(params.stopLossPrice as number) && (params.stopLossPrice as number) > 0 ? Number(params.stopLossPrice) : null;
    if (sl != null) {
        if (dir * (price - sl) <= 0) {
            notes.push('sl_wrong_side_dropped');
            sl = null;
        } else if (atr) {
            const distAtr = Math.abs(price - sl) / atr;
            const minAtr = isEntry ? ENTRY_SL_MIN_ATR : AMEND_MIN_GAP_ATR;
            if (distAtr < minAtr) {
                notes.push('sl_too_close_dropped');
                sl = null;
            } else if (distAtr > EXCHANGE_SL_MAX_ATR_MULT) {
                sl = price - dir * EXCHANGE_SL_MAX_ATR_MULT * atr;
                notes.push('sl_clamped_max_atr');
            }
        }
        // Tighten-only guard on amends: a new stop below the standing stop
        // (long) / above it (short) loosens protection — the martingale-style
        // stop walk on a losing position. Dropped, standing stop stays.
        const standingSl =
            Number.isFinite(params.standingStopLossPrice as number) && (params.standingStopLossPrice as number) > 0
                ? Number(params.standingStopLossPrice)
                : null;
        if (sl != null && isAmend && standingSl != null && dir * (sl - standingSl) < 0) {
            notes.push('sl_loosened_dropped');
            sl = null;
        }
    }
    if (sl != null && !(sl > 0)) sl = null;

    return { takeProfitPrice: tp, stopLossPrice: sl, notes };
}

// ------------------------------
// Pullback limit entry sanitation
// ------------------------------

/**
 * Validate the model's pullback entry limit against live price + primary ATR.
 * Returns the usable limit price (null = market entry as requested), or
 * dropEntry=true when the limit was invalid and the entry must be skipped
 * this tick. Only flat BUY/SELL qualifies.
 */
export function sanitizeEntryLimit(params: {
    action: string;
    positionOpen: boolean;
    price: number;
    primaryAtr: number | null;
    entryLimitPrice: number | null;
    // Test seam; production callers rely on the env-derived default.
    pullbackLimitEnabled?: boolean;
}): { entryLimitPrice: number | null; dropEntry: boolean; notes: string[] } {
    const notes: string[] = [];
    const action = String(params.action || '').toUpperCase();
    const price = Number(params.price);
    const atr =
        Number.isFinite(params.primaryAtr as number) && (params.primaryAtr as number) > 0
            ? Number(params.primaryAtr)
            : null;
    const raw =
        Number.isFinite(params.entryLimitPrice as number) && (params.entryLimitPrice as number) > 0
            ? Number(params.entryLimitPrice)
            : null;
    if (raw == null) return { entryLimitPrice: null, dropEntry: false, notes };
    if (params.positionOpen || (action !== 'BUY' && action !== 'SELL') || !(price > 0)) {
        return { entryLimitPrice: null, dropEntry: false, notes: ['limit_not_applicable'] };
    }
    // Feature flag OFF (swing default): the prompt instructs entry_limit_price
    // to be null, so a model-sent limit is a contract violation. Drop the entry
    // rather than converting to market — the limit signals the model judged the
    // CURRENT price wrong, and filling it here at market is the exact chase the
    // flag-on prose forbids.
    if (!(params.pullbackLimitEnabled ?? PULLBACK_LIMIT_ENABLED)) {
        return { entryLimitPrice: null, dropEntry: true, notes: ['limit_disabled_entry_dropped'] };
    }
    if (!atr) return { entryLimitPrice: null, dropEntry: true, notes: ['limit_no_atr_entry_dropped'] };

    const dir = action === 'BUY' ? 1 : -1;
    // Pullback distance: positive = on the pullback side of price.
    const distAtr = (dir * (price - raw)) / atr;
    if (distAtr <= 0) {
        notes.push('limit_wrong_side_entry_dropped');
        return { entryLimitPrice: null, dropEntry: true, notes };
    }
    if (distAtr < ENTRY_LIMIT_MIN_ATR) {
        notes.push('limit_too_close_entry_dropped');
        return { entryLimitPrice: null, dropEntry: true, notes };
    }
    if (distAtr > ENTRY_LIMIT_MAX_ATR) {
        const clamped = price - dir * ENTRY_LIMIT_MAX_ATR * atr;
        notes.push('limit_clamped_max_atr');
        return { entryLimitPrice: clamped > 0 ? clamped : null, dropEntry: false, notes };
    }
    return { entryLimitPrice: raw, dropEntry: false, notes };
}

// ------------------------------
// OpenAI API Call
// ------------------------------
