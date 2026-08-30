// lib/swing/decisionConfig.ts
//
// The swing decision's VOCABULARY: every domain type, every env-tunable
// threshold, every feature flag. Leaf module — it imports nothing from the rest
// of the decision stack, so prompt.ts, signals.ts and decisionRules.ts can all
// depend on it without cycles.
//
// The ATR/minute constants are deliberately shared rather than duplicated: the
// prompt DESCRIBES them to the model and the sanitizers ENFORCE them, so a
// single definition keeps the prose from drifting away from the rule.

import type { ForexSessionLevelsContext } from './sessionLevels';

export type PositionContext = {
    side: 'long' | 'short';
    entry_price?: number;
    entry_ts?: string;
    hold_minutes?: number;
    // PnL on MARGIN (return on equity, leverage-multiplied) — the same scale the
    // broker reports. max_drawdown_pct / max_profit_pct track this scale too.
    unrealized_pnl_pct_on_margin?: number;
    // Unleveraged price-scale move vs entry (side-signed) = on-margin pct ÷ leverage.
    price_move_pct?: number;
    leverage?: number | null;
    max_drawdown_pct?: number;
    max_profit_pct?: number;
    breakeven_price?: number;
    taker_fee_rate?: number;
    // Standing exchange-side bracket on the open position (null = no resting
    // order on that side). Shown to the model so TP/SL amendments are made
    // against the actual current levels.
    take_profit_price?: number | null;
    stop_loss_price?: number | null;
};

export type MomentumSignals = {
    macroTrendUp: boolean;
    macroTrendDown: boolean;
    longMomentum: boolean;
    shortMomentum: boolean;
    nearPrimaryEMA20: boolean;
    nearMicroEMA20: boolean;
    primaryRSI?: number | null;
    primarySlope?: number | null;
    microRSI?: number | null;
    primaryAtr?: number | null;
    microExtensionInAtr?: number | null;
    info?: Record<string, unknown>;
};

// Structural views of the loosely-typed market bundle / analytics objects
// built in lib/analytics.ts — only the fields this module actually reads.
type SwingTickerRow = {
    lastPr?: number | string | null;
    last?: number | string | null;
    close?: number | string | null;
    price?: number | string | null;
    change24h?: number | string | null;
    changeUtc24h?: number | string | null;
    chgPct?: number | string | null;
};

// Broker candle row (`[tsMs, open, high, low, close, volume, ...]`).
type SwingCandleRow = {
    [index: number]: unknown;
    volume?: number | string | null;
};

type SwingFundingRow = { fundingRate?: number | string | null };

type SwingFundingTimeRow = {
    nextFundingTime?: number | string | null;
    ratePeriod?: number | string | null;
};

export type SwingMarketBundle = {
    ticker?: SwingTickerRow | SwingTickerRow[] | null;
    candles?: SwingCandleRow[] | null;
    funding?: SwingFundingRow | SwingFundingRow[] | null;
    fundingTime?: SwingFundingTimeRow | SwingFundingTimeRow[] | null;
};

export type SwingAnalytics = {
    spread?: number | string | null;
    spreadAbs?: number | string | null;
    spreadBps?: number | string | null;
    bestBid?: number | string | null;
    bestAsk?: number | string | null;
    topWalls?: { bid?: unknown; ask?: unknown } | null;
    volume_profile?: Array<{ price?: number | null; volume?: unknown }> | null;
};

// Gate booleans as consumed here (getGates output shape; tests pass partials).
export type SwingGatesInput = {
    regime_trend_up?: boolean | null;
    regime_trend_down?: boolean | null;
    spread_ok?: boolean | null;
    liquidity_ok?: boolean | null;
    atr_ok?: boolean | null;
    slippage_ok?: boolean | null;
};

export type DecisionPolicy = 'strict' | 'balanced';

export type ForexEventContextForPrompt = {
    source?: string;
    pair?: string | null;
    status?: 'clear' | 'active' | 'stale' | string;
    staleData?: boolean;
    reasonCodes?: string[];
    activeEvents?: Array<{
        timestamp_utc?: string;
        currency?: string;
        impact?: string;
        event_name?: string;
        minutesToEvent?: number;
    }>;
    nextEvents?: Array<{
        timestamp_utc?: string;
        currency?: string;
        impact?: string;
        event_name?: string;
        minutesToEvent?: number;
    }>;
    // Just-released events past their blackout window (minutesToEvent < 0);
    // quantified reactions ride separately in market.event_reaction.
    recentEvents?: Array<{
        timestamp_utc?: string;
        currency?: string;
        impact?: string;
        event_name?: string;
        minutesToEvent?: number;
    }>;
};

// Capital-only venue context for the prompt (built in /api/analyze from the
// same /markets/{epic} fetch as the market-closed gate). venue_session is
// present only when the schedule confirms the venue is currently open —
// timestamps are ISO UTC, durations in minutes. overnight_fee_pct_per_day is
// Capital's daily funding adjustment per side (negative = that side pays to
// hold overnight). All prompt prose referencing these fields must stay
// conditional on their presence.
export type CapitalMarketContextForPrompt = {
    venue_session: {
        closes_at_utc: string;
        minutes_to_close: number;
        reopens_at_utc: string | null;
        // Minutes since the current session opened (null when the schedule
        // doesn't expose it). Small values mean gap candles and wide spreads.
        minutes_since_open: number | null;
    } | null;
    // Venue liquidity clock (lib/swing/sessionEvents): cash opens/closes,
    // lunch breaks, Globex halts, weekly thin reopen + derived phase. Present
    // only for session-traded categories; prompt prose must stay conditional.
    venue_events?: {
        venue: string;
        liquidity_phase: string;
        recent: Array<{ event: string; at_utc: string; minutes_ago: number }>;
        upcoming: Array<{ event: string; at_utc: string; minutes_to: number }>;
    } | null;
    overnight_fee_pct_per_day: { long: number | null; short: number | null } | null;
};

export function resolveDecisionPolicy(value?: string | null): DecisionPolicy {
    const raw = String(value ?? process.env.AI_DECISION_POLICY ?? 'strict')
        .trim()
        .toLowerCase();
    return raw === 'balanced' ? 'balanced' : 'strict';
}

// Extension (distance from EMA20, in ATRs) thresholds per decision policy.
// Single source of truth for BOTH the prompt's soft-judgment prose and the
// pre-AI extension hard gate in /api/analyze: beyond `avoid` the prompt tells
// the model to avoid fresh entries — empirically it always HOLDs there, so the
// gate skips the call entirely when flat.
export function resolveExtensionThresholds(policy?: DecisionPolicy | string | null): {
    microAvoid: number;
    microNoEntry: number;
    primaryAvoid: number;
} {
    const strict = resolveDecisionPolicy(typeof policy === 'string' ? policy : (policy ?? undefined)) === 'strict';
    return {
        microAvoid: strict ? 2.5 : 2.8,
        microNoEntry: strict ? 3 : 3.3,
        primaryAvoid: strict ? 2.5 : 2.8,
    };
}

// ------------------------------
// Actionability gate (pre-AI, flat entries)
// ------------------------------
// Derived empirically from the decision history, NOT hand-tuned: across 543 flat
// AI calls the model opened only 3.3% of the time, and those opens were almost
// entirely (a) a confirmed primary structure break, or (b) a bounce off a level
// with room to run and micro structure turning that way. It HOLDs when sandwiched
// between nearby support AND resistance with no break (62% of holds, 0% of opens).
// This gate fires only when a trade is plausible → backtest: 100% recall on opens,
// ~76% fewer AI calls than the old signal_strength≥MEDIUM gate. Thresholds are the
// ATR proximity ("at a level") and room-to-run distance; both env-tunable.
export const ACTIONABILITY_NEAR_ATR = (() => {
    const n = Number(process.env.SWING_ACTIONABILITY_NEAR_ATR);
    return Number.isFinite(n) && n > 0 ? n : 0.6;
})();

export const ACTIONABILITY_ROOM_ATR = (() => {
    const n = Number(process.env.SWING_ACTIONABILITY_ROOM_ATR);
    return Number.isFinite(n) && n > 0 ? n : 1.5;
})();

// A setup pressing within this ATR distance of a near, unbroken MAJOR (context)
// opposing level is rejected (the AI HOLDs those: "confirmed breakdown but
// sitting on major weekly support" and the bullish mirror). Re-validated
// 2026-07-08 over 8 weeks / 800 flat AI calls: at 0.5 the check (applied to
// both the confirmed and bounce branches) skips 72 more S/R-flavored HOLD
// calls than 0.3 and blocks exactly 1 recorded open — a GOLD short into weekly
// support that lost money (pnl_net −11.87, same-day stop). 0.6 skips ~21 more
// but blocks a second open of unknown outcome. Distances measured on the
// CONTEXT timeframe's own ATR (state.location.context_*_dist_atr). Do NOT
// extend this to PRIMARY opposing levels: opens routinely push through those
// (7/39 recorded opens sat <0.3 primary-ATR from one), and a both-sides
// primary pinch (<0.3 each) still contained 5 real opens.
export const ACTIONABILITY_WALL_ATR = (() => {
    const n = Number(process.env.SWING_ACTIONABILITY_WALL_ATR);
    return Number.isFinite(n) && n > 0 ? n : 0.5;
})();

// ------------------------------
// Re-entry cooldown (anti-churn, flat entries)
// ------------------------------
// After a position closes (AI close, auto-close or broker stop), re-entering the
// SAME direction on the same symbol is blocked for this many minutes. Motivated by
// the decision history: same-direction re-opens within hours of a close were fee
// bleed (e.g. 3 NATURALGAS SELLs and 3 US100 BUYs on single days). One primary bar
// (4H) by default; 0 disables.
export const REENTRY_COOLDOWN_MIN = (() => {
    const n = Number(process.env.SWING_REENTRY_COOLDOWN_MIN);
    return Number.isFinite(n) && n >= 0 ? n : 240;
})();

// ------------------------------
// Intraday tactics — flag-gated OFF for the swing model
// ------------------------------
// Preserved (not deleted) for a possible future day-trade model. Both default
// OFF: the swing record showed they were the loss engine — every post-mortem
// to date blamed a resting pullback limit filled at a bare retest, and the
// offensive session playbook is what placed those limits in a sweep's path.
// Session/venue FACTS (schedules, levels, sweep measurements) render regardless
// of these flags; only the tactics prose and mechanisms are gated.
const flagOn = (raw: unknown) => ['1', 'true', 'yes', 'on'].includes(String(raw ?? '').trim().toLowerCase());

export const flagOff = (raw: unknown) => ['0', 'false', 'no', 'off'].includes(String(raw ?? '').trim().toLowerCase());

// Resting pullback-limit entries (entry_limit_price tool + cancelled_pending_entry
// context). Off = market entries only; a model-sent limit drops the entry.
export const PULLBACK_LIMIT_ENABLED = flagOn(process.env.SWING_PULLBACK_LIMIT_ENABLED);

// Offensive session-liquidity playbook (sweep-capture resting entries,
// opening-drive tactics) + the sweep-reclaim re-entry-cooldown exception.
export const SESSION_OFFENSE_ENABLED = flagOn(process.env.SWING_SESSION_OFFENSE_ENABLED);

// In-position wake bands: the model declares price levels INSIDE its bracket
// at which the 1-min watcher fires an early management look ("wake me if we
// lose 3.42 support") instead of waiting for the next primary bar close.
// Purely additive — suppresses nothing; replaced by every real in-position AI
// call (null = cleared). Gates the prompt prose, the eligibility routing in
// normalizeDecision, and the analyze/watcher wiring. Default ON — set
// ENABLE_POSITION_WAKE_BANDS=false to deactivate (opt-out, not opt-in).
export const POSITION_WAKE_ENABLED = !flagOff(process.env.ENABLE_POSITION_WAKE_BANDS);

// Min band distance from current price in primary-ATR units — the churn guard:
// a band glued to price would re-fire a full AI call every ~5 min (the
// watcher's fired-marker TTL).
export const POSITION_WAKE_MIN_ATR = (() => {
    const n = Number(process.env.SWING_POSITION_WAKE_MIN_ATR);
    return Number.isFinite(n) && n > 0 ? n : 0.3;
})();

export type LastClosedPosition = {
    side: 'long' | 'short';
    exitTsMs: number;
};

export type ActionabilityInputs = {
    microEntryOk: boolean;
    primaryBreakoutConfirmed: boolean;
    primaryBreakdownConfirmed: boolean;
    primaryBreakoutRetestOk: boolean;
    primaryBreakoutRetestDir?: string | null;
    primaryBos: boolean;
    primaryBosDir?: string | null;
    primaryBreakState?: string | null; // 'above' | 'below' | 'inside'
    primarySupportDistAtr?: number | null;
    primaryResistanceDistAtr?: number | null;
    microBreakoutRetestOk: boolean;
    microBreakoutRetestDir?: string | null;
    microBos: boolean;
    microBosDir?: string | null;
    microBreakState?: string | null;
    // major (context/weekly) opposing wall — used to reject confirmed setups that
    // press straight into a near, unbroken higher-timeframe level.
    contextSupportDistAtr?: number | null;
    contextSupportState?: string | null;
    contextResistanceDistAtr?: number | null;
    contextResistanceState?: string | null;
};

export type PromptDecisionContext = {
    // Populated on the context returned by computeSwingState; absent on the input passed
    // to computeSignalStrength (which produces it).
    signal_strength?: 'LOW' | 'MEDIUM' | 'HIGH';
    micro_bias_calc: string;
    primary_bias: string;
    macro_bias: string;
    context_bias: string;
    primary_trend_up: boolean;
    primary_trend_down: boolean;
    primary_breakdown_confirmed: boolean;
    primary_breakout_confirmed: boolean;
    micro_entry_ok: boolean;
    aligned_driver_count: number;
    regime_alignment: number;
    location_confluence_score: number;
    micro_extension_atr: number | null;
    primary_extension_atr: number | null;
    breakout_retest_ok_primary: boolean;
    breakout_retest_dir_primary: string | null;
    forex_session_context?: ForexSessionLevelsContext | null;
};

// AI-requested quiet period on a flat symbol. Bounds are sized for the
// 4H-close cadence: the floor (default 360 = 6h) guarantees any cooldown
// suppresses at least the NEXT bar-close evaluation — anything shorter than
// one bar expires before the next look and does nothing, and exactly 4h would
// race the close-time cron jitter. The ceiling (default 1440 = one day, up to
// six evaluations) exists because the wake bands only cover PRICE — a cooldown
// is blind to news, session flips and regime changes, so it must stay
// renewable rather than open-ended. Renewal is cheap (one gated call per
// cooldown). Legacy 15-min cadence (SWING_EVAL_PRIMARY_CLOSE_ONLY=0): set
// SWING_AI_COOLDOWN_MIN_MIN=15 to restore short cooldowns.
export const HOLD_COOLDOWN_MIN_MINUTES = (() => {
    const n = Number(process.env.SWING_AI_COOLDOWN_MIN_MIN);
    return Number.isFinite(n) && n >= 1 ? Math.round(n) : 360;
})();

export const HOLD_COOLDOWN_MAX_MINUTES = (() => {
    const n = Number(process.env.SWING_AI_COOLDOWN_MAX_MIN);
    return Number.isFinite(n) && n >= HOLD_COOLDOWN_MIN_MINUTES ? Math.round(n) : Math.max(1440, HOLD_COOLDOWN_MIN_MINUTES);
})();

// Entry TP fallback mirrors the 3×ATR catastrophe stop in /api/analyze, so an
// entry the model leaves without a target still gets a symmetric (~1:1 R)
// exchange-side bracket instead of an unbounded upside leg.
export const EXCHANGE_TP_FALLBACK_ATR_MULT = 3;

// A stop may never sit wider than the catastrophe distance from CURRENT price,
// and an amendment may never sit further from price than the STANDING stop —
// amendments tighten protection, never loosen it (blocks walking the stop away
// on a losing position).
export const EXCHANGE_SL_MAX_ATR_MULT = 3;

// Swing floors: a 1–10 day hold has to survive many 4H bars, so a target
// closer than 2 primary-ATR isn't a swing target and a stop inside 1 ATR sits
// in routine oscillation (live record: 0.46%-avg stops were swept in minutes —
// trades <1h old carried the entire system loss).
export const ENTRY_TP_MIN_ATR = 2;

// An entry stop closer than this is inside ordinary bar noise and would likely
// be wicked out immediately — dropped in favour of the catastrophe default.
export const ENTRY_SL_MIN_ATR = 1;

export const TP_MAX_ATR = 10;

export const AMEND_MIN_GAP_ATR = 0.1;

// A pullback limit must be a genuine pullback: at least MIN_ATR below (BUY) /
// above (SELL) current price. An invalid limit (wrong side, inside the noise
// band, or unverifiable without ATR) DROPS the entry for this tick instead of
// silently converting to a market order — the model asked for a patience
// price, and filling it at market is exactly the chase the prompt forbids
// (null from the model is the only way to request market). Beyond MAX_ATR the
// fill odds within the one-tick TTL are negligible and the bracket math
// distorts, so it clamps.
export const ENTRY_LIMIT_MIN_ATR = 0.1;

export const ENTRY_LIMIT_MAX_ATR = 1.5;
