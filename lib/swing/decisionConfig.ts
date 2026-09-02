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
    // Default flipped strict -> balanced 2026-09-02. `strict` existed mainly to
    // remove the trend guard's exception (the guard is gone) and to tighten the
    // anti-flip lookback (also gone). What it still changes: the extension
    // thresholds below, and forcing CLOSE rather than HOLD when base gates fail
    // in a position — and forcing an exit into a market whose spread/liquidity
    // just failed is the worse of the two.
    const raw = String(value ?? process.env.AI_DECISION_POLICY ?? 'balanced')
        .trim()
        .toLowerCase();
    return raw === 'balanced' ? 'balanced' : 'strict';
}

// Extension (distance from EMA20, in ATRs) thresholds per decision policy.
// Sole consumer since 2026-09-02: the pre-AI extension hard gate in
// /api/analyze. It used to also feed prompt prose ("avoid fresh entries beyond
// X") — that prose is gone, so these numbers are no longer stated to the model
// and nothing in the prompt can drift from them.
//
// NOTE: the strict -> balanced default flip (resolveDecisionPolicy) moves every
// number here, so it loosened the extension gate 2.5 -> 2.8 ATR as a side
// effect. Deliberate but incidental: it was not part of the case for the flip,
// and it is the one behavioural change the flip makes on ticks that never reach
// the model at all.
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
//
// Narrowed 2026-08-30 to structure only. It used to also veto on entry timing
// (micro_entry_ok) and on pressing into a near context wall; both premises
// assumed a market fill was the only way in, so both deleted the answer the
// model can now give — rest an order at the level instead of paying it. The
// sandwiched/no-break branch is what carries the gate in practice (78% of its
// skips); the wall branches are reported in the reason but no longer reject.
export const ACTIONABILITY_NEAR_ATR = (() => {
    const n = Number(process.env.SWING_ACTIONABILITY_NEAR_ATR);
    return Number.isFinite(n) && n > 0 ? n : 0.6;
})();

export const ACTIONABILITY_ROOM_ATR = (() => {
    const n = Number(process.env.SWING_ACTIONABILITY_ROOM_ATR);
    return Number.isFinite(n) && n > 0 ? n : 1.5;
})();

// Door 4 (geometry): how close to the channel edge counts as "at the edge".
// channel_pos is 0 at the channel low and 1 at the high, so 0.15 admits the
// outer ~15% at either end — a pullback to the channel floor in an up-slope,
// or its mirror. Deliberately tighter than a "wave position" read: this only
// decides whether the tick is worth a call, not whether the trade is good.
export const ACTIONABILITY_CHANNEL_EDGE = (() => {
    const n = Number(process.env.SWING_ACTIONABILITY_CHANNEL_EDGE);
    return Number.isFinite(n) && n > 0 && n < 0.5 ? n : 0.15;
})();

// A setup pressing within this ATR distance of a near, unbroken MAJOR (context)
// opposing level. This NO LONGER REJECTS — since 2026-08-30 the wall branches
// return actionable:true and only name the wall in the reason, so the trail
// stays queryable and the model weighs the wall itself (it reaches the prompt
// as location.context_*_dist_atr). The threshold now only decides which of two
// actionable reasons gets recorded. The measurement that once justified
// rejecting, kept because it is what would have to be re-argued to bring the
// rejection back: re-validated
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
// Default 240 -> 0 (off) 2026-09-02. The anti-churn case was real (repeat
// same-day opens bled fees) but it is a blanket directional block: it also eats
// the reclaim re-entry after a stop-out, which is the highest-edge version of
// the same trade, and its exception is gated behind SWING_SESSION_OFFENSE_ENABLED
// which is off for swing. market.recent_actions already shows the model its own
// recent closes and their PnL, so it can decline a churn re-entry itself.
// Set SWING_REENTRY_COOLDOWN_MIN=240 to restore it if churn reappears.
export const REENTRY_COOLDOWN_MIN = (() => {
    const n = Number(process.env.SWING_REENTRY_COOLDOWN_MIN);
    return Number.isFinite(n) && n >= 0 ? n : 0;
})();

// ------------------------------
// Intraday tactics — flag-gated OFF for the swing model
// ------------------------------
// Preserved (not deleted) for a possible future day-trade model. Defaults
// OFF: the offensive session playbook is what placed resting limits in a
// sweep's path, and every post-mortem to date blamed a limit filled at a bare
// retest. Note what that verdict is ABOUT — WHERE the model was told to rest
// orders, not whether resting orders should exist. The tool/strategy split
// below is the correction: the hand is always handed over, the playbook is not.
// Session/venue FACTS (schedules, levels, sweep measurements) render regardless
// of these flags; only the tactics prose and mechanisms are gated.
const flagOn = (raw: unknown) => ['1', 'true', 'yes', 'on'].includes(String(raw ?? '').trim().toLowerCase());

export const flagOff = (raw: unknown) => ['0', 'false', 'no', 'off'].includes(String(raw ?? '').trim().toLowerCase());

// ------------------------------
// Resting entry orders — the TOOL, not a strategy
// ------------------------------
// A resting entry is an order parked away from live price. Two kinds, defined
// purely by geometry relative to the trade's direction:
//
//   limit — rests AGAINST the trade (BUY below price, SELL above). Fills when
//           price comes back to you. Adversely selected: whoever fills you was
//           willing to trade through your level.
//   stop  — rests WITH the trade (BUY above price, SELL below). Triggers when
//           price goes your way. Favourably selected, but pays the spread and
//           is triggered by sweeps.
//
// Neither is a strategy. "Pullback", "breakout", "range fade", "level bounce"
// are strategies, and which one to run is the model's call from the data — it
// declares its pick in `strategy` (decisionSchema) so the choice is measurable.
// Code owns only the ENVELOPE (side validity, distance windows, TTL) and the
// venue plumbing. Both kinds are handed over unconditionally.
export type RestingEntryKind = 'limit' | 'stop';

export type RestingEntryMode = 'off' | 'limit' | 'stop' | 'both';

// Deliberately a constant, not an env read: the tools are part of the model's
// permanent surface, not a rollout knob. Narrow it here to disable a kind.
export const RESTING_ENTRY_MODE: RestingEntryMode = 'both';

// Per-venue order-type support. Capital rests both kinds on the documented
// /workingorders endpoint (`type` is LIMIT or STOP). Bitget needs two distinct
// order books: plain `place-order` for limits, `place-plan-order`
// (planType normal_plan) for stops. A kind absent here drops the entry rather
// than degrading to another order type — a stop silently placed as a limit is
// the OPPOSITE trade.
// Capital: /workingorders documents `type` as exactly LIMIT or STOP, and both
// rest, list and cancel through the same endpoint the pipeline already drives.
//
// Bitget: validated end-to-end on DEMO 2026-08-30 via
// scripts/validate-bitget-stop-entry.ts (20/20) — place-plan-order accepted,
// the venue stored trigger/side/size and BOTH bracket legs, our mapper reads
// the plan row, ONE sweep returns both order books, the sweep clears both, and
// a fired trigger opened the position with its TP/SL attached.
//
// That run also caught the bug this gate existed for: place-plan-order ACCEPTS
// presetStopLossPrice / presetStopSurplusPrice (the place-order names) and
// silently discards them — no error, order rests normally. Shipping on the
// docs-derived body would have opened every triggered crypto stop entry NAKED.
// The plan book's own names (stopLossTriggerPrice / stopSurplusTriggerPrice)
// are what stick; see the comment in executeDecision.
//
// A venue absent a kind here DROPS that entry in the sanitizer rather than
// degrading it to the other order type — a stop placed as a limit is the
// opposite trade.
export const RESTING_ENTRY_VENUE_SUPPORT: Record<string, readonly RestingEntryKind[]> = {
    capital: ['limit', 'stop'],
    bitget: ['limit', 'stop'],
};

export function restingEntryKindAllowed(kind: RestingEntryKind, platform?: string | null): boolean {
    if (RESTING_ENTRY_MODE !== 'both' && RESTING_ENTRY_MODE !== kind) return false;
    const key = String(platform || '').trim().toLowerCase();
    if (!key) return true;
    const supported = RESTING_ENTRY_VENUE_SUPPORT[key];
    return supported ? supported.includes(kind) : true;
}

export function restingEntryKindsFor(platform?: string | null): RestingEntryKind[] {
    return (['limit', 'stop'] as const).filter((kind) => restingEntryKindAllowed(kind, platform));
}

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
    // Primary wave geometry — the anchor/geometry doors (see evaluateActionability).
    // Distances are ABSOLUTE primary-ATR from current price.
    primaryChannelPos?: number | null;
    primarySupportTrendlineDistAtr?: number | null;
    primaryResistanceTrendlineDistAtr?: number | null;
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

// (No stop-WIDTH ceiling. EXCHANGE_SL_MAX_ATR_MULT = 3 capped how far a stop
// could sit from price — a leftover from stop-blind fixed-notional sizing.
// Sizing is now risk-based (notional = riskUsd / stopDistancePct), so a wider
// stop is already a proportionally smaller position at identical dollar risk:
// the cap did no risk work, it only overrode structure. It never fired once in
// 2,435 calls. Amendments are still tighten-only, which is the guard that
// actually blocks walking a stop away from a losing position.)

// Bracket geometry is the MODEL's, not ours. The two floors that used to sit
// here — ENTRY_TP_MIN_ATR=2 and ENTRY_SL_MIN_ATR=1 — were removed 2026-09-02.
// Both looked like risk control and were really strategy: together they made any
// trade inside a sub-2-ATR range inexpressible, which is the breakout bias
// written as arithmetic. Measured over 45 days (docs/measured-hold-causes.md):
// 84.5% of AI calls were HOLD, 23.8% naming room/reward/target as the blocker —
// one verbatim, "limiting a compliant swing target". They also failed backwards:
// a violating leg was DROPPED and replaced by the wide default, so asking for a
// near target produced a far one.
//
// What code still owns after this is exactly three things — legs on the correct
// side of price, a noise floor (BRACKET_MIN_GAP_ATR), and tighten-only stop
// amends. All three are mechanical facts about the venue, not views on the
// trade. Whether a target pays for its stop is a judgment made with the levels
// in view, so it sits with the model, which has them.
//
// Sizing absorbs the tight stops this admits: notional = riskUsd /
// stopDistancePct is capped at EXPOSURE_CAP_EQUITY_MULT× equity (riskSizing.ts),
// a ceiling that already bound at the old 1-ATR floor, so removing it lowers
// realized risk rather than raising it.

// (No TP ceiling either. TP_MAX_ATR = 10 never fired, and a target too far to
// reach is inert rather than dangerous — it simply does not fill.)

// The only distance a bracket leg must clear, entry or amendment: enough to be
// a price rather than noise sitting on top of the current print. Not a view on
// whether the target is any good — that judgment is the model's.
export const BRACKET_MIN_GAP_ATR = 0.1;

// Resting-entry distance envelope, in primary ATR from live price. An invalid
// resting price (wrong side for its kind, inside the noise band, or
// unverifiable without ATR) DROPS the entry for this tick instead of silently
// converting to a market order — the model asked for a specific price, and
// filling it at market is a different trade than the one it decided on (null
// from the model is the only way to request market). Beyond MAX the level is so
// far from price that the bracket math distorts, so it clamps rather than drops
// — a correction, not a rejection.
export const ENTRY_LIMIT_MIN_ATR = 0.1;

export const ENTRY_LIMIT_MAX_ATR = 1.5;

// A stop-entry needs more clearance than a limit: it is TRIGGERED by the noise
// it sits in rather than filled by it, so a stop parked as close as a limit
// may fire on the current bar's wick before the move it is waiting for exists.
export const ENTRY_STOP_MIN_ATR = 0.25;

export const ENTRY_STOP_MAX_ATR = 1.5;

export const RESTING_ENTRY_WINDOWS: Record<RestingEntryKind, { minAtr: number; maxAtr: number }> = {
    limit: { minAtr: ENTRY_LIMIT_MIN_ATR, maxAtr: ENTRY_LIMIT_MAX_ATR },
    stop: { minAtr: ENTRY_STOP_MIN_ATR, maxAtr: ENTRY_STOP_MAX_ATR },
};

// A resting entry survives evaluations (it is a standing commitment — see the
// resting_entry handling in /api/analyze), so nothing else bounds its life if
// this pipeline stops running. This is the backstop under our OWN outages, not
// a view on how long an idea stays good: a stop firing two days late on a
// thesis nobody re-checked is the failure it exists to prevent. The model
// withdraws or supersedes long before this on any tick that actually runs.
export const RESTING_ENTRY_MAX_AGE_MINUTES = 48 * 60;
