// lib/ai.ts

import {
    AI_BASE_URL,
    AI_MODEL,
    CONTEXT_TIMEFRAME,
    DEFAULT_TAKER_FEE_RATE,
    MACRO_TIMEFRAME,
    MICRO_TIMEFRAME,
    PRIMARY_TIMEFRAME,
    TRADE_WINDOW_MINUTES,
} from './constants';
import type { MultiTFIndicators } from './indicators';
import type { EventReactionMeasurement } from './swing/eventReaction';
import type { BtcContext } from './swing/btcContext';
import type { ForexSessionLevelsContext } from './swing/sessionLevels';
import type { RecentActionEntry } from './swing/recentActions';
import { computeWaveGeometry } from './swing/waveGeometry';
import type { NanoContext } from './swing/waveGeometry';
import { setEvaluation, getEvaluation } from './utils';

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
    info?: Record<string, any>;
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

const indicatorRegexCache = new Map<string, RegExp>();

function readIndicator(name: string, src: string): number | null {
    if (!src) return null;
    if (!indicatorRegexCache.has(name)) {
        indicatorRegexCache.set(name, new RegExp(`${name}=([+-]?[0-9]*\.?[0-9]+)`));
    }
    const regex = indicatorRegexCache.get(name)!;
    const match = src.match(regex);
    if (!match) return null;
    const val = Number(match[1]);
    return Number.isFinite(val) ? val : null;
}

function distanceOk(price: number, target: number | null, atr: number | null) {
    if (!Number.isFinite(price) || !Number.isFinite(target as number)) return false;
    const threshold = Number.isFinite(atr as number) && (atr as number) > 0 ? (atr as number) * 0.8 : price * 0.0015;
    return Math.abs(price - (target as number)) <= threshold;
}

function microDistanceOk(price: number, target: number | null, atr: number | null) {
    if (!Number.isFinite(price) || !Number.isFinite(target as number)) return false;
    const threshold = Number.isFinite(atr as number) && (atr as number) > 0 ? (atr as number) * 1.2 : price * 0.0008;
    return Math.abs(price - (target as number)) <= threshold;
}

export function computeMomentumSignals(params: {
    price: number;
    indicators: MultiTFIndicators;
    gates: { regime_trend_up: boolean; regime_trend_down: boolean };
    primaryTimeframe: string;
}): MomentumSignals {
    const { price, indicators, gates, primaryTimeframe } = params;
    const macroSummary = indicators.macro || '';
    const microSummary = indicators.micro || '';
    const primarySummary = indicators.primary?.summary || '';
    const primaryTf = String(primaryTimeframe || '').trim();
    const microTf = String(indicators.microTimeFrame || '').trim();
    const primaryMetrics = primaryTf ? indicators.metrics?.[primaryTf] : undefined;
    const microMetrics = microTf ? indicators.metrics?.[microTf] : undefined;

    const ema50Macro = readIndicator('EMA50', macroSummary);
    const ema50Primary = readIndicator('EMA50', primarySummary);
    const ema20Primary = readIndicator('EMA20', primarySummary);
    const ema20Micro = readIndicator('EMA20', microSummary);
    const atrPrimaryMetric = Number(primaryMetrics?.atr);
    const atrMicroMetric = Number(microMetrics?.atr);
    const atrPrimary = Number.isFinite(atrPrimaryMetric) && atrPrimaryMetric > 0 ? atrPrimaryMetric : readIndicator('ATR', primarySummary);
    const atrMicro = Number.isFinite(atrMicroMetric) && atrMicroMetric > 0 ? atrMicroMetric : readIndicator('ATR', microSummary);
    const rsiPrimary = readIndicator('RSI', primarySummary);
    const rsiMicro = readIndicator('RSI', microSummary);
    const slopePrimary = readIndicator('slopeEMA21_10', primarySummary);

    const macroTrendUp = Boolean(gates.regime_trend_up);
    const macroTrendDown = Boolean(gates.regime_trend_down);

    const priceAbovePrimary50 = Number.isFinite(ema50Primary as number) ? price >= (ema50Primary as number) : true;
    const priceBelowPrimary50 = Number.isFinite(ema50Primary as number) ? price <= (ema50Primary as number) : true;

    const rsiPullbackLong = Number.isFinite(rsiPrimary as number)
        ? (rsiPrimary as number) >= 35 && (rsiPrimary as number) <= 50
        : false;
    const rsiPullbackShort = Number.isFinite(rsiPrimary as number)
        ? (rsiPrimary as number) >= 50 && (rsiPrimary as number) <= 65
        : false;

    const slopeUp = Number.isFinite(slopePrimary as number) ? (slopePrimary as number) > 0 : false;
    const slopeDown = Number.isFinite(slopePrimary as number) ? (slopePrimary as number) < 0 : false;

    const nearPrimary = distanceOk(price, ema20Primary, atrPrimary);
    const nearMicro = microDistanceOk(price, ema20Micro, atrMicro);
    const microOversold = Number.isFinite(rsiMicro as number) ? (rsiMicro as number) <= 40 : false;
    const microOverbought = Number.isFinite(rsiMicro as number) ? (rsiMicro as number) >= 60 : false;
    const microEntryOk = nearPrimary || nearMicro || microOversold || microOverbought;

    const longMomentum = priceAbovePrimary50 && rsiPullbackLong && slopeUp;
    const shortMomentum = priceBelowPrimary50 && rsiPullbackShort && slopeDown;

    const microExtensionInAtr =
        Number.isFinite(atrMicro as number) && (atrMicro as number) > 0 && Number.isFinite(ema20Micro as number)
            ? (price - (ema20Micro as number)) / (atrMicro as number)
            : null;

    return {
        macroTrendUp,
        macroTrendDown,
        longMomentum,
        shortMomentum,
        nearPrimaryEMA20: nearPrimary,
        nearMicroEMA20: nearMicro,
        primaryRSI: rsiPrimary,
        primarySlope: slopePrimary,
        microRSI: rsiMicro,
        primaryAtr: atrPrimary,
        microExtensionInAtr,
        info: {
            primaryTimeframe,
            microEntryOk,
        },
    };
}

// Persist the last evaluation for a symbol
export async function persistEvaluation(symbol: string, evaluation: any) {
    await setEvaluation(symbol, evaluation);
}

// Retrieve the last evaluation for a symbol
export async function getLastEvaluation(symbol: string) {
    return getEvaluation(symbol);
}

// ------------------------------
// Prompt Builder (with guardrails, regime, momentum & extension gates)
// ------------------------------

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
const ACTIONABILITY_NEAR_ATR = (() => {
    const n = Number(process.env.SWING_ACTIONABILITY_NEAR_ATR);
    return Number.isFinite(n) && n > 0 ? n : 0.6;
})();
const ACTIONABILITY_ROOM_ATR = (() => {
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
const ACTIONABILITY_WALL_ATR = (() => {
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
// Resting pullback-limit entries (entry_limit_price tool + cancelled_pending_entry
// context). Off = market entries only; a model-sent limit drops the entry.
export const PULLBACK_LIMIT_ENABLED = flagOn(process.env.SWING_PULLBACK_LIMIT_ENABLED);
// Offensive session-liquidity playbook (sweep-capture resting entries,
// opening-drive tactics) + the sweep-reclaim re-entry-cooldown exception.
export const SESSION_OFFENSE_ENABLED = flagOn(process.env.SWING_SESSION_OFFENSE_ENABLED);

export type LastClosedPosition = {
    side: 'long' | 'short';
    exitTsMs: number;
};

// The cooldown that applies to a flat tick right now, or null when inactive.
export function resolveReentryCooldown(
    lastClosed: LastClosedPosition | null | undefined,
    nowMs = Date.now(),
): { blockedSide: 'long' | 'short'; minutesLeft: number } | null {
    if (!lastClosed || REENTRY_COOLDOWN_MIN <= 0) return null;
    const elapsedMin = (nowMs - lastClosed.exitTsMs) / 60_000;
    if (!(elapsedMin >= 0) || elapsedMin >= REENTRY_COOLDOWN_MIN) return null;
    return { blockedSide: lastClosed.side, minutesLeft: Math.ceil(REENTRY_COOLDOWN_MIN - elapsedMin) };
}

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

export function evaluateActionability(x: ActionabilityInputs): { actionable: boolean; reason: string } {
    // Entry timing is a hard prerequisite (all opens had it).
    if (!x.microEntryOk) return { actionable: false, reason: 'micro_entry_ok_false' };
    // (a) confirmed primary structure — the universal opener (17/18 opens, all asset classes).
    const confirmed =
        x.primaryBreakoutConfirmed ||
        x.primaryBreakdownConfirmed ||
        x.primaryBreakoutRetestOk ||
        x.primaryBos ||
        (!!x.primaryBreakState && x.primaryBreakState !== 'inside');
    if (confirmed) {
        // ...but skip if the confirmed direction presses straight into a NEAR, UNBROKEN
        // MAJOR (context/weekly) opposing wall — the AI reliably HOLDs "confirmed
        // breakdown but sitting on major weekly support" (and the bullish mirror).
        // Validated: at 0.3 ATR this drops 104 such HOLD-calls with 0 opens lost.
        // Scoped to CONTEXT levels (opens push through primary levels) and to unbroken
        // walls (a broken/retesting level is no longer in the way).
        const confDown =
            x.primaryBreakdownConfirmed ||
            (x.primaryBreakoutRetestOk && x.primaryBreakoutRetestDir === 'down') ||
            (x.primaryBos && x.primaryBosDir === 'down') ||
            x.primaryBreakState === 'below';
        const confUp =
            x.primaryBreakoutConfirmed ||
            (x.primaryBreakoutRetestOk && x.primaryBreakoutRetestDir === 'up') ||
            (x.primaryBos && x.primaryBosDir === 'up') ||
            x.primaryBreakState === 'above';
        const dir = confDown && !confUp ? 'down' : confUp && !confDown ? 'up' : null;
        const blocking = (s?: string | null) => !!s && s !== 'broken' && s !== 'retesting';
        const csd = Number.isFinite(x.contextSupportDistAtr as number) ? (x.contextSupportDistAtr as number) : null;
        const crd = Number.isFinite(x.contextResistanceDistAtr as number) ? (x.contextResistanceDistAtr as number) : null;
        const intoWall =
            (dir === 'down' && csd != null && csd <= ACTIONABILITY_WALL_ATR && blocking(x.contextSupportState)) ||
            (dir === 'up' && crd != null && crd <= ACTIONABILITY_WALL_ATR && blocking(x.contextResistanceState));
        if (intoWall) return { actionable: false, reason: 'into_context_wall' };
        return { actionable: true, reason: 'confirmed_primary_structure' };
    }
    // (b) tight bounce — at one level, opposite level far (room to run), micro turning that way.
    const sup = Number.isFinite(x.primarySupportDistAtr as number) ? (x.primarySupportDistAtr as number) : null;
    const res = Number.isFinite(x.primaryResistanceDistAtr as number) ? (x.primaryResistanceDistAtr as number) : null;
    const microUp =
        (x.microBreakoutRetestOk && x.microBreakoutRetestDir === 'up') ||
        (x.microBos && x.microBosDir === 'up') ||
        x.microBreakState === 'above';
    const microDown =
        (x.microBreakoutRetestOk && x.microBreakoutRetestDir === 'down') ||
        (x.microBos && x.microBosDir === 'down') ||
        x.microBreakState === 'below';
    const longBounce =
        sup != null && res != null && sup <= ACTIONABILITY_NEAR_ATR && res >= ACTIONABILITY_ROOM_ATR && microUp;
    const shortBounce =
        sup != null && res != null && res <= ACTIONABILITY_NEAR_ATR && sup >= ACTIONABILITY_ROOM_ATR && microDown;
    // Same context-wall rejection as the confirmed branch: a bounce whose room
    // runs straight into a near, unbroken MAJOR (context/weekly) wall is a HOLD.
    // (The primary opposing level is already required to be ≥ ROOM away above.)
    const blockingBounce = (s?: string | null) => !!s && s !== 'broken' && s !== 'retesting';
    const bounceCsd = Number.isFinite(x.contextSupportDistAtr as number) ? (x.contextSupportDistAtr as number) : null;
    const bounceCrd = Number.isFinite(x.contextResistanceDistAtr as number) ? (x.contextResistanceDistAtr as number) : null;
    if (
        longBounce &&
        bounceCrd != null &&
        bounceCrd <= ACTIONABILITY_WALL_ATR &&
        blockingBounce(x.contextResistanceState)
    ) {
        return { actionable: false, reason: 'bounce_into_context_wall' };
    }
    if (
        shortBounce &&
        bounceCsd != null &&
        bounceCsd <= ACTIONABILITY_WALL_ATR &&
        blockingBounce(x.contextSupportState)
    ) {
        return { actionable: false, reason: 'bounce_into_context_wall' };
    }
    if (longBounce) return { actionable: true, reason: 'bounce_long' };
    if (shortBounce) return { actionable: true, reason: 'bounce_short' };
    // sandwiched / no break → the AI HOLDs these; skip the call.
    return { actionable: false, reason: 'boxed_or_unconfirmed' };
}

// Derivation half of the old buildPrompt: computes signal_strength + the decision
// context (cheap), and returns an `assemble(news)` closure that builds the actual
// STATE/MARKET prompt strings (the expensive JSON.stringify + template work).
// Callers can read the actionability gate BEFORE assembling — so non-actionable
// flat ticks never pay for prompt assembly or the news fetch. News is not needed here.
export function computeSwingState(
    symbol: string,
    timeframe: string,
    bundle: any,
    analytics: any,
    position_status: string = 'none',
    forex_event_context: ForexEventContextForPrompt | null = null,
    forex_session_context: ForexSessionLevelsContext | null = null,
    indicators: MultiTFIndicators,
    gates: any, // <--- Retain the gates object for the base gate checks
    position_context: PositionContext | null = null,
    momentumSignalsOverride?: MomentumSignals,
    recentActions: RecentActionEntry[] = [],
    realizedRoiPct?: number | null,
    dryRun?: boolean,
    spreadBpsOverride?: number,
    decisionPolicy?: DecisionPolicy,
    category?: string | null,
    platform?: string | null,
    lastClosedPosition?: LastClosedPosition | null,
    nowMs?: number,
    capitalMarketContext?: CapitalMarketContextForPrompt | null,
    // Set when this evaluation exists because price crossed the wake band the
    // model attached to its previous flat HOLD cooldown (the caller bypasses
    // the flat quality gates for these ticks). Surfaces as market.cooldown_wake.
    cooldownWake?: { crossed: 'above' | 'below'; level: number; setAtMs: number | null } | null,
) {
    const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
    const price = Number(t?.lastPr ?? t?.last ?? t?.close ?? t?.price);
    const change = Number(t?.change24h ?? t?.changeUtc24h ?? t?.chgPct);
    const last = price; // Use price as 'last'
    const microTimeframe = indicators.microTimeFrame ?? MICRO_TIMEFRAME;
    const macroTimeframe = indicators.macroTimeFrame ?? MACRO_TIMEFRAME;
    const primaryTimeframe = indicators.primary?.timeframe ?? PRIMARY_TIMEFRAME;
    const contextTimeframe = indicators.context?.timeframe ?? indicators.contextTimeFrame ?? CONTEXT_TIMEFRAME;
    const momentumSignals =
        momentumSignalsOverride ??
        computeMomentumSignals({
            price: last,
            indicators,
            gates,
            primaryTimeframe,
        });
    // Wave geometry (regression channel, pivot trendlines, last swing points)
    // on the entry-relevant timeframes, from candles already fetched for the
    // indicators — zero extra I/O. Nano (15m) geometry is fetched separately by
    // the caller only once all gates pass, and enters via assemble().
    const microGeometry = computeWaveGeometry(indicators.rawCandles?.[microTimeframe]);
    const primaryGeometry = computeWaveGeometry(indicators.rawCandles?.[primaryTimeframe]);
    const spreadAbsRaw = Number(analytics?.spreadAbs ?? analytics?.spread);
    const spreadBpsFromAnalytics = Number(analytics?.spreadBps);
    const spreadBpsCanonical = Number.isFinite(spreadBpsOverride as number)
        ? Number(spreadBpsOverride)
        : Number.isFinite(spreadBpsFromAnalytics)
          ? spreadBpsFromAnalytics
          : Number.isFinite(spreadAbsRaw) && last > 0
            ? (spreadAbsRaw / last) * 1e4
            : 999;
    const bestBidRaw = Number(analytics?.bestBid);
    const bestAskRaw = Number(analytics?.bestAsk);
    const candles = Array.isArray(bundle.candles) ? bundle.candles : [];
    const priceTrendPoints = candles
        .slice(-5)
        .map((c: any) => {
            const tsRaw = Number(c?.[0]);
            if (!Number.isFinite(tsRaw)) return null;
            const toNum = (v: any) => {
                const n = Number(v);
                return Number.isFinite(n) ? Number(n.toFixed(6)) : null;
            };
            const open = toNum(c?.[1]);
            const high = toNum(c?.[2]);
            const low = toNum(c?.[3]);
            const close = toNum(c?.[4]);
            const volume = toNum(c?.[5] ?? c?.volume);
            const tsMs = tsRaw > 1e12 ? tsRaw : tsRaw * 1000;
            return {
                ts: new Date(tsMs).toISOString(),
                open,
                high,
                low,
                close,
                volume,
            };
        })
        .filter((p: any) => p !== null);

    const recentActionsExists = Array.isArray(recentActions) && recentActions.length > 0;
    const actionsToShow = recentActionsExists ? Math.min(recentActions.length, 5) : 5;
    const sr = indicators.sr || {};
    const primarySR = sr[primaryTimeframe] ?? sr[indicators.primary?.timeframe || primaryTimeframe];
    const contextSR = sr[contextTimeframe] ?? sr[indicators.context?.timeframe || contextTimeframe];

    const contextSummary = indicators.context?.summary ?? '';
    const contextBias = contextSummary.includes('trend=up')
        ? 'UP'
        : contextSummary.includes('trend=down')
          ? 'DOWN'
          : 'NEUTRAL';

    // ---- Extract indicators for raw metrics (Micro, Macro, Primary) ----
    const micro = indicators.micro || '';
    const macro = indicators.macro || '';
    const primary = indicators.primary?.summary || '';

    // Technical values we want the AI to judge (values, not booleans)
    const ema20_micro = readIndicator('EMA20', micro);
    const ema20_primary = readIndicator('EMA20', primary);
    const slope21_micro = readIndicator('slopeEMA21_10', micro) ?? 0; // % per bar
    const slope21_primary = readIndicator('slopeEMA21_10', primary) ?? 0; // % per bar
    const atr_micro = readIndicator('ATR', micro);
    const atr_macro = readIndicator('ATR', macro);
    const atr_primary = readIndicator('ATR', primary);
    const rsi_micro = readIndicator('RSI', micro);
    const rsi_macro = readIndicator('RSI', macro);
    const rsi_primary = readIndicator('RSI', primary);
    const htfSupportDist = contextSR?.support?.dist_in_atr;
    const htfResistanceDist = contextSR?.resistance?.dist_in_atr;
    let intoContextSupport = false;
    let intoContextResistance = false;
    if (Number.isFinite(htfSupportDist as number)) {
        intoContextSupport = (htfSupportDist as number) < 0.6;
    }
    if (Number.isFinite(htfResistanceDist as number)) {
        intoContextResistance = (htfResistanceDist as number) < 0.6;
    }
    const chopRisk = intoContextSupport && intoContextResistance;
    const htfBreakdownConfirmed = contextSR?.support?.level_state === 'broken';
    const htfBreakoutConfirmed = contextSR?.resistance?.level_state === 'broken';

    // --- KEY METRICS (VALUES, NOT JUDGMENTS) ---
    const atr_pct_macro = last > 0 && atr_macro ? (atr_macro / last) * 100 : 0;
    const atr_pct_primary = last > 0 && atr_primary ? (atr_primary / last) * 100 : 0;

    // Calculate extension (distance from EMA20 in 1m-ATRs)
    const distance_from_ema_atr =
        Number.isFinite(atr_micro as number) && (atr_micro as number) > 0 && Number.isFinite(ema20_micro as number)
            ? (last - (ema20_micro as number)) / (atr_micro as number)
            : 0;
    const distance_from_ema20_primary_atr =
        Number.isFinite(atr_primary as number) &&
        (atr_primary as number) > 0 &&
        Number.isFinite(ema20_primary as number)
            ? (last - (ema20_primary as number)) / (atr_primary as number)
            : 0;

    const metricsByTf = indicators.metrics || {};
    const microMetrics = metricsByTf[microTimeframe] || {};
    const primaryMetrics = metricsByTf[primaryTimeframe] || {};
    const macroMetrics = metricsByTf[macroTimeframe] || {};

    const microStructureState = microMetrics.structure ?? 'range';
    const microBos = Boolean(microMetrics.bos);
    const microBosDir = microBos ? (microMetrics.bosDir ?? null) : null;
    const microChoch = Boolean(microMetrics.choch);
    const microBreakoutRetestOk = Boolean(microMetrics.breakoutRetestOk);
    const microBreakoutRetestDir = microMetrics.breakoutRetestDir ?? null;
    const microStructureBreakState = microMetrics.structureBreakState ?? 'inside';

    const atrPctile4h = typeof primaryMetrics.atrPctile === 'number' ? primaryMetrics.atrPctile : null;
    const atrPctile1d = typeof macroMetrics.atrPctile === 'number' ? macroMetrics.atrPctile : null;
    const rvol4h = typeof primaryMetrics.rvol === 'number' ? primaryMetrics.rvol : null;
    const rvol1d = typeof macroMetrics.rvol === 'number' ? macroMetrics.rvol : null;
    const structure4hState = primaryMetrics.structure ?? 'range';
    const bos4h = Boolean(primaryMetrics.bos);
    const bosDir4h = bos4h ? (primaryMetrics.bosDir ?? null) : null;
    const choch4h = Boolean(primaryMetrics.choch);
    const breakoutRetestOk4h = Boolean(primaryMetrics.breakoutRetestOk);
    const breakoutRetestDir4h = primaryMetrics.breakoutRetestDir ?? null;
    const structureBreakState4h = primaryMetrics.structureBreakState ?? 'inside';
    const valueState1d = macroMetrics.valueState ?? 'n/a';
    const primaryBias =
        Number.isFinite(slope21_primary as number) &&
        Number.isFinite(rsi_primary as number) &&
        Number.isFinite(ema20_primary as number)
            ? (slope21_primary as number) > 0 && (rsi_primary as number) >= 50 && price >= (ema20_primary as number)
                ? 'up'
                : (slope21_primary as number) < 0 && (rsi_primary as number) <= 50 && price <= (ema20_primary as number)
                  ? 'down'
                  : 'neutral'
            : 'neutral';
    let microBiasCalc: 'up' | 'down' | 'neutral' = 'neutral';
    let microBiasSource:
        | 'structure_breakout_retest'
        | 'structure_break_state'
        | 'structure_bos'
        | 'structure_state'
        | 'ema_slope_rsi'
        | 'neutral' = 'neutral';
    // Precedence for micro bias:
    // 1) structure (breakout/retest, break-state, BOS, structure state), 2) EMA+slope+RSI, 3) neutral.
    if (microBreakoutRetestOk && microBreakoutRetestDir === 'up') {
        microBiasCalc = 'up';
        microBiasSource = 'structure_breakout_retest';
    } else if (microBreakoutRetestOk && microBreakoutRetestDir === 'down') {
        microBiasCalc = 'down';
        microBiasSource = 'structure_breakout_retest';
    } else if (microStructureBreakState === 'above') {
        microBiasCalc = 'up';
        microBiasSource = 'structure_break_state';
    } else if (microStructureBreakState === 'below') {
        microBiasCalc = 'down';
        microBiasSource = 'structure_break_state';
    } else if (microBos && microBosDir === 'up') {
        microBiasCalc = 'up';
        microBiasSource = 'structure_bos';
    } else if (microBos && microBosDir === 'down') {
        microBiasCalc = 'down';
        microBiasSource = 'structure_bos';
    } else if (microStructureState === 'bull') {
        microBiasCalc = 'up';
        microBiasSource = 'structure_state';
    } else if (microStructureState === 'bear') {
        microBiasCalc = 'down';
        microBiasSource = 'structure_state';
    } else if (
        Number.isFinite(slope21_micro as number) &&
        Number.isFinite(rsi_micro as number) &&
        Number.isFinite(ema20_micro as number)
    ) {
        microBiasCalc =
            (slope21_micro as number) > 0 && (rsi_micro as number) >= 50 && price >= (ema20_micro as number)
                ? 'up'
                : (slope21_micro as number) < 0 && (rsi_micro as number) <= 50 && price <= (ema20_micro as number)
                  ? 'down'
                  : 'neutral';
        microBiasSource = microBiasCalc === 'neutral' ? 'neutral' : 'ema_slope_rsi';
    }
    const microBiasLabel = microBiasCalc.toUpperCase();
    const primaryTrendUp = structure4hState === 'bull' || (bos4h && bosDir4h === 'up') || primaryBias === 'up';
    const primaryTrendDown = structure4hState === 'bear' || (bos4h && bosDir4h === 'down') || primaryBias === 'down';
    const primaryBreakdownConfirmed =
        structureBreakState4h === 'below' || (breakoutRetestOk4h && breakoutRetestDir4h === 'down');
    const primaryBreakoutConfirmed =
        structureBreakState4h === 'above' || (breakoutRetestOk4h && breakoutRetestDir4h === 'up');


    // --- SIGNAL STRENGTH DRIVERS & CLOSING GUIDANCE ---
    const clampNumber = (value: number | null | undefined, digits = 3) =>
        Number.isFinite(value as number) ? Number((value as number).toFixed(digits)) : null;
    const trendBias = gates.regime_trend_up ? 1 : gates.regime_trend_down ? -1 : 0;

    const supportProximity = typeof htfSupportDist === 'number' ? Math.max(0, 1 - Math.min(htfSupportDist, 2) / 2) : 0;
    const resistanceProximity =
        typeof htfResistanceDist === 'number' ? Math.max(0, 1 - Math.min(htfResistanceDist, 2) / 2) : 0;
    const locationScoreLong = Math.min(1, supportProximity + (htfBreakoutConfirmed ? 0.3 : 0));
    const locationScoreShort = Math.min(1, resistanceProximity + (htfBreakdownConfirmed ? 0.3 : 0));
    const locationConfluenceScore = Math.max(locationScoreLong, locationScoreShort);

    const nearPrimarySupport =
        typeof primarySR?.support?.dist_in_atr === 'number' ? primarySR.support.dist_in_atr <= 0.6 : false;
    const nearPrimaryResistance =
        typeof primarySR?.resistance?.dist_in_atr === 'number' ? primarySR.resistance.dist_in_atr <= 0.6 : false;

    const macroBias = momentumSignals.macroTrendUp ? 'UP' : momentumSignals.macroTrendDown ? 'DOWN' : 'NEUTRAL';
    const regimeAlignmentRaw =
        primaryBias === 'up'
            ? (macroBias === 'UP' ? 1 : macroBias === 'DOWN' ? -1 : 0) +
              (contextBias === 'UP' ? 1 : contextBias === 'DOWN' ? -1 : 0)
            : primaryBias === 'down'
              ? (macroBias === 'DOWN' ? 1 : macroBias === 'UP' ? -1 : 0) +
                (contextBias === 'DOWN' ? 1 : contextBias === 'UP' ? -1 : 0)
              : 0;
    const regimeAlignment = Math.max(-1, Math.min(1, regimeAlignmentRaw / 2));

    const valueOkLong = valueState1d === 'n/a' ? false : valueState1d !== 'below_val';
    const valueOkShort = valueState1d === 'n/a' ? false : valueState1d !== 'above_vah';

    // Directional drivers (macro, context) require ACTUAL alignment with the side,
    // not mere non-opposition. The old `!== opposite` form was near-tautological for
    // the favored side (a NEUTRAL bias counted for both sides), inflating
    // aligned_driver_count so MEDIUM became the floor. `=== direction` makes the
    // count reflect genuine multi-timeframe confluence. The at-level exception
    // (intoContextSupport/Resistance) on the context driver is retained.
    const longDrivers = [
        structure4hState === 'bull' || (bos4h && bosDir4h === 'up'),
        (breakoutRetestOk4h && breakoutRetestDir4h === 'up') || nearPrimarySupport,
        macroBias === 'UP',
        contextBias === 'UP' || intoContextSupport,
        valueOkLong,
    ];

    const shortDrivers = [
        structure4hState === 'bear' || (bos4h && bosDir4h === 'down'),
        (breakoutRetestOk4h && breakoutRetestDir4h === 'down') || nearPrimaryResistance,
        macroBias === 'DOWN',
        contextBias === 'DOWN' || intoContextResistance,
        valueOkShort,
    ];

    const countTrue = (items: boolean[]) => items.reduce((acc, v) => acc + (v ? 1 : 0), 0);
    const longAlignedDriverCount = countTrue(longDrivers);
    const shortAlignedDriverCount = countTrue(shortDrivers);
    const alignedDriverCount = Math.max(longAlignedDriverCount, shortAlignedDriverCount);

    const positionSide = position_context?.side;
    const priceVsBreakevenPctRaw =
        position_context?.breakeven_price && Number.isFinite(position_context.breakeven_price) && price > 0
            ? ((price - position_context.breakeven_price) / position_context.breakeven_price) * 100
            : null;
    const priceVsBreakevenPct =
        positionSide === 'short'
            ? clampNumber(-(priceVsBreakevenPctRaw ?? 0), 3)
            : clampNumber(priceVsBreakevenPctRaw ?? null, 3);
    const macroSupportsPosition =
        positionSide === 'long' ? gates.regime_trend_up : positionSide === 'short' ? gates.regime_trend_down : null;
    const macroOpposesPosition =
        positionSide === 'long' ? gates.regime_trend_down : positionSide === 'short' ? gates.regime_trend_up : null;
    const closingGuidance = {
        macro_bias: trendBias,
        price_vs_breakeven_pct: priceVsBreakevenPct,
        hold_minutes: clampNumber(position_context?.hold_minutes ?? null, 1),
        macro_supports_position: macroSupportsPosition,
        macro_opposes_position: macroOpposesPosition,
    };
    // Costs (educate the model). Two venue models:
    // - Bitget perps: taker fee per side (+ slippage); spread is negligible.
    // - Capital CFDs: NO commission — the real round-trip cost is crossing the
    //   spread once (entry at ask, exit at bid) plus slippage, and holding cost
    //   is the per-night funding adjustment (overnight_fee_pct_per_day below).
    const isCapital = String(platform || '').toLowerCase() === 'capital';
    const taker_fee_rate_side = isCapital
        ? 0
        : Number.isFinite(position_context?.taker_fee_rate as number)
          ? Math.max(0, Number(position_context?.taker_fee_rate))
          : DEFAULT_TAKER_FEE_RATE; // default set via env (e.g., 0.0006 = 6 bps per side)
    const taker_round_trip_bps = Number((taker_fee_rate_side * 2 * 10000).toFixed(2));
    const slippage_bps = 2;
    // Capital: spread cost, only when a real quote was measured this tick — the
    // 999 canonical fallback means "unknown", never a cost.
    const spread_round_trip_bps =
        isCapital && Number.isFinite(spreadBpsCanonical) && spreadBpsCanonical < 999
            ? Number(spreadBpsCanonical.toFixed(1))
            : null;
    const total_cost_bps =
        isCapital && spread_round_trip_bps === null
            ? null
            : Number((taker_round_trip_bps + slippage_bps + (spread_round_trip_bps ?? 0)).toFixed(1));
    const overnight_fee_pct_per_day = isCapital
        ? capitalMarketContext?.overnight_fee_pct_per_day ?? null
        : null;
    // Perp funding (Bitget only — Capital bundles carry none of these fields).
    // bundle.funding = current-fund-rate data ({ fundingRate }, decimal per
    // interval: 0.0001 = 0.01%); bundle.fundingTime = funding-time data
    // ({ nextFundingTime ms, ratePeriod hours }). Positive rate = longs pay
    // shorts. Fails to null field-by-field — all prompt prose referencing
    // funding stays conditional on what was actually measured.
    const fundingRow = Array.isArray(bundle?.funding) ? bundle.funding[0] : bundle?.funding;
    const fundingRateDecimal = Number(fundingRow?.fundingRate);
    const fundingTimeRow = Array.isArray(bundle?.fundingTime) ? bundle.fundingTime[0] : bundle?.fundingTime;
    const nextFundingAtMsRaw = Number(fundingTimeRow?.nextFundingTime);
    const fundingIntervalHoursRaw = Number(fundingTimeRow?.ratePeriod);
    const perpFunding =
        !isCapital && Number.isFinite(fundingRateDecimal)
            ? {
                  rate_pct_per_interval: Number((fundingRateDecimal * 100).toFixed(4)),
                  interval_hours:
                      Number.isFinite(fundingIntervalHoursRaw) && fundingIntervalHoursRaw > 0
                          ? fundingIntervalHoursRaw
                          : null,
                  next_funding_at_ms:
                      Number.isFinite(nextFundingAtMsRaw) && nextFundingAtMsRaw > 0 ? nextFundingAtMsRaw : null,
              }
            : null;

    const resolvedDecisionPolicy = resolveDecisionPolicy(decisionPolicy);
    const strictPolicy = resolvedDecisionPolicy === 'strict';
    const decisionPolicyLabel = strictPolicy ? 'strict_guardrails' : 'balanced_guardrails';

    // Anti-churn: active only when flat (in-position ticks don't re-enter).
    const cooldownNow = position_context ? null : resolveReentryCooldown(lastClosedPosition);
    const reentryCooldown = cooldownNow
        ? { blocked_side: cooldownNow.blockedSide, minutes_left: cooldownNow.minutesLeft }
        : null;

    // Extension thresholds: single source of truth (shared with the pre-AI
    // extension hard gate in /api/analyze). Referenced in the soft-judgment
    // guidance below so the prose can never drift from the numbers we actually use.
    const {
        microAvoid: extensionMicroAvoid,
        microNoEntry: extensionMicroNoEntry,
        primaryAvoid: extensionPrimaryAvoid,
    } = resolveExtensionThresholds(resolvedDecisionPolicy);

    const modeLabel = dryRun ? 'simulation' : 'live';
    const baseSymbol = symbol.replace(/USDT$/i, '');
    const assetClass = String(category || '').toLowerCase() || 'unknown';
    // On Capital, leverage is fixed by the broker per asset class — the model does
    // not pick it. Only crypto (Bitget) takes a model-chosen 5–10 leverage.
    const leverageGuidance = isCapital
        ? 'Leverage: do NOT set it — on this venue leverage is broker-defined per asset class, not chosen here. Always output leverage=null.'
        : `Leverage 5–10 (crypto): position size is computed in code from fixed dollar risk ÷ your stop distance, so leverage does NOT change what a stop-out costs — it only sets how much margin the position locks (higher = less margin tied up, liquidation nearer). Prefer 5–6 when volatility is elevated or near major ${contextTimeframe} levels; null on HOLD/CLOSE.`;
    const leverageTask = isCapital
        ? 'do NOT output a leverage field — leverage is broker-defined per asset class on this venue.'
        : 'leverage 5–10 for BUY/SELL/REVERSE, else null.';
    // Capital: omit the leverage key entirely (no comma). Bitget: include it.
    const leverageJsonField = isCapital ? '' : ',"leverage":null|5|6|7|8|9|10';

    // Profit-lock margin-recycle maneuver (crypto only). The crypto schema always
    // carries these keys (nullable); the maneuver is only *explained* to the model
    // when ENABLE_CRYPTO_MARGIN_RECYCLE is set — otherwise it's told to null them.
    const marginRecycleEnabled = !isCapital && process.env.ENABLE_CRYPTO_MARGIN_RECYCLE === 'true';
    const manageJsonField = isCapital ? '' : ',"raise_leverage_to":null|int,"move_stop_to_be":true|false|null';
    const manageGuidance = isCapital
        ? ''
        : marginRecycleEnabled
          ? 'Margin recycle (only with a real profit cushion): on HOLD or a partial CLOSE you MAY move the stop to breakeven (move_stop_to_be=true) and raise leverage (raise_leverage_to, up to the symbol exchange max — the system clamps your value to [current, max]). On isolated margin this frees margin for future trades WITHOUT cutting size, and the breakeven stop caps the remainder’s risk. The natural pairing on a winner reaching a major opposite level is ONE decision that combines all three: raise_leverage_to + exit_size_pct 30–70 + a tightened stop_loss_price — the system executes breakeven-stop → leverage raise → trim → your stop, and applies your stop only when it is TIGHTER than the breakeven trigger (a looser one is dropped, the breakeven floor stands). A leverage raise always forces a breakeven stop first; if that stop cannot rest, the raise is aborted. Not in profit → null both.'
          : 'Always output raise_leverage_to=null and move_stop_to_be=null (feature disabled).';

    // signal_strength is OWNED BY CODE (computeSignalStrength). It is NOT shown to the
    // model (we don't want it anchoring the model's analysis) — it drives only the
    // pre-prompt budget gate and postprocessDecision's exception thresholds.
    const signalStrength = computeSignalStrength({
        micro_bias_calc: microBiasLabel,
        primary_bias: primaryBias,
        macro_bias: macroBias,
        context_bias: contextBias,
        primary_trend_up: primaryTrendUp,
        primary_trend_down: primaryTrendDown,
        primary_breakdown_confirmed: primaryBreakdownConfirmed,
        primary_breakout_confirmed: primaryBreakoutConfirmed,
        micro_entry_ok: Boolean(momentumSignals.info?.microEntryOk),
        aligned_driver_count: alignedDriverCount,
        regime_alignment: regimeAlignment,
        location_confluence_score: locationConfluenceScore,
        micro_extension_atr: momentumSignals.microExtensionInAtr ?? null,
        primary_extension_atr: distance_from_ema20_primary_atr,
        breakout_retest_ok_primary: breakoutRetestOk4h,
        breakout_retest_dir_primary: breakoutRetestDir4h ?? null,
    });

    // Assembly half: builds the STATE/MARKET JSON + system/user strings. This is the
    // expensive part (two JSON.stringify + a large template), so it's deferred behind
    // this closure and only run once we know the AI will be called. Captures the
    // derivation scope above, so no state needs threading through. News enters here.
    const assemble = (
        news_sentiment: string | null = null,
        news_headlines: string[] = [],
        nano_context: NanoContext | null = null,
        // The pullback limit from the PREVIOUS evaluation that rested without
        // filling and was just cancelled for this re-evaluation (flat only).
        cancelled_pending_entry: { side: 'BUY' | 'SELL' | null; price: number | null; age_min: number | null } | null = null,
        // Quantified price reaction to just-released high-impact events
        // (market.forex_events.recentEvents). Computed by the caller from the
        // nano 15m candles, so it enters here like nano_context does.
        event_reaction: EventReactionMeasurement[] | null = null,
        // BTC regime coupling for non-BTC crypto ticks (measured correlation/
        // beta + BTC recent state). Fetched by the caller, enters like nano.
        btc_context: BtcContext | null = null,
        // Curated lessons distilled from post-mortems of past LOSING trades on
        // this symbol / its asset class / globally (max 5, confidence-sorted —
        // see lib/swing/lessons.ts). Rendered in the USER turn so the cached
        // system prefix stays byte-stable; [] / null omits the block.
        lessons: Array<{ scope: string; lesson: string }> | null = null,
    ) => {
    const normalizedNewsSentiment =
        typeof news_sentiment === 'string' && news_sentiment.length > 0 ? news_sentiment : null;
    const normalizedHeadlines = Array.isArray(news_headlines) ? news_headlines.filter((h) => !!h).slice(0, 5) : [];

    const srLevel = (lvl: any) =>
        lvl
            ? {
                  price: lvl.price,
                  dist_atr: lvl.dist_in_atr,
                  strength: lvl.level_strength,
                  type: lvl.level_type,
                  state: lvl.level_state,
              }
            : null;

    // ---- Single structured payload: one encoding (JSON), no duplicated keys ----
    // STATE = derived signals (what to reason over). MARKET = raw inputs (price/tape/news).
    // Explicit UTC "now" so the model reasons over an unambiguous anchor instead of
    // reconstructing it from scattered ISO timestamps. All fields are UTC; awareness
    // only — no time-based rule is enforced here (those stay in code gates if/when added).
    const nowDate = new Date(Number.isFinite(nowMs) ? (nowMs as number) : Date.now());
    const DOW = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
    const state = {
        time: {
            iso_utc: nowDate.toISOString(),
            date_utc: nowDate.toISOString().slice(0, 10),
            day_of_week_utc: DOW[nowDate.getUTCDay()],
            hour_utc: nowDate.getUTCHours(),
        },
        biases: {
            micro: microBiasLabel,
            micro_source: microBiasSource,
            primary: primaryBias,
            macro: macroBias,
            context: contextBias,
        },
        trend: {
            primary_up: primaryTrendUp,
            primary_down: primaryTrendDown,
            primary_breakout_confirmed: primaryBreakoutConfirmed,
            primary_breakdown_confirmed: primaryBreakdownConfirmed,
            macro_up: momentumSignals.macroTrendUp,
            macro_down: momentumSignals.macroTrendDown,
        },
        structure: {
            micro: {
                state: microStructureState,
                break_state: microStructureBreakState,
                bos: microBos,
                bos_dir: microBosDir,
                choch: microChoch,
                breakout_retest_ok: microBreakoutRetestOk,
                breakout_retest_dir: microBreakoutRetestDir,
            },
            primary: {
                state: structure4hState,
                break_state: structureBreakState4h,
                bos: bos4h,
                bos_dir: bosDir4h,
                choch: choch4h,
                breakout_retest_ok: breakoutRetestOk4h,
                breakout_retest_dir: breakoutRetestDir4h,
            },
        },
        momentum: {
            rsi: {
                micro: clampNumber(rsi_micro, 1),
                primary: clampNumber(rsi_primary, 1),
                macro: clampNumber(rsi_macro, 1),
            },
            slope_micro_pct_per_bar: clampNumber(slope21_micro, 4),
            slope_primary_pct_per_bar: clampNumber(slope21_primary, 4),
            micro_entry_ok: Boolean(momentumSignals.info?.microEntryOk),
        },
        extension_atr: {
            micro: clampNumber(distance_from_ema_atr, 2),
            primary: clampNumber(distance_from_ema20_primary_atr, 2),
        },
        volatility: {
            atr_pct: { primary: clampNumber(atr_pct_primary, 3), macro: clampNumber(atr_pct_macro, 3) },
            atr_pctile: { primary: clampNumber(atrPctile4h, 0), macro: clampNumber(atrPctile1d, 0) },
            rvol: { primary: clampNumber(rvol4h, 2), macro: clampNumber(rvol1d, 2) },
            value_state_macro: valueState1d,
        },
        location: {
            context_support_dist_atr: clampNumber(htfSupportDist ?? null, 3),
            context_resistance_dist_atr: clampNumber(htfResistanceDist ?? null, 3),
            context_breakout_confirmed: htfBreakoutConfirmed,
            context_breakdown_confirmed: htfBreakdownConfirmed,
            chop_risk: chopRisk,
        },
        levels: {
            primary: { support: srLevel(primarySR?.support), resistance: srLevel(primarySR?.resistance) },
            context: { support: srLevel(contextSR?.support), resistance: srLevel(contextSR?.resistance) },
        },
        // Wave geometry per timeframe: regression channel (slope_atr, channel_pos
        // 0=low..1=high, width), pivot trendlines (live price + slope + touches)
        // and last swing high/low (price, signed ATR distance, bars ago). nano =
        // 15m entry timing, present only when the AI is actually being called.
        geometry: {
            ...(nano_context ? { nano: nano_context } : {}),
            ...(microGeometry ? { micro: microGeometry } : {}),
            ...(primaryGeometry ? { primary: primaryGeometry } : {}),
        },
        gates: {
            spread_ok: gates.spread_ok,
            liquidity_ok: gates.liquidity_ok,
            atr_ok: gates.atr_ok,
            slippage_ok: gates.slippage_ok,
        },
        costs: isCapital
            ? {
                  // Capital CFDs: commission-free — cost = spread + slippage, plus a
                  // per-night funding adjustment while held. total_cost_bps is null
                  // when no live quote was measured this tick (spread unknown).
                  commission_bps: 0,
                  spread_round_trip_bps,
                  slippage_bps,
                  total_cost_bps,
                  overnight_fee_pct_per_day,
                  recent_realized_pnl_pct: clampNumber(realizedRoiPct ?? null, 2),
              }
            : {
                  round_trip_fee_bps: taker_round_trip_bps,
                  slippage_bps,
                  total_cost_bps,
                  // Perp funding while held (positive = longs pay shorts). Key
                  // omitted entirely when the rate wasn't measured this tick.
                  ...(perpFunding
                      ? {
                            funding: {
                                rate_pct_per_interval: perpFunding.rate_pct_per_interval,
                                interval_hours: perpFunding.interval_hours,
                                next_funding_at_utc: perpFunding.next_funding_at_ms
                                    ? new Date(perpFunding.next_funding_at_ms).toISOString()
                                    : null,
                                minutes_to_next_funding: perpFunding.next_funding_at_ms
                                    ? Math.max(
                                          0,
                                          Math.round(
                                              (perpFunding.next_funding_at_ms - nowDate.getTime()) / 60_000,
                                          ),
                                      )
                                    : null,
                            },
                        }
                      : {}),
                  recent_realized_pnl_pct: clampNumber(realizedRoiPct ?? null, 2),
              },
        position: position_context
            ? { open: true, ...position_context }
            : {
                  open: false,
                  status: position_status,
                  reentry_cooldown: reentryCooldown,
                  ...(cancelled_pending_entry && PULLBACK_LIMIT_ENABLED ? { cancelled_pending_entry } : {}),
              },
        closing_guardrails: position_context ? closingGuidance : null,
    };

    const market: Record<string, any> = {
        price: {
            last: Number.isFinite(price) ? price : null,
            change_24h_pct: Number.isFinite(change) ? change : null,
        },
        recent_candles: priceTrendPoints,
        liquidity: {
            spread_bps: clampNumber(spreadBpsCanonical, 4),
            best_bid: Number.isFinite(bestBidRaw) ? bestBidRaw : null,
            best_ask: Number.isFinite(bestAskRaw) ? bestAskRaw : null,
            bid_walls: analytics.topWalls?.bid ?? [],
            ask_walls: analytics.topWalls?.ask ?? [],
        },
        volume_profile: (analytics.volume_profile || [])
            .slice(0, 10)
            .map((v: any) => ({ price: clampNumber(v.price, 2), volume: v.volume })),
        news: { sentiment: normalizedNewsSentiment, headlines: normalizedHeadlines },
        recent_actions: recentActionsExists
            ? recentActions.slice(-1 * actionsToShow).map((a) => {
                  // Annotate partial closes (e.g. "CLOSE 30%") so the model can tell a
                  // trim from a full exit; a 100%/absent pct stays a bare "CLOSE".
                  const partial =
                      a.action === 'CLOSE' && a.closePct != null && a.closePct > 0 && a.closePct < 100;
                  const row: any = {
                      action: partial ? `CLOSE ${Math.round(a.closePct as number)}%` : a.action,
                      ts: new Date(a.timestamp).toISOString(),
                  };
                  // Measured follow-through (see the recent_actions prose in the
                  // system prompt): what the model asked for vs what happened.
                  if (a.entryLimitPrice != null) row.entry_limit = a.entryLimitPrice;
                  if ((a.reissueCount ?? 1) > 1) row.reissued_count = a.reissueCount;
                  if (a.outcome === 'never_filled' || a.outcome === 'still_open') {
                      row.outcome = a.outcome;
                  } else if (a.outcome && typeof a.outcome === 'object') {
                      row.outcome = {
                          closed_pnl_pct_on_margin: a.outcome.closedPnlPctOnMargin,
                          held_min: a.outcome.heldMin,
                      };
                  }
                  return row;
              })
            : [],
    };
    if (forex_event_context && typeof forex_event_context === 'object') {
        market.forex_events = forex_event_context;
    }
    if (forex_session_context && typeof forex_session_context === 'object') {
        market.forex_session = forex_session_context;
    }
    if (Array.isArray(event_reaction) && event_reaction.length > 0) {
        market.event_reaction = event_reaction;
    }
    if (btc_context && typeof btc_context === 'object') {
        market.btc_context = btc_context;
    }
    if (cooldownWake && Number.isFinite(cooldownWake.level)) {
        const wakeNowMs = Number.isFinite(nowMs as number) ? (nowMs as number) : Date.now();
        market.cooldown_wake = {
            crossed: cooldownWake.crossed,
            level: cooldownWake.level,
            set_minutes_ago:
                cooldownWake.setAtMs && cooldownWake.setAtMs > 0
                    ? Math.max(0, Math.round((wakeNowMs - cooldownWake.setAtMs) / 60_000))
                    : null,
        };
    }
    if (capitalMarketContext?.venue_session) {
        market.venue_session = capitalMarketContext.venue_session;
    }
    if (capitalMarketContext?.venue_events) {
        market.venue_events = capitalMarketContext.venue_events;
    }

    // Each note describes ONLY the context this category actually receives (see
    // /api/analyze: forex_session AND forex_events are both built for
    // forex/commodity/index — events resolve to the instrument's macro currency;
    // crypto gets forex_events on the USD calendar but no session levels).
    // Keep prose aligned with the data or it misleads.
    const assetNote =
        assetClass === 'crypto'
            ? `Asset class: crypto. Trades 24/7 — no session boundaries or weekend gaps, and no session-levels block is provided. market.forex_events carries the USD macro calendar (CPI/NFP/FOMC) — crypto reacts to these like a USD risk asset; treat it as event-risk context, never a standalone trigger, and avoid initiating new risk into an imminent high-impact event. News/sentiment can move price fast. Perp funding, when measured, is in state.costs.funding (borrow is not modeled); judge on structure, regime and location.`
            : assetClass === 'forex'
              ? 'Asset class: forex. Liquidity and volatility are session-dependent and weekend gaps exist. Treat market.forex_session levels and market.forex_events as first-class swing context (location + event risk), never as a standalone entry trigger. Avoid initiating new risk into an imminent high-impact event.'
              : assetClass === 'commodity'
                ? 'Asset class: commodity (e.g. metals). Sensitive to USD, real yields and risk-on/off flows; strongly session-driven (London/NY). market.forex_events carries the relevant macro calendar (USD for metals — CPI/NFP/FOMC). Use market.forex_session levels + events as location and risk context, not standalone triggers; avoid initiating new risk into an imminent high-impact event.'
                : assetClass === 'index'
                  ? "Asset class: index. Session-driven and gap-prone around the cash open/close. market.forex_events carries the index's home-economy macro calendar. Use market.forex_session levels + events as location and risk context, not standalone triggers; avoid initiating new risk into an imminent high-impact event."
                  : `Asset class: ${assetClass}. No session or event context is provided; judge on structure, regime, location and cost.`;

    const trendGuardException = strictPolicy
        ? 'no exceptions'
        : 'rare exception: a confirmed primary breakout/breakdown in the new direction';
    const microEntryException = strictPolicy
        ? 'unless a confirmed primary breakout-retest'
        : 'unless a confirmed primary breakout-retest, or strong multi-factor structure+location alignment';
    const antiFlipWindow = strictPolicy ? 'the last 2 calls' : 'the previous call';
    const antiFlipStrength = strictPolicy ? 'strong conviction' : 'at least moderate conviction';

    // Venue-session note. Keyed on the VENUE (byte-stable across ticks for cache
    // prefix stability), phrased "when present" so the prose stays honest when a
    // given tick's payload doesn't carry the block (venue_session is only built
    // while the venue is inside a session).
    const venueSessionNote = isCapital
        ? `\nVenue session (market.venue_session, ISO UTC; present only while the venue is open): when present, the current session ends at closes_at_utc (minutes_to_close min from now) and trading resumes at reopens_at_utc. While the venue is closed your exchange-side TP/SL bracket CANNOT fill, and the reopen can gap past your stop for a worse fill. Near the session end, avoid opening fresh risk unless the setup explicitly justifies holding through the closed-venue gap.`
        : '';

    // Venue liquidity clock: schedule facts (when the tape's character changes),
    // paired with the session-offense guidance bullet below.
    const venueEventsNote = isCapital
        ? `\nVenue liquidity clock (market.venue_events, ISO UTC; when present): recent/upcoming venue events (cash open/close, lunch break, exchange maintenance halt, weekly reopen) with minutes_ago/minutes_to, plus liquidity_phase ∈ {pre_open, opening_drive, into_close, venue_break, off_hours, thin_reopen, normal}. These are schedule facts, not signals — the session-offense guidance says how to trade around them.`
        : '';

    // Session doctrine, two modes (SESSION_OFFENSE_ENABLED, default OFF):
    // OFF = swing-defensive — session sweeps and venue phases are HAZARD
    // context (don't chase a sweep, don't add risk into thin tape), never an
    // entry playbook. ON = the intraday offense doctrine (sweep-capture resting
    // entries, opening-drive tactics), preserved for a future day-trade model.
    // Keyed on asset class (forex_session is built for forex/commodity/index;
    // crypto never gets it) with "when present" phrasing for tick-level blocks.
    const sessionOffenseGuidance =
        assetClass === 'forex' || assetClass === 'commodity' || assetClass === 'index'
            ? SESSION_OFFENSE_ENABLED
                ? `\n- Session liquidity offense (market.forex_session.signals, when present; phase tactics need market.venue_events): a sweep of a prior-day/session extreme is REVERSAL fuel, not continuation proof. When swept*Low=true, do NOT open or rest fresh shorts below that low unless price has ACCEPTED below it (primary close under the level); bullishLiquidityReclaim=true (a swept low reclaimed) is a long trigger AT the extreme — mirror exactly for swept highs (bearishLiquidityRejection). The offensive resting entry around a liquidity event sits BEYOND the level likely to be swept — BUY below the prior-day/session low when primary drift is up, SELL above the swept high when drift is down — so the stop-run itself fills you at the extreme and the snap-back is the trade; stop past the sweep extension, target back inside the range. Never leave a shallow pullback limit resting IN THE PATH of an imminent venue event: it fills exactly when the level breaks against you. Phase tactics (when market.venue_events is present): opening_drive = displacement window — enter WITH the confirmed drive at market, or fade a COMPLETED sweep at an extreme; pre_open / into_close / venue_break / off_hours = thin, gap-prone tape — no fresh momentum entries, sweep-fade at clear levels only, reduced conviction; thin_reopen = the worst spreads of the week, treat fills as suspect and prefer HOLD.`
                : `\n- Session liquidity (market.forex_session.signals + market.venue_events, when present — DEFENSIVE context for a swing book, not an entry playbook): a sweep of a prior-day/session extreme is REVERSAL fuel, not continuation proof — do NOT open fresh risk in the sweep's direction unless price has ACCEPTED beyond the level (primary close through it), and never chase the sweep itself. During thin phases (pre_open, into_close, venue_break, off_hours, thin_reopen) spreads and gap risk are at their worst: no fresh entries there on session-timing grounds alone — a swing entry must be valid on primary structure regardless of the session clock, and if it is, the phase only argues for waiting, not hurrying.`
            : '';

    // Post-event reaction doctrine: how to read market.event_reaction (built only
    // for the calendar-carrying asset classes, and only when a high-impact event
    // released within the recent lookback). Keyed on asset class with "when
    // present" phrasing — same byte-stability rule as the other notes. The base
    // rates cited are measured (1m study, CPI/NFP/FOMC Jun 2024–Jun 2026): pre-
    // release drift direction ~50/50; net 30m move ≈ 1/3 of the 30m range; the
    // ~45min reaction direction persisted over the following ~4h on gold/EUR and
    // was gone by 24h. Crypto gets its own variant with crypto-measured base
    // rates (BTC/ETH/SOL replication, see eventReaction.ts header): persistence
    // confirmed for CPI/FOMC but NOT for NFP, which tends to give back — the
    // separate string keeps the other classes' prompt bytes unchanged.
    const eventReactionGuidance =
        assetClass === 'forex' || assetClass === 'commodity' || assetClass === 'index'
            ? `\n- Post-event reaction (market.event_reaction, when present): a high-impact release just happened (see market.forex_events.recentEvents); each entry quantifies the reaction since the pre-release close — ret_since_release_bp (signed net move), range_since_release_bp (total excursion incl. whipsaw), retrace_pct (0 = price at the reaction extreme, 1 = push fully given back), minutes_since_release. Measured base rates: the PRE-release drift direction carries no information, and the release burst is mostly whipsaw (net move ≈ one-third of range) — but a decisive reaction direction, once established ~45 min after release, has historically persisted over the following ~2–4 h and decays after. Read it accordingly: large |ret| with low retrace_pct = post-event drift context in that direction; large range with |ret| near zero = undecided, treat as chop; retrace_pct ≈ 1 = the event is spent as a directional input. Weigh WITH structure/location as usual — context, never a standalone trigger.`
            : assetClass === 'crypto'
              ? `\n- Post-event reaction (market.event_reaction, when present): a high-impact USD release just happened (see market.forex_events.recentEvents); each entry quantifies the reaction since the pre-release close — ret_since_release_bp (signed net move), range_since_release_bp (total excursion incl. whipsaw), retrace_pct (0 = price at the reaction extreme, 1 = push fully given back), minutes_since_release. Measured base rates on crypto (BTC/ETH/SOL, 2024–2026): the PRE-release drift direction carries no information (pre-FOMC drift, if anything, reversed), and the release burst is mostly whipsaw — but a decisive reaction direction, once established ~45 min after release, held through the following ~2–4 h on CPI and FOMC releases. NFP is the exception: its reaction direction was the least reliable and on average partially gave back, so discount NFP drift. Read it accordingly: large |ret| with low retrace_pct = post-event drift context in that direction (except NFP); large range with |ret| near zero = undecided, treat as chop; retrace_pct ≈ 1 = the event is spent as a directional input. Weigh WITH structure/location as usual — context, never a standalone trigger.`
              : '';

    // BTC regime doctrine: how to read market.btc_context (built only for
    // non-BTC crypto ticks). "When present" phrasing keeps the prose honest on
    // BTC's own ticks, which never carry the block. Coupling numbers are FED,
    // not asserted — correlation is regime-dependent (SOL 30d slipped 0.84→0.77
    // while LINK sat at 0.94, Jul 2026), so the model weighs the measured value
    // instead of a hardcoded "crypto = BTC beta" claim.
    const btcContextGuidance =
        assetClass === 'crypto'
            ? `\n- BTC regime (market.btc_context, when present — non-BTC crypto only): this asset's measured coupling to BTC — corr_30d/corr_90d (daily-return correlation), beta_90d, btc.ret_*_bp (BTC's own recent moves), and alt_vs_btc_residual_7d_bp (this asset's 7d return minus beta x BTC's; positive = idiosyncratic strength). At high correlation (corr ≳ 0.8) alts rarely sustain moves against the BTC regime: a fresh position against BTC's current direction needs idiosyncratic justification (see the residual and news), and a deteriorating BTC weakens an otherwise clean alt setup. At lower correlation weigh BTC context proportionally less. Measurements, not a verdict — combine with structure/location as usual, never a standalone trigger.`
            : '';

    // Cost prose per venue. The live NUMBERS live in state.costs (user turn) —
    // this prose only explains how to read them, so the system prompt stays
    // byte-identical across ticks (prompt-caching prefix stability). Fields that
    // may be absent are described with "when present" so the prose never claims
    // a measurement that wasn't taken this tick.
    const costChurnLine = isCapital
        ? `Cost/churn: no commission on this venue; round-trip cost = state.costs.total_cost_bps (spread crossed once over entry+exit, + slippage; null = no live quote was measured this tick — spread unknown, see market.liquidity.spread_bps). Holding cost: state.costs.overnight_fee_pct_per_day, when present, accrues each night held (per side; negative = you pay) — over a multi-day swing it can rival the round-trip cost, so weigh it on HOLD vs CLOSE and when choosing direction. If the expected swing is not clearly larger than cost, or the setup is unclear/MED-LOW quality, prefer HOLD.`
        : `Cost/churn: round-trip cost = state.costs.total_cost_bps. Perp funding (state.costs.funding, present only when measured this tick): rate_pct_per_interval accrues each funding settlement (every interval_hours hours when given; next charge at next_funding_at_utc when given) while held — positive = longs pay shorts, negative = shorts pay longs. Over a multi-day hold funding can rival the round-trip fee; weigh it on HOLD vs CLOSE and when choosing direction. If the expected swing is not clearly larger than cost, or the setup is unclear/MED-LOW quality, prefer HOLD.`;

    const bracketVenueNote = isCapital
        ? 'rests on the venue between these evaluations, but fills ONLY while the venue is open — a reopening gap can jump the stop and fill worse than the stop level'
        : 'fills 24/7 between these evaluations';

    const sys = `
You are an expert swing-trading market-structure analyst. Decide one action and size it.

${assetNote}${venueSessionNote}${venueEventsNote}

TIMEFRAMES (fixed)
- micro=${microTimeframe} (entry timing/confirmation), primary=${primaryTimeframe} (setup+execution), macro=${macroTimeframe} (regime bias), context=${contextTimeframe} (HTF location + major levels, risk lever), nano=15m (state.geometry.nano, flat entry scans only — fine-timing of an already-valid entry, never a setup by itself and never an exit signal).
Strategy: ${primaryTimeframe} swing setups with ${microTimeframe} confirmation, aligned with (or tactically fading) the ${macroTimeframe} regime while respecting ${contextTimeframe} location. Holding horizon ~1–10 days. Prefer fewer, higher-quality trades; avoid churn.

CADENCE (how often you are actually consulted)
- You are evaluated once per ${primaryTimeframe} bar close — flat scans and in-position management alike. Between looks the exchange-side TP/SL bracket is the ONLY manager, so every bracket you leave behind must stand on its own for at least one full ${primaryTimeframe} bar.
- Earlier looks happen only when: a wake band you set is crossed (flat)${PULLBACK_LIMIT_ENABLED ? ', your resting pullback limit was swept,' : ''} or, in a position, price has moved several primary-ATRs since your last look (emergency check — do not rely on it for routine management). Both conditions are watched roughly once per MINUTE, so a crossed band reaches you almost immediately — place bands exactly at the decision levels, no padding needed, and trust HOLD + a wake band over a marginal entry taken "so you don't miss it". Plan levels; do not plan to watch.

INPUTS
- You receive two JSON objects: STATE (derived signals — your single source of truth) and MARKET (raw price/tape/news). All keys are pre-computed; do not invent fields.
- micro_bias precedence (already applied in state.biases.micro): structure (breakout-retest → break-state → BOS → structure-state) first, momentum (EMA slope+RSI+price vs EMA20) as fallback; structure wins ties.
- market.recent_actions: your last few decisions on this symbol (oldest first) with their MEASURED follow-through where known — entry_limit = the pullback limit that entry rested at; reissued_count = consecutive re-issues of the same limit collapsed into one row (one idea, not repeated trades); outcome ∈ never_filled (the limit was cancelled unfilled — NO position resulted, you did not trade) | still_open | {closed_pnl_pct_on_margin (leverage-multiplied), held_min}. Weigh outcomes as recent evidence about your read of this market — e.g. a just-stopped-out direction needs a materially changed setup, and a never_filled entry means that idea was never tested.
- LESSONS (user turn, when present): 1-2 line lessons distilled from forensic post-mortems of your own past LOSING trades on this symbol, its asset class, or any instrument ([scope] tag). These are failure modes you have actually exhibited, not generic advice — before entering, check the setup against them and note in your reason when one applies. They are cautionary evidence like recent_actions outcomes, never hard rules: current structure and measurements win on conflict.

DECISION OWNERSHIP
- You own the conviction read: judge setup quality and selectivity yourself from the structure, location, regime and momentum measurements in STATE — there is no pre-computed verdict to defer to.${isCapital ? '' : ' Position size is computed in code from a fixed dollar risk and your stop distance — your conviction is expressed through taking or skipping the trade and through stop/target placement, not through size.'}
- The HARD constraints below are enforced in code AFTER you respond. Do not spend reasoning re-deriving them — if you violate one your action is silently coerced (a wasted call). Just stay inside them:
  1. Allowed actions: flat → BUY/SELL/HOLD; in a position → HOLD/CLOSE/REVERSE only.
  2. Trend guard: no counter-trend entry/flip against an aligned primary+micro trend (${trendGuardException}).
  3. Entry timing: when flat and momentum.micro_entry_ok=false, entries are blocked (${microEntryException}).
  4. Anti-flip: a repeated CLOSE/REVERSE within ${antiFlipWindow} is blocked unless ${antiFlipStrength}.
  5. Base gates: if any of state.gates.{spread_ok,liquidity_ok,atr_ok,slippage_ok} is false → entries forced to HOLD and risk-off forced while in a position.${REENTRY_COOLDOWN_MIN > 0 ? `\n  6. Re-entry cooldown: for ${REENTRY_COOLDOWN_MIN} min after a position closes, re-entering the SAME direction is blocked (state.position.reentry_cooldown shows the blocked side when active; the opposite direction stays allowed).${SESSION_OFFENSE_ENABLED ? ' Exception: a sweep-reclaim re-entry passes — when the matching reclaim signal is live (market.forex_session.signals.bullishLiquidityReclaim for a blocked long, bearishLiquidityRejection for a blocked short), the block is lifted, so a stop-out on a swept extreme does NOT forfeit the reclaim trade.' : ''}` : ''}

YOUR JOB (soft judgment — where your reasoning actually matters)
- Pick the highest-quality action consistent with STATE, then size it. Structure (BOS/CHoCH/breakout-retest) outweighs raw momentum.
- Location vs regime: prefer entries aligned with macro+context. Counter-regime only at extreme location with clean invalidation. Do NOT open into a near opposite level (levels.*.dist_atr or location.context_*_dist_atr under ~0.6 ATR) unless the matching breakout/breakdown is confirmed. If both nearest levels are close (location.chop_risk), treat as chop and avoid fresh entries without clean confirmed level logic.
- Level-bounce entries are a first-class setup, NOT a counter-regime fade: at one primary level (dist_atr ≤ ~${ACTIONABILITY_NEAR_ATR}) with the opposite level far (≥ ~${ACTIONABILITY_ROOM_ATR} ATR of room) and micro structure turning that way, an entry toward the room is legitimate even when macro/context lean against it. Judge it on the level's strength/state and the micro turn; invalidation sits just beyond the level, so the risk is defined. Do not reject these solely for regime misalignment.
- Extension (risk control, not a signal): |state.extension_atr.micro| ≥ ${extensionMicroAvoid} or |state.extension_atr.primary| ≥ ${extensionPrimaryAvoid} → avoid fresh entries; micro > ${extensionMicroNoEntry} → strongly prefer none. RSI extremes are NOT a counter-trend trigger by themselves — only "permission" once structure shows damage/flip.
- Wave position (state.geometry — WHERE in the wave to act; structure/levels still decide WHETHER): channel_pos maps price inside the timeframe's regression channel (0=low, 1=high), slope_atr is its drift per bar. Time entries into the wave, not onto its crest: in an up-sloping channel prefer longs near the channel low / last_swing_low (channel_pos ≲ 0.4) and AVOID fresh longs at channel_pos ≳ 0.75 or right at last_swing_high without a confirmed break — mirror for shorts in a down-slope. support_trendline / resistance_trendline give the live trendline price and slope; a close through them plus a structure signal = break, a touch alone = reaction point. When geometry.nano is present, use it to fine-time the trigger (nano wave trough in an up leg beats a nano crest) — never as a standalone reason to trade against micro/primary structure. If a good setup sits at a bad wave position, HOLD and wait for the pullback rather than paying the crest.${sessionOffenseGuidance}${eventReactionGuidance}${btcContextGuidance}
- ${costChurnLine}
- In a position: PnL scales — state.position.unrealized_pnl_pct_on_margin (and max_drawdown_pct/max_profit_pct) are leverage-multiplied return on margin; price_move_pct and closing_guardrails.price_vs_breakeven_pct are on PRICE scale. Judge "how far has this actually moved" on price scale, not margin scale.
- In-position discipline (this is a SWING trade — the resting TP/SL bracket is the exit plan, your job is to protect it, not to re-litigate it every look): the DEFAULT action is HOLD, tightening stop_loss_price behind structure as profit builds (tighten-only, enforced). A full CLOSE is justified ONLY by (a) a CONFIRMED primary-timeframe structure flip against the position (BOS/CHoCH against you, or the primary breakout/breakdown that founded the entry decisively unwound), or (b) the thesis completing at/near the target. Proximity to an opposite level that has NOT rejected, micro-timeframe wiggles, an event on the calendar, or impatience are NOT close reasons — near a level the correct tools are a stop tighten or, after meaningful gains into a MAJOR opposite level, a 30–70% trim (exit_size_pct). Every early full exit forfeits the multi-ATR target the entry's risk was sized against. REVERSE = full close then open opposite (exit_size_pct=100, no partials) and only on a confirmed primary structure flip.${
        position_context
            ? `\n- Entry thesis: earlier turns of this conversation are your own entry decision and management ticks for this position — manage against that thesis: HOLD while it stays intact; trim/CLOSE when it is invalidated or has played out. Weigh it as context, not a command: current structure wins on conflict. If this conversation has no earlier turns (position adopted mid-life), judge purely from current structure.`
            : ''
    }
- Exchange-side TP/SL bracket (${bracketVenueNote}):
  • On BUY/SELL — and on REVERSE, for the NEW opposite-side position — ALWAYS set take_profit_price: a structural price target (next opposing level from state.levels, measured move, or value-area edge), at least ~${ENTRY_TP_MIN_ATR} primary-ATR away. It rests on the exchange until it fills or a later evaluation amends it. If you output null, the system attaches a wide ${EXCHANGE_TP_FALLBACK_ATR_MULT}×ATR default. You SHOULD also set stop_loss_price: the structural invalidation level (just past the swing/level that voids the setup), ${ENTRY_SL_MIN_ATR}–${EXCHANGE_SL_MAX_ATR_MULT} primary-ATR from entry. If you output null (or the level is invalid), a wide ${EXCHANGE_SL_MAX_ATR_MULT}×ATR catastrophe stop is attached instead — a real structural stop is almost always better than that default.
  • In a position (HOLD or partial CLOSE), you MAY amend the standing bracket: output a new take_profit_price and/or stop_loss_price, or null to leave a leg unchanged. state.position.take_profit_price / stop_loss_price show the current resting levels (null = none on that leg). Tighten the stop as profit builds (structure-based, e.g. just past the last defended swing); move the TP only for a structural reason, not to chase price.
  • Both must sit on the correct side of current price; a stop may never sit wider than ${EXCHANGE_SL_MAX_ATR_MULT}×ATR from current price, and a stop AMENDMENT may only TIGHTEN — a level looser than the standing stop is dropped. Invalid values are clamped or dropped in code — don't waste them.
${
        PULLBACK_LIMIT_ENABLED
            ? `- Pullback limit entry (flat BUY/SELL only): when the SETUP is valid but the WAVE POSITION is bad (channel_pos high for a long / low for a short, price at a crest), set entry_limit_price to the pullback level you would rather pay — e.g. the channel low, last_swing_low, or a broken level's retest (BUY below current price, SELL above; usable window ${ENTRY_LIMIT_MIN_ATR}–${ENTRY_LIMIT_MAX_ATR} primary-ATR from price). The order rests on the venue and is CANCELLED at the next evaluation if unfilled — short-lived, not a standing commitment. It is NOT a free option: the market decides your fill, so whoever pushes price through your level is trading against you at that moment. Rest a limit only where being hit by a violent move is what you WANT — deep in structure (a genuine wave trough/crest, a defended swing, a broken level's retest) or beyond a sweepable extreme — never AT a bare trendline price or a shallow retracement, where the only fill available is the break that voids your thesis. Your take_profit_price and stop_loss_price are anchored at the LIMIT price. null = enter at market now. An INVALID limit (wrong side of price, or closer than ${ENTRY_LIMIT_MIN_ATR} ATR) drops the ENTIRE entry for this evaluation — it does NOT fall back to market, so send null when you actually want market. Use market when timing is already good; use the limit instead of HOLDing when only timing is wrong. When state.position.cancelled_pending_entry is present, YOUR previous pullback limit (side/price/age_min) just rested without filling and has been cancelled for this evaluation — decide fresh with that knowledge: re-issue it (same or adjusted level) if the setup still holds, switch to market if the move is confirmed and running without you, or drop the idea if the setup degraded. Do not treat it as a commitment — and do not chase: a third consecutive unfilled re-issue of the same idea while price trends away from the level means the pullback is not coming; commit at market or abandon the idea, don't keep trailing a limit behind the move. When this evaluation continues the conversation in which you placed that limit, your original reasoning is in the turns above — re-validate that thesis against the CURRENT measurements (what changed since you placed it?) instead of re-deriving the setup from scratch.`
            : `- entry_limit_price: ALWAYS null — resting pullback limits are disabled (a resting limit's fill is adversely selected: it fills exactly when the level breaks against the thesis). Entries execute at market, so only enter when the timing is right NOW. If the setup is valid but the wave position is bad, HOLD and set cooldown_wake_above/below at the level you would rather pay — you will be re-evaluated the moment price gets there; entering there at market after a confirmed reaction beats resting blind in the move's path.`
    }
- Flat cooldown (flat HOLD only; ignored on any other action or in a position — enforced in code): when the setup is far from actionable and you expect nothing decision-relevant for a while, set cooldown_minutes (${HOLD_COOLDOWN_MIN_MINUTES}–${HOLD_COOLDOWN_MAX_MINUTES}, code clamps) to suppress flat re-evaluations of this symbol. STRONGLY prefer the conditional form: also set cooldown_wake_above and/or cooldown_wake_below — price levels that END the cooldown the moment price crosses them (the breakout/breakdown levels that would change your mind), so a real move still reaches you immediately while chop does not. wake_above must sit above current price, wake_below below it (a wrong-side band is dropped, the cooldown stays). The cooldown never mutes in-position management${PULLBACK_LIMIT_ENABLED ? ' or resting-limit re-evaluations' : ''} — only fresh flat scans. null = keep the normal cadence; an unconditional cooldown (no bands) is acceptable only when no nearby level would change your read.
- Wake-band trigger (market.cooldown_wake, when present): THIS evaluation exists because price crossed the wake band you set on a previous flat HOLD (crossed = which side, level, set_minutes_ago). Treat it as the breakout/breakdown check you scheduled, not a routine scan: judge whether the move through that level is real (acceptance, structure break) or a sweep/fake-out, and act on that read. ${
        PULLBACK_LIMIT_ENABLED
            ? 'If the move is real but the wave position is already poor, a pullback limit at the broken level’s retest is the natural tool — you asked to be woken precisely so you would not have to chase later.'
            : 'If the move is real but the wave position is already poor, do NOT chase the extension — set a fresh wake band at the broken level so the retest itself wakes you for a market entry on confirmation.'
    } Do not re-set a cooldown with the same band unless you explicitly judge the cross a fake-out.
- ${leverageGuidance}${manageGuidance ? `\n- ${manageGuidance}` : ''}
- Position truthfulness: never describe a position as winning when unrealized_pnl_pct_on_margin < 0 or price_vs_breakeven_pct is on the losing side.

OUTPUT
- Strict JSON only, parseable by JSON.parse — no markdown, comments, trailing commas, or extra keys.
- Decision policy mode: ${decisionPolicyLabel}.
`.trim();

    const user = `
You are analyzing ${baseSymbol} for swing trading (mode=${modeLabel}, asset_class=${assetClass}).
Timeframes: micro=${microTimeframe}, primary=${primaryTimeframe}, macro=${macroTimeframe}, context=${contextTimeframe}${nano_context ? ', nano=15m' : ''}. Evaluated on ${primaryTimeframe} bar closes; a crossed wake band${PULLBACK_LIMIT_ENABLED ? ', a swept resting entry,' : ''} or an outsized move triggers an earlier look — otherwise assume "the next evaluation" is one ${primaryTimeframe} bar away, and let the exchange-side bracket do its job in between. Decision policy: ${decisionPolicyLabel}.
S/R levels are swing-pivot derived per timeframe (~150 bars); distances are in that timeframe's ATR; level state ∈ {at_level, approaching, rejected, broken, retesting}.

STATE (derived signals — single source of truth):
${JSON.stringify(state)}

MARKET (raw inputs):
${JSON.stringify(market)}
${
    Array.isArray(lessons) && lessons.length
        ? `\nLESSONS (from post-mortems of your past losing trades — see INPUTS):\n${lessons
              .map((l) => `- [${l.scope}] ${l.lesson}`)
              .join('\n')}\n`
        : ''
}
TASKS:
1) Output exactly one allowed action (see DECISION OWNERSHIP): flat → BUY/SELL/HOLD; in a position → HOLD/CLOSE/REVERSE.
2) ${leverageTask}
3) exit_size_pct for CLOSE/REVERSE (100 = full close, 30–70 = trim), else null.
4) take_profit_price: REQUIRED price target on BUY/SELL/REVERSE (resting exchange TP; on REVERSE target the NEW opposite-side position); on in-position HOLD/partial CLOSE a new level amends the standing TP (null = unchanged); else null. stop_loss_price: on BUY/SELL/REVERSE the structural invalidation stop (null = wide catastrophe default); on in-position HOLD/partial CLOSE amends the standing stop, tighten-only (null = unchanged); else null.
5) ${
    PULLBACK_LIMIT_ENABLED
        ? 'entry_limit_price: on flat BUY/SELL you MAY rest a pullback limit instead of market (see guidance; cancelled next evaluation if unfilled); else null.'
        : 'entry_limit_price: ALWAYS null (market entries only — see guidance).'
}
6) cooldown_minutes (+ optional cooldown_wake_above/cooldown_wake_below): on a flat HOLD you MAY request a quiet period (see flat-cooldown guidance); else null.
7) summary ≤3 lines; reason = brief rationale.

Respond with strict JSON only:
{"action":"BUY|SELL|HOLD|CLOSE|REVERSE","summary":"≤2 lines","reason":"brief rationale","exit_size_pct":null|0-100,"take_profit_price":null|price,"stop_loss_price":null|price,"entry_limit_price":null|price,"cooldown_minutes":null|minutes,"cooldown_wake_above":null|price,"cooldown_wake_below":null|price${leverageJsonField}${manageJsonField}}
`;

        return { system: sys, user };
    };

    const context = {
        // Code-owned conviction. NOT shown to the model (kept out of the prompt so it
        // doesn't anchor the model's analysis) — used only to gate the AI call before
        // spending it (flat + sub-MEDIUM → no call) and by postprocessDecision.
        signal_strength: signalStrength,
        micro_bias_calc: microBiasLabel,
        primary_bias: primaryBias,
        macro_bias: macroBias,
        context_bias: contextBias,
        primary_trend_up: primaryTrendUp,
        primary_trend_down: primaryTrendDown,
        primary_breakdown_confirmed: primaryBreakdownConfirmed,
        primary_breakout_confirmed: primaryBreakoutConfirmed,
        micro_entry_ok: Boolean(momentumSignals.info?.microEntryOk),
        aligned_driver_count: alignedDriverCount,
        regime_alignment: regimeAlignment,
        location_confluence_score: locationConfluenceScore,
        micro_extension_atr: momentumSignals.microExtensionInAtr ?? null,
        primary_extension_atr: distance_from_ema20_primary_atr,
        breakout_retest_ok_primary: breakoutRetestOk4h,
        breakout_retest_dir_primary: breakoutRetestDir4h ?? null,
        forex_session_context,
    };

    const actionability = evaluateActionability({
        microEntryOk: Boolean(momentumSignals.info?.microEntryOk),
        primaryBreakoutConfirmed,
        primaryBreakdownConfirmed,
        primaryBreakoutRetestOk: breakoutRetestOk4h,
        primaryBreakoutRetestDir: breakoutRetestDir4h,
        primaryBos: bos4h,
        primaryBosDir: bosDir4h,
        primaryBreakState: structureBreakState4h,
        primarySupportDistAtr: primarySR?.support?.dist_in_atr ?? null,
        primaryResistanceDistAtr: primarySR?.resistance?.dist_in_atr ?? null,
        microBreakoutRetestOk,
        microBreakoutRetestDir,
        microBos,
        microBosDir,
        microBreakState: microStructureBreakState,
        contextSupportDistAtr: contextSR?.support?.dist_in_atr ?? null,
        contextSupportState: contextSR?.support?.level_state ?? null,
        contextResistanceDistAtr: contextSR?.resistance?.dist_in_atr ?? null,
        contextResistanceState: contextSR?.resistance?.level_state ?? null,
    });

    return { signalStrength, context, assemble, actionability };
}

// Backward-compatible wrapper: original buildPrompt behavior (derive + assemble in
// one call). The hourly swing path uses computeSwingState directly so it can gate on
// signal_strength before assembling/fetching news.
export async function buildPrompt(
    symbol: string,
    timeframe: string,
    bundle: any,
    analytics: any,
    position_status: string = 'none',
    news_sentiment: string | null = null,
    news_headlines: string[] = [],
    forex_event_context: ForexEventContextForPrompt | null = null,
    forex_session_context: ForexSessionLevelsContext | null = null,
    indicators: MultiTFIndicators,
    gates: any,
    position_context: PositionContext | null = null,
    momentumSignalsOverride?: MomentumSignals,
    recentActions: RecentActionEntry[] = [],
    realizedRoiPct?: number | null,
    dryRun?: boolean,
    spreadBpsOverride?: number,
    decisionPolicy?: DecisionPolicy,
    category?: string | null,
    platform?: string | null,
) {
    const { context, assemble } = computeSwingState(
        symbol,
        timeframe,
        bundle,
        analytics,
        position_status,
        forex_event_context,
        forex_session_context,
        indicators,
        gates,
        position_context,
        momentumSignalsOverride,
        recentActions,
        realizedRoiPct,
        dryRun,
        spreadBpsOverride,
        decisionPolicy,
        category,
        platform,
    );
    const { system, user } = assemble(news_sentiment, news_headlines);
    return { system, user, context };
}

export type PromptDecisionContext = {
    // Populated on the context returned by buildPrompt; absent on the input passed
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

const toBiasLabel = (value: string): 'UP' | 'DOWN' | 'NEUTRAL' => {
    const v = value.toLowerCase();
    if (v === 'up') return 'UP';
    if (v === 'down') return 'DOWN';
    return 'NEUTRAL';
};

export function computeSignalStrength(context: PromptDecisionContext): 'LOW' | 'MEDIUM' | 'HIGH' {
    const aligned = Number.isFinite(context.aligned_driver_count) ? context.aligned_driver_count : 0;
    const regime = Number.isFinite(context.regime_alignment) ? Math.abs(context.regime_alignment) : 0;
    const location = Number.isFinite(context.location_confluence_score) ? context.location_confluence_score : 0;
    const microExt = Number.isFinite(context.micro_extension_atr as number)
        ? Math.abs(context.micro_extension_atr as number)
        : 0;
    const primaryExt = Number.isFinite(context.primary_extension_atr as number)
        ? Math.abs(context.primary_extension_atr as number)
        : 0;

    let score = 0;
    if (aligned >= 5) score += 3;
    else if (aligned >= 4) score += 2;
    else if (aligned >= 3) score += 1;

    if (regime >= 0.5) score += 1;
    if (location >= 0.6) score += 1;

    if (microExt >= 2.5 || primaryExt >= 2.5) score -= 1;
    if (microExt >= 3 || primaryExt >= 3) score -= 1;

    if (score >= 4) return 'HIGH';
    if (score >= 2) return 'MEDIUM';
    return 'LOW';
}

export function postprocessDecision(params: {
    decision: any;
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

    if (!positionOpen && !context.micro_entry_ok && (action === 'BUY' || action === 'SELL')) {
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
                ? Number(decision.leverage)
                : null
            : null;
    const exit_size_pct =
        action === 'CLOSE' || action === 'REVERSE'
            ? Number.isFinite(decision?.exit_size_pct as number)
                ? Number(decision.exit_size_pct)
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
        Number(decision.raise_leverage_to) > 0
            ? Math.round(Number(decision.raise_leverage_to))
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
    // Flat-HOLD cooldown: type/eligibility coercion only — clamping and wake-band
    // side validation happen in sanitizeHoldCooldown in the API route (live price).
    const cooldownEligible = !positionOpen && action === 'HOLD';
    const cooldown_minutes =
        cooldownEligible && Number.isFinite(Number(decision?.cooldown_minutes)) && Number(decision.cooldown_minutes) > 0
            ? Math.round(Number(decision.cooldown_minutes))
            : null;
    const cooldown_wake_above = cooldownEligible ? coercePrice(decision?.cooldown_wake_above) : null;
    const cooldown_wake_below = cooldownEligible ? coercePrice(decision?.cooldown_wake_below) : null;

    return {
        ...decision,
        action,
        leverage,
        exit_size_pct,
        raise_leverage_to,
        move_stop_to_be,
        take_profit_price,
        stop_loss_price,
        entry_limit_price,
        cooldown_minutes,
        cooldown_wake_above,
        cooldown_wake_below,
        signal_strength: signalStrength,
        micro_bias: microBias,
        primary_bias: primaryBias,
        macro_bias: macroBias,
        context_bias: contextBias,
    };
}

// ------------------------------
// Flat-HOLD cooldown sanitation
// ------------------------------

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

export type HoldCooldown = {
    cooldownMinutes: number | null;
    wakeAbove: number | null;
    wakeBelow: number | null;
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
}): HoldCooldown {
    const notes: string[] = [];
    if (params.positionOpen || String(params.action).toUpperCase() !== 'HOLD') {
        return { cooldownMinutes: null, wakeAbove: null, wakeBelow: null, notes };
    }
    const rawMinutes = Number(params.cooldownMinutes);
    if (!Number.isFinite(rawMinutes) || rawMinutes <= 0) {
        return { cooldownMinutes: null, wakeAbove: null, wakeBelow: null, notes };
    }
    const cooldownMinutes = Math.min(HOLD_COOLDOWN_MAX_MINUTES, Math.max(HOLD_COOLDOWN_MIN_MINUTES, Math.round(rawMinutes)));
    if (cooldownMinutes !== Math.round(rawMinutes)) notes.push(`clamped_${Math.round(rawMinutes)}m_to_${cooldownMinutes}m`);

    const price = Number(params.price);
    const priceKnown = Number.isFinite(price) && price > 0;
    let wakeAbove = Number(params.wakeAbove);
    let wakeBelow = Number(params.wakeBelow);
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
    return { cooldownMinutes, wakeAbove: above, wakeBelow: below, notes };
}

// ------------------------------
// Exchange-side TP/SL sanitation
// ------------------------------

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
const ENTRY_TP_MIN_ATR = 2;
// An entry stop closer than this is inside ordinary bar noise and would likely
// be wicked out immediately — dropped in favour of the catastrophe default.
const ENTRY_SL_MIN_ATR = 1;
const TP_MAX_ATR = 10;
const AMEND_MIN_GAP_ATR = 0.1;

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

// A pullback limit must be a genuine pullback: at least MIN_ATR below (BUY) /
// above (SELL) current price. An invalid limit (wrong side, inside the noise
// band, or unverifiable without ATR) DROPS the entry for this tick instead of
// silently converting to a market order — the model asked for a patience
// price, and filling it at market is exactly the chase the prompt forbids
// (null from the model is the only way to request market). Beyond MAX_ATR the
// fill odds within the one-tick TTL are negligible and the bracket math
// distorts, so it clamps.
const ENTRY_LIMIT_MIN_ATR = 0.1;
const ENTRY_LIMIT_MAX_ATR = 1.5;

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

// Strict Structured-Outputs schema for the swing decision. Mirrors the JSON the prompt
// asks for; strict mode requires every property in `required` and additionalProperties:false.
// Nullable fields use a type union (e.g. ['integer','null']).
export const SWING_DECISION_SCHEMA = {
    name: 'swing_decision',
    schema: {
        type: 'object',
        additionalProperties: false,
        required: [
            'action',
            'summary',
            'reason',
            'exit_size_pct',
            'leverage',
            'raise_leverage_to',
            'move_stop_to_be',
            'take_profit_price',
            'stop_loss_price',
            'entry_limit_price',
            'cooldown_minutes',
            'cooldown_wake_above',
            'cooldown_wake_below',
        ],
        properties: {
            action: { type: 'string', enum: ['BUY', 'SELL', 'HOLD', 'CLOSE', 'REVERSE'] },
            summary: { type: 'string' },
            reason: { type: 'string' },
            exit_size_pct: { type: ['number', 'null'], minimum: 0, maximum: 100 },
            leverage: { type: ['integer', 'null'], minimum: 5, maximum: 10 },
            // Profit-lock margin-recycle maneuver (crypto only). Execution clamps
            // raise_leverage_to to [current, symbol max]; 125 is a generous ceiling.
            raise_leverage_to: { type: ['integer', 'null'], minimum: 1, maximum: 125 },
            move_stop_to_be: { type: ['boolean', 'null'] },
            // Exchange-side bracket. Entry: take_profit_price is the resting TP
            // attached with the order; stop_loss_price is the structural
            // invalidation stop (null = code-owned 3×ATR catastrophe default).
            // In-position: either field amends the standing bracket (null =
            // leave unchanged; stop amends tighten-only). Price-level sanity
            // (side/distance vs live price+ATR) is enforced in code after parse.
            take_profit_price: { type: ['number', 'null'], minimum: 0 },
            stop_loss_price: { type: ['number', 'null'], minimum: 0 },
            // Pullback limit entry (flat BUY/SELL, only when the
            // SWING_PULLBACK_LIMIT_ENABLED day-trade flag is on): rest a LIMIT
            // at this price instead of entering at market. One-tick TTL —
            // cancelled at the next evaluation if unfilled. null = market;
            // with the flag off (swing default) a non-null value drops the
            // entry (sanitizeEntryLimit).
            entry_limit_price: { type: ['number', 'null'], minimum: 0 },
            // Flat-HOLD cooldown: quiet period request (minutes, code-clamped)
            // with optional wake bands that end it early when price crosses
            // them. Only honored when flat + action=HOLD.
            cooldown_minutes: { type: ['integer', 'null'] },
            cooldown_wake_above: { type: ['number', 'null'], minimum: 0 },
            cooldown_wake_below: { type: ['number', 'null'], minimum: 0 },
        },
    },
} as const;

// Capital decides leverage by asset class, so the model is not asked for it and
// the schema omits the field entirely (strict structured-output requires the
// schema to match the prompt's JSON exactly — no leverage key at all).
export const SWING_DECISION_SCHEMA_NO_LEVERAGE = {
    name: 'swing_decision',
    schema: {
        type: 'object',
        additionalProperties: false,
        required: [
            'action',
            'summary',
            'reason',
            'exit_size_pct',
            'take_profit_price',
            'stop_loss_price',
            'entry_limit_price',
            'cooldown_minutes',
            'cooldown_wake_above',
            'cooldown_wake_below',
        ],
        properties: {
            action: { type: 'string', enum: ['BUY', 'SELL', 'HOLD', 'CLOSE', 'REVERSE'] },
            summary: { type: 'string' },
            reason: { type: 'string' },
            exit_size_pct: { type: ['number', 'null'], minimum: 0, maximum: 100 },
            // Exchange-side bracket (see SWING_DECISION_SCHEMA).
            take_profit_price: { type: ['number', 'null'], minimum: 0 },
            stop_loss_price: { type: ['number', 'null'], minimum: 0 },
            entry_limit_price: { type: ['number', 'null'], minimum: 0 },
            // Flat-HOLD cooldown (see SWING_DECISION_SCHEMA).
            cooldown_minutes: { type: ['integer', 'null'] },
            cooldown_wake_above: { type: ['number', 'null'], minimum: 0 },
            cooldown_wake_below: { type: ['number', 'null'], minimum: 0 },
        },
    },
} as const;

export type AiThreadCallResult = {
    json: any;
    // Responses API id of THIS call (`resp_...`) — persist it and pass it back as
    // `previousResponseId` on the next tick to continue the conversation.
    responseId: string | null;
    // Model that actually served the call (from the API response, not the
    // request) — persisted on the decision row for post-mortems.
    model: string | null;
    // Token accounting, normalized to the same field names the Claude client
    // returns so decision rows are provider-uniform. cached input tokens map to
    // cache_read; the Responses API has no cache-creation notion (null).
    usage: {
        input_tokens: number;
        output_tokens: number;
        cache_creation_input_tokens: number | null;
        cache_read_input_tokens: number | null;
    } | null;
};

async function readAiErrorDetails(res: Response): Promise<string> {
    try {
        const errJson = await res.json();
        const msg =
            (errJson as any)?.error?.message ||
            (errJson as any)?.message ||
            (typeof errJson === 'string' ? errJson : JSON.stringify(errJson));
        return msg ? ` - ${msg}` : '';
    } catch {
        try {
            const errText = await res.text();
            return errText ? ` - ${errText.slice(0, 600)}` : '';
        } catch {
            return '';
        }
    }
}

// OpenAI Responses API (stateful). When `previousResponseId` is passed the server
// replays the whole stored conversation — the entry decision and every in-position
// management tick — as context, so the model manages a position with memory of its
// own thesis instead of a stateless snapshot each tick. Responses are stored
// server-side (`store: true`, ~30-day retention); a lost/expired chain head
// degrades to a stateless call instead of failing the trading tick, and the caller
// re-anchors the chain on the returned responseId.
export async function callAIThread(
    system: string,
    user: string,
    schema?: { name: string; schema: Record<string, unknown> },
    opts?: { previousResponseId?: string | null },
): Promise<AiThreadCallResult> {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error('Missing OPENAI_API_KEY');

    // Structured Outputs (json_schema, strict) guarantees the response shape at the
    // API layer when a caller supplies a schema; otherwise fall back to JSON mode.
    // Responses API uses a flattened text.format (no chat-completions wrapper).
    const format = schema
        ? { type: 'json_schema', name: schema.name, schema: schema.schema, strict: true }
        : { type: 'json_object' };

    const request = (previousResponseId: string | null) =>
        fetch(`${AI_BASE_URL}/responses`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                Authorization: `Bearer ${apiKey}`,
            },
            body: JSON.stringify({
                model: AI_MODEL,
                // `instructions` is per-call (NOT inherited via previous_response_id),
                // so the system prompt rides along on every turn of a chain.
                instructions: system,
                input: user,
                // gpt-5.x reasoning models only accept the default temperature (1);
                // determinism comes from reasoning effort + the post-processing gates.
                reasoning: { effort: 'medium' },
                text: { format },
                store: true,
                // Long position threads grow the stored context each tick; drop
                // middle turns server-side instead of erroring the tick when the
                // chain outgrows the model's context window.
                truncation: 'auto',
                ...(previousResponseId ? { previous_response_id: previousResponseId } : {}),
            }),
        });

    let usedPreviousResponseId = opts?.previousResponseId ?? null;
    let res = await request(usedPreviousResponseId);
    if (!res.ok && usedPreviousResponseId && (res.status === 400 || res.status === 404)) {
        const details = await readAiErrorDetails(res);
        if (/previous[_ ]?response/i.test(details)) {
            console.warn(`AI thread head ${usedPreviousResponseId} rejected (${res.status}${details}); retrying stateless`);
            usedPreviousResponseId = null;
            res = await request(null);
        } else {
            throw new Error(`AI error: ${res.status} ${res.statusText}${details}`);
        }
    }

    if (!res.ok) {
        const details = await readAiErrorDetails(res);
        throw new Error(`AI error: ${res.status} ${res.statusText}${details}`);
    }

    const data = await res.json();
    // Raw REST shape: output is an array of items (reasoning, message, ...);
    // the assistant text lives on the message item's output_text content part.
    const message = Array.isArray(data?.output) ? data.output.find((item: any) => item?.type === 'message') : null;
    const text =
        message?.content?.find?.((c: any) => c?.type === 'output_text')?.text ||
        (typeof data?.output_text === 'string' ? data.output_text : '') ||
        '{}';
    const responseId = typeof data?.id === 'string' && data.id ? data.id : null;
    const model = typeof data?.model === 'string' && data.model ? data.model : AI_MODEL;
    const rawUsage = data?.usage;
    const usage =
        rawUsage && Number.isFinite(Number(rawUsage.input_tokens))
            ? {
                  input_tokens: Number(rawUsage.input_tokens),
                  output_tokens: Number(rawUsage.output_tokens) || 0,
                  cache_creation_input_tokens: null,
                  cache_read_input_tokens: Number.isFinite(Number(rawUsage.input_tokens_details?.cached_tokens))
                      ? Number(rawUsage.input_tokens_details.cached_tokens)
                      : null,
              }
            : null;
    try {
        return { json: JSON.parse(text), responseId, model, usage };
    } catch {
        throw new Error(`AI returned non-JSON content: ${String(text).slice(0, 600)}`);
    }
}

// Stateless calls (forex advisor, evaluations) go through
// lib/aiProvider.callStatelessAI — same provider switch as the swing decision.
