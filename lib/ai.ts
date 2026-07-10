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
import type { ForexSessionLevelsContext } from './swing/sessionLevels';
import { computeWaveGeometry } from './swing/waveGeometry';
import type { NanoContext } from './swing/waveGeometry';
import { setEvaluation, getEvaluation } from './utils';

// The AI's own prior rationale attached to the open position: the decision that
// opened it, and any partial trims since. Prior reasoning fed back verbatim —
// not a code-computed verdict — so it stays inside the measurements-not-verdicts
// prompt contract.
export type PositionDecisionNote = {
    action: string;
    ts?: string;
    price?: number; // market price when the decision was made
    summary?: string;
    reason?: string;
    exit_size_pct?: number;
};

export type PositionContext = {
    side: 'long' | 'short';
    entry_price?: number;
    entry_ts?: string;
    hold_minutes?: number;
    unrealized_pnl_pct?: number;
    max_drawdown_pct?: number;
    max_profit_pct?: number;
    breakeven_price?: number;
    taker_fee_rate?: number;
    // Standing exchange-side bracket on the open position (null = no resting
    // order on that side). Shown to the model so TP/SL amendments are made
    // against the actual current levels.
    take_profit_price?: number | null;
    stop_loss_price?: number | null;
    opening_decision?: PositionDecisionNote | null;
    partial_closes?: PositionDecisionNote[];
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
    recentActions: { action: string; timestamp: number; closePct?: number | null }[] = [],
    realizedRoiPct?: number | null,
    dryRun?: boolean,
    spreadBpsOverride?: number,
    decisionPolicy?: DecisionPolicy,
    category?: string | null,
    platform?: string | null,
    lastClosedPosition?: LastClosedPosition | null,
    nowMs?: number,
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
    // Costs (educate the model)
    const taker_fee_rate_side = Number.isFinite(position_context?.taker_fee_rate as number)
        ? Math.max(0, Number(position_context?.taker_fee_rate))
        : DEFAULT_TAKER_FEE_RATE; // default set via env (e.g., 0.0006 = 6 bps per side)
    const taker_round_trip_bps = Number((taker_fee_rate_side * 2 * 10000).toFixed(2));
    const slippage_bps = 2;
    const total_cost_bps = Number((taker_round_trip_bps + slippage_bps).toFixed(1));

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
    // not pick it. Only crypto (Bitget) takes a model-chosen 1–5 leverage.
    const isCapital = String(platform || '').toLowerCase() === 'capital';
    const leverageGuidance = isCapital
        ? 'Leverage: do NOT set it — on this venue leverage is broker-defined per asset class, not chosen here. Always output leverage=null.'
        : `Leverage 1–5 by conviction AND risk: cut to 1–2 even on HIGH conviction when extended or near major ${contextTimeframe} levels. null on HOLD/CLOSE.`;
    const leverageTask = isCapital
        ? 'do NOT output a leverage field — leverage is broker-defined per asset class on this venue.'
        : 'leverage 1–5 for BUY/SELL/REVERSE, else null.';
    // Capital: omit the leverage key entirely (no comma). Bitget: include it.
    const leverageJsonField = isCapital ? '' : ',"leverage":null|1|2|3|4|5';

    // Profit-lock margin-recycle maneuver (crypto only). The crypto schema always
    // carries these keys (nullable); the maneuver is only *explained* to the model
    // when ENABLE_CRYPTO_MARGIN_RECYCLE is set — otherwise it's told to null them.
    const marginRecycleEnabled = !isCapital && process.env.ENABLE_CRYPTO_MARGIN_RECYCLE === 'true';
    const manageJsonField = isCapital ? '' : ',"raise_leverage_to":null|int,"move_stop_to_be":true|false|null';
    const manageGuidance = isCapital
        ? ''
        : marginRecycleEnabled
          ? 'Margin recycle (only with a real profit cushion): on HOLD or a partial CLOSE you MAY move the stop to breakeven (move_stop_to_be=true) and raise leverage (raise_leverage_to, up to the symbol exchange max — the system clamps your value to [current, max]). On isolated margin this frees margin for future trades WITHOUT cutting size, and the breakeven stop caps the remainder’s risk. A leverage raise always forces a breakeven stop first. Not in profit → null both.'
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
        costs: {
            round_trip_fee_bps: taker_round_trip_bps,
            slippage_bps,
            total_cost_bps,
            recent_realized_pnl_pct: clampNumber(realizedRoiPct ?? null, 2),
        },
        position: position_context
            ? { open: true, ...position_context }
            : { open: false, status: position_status, reentry_cooldown: reentryCooldown },
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
                  return {
                      action: partial ? `CLOSE ${Math.round(a.closePct as number)}%` : a.action,
                      ts: new Date(a.timestamp).toISOString(),
                  };
              })
            : [],
    };
    if (forex_event_context && typeof forex_event_context === 'object') {
        market.forex_events = forex_event_context;
    }
    if (forex_session_context && typeof forex_session_context === 'object') {
        market.forex_session = forex_session_context;
    }

    // Each note describes ONLY the context this category actually receives (see
    // /api/analyze: forex_session is built for forex/commodity/index; forex_events for
    // forex only; crypto gets neither). Keep prose aligned with the data or it misleads.
    const assetNote =
        assetClass === 'crypto'
            ? 'Asset class: crypto. Trades 24/7 — no session boundaries or weekend gaps, and no session-levels or economic-calendar block is provided. News/sentiment can move price fast. Funding/borrow is not modeled here; judge on structure, regime and location.'
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

    const sys = `
You are an expert swing-trading market-structure analyst. Decide one action and size it.

${assetNote}

TIMEFRAMES (fixed)
- micro=${microTimeframe} (entry timing/confirmation), primary=${primaryTimeframe} (setup+execution), macro=${macroTimeframe} (regime bias), context=${contextTimeframe} (HTF location + major levels, risk lever)${nano_context ? ', nano=15m (state.geometry.nano — wave/entry timing only, never a setup by itself)' : ''}.
Strategy: ${primaryTimeframe} swing setups with ${microTimeframe} confirmation, aligned with (or tactically fading) the ${macroTimeframe} regime while respecting ${contextTimeframe} location. Holding horizon ~1–10 days. Prefer fewer, higher-quality trades; avoid churn.

INPUTS
- You receive two JSON objects: STATE (derived signals — your single source of truth) and MARKET (raw price/tape/news). All keys are pre-computed; do not invent fields.
- micro_bias precedence (already applied in state.biases.micro): structure (breakout-retest → break-state → BOS → structure-state) first, momentum (EMA slope+RSI+price vs EMA20) as fallback; structure wins ties.

DECISION OWNERSHIP
- You own the conviction read: judge setup quality and selectivity yourself from the structure, location, regime and momentum measurements in STATE — there is no pre-computed verdict to defer to.${isCapital ? '' : ' Size leverage to that conviction.'}
- The HARD constraints below are enforced in code AFTER you respond. Do not spend reasoning re-deriving them — if you violate one your action is silently coerced (a wasted call). Just stay inside them:
  1. Allowed actions: flat → BUY/SELL/HOLD; in a position → HOLD/CLOSE/REVERSE only.
  2. Trend guard: no counter-trend entry/flip against an aligned primary+micro trend (${trendGuardException}).
  3. Entry timing: when flat and momentum.micro_entry_ok=false, entries are blocked (${microEntryException}).
  4. Anti-flip: a repeated CLOSE/REVERSE within ${antiFlipWindow} is blocked unless ${antiFlipStrength}.
  5. Base gates: if any of state.gates.{spread_ok,liquidity_ok,atr_ok,slippage_ok} is false → entries forced to HOLD and risk-off forced while in a position.${REENTRY_COOLDOWN_MIN > 0 ? `\n  6. Re-entry cooldown: for ${REENTRY_COOLDOWN_MIN} min after a position closes, re-entering the SAME direction is blocked (state.position.reentry_cooldown shows the blocked side when active; the opposite direction stays allowed).` : ''}

YOUR JOB (soft judgment — where your reasoning actually matters)
- Pick the highest-quality action consistent with STATE, then size it. Structure (BOS/CHoCH/breakout-retest) outweighs raw momentum.
- Location vs regime: prefer entries aligned with macro+context. Counter-regime only at extreme location with clean invalidation. Do NOT open into a near opposite level (levels.*.dist_atr or location.context_*_dist_atr under ~0.6 ATR) unless the matching breakout/breakdown is confirmed. If both nearest levels are close (location.chop_risk), treat as chop and avoid fresh entries without clean confirmed level logic.
- Level-bounce entries are a first-class setup, NOT a counter-regime fade: at one primary level (dist_atr ≤ ~${ACTIONABILITY_NEAR_ATR}) with the opposite level far (≥ ~${ACTIONABILITY_ROOM_ATR} ATR of room) and micro structure turning that way, an entry toward the room is legitimate even when macro/context lean against it. Judge it on the level's strength/state and the micro turn; invalidation sits just beyond the level, so the risk is defined. Do not reject these solely for regime misalignment.
- Extension (risk control, not a signal): |state.extension_atr.micro| ≥ ${extensionMicroAvoid} or |state.extension_atr.primary| ≥ ${extensionPrimaryAvoid} → avoid fresh entries; micro > ${extensionMicroNoEntry} → strongly prefer none. RSI extremes are NOT a counter-trend trigger by themselves — only "permission" once structure shows damage/flip.
- Wave position (state.geometry — WHERE in the wave to act; structure/levels still decide WHETHER): channel_pos maps price inside the timeframe's regression channel (0=low, 1=high), slope_atr is its drift per bar. Time entries into the wave, not onto its crest: in an up-sloping channel prefer longs near the channel low / last_swing_low (channel_pos ≲ 0.4) and AVOID fresh longs at channel_pos ≳ 0.75 or right at last_swing_high without a confirmed break — mirror for shorts in a down-slope. support_trendline / resistance_trendline give the live trendline price and slope; a close through them plus a structure signal = break, a touch alone = reaction point. When geometry.nano is present, use it to fine-time the trigger (nano wave trough in an up leg beats a nano crest) — never as a standalone reason to trade against micro/primary structure. If a good setup sits at a bad wave position, HOLD and wait for the pullback rather than paying the crest.
- Cost/churn: round-trip cost ≈ ${total_cost_bps} bps. If the expected swing is not clearly larger than cost, or the setup is unclear/MED-LOW quality, prefer HOLD.
- In a position: prefer HOLD when regime supports it and there is no strong opposite structure (especially |unrealized_pnl_pct| < 0.25%). Trim 30–70% (exit_size_pct) on gains into a major opposite level, weakening regime, or exhausted volatility expansion. REVERSE = full close then open opposite (exit_size_pct=100, no partials) and only on a confirmed primary structure flip.${
        position_context?.opening_decision
            ? `\n- Entry thesis: state.position.opening_decision is your own rationale that opened this position${position_context.partial_closes?.length ? ', and state.position.partial_closes are the trims you took since, with their reasons' : ''}. Manage against that thesis — HOLD while it stays intact; trim/CLOSE when it is invalidated or has played out. Weigh it as context, not a command: current structure wins on conflict.`
            : ''
    }
- Exchange-side TP/SL bracket (fills 24/7 between these evaluations):
  • On BUY/SELL — and on REVERSE, for the NEW opposite-side position — ALWAYS set take_profit_price: a structural price target (next opposing level from state.levels, measured move, or value-area edge), at least ~${ENTRY_TP_MIN_ATR} primary-ATR away. It rests on the exchange until the next evaluation. If you output null, the system attaches a wide ${EXCHANGE_TP_FALLBACK_ATR_MULT}×ATR default. Output stop_loss_price=null on entries — a ${EXCHANGE_TP_FALLBACK_ATR_MULT}×ATR catastrophe stop is attached automatically.
  • In a position (HOLD or partial CLOSE), you MAY amend the standing bracket: output a new take_profit_price and/or stop_loss_price, or null to leave a leg unchanged. state.position.take_profit_price / stop_loss_price show the current resting levels (null = none on that leg). Tighten the stop as profit builds (structure-based, e.g. just past the last defended swing); move the TP only for a structural reason, not to chase price.
  • Both must sit on the correct side of current price; a stop may never sit wider than ${EXCHANGE_SL_MAX_ATR_MULT}×ATR from current price. Invalid values are clamped or dropped in code — don't waste them.
- Pullback limit entry (flat BUY/SELL only): when the SETUP is valid but the WAVE POSITION is bad (channel_pos high for a long / low for a short, price at a crest), set entry_limit_price to the pullback level you would rather pay — e.g. the channel low, last_swing_low, a trendline touch, or a broken level's retest (BUY below current price, SELL above; usable window ${ENTRY_LIMIT_MIN_ATR}–${ENTRY_LIMIT_MAX_ATR} primary-ATR from price). The order rests on the venue and is CANCELLED at the next evaluation if unfilled (~1h TTL) — so it is a free option on better timing, not a standing commitment. Your take_profit_price and the automatic catastrophe stop are anchored at the LIMIT price. null = enter at market now. Use market when timing is already good; use the limit instead of HOLDing when only timing is wrong.
- ${leverageGuidance}${manageGuidance ? `\n- ${manageGuidance}` : ''}
- Position truthfulness: never describe a position as winning when unrealized_pnl_pct < 0 or price_vs_breakeven_pct is on the losing side.

OUTPUT
- Strict JSON only, parseable by JSON.parse — no markdown, comments, trailing commas, or extra keys.
- Decision policy mode: ${decisionPolicyLabel}.
`.trim();

    const user = `
You are analyzing ${baseSymbol} for swing trading (mode=${modeLabel}, asset_class=${assetClass}).
Timeframes: micro=${microTimeframe}, primary=${primaryTimeframe}, macro=${macroTimeframe}, context=${contextTimeframe}${nano_context ? ', nano=15m' : ''}. Called ~once per ${microTimeframe}. Decision policy: ${decisionPolicyLabel}.
S/R levels are swing-pivot derived per timeframe (~150 bars); distances are in that timeframe's ATR; level state ∈ {at_level, approaching, rejected, broken, retesting}.

STATE (derived signals — single source of truth):
${JSON.stringify(state)}

MARKET (raw inputs):
${JSON.stringify(market)}

TASKS:
1) Output exactly one allowed action (see DECISION OWNERSHIP): flat → BUY/SELL/HOLD; in a position → HOLD/CLOSE/REVERSE.
2) ${leverageTask}
3) exit_size_pct for CLOSE/REVERSE (100 = full close, 30–70 = trim), else null.
4) take_profit_price: REQUIRED price target on BUY/SELL/REVERSE (resting exchange TP; on REVERSE target the NEW opposite-side position); on in-position HOLD/partial CLOSE a new level amends the standing TP (null = unchanged); else null. stop_loss_price: only to amend the stop while in a position (null = unchanged); always null on entries/REVERSE.
5) entry_limit_price: on flat BUY/SELL you MAY rest a pullback limit instead of market (see guidance; cancelled next evaluation if unfilled); else null.
6) summary ≤3 lines; reason = brief rationale.

Respond with strict JSON only:
{"action":"BUY|SELL|HOLD|CLOSE|REVERSE","summary":"≤2 lines","reason":"brief rationale","exit_size_pct":null|0-100,"take_profit_price":null|price,"stop_loss_price":null|price,"entry_limit_price":null|price${leverageJsonField}${manageJsonField}}
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
    recentActions: { action: string; timestamp: number; closePct?: number | null }[] = [],
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
    recentActions: { action: string; timestamp: number; closePct?: number | null }[];
    positionContext: PositionContext | null;
    policy?: DecisionPolicy;
    lastClosedPosition?: LastClosedPosition | null;
}) {
    const { decision, context, gates, positionOpen, recentActions, positionContext, policy, lastClosedPosition } =
        params;
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
    if (!positionOpen && (action === 'BUY' || action === 'SELL')) {
        const cooldown = resolveReentryCooldown(lastClosedPosition);
        if (cooldown && desiredSide === cooldown.blockedSide) action = 'HOLD';
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
    // fallback) is enforced by sanitizeExchangeTpSl in the API route, which has
    // the live price + ATR. Entry SL stays code-owned (catastrophe stop), so it
    // is nulled on BUY/SELL/REVERSE (REVERSE opens a fresh position and gets the
    // same entry bracket, targeted for the NEW side).
    const coercePrice = (v: unknown) => {
        const n = Number(v);
        return Number.isFinite(n) && n > 0 ? n : null;
    };
    const isEntryAction = action === 'BUY' || action === 'SELL' || action === 'REVERSE';
    const tpslAmendEligible =
        positionOpen && (action === 'HOLD' || (action === 'CLOSE' && exit_size_pct != null && exit_size_pct < 100));
    const take_profit_price =
        isEntryAction || tpslAmendEligible ? coercePrice(decision?.take_profit_price) : null;
    const stop_loss_price = tpslAmendEligible ? coercePrice(decision?.stop_loss_price) : null;
    // Pullback limit entry: flat BUY/SELL only (REVERSE stays market — it must
    // actually flip the exposure, not maybe-flip it). Price-side/distance
    // sanity is enforced by sanitizeEntryLimit in the API route.
    const entry_limit_price =
        !positionOpen && (action === 'BUY' || action === 'SELL') ? coercePrice(decision?.entry_limit_price) : null;

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
        signal_strength: signalStrength,
        micro_bias: microBias,
        primary_bias: primaryBias,
        macro_bias: macroBias,
        context_bias: contextBias,
    };
}

// ------------------------------
// Exchange-side TP/SL sanitation
// ------------------------------

// Entry TP fallback mirrors the 3×ATR catastrophe stop in /api/analyze, so an
// entry the model leaves without a target still gets a symmetric (~1:1 R)
// exchange-side bracket instead of an unbounded upside leg.
export const EXCHANGE_TP_FALLBACK_ATR_MULT = 3;
// A stop may never sit wider than the catastrophe distance from CURRENT price —
// amendments can tighten protection, never loosen it past the entry risk model.
export const EXCHANGE_SL_MAX_ATR_MULT = 3;
const ENTRY_TP_MIN_ATR = 0.5;
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
 * Entries: `side` is derived from the action; stop_loss_price is always null
 * (the catastrophe stop is code-owned). REVERSE is an entry for the OPPOSITE
 * of the current position side — same treatment (TP with fallback, SL
 * code-owned). In-position (HOLD / partial CLOSE): both legs may amend the
 * standing bracket; null = leave unchanged.
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

    // Take profit: must sit on the profit side of price; entries need real room
    // (≥0.5 ATR) so the resting TP isn't an instant fill, amends just need to
    // clear the current price by a noise buffer.
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

    // Stop loss: amend-only (entry SL is the code-owned catastrophe stop). Must
    // be protective vs current price and never wider than the catastrophe
    // distance from current price.
    let sl = !isEntry && Number.isFinite(params.stopLossPrice as number) && (params.stopLossPrice as number) > 0 ? Number(params.stopLossPrice) : null;
    if (sl != null) {
        if (dir * (price - sl) <= 0) {
            notes.push('sl_wrong_side_dropped');
            sl = null;
        } else if (atr) {
            const distAtr = Math.abs(price - sl) / atr;
            if (distAtr < AMEND_MIN_GAP_ATR) {
                notes.push('sl_too_close_dropped');
                sl = null;
            } else if (distAtr > EXCHANGE_SL_MAX_ATR_MULT) {
                sl = price - dir * EXCHANGE_SL_MAX_ATR_MULT * atr;
                notes.push('sl_clamped_max_atr');
            }
        }
    }
    if (sl != null && !(sl > 0)) sl = null;

    return { takeProfitPrice: tp, stopLossPrice: sl, notes };
}

// ------------------------------
// Pullback limit entry sanitation
// ------------------------------

// A pullback limit must be a genuine pullback: at least MIN_ATR below (BUY) /
// above (SELL) current price — anything closer is effectively a market order,
// so we just market it. Beyond MAX_ATR the fill odds within the one-tick TTL
// are negligible and the bracket math distorts, so it clamps.
const ENTRY_LIMIT_MIN_ATR = 0.1;
const ENTRY_LIMIT_MAX_ATR = 1.5;

/**
 * Validate the model's pullback entry limit against live price + primary ATR.
 * Returns the usable limit price or null (= market entry), with notes.
 * Only flat BUY/SELL qualifies; a missing ATR fails open to market entry.
 */
export function sanitizeEntryLimit(params: {
    action: string;
    positionOpen: boolean;
    price: number;
    primaryAtr: number | null;
    entryLimitPrice: number | null;
}): { entryLimitPrice: number | null; notes: string[] } {
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
    if (raw == null) return { entryLimitPrice: null, notes };
    if (params.positionOpen || (action !== 'BUY' && action !== 'SELL') || !(price > 0)) {
        return { entryLimitPrice: null, notes: ['limit_not_applicable'] };
    }
    if (!atr) return { entryLimitPrice: null, notes: ['limit_dropped_no_atr'] };

    const dir = action === 'BUY' ? 1 : -1;
    // Pullback distance: positive = on the pullback side of price.
    const distAtr = (dir * (price - raw)) / atr;
    if (distAtr < ENTRY_LIMIT_MIN_ATR) {
        notes.push('limit_too_close_market_entry');
        return { entryLimitPrice: null, notes };
    }
    if (distAtr > ENTRY_LIMIT_MAX_ATR) {
        const clamped = price - dir * ENTRY_LIMIT_MAX_ATR * atr;
        notes.push('limit_clamped_max_atr');
        return { entryLimitPrice: clamped > 0 ? clamped : null, notes };
    }
    return { entryLimitPrice: raw, notes };
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
        ],
        properties: {
            action: { type: 'string', enum: ['BUY', 'SELL', 'HOLD', 'CLOSE', 'REVERSE'] },
            summary: { type: 'string' },
            reason: { type: 'string' },
            exit_size_pct: { type: ['number', 'null'], minimum: 0, maximum: 100 },
            leverage: { type: ['integer', 'null'], minimum: 1, maximum: 5 },
            // Profit-lock margin-recycle maneuver (crypto only). Execution clamps
            // raise_leverage_to to [current, symbol max]; 125 is a generous ceiling.
            raise_leverage_to: { type: ['integer', 'null'], minimum: 1, maximum: 125 },
            move_stop_to_be: { type: ['boolean', 'null'] },
            // Exchange-side bracket. Entry: take_profit_price is the resting TP
            // attached with the order. In-position: either field amends the
            // standing bracket (null = leave unchanged). Price-level sanity
            // (side/distance vs live price+ATR) is enforced in code after parse.
            take_profit_price: { type: ['number', 'null'], minimum: 0 },
            stop_loss_price: { type: ['number', 'null'], minimum: 0 },
            // Pullback limit entry (flat BUY/SELL only): rest a LIMIT at this
            // price instead of entering at market. One-tick TTL — cancelled at
            // the next AI evaluation / hourly tick if unfilled. null = market.
            entry_limit_price: { type: ['number', 'null'], minimum: 0 },
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
        required: ['action', 'summary', 'reason', 'exit_size_pct', 'take_profit_price', 'stop_loss_price', 'entry_limit_price'],
        properties: {
            action: { type: 'string', enum: ['BUY', 'SELL', 'HOLD', 'CLOSE', 'REVERSE'] },
            summary: { type: 'string' },
            reason: { type: 'string' },
            exit_size_pct: { type: ['number', 'null'], minimum: 0, maximum: 100 },
            // Exchange-side bracket (see SWING_DECISION_SCHEMA).
            take_profit_price: { type: ['number', 'null'], minimum: 0 },
            stop_loss_price: { type: ['number', 'null'], minimum: 0 },
            entry_limit_price: { type: ['number', 'null'], minimum: 0 },
        },
    },
} as const;

export async function callAI(
    system: string,
    user: string,
    schema?: { name: string; schema: Record<string, unknown> },
) {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error('Missing OPENAI_API_KEY');

    const base = AI_BASE_URL;
    const model = AI_MODEL;

    // Structured Outputs (json_schema, strict) guarantees the response shape at the API
    // layer when a caller supplies a schema; otherwise fall back to free-form JSON mode.
    const response_format = schema
        ? { type: 'json_schema', json_schema: { name: schema.name, schema: schema.schema, strict: true } }
        : { type: 'json_object' };

    const res = await fetch(`${base}/chat/completions`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
            model,
            messages: [
                { role: 'system', content: system },
                { role: 'user', content: user },
            ],
            // gpt-5.x reasoning models only accept the default temperature (1); determinism
            // comes from reasoning_effort + the post-processing gates, not a low temperature.
            reasoning_effort: 'medium',
            response_format,
        }),
    });

    if (!res.ok) {
        let details = '';
        try {
            const errJson = await res.json();
            const msg =
                errJson?.error?.message ||
                errJson?.message ||
                (typeof errJson === 'string' ? errJson : JSON.stringify(errJson));
            details = msg ? ` - ${msg}` : '';
        } catch {
            try {
                const errText = await res.text();
                details = errText ? ` - ${errText.slice(0, 600)}` : '';
            } catch {
                details = '';
            }
        }
        throw new Error(`AI error: ${res.status} ${res.statusText}${details}`);
    }

    const data = await res.json();
    const text = data.choices?.[0]?.message?.content || '{}';
    try {
        return JSON.parse(text);
    } catch {
        throw new Error(`AI returned non-JSON content: ${String(text).slice(0, 600)}`);
    }
}
