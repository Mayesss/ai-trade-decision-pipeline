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
import { setEvaluation, getEvaluation } from './utils';

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
    gates: any, // <--- Retain the gates object for the base gate checks
    position_context: PositionContext | null = null,
    momentumSignalsOverride?: MomentumSignals,
    recentActions: { action: string; timestamp: number }[] = [],
    realizedRoiPct?: number | null,
    dryRun?: boolean,
    spreadBpsOverride?: number,
    decisionPolicy?: DecisionPolicy,
    category?: string | null,
    platform?: string | null,
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

    const normalizedNewsSentiment =
        typeof news_sentiment === 'string' && news_sentiment.length > 0 ? news_sentiment : null;
    const normalizedHeadlines = Array.isArray(news_headlines) ? news_headlines.filter((h) => !!h).slice(0, 5) : [];

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
    const oversoldMicro = typeof rsi_micro === 'number' && rsi_micro <= 35;
    const overboughtMicro = typeof rsi_micro === 'number' && rsi_micro >= 65;
    const reversalOpportunity = oversoldMicro ? 'oversold' : overboughtMicro ? 'overbought' : null;

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

    const longDrivers = [
        structure4hState === 'bull' || (bos4h && bosDir4h === 'up'),
        (breakoutRetestOk4h && breakoutRetestDir4h === 'up') || nearPrimarySupport,
        macroBias !== 'DOWN',
        contextBias !== 'DOWN' || intoContextSupport,
        valueOkLong,
    ];

    const shortDrivers = [
        structure4hState === 'bear' || (bos4h && bosDir4h === 'down'),
        (breakoutRetestOk4h && breakoutRetestDir4h === 'down') || nearPrimaryResistance,
        macroBias !== 'UP',
        contextBias !== 'UP' || intoContextResistance,
        valueOkShort,
    ];

    const countTrue = (items: boolean[]) => items.reduce((acc, v) => acc + (v ? 1 : 0), 0);
    const longAlignedDriverCount = countTrue(longDrivers);
    const shortAlignedDriverCount = countTrue(shortDrivers);
    const alignedDriverCount = Math.max(longAlignedDriverCount, shortAlignedDriverCount);
    const favoredSide =
        longAlignedDriverCount > shortAlignedDriverCount
            ? 'long'
            : shortAlignedDriverCount > longAlignedDriverCount
              ? 'short'
              : 'neutral';

    const macroPenalty =
        (momentumSignals.macroTrendDown && favoredSide === 'long') ||
        (momentumSignals.macroTrendUp && favoredSide === 'short');
    const mediumActionReady =
        alignedDriverCount >= 4 && (momentumSignals.longMomentum || momentumSignals.shortMomentum) && !macroPenalty;

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
    const closingAlert = Boolean(macroOpposesPosition && (priceVsBreakevenPct ?? 0) < -0.15);

    const reverseConfidence =
        alignedDriverCount >= 5 && macroOpposesPosition ? 'high' : alignedDriverCount >= 4 ? 'medium' : 'low';

    const closingGuidance = {
        macro_bias: trendBias,
        price_vs_breakeven_pct: priceVsBreakevenPct,
        hold_minutes: clampNumber(position_context?.hold_minutes ?? null, 1),
        macro_supports_position: macroSupportsPosition,
        macro_opposes_position: macroOpposesPosition,
        closing_alert: closingAlert,
        reversal_opportunity: reversalOpportunity,
        reverse_confidence: reverseConfidence,
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

    // Extension thresholds: single source of truth. Referenced in the soft-judgment
    // guidance below so the prose can never drift from the numbers we actually use.
    const extensionMicroAvoid = strictPolicy ? 2.5 : 2.8;
    const extensionMicroNoEntry = strictPolicy ? 3 : 3.3;
    const extensionPrimaryAvoid = strictPolicy ? 2.5 : 2.8;

    const modeLabel = dryRun ? 'simulation' : 'live';
    const baseSymbol = symbol.replace(/USDT$/i, '');
    const assetClass = String(category || '').toLowerCase() || 'unknown';
    // On Capital, leverage is fixed by the broker per asset class — the model does
    // not pick it. Only crypto (Bitget) takes a model-chosen 1–5 leverage.
    const isCapital = String(platform || '').toLowerCase() === 'capital';
    const leverageOwnershipNote = isCapital
        ? 'Use it to set selectivity.'
        : 'Use it to set selectivity and leverage.';
    const leverageGuidance = isCapital
        ? 'Leverage: do NOT set it — on this venue leverage is broker-defined per asset class, not chosen here. Always output leverage=null.'
        : `Leverage 1–5 by conviction AND risk: cut to 1–2 even on HIGH conviction when extended or near major ${contextTimeframe} levels. null on HOLD/CLOSE.`;
    const leverageTask = isCapital
        ? 'do NOT output a leverage field — leverage is broker-defined per asset class on this venue.'
        : 'leverage 1–5 for BUY/SELL/REVERSE, else null.';
    // Capital: omit the leverage key entirely (no comma). Bitget: include it.
    const leverageJsonField = isCapital ? '' : ',"leverage":null|1|2|3|4|5';

    // signal_strength is OWNED BY CODE (computeSignalStrength). We compute it once here
    // and hand it to the model as a given input — the model must NOT recompute it, and
    // postprocessDecision gates on this same value.
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
    const state = {
        signal_strength: signalStrength, // code-owned; given, not for you to recompute
        favored_side: favoredSide,
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
            long: momentumSignals.longMomentum,
            short: momentumSignals.shortMomentum,
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
        drivers: {
            aligned_count: alignedDriverCount,
            long: longAlignedDriverCount,
            short: shortAlignedDriverCount,
            regime_alignment: clampNumber(regimeAlignment, 2),
            medium_action_ready: mediumActionReady,
        },
        location: {
            into_context_support: intoContextSupport,
            into_context_resistance: intoContextResistance,
            context_support_dist_atr: clampNumber(htfSupportDist ?? null, 3),
            context_resistance_dist_atr: clampNumber(htfResistanceDist ?? null, 3),
            context_breakout_confirmed: htfBreakoutConfirmed,
            context_breakdown_confirmed: htfBreakdownConfirmed,
            near_primary_support: nearPrimarySupport,
            near_primary_resistance: nearPrimaryResistance,
            confluence_score: clampNumber(locationConfluenceScore, 3),
            chop_risk: chopRisk,
        },
        levels: {
            primary: { support: srLevel(primarySR?.support), resistance: srLevel(primarySR?.resistance) },
            context: { support: srLevel(contextSR?.support), resistance: srLevel(contextSR?.resistance) },
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
            : { open: false, status: position_status },
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
            ? recentActions
                  .slice(-1 * actionsToShow)
                  .map((a) => ({ action: a.action, ts: new Date(a.timestamp).toISOString() }))
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
        : 'unless signal_strength=HIGH and the matching primary breakout/breakdown is confirmed';
    const microEntryException = strictPolicy
        ? 'unless signal_strength=HIGH with a confirmed primary breakout/retest'
        : 'unless signal_strength=HIGH with a confirmed primary breakout/retest, or (balanced policy) strength≥MEDIUM with drivers.aligned_count≥4';
    const antiFlipWindow = strictPolicy ? 'the last 2 calls' : 'the previous call';
    const antiFlipStrength = strictPolicy ? 'HIGH' : 'at least MEDIUM';

    const sys = `
You are an expert swing-trading market-structure analyst. Decide one action and size it.

${assetNote}

TIMEFRAMES (fixed)
- micro=${microTimeframe} (entry timing/confirmation), primary=${primaryTimeframe} (setup+execution), macro=${macroTimeframe} (regime bias), context=${contextTimeframe} (HTF location + major levels, risk lever).
Strategy: ${primaryTimeframe} swing setups with ${microTimeframe} confirmation, aligned with (or tactically fading) the ${macroTimeframe} regime while respecting ${contextTimeframe} location. Holding horizon ~1–10 days. Prefer fewer, higher-quality trades; avoid churn.

INPUTS
- You receive two JSON objects: STATE (derived signals — your single source of truth) and MARKET (raw price/tape/news). All keys are pre-computed; do not invent fields.
- micro_bias precedence (already applied in state.biases.micro): structure (breakout-retest → break-state → BOS → structure-state) first, momentum (EMA slope+RSI+price vs EMA20) as fallback; structure wins ties.

DECISION OWNERSHIP
- state.signal_strength (LOW/MEDIUM/HIGH) is computed by the system and is GROUND TRUTH. Do NOT recompute, rescale, or argue with it. ${leverageOwnershipNote}
- The HARD constraints below are enforced in code AFTER you respond. Do not spend reasoning re-deriving them — if you violate one your action is silently coerced (a wasted call). Just stay inside them:
  1. Allowed actions: flat → BUY/SELL/HOLD; in a position → HOLD/CLOSE/REVERSE only.
  2. Trend guard: no counter-trend entry/flip against an aligned primary+micro trend (${trendGuardException}).
  3. Entry timing: when flat and momentum.micro_entry_ok=false, entries are blocked (${microEntryException}).
  4. Anti-flip: a repeated CLOSE/REVERSE within ${antiFlipWindow} is blocked unless strength is ${antiFlipStrength}.
  5. Base gates: if any of state.gates.{spread_ok,liquidity_ok,atr_ok,slippage_ok} is false → entries forced to HOLD and risk-off forced while in a position.

YOUR JOB (soft judgment — where your reasoning actually matters)
- Pick the highest-quality action consistent with STATE, then size it. Structure (BOS/CHoCH/breakout-retest) outweighs raw momentum.
- Location vs regime: prefer entries aligned with macro+context. Counter-regime only at extreme location with clean invalidation. Do NOT open into a near opposite level (location.near_primary_support/resistance, or context distance < 0.6 ATR) unless the matching breakout/breakdown is confirmed. If both nearest levels are close (location.chop_risk), treat as chop and avoid fresh entries unless strength=HIGH with clean level logic.
- Extension (risk control, not a signal): |state.extension_atr.micro| ≥ ${extensionMicroAvoid} or |state.extension_atr.primary| ≥ ${extensionPrimaryAvoid} → avoid fresh entries; micro > ${extensionMicroNoEntry} → strongly prefer none. RSI extremes are NOT a counter-trend trigger by themselves — only "permission" once structure shows damage/flip.
- Cost/churn: round-trip cost ≈ ${total_cost_bps} bps. If the expected swing is not clearly larger than cost, or the setup is unclear/MED-LOW quality, prefer HOLD.
- In a position: prefer HOLD when regime supports it and there is no strong opposite structure (especially |unrealized_pnl_pct| < 0.25%). Trim 30–70% (exit_size_pct) on gains into a major opposite level, weakening regime, or exhausted volatility expansion. REVERSE = full close then open opposite (exit_size_pct=100, no partials) and only on a confirmed primary structure flip with state.closing_guardrails.reverse_confidence=high.
- ${leverageGuidance}
- Position truthfulness: never describe a position as winning when unrealized_pnl_pct < 0 or price_vs_breakeven_pct is on the losing side.

OUTPUT
- Strict JSON only, parseable by JSON.parse — no markdown, comments, trailing commas, or extra keys.
- Decision policy mode: ${decisionPolicyLabel}.
`.trim();

    const user = `
You are analyzing ${baseSymbol} for swing trading (mode=${modeLabel}, asset_class=${assetClass}).
Timeframes: micro=${microTimeframe}, primary=${primaryTimeframe}, macro=${macroTimeframe}, context=${contextTimeframe}. Called ~once per ${microTimeframe}. Decision policy: ${decisionPolicyLabel}.
S/R levels are swing-pivot derived per timeframe (~150 bars); distances are in that timeframe's ATR; level state ∈ {at_level, approaching, rejected, broken, retesting}.

STATE (derived signals — single source of truth):
${JSON.stringify(state)}

MARKET (raw inputs):
${JSON.stringify(market)}

TASKS:
1) Output exactly one allowed action (see DECISION OWNERSHIP): flat → BUY/SELL/HOLD; in a position → HOLD/CLOSE/REVERSE.
2) ${leverageTask}
3) exit_size_pct for CLOSE/REVERSE (100 = full close, 30–70 = trim), else null.
4) summary ≤3 lines; reason = brief rationale.

Respond with strict JSON only:
{"action":"BUY|SELL|HOLD|CLOSE|REVERSE","summary":"≤2 lines","reason":"brief rationale","exit_size_pct":null|0-100${leverageJsonField}}
`;

    const context = {
        // Exposed so the caller can gate the AI call on the code-owned conviction
        // before spending it (flat + sub-MEDIUM → no AI call). Same value the
        // model receives and postprocessDecision gates on.
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

    return { system: sys, user, context };
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
    recentActions: { action: string; timestamp: number }[];
    positionContext: PositionContext | null;
    policy?: DecisionPolicy;
}) {
    const { decision, context, gates, positionOpen, recentActions, positionContext, policy } = params;
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

    return {
        ...decision,
        action,
        leverage,
        exit_size_pct,
        signal_strength: signalStrength,
        micro_bias: microBias,
        primary_bias: primaryBias,
        macro_bias: macroBias,
        context_bias: contextBias,
    };
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
        required: ['action', 'summary', 'reason', 'exit_size_pct', 'leverage'],
        properties: {
            action: { type: 'string', enum: ['BUY', 'SELL', 'HOLD', 'CLOSE', 'REVERSE'] },
            summary: { type: 'string' },
            reason: { type: 'string' },
            exit_size_pct: { type: ['number', 'null'], minimum: 0, maximum: 100 },
            leverage: { type: ['integer', 'null'], minimum: 1, maximum: 5 },
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
        required: ['action', 'summary', 'reason', 'exit_size_pct'],
        properties: {
            action: { type: 'string', enum: ['BUY', 'SELL', 'HOLD', 'CLOSE', 'REVERSE'] },
            summary: { type: 'string' },
            reason: { type: 'string' },
            exit_size_pct: { type: ['number', 'null'], minimum: 0, maximum: 100 },
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
