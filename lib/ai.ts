// lib/ai.ts

import { AI_BASE_URL, AI_MODEL, TRADE_WINDOW_MINUTES } from './constants';
import type { MultiTFIndicators } from './indicators';
import { setEvaluation, getEvaluation } from './utils';

export type PositionContext = {
    side: 'long' | 'short';
    entry_price?: number;
    entry_ts?: string;
    hold_minutes?: number;
    unrealized_pnl_pct?: number;
    breakeven_price?: number;
    taker_fee_rate?: number;
    flow?: {
        cvd?: number;
        ob_imbalance?: number;
        pressure_delta?: number;
        alignment?: 'bullish' | 'bearish' | 'neutral';
        against_position?: boolean;
    };
};

type FlowSupport = 'buy' | 'sell' | 'neutral';

export type MomentumSignals = {
    macroTrendUp: boolean;
    macroTrendDown: boolean;
    longMomentum: boolean;
    shortMomentum: boolean;
    flowSupports: FlowSupport;
    flowBias: number;
    nearPrimaryEMA20: boolean;
    nearMicroEMA20: boolean;
    longFlowOk: boolean;
    shortFlowOk: boolean;
    entryReadyLong: boolean;
    entryReadyShort: boolean;
    primaryRSI?: number | null;
    primarySlope?: number | null;
    microRSI?: number | null;
    primaryAtr?: number | null;
    microExtensionInAtr?: number | null;
    info?: Record<string, any>;
};

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
    analytics: any;
    indicators: MultiTFIndicators;
    gates: { regime_trend_up: boolean; regime_trend_down: boolean };
    primaryTimeframe: string;
}): MomentumSignals {
    const { price, analytics, indicators, gates, primaryTimeframe } = params;
    const macroSummary = indicators.macro || '';
    const microSummary = indicators.micro || '';
    const primarySummary = indicators.primary?.summary || '';

    const ema50Macro = readIndicator('EMA50', macroSummary);
    const ema50Primary = readIndicator('EMA50', primarySummary);
    const ema20Primary = readIndicator('EMA20', primarySummary);
    const ema20Micro = readIndicator('EMA20', microSummary);
    const atrPrimary = readIndicator('ATR', primarySummary);
    const atrMicro = readIndicator('ATR', microSummary);
    const rsiPrimary = readIndicator('RSI', primarySummary);
    const rsiMicro = readIndicator('RSI', microSummary);
    const slopePrimary = readIndicator('slopeEMA21_10', primarySummary);

    const obImb = Number(analytics?.obImb ?? 0);
    const cvd = Number(analytics?.cvd ?? 0);
    const cvdStrength = Math.tanh(cvd / 50);
    const flowBiasRaw = (cvdStrength + obImb) / 2;
    const flowSupports: FlowSupport = flowBiasRaw > 0.15 ? 'buy' : flowBiasRaw < -0.15 ? 'sell' : 'neutral';
    const longFlowOk = flowSupports !== 'sell';
    const shortFlowOk = flowSupports !== 'buy';

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

    const longMomentum = macroTrendUp && priceAbovePrimary50 && rsiPullbackLong && slopeUp && longFlowOk;
    const shortMomentum = macroTrendDown && priceBelowPrimary50 && rsiPullbackShort && slopeDown && shortFlowOk;

    const entryReadyLong = longMomentum && microEntryOk && longFlowOk;
    const entryReadyShort = shortMomentum && microEntryOk && shortFlowOk;

    const microExtensionInAtr =
        Number.isFinite(atrMicro as number) && (atrMicro as number) > 0 && Number.isFinite(ema20Micro as number)
            ? (price - (ema20Micro as number)) / (atrMicro as number)
            : null;

    return {
        macroTrendUp,
        macroTrendDown,
        longMomentum,
        shortMomentum,
        flowSupports,
        flowBias: flowBiasRaw,
        nearPrimaryEMA20: nearPrimary,
        nearMicroEMA20: nearMicro,
        longFlowOk,
        shortFlowOk,
        entryReadyLong,
        entryReadyShort,
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

export function buildPrompt(
    symbol: string,
    timeframe: string,
    bundle: any,
    analytics: any,
    position_status: string = 'none',
    news_sentiment: string | null = null,
    news_headlines: string[] = [],
    indicators: MultiTFIndicators,
    gates: any, // <--- Retain the gates object for the base gate checks
    position_context: PositionContext | null = null,
    momentumSignalsOverride?: MomentumSignals,
    recentActions: { action: string; timestamp: number }[] = [],
    realizedRoiPct?: number | null,
    dryRun?: boolean,
) {
    const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
    const price = Number(t?.lastPr ?? t?.last ?? t?.close ?? t?.price);
    const change = Number(t?.change24h ?? t?.changeUtc24h ?? t?.chgPct);
    const last = price; // Use price as 'last'
    const primaryTimeframe = indicators.primary?.timeframe ?? timeframe;
    const momentumSignals =
        momentumSignalsOverride ??
        computeMomentumSignals({
            price: last,
            analytics,
            indicators,
            gates,
            primaryTimeframe,
        });

    const market_data = `price=${price}, change24h=${Number.isFinite(change) ? change : 'n/a'}`;

    const order_flow = `buys=${analytics.buys.toFixed(3)}, sells=${analytics.sells.toFixed(
        3,
    )}, CVD=${analytics.cvd.toFixed(3)} (obImb=${analytics.obImb.toFixed(2)})`;

    const liquidity_data = `top bid walls: ${JSON.stringify(analytics.topWalls.bid)}, top ask walls: ${JSON.stringify(
        analytics.topWalls.ask,
    )}`;

    const derivatives = `funding=${bundle.funding?.[0]?.fundingRate ?? 'n/a'}, openInterest=${
        bundle.oi?.openInterestList?.[0]?.size ?? bundle.oi?.openInterestList?.[0]?.openInterest ?? 'n/a'
    }`;

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
    const priceTrendSeries = JSON.stringify(priceTrendPoints);

    const normalizedNewsSentiment =
        typeof news_sentiment === 'string' && news_sentiment.length > 0 ? news_sentiment : null;
    const newsSentimentBlock = normalizedNewsSentiment
        ? `- News sentiment ONLY: ${normalizedNewsSentiment.toLowerCase()}\n`
        : '';
    const normalizedHeadlines = Array.isArray(news_headlines) ? news_headlines.filter((h) => !!h).slice(0, 5) : [];
    const newsHeadlinesBlock = normalizedHeadlines.length
        ? `- Latest ${normalizedHeadlines.length} News headlines: ${normalizedHeadlines.join(' | ')}\n`
        : '';

    const recentActionsExists = Array.isArray(recentActions) && recentActions.length > 0;
    const actionsToShow = recentActionsExists ? Math.min(recentActions.length, 5) : 5;
    const recentActionsBlock = recentActionsExists
        ? `- Recent actions (last ${actionsToShow}): ${recentActions
              .slice(-1 * actionsToShow)
              .map((a) => `${a.action}@${new Date(a.timestamp).toISOString()}`)
              .join(' | ')}\n`
        : '';
    const positionContextBlock = position_context
        ? `- Position context (JSON): ${JSON.stringify(position_context)}\n`
        : '';
    const primaryIndicatorsBlock = indicators.primary
        ? `- Primary timeframe (${indicators.primary.timeframe}) indicators: ${indicators.primary.summary}\n`
        : '';
    const contextTimeframe = indicators.context?.timeframe ?? indicators.contextTimeFrame ?? 'context';
    const sr = indicators.sr || {};
    const primarySR = sr[primaryTimeframe] ?? sr[indicators.primary?.timeframe || primaryTimeframe];
    const contextSR = sr[contextTimeframe] ?? sr[indicators.context?.timeframe || contextTimeframe];

    const contextSummary = indicators.context?.summary ?? '';
    const contextBias = contextSummary.includes('trend=up')
        ? 'UP'
        : contextSummary.includes('trend=down')
        ? 'DOWN'
        : 'NEUTRAL';
    const contextIndicatorsBlock =
        contextSummary && contextTimeframe
            ? `- Context timeframe (${contextTimeframe}) indicators: ${contextSummary}\n`
            : '';
    const formatLevel = (lvl: any, kind: 'support' | 'resistance') =>
        lvl
            ? `${kind}_price=${lvl.price}, dist_in_atr=${lvl.dist_in_atr}, strength=${lvl.level_strength}, type=${lvl.level_type}, state=${lvl.level_state}`
            : `${kind}=n/a`;
    const contextSRBlock = contextSR
        ? `- Context S/R (${contextTimeframe}): ${formatLevel(contextSR.support, 'support')} | ${formatLevel(
              contextSR.resistance,
              'resistance',
          )}\n`
        : '';
    const primarySRBlock =
        primarySR && primaryTimeframe
            ? `- Primary S/R (${primaryTimeframe}): ${formatLevel(primarySR.support, 'support')} | ${formatLevel(
                  primarySR.resistance,
                  'resistance',
              )}\n`
            : '';

    const vol_profile_str = (analytics.volume_profile || [])
        .slice(0, 10)
        .map((v: any) => `(${v.price.toFixed(2)} → ${v.volume})`)
        .join(', ');

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
    if (Number.isFinite(htfSupportDist as number) || Number.isFinite(htfResistanceDist as number)) {
        const supportDist = Number.isFinite(htfSupportDist as number) ? (htfSupportDist as number) : Infinity;
        const resistanceDist = Number.isFinite(htfResistanceDist as number) ? (htfResistanceDist as number) : Infinity;
        if (supportDist <= resistanceDist) {
            intoContextSupport = supportDist < 0.6;
        } else {
            intoContextResistance = resistanceDist < 0.6;
        }
    }
    const htfBreakdownConfirmed = contextSR?.support?.level_state === 'broken';
    const htfBreakoutConfirmed = contextSR?.resistance?.level_state === 'broken';

    // --- KEY METRICS (VALUES, NOT JUDGMENTS) ---
    const spread_bps = last > 0 ? ((analytics.spread || 0) / last) * 1e4 : 999;
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

    const rsiMicroDisplay = Number.isFinite(rsi_micro as number) ? (rsi_micro as number).toFixed(1) : 'n/a';
    const rsiMacroDisplay = Number.isFinite(rsi_macro as number) ? (rsi_macro as number).toFixed(1) : 'n/a';
    const rsiPrimaryDisplay = Number.isFinite(rsi_primary as number) ? (rsi_primary as number).toFixed(1) : 'n/a';
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

    const key_metrics =
        `spread_bps=${spread_bps.toFixed(2)}, book_imbalance=${analytics.obImb.toFixed(2)}, ` +
        `atr_pct_${indicators.macroTimeFrame}=${atr_pct_macro.toFixed(2)}%, rsi_${
            indicators.microTimeFrame
        }=${rsiMicroDisplay}, ` +
        `rsi_${indicators.macroTimeFrame}=${rsiMacroDisplay}, micro_slope_pct_per_bar=${slope21_micro.toFixed(4)}, ` +
        `dist_from_ema20_${indicators.microTimeFrame}_in_atr=${distance_from_ema_atr.toFixed(2)}, ` +
        `atr_pct_${primaryTimeframe}=${atr_pct_primary.toFixed(2)}%, rsi_${primaryTimeframe}=${rsiPrimaryDisplay}, ` +
        `primary_slope_pct_per_bar=${slope21_primary.toFixed(
            4,
        )}, dist_from_ema20_${primaryTimeframe}_in_atr=${distance_from_ema20_primary_atr.toFixed(2)}`;

    // --- SIGNAL STRENGTH DRIVERS & CLOSING GUIDANCE ---
    const clampNumber = (value: number | null | undefined, digits = 3) =>
        Number.isFinite(value as number) ? Number((value as number).toFixed(digits)) : null;
    const trendBias = gates.regime_trend_up ? 1 : gates.regime_trend_down ? -1 : 0;
    const cvdStrength = clampNumber(Math.tanh(analytics.cvd / 50));
    const oversoldMicro = typeof rsi_micro === 'number' && rsi_micro < 35;
    const overboughtMicro = typeof rsi_micro === 'number' && rsi_micro > 65;
    const reversalOpportunity =
        oversoldMicro && (cvdStrength ?? 0) < -0.4
            ? 'oversold_with_sell_pressure'
            : overboughtMicro && (cvdStrength ?? 0) > 0.4
            ? 'overbought_with_buy_pressure'
            : null;

    const contextBiasDriver = contextBias === 'UP' ? 0.6 : contextBias === 'DOWN' ? -0.6 : 0;
    const supportProximity = typeof htfSupportDist === 'number' ? Math.max(0, 1 - Math.min(htfSupportDist, 2) / 2) : 0;
    const resistanceProximity =
        typeof htfResistanceDist === 'number' ? Math.max(0, 1 - Math.min(htfResistanceDist, 2) / 2) : 0;
    const locationConfluenceScore = Math.min(
        1,
        Math.max(supportProximity, resistanceProximity) + (htfBreakoutConfirmed || htfBreakdownConfirmed ? 0.3 : 0),
    );

    const driverComponents = [
        trendBias,
        cvdStrength ?? 0,
        analytics.obImb ?? 0,
        slope21_micro,
        distance_from_ema_atr,
        slope21_primary,
        distance_from_ema20_primary_atr,
        contextBiasDriver,
    ];
    const longAlignedDriverCount = driverComponents.filter((v) => v >= 0.35).length;
    const shortAlignedDriverCount = driverComponents.filter((v) => v <= -0.35).length;
    const alignedDriverCount = Math.max(longAlignedDriverCount, shortAlignedDriverCount);

    const flowBiasRaw = momentumSignals.flowBias ?? ((cvdStrength ?? 0) + (analytics.obImb ?? 0)) / 2;
    const flowBias = clampNumber(flowBiasRaw, 3);
    const flowSupports =
        momentumSignals.flowSupports ?? (flowBiasRaw > 0.25 ? 'buy' : flowBiasRaw < -0.25 ? 'sell' : 'neutral');

    const mediumActionReady =
        alignedDriverCount >= 3 &&
        Math.abs(flowBiasRaw) >= 0.25 &&
        (momentumSignals.longMomentum || momentumSignals.shortMomentum);

    const signalDrivers = {
        trend_bias: trendBias,
        cvd_strength: cvdStrength,
        orderbook_pressure: clampNumber(analytics.obImb, 3),
        momentum_slope_pct_per_bar: clampNumber(slope21_micro, 4),
        extension_atr: clampNumber(distance_from_ema_atr, 3),
        atr_pct_macro: clampNumber(atr_pct_macro, 3),
        rsi_micro,
        rsi_macro,
        rsi_primary,
        oversold_rsi_micro: oversoldMicro,
        overbought_rsi_micro: overboughtMicro,
        aligned_driver_count: alignedDriverCount,
        aligned_driver_count_long: longAlignedDriverCount,
        aligned_driver_count_short: shortAlignedDriverCount,
        flow_bias: flowBias,
        flow_supports: flowSupports,
        medium_action_ready: mediumActionReady,
        macro_trend_up: momentumSignals.macroTrendUp,
        macro_trend_down: momentumSignals.macroTrendDown,
        long_momentum: momentumSignals.longMomentum,
        short_momentum: momentumSignals.shortMomentum,
        entry_ready_long: momentumSignals.entryReadyLong,
        entry_ready_short: momentumSignals.entryReadyShort,
        micro_extension_atr: clampNumber(momentumSignals.microExtensionInAtr ?? null, 3),
        atr_pct_primary: clampNumber(atr_pct_primary, 3),
        primary_extension_atr: clampNumber(distance_from_ema20_primary_atr, 3),
        primary_slope_pct_per_bar: clampNumber(slope21_primary, 4),
        context_bias: contextBias,
        context_bias_driver: clampNumber(contextBiasDriver, 3),
        location_confluence_score: clampNumber(locationConfluenceScore, 3),
        into_context_support: intoContextSupport,
        into_context_resistance: intoContextResistance,
        context_breakdown_confirmed: htfBreakdownConfirmed,
        context_breakout_confirmed: htfBreakoutConfirmed,
        context_support_dist_atr: clampNumber(htfSupportDist ?? null, 3),
        context_resistance_dist_atr: clampNumber(htfResistanceDist ?? null, 3),
    };
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
    const flowAgainstPosition =
        positionSide === 'long'
            ? analytics.obImb < -0.15 || (cvdStrength ?? 0) < -0.35
            : positionSide === 'short'
            ? analytics.obImb > 0.15 || (cvdStrength ?? 0) > 0.35
            : null;
    const closingAlert = Boolean(
        flowAgainstPosition &&
            (Math.abs(cvdStrength ?? 0) > 0.4 || Math.abs(analytics.obImb) > 0.2 || (priceVsBreakevenPct ?? 0) < -0.15),
    );

    const flowContradictionScore =
        positionSide === 'long'
            ? Math.max(0, -(analytics.obImb ?? 0) + -(cvdStrength ?? 0))
            : positionSide === 'short'
            ? Math.max(0, (analytics.obImb ?? 0) + (cvdStrength ?? 0))
            : 0;

    const reverseConfidence =
        flowContradictionScore >= 0.8
            ? 'high'
            : flowContradictionScore >= 0.5
            ? reversalOpportunity
                ? 'high'
                : 'medium'
            : 'low';

    const closingGuidance = {
        macro_bias: trendBias,
        flow_pressure: clampNumber(analytics.obImb, 3),
        cvd_strength: cvdStrength,
        price_vs_breakeven_pct: priceVsBreakevenPct,
        hold_minutes: clampNumber(position_context?.hold_minutes ?? null, 1),
        macro_supports_position: macroSupportsPosition,
        macro_opposes_position: macroOpposesPosition,
        flow_against_position: flowAgainstPosition,
        closing_alert: closingAlert,
        reversal_opportunity: reversalOpportunity,
        flow_contradiction_score: clampNumber(flowContradictionScore, 3),
        reverse_confidence: reverseConfidence,
    };
    // Costs (educate the model)
    const taker_fee_rate_side = Number.isFinite(position_context?.taker_fee_rate as number)
        ? Math.max(0, Number(position_context?.taker_fee_rate))
        : 0.00025; // default 2.5 bps per side
    const taker_round_trip_bps = Number((taker_fee_rate_side * 2 * 10000).toFixed(2));
    const slippage_bps = 2;
    const total_cost_bps = Number((taker_round_trip_bps + slippage_bps).toFixed(1));

    const risk_policy =
        `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, ` +
        `stop=1.5xATR(${indicators.macroTimeFrame}), take_profit=2.5xATR(${indicators.macroTimeFrame}), ` +
        `time_stop=${parseInt(timeframe, 10) * 3} minutes (execution-layer timer)`;

    // We only pass the BASE gates now, as the AI will judge the strategy gates using metrics
    const base_gating_flags = `spread_ok=${gates.spread_ok}, liquidity_ok=${gates.liquidity_ok}, atr_ok=${gates.atr_ok}, slippage_ok=${gates.slippage_ok}`;

    // We still pass the Regime for a strong trend bias signal
    const regime_flags = `regime_trend_up=${gates.regime_trend_up}, regime_trend_down=${gates.regime_trend_down}`;

    const realizedRoiLine =
        realizedRoiPct !== undefined && realizedRoiPct !== null && Number.isFinite(realizedRoiPct)
            ? `- Recent realized PnL (last closed): ${Number(realizedRoiPct).toFixed(2)}%`
            : '';

    const sys = `
You are an expert crypto market microstructure analyst and short-term trading assistant.
Primary strategy: ${primaryTimeframe} momentum within a ${indicators.macroTimeFrame} structure with a ${indicators.microTimeFrame} confirmation — take trades when there is a clear short-term directional edge from tape/orderbook and recent price action. Macro (${indicators.macroTimeFrame}) trend is a bias, not a hard filter. I will call you roughly once per ${primaryTimeframe}.
Bias definitions (explicit): micro_bias = flow/tape + recent price on ${indicators.microTimeFrame}; primary_bias = slope/RSI/EMA20 alignment on ${primaryTimeframe}; macro_bias = regime_trend_up/down on ${indicators.macroTimeFrame}; context_bias = trend/regime on ${contextTimeframe} (risk lever).
Decision ladder: Base gates → biases (macro/primary) → signal drivers + entry_ready → action.
Signal strength is driven by aligned_driver_count + flow_bias + extensions (micro + primary); strong flow with many aligned drivers → HIGH.
Respond in strict JSON ONLY.
Output must be valid JSON parseable by JSON.parse with no trailing commas or extra keys; no markdown or commentary. Keep keys minimal—no extra fields.

GENERAL RULES
- **Base gates**: when flat, if ANY of spread_ok, liquidity_ok, atr_ok, slippage_ok is false → action="HOLD"; if in a position, do not block exits—prefer risk-off (CLOSE) rather than HOLD when gates fail. Exits (CLOSE/REVERSE) must never be blocked by base gates.
- **Costs**: use taker_fee_rate_side from context; total_cost_bps = fees (round-trip) + slippage = ~${total_cost_bps}bps. Use total_cost_bps as the single cost anchor (do not mix conflicting heuristics). If expected edge is small vs total_cost_bps → HOLD; avoid churn around breakeven.
- **Macro bias**: trades WITH macro trend are preferred and can be taken on MEDIUM or HIGH signals. Trades AGAINST macro require HIGH signal_strength or very strong flow/tape.
- **Macro neutral**: when macro_bias is neutral (regime_trend_up=false and regime_trend_down=false), treat macro_bias=NEUTRAL. Be more selective but do not block trades solely due to neutrality; lean on micro/context and strong flow.
- **Context bias (${contextTimeframe})**: context_bias=${contextBias}. Treat it as a risk lever, not a gate. When aligned with the intended direction, you may allow MEDIUM signals more readily (especially near good entries). When against, require HIGH signal_strength plus strong flow/tape and a better entry/extension (or smaller size if sizing elsewhere). Use it to upgrade/downgrade signal_strength or selectivity near levels, not to block trades outright.
- **Support/Resistance (machine-usable)**: Derived from swing pivots; distances are expressed in ATRs of that timeframe. location_confluence_score is capped (max +1 driver) to avoid double-counting trend+location. Treat into_context_support/resistance and context_breakdown/breakout flags as risk modifiers: avoid selling into strong support unless breakdown is confirmed or flow is very strong; be faster to take profit when at strong opposite levels.
- **S/R dual at_level**: if both support and resistance are at_level, treat as chop—avoid new entries unless signal_strength is HIGH with strong flow and favorable risk; favor HOLD or risk-off if extended.
- **Signal usage**:
  - Treat aligned_driver_count ≥ 4 as "strong micro structure" (or ≥ 3.8 with strong flow_supports not opposite).
  - If signal_strength = HIGH and flow_supports is "buy" or "sell", you should normally choose that direction when flat (provided base gates true).
  - If signal_strength = MEDIUM and aligned_driver_count ≥ 4 and flow_supports is not opposite, you may trade, but be selective near extremes.
  - Only take new entries when entry_ready_long/short = true. Exception: allow HIGH signal_strength with aligned_driver_count ≥ 5 to proceed even if entry_ready_long/short is false (to avoid conflicting rules); document the exception in reasoning.
- **Sign conventions**: price_vs_breakeven_pct > 0 means price is above breakeven; < 0 means below. For shorts, price_vs_breakeven_pct < 0 = losing (price above breakeven), > 0 = winning (price below breakeven). For longs, < 0 = losing, > 0 = winning.
- **Temporal inertia**: avoid more than one action change (CLOSE/REVERSE) in the same direction within the last 2 calls unless signal_strength stays HIGH and flow_contradiction_score is increasing.
- **Exit sizing**: Default exit_size_pct = 100 (full close). For CLOSE/REVERSE you may set exit_size_pct to 25–75 to trim risk instead of flattening when conditions weaken but are not fully opposite; keep it null/omit when not needed. Use round numbers (25/50/75) and avoid overusing partials.

ACTIONS LOGIC
- **No position open**:
  - If base gates true AND signal_strength = HIGH AND (entry_ready_long/short = true OR aligned_driver_count ≥ 5):
      - action="BUY" when flow_supports="buy".
      - action="SELL" when flow_supports="sell".
  - If signal_strength = MEDIUM AND aligned_driver_count ≥ 4 (or very close with strong flow):
      - Prefer BUY/SELL in direction of flow_supports OR macro trend if flow is neutral.
      - If context_bias opposes the intended direction, only take MEDIUM when aligned_driver_count ≥ 5 with strong flow and price not extremely extended; if context_bias aligns, MEDIUM with aligned_driver_count ≥ 4 is acceptable when entry_ready is true.
  - Avoid opening new shorts when dist_to_support_in_atr_${contextTimeframe} < 0.6 unless flow is strongly bearish AND context_breakdown_confirmed=true; similarly avoid new longs when dist_to_resistance_in_atr_${contextTimeframe} < 0.6 unless breakout_confirmed=true and flow clearly supports.
  - If macro_bias = DOWN and you want to open long from flat, require: flow_supports="buy", aligned_driver_count ≥ 5, cvd_strength strongly positive, dist_from_ema20_${indicators.microTimeFrame}_in_atr < 1.5, and signal_strength = HIGH.
  - Use action="HOLD" when signal_strength = LOW, when macro and micro signals clearly conflict, or when price is extremely extended (|dist_from_ema20_${indicators.microTimeFrame}_in_atr| > 2.5) and flow is weak/fading.
- **Position open**:
  - If signal turns clearly opposite with HIGH strength → action="CLOSE" or "REVERSE" (subject to reversal guards below).
  - Use partial exits when trimming risk is better than flattening: set exit_size_pct to 30-70 when signal deteriorates but not fully opposite, when nearing strong level with decent unrealized gains, or when flow flips but macro/context still align. Default to full (100) if not set. Avoid tiny trims (<20%) unless explicitly justified.
  - Prefer HOLD if macro_supports_position=true and no strong opposite flow.
  - Prefer HOLD over CLOSE when |unrealized_pnl_pct| < 0.25% and there is no HIGH opposite signal.
  - Ignore MEDIUM opposite signals if |price_vs_breakeven_pct| < 0.2% and |dist_from_ema20_${indicators.microTimeFrame}_in_atr| ≤ 2.5 unless signal_strength = HIGH.
  - If short and into strong ${contextTimeframe} support (dist_to_support_in_atr < 0.6 or at_level) and flow weakens/flips, bias toward CLOSE over HOLD; mirror for longs into strong resistance.
  - If unrealized_pnl_pct is small and signal deteriorates to LOW → "CLOSE".
  - When |dist_from_ema20_${indicators.microTimeFrame}_in_atr| > 2.5 and macro_bias = DOWN: prefer "CLOSE" (take profit) on shorts when flow flips bullish; only consider "REVERSE" if losing (price_vs_breakeven_pct < 0) and flow_contradiction_score ≥ 1.0.
  - When losing (price_vs_breakeven_pct < 0) and reverse_confidence = "medium" with flow_contradiction_score ≥ 0.6, be more aggressive to exit: prioritize "CLOSE"; consider "REVERSE" only if flow_supports clearly opposite and aligned_driver_count ≥ 5.

REVERSAL DISCIPLINE
- REVERSE only if ALL are true: "Closing guardrails".reverse_confidence = "high", flow_contradiction_score ≥ 0.8, aligned_driver_count ≥ 5, signal_strength = HIGH. If reverse_confidence = "medium" and price_vs_breakeven_pct < 0, only consider REVERSE when flow_contradiction_score ≥ 0.6 and flow_supports is clearly opposite; otherwise CLOSE.
- Do NOT REVERSE if unrealized_pnl_pct < -0.5% and we are not near stop and no major regime change, or if reverse_confidence != "high" (except the losing/medium case above).
- REVERSE is close + open opposite; if conditions are not met, prefer HOLD or CLOSE.
- Prefer HOLD over CLOSE when |unrealized_pnl_pct| < 0.25% and no HIGH opposite signal.

EXTENSION / OVERBOUGHT-OVERSOLD
- Extension buckets: |dist_from_ema20_${indicators.microTimeFrame}_in_atr| in [2,2.5) → need strong flow/tape to enter; ≥ 2.5 → prefer CLOSE/avoid new entries unless losing with strong contradiction; > 3 → avoid new entries and favor flattening over flipping.
- If 'dist_from_ema20_${indicators.microTimeFrame}_in_atr' > 2 or < -2, require stronger confirmation for new entries; never ignore strong tape/flow cues solely because price looks extended.
- If |dist_from_ema20_${indicators.microTimeFrame}_in_atr| > 2.5 and macro_bias = DOWN, avoid flipping long unless losing and flow_contradiction_score ≥ 1.0.
- Use higher-timeframe stretch: when |dist_from_ema20_${primaryTimeframe}_in_atr| ≥ 2, be more selective on new entries and tighten profit-taking; when ≥ 2.5, avoid fresh entries unless losing with strong contradiction and flow clearly supports the turn.
- If |dist_from_ema20_${indicators.microTimeFrame}_in_atr| > 3 OR |dist_from_ema20_${primaryTimeframe}_in_atr| > 3, avoid new entries; only consider flattening or, if losing with strong contradiction and HIGH signal, flipping with care.

BIAS DEFINITIONS
- micro_bias = short-term directional edge from flow/tape + recent price action on ${indicators.microTimeFrame}.
- primary_bias = slope/RSI/EMA20 alignment on ${primaryTimeframe}.
- macro_bias = regime_trend_up / regime_trend_down on ${indicators.macroTimeFrame}; if both are false, macro_bias=NEUTRAL.
- context_bias = trend/regime on ${contextTimeframe} (higher timeframe); it modulates risk/selectivity but is not a hard gate.
- Support/Resistance = swing pivot levels; nearest_support_price / nearest_resistance_price and dist_to_*_in_atr use the ATR of that timeframe. level_state ∈ {at_level, approaching, rejected, broken, retesting}. location_confluence_score is a capped (0–1) proximity/break state; do not double-count it with trend.
- Trades WITH macro_bias are preferred; trades AGAINST macro_bias require micro_bias strongly opposite + HIGH signal_strength.
`.trim();

    const modeLabel = dryRun ? 'simulation' : 'live';
    const user = `
You are analyzing ${symbol} on a ${timeframe} horizon (mode=${modeLabel}).

 RISK/COSTS:
 - ${risk_policy}
${realizedRoiLine ? `${realizedRoiLine}\n` : ''}
- S/R method: swing-pivot levels per timeframe (~150 bars), distances expressed in that timeframe's ATR, level_state ∈ {at_level, approaching, rejected, broken, retesting}.

BASE GATES (Filter for tradeability):
- ${base_gating_flags}

STRATEGY BIAS (Macro Trend):
- ${regime_flags}
- Context bias (${contextTimeframe}): ${contextBias}

KEY METRICS (Values for Judgment):
- ${key_metrics}

DATA INPUTS (with explicit windows):
- Current price and % change (now): ${market_data}
- Volume profile (fixed lookback window = ${TRADE_WINDOW_MINUTES}m): ${vol_profile_str}
- Order flow summary (1m tape/CVD, last 5–15m): ${order_flow}
- Order book & liquidity (snapshot): ${liquidity_data}
- Funding rate & open interest (last 30–60m): ${derivatives}
- Recent price trend (last ${priceTrendPoints.length} bars): ${priceTrendSeries}
${newsSentimentBlock}${newsHeadlinesBlock}${recentActionsBlock}- Current position: ${position_status}
${positionContextBlock}- Technical (short-term, ${indicators.microTimeFrame}, last 30 candles): ${indicators.micro}
- Macro (${indicators.macroTimeFrame}, last 30 candles): ${indicators.macro}
${contextIndicatorsBlock}${contextSRBlock}${primaryIndicatorsBlock}${primarySRBlock}
- HTF location flags: {into_support=${intoContextSupport}, into_resistance=${intoContextResistance}, breakdown_confirmed=${htfBreakdownConfirmed}, breakout_confirmed=${htfBreakoutConfirmed}, location_confluence_score=${clampNumber(
        locationConfluenceScore,
        3,
    )}}
- Momentum state: ${JSON.stringify({
        macro_trend_up: momentumSignals.macroTrendUp,
        macro_trend_down: momentumSignals.macroTrendDown,
        long_momentum: momentumSignals.longMomentum,
        short_momentum: momentumSignals.shortMomentum,
        flow_supports: momentumSignals.flowSupports,
        near_primary: momentumSignals.nearPrimaryEMA20,
        near_micro: momentumSignals.nearMicroEMA20,
        entry_ready_long: momentumSignals.entryReadyLong,
        entry_ready_short: momentumSignals.entryReadyShort,
        micro_extension_atr: momentumSignals.microExtensionInAtr,
        primary_extension_atr: clampNumber(distance_from_ema20_primary_atr, 3),
        primary_slope_pct_per_bar: clampNumber(slope21_primary, 4),
        primary_bias: primaryBias,
        context_bias: contextBias,
        into_context_support: intoContextSupport,
        into_context_resistance: intoContextResistance,
        context_breakdown_confirmed: htfBreakdownConfirmed,
        context_breakout_confirmed: htfBreakoutConfirmed,
        location_confluence_score: clampNumber(locationConfluenceScore, 3),
    })}
- Signal strength drivers: ${JSON.stringify(signalDrivers)}
- Closing guardrails: ${JSON.stringify(closingGuidance)}

TASKS:
1) Evaluate micro_bias (UP/DOWN/NEUTRAL) from short-term flow/tape + recent price action, primary_bias (UP/DOWN/NEUTRAL) from slope/RSI/EMA20 alignment on ${primaryTimeframe}, macro_bias (UP/DOWN/NEUTRAL) from the macro regime flags, and context_bias (UP/DOWN/NEUTRAL) from the ${contextTimeframe} trend/regime.
2) Output one action only: "BUY", "SELL", "HOLD", "CLOSE", or "REVERSE".
   - If no position is open, return BUY/SELL/HOLD.
   - If a position is open, you may HOLD, CLOSE, or REVERSE (REVERSE = close + open opposite side).
3) Assess signal strength: LOW, MEDIUM, or HIGH (modulate selectivity using context_bias rules).
4) Summarize in ≤2 lines.

JSON OUTPUT (strict):
{"action":"BUY|SELL|HOLD|CLOSE|REVERSE","micro_bias":"UP|DOWN|NEUTRAL","primary_bias":"UP|DOWN|NEUTRAL","macro_bias":"UP|DOWN|NEUTRAL","context_bias":"UP|DOWN|NEUTRAL","signal_strength":"LOW|MEDIUM|HIGH","summary":"≤2 lines","reason":"brief rationale","exit_size_pct":null|0-100}
`.trim();

    return { system: sys, user };
}

// ------------------------------
// OpenAI API Call
// ------------------------------

export async function callAI(system: string, user: string) {
    const apiKey = process.env.OPENAI_API_KEY;
    if (!apiKey) throw new Error('Missing OPENAI_API_KEY');

    const base = AI_BASE_URL;
    const model = AI_MODEL;

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
            temperature: 0.2,
            response_format: { type: 'json_object' },
        }),
    });

    if (!res.ok) throw new Error(`AI error: ${res.status} ${res.statusText}`);

    const data = await res.json();
    const text = data.choices?.[0]?.message?.content || '{}';
    return JSON.parse(text);
}
