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
import { setEvaluation, getEvaluation } from './utils';

export type PositionContext = {
    side: 'long' | 'short';
    entry_price?: number;
    entry_ts?: string;
    hold_minutes?: number;
    unrealized_pnl_pct?: number;
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

    const ema50Macro = readIndicator('EMA50', macroSummary);
    const ema50Primary = readIndicator('EMA50', primarySummary);
    const ema20Primary = readIndicator('EMA20', primarySummary);
    const ema20Micro = readIndicator('EMA20', microSummary);
    const atrPrimary = readIndicator('ATR', primarySummary);
    const atrMicro = readIndicator('ATR', microSummary);
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

    const longMomentum = macroTrendUp && priceAbovePrimary50 && rsiPullbackLong && slopeUp;
    const shortMomentum = macroTrendDown && priceBelowPrimary50 && rsiPullbackShort && slopeDown;

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
    const toNum = (value: any) => {
        const n = Number(value);
        return Number.isFinite(n) ? n : null;
    };

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

    const market_data = `price=${price}, change24h=${Number.isFinite(change) ? change : 'n/a'}`;

    const liquidity_data = `top bid walls: ${JSON.stringify(analytics.topWalls.bid)}, top ask walls: ${JSON.stringify(
        analytics.topWalls.ask,
    )}`;

    const derivatives = `funding=${bundle.funding?.[0]?.fundingRate ?? 'n/a'}, openInterest=${
        bundle.oi?.openInterestList?.[0]?.size ?? bundle.oi?.openInterestList?.[0]?.openInterest ?? 'n/a'
    }`;

    const extractFundingSeries = (funding: any): number[] => {
        const arr = Array.isArray(funding) ? funding : funding?.data || [];
        return (arr || [])
            .map((f: any) => toNum(f?.fundingRate ?? f?.rate ?? f?.interestRate))
            .filter((v: number | null): v is number => Number.isFinite(v));
    };

    const extractOiSeries = (oi: any): number[] => {
        const list = oi?.openInterestList || oi?.data || [];
        return (list || [])
            .map((item: any) =>
                toNum(item?.size ?? item?.openInterest ?? item?.openInterestUsd ?? item?.openInterestValue),
            )
            .filter((v: number | null): v is number => Number.isFinite(v));
    };

    const computeZScore = (values: number[]): number | null => {
        if (values.length < 5) return null;
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        const variance = values.reduce((a, b) => a + (b - mean) ** 2, 0) / values.length;
        const std = Math.sqrt(variance);
        if (!(std > 0)) return null;
        return (values[values.length - 1]! - mean) / std;
    };

    const fundingRates = extractFundingSeries(bundle.funding);
    const fundingZ14d = computeZScore(fundingRates);
    const oiSeries = extractOiSeries(bundle.oi);
    const oiChg24hPct =
        oiSeries.length >= 2 && oiSeries[0]! > 0 ? ((oiSeries[oiSeries.length - 1]! - oiSeries[0]!) / oiSeries[0]!) * 100 : null;

    const oiPriceDiv =
        Number.isFinite(oiChg24hPct) && Number.isFinite(change)
            ? oiChg24hPct! > 0 && change > 0
                ? 'trend_supported'
                : oiChg24hPct! > 0 && change < 0
                ? 'crowded'
                : oiChg24hPct! < 0 && change > 0
                ? 'short_cover'
                : 'neutral'
            : 'neutral';

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
        ? `- Primary timeframe (${primaryTimeframe}) indicators: ${indicators.primary.summary}\n`
        : '';
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
    const metricsByTf = indicators.metrics || {};
    const primaryMetrics = metricsByTf[primaryTimeframe] || {};
    const macroMetrics = metricsByTf[macroTimeframe] || {};

    const atrPctile4h = typeof primaryMetrics.atrPctile === 'number' ? primaryMetrics.atrPctile : null;
    const atrPctile1d = typeof macroMetrics.atrPctile === 'number' ? macroMetrics.atrPctile : null;
    const rvol4h = typeof primaryMetrics.rvol === 'number' ? primaryMetrics.rvol : null;
    const rvol1d = typeof macroMetrics.rvol === 'number' ? macroMetrics.rvol : null;
    const structure4hState = primaryMetrics.structure ?? 'range';
    const bos4h = Boolean(primaryMetrics.bos);
    const bosDir4h = primaryMetrics.bosDir ?? null;
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

    const formatNum = (value: number | null | undefined, digits = 2) =>
        Number.isFinite(value as number) ? Number(value).toFixed(digits) : 'n/a';

    const microKey = microTimeframe.toLowerCase();
    const primaryKey = primaryTimeframe.toLowerCase();
    const macroKey = macroTimeframe.toLowerCase();

    const key_metrics =
        `spread_bps=${spread_bps.toFixed(2)}, ` +
        `atr_pct_${macroKey}=${atr_pct_macro.toFixed(2)}%, atr_pct_${primaryKey}=${atr_pct_primary.toFixed(2)}%, ` +
        `atr_pctile_${macroKey}=${formatNum(atrPctile1d, 0)}, atr_pctile_${primaryKey}=${formatNum(atrPctile4h, 0)}, ` +
        `rsi_${microKey}=${rsiMicroDisplay}, rsi_${primaryKey}=${rsiPrimaryDisplay}, rsi_${macroKey}=${rsiMacroDisplay}, ` +
        `primary_slope_pct_per_bar=${slope21_primary.toFixed(4)}, ` +
        `dist_from_ema20_${microKey}_in_atr=${distance_from_ema_atr.toFixed(2)}, dist_from_ema20_${primaryKey}_in_atr=${distance_from_ema20_primary_atr.toFixed(2)}, ` +
        `structure_${primaryKey}=${structure4hState}, bos_${primaryKey}=${bos4h ? 1 : 0}, choch_${primaryKey}=${choch4h ? 1 : 0}, ` +
        `rvol_${primaryKey}=${formatNum(rvol4h, 2)}, rvol_${macroKey}=${formatNum(rvol1d, 2)}, value_state_${macroKey}=${valueState1d}, ` +
        `funding_z_14d=${formatNum(fundingZ14d, 2)}, oi_chg_24h_pct=${formatNum(oiChg24hPct, 2)}, oi_price_div=${oiPriceDiv}`;

    // --- SIGNAL STRENGTH DRIVERS & CLOSING GUIDANCE ---
    const clampNumber = (value: number | null | undefined, digits = 3) =>
        Number.isFinite(value as number) ? Number((value as number).toFixed(digits)) : null;
    const trendBias = gates.regime_trend_up ? 1 : gates.regime_trend_down ? -1 : 0;
    const oversoldMicro = typeof rsi_micro === 'number' && rsi_micro < 35;
    const overboughtMicro = typeof rsi_micro === 'number' && rsi_micro > 65;
    const reversalOpportunity = oversoldMicro ? 'oversold' : overboughtMicro ? 'overbought' : null;

    const contextBiasDriver = contextBias === 'UP' ? 0.6 : contextBias === 'DOWN' ? -0.6 : 0;
    const supportProximity = typeof htfSupportDist === 'number' ? Math.max(0, 1 - Math.min(htfSupportDist, 2) / 2) : 0;
    const resistanceProximity =
        typeof htfResistanceDist === 'number' ? Math.max(0, 1 - Math.min(htfResistanceDist, 2) / 2) : 0;
    const locationScoreLong = Math.min(1, supportProximity + (htfBreakoutConfirmed ? 0.3 : 0));
    const locationScoreShort = Math.min(1, resistanceProximity + (htfBreakdownConfirmed ? 0.3 : 0));
    const locationConfluenceScore = Math.max(locationScoreLong, locationScoreShort);

    const nearPrimarySupport = typeof primarySR?.support?.dist_in_atr === 'number' ? primarySR.support.dist_in_atr <= 0.6 : false;
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
    const fundingOkLong = Number.isFinite(fundingZ14d as number) ? (fundingZ14d as number) < 1.5 : false;
    const fundingOkShort = Number.isFinite(fundingZ14d as number) ? (fundingZ14d as number) > -1.5 : false;

    const longDrivers = [
        structure4hState === 'bull' || bosDir4h === 'up',
        (breakoutRetestOk4h && breakoutRetestDir4h === 'up') || nearPrimarySupport,
        macroBias !== 'DOWN',
        contextBias !== 'DOWN' || intoContextSupport,
        valueOkLong,
        Number.isFinite(rvol4h as number) ? (rvol4h as number) >= 1.2 : false,
        fundingOkLong,
    ];

    const shortDrivers = [
        structure4hState === 'bear' || bosDir4h === 'down',
        (breakoutRetestOk4h && breakoutRetestDir4h === 'down') || nearPrimaryResistance,
        macroBias !== 'UP',
        contextBias !== 'UP' || intoContextResistance,
        valueOkShort,
        Number.isFinite(rvol4h as number) ? (rvol4h as number) >= 1.2 : false,
        fundingOkShort,
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

    const mediumActionReady =
        alignedDriverCount >= 4 && (momentumSignals.longMomentum || momentumSignals.shortMomentum);

    const signalDrivers: Record<string, any> = {
        macro_trend_up: momentumSignals.macroTrendUp,
        macro_trend_down: momentumSignals.macroTrendDown,
        context_bias: contextBias,
        context_bias_driver: clampNumber(contextBiasDriver, 3),
        regime_alignment: clampNumber(regimeAlignment, 2),

        primary_slope_pct_per_bar: clampNumber(slope21_primary, 4),
        micro_slope_pct_per_bar: clampNumber(slope21_micro, 4),

        funding_z_14d: clampNumber(fundingZ14d, 2),
        oi_chg_24h_pct: clampNumber(oiChg24hPct, 2),
        oi_price_divergence: oiPriceDiv,

        aligned_driver_count: alignedDriverCount,
        aligned_driver_count_long: longAlignedDriverCount,
        aligned_driver_count_short: shortAlignedDriverCount,
        favored_side: favoredSide,

        medium_action_ready: mediumActionReady,
        long_momentum: momentumSignals.longMomentum,
        short_momentum: momentumSignals.shortMomentum,
        micro_extension_atr: clampNumber(momentumSignals.microExtensionInAtr ?? null, 3),
        primary_extension_atr: clampNumber(distance_from_ema20_primary_atr, 3),
        location_confluence_score: clampNumber(locationConfluenceScore, 3),
        location_score_long: clampNumber(locationScoreLong, 3),
        location_score_short: clampNumber(locationScoreShort, 3),
        into_context_support: intoContextSupport,
        into_context_resistance: intoContextResistance,
        chop_risk: chopRisk,
        context_breakdown_confirmed: htfBreakdownConfirmed,
        context_breakout_confirmed: htfBreakoutConfirmed,
        context_support_dist_atr: clampNumber(htfSupportDist ?? null, 3),
        context_resistance_dist_atr: clampNumber(htfResistanceDist ?? null, 3),
    };

    signalDrivers[`rsi_${microKey}`] = rsi_micro;
    signalDrivers[`rsi_${primaryKey}`] = rsi_primary;
    signalDrivers[`rsi_${macroKey}`] = rsi_macro;

    signalDrivers[`dist_from_ema20_${microKey}_in_atr`] = clampNumber(distance_from_ema_atr, 3);
    signalDrivers[`dist_from_ema20_${primaryKey}_in_atr`] = clampNumber(distance_from_ema20_primary_atr, 3);

    signalDrivers[`atr_pct_${macroKey}`] = clampNumber(atr_pct_macro, 3);
    signalDrivers[`atr_pct_${primaryKey}`] = clampNumber(atr_pct_primary, 3);
    signalDrivers[`atr_pctile_${macroKey}`] = clampNumber(atrPctile1d, 0);
    signalDrivers[`atr_pctile_${primaryKey}`] = clampNumber(atrPctile4h, 0);

    signalDrivers[`structure_${primaryKey}_state`] = structure4hState;
    signalDrivers[`bos_${primaryKey}`] = bos4h;
    signalDrivers[`bos_dir_${primaryKey}`] = bosDir4h;
    signalDrivers[`choch_${primaryKey}`] = choch4h;
    signalDrivers[`breakout_retest_ok_${primaryKey}`] = breakoutRetestOk4h;
    signalDrivers[`breakout_retest_dir_${primaryKey}`] = breakoutRetestDir4h;
    signalDrivers[`structure_break_state_${primaryKey}`] = structureBreakState4h;

    signalDrivers[`rvol_${primaryKey}`] = clampNumber(rvol4h, 2);
    signalDrivers[`rvol_${macroKey}`] = clampNumber(rvol1d, 2);
    signalDrivers[`value_state_${macroKey}`] = valueState1d;
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

    const tfToMinutes = (tf: string): number => {
        const m = tf.match(/^(\d+)\s*(m|h|d)$/i);
        if (!m) return 240;
        const n = Number(m[1]);
        const unit = m[2].toLowerCase();
        if (!Number.isFinite(n) || n <= 0) return 240;
        return unit === 'm' ? n : unit === 'h' ? n * 60 : n * 1440;
    };
    const time_stop_minutes = tfToMinutes(primaryTimeframe) * 3;

    const risk_policy =
        `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, ` +
        `stop=1.5xATR(${primaryTimeframe}), take_profit=2.5xATR(${primaryTimeframe}), ` +
        `time_stop=${time_stop_minutes} minutes (execution-layer timer)`;

    // We only pass the BASE gates now, as the AI will judge the strategy gates using metrics
    const base_gating_flags = `spread_ok=${gates.spread_ok}, liquidity_ok=${gates.liquidity_ok}, atr_ok=${gates.atr_ok}, slippage_ok=${gates.slippage_ok}`;

    // We still pass the Regime for a strong trend bias signal
    const regime_flags = `regime_trend_up=${gates.regime_trend_up}, regime_trend_down=${gates.regime_trend_down}`;

    const realizedRoiLine =
        realizedRoiPct !== undefined && realizedRoiPct !== null && Number.isFinite(realizedRoiPct)
            ? `- Recent realized PnL (last closed): ${Number(realizedRoiPct).toFixed(2)}%`
            : '';

    const sys = `
You are an expert crypto swing-trading market structure analyst and trading assistant.

TIMEFRAMES (fixed for this mode)
- micro: ${microTimeframe} (entry timing / confirmation)
- primary: ${primaryTimeframe} (setup + execution timeframe)
- macro: ${macroTimeframe
} (regime bias, not a hard filter)
- context: ${contextTimeframe} (higher-timeframe location + major levels, risk lever)

Primary strategy: ${primaryTimeframe} swing setups executed with ${microTimeframe} confirmation, aligned with (or tactically fading) the ${macroTimeframe
} regime while respecting ${contextTimeframe} location/levels.
Holding horizon: typically 1–10 days. Prefer fewer, higher-quality trades; avoid churn.

Decision ladder: Base gates → biases (context/macro/primary) → setup drivers → action.
Signal strength is driven by aligned_driver_count + regime_alignment + location/level_quality + extension/mean-reversion risk; use a 1–5 scale (1=weak, 3=base, 5=strongest). LOW/MEDIUM/HIGH are acceptable aliases (LOW≈1-2, MEDIUM≈3, HIGH≈4-5).
Respond in strict JSON ONLY.
Output must be valid JSON parseable by JSON.parse with no trailing commas or extra keys; no markdown or commentary. Keep keys minimal—no extra fields.

GENERAL RULES
- **Base gates**: when flat, if ANY spread_ok/liquidity_ok/atr_ok/slippage_ok is false → action="HOLD". In a position, never block exits; if gates fail, prefer risk-off (CLOSE) over HOLD.
- **Costs / churn control**: total_cost_bps = ~${total_cost_bps}bps (round-trip fees + slippage). For swing entries, avoid trades where the expected move is not clearly larger than costs; default filter: if edge is unclear or setup quality is MED/LOW → HOLD.
- **Leverage**: For BUY/SELL/REVERSE pick leverage 1–5 (integer). Default null on HOLD/CLOSE.
  - Choose leverage based on conviction AND risk (regime alignment, extension, proximity to major levels, volatility). Even on HIGH conviction, use 1–2x if stretched or near major ${contextTimeframe} levels. Never exceed 5.
- **Macro/context usage**:
  - macro_bias (${macroTimeframe
}) is a bias, not a hard filter. Trades with macro_bias are preferred.
  - context_bias (${contextTimeframe}) is a risk lever: when aligned, accept MEDIUM setups; when opposed, require HIGH quality and non-extended entries. Use it to adjust selectivity and leverage, not as a hard gate.
- **Support/Resistance & location**: swing-pivot derived per timeframe; distances in ATR of that timeframe.
  - Avoid opening new positions directly into strong opposite levels (e.g., long into nearby resistance, short into nearby support) unless breakout/breakdown is confirmed and strength is HIGH.
  - If both nearest support and resistance are close / at_level, treat as range/chop: avoid fresh entries unless signal_strength is HIGH with clean level logic.
- **Position truthfulness**: NEVER describe a position as winning if unrealized_pnl_pct < 0 or if price_vs_breakeven_pct is on the losing side for that direction.
- **Temporal inertia (anti-flip)**: avoid more than one action change (CLOSE/REVERSE) in the same direction within the last 2 calls unless signal_strength stays HIGH and regime/structure invalidation is strengthening.
- **Exit sizing**: Default exit_size_pct = 100 (full close). Use 30–70 when trimming risk (approaching major opposite level with gains, regime weakening, structure damage without full reversal). Avoid trims <20%; omit when not needed.

BIAS DEFINITIONS (swing-oriented)
- primary_bias (${primaryTimeframe}): trend/structure bias from EMA alignment/slope, RSI, HH/HL vs LH/LL, and ${primaryTimeframe} range state.
- micro_bias (${microTimeframe}): timing bias from ${microTimeframe} structure (break/retest, pullback continuation, reversal failure), momentum, and reaction at levels.
- macro_bias (${macroTimeframe
}): regime trend up/down; if both false → NEUTRAL.
- context_bias (${contextTimeframe}): higher-timeframe regime/trend + location; modulates risk/selectivity.

SETUP DRIVERS (what “aligned_driver_count” should represent)
Count drivers that materially support a directional swing trade:
- Structure: HH/HL or LH/LL alignment on ${primaryTimeframe}, with ${microTimeframe} confirmation (break + hold / retest).
- Level logic: entry near meaningful ${primaryTimeframe} support/resistance, or post-breakout retest; clean invalidation.
- Regime alignment: ${macroTimeframe
} + ${contextTimeframe} supportive (or a high-quality counter-regime mean-reversion at extreme location).
- Momentum quality: RSI/slope confirmation on ${primaryTimeframe}; ${microTimeframe} impulse/continuation vs fading.
- Positioning/derivatives context: funding/OI extremes or supportive trend in OI (as a modifier, not primary).
- Volatility/ATR sanity: not entering after exhausted expansion unless continuation setup is exceptionally clean.

ACTIONS LOGIC
- **No position open**:
  - With base gates true + HIGH + aligned_driver_count ≥ 4 → BUY/SELL in the direction of the primary_bias, unless ${microTimeframe} invalidates.
  - MEDIUM requires aligned_driver_count ≥ 4 AND acceptable location (not into strong opposite level, not overly extended).
  - Counter-macro trades are allowed but selective:
    - Require HIGH, aligned_driver_count ≥ 5, clear ${primaryTimeframe} structure reversal, and non-extended entry (|dist_from_ema20_${microTimeframe}_in_atr| ≤ 1.8 and |dist_from_ema20_${primaryTimeframe}_in_atr| ≤ 2.0).
  - Avoid new shorts when dist_to_support_in_atr_${contextTimeframe} < 0.6 unless breakdown_confirmed=true; mirror for longs vs resistance/breakout.
  - HOLD on LOW signals, unclear structure, mixed regime/location, or extreme extension without clean continuation logic.
- **Position open**:
  - Strong opposite + HIGH → CLOSE or REVERSE (use reversal rules).
  - Trim 30–70% when: gains into major opposite level, structure weakens, regime alignment deteriorates, or volatility expansion looks exhausted.
  - Prefer HOLD if macro/context support the position and no strong opposite structure signal; prefer HOLD over CLOSE when |unrealized_pnl_pct| < 0.25% and no HIGH opposite signal.

REVERSAL DISCIPLINE (swing)
- REVERSE = close entire current position (exit_size_pct=100) then open opposite side. No partial reversals.
- REVERSE only if reverse_confidence="high", aligned_driver_count ≥ 5, and signal_strength = HIGH, plus clear ${primaryTimeframe} structure flip (break + acceptance) confirmed by ${microTimeframe}.
- Do NOT REVERSE if unrealized_pnl_pct < -0.5% without major regime/structure change; if conditions fail, prefer CLOSE (risk-off) or HOLD (if still valid).

EXTENSION / OVERBOUGHT-OVERSOLD (swing)
- Use extension as risk control, not as a standalone signal.
- On ${microTimeframe}: |dist_from_ema20_${microTimeframe}_in_atr| in [2,2.5) → require cleaner level/invalidation; ≥ 2.5 → avoid fresh entries; > 3 → strongly prefer no new entries.
- On ${primaryTimeframe}: |dist_from_ema20_${primaryTimeframe}_in_atr| ≥ 2.0 → be selective and tighten profit-taking; ≥ 2.5 avoid fresh entries unless this is a post-breakout retest with HIGH strength.
`.trim();

    const modeLabel = dryRun ? 'simulation' : 'live';
    const user = `
You are analyzing ${symbol} for swing trading (mode=${modeLabel}).
Timeframes: micro=${microTimeframe}, primary=${primaryTimeframe}, macro=${macroTimeframe
}, context=${contextTimeframe}. I will call you roughly once per ${primaryTimeframe}.

RISK/COSTS:
- ${risk_policy}
${realizedRoiLine ? `${realizedRoiLine}\n` : ''}
- S/R method: swing-pivot levels per timeframe (~150 bars), distances expressed in that timeframe's ATR, level_state ∈ {at_level, approaching, rejected, broken, retesting}.

BASE GATES (tradeability):
- ${base_gating_flags}

REGIME / BIASES:
- ${regime_flags}  // macro (${macroTimeframe
}) regime flags
- Context bias (${contextTimeframe}): ${contextBias}

KEY METRICS:
- ${key_metrics}

DATA INPUTS (swing-relevant windows):
- Current price and % change (now): ${market_data}
- Volume / activity (lookback window = ${TRADE_WINDOW_MINUTES}m): ${vol_profile_str}
- Price action (recent bars for structure context): ${priceTrendSeries}
- Derivatives positioning (last 2–4 ${primaryTimeframe}): ${derivatives}
- Liquidity/spread snapshot (cost sanity check): ${liquidity_data}
${newsSentimentBlock}${newsHeadlinesBlock}${recentActionsBlock}- Current position: ${position_status}
${positionContextBlock}- Technical (micro ${microTimeframe}, last 60 candles): ${indicators.micro}
- Primary (${primaryTimeframe}, last 60 candles): ${indicators.primary?.summary ?? 'n/a'}
- Macro (${macroTimeframe
}, last 60 candles): ${indicators.macro}
${contextIndicatorsBlock}${contextSRBlock}${primaryIndicatorsBlock}${primarySRBlock}
- HTF location flags: {into_support=${intoContextSupport}, into_resistance=${intoContextResistance}, breakdown_confirmed=${htfBreakdownConfirmed}, breakout_confirmed=${htfBreakoutConfirmed}, location_confluence_score=${clampNumber(locationConfluenceScore, 3)}}

- Swing state (compact):
${JSON.stringify({
  macro_trend_up: momentumSignals.macroTrendUp,
  macro_trend_down: momentumSignals.macroTrendDown,
  primary_bias: primaryBias,
  context_bias: contextBias,
  into_context_support: intoContextSupport,
  into_context_resistance: intoContextResistance,
  context_breakdown_confirmed: htfBreakdownConfirmed,
  context_breakout_confirmed: htfBreakoutConfirmed,
  location_confluence_score: clampNumber(locationConfluenceScore, 3),
  micro_extension_atr: momentumSignals.microExtensionInAtr,              // interpret as ${microTimeframe}
  primary_extension_atr: clampNumber(distance_from_ema20_primary_atr, 3) // interpret as ${primaryTimeframe}
})}

- Signal strength drivers: ${JSON.stringify(signalDrivers)}
- Closing guardrails: ${JSON.stringify(closingGuidance)}

TASKS:
1) Determine micro_bias (${microTimeframe}), primary_bias (${primaryTimeframe}), macro_bias (${macroTimeframe
}), and context_bias (${contextTimeframe}).
2) Output exactly one action: "BUY", "SELL", "HOLD", "CLOSE", or "REVERSE".
   - If no position: BUY/SELL/HOLD.
   - If in position: HOLD/CLOSE/REVERSE only.
3) Pick leverage (1–5) for BUY/SELL/REVERSE; use null for HOLD/CLOSE.
4) Provide signal_strength (1–5 or LOW/MEDIUM/HIGH).
5) Summarize in ≤2 lines.

JSON OUTPUT (strict):
{"action":"BUY|SELL|HOLD|CLOSE|REVERSE","micro_bias":"UP|DOWN|NEUTRAL","primary_bias":"UP|DOWN|NEUTRAL","macro_bias":"UP|DOWN|NEUTRAL","context_bias":"UP|DOWN|NEUTRAL","signal_strength":"1|2|3|4|5|LOW|MEDIUM|HIGH","summary":"≤2 lines","reason":"brief rationale","exit_size_pct":null|0-100,"leverage":null|1|2|3|4|5}
`;

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
