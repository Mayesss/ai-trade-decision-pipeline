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
    indicators: MultiTFIndicators,
    gates: any, // <--- Retain the gates object for the base gate checks
    position_context: PositionContext | null = null,
    momentumSignalsOverride?: MomentumSignals,
    recentActions: { action: string; timestamp: number }[] = [],
    realizedRoiPct?: number | null,
    dryRun?: boolean,
    spreadBpsOverride?: number,
    decisionPolicy?: DecisionPolicy,
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
    const bestBidLabel = Number.isFinite(bestBidRaw) ? bestBidRaw.toFixed(2) : 'n/a';
    const bestAskLabel = Number.isFinite(bestAskRaw) ? bestAskRaw.toFixed(2) : 'n/a';

    const market_data = `price=${price}, change24h=${Number.isFinite(change) ? change : 'n/a'}`;

    const liquidity_data = `spread_bps=${spreadBpsCanonical.toFixed(6)}, best_bid=${bestBidLabel}, best_ask=${bestAskLabel}, top bid walls: ${JSON.stringify(
        analytics.topWalls.bid,
    )}, top ask walls: ${JSON.stringify(analytics.topWalls.ask)}`;

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
    const forexEventContextBlock =
        forex_event_context && typeof forex_event_context === 'object'
            ? `- Forex macro events (advisory only; not hard-gated): ${JSON.stringify(forex_event_context)}\n`
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
    const contextCandleDepthRaw = indicators.candleDepth?.[contextTimeframe];
    const contextCandleDepth = Number.isFinite(contextCandleDepthRaw as number) ? Number(contextCandleDepthRaw) : null;
    const contextDepthBlock =
        contextCandleDepth !== null
            ? `- Context candle depth (${contextTimeframe}): ${contextCandleDepth} candles loaded (requested up to 200)\n`
            : '';
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
    const spread_bps = spreadBpsCanonical;
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

    const formatNum = (value: number | null | undefined, digits = 2) =>
        Number.isFinite(value as number) ? Number(value).toFixed(digits) : 'n/a';

    const microKey = microTimeframe.toLowerCase();
    const primaryKey = primaryTimeframe.toLowerCase();
    const macroKey = macroTimeframe.toLowerCase();

    const key_metrics =
        `spread_bps=${spread_bps.toFixed(6)}, ` +
        `atr_pct_${macroKey}=${atr_pct_macro.toFixed(2)}%, atr_pct_${primaryKey}=${atr_pct_primary.toFixed(2)}%, ` +
        `atr_pctile_${macroKey}=${formatNum(atrPctile1d, 0)}, atr_pctile_${primaryKey}=${formatNum(atrPctile4h, 0)}, ` +
        `rsi_${microKey}=${rsiMicroDisplay}, rsi_${primaryKey}=${rsiPrimaryDisplay}, rsi_${macroKey}=${rsiMacroDisplay}, ` +
        `primary_slope_pct_per_bar=${slope21_primary.toFixed(4)}, ` +
        `dist_from_ema20_${microKey}_in_atr=${distance_from_ema_atr.toFixed(2)}, dist_from_ema20_${primaryKey}_in_atr=${distance_from_ema20_primary_atr.toFixed(2)}, ` +
        `structure_${primaryKey}=${structure4hState}, bos_${primaryKey}=${String(bos4h)}, choch_${primaryKey}=${String(choch4h)}, ` +
        `rvol_${primaryKey}=${formatNum(rvol4h, 2)}, rvol_${macroKey}=${formatNum(rvol1d, 2)}, value_state_${macroKey}=${valueState1d}`;

    // --- SIGNAL STRENGTH DRIVERS & CLOSING GUIDANCE ---
    const clampNumber = (value: number | null | undefined, digits = 3) =>
        Number.isFinite(value as number) ? Number((value as number).toFixed(digits)) : null;
    const trendBias = gates.regime_trend_up ? 1 : gates.regime_trend_down ? -1 : 0;
    const oversoldMicro = typeof rsi_micro === 'number' && rsi_micro <= 35;
    const overboughtMicro = typeof rsi_micro === 'number' && rsi_micro >= 65;
    const reversalOpportunity = oversoldMicro ? 'oversold' : overboughtMicro ? 'overbought' : null;

    const contextBiasDriver = contextBias === 'UP' ? 0.6 : contextBias === 'DOWN' ? -0.6 : 0;
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

    const signalDrivers: Record<string, any> = {
        macro_trend_up: momentumSignals.macroTrendUp,
        macro_trend_down: momentumSignals.macroTrendDown,
        context_bias: contextBias,
        context_bias_driver: clampNumber(contextBiasDriver, 3),
        regime_alignment: clampNumber(regimeAlignment, 2),
        micro_entry_ok: Boolean(momentumSignals.info?.microEntryOk),
        micro_bias: microBiasLabel,
        micro_bias_calc: microBiasLabel,
        micro_bias_source: microBiasSource,
        micro_structure_state: microStructureState,
        micro_structure_break_state: microStructureBreakState,
        micro_bos: microBos,
        micro_bos_dir: microBosDir,
        micro_choch: microChoch,
        micro_breakout_retest_ok: microBreakoutRetestOk,
        micro_breakout_retest_dir: microBreakoutRetestDir,
        primary_trend_up: primaryTrendUp,
        primary_trend_down: primaryTrendDown,
        primary_breakdown_confirmed: primaryBreakdownConfirmed,
        primary_breakout_confirmed: primaryBreakoutConfirmed,

        primary_slope_pct_per_bar: clampNumber(slope21_primary, 4),
        micro_slope_pct_per_bar: clampNumber(slope21_micro, 4),

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

    const risk_policy = `fees=${taker_round_trip_bps}bps round-trip, slippage=${slippage_bps}bps, `;
    // `stop=1.5xATR(${primaryTimeframe}), take_profit=2.5xATR(${primaryTimeframe}), ` +
    //`time_stop=${time_stop_minutes} minutes (execution-layer timer)`;

    // We only pass the BASE gates now, as the AI will judge the strategy gates using metrics
    const base_gating_flags = `spread_ok=${gates.spread_ok}, liquidity_ok=${gates.liquidity_ok}, atr_ok=${gates.atr_ok}, slippage_ok=${gates.slippage_ok}`;

    // We still pass the Regime for a strong trend bias signal
    const regime_flags = `regime_trend_up=${gates.regime_trend_up}, regime_trend_down=${gates.regime_trend_down}`;

    const realizedRoiLine =
        realizedRoiPct !== undefined && realizedRoiPct !== null && Number.isFinite(realizedRoiPct)
            ? `- Recent realized PnL (last closed): ${Number(realizedRoiPct).toFixed(2)}%`
            : '';
    const resolvedDecisionPolicy = resolveDecisionPolicy(decisionPolicy);
    const strictPolicy = resolvedDecisionPolicy === 'strict';
    const decisionPolicyLabel = strictPolicy ? 'strict_guardrails' : 'balanced_guardrails';
    const baseGatesRule = strictPolicy
        ? '**Base gates**: when flat, if ANY spread_ok/liquidity_ok/atr_ok/slippage_ok is false -> action="HOLD". In a position, never block exits; if gates fail, prefer risk-off (CLOSE) over HOLD.'
        : '**Base gates**: when flat, default to HOLD if spread/liquidity/ATR/slippage is poor. In a position, never add risk when gates fail; prefer HOLD/CLOSE and avoid REVERSE unless the invalidation is very clear.';
    const microEntryRule = strictPolicy
        ? 'If flat and micro_entry_ok=false -> HOLD unless signal_strength=HIGH and breakout_retest_ok_primary=true.'
        : 'If flat and micro_entry_ok=false, treat as caution: favor HOLD unless setup quality is at least MEDIUM with clear structure confirmation (e.g., breakout/retest).';
    const trendGuardHeading = strictPolicy
        ? `**Hard trend guard (${microTimeframe}+${primaryTimeframe})**:`
        : `**Trend guard (${microTimeframe}+${primaryTimeframe})**:`;
    const trendGuardShortRule = strictPolicy
        ? `If ${primaryTimeframe} trend is UP (primary_trend_up=true) AND ${microTimeframe} trend is UP (micro_bias=UP), do NOT short.`
        : `If ${primaryTimeframe} trend is UP (primary_trend_up=true) AND ${microTimeframe} trend is UP (micro_bias=UP), strongly avoid shorts unless there is explicit structural invalidation.`;
    const trendGuardLongRule = strictPolicy
        ? `If ${primaryTimeframe} trend is DOWN (primary_trend_down=true) AND ${microTimeframe} trend is DOWN (micro_bias=DOWN), do NOT long.`
        : `If ${primaryTimeframe} trend is DOWN (primary_trend_down=true) AND ${microTimeframe} trend is DOWN (micro_bias=DOWN), strongly avoid longs unless there is explicit structural invalidation.`;
    const rangeHandlingRule = strictPolicy
        ? `If structure_${primaryKey}=range, do NOT pick direction from ${macroTimeframe}/${contextTimeframe} trend alone. Favor edge trades (near ${primaryTimeframe} support/resistance with tight invalidation) or breakout + retest in the breakout direction.`
        : `If structure_${primaryKey}=range, de-prioritize direction picks from ${macroTimeframe}/${contextTimeframe} trend alone. Prefer edge trades (near ${primaryTimeframe} support/resistance) or breakout + retest in the breakout direction.`;
    const antiFlipRule = strictPolicy
        ? '**Temporal inertia (anti-flip)**: avoid more than one action change (CLOSE/REVERSE) in the same direction within the last 2 calls unless signal_strength stays HIGH and regime/structure invalidation is strengthening.'
        : '**Temporal inertia (anti-flip)**: avoid repeated CLOSE/REVERSE flips in back-to-back calls unless signal_strength is at least MEDIUM and invalidation keeps strengthening.';
    const reversalLossRule = strictPolicy
        ? 'Do NOT REVERSE if unrealized_pnl_pct < -0.5% without major regime/structure change; if conditions fail, prefer CLOSE (risk-off) or HOLD (if still valid).'
        : 'If unrealized_pnl_pct < -0.5%, be conservative on REVERSE unless there is a major regime/structure change; otherwise prefer CLOSE or HOLD.';
    const extensionMicroWarn = strictPolicy ? 2 : 2.3;
    const extensionMicroAvoid = strictPolicy ? 2.5 : 2.8;
    const extensionMicroNoEntry = strictPolicy ? 3 : 3.3;
    const extensionPrimaryWarn = strictPolicy ? 2 : 2.3;
    const extensionPrimaryAvoid = strictPolicy ? 2.5 : 2.8;

    const sys = `
You are an expert swing-trading market structure analyst and trading assistant.

TIMEFRAMES (fixed for this mode)
- micro: ${microTimeframe} (entry timing / confirmation)
- primary: ${primaryTimeframe} (setup + execution timeframe)
- macro: ${macroTimeframe} (regime bias, not a hard filter)
- context: ${contextTimeframe} (higher-timeframe location + major levels, risk lever)

Primary strategy: ${primaryTimeframe} swing setups executed with ${microTimeframe} confirmation, aligned with (or tactically fading) the ${macroTimeframe} regime while respecting ${contextTimeframe} location/levels.
Holding horizon: typically 1–10 days. Prefer fewer, higher-quality trades; avoid churn.

Decision ladder: Base gates → biases (context/macro/primary) → setup drivers → action.
Signal strength is driven by aligned_driver_count + regime_alignment + location/level_quality + extension/mean-reversion risk; use a 1–5 scale (1=weak, 3=base, 5=strongest). LOW/MEDIUM/HIGH are acceptable aliases (LOW≈1-2, MEDIUM≈3, HIGH≈4-5).
Respond in strict JSON ONLY.
Output must be valid JSON parseable by JSON.parse with no trailing commas or extra keys; no markdown or commentary. Keep keys minimal—no extra fields.
Decision policy mode: ${decisionPolicyLabel}.

GENERAL RULES
- ${baseGatesRule}
- ${microEntryRule}
- **Costs / churn control**: total_cost_bps = ~${total_cost_bps}bps (round-trip fees + slippage). For swing entries, avoid trades where the expected move is not clearly larger than costs; default filter: if edge is unclear or setup quality is MED/LOW → HOLD.
- **Leverage**: For BUY/SELL/REVERSE pick leverage 1–5 (integer). Default null on HOLD/CLOSE.
  - Choose leverage based on conviction AND risk (regime alignment, extension, proximity to major levels, volatility). Even on HIGH conviction, use 1–2x if stretched or near major ${contextTimeframe} levels. Never exceed 5.
- **Macro/context usage**:
  - macro_bias (${macroTimeframe}) is a bias, not a hard filter. Trades with macro_bias are preferred.
  - context_bias (${contextTimeframe}) is a risk lever: when aligned, accept MEDIUM setups; when opposed, require HIGH quality and non-extended entries. Use it to adjust selectivity and leverage, not as a hard gate.
- ${trendGuardHeading}
  - ${trendGuardShortRule}
    - Exception: only allow a short if primary_breakdown_confirmed=true AND ${microTimeframe} confirms with lower-high + breakdown/retest (micro_bias=DOWN with clear bearish structure).
  - ${trendGuardLongRule}
    - Exception: only allow a long if primary_breakout_confirmed=true AND ${microTimeframe} confirms with higher-low + breakout/retest (micro_bias=UP with clear bullish structure).
- **Support/Resistance & location**: swing-pivot derived per timeframe; distances in ATR of that timeframe.
  - Avoid opening new positions directly into strong opposite levels (e.g., long into nearby resistance, short into nearby support) unless breakout/breakdown is confirmed and strength is HIGH.
  - If both nearest support and resistance are close / at_level, treat as range/chop: avoid fresh entries unless signal_strength is HIGH with clean level logic.
- **Range handling (${primaryTimeframe})**:
  - ${rangeHandlingRule}
  - When range and breakout state conflicts with location (e.g., approaching resistance but structure_break_state_${primaryKey}=above with breakout_retest_ok_${primaryKey}=true and dir=up), treat it as a breakout+retest and prefer LONG setups; do not short into that conflict.
- **Position truthfulness**: NEVER describe a position as winning if unrealized_pnl_pct < 0 or if price_vs_breakeven_pct is on the losing side for that direction.
- ${antiFlipRule}
- **Exit sizing**: Default exit_size_pct = 100 (full close). Use 30–70 when trimming risk (approaching major opposite level with gains, regime weakening, structure damage without full reversal). Avoid trims <20%; omit when not needed.

BIAS DEFINITIONS (swing-oriented)
- primary_bias (${primaryTimeframe}): trend/structure bias from EMA alignment/slope, RSI, HH/HL vs LH/LL, and ${primaryTimeframe} range state.
- micro_bias (${microTimeframe}): timing bias from ${microTimeframe} structure (break/retest, pullback continuation, reversal failure), momentum, and reaction at levels.
- macro_bias (${macroTimeframe}): regime trend up/down; if both false → NEUTRAL.
- context_bias (${contextTimeframe}): higher-timeframe regime/trend + location; modulates risk/selectivity.

MICRO BIAS NORMALIZATION (${microTimeframe})
- Compute micro_bias with strict precedence:
  1) Structure first: breakout_retest_dir_${microKey} → structure_break_state_${microKey} → bos_dir_${microKey} → structure_${microKey}_state
  2) Momentum fallback: EMA slope + RSI + price vs EMA20 on ${microTimeframe}
  3) Else NEUTRAL
- If structure and momentum disagree, structure wins.

SETUP DRIVERS (what “aligned_driver_count” should represent)
Count drivers that materially support a directional swing trade:
- Structure: HH/HL or LH/LL alignment on ${primaryTimeframe}, with ${microTimeframe} confirmation (break + hold / retest).
- Level logic: entry near meaningful ${primaryTimeframe} support/resistance, or post-breakout retest; clean invalidation.
- Regime alignment: ${macroTimeframe} + ${contextTimeframe} supportive (or a high-quality counter-regime mean-reversion at extreme location).
- Momentum quality: RSI/slope confirmation on ${primaryTimeframe}; ${microTimeframe} impulse/continuation vs fading.
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
- ${reversalLossRule}

EXTENSION / OVERBOUGHT-OVERSOLD (swing)
- Use extension as risk control, not as a standalone signal.
- Overbought/oversold is NOT a counter-trend trigger by itself. Treat RSI extremes + extension as "permission" only when structure shows damage/flip (e.g., ${microTimeframe} fails to make HH/LL and breaks key support/resistance, followed by ${primaryTimeframe} rolling over).
- On ${microTimeframe}: |dist_from_ema20_${microTimeframe}_in_atr| in [${extensionMicroWarn},${extensionMicroAvoid}) → require cleaner level/invalidation; ≥ ${extensionMicroAvoid} → avoid fresh entries; > ${extensionMicroNoEntry} → strongly prefer no new entries.
- On ${primaryTimeframe}: |dist_from_ema20_${primaryTimeframe}_in_atr| ≥ ${extensionPrimaryWarn} → be selective and tighten profit-taking; ≥ ${extensionPrimaryAvoid} avoid fresh entries unless this is a post-breakout retest with HIGH strength.
`.trim();

    const modeLabel = dryRun ? 'simulation' : 'live';
    const baseSymbol = symbol.replace(/USDT$/i, '');
    const user = `
You are analyzing ${baseSymbol} for swing trading (mode=${modeLabel}).
Timeframes: micro=${microTimeframe}, primary=${primaryTimeframe}, macro=${macroTimeframe}, context=${contextTimeframe}. I will call you roughly once per ${microTimeframe}.
Decision policy: ${decisionPolicyLabel}.

RISK/COSTS:
- ${risk_policy}
${realizedRoiLine ? `${realizedRoiLine}\n` : ''}
- S/R method: swing-pivot levels per timeframe (~150 bars), distances expressed in that timeframe's ATR, level_state ∈ {at_level, approaching, rejected, broken, retesting}.

BASE GATES (tradeability):
- ${base_gating_flags}

REGIME / BIASES:
- ${regime_flags}  // macro (${macroTimeframe}) regime flags
- Context bias (${contextTimeframe}): ${contextBias}

KEY METRICS:
- ${key_metrics}

DATA INPUTS (swing-relevant windows):
- Current price and % change (now): ${market_data}
- Volume / activity (lookback window = ${TRADE_WINDOW_MINUTES}m): ${vol_profile_str}
- Price action (recent bars for structure context): ${priceTrendSeries}
- Liquidity/spread snapshot (cost sanity check): ${liquidity_data}
${newsSentimentBlock}${newsHeadlinesBlock}${forexEventContextBlock}${recentActionsBlock}- Current position: ${position_status}
${positionContextBlock}- Technical (micro ${microTimeframe}, last 60 candles): ${indicators.micro}
- Primary (${primaryTimeframe}, last 60 candles): ${indicators.primary?.summary ?? 'n/a'}
- Macro (${macroTimeframe}, last 60 candles): ${indicators.macro}
${contextIndicatorsBlock}${contextDepthBlock}${contextSRBlock}${primaryIndicatorsBlock}${primarySRBlock}
- HTF location flags: {into_support=${intoContextSupport}, into_resistance=${intoContextResistance}, breakdown_confirmed=${htfBreakdownConfirmed}, breakout_confirmed=${htfBreakoutConfirmed}, location_confluence_score=${clampNumber(locationConfluenceScore, 3)}}

- Swing state (compact):
${JSON.stringify({
    macro_trend_up: momentumSignals.macroTrendUp,
    macro_trend_down: momentumSignals.macroTrendDown,
    primary_bias: primaryBias,
    context_bias: contextBias,
    micro_bias: microBiasLabel,
    micro_bias_calc: microBiasLabel,
    micro_bias_source: microBiasSource,
    micro_structure_state: microStructureState,
    micro_structure_break_state: microStructureBreakState,
    primary_trend_up: primaryTrendUp,
    primary_trend_down: primaryTrendDown,
    primary_breakdown_confirmed: primaryBreakdownConfirmed,
    primary_breakout_confirmed: primaryBreakoutConfirmed,
    into_context_support: intoContextSupport,
    into_context_resistance: intoContextResistance,
    context_breakdown_confirmed: htfBreakdownConfirmed,
    context_breakout_confirmed: htfBreakoutConfirmed,
    location_confluence_score: clampNumber(locationConfluenceScore, 3),
    micro_extension_atr: momentumSignals.microExtensionInAtr, // interpret as ${microTimeframe}
    primary_extension_atr: clampNumber(distance_from_ema20_primary_atr, 3), // interpret as ${primaryTimeframe}
})}

- Signal strength drivers: ${JSON.stringify(signalDrivers)}
- Closing guardrails: ${JSON.stringify(closingGuidance)}

TASKS:
1) Output exactly one action: "BUY", "SELL", "HOLD", "CLOSE", or "REVERSE".
   - If no position: BUY/SELL/HOLD.
   - If in position: HOLD/CLOSE/REVERSE only.
2) Pick leverage (1–5) for BUY/SELL/REVERSE; use null for HOLD/CLOSE.
3) Summarize in ≤3 lines.

JSON OUTPUT (strict):
{"action":"BUY|SELL|HOLD|CLOSE|REVERSE","summary":"≤2 lines","reason":"brief rationale","exit_size_pct":null|0-100,"leverage":null|1|2|3|4|5}
`;

    const context = {
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
    };

    return { system: sys, user, context };
}

export type PromptDecisionContext = {
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
