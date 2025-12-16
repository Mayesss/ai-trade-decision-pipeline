import { bitgetFetch, resolveProductType } from '../../lib/bitget';
import {
    computeATR,
    computeEMA,
    computeRSI_Wilder,
    computeSMA,
    computeSRLevels,
    computeVWAP,
    LevelDescriptor,
    LevelState,
    MultiTFIndicators,
    slopePct,
    SRLevels,
} from '../../lib/indicators';

export type TimeframeKey = '1D' | '4H' | '1H' | '15m';

export type TimeframeMetrics = {
    timeframe: TimeframeKey;
    close: number;
    atr: number;
    atr_pct: number;
    rsi: number;
    ema9: number;
    ema20: number;
    ema21: number;
    ema50: number;
    sma200: number;
    vwap: number;
    slopeEMA21_10: number;
    dist_from_ema20_in_atr: number;
    trend: 'up' | 'down' | 'neutral';
    sr?: SRLevels;
};

const TIMEFRAME_LOOKBACKS: Record<TimeframeKey, number> = {
    '1D': 220,
    '4H': 220,
    '1H': 220,
    '15m': 320,
};

function ensureAscending(candles: any[]) {
    return (candles || []).slice().sort((a: any, b: any) => Number(a[0]) - Number(b[0]));
}

function toNumber(val: any, fallback = 0) {
    const n = Number(val);
    return Number.isFinite(n) ? n : fallback;
}

function deriveTrend(ema20: number, ema50: number, sma200: number, slope: number): 'up' | 'down' | 'neutral' {
    if (ema20 > ema50 && ema50 > sma200 && slope > 0) return 'up';
    if (ema20 < ema50 && ema50 < sma200 && slope < 0) return 'down';
    return 'neutral';
}

function summaryFromMetrics(m: TimeframeMetrics) {
    return `VWAP=${m.vwap.toFixed(2)}, RSI=${m.rsi.toFixed(1)}, trend=${m.trend}, ATR=${m.atr.toFixed(
        2,
    )}, EMA9=${m.ema9.toFixed(2)}, EMA21=${m.ema21.toFixed(2)}, EMA20=${m.ema20.toFixed(2)}, EMA50=${m.ema50.toFixed(
        2,
    )}, SMA200=${m.sma200.toFixed(2)}, slopeEMA21_10=${m.slopeEMA21_10.toFixed(3)}%/bar`;
}

export async function fetchCandles(symbol: string, timeframe: TimeframeKey, limit?: number) {
    const productType = resolveProductType();
    const candles = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
        symbol,
        productType,
        granularity: timeframe,
        limit: limit ?? TIMEFRAME_LOOKBACKS[timeframe],
    });
    return ensureAscending(candles);
}

export function computeMetrics(timeframe: TimeframeKey, candles: any[]): TimeframeMetrics {
    const closes = ensureAscending(candles).map((c) => toNumber(c[4]));
    const lastClose = closes.at(-1) ?? 0;

    const ema9 = computeEMA(closes, 9).at(-1) ?? lastClose;
    const ema20 = computeEMA(closes, 20).at(-1) ?? lastClose;
    const ema21 = computeEMA(closes, 21).at(-1) ?? lastClose;
    const ema50 = computeEMA(closes, 50).at(-1) ?? lastClose;
    const sma200 = computeSMA(closes, 200).at(-1) ?? lastClose;
    const rsi = computeRSI_Wilder(closes, 14);
    const atr = computeATR(candles, 14);
    const vwap = computeVWAP(candles);
    const slope = slopePct(computeEMA(closes, 21), 10);

    const dist_from_ema20_in_atr = atr > 0 ? (lastClose - ema20) / atr : 0;
    const atr_pct = lastClose > 0 ? atr / lastClose : 0;
    const trend = deriveTrend(ema20, ema50, sma200, slope);
    const sr = computeSRLevels(candles, atr, timeframe);

    return {
        timeframe,
        close: lastClose,
        atr,
        atr_pct,
        rsi,
        ema9,
        ema20,
        ema21,
        ema50,
        sma200,
        vwap,
        slopeEMA21_10: slope,
        dist_from_ema20_in_atr,
        trend,
        sr,
    };
}

export function buildIndicatorsFromMetrics(
    metrics: Record<TimeframeKey, TimeframeMetrics>,
): MultiTFIndicators {
    const micro = metrics['15m'];
    const macro = metrics['4H'];
    const primary = metrics['1H'];
    const context = metrics['1D'];

    return {
        micro: summaryFromMetrics(micro),
        macro: summaryFromMetrics(macro),
        microTimeFrame: '15m',
        macroTimeFrame: '4H',
        contextTimeFrame: '1D',
        primary: { timeframe: '1H', summary: summaryFromMetrics(primary) },
        context: { timeframe: '1D', summary: summaryFromMetrics(context) },
        sr: {
            '15m': micro.sr,
            '4H': macro.sr,
            '1H': primary.sr,
            '1D': context.sr,
        },
    };
}

export function deriveRegimeFlags(macro: TimeframeMetrics) {
    const slopeUp = macro.slopeEMA21_10 > 0;
    const slopeDown = macro.slopeEMA21_10 < 0;
    const emaAlignedUp = macro.ema20 > macro.ema50;
    const emaAlignedDown = macro.ema20 < macro.ema50;
    const rsiUpper = macro.rsi >= 55;
    const rsiLower = macro.rsi <= 45;

    const regime_trend_up = emaAlignedUp && slopeUp && rsiUpper;
    const regime_trend_down = emaAlignedDown && slopeDown && rsiLower;
    return { regime_trend_up, regime_trend_down };
}

export function summarizeLevel(level?: LevelDescriptor) {
    if (!level) return null;
    return {
        price: level.price,
        dist_in_atr: level.dist_in_atr,
        strength: level.level_strength,
        state: level.level_state as LevelState,
    };
}

export function computeLocationConfluence(levels: Record<TimeframeKey, TimeframeMetrics>) {
    const distances: number[] = [];
    (['15m', '1H', '4H', '1D'] as TimeframeKey[]).forEach((tf) => {
        const sr = levels[tf]?.sr;
        if (sr?.support?.dist_in_atr !== undefined) distances.push(sr.support.dist_in_atr);
        if (sr?.resistance?.dist_in_atr !== undefined) distances.push(sr.resistance.dist_in_atr);
    });

    const minDist = distances.length ? Math.min(...distances) : Infinity;
    const proximity = Number.isFinite(minDist) ? Math.max(0, 1 - Math.min(minDist, 2) / 2) : 0;
    const contextSr = levels['1D']?.sr;
    const breakBonus =
        contextSr?.support?.level_state === 'broken' || contextSr?.resistance?.level_state === 'broken' ? 0.25 : 0;

    const into_context_support =
        typeof contextSr?.support?.dist_in_atr === 'number' ? contextSr.support.dist_in_atr < 0.6 : false;
    const into_context_resistance =
        typeof contextSr?.resistance?.dist_in_atr === 'number' ? contextSr.resistance.dist_in_atr < 0.6 : false;

    return {
        location_confluence_score: Number(Math.min(1, proximity + breakBonus).toFixed(3)),
        into_context_support,
        into_context_resistance,
        context_breakdown_confirmed: contextSr?.support?.level_state === 'broken',
        context_breakout_confirmed: contextSr?.resistance?.level_state === 'broken',
    };
}
