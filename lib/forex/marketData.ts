import { fetchCapitalCandlesByEpic, fetchCapitalLivePrice, resolveCapitalEpicRuntime } from '../capital';
import { computeATR, computeEMA } from '../indicators';
import type { ForexPairMetrics, ForexSessionTag } from './types';
import { getForexStrategyConfig, pipSizeForPair } from './config';

export type ForexTrendDirection = 'up' | 'down' | 'neutral';

export interface ForexPairMarketState {
    pair: string;
    epic: string;
    nowMs: number;
    sessionTag: ForexSessionTag;
    price: number;
    bid: number;
    offer: number;
    spreadAbs: number;
    spreadPips: number;
    atr5m: number;
    atr1h: number;
    atr4h: number;
    atr1hPercent: number;
    spreadToAtr1h: number;
    trendDirection1h: ForexTrendDirection;
    trendStrength1h: number;
    chopScore1h: number;
    shockFlag: boolean;
    nearestSupport: number | null;
    nearestResistance: number | null;
    distanceToSupportAtr1h: number | null;
    distanceToResistanceAtr1h: number | null;
    candles: {
        m5: any[];
        m15: any[];
        h1: any[];
        h4: any[];
        d1: any[];
    };
}

function safeNumber(value: unknown): number {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
}

function computeSlopePct(series: number[], lookback = 10): number {
    const values = series.filter((v) => Number.isFinite(v));
    if (values.length <= lookback) return 0;
    const last = values[values.length - 1] as number;
    const prev = values[values.length - 1 - lookback] as number;
    if (!Number.isFinite(last) || !Number.isFinite(prev) || last === 0) return 0;
    return ((last - prev) / last) * 100;
}

function countCrosses(seriesA: number[], seriesB: number[], lookback = 25): number {
    const len = Math.min(seriesA.length, seriesB.length);
    if (len < 3) return 0;
    const start = Math.max(1, len - lookback);
    let crosses = 0;
    for (let i = start; i < len; i += 1) {
        const prevDiff = seriesA[i - 1]! - seriesB[i - 1]!;
        const currDiff = seriesA[i]! - seriesB[i]!;
        if ((prevDiff <= 0 && currDiff > 0) || (prevDiff >= 0 && currDiff < 0)) crosses += 1;
    }
    return crosses;
}

function detectSessionTag(nowMs: number): ForexSessionTag {
    const hour = new Date(nowMs).getUTCHours();
    if (hour >= 7 && hour < 12) return 'LONDON';
    if (hour >= 12 && hour < 16) return 'OVERLAP';
    if (hour >= 16 && hour < 21) return 'NEW_YORK';
    if (hour >= 0 && hour < 7) return 'ASIA';
    return 'DEAD_HOURS';
}

function sessionFactor(sessionTag: ForexSessionTag): number {
    if (sessionTag === 'OVERLAP') return 1.2;
    if (sessionTag === 'LONDON' || sessionTag === 'NEW_YORK') return 1.0;
    if (sessionTag === 'ASIA') return 0.8;
    return 0.6;
}

function nearestLevels(candles4h: any[], price: number, atr1h: number, lookbackBars: number) {
    const window = candles4h.slice(-Math.max(20, lookbackBars));
    let support: number | null = null;
    let resistance: number | null = null;

    for (const candle of window) {
        const low = safeNumber(candle?.[3]);
        const high = safeNumber(candle?.[2]);
        if (low > 0 && low <= price) {
            if (support === null || low > support) support = low;
        }
        if (high > 0 && high >= price) {
            if (resistance === null || high < resistance) resistance = high;
        }
    }

    const distSupport = support !== null && atr1h > 0 ? Math.abs((price - support) / atr1h) : null;
    const distResistance = resistance !== null && atr1h > 0 ? Math.abs((resistance - price) / atr1h) : null;

    return { support, resistance, distSupport, distResistance };
}

export function toPairMetrics(state: ForexPairMarketState): ForexPairMetrics {
    return {
        pair: state.pair,
        epic: state.epic,
        sessionTag: state.sessionTag,
        price: state.price,
        spreadAbs: state.spreadAbs,
        spreadPips: state.spreadPips,
        spreadToAtr1h: state.spreadToAtr1h,
        atr1h: state.atr1h,
        atr4h: state.atr4h,
        atr1hPercent: state.atr1hPercent,
        trendStrength: state.trendStrength1h,
        chopScore: state.chopScore1h,
        shockFlag: state.shockFlag,
        timestampMs: state.nowMs,
    };
}

export function computeSelectorScore(metrics: ForexPairMetrics): number {
    const sessionMul = sessionFactor(metrics.sessionTag);
    const structureMul = Math.max(0.2, Math.min(1.6, metrics.trendStrength * (1 - metrics.chopScore)));
    const costEfficiency = 1 / Math.max(metrics.spreadToAtr1h, 1e-6);
    const volatilityMul = Math.max(0.05, metrics.atr1hPercent * 100);
    return costEfficiency * volatilityMul * sessionMul * structureMul;
}

export async function loadForexPairMarketState(pair: string, nowMs = Date.now()): Promise<ForexPairMarketState> {
    const normalizedPair = String(pair || '').trim().toUpperCase();
    const cfg = getForexStrategyConfig();
    const resolved = await resolveCapitalEpicRuntime(normalizedPair);

    const [quote, candles5m, candles15m, candles1h, candles4h, candles1d] = await Promise.all([
        fetchCapitalLivePrice(normalizedPair),
        fetchCapitalCandlesByEpic(resolved.epic, '5m', 260),
        fetchCapitalCandlesByEpic(resolved.epic, '15m', 220),
        fetchCapitalCandlesByEpic(resolved.epic, '1H', 260),
        fetchCapitalCandlesByEpic(resolved.epic, '4H', 260),
        fetchCapitalCandlesByEpic(resolved.epic, '1D', 220),
    ]);

    const price = safeNumber(quote.price);
    const bid = safeNumber(quote.bid);
    const offer = safeNumber(quote.offer);
    const spreadAbs = bid > 0 && offer > 0 ? Math.max(0, offer - bid) : 0;
    const spreadPips = pipSizeForPair(normalizedPair) > 0 ? spreadAbs / pipSizeForPair(normalizedPair) : 0;

    const closes1h = candles1h.map((c) => safeNumber(c?.[4])).filter((v) => v > 0);
    const ema50 = computeEMA(closes1h, 50);
    const ema200 = computeEMA(closes1h, 200);
    const ema20 = computeEMA(closes1h, 20);

    const ema50Last = ema50.at(-1) ?? closes1h.at(-1) ?? price;
    const ema200Last = ema200.at(-1) ?? closes1h.at(-1) ?? price;
    const emaSlope = computeSlopePct(ema50, 8);

    const trendDirection1h: ForexTrendDirection =
        price > ema50Last && ema50Last > ema200Last && emaSlope > 0
            ? 'up'
            : price < ema50Last && ema50Last < ema200Last && emaSlope < 0
              ? 'down'
              : 'neutral';

    const trendStrength1h = Math.max(0, Math.min(2, Math.abs((ema50Last - ema200Last) / Math.max(price, 1e-9)) * 1000));
    const crosses = countCrosses(ema20, ema50, 28);
    const chopScore1h = Math.max(0, Math.min(1, crosses / 8));

    const atr5m = computeATR(candles5m, 14);
    const atr1h = computeATR(candles1h, 14);
    const atr4h = computeATR(candles4h, 14);
    const atr1hPercent = price > 0 && atr1h > 0 ? atr1h / price : 0;
    const spreadToAtr1h = atr1h > 0 ? spreadAbs / atr1h : Number.POSITIVE_INFINITY;

    const last5m = candles5m.at(-1);
    const range5m = last5m ? Math.abs(safeNumber(last5m[2]) - safeNumber(last5m[3])) : 0;
    const shockFlag = atr5m > 0 ? range5m / atr5m >= cfg.risk.shockCandleAtr5m : false;

    const levels = nearestLevels(candles4h, price, atr1h, cfg.htf.supportResistanceLookbackBars);

    return {
        pair: normalizedPair,
        epic: resolved.epic,
        nowMs,
        sessionTag: detectSessionTag(nowMs),
        price,
        bid,
        offer,
        spreadAbs,
        spreadPips,
        atr5m,
        atr1h,
        atr4h,
        atr1hPercent,
        spreadToAtr1h,
        trendDirection1h,
        trendStrength1h,
        chopScore1h,
        shockFlag,
        nearestSupport: levels.support,
        nearestResistance: levels.resistance,
        distanceToSupportAtr1h: levels.distSupport,
        distanceToResistanceAtr1h: levels.distResistance,
        candles: {
            m5: candles5m,
            m15: candles15m,
            h1: candles1h,
            h4: candles4h,
            d1: candles1d,
        },
    };
}
