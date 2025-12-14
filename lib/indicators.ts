// lib/indicators.ts

import { bitgetFetch, resolveProductType } from './bitget';

export interface IndicatorSummary {
    timeframe: string;
    summary: string;
}

export interface MultiTFIndicators {
    micro: string;
    macro: string;
    microTimeFrame: string;
    macroTimeFrame: string;
    primary?: IndicatorSummary;
    context?: IndicatorSummary;
    contextTimeFrame?: string;
    sr?: Record<string, SRLevels | undefined>;
}

export interface IndicatorTimeframeOptions {
    micro?: string;
    macro?: string;
    primary?: string;
    context?: string;
}

export type LevelState = 'at_level' | 'approaching' | 'rejected' | 'broken' | 'retesting';

export interface LevelDescriptor {
    price: number;
    dist_in_atr: number;
    level_strength: number;
    level_type: string;
    level_state: LevelState;
}

export interface SRLevels {
    timeframe: string;
    atr: number;
    support?: LevelDescriptor;
    resistance?: LevelDescriptor;
}

// ------------------------------
// Indicator Calculations
// ------------------------------

export function computeVWAP(candles: any[]): number {
    let cumPV = 0,
        cumVol = 0;
    for (const c of candles) {
        const high = parseFloat(c[2]);
        const low = parseFloat(c[3]);
        const close = parseFloat(c[4]);
        const volume = parseFloat(c[5]);
        const typical = (high + low + close) / 3;
        cumPV += typical * volume;
        cumVol += volume;
    }
    return cumVol > 0 ? cumPV / cumVol : 0;
}

// Wilder RSI (more standard)
export function computeRSI_Wilder(closes: number[], period = 14): number {
    if (closes.length <= period) return 50;
    let gains = 0,
        losses = 0;
    for (let i = 1; i <= period; i++) {
        const diff = closes[i]! - closes[i - 1]!;
        if (diff > 0) gains += diff;
        else losses -= diff;
    }
    let avgGain = gains / period;
    let avgLoss = losses / period;

    for (let i = period + 1; i < closes.length; i++) {
        const diff = closes[i]! - closes[i - 1]!;
        avgGain = (avgGain * (period - 1) + (diff > 0 ? diff : 0)) / period;
        avgLoss = (avgLoss * (period - 1) + (diff < 0 ? -diff : 0)) / period;
    }
    if (avgLoss === 0) return 100;
    const rs = avgGain / avgLoss;
    return 100 - 100 / (1 + rs);
}

// EMA with SMA seed
export function computeEMA(closes: number[], period: number): number[] {
    const ema: number[] = [];
    if (closes.length === 0) return ema;
    if (closes.length < period) {
        // seed with first close if too short
        ema[0] = closes[0]!;
    } else {
        const sma = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
        ema[period - 1] = sma;
    }
    const k = 2 / (period + 1);
    for (let i = ema[0] !== undefined ? 1 : period; i < closes.length; i++) {
        const prev = ema[i - 1] ?? closes[i - 1];
        ema[i] = closes[i]! * k + prev * (1 - k);
    }
    return ema;
}

// ATR (simple version)
export function computeATR(candles: any[], period = 14): number {
    if (!candles || candles.length < period + 1) return 0;
    const trs: number[] = [];
    for (let i = 1; i < candles.length; i++) {
        const high = Number(candles[i][2]);
        const low = Number(candles[i][3]);
        const prevClose = Number(candles[i - 1][4]);
        const tr = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));
        trs.push(tr);
    }
    if (trs.length < period) return 0;
    return trs.slice(-period).reduce((a, b) => a + b, 0) / period;
}

function ensureAscending(cs: any[]) {
    return cs.slice().sort((a: any, b: any) => Number(a[0]) - Number(b[0]));
}

export function computeSMA(closes: number[], period: number): number[] {
    const out: number[] = [];
    let sum = 0;
    for (let i = 0; i < closes.length; i++) {
        sum += closes[i]!;
        if (i >= period) sum -= closes[i - period]!;
        const window = Math.min(period, i + 1);
        out[i] = window > 0 ? sum / window : NaN;
    }
    return out;
}

function clamp(val: number, min: number, max: number) {
    return Math.min(Math.max(val, min), max);
}

function computeSwingLevels(candles: any[], lookback = 150) {
    const swings: { type: 'high' | 'low'; price: number; index: number }[] = [];
    const start = Math.max(2, candles.length - lookback);
    const end = candles.length - 2;
    for (let i = start; i < end; i++) {
        const high = Number(candles[i][2]);
        const low = Number(candles[i][3]);
        const prev1High = Number(candles[i - 1][2]);
        const prev2High = Number(candles[i - 2][2]);
        const next1High = Number(candles[i + 1][2]);
        const next2High = Number(candles[i + 2][2]);

        const prev1Low = Number(candles[i - 1][3]);
        const prev2Low = Number(candles[i - 2][3]);
        const next1Low = Number(candles[i + 1][3]);
        const next2Low = Number(candles[i + 2][3]);

        const isHigh = high > prev1High && high > prev2High && high >= next1High && high >= next2High;
        const isLow = low < prev1Low && low < prev2Low && low <= next1Low && low <= next2Low;

        if (isHigh) swings.push({ type: 'high', price: high, index: i });
        if (isLow) swings.push({ type: 'low', price: low, index: i });
    }
    return swings;
}

function deriveLevelState(price: number, levelPrice: number, atr: number, side: 'support' | 'resistance'): LevelState {
    if (!Number.isFinite(price) || !Number.isFinite(levelPrice) || !Number.isFinite(atr) || atr <= 0) return 'rejected';
    const diffAtr = (price - levelPrice) / atr;
    const near = Math.abs(diffAtr) <= 0.2;
    if (near) return 'at_level';

    if (side === 'support') {
        if (diffAtr < -0.3) return 'broken';
        if (diffAtr < 0) return 'retesting';
        if (diffAtr <= 0.6) return 'approaching';
        return 'rejected';
    }

    if (diffAtr > 0.3) return 'broken';
    if (diffAtr > 0) return 'retesting';
    if (diffAtr >= -0.6) return 'approaching';
    return 'rejected';
}

function computeSRLevels(candles: any[], atr: number, timeframe: string): SRLevels | undefined {
    if (!Array.isArray(candles) || candles.length < 20) return undefined;
    const lookback = 150;
    const swings = computeSwingLevels(candles, lookback);
    const lastClose = Number(candles.at(-1)?.[4]);
    if (!Number.isFinite(lastClose)) return undefined;

    let nearestSupport: { price: number; idx: number } | null = null;
    let nearestResistance: { price: number; idx: number } | null = null;

    for (const s of swings) {
        if (s.type === 'low' && s.price <= lastClose) {
            if (!nearestSupport || s.price > nearestSupport.price) nearestSupport = { price: s.price, idx: s.index };
        } else if (s.type === 'high' && s.price >= lastClose) {
            if (!nearestResistance || s.price < nearestResistance.price)
                nearestResistance = { price: s.price, idx: s.index };
        }
    }

    const levelFromSwing = (side: 'support' | 'resistance', level: { price: number; idx: number } | null) => {
        if (!level || !Number.isFinite(atr) || atr <= 0) return undefined;
        const distInAtr = Math.abs((lastClose - level.price) / atr);
        const barsAgo = candles.length - level.idx;
        const recencyStrength = clamp(1 - barsAgo / Math.max(lookback, 1), 0.2, 1);
        const level_state = deriveLevelState(lastClose, level.price, atr, side);
        return {
            price: Number(level.price.toFixed(4)),
            dist_in_atr: Number(distInAtr.toFixed(3)),
            level_strength: Number(recencyStrength.toFixed(3)),
            level_type: 'swing_pivot',
            level_state,
        };
    };

    const support = levelFromSwing('support', nearestSupport);
    const resistance = levelFromSwing('resistance', nearestResistance);

    // Avoid contradictory "at_level" on both sides; keep the closer one as at_level, downgrade the other to approaching.
    if (support?.level_state === 'at_level' && resistance?.level_state === 'at_level') {
        const supportDist = Math.abs(((lastClose - support.price) / atr));
        const resistanceDist = Math.abs(((lastClose - resistance.price) / atr));
        if (supportDist <= resistanceDist) {
            resistance.level_state = 'approaching';
        } else {
            support.level_state = 'approaching';
        }
    }

    return {
        timeframe,
        atr,
        support,
        resistance,
    };
}

// slope as pct per bar (uses last vs N bars ago)
export function slopePct(series: number[], lookback: number): number {
    const filtered = series.filter((v) => Number.isFinite(v));
    const n = filtered.length;
    if (n <= lookback) return 0;
    const last = filtered[n - 1]!;
    const prev = filtered[n - 1 - lookback]!;
    if (!isFinite(last) || !isFinite(prev) || last === 0) return 0;
    return ((last - prev) / last) * (100 / lookback); // % per bar
}

// ------------------------------
// Multi-Timeframe Indicators (FUTURES only)
// ------------------------------

export async function calculateMultiTFIndicators(
    symbol: string,
    opts: IndicatorTimeframeOptions = {},
): Promise<MultiTFIndicators> {
    const productType = resolveProductType(); // futures only
    const microTF = opts.micro || '15m';
    const macroTF = opts.macro || '4H';
    const primaryTF = opts.primary || '1H';
    const contextTF = opts.context || '1D';

    async function fetchCandles(tf: string) {
        const cs = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
            symbol,
            productType,
            granularity: tf,
            limit: 200,
        });
        return ensureAscending(cs);
    }

    const requests = new Map<string, Promise<any[]>>();
    const addRequest = (tf: string) => {
        if (!requests.has(tf)) requests.set(tf, fetchCandles(tf));
    };
    addRequest(microTF);
    addRequest(macroTF);
    addRequest(contextTF);
    addRequest(primaryTF);

    const entries = Array.from(requests.entries());
    const summaries = new Map<string, { summary: string; atr: number; sr?: SRLevels }>();

    const build = (candles: any[], tf: string) => {
        const closes = candles.map((c) => parseFloat(c[4]));
        const vwap = computeVWAP(candles);
        const rsi = computeRSI_Wilder(closes, 14);

        const ema9 = computeEMA(closes, 9);
        const ema21 = computeEMA(closes, 21);
        const ema20 = computeEMA(closes, 20);
        const ema50 = computeEMA(closes, 50);
        const sma200 = computeSMA(closes, 200);

        const e9 = ema9.at(-1)! ?? closes.at(-1)!;
        const e21 = ema21.at(-1)! ?? closes.at(-1)!;
        const e20 = ema20.at(-1)! ?? closes.at(-1)!;
        const e50 = ema50.at(-1)! ?? closes.at(-1)!;
        const s200 = sma200.at(-1)! ?? closes.at(-1)!;

        const trend = e20 > e50 ? 'up' : 'down';

        const atr = computeATR(candles, 14);

        // momentum slope gate (10-bar slope of EMA21)
        const momSlope = slopePct(ema21, 10); // % per bar

        return {
            summary: `VWAP=${vwap.toFixed(2)}, RSI=${rsi.toFixed(1)}, trend=${trend}, ATR=${atr.toFixed(
                2,
            )}, EMA9=${e9.toFixed(2)}, EMA21=${e21.toFixed(2)}, EMA20=${e20.toFixed(2)}, EMA50=${e50.toFixed(
                2,
            )}, SMA200=${s200.toFixed(2)}, slopeEMA21_10=${momSlope.toFixed(3)}%/bar`,
            atr,
            sr: computeSRLevels(candles, atr, tf),
        };
    };

    await Promise.all(
        entries.map(async ([tf, promise]) => {
            const candles = await promise;
            summaries.set(tf, build(candles, tf));
        }),
    );

    const out: MultiTFIndicators = {
        micro: summaries.get(microTF)?.summary ?? '',
        macro: summaries.get(macroTF)?.summary ?? '',
        microTimeFrame: microTF,
        macroTimeFrame: macroTF,
        contextTimeFrame: contextTF,
        sr: {},
    };

    out.sr![microTF] = summaries.get(microTF)?.sr;
    out.sr![macroTF] = summaries.get(macroTF)?.sr;
    out.sr![contextTF] = summaries.get(contextTF)?.sr;

    out.primary = {
        timeframe: primaryTF,
        summary: summaries.get(primaryTF)?.summary ?? summaries.get(microTF)?.summary ?? '',
    };
    out.sr![primaryTF] = summaries.get(primaryTF)?.sr;

    out.context = {
        timeframe: contextTF,
        summary: summaries.get(contextTF)?.summary ?? summaries.get(macroTF)?.summary ?? '',
    };

    return out;
}
