// lib/indicators.ts

import { bitgetFetch, resolveProductType } from './bitget';
import { CONTEXT_TIMEFRAME, MACRO_TIMEFRAME, MICRO_TIMEFRAME, PRIMARY_TIMEFRAME } from './constants';

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
    candleDepth?: Record<string, number | undefined>;
    sr?: Record<string, SRLevels | undefined>;
    metrics?: Record<string, TimeframeMetrics | undefined>;
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

export type StructureState = 'bull' | 'bear' | 'range';
export type ValueState = 'above_vah' | 'below_val' | 'inside_value';

export interface TimeframeMetrics {
    atr?: number;
    atrPctile?: number;
    rvol?: number;
    structure?: StructureState;
    bos?: boolean;
    bosDir?: 'up' | 'down' | null;
    structureBreakState?: 'above' | 'below' | 'inside';
    choch?: boolean;
    breakoutRetestOk?: boolean;
    breakoutRetestDir?: 'up' | 'down' | null;
    valueState?: ValueState;
    vah?: number;
    val?: number;
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
    if (!candles || candles.length < 2) return 0;
    const trs: number[] = [];
    for (let i = 1; i < candles.length; i++) {
        const high = Number(candles[i][2]);
        const low = Number(candles[i][3]);
        const prevClose = Number(candles[i - 1][4]);
        const tr = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));
        trs.push(tr);
    }
    const effectivePeriod = Math.min(period, trs.length);
    if (effectivePeriod <= 0) return 0;
    return trs.slice(-effectivePeriod).reduce((a, b) => a + b, 0) / effectivePeriod;
}

function computeAtrSeries(candles: any[], period = 14): number[] {
    if (!candles || candles.length < 2) return [];
    const trs: number[] = [];
    for (let i = 1; i < candles.length; i++) {
        const high = Number(candles[i][2]);
        const low = Number(candles[i][3]);
        const prevClose = Number(candles[i - 1][4]);
        const tr = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));
        trs.push(tr);
    }
    const effectivePeriod = Math.min(period, trs.length);
    if (effectivePeriod <= 0) return [];
    const atrs: number[] = [];
    let sum = 0;
    for (let i = 0; i < trs.length; i++) {
        sum += trs[i]!;
        if (i >= effectivePeriod) sum -= trs[i - effectivePeriod]!;
        if (i >= effectivePeriod - 1) atrs.push(sum / effectivePeriod);
    }
    return atrs.filter((v) => Number.isFinite(v));
}

function percentileRank(values: number[], current: number): number | undefined {
    if (!values.length || !Number.isFinite(current)) return undefined;
    const sorted = values.slice().sort((a, b) => a - b);
    let count = 0;
    for (const v of sorted) if (v <= current) count += 1;
    return (count / sorted.length) * 100;
}

function computeRvol(candles: any[], lookback = 20): number | undefined {
    if (!Array.isArray(candles) || candles.length <= lookback) return undefined;
    const vols = candles.map((c) => Number(c?.[5] ?? c?.volume)).filter((v) => Number.isFinite(v));
    if (vols.length <= lookback) return undefined;
    const current = vols[vols.length - 1]!;
    const window = vols.slice(vols.length - 1 - lookback, vols.length - 1);
    const avg = window.reduce((a, b) => a + b, 0) / window.length;
    if (!Number.isFinite(avg) || avg <= 0) return undefined;
    return current / avg;
}

function computeValueArea(candles: any[], binsCount = 24, valuePct = 0.7) {
    if (!Array.isArray(candles) || candles.length < 20) return undefined;
    const points = candles
        .map((c) => {
            const high = Number(c?.[2]);
            const low = Number(c?.[3]);
            const close = Number(c?.[4]);
            const vol = Number(c?.[5] ?? c?.volume);
            if (!Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close) || !Number.isFinite(vol)) {
                return null;
            }
            const typical = (high + low + close) / 3;
            return { price: typical, volume: Math.max(0, vol) };
        })
        .filter((p) => p !== null) as { price: number; volume: number }[];

    if (!points.length) return undefined;
    const prices = points.map((p) => p.price);
    const minP = Math.min(...prices);
    const maxP = Math.max(...prices);
    if (!Number.isFinite(minP) || !Number.isFinite(maxP) || maxP <= minP) return undefined;
    const binSize = (maxP - minP) / binsCount;
    if (!(binSize > 0)) return undefined;

    const bins = new Array<number>(binsCount).fill(0);
    let totalVol = 0;
    for (const p of points) {
        const idx = Math.min(binsCount - 1, Math.max(0, Math.floor((p.price - minP) / binSize)));
        bins[idx] = (bins[idx] || 0) + p.volume;
        totalVol += p.volume;
    }
    if (!(totalVol > 0)) return undefined;

    const ranked = bins
        .map((v, i) => ({ v, i }))
        .sort((a, b) => b.v - a.v);

    let acc = 0;
    let minIdx = binsCount - 1;
    let maxIdx = 0;
    for (const bin of ranked) {
        acc += bin.v;
        minIdx = Math.min(minIdx, bin.i);
        maxIdx = Math.max(maxIdx, bin.i);
        if (acc / totalVol >= valuePct) break;
    }

    const val = minP + minIdx * binSize;
    const vah = minP + (maxIdx + 1) * binSize;
    return { val, vah };
}

function computeStructureMetrics(candles: any[]): TimeframeMetrics {
    if (!Array.isArray(candles) || candles.length < 20) {
        return { structure: 'range', bos: false, choch: false, breakoutRetestOk: false };
    }
    const swings = computeSwingLevels(candles, 120);
    const highs = swings.filter((s) => s.type === 'high');
    const lows = swings.filter((s) => s.type === 'low');
    if (highs.length < 2 || lows.length < 2) {
        return { structure: 'range', bos: false, choch: false, breakoutRetestOk: false };
    }

    const lastHigh = highs[highs.length - 1]!;
    const prevHigh = highs[highs.length - 2]!;
    const lastLow = lows[lows.length - 1]!;
    const prevLow = lows[lows.length - 2]!;

    const atrLocal = computeATR(candles, 14);
    const highDelta = Math.abs(lastHigh.price - prevHigh.price);
    const lowDelta = Math.abs(lastLow.price - prevLow.price);
    const rangeLike = Number.isFinite(atrLocal) && atrLocal > 0 ? highDelta < 0.5 * atrLocal && lowDelta < 0.5 * atrLocal : false;

    const state: StructureState =
        rangeLike
            ? 'range'
            : lastHigh.price > prevHigh.price && lastLow.price > prevLow.price
            ? 'bull'
            : lastHigh.price < prevHigh.price && lastLow.price < prevLow.price
            ? 'bear'
            : 'range';

    const lastClose = Number(candles.at(-1)?.[4]);
    const prevClose = Number(candles.at(-2)?.[4]);
    const bosUp =
        Number.isFinite(prevClose) && Number.isFinite(lastClose) && prevClose <= lastHigh.price && lastClose > lastHigh.price;
    const bosDown =
        Number.isFinite(prevClose) && Number.isFinite(lastClose) && prevClose >= lastLow.price && lastClose < lastLow.price;
    const bos = state === 'bull' ? bosUp : state === 'bear' ? bosDown : bosUp || bosDown;
    const bosDir = bosUp ? 'up' : bosDown ? 'down' : null;
    const structureBreakState =
        Number.isFinite(lastClose) && lastClose > lastHigh.price
            ? 'above'
            : Number.isFinite(lastClose) && lastClose < lastLow.price
            ? 'below'
            : 'inside';
    const choch = state === 'bull' ? bosDown : state === 'bear' ? bosUp : false;

    const retestLookback = 10;
    const startIdx = Math.max(1, candles.length - retestLookback);
    let bosIdxUp: number | null = null;
    let bosIdxDown: number | null = null;
    for (let i = candles.length - 1; i >= startIdx; i--) {
        const closeNow = Number(candles[i]?.[4]);
        const closePrev = Number(candles[i - 1]?.[4]);
        if (Number.isFinite(closePrev) && Number.isFinite(closeNow) && closePrev <= lastHigh.price && closeNow > lastHigh.price) {
            bosIdxUp = i;
            break;
        }
    }
    for (let i = candles.length - 1; i >= startIdx; i--) {
        const closeNow = Number(candles[i]?.[4]);
        const closePrev = Number(candles[i - 1]?.[4]);
        if (Number.isFinite(closePrev) && Number.isFinite(closeNow) && closePrev >= lastLow.price && closeNow < lastLow.price) {
            bosIdxDown = i;
            break;
        }
    }

    const retestBuffer = Number.isFinite(atrLocal) && atrLocal > 0 ? atrLocal * 0.2 : 0;
    const hasUp = bosIdxUp !== null;
    const hasDown = bosIdxDown !== null;
    const breakoutDir =
        hasUp && hasDown
            ? (bosIdxUp as number) > (bosIdxDown as number)
                ? 'up'
                : 'down'
            : hasUp
            ? 'up'
            : hasDown
            ? 'down'
            : null;

    let breakoutRetestOk = false;
    if (breakoutDir === 'up' && bosIdxUp !== null && Number.isFinite(lastClose) && lastClose > lastHigh.price) {
        let minLow = Infinity;
        for (let i = bosIdxUp; i < candles.length; i++) {
            const low = Number(candles[i]?.[3]);
            if (Number.isFinite(low)) minLow = Math.min(minLow, low);
        }
        breakoutRetestOk = minLow <= lastHigh.price + retestBuffer;
    } else if (breakoutDir === 'down' && bosIdxDown !== null && Number.isFinite(lastClose) && lastClose < lastLow.price) {
        let maxHigh = -Infinity;
        for (let i = bosIdxDown; i < candles.length; i++) {
            const high = Number(candles[i]?.[2]);
            if (Number.isFinite(high)) maxHigh = Math.max(maxHigh, high);
        }
        breakoutRetestOk = maxHigh >= lastLow.price - retestBuffer;
    }
    const breakoutRetestDir = breakoutDir;

    return { structure: state, bos, bosDir, structureBreakState, choch, breakoutRetestOk, breakoutRetestDir };
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
    if (!Array.isArray(candles) || candles.length < 2) return undefined;
    const tf = String(timeframe || '').trim().toUpperCase();
    // Weekly candles are sparse on Bitget for newer listings; allow earlier S/R generation there.
    const minCandles = tf.endsWith('W') ? 8 : 20;
    if (candles.length < minCandles) return undefined;
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

    // Fallback for sparse/high-timeframe data where strict pivot detection yields no usable level.
    if (!nearestSupport || !nearestResistance) {
        const startIdx = Math.max(0, candles.length - Math.min(40, candles.length));
        let fallbackSupport: { price: number; idx: number } | null = null;
        let fallbackResistance: { price: number; idx: number } | null = null;
        for (let i = startIdx; i < candles.length; i++) {
            const low = Number(candles[i]?.[3]);
            const high = Number(candles[i]?.[2]);
            if (!nearestSupport && Number.isFinite(low) && low <= lastClose) {
                if (!fallbackSupport || low > fallbackSupport.price) fallbackSupport = { price: low, idx: i };
            }
            if (!nearestResistance && Number.isFinite(high) && high >= lastClose) {
                if (!fallbackResistance || high < fallbackResistance.price) fallbackResistance = { price: high, idx: i };
            }
        }
        if (!nearestSupport && fallbackSupport) nearestSupport = fallbackSupport;
        if (!nearestResistance && fallbackResistance) nearestResistance = fallbackResistance;
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
    const normalizeTimeframe = (tf: string) => {
        if (tf === '4D') return '1W';
        return tf;
    };
    const microTF = normalizeTimeframe(opts.micro || MICRO_TIMEFRAME);
    const macroTF = normalizeTimeframe(opts.macro || MACRO_TIMEFRAME);
    const primaryTF = normalizeTimeframe(opts.primary || PRIMARY_TIMEFRAME);
    const contextTF = normalizeTimeframe(opts.context || CONTEXT_TIMEFRAME);

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
    const summaries = new Map<
        string,
        { summary: string; atr: number; candleCount: number; sr?: SRLevels; metrics?: TimeframeMetrics }
    >();

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
        const atrSeries = computeAtrSeries(candles, 14);
        const atrPctile = percentileRank(atrSeries, atr);
        const rvol = computeRvol(candles, 20);
        const structureMetrics = computeStructureMetrics(candles);
        const valueArea = computeValueArea(candles, 24, 0.7);
        const lastClose = Number(candles.at(-1)?.[4]);
        const valueState: ValueState | undefined =
            valueArea && Number.isFinite(lastClose)
                ? lastClose > valueArea.vah
                    ? 'above_vah'
                    : lastClose < valueArea.val
                    ? 'below_val'
                    : 'inside_value'
                : undefined;

        // momentum slope gate (10-bar slope of EMA21)
        const momSlope = slopePct(ema21, 10); // % per bar

        return {
            summary: `VWAP=${vwap.toFixed(2)}, RSI=${rsi.toFixed(1)}, trend=${trend}, ATR=${atr.toFixed(
                2,
            )}, EMA9=${e9.toFixed(2)}, EMA21=${e21.toFixed(2)}, EMA20=${e20.toFixed(2)}, EMA50=${e50.toFixed(
                2,
            )}, SMA200=${s200.toFixed(2)}, slopeEMA21_10=${momSlope.toFixed(3)}%/bar`,
            atr,
            candleCount: candles.length,
            sr: computeSRLevels(candles, atr, tf),
            metrics: {
                atr,
                atrPctile,
                rvol,
                structure: structureMetrics.structure,
                bos: structureMetrics.bos,
                bosDir: structureMetrics.bosDir,
                structureBreakState: structureMetrics.structureBreakState,
                choch: structureMetrics.choch,
                breakoutRetestOk: structureMetrics.breakoutRetestOk,
                breakoutRetestDir: structureMetrics.breakoutRetestDir,
                valueState,
                vah: valueArea?.vah,
                val: valueArea?.val,
            },
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
        candleDepth: {},
        sr: {},
        metrics: {},
    };

    out.candleDepth![microTF] = summaries.get(microTF)?.candleCount;
    out.candleDepth![macroTF] = summaries.get(macroTF)?.candleCount;
    out.candleDepth![contextTF] = summaries.get(contextTF)?.candleCount;
    out.candleDepth![primaryTF] = summaries.get(primaryTF)?.candleCount;

    out.sr![microTF] = summaries.get(microTF)?.sr;
    out.sr![macroTF] = summaries.get(macroTF)?.sr;
    out.sr![contextTF] = summaries.get(contextTF)?.sr;
    out.metrics![microTF] = summaries.get(microTF)?.metrics;
    out.metrics![macroTF] = summaries.get(macroTF)?.metrics;
    out.metrics![contextTF] = summaries.get(contextTF)?.metrics;

    out.primary = {
        timeframe: primaryTF,
        summary: summaries.get(primaryTF)?.summary ?? summaries.get(microTF)?.summary ?? '',
    };
    out.sr![primaryTF] = summaries.get(primaryTF)?.sr;
    out.metrics![primaryTF] = summaries.get(primaryTF)?.metrics;

    out.context = {
        timeframe: contextTF,
        summary: summaries.get(contextTF)?.summary ?? summaries.get(macroTF)?.summary ?? '',
    };

    return out;
}
