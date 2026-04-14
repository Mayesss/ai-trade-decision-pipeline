import crypto from 'crypto';

import { minuteOfDayInTimeZone } from '../sessions';
import type { ScalpCandle, ScalpEntrySessionProfile } from '../types';
import type { ScalpAdaptiveFeatureContext, ScalpAdaptiveTrainingRow } from './types';

function ts(candle: ScalpCandle): number {
  return candle[0];
}

function open(candle: ScalpCandle): number {
  return candle[1];
}

function high(candle: ScalpCandle): number {
  return candle[2];
}

function low(candle: ScalpCandle): number {
  return candle[3];
}

function close(candle: ScalpCandle): number {
  return candle[4];
}

function volume(candle: ScalpCandle): number {
  return candle[5];
}

function safeDiv(n: number, d: number): number {
  if (!(Number.isFinite(n) && Number.isFinite(d) && d !== 0)) return 0;
  return n / d;
}

function clip(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

// Incremental EMA/ATR: candle arrays only grow during replay.
// Cache by array identity + length so repeated calls with the same
// (growing) array extend from the previous result instead of recomputing.

const emaCacheMap = new WeakMap<number[], { period: number; out: number[] }>();

function computeEmaSeries(values: number[], period: number): number[] {
  if (!values.length) return [];
  const p = Math.max(1, Math.floor(period));
  const cached = emaCacheMap.get(values);
  let out: number[];
  let startIdx: number;
  if (cached && cached.period === p && cached.out.length <= values.length) {
    out = cached.out;
    startIdx = out.length;
  } else {
    out = [values[0] as number];
    startIdx = 1;
  }
  const k = 2 / (p + 1);
  for (let i = startIdx; i < values.length; i += 1) {
    out[i] = (values[i] as number) * k + (out[i - 1] as number) * (1 - k);
  }
  emaCacheMap.set(values, { period: p, out });
  return out;
}

const atrCacheMap = new WeakMap<ScalpCandle[], { period: number; out: number[]; tr: number[]; rolling: number }>();

function computeAtrSeries(candles: ScalpCandle[], period: number): number[] {
  if (candles.length < 2) return [];
  const p = Math.max(1, Math.floor(period));
  const cached = atrCacheMap.get(candles);
  let out: number[];
  let tr: number[];
  let rolling: number;
  let startIdx: number;
  if (cached && cached.period === p && cached.out.length <= candles.length) {
    out = cached.out;
    tr = cached.tr;
    rolling = cached.rolling;
    startIdx = out.length;
  } else {
    out = [0];
    tr = [0];
    rolling = 0;
    startIdx = 1;
  }
  for (let i = startIdx; i < candles.length; i += 1) {
    const prevClose = close(candles[i - 1] as ScalpCandle);
    const trVal = Math.max(
      high(candles[i] as ScalpCandle) - low(candles[i] as ScalpCandle),
      Math.abs(high(candles[i] as ScalpCandle) - prevClose),
      Math.abs(low(candles[i] as ScalpCandle) - prevClose),
    );
    tr[i] = trVal;
    rolling += trVal;
    if (i > p) rolling -= tr[i - p] as number;
    const divisor = Math.min(i, p);
    out[i] = divisor > 0 ? rolling / divisor : 0;
  }
  if (out.length > 1 && out[0] === 0) out[0] = out[1] || 0;
  atrCacheMap.set(candles, { period: p, out, tr, rolling });
  return out;
}

function computeAdx(candles: ScalpCandle[], period: number): number {
  const p = Math.max(2, Math.floor(period));
  if (candles.length < p + 2) return 0;

  const tr = new Array<number>(candles.length).fill(0);
  const plusDm = new Array<number>(candles.length).fill(0);
  const minusDm = new Array<number>(candles.length).fill(0);

  for (let i = 1; i < candles.length; i += 1) {
    const upMove = high(candles[i] as ScalpCandle) - high(candles[i - 1] as ScalpCandle);
    const downMove = low(candles[i - 1] as ScalpCandle) - low(candles[i] as ScalpCandle);
    plusDm[i] = upMove > downMove && upMove > 0 ? upMove : 0;
    minusDm[i] = downMove > upMove && downMove > 0 ? downMove : 0;
    tr[i] = Math.max(
      high(candles[i] as ScalpCandle) - low(candles[i] as ScalpCandle),
      Math.abs(high(candles[i] as ScalpCandle) - close(candles[i - 1] as ScalpCandle)),
      Math.abs(low(candles[i] as ScalpCandle) - close(candles[i - 1] as ScalpCandle)),
    );
  }

  let trSmooth = 0;
  let plusSmooth = 0;
  let minusSmooth = 0;
  for (let i = 1; i <= p && i < candles.length; i += 1) {
    trSmooth += tr[i] as number;
    plusSmooth += plusDm[i] as number;
    minusSmooth += minusDm[i] as number;
  }
  if (!(trSmooth > 0)) return 0;

  const dxRows: number[] = [];
  for (let i = p + 1; i < candles.length; i += 1) {
    trSmooth = trSmooth - trSmooth / p + (tr[i] as number);
    plusSmooth = plusSmooth - plusSmooth / p + (plusDm[i] as number);
    minusSmooth = minusSmooth - minusSmooth / p + (minusDm[i] as number);
    if (!(trSmooth > 0)) continue;
    const plusDi = (100 * plusSmooth) / trSmooth;
    const minusDi = (100 * minusSmooth) / trSmooth;
    const diSum = plusDi + minusDi;
    if (!(diSum > 0)) continue;
    const dx = (100 * Math.abs(plusDi - minusDi)) / diSum;
    if (Number.isFinite(dx)) dxRows.push(dx);
  }
  if (!dxRows.length) return 0;
  const take = Math.min(dxRows.length, p);
  return dxRows.slice(-take).reduce((acc, v) => acc + v, 0) / take;
}

function percentileRank(values: number[], current: number): number {
  if (!values.length || !Number.isFinite(current)) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  let count = 0;
  for (const v of sorted) {
    if (v <= current) count += 1;
  }
  return (count / sorted.length) * 100;
}

function profileTimeZone(profile: ScalpEntrySessionProfile): string {
  if (profile === 'tokyo') return 'Asia/Tokyo';
  if (profile === 'newyork') return 'America/New_York';
  if (profile === 'pacific') return 'America/Los_Angeles';
  if (profile === 'sydney') return 'Australia/Sydney';
  return 'Europe/Berlin';
}

function hashTokens(tokens: string[]): string {
  return crypto.createHash('sha1').update(tokens.join('|')).digest('hex').slice(0, 20);
}

function bodyRangeToken(candle: ScalpCandle): string {
  const range = Math.max(1e-9, high(candle) - low(candle));
  const body = Math.abs(close(candle) - open(candle));
  const bodyRatio = body / range;
  const upperWick = high(candle) - Math.max(open(candle), close(candle));
  const lowerWick = Math.min(open(candle), close(candle)) - low(candle);
  const wickSkew = safeDiv(upperWick - lowerWick, range);
  const bodyBucket = bodyRatio < 0.3 ? 'S' : bodyRatio < 0.6 ? 'M' : 'L';
  const skewBucket = wickSkew > 0.2 ? 'UP' : wickSkew < -0.2 ? 'DOWN' : 'NEUTRAL';
  return `SHAPE:BODY_${bodyBucket}:WICK_${skewBucket}`;
}

function structureToken(confirmCandles: ScalpCandle[]): string {
  if (confirmCandles.length < 24) return 'STRUCT:INSUFFICIENT';
  const now = confirmCandles[confirmCandles.length - 1] as ScalpCandle;
  const prev = confirmCandles[confirmCandles.length - 2] as ScalpCandle;
  const lookback = confirmCandles.slice(-22, -2);
  const swingHigh = Math.max(...lookback.map(high));
  const swingLow = Math.min(...lookback.map(low));
  const c = close(now);
  const prevClose = close(prev);
  if (c > swingHigh) return 'STRUCT:BREAK_UP';
  if (c < swingLow) return 'STRUCT:BREAK_DOWN';
  const mid = (swingHigh + swingLow) / 2;
  if (prevClose <= mid && c > mid) return 'STRUCT:RECLAIM_UP';
  if (prevClose >= mid && c < mid) return 'STRUCT:RECLAIM_DOWN';
  return 'STRUCT:INSIDE';
}

export function aggregateCandlesToTimeframe(candles: ScalpCandle[], timeframeMinutes: number): ScalpCandle[] {
  const timeframeMs = Math.max(1, Math.floor(timeframeMinutes)) * 60_000;
  if (!candles.length) return [];
  const sorted = candles.slice().sort((a, b) => ts(a) - ts(b));
  const out: ScalpCandle[] = [];
  let currentBucket = -1;
  let agg: ScalpCandle | null = null;

  for (const candle of sorted) {
    const bucket = Math.floor(ts(candle) / timeframeMs) * timeframeMs;
    if (!agg || bucket !== currentBucket) {
      if (agg) out.push(agg);
      currentBucket = bucket;
      agg = [bucket, open(candle), high(candle), low(candle), close(candle), volume(candle)];
      continue;
    }
    agg[2] = Math.max(agg[2], high(candle));
    agg[3] = Math.min(agg[3], low(candle));
    agg[4] = close(candle);
    agg[5] = (agg[5] || 0) + (volume(candle) || 0);
  }
  if (agg) out.push(agg);
  return out;
}

// ---------------------------------------------------------------------------
// Incremental feature state — avoids O(n) recomputation on every tick.
// EMA updates are O(1), ADX/ATR maintain rolling smoothed values.
// ---------------------------------------------------------------------------

export interface IncrementalFeatureState {
  // Base candle tracking
  baseCandlesProcessed: number;
  ema50: number;
  ema200: number;
  // ADX Wilder smoothing state
  adxTrSmooth: number;
  adxPlusSmooth: number;
  adxMinusSmooth: number;
  adxDxRing: number[];       // ring buffer of recent DX values
  adxDxRingIdx: number;
  adxInitCount: number;      // candles processed during ADX init phase
  adxPeriod: number;
  adxValue: number;
  prevBaseCandle: ScalpCandle | null;
  // Confirm candle ATR tracking
  confirmCandlesProcessed: number;
  atrRollingSum: number;
  atrTrRing: number[];       // ring buffer of TR values for SMA
  atrTrRingIdx: number;
  atrPeriod: number;
  atrCurrent: number;
  atrRecentRing: number[];   // ring buffer of recent ATR values for percentile
  atrRecentRingIdx: number;
  atrRecentCount: number;
  prevConfirmCandle: ScalpCandle | null;
}

export function createIncrementalFeatureState(): IncrementalFeatureState {
  const adxPeriod = 14;
  const atrPeriod = 14;
  const atrLookback = 200;
  return {
    baseCandlesProcessed: 0,
    ema50: 0,
    ema200: 0,
    adxTrSmooth: 0,
    adxPlusSmooth: 0,
    adxMinusSmooth: 0,
    adxDxRing: new Array(adxPeriod).fill(0),
    adxDxRingIdx: 0,
    adxInitCount: 0,
    adxPeriod,
    adxValue: 0,
    prevBaseCandle: null,
    confirmCandlesProcessed: 0,
    atrRollingSum: 0,
    atrTrRing: new Array(atrPeriod).fill(0),
    atrTrRingIdx: 0,
    atrPeriod,
    atrCurrent: 0,
    atrRecentRing: new Array(atrLookback).fill(0),
    atrRecentRingIdx: 0,
    atrRecentCount: 0,
    prevConfirmCandle: null,
  };
}

function updateBaseIndicators(state: IncrementalFeatureState, baseCandles: ScalpCandle[]): void {
  const k50 = 2 / 51;
  const k200 = 2 / 201;
  const p = state.adxPeriod;

  for (let i = state.baseCandlesProcessed; i < baseCandles.length; i += 1) {
    const candle = baseCandles[i]!;
    const c = close(candle);

    // EMA update
    if (i === 0) {
      state.ema50 = c;
      state.ema200 = c;
    } else {
      state.ema50 = c * k50 + state.ema50 * (1 - k50);
      state.ema200 = c * k200 + state.ema200 * (1 - k200);
    }

    // ADX update
    if (state.prevBaseCandle) {
      const upMove = high(candle) - high(state.prevBaseCandle);
      const downMove = low(state.prevBaseCandle) - low(candle);
      const pDm = upMove > downMove && upMove > 0 ? upMove : 0;
      const mDm = downMove > upMove && downMove > 0 ? downMove : 0;
      const tr = Math.max(
        high(candle) - low(candle),
        Math.abs(high(candle) - close(state.prevBaseCandle)),
        Math.abs(low(candle) - close(state.prevBaseCandle)),
      );

      state.adxInitCount += 1;
      if (state.adxInitCount <= p) {
        // Accumulating initial sums
        state.adxTrSmooth += tr;
        state.adxPlusSmooth += pDm;
        state.adxMinusSmooth += mDm;
      } else {
        // Wilder smoothing
        state.adxTrSmooth = state.adxTrSmooth - state.adxTrSmooth / p + tr;
        state.adxPlusSmooth = state.adxPlusSmooth - state.adxPlusSmooth / p + pDm;
        state.adxMinusSmooth = state.adxMinusSmooth - state.adxMinusSmooth / p + mDm;

        if (state.adxTrSmooth > 0) {
          const plusDi = (100 * state.adxPlusSmooth) / state.adxTrSmooth;
          const minusDi = (100 * state.adxMinusSmooth) / state.adxTrSmooth;
          const diSum = plusDi + minusDi;
          if (diSum > 0) {
            const dx = (100 * Math.abs(plusDi - minusDi)) / diSum;
            if (Number.isFinite(dx)) {
              state.adxDxRing[state.adxDxRingIdx % p] = dx;
              state.adxDxRingIdx += 1;
              const count = Math.min(state.adxDxRingIdx, p);
              let sum = 0;
              for (let j = 0; j < count; j += 1) sum += state.adxDxRing[j]!;
              state.adxValue = sum / count;
            }
          }
        }
      }
    }
    state.prevBaseCandle = candle;
  }
  state.baseCandlesProcessed = baseCandles.length;
}

function updateConfirmIndicators(state: IncrementalFeatureState, confirmCandles: ScalpCandle[]): void {
  const p = state.atrPeriod;

  for (let i = state.confirmCandlesProcessed; i < confirmCandles.length; i += 1) {
    const candle = confirmCandles[i]!;
    if (state.prevConfirmCandle) {
      const prevC = close(state.prevConfirmCandle);
      const tr = Math.max(
        high(candle) - low(candle),
        Math.abs(high(candle) - prevC),
        Math.abs(low(candle) - prevC),
      );

      // SMA-based ATR using ring buffer
      const oldTr = state.atrTrRing[state.atrTrRingIdx % p]!;
      state.atrTrRing[state.atrTrRingIdx % p] = tr;
      state.atrTrRingIdx += 1;
      const filled = Math.min(state.atrTrRingIdx, p);
      if (state.atrTrRingIdx <= p) {
        state.atrRollingSum += tr;
      } else {
        state.atrRollingSum += tr - oldTr;
      }
      state.atrCurrent = filled > 0 ? state.atrRollingSum / filled : 0;

      // Track recent ATR values for percentile rank
      state.atrRecentRing[state.atrRecentRingIdx % state.atrRecentRing.length] = state.atrCurrent;
      state.atrRecentRingIdx += 1;
      state.atrRecentCount = Math.min(state.atrRecentCount + 1, state.atrRecentRing.length);
    }
    state.prevConfirmCandle = candle;
  }
  state.confirmCandlesProcessed = confirmCandles.length;
}

function incrementalPercentileRank(ring: number[], count: number, current: number): number {
  if (count === 0 || !Number.isFinite(current)) return 0;
  let below = 0;
  for (let i = 0; i < count; i += 1) {
    if (ring[i]! <= current) below += 1;
  }
  return (below / count) * 100;
}

export function deriveAdaptiveFeatureContext(params: {
  baseCandles: ScalpCandle[];
  confirmCandles: ScalpCandle[];
  nowMs: number;
  entrySessionProfile: ScalpEntrySessionProfile;
  incrementalState?: IncrementalFeatureState;
}): ScalpAdaptiveFeatureContext {
  const baseCandles = params.baseCandles;
  const confirmCandles = params.confirmCandles;
  const inc = params.incrementalState;

  let e50: number;
  let e200: number;
  let adx: number;
  let atrCurrent: number;
  let atrRank: number;

  if (inc) {
    // Incremental path — O(new candles) instead of O(all candles)
    updateBaseIndicators(inc, baseCandles);
    updateConfirmIndicators(inc, confirmCandles);
    e50 = inc.ema50;
    e200 = inc.ema200;
    adx = inc.adxValue;
    atrCurrent = inc.atrCurrent;
    atrRank = incrementalPercentileRank(inc.atrRecentRing, inc.atrRecentCount, atrCurrent);
  } else {
    // Original full-recomputation path (used by training, tests, etc.)
    const closes = baseCandles.map(close);
    const ema50 = computeEmaSeries(closes, 50);
    const ema200 = computeEmaSeries(closes, 200);
    const lastBase = baseCandles[baseCandles.length - 1] || null;
    const lastClose = lastBase ? close(lastBase) : 0;
    e50 = ema50.length ? (ema50[ema50.length - 1] as number) : lastClose;
    e200 = ema200.length ? (ema200[ema200.length - 1] as number) : lastClose;
    adx = computeAdx(baseCandles, 14);
    const confirmAtr = computeAtrSeries(confirmCandles, 14);
    atrCurrent = confirmAtr.length ? (confirmAtr[confirmAtr.length - 1] as number) : 0;
    const atrLookback = confirmAtr.slice(Math.max(0, confirmAtr.length - 200));
    atrRank = percentileRank(atrLookback, atrCurrent);
  }

  const regimeRelation = e50 > e200 * 1.0001 ? 'BULL' : e50 < e200 * 0.9999 ? 'BEAR' : 'FLAT';
  const adxBucket = adx < 18 ? 'LOW' : adx < 25 ? 'MID' : 'HIGH';
  const regimeToken = `REGIME:${regimeRelation}:ADX_${adxBucket}`;

  const volBucket = atrRank < 33 ? 'LOW' : atrRank < 66 ? 'MID' : 'HIGH';
  const volToken = `VOL:ATR_PCTL_${volBucket}`;

  const shapeToken = confirmCandles.length
    ? bodyRangeToken(confirmCandles[confirmCandles.length - 1] as ScalpCandle)
    : 'SHAPE:EMPTY';
  const structToken = structureToken(confirmCandles);

  const minuteOfDay = minuteOfDayInTimeZone(params.nowMs, profileTimeZone(params.entrySessionProfile));
  const safeMinute = minuteOfDay >= 0 ? minuteOfDay : 0;
  const quarterHourBucket = Math.floor(safeMinute / 15);
  const sessionToken = `SESSION:${params.entrySessionProfile.toUpperCase()}:Q${quarterHourBucket}`;

  const tokens = [regimeToken, volToken, shapeToken, structToken, sessionToken];
  return {
    tsMs: params.nowMs,
    tokens,
    featureHash: hashTokens(tokens),
    tokenMap: {
      regimeToken,
      volToken,
      shapeToken,
      structToken,
      sessionToken,
    },
    quarterHourBucket,
  };
}

export function buildAdaptiveTrainingRowsFromMinuteCandles(params: {
  candles1m: ScalpCandle[];
  symbol: string;
  entrySessionProfile: ScalpEntrySessionProfile;
  lookaheadBars?: number;
}): ScalpAdaptiveTrainingRow[] {
  const candles1m = params.candles1m.slice().sort((a, b) => ts(a) - ts(b));
  if (candles1m.length < 500) return [];

  const baseCandles = aggregateCandlesToTimeframe(candles1m, 15);
  const confirmCandles = aggregateCandlesToTimeframe(candles1m, 3);
  if (baseCandles.length < 240 || confirmCandles.length < 300) return [];

  const atrBase = computeAtrSeries(baseCandles, 14);
  const lookahead = Math.max(1, Math.min(48, Math.floor(Number(params.lookaheadBars) || 4)));
  const rows: ScalpAdaptiveTrainingRow[] = [];

  let confirmCursor = 0;
  let confirmClosed: ScalpCandle[] = [];
  for (let i = 220; i < baseCandles.length - lookahead; i += 1) {
    const baseNow = baseCandles[i] as ScalpCandle;
    const nowMs = ts(baseNow) + 15 * 60_000;

    while (confirmCursor < confirmCandles.length && ts(confirmCandles[confirmCursor] as ScalpCandle) < nowMs) {
      confirmClosed.push(confirmCandles[confirmCursor] as ScalpCandle);
      confirmCursor += 1;
    }

    if (confirmClosed.length < 240) continue;
    const baseSlice = baseCandles.slice(0, i + 1);
    const confirmSlice = confirmClosed.slice(Math.max(0, confirmClosed.length - 320));
    const context = deriveAdaptiveFeatureContext({
      baseCandles: baseSlice,
      confirmCandles: confirmSlice,
      nowMs,
      entrySessionProfile: params.entrySessionProfile,
    });

    const currentClose = close(baseNow);
    const futureClose = close(baseCandles[i + lookahead] as ScalpCandle);
    const atr = Math.max(atrBase[i] || 0, Math.abs(currentClose) * 1e-6);
    const proxyR = clip((futureClose - currentClose) / atr, -6, 6);

    rows.push({
      ...context,
      symbol: params.symbol,
      entrySessionProfile: params.entrySessionProfile,
      proxyR,
      positive: proxyR > 0,
    });
  }

  return rows;
}
