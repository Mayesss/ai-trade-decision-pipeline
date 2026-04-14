import type { ScalpCandle, ScalpDirectionalBias, ScalpSessionState, ScalpStrategyConfig } from '../types';
import type { ScalpStrategyEntryIntent, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';

export type SyntheticSignal = {
    direction: ScalpDirectionalBias;
    signalTsMs: number;
    entryPrice: number;
    stopAnchor: number;
    zoneLow: number;
    zoneHigh: number;
    reasonCodes: string[];
};

export function ts(candle: ScalpCandle): number {
    return candle[0];
}

export function open(candle: ScalpCandle): number {
    return candle[1];
}

export function high(candle: ScalpCandle): number {
    return candle[2];
}

export function low(candle: ScalpCandle): number {
    return candle[3];
}

export function close(candle: ScalpCandle): number {
    return candle[4];
}

export function candleRange(candle: ScalpCandle): number {
    return Math.max(0, high(candle) - low(candle));
}

export function candleBody(candle: ScalpCandle): number {
    return Math.abs(close(candle) - open(candle));
}

// Incremental EMA/ATR: during replay, candle arrays only grow (push-only).
// We cache the previous result and extend from where we left off.
// This turns O(n) per tick into O(new candles) — typically O(1).

// Cache close-value extraction so the same array reference is reused across ticks.
// This allows the EMA WeakMap to hit on subsequent calls with the growing candle array.
const closesCache = new WeakMap<ScalpCandle[], number[]>();

export function extractCloses(candles: ScalpCandle[]): number[] {
    let cached = closesCache.get(candles);
    if (cached && cached.length === candles.length) return cached;
    if (!cached) cached = [];
    for (let i = cached.length; i < candles.length; i += 1) {
        cached[i] = close(candles[i]!);
    }
    closesCache.set(candles, cached);
    return cached;
}

const emaCache = new WeakMap<number[], { period: number; out: number[] }>();

export function computeEmaSeries(values: number[], period: number): number[] {
    const p = Math.max(1, Math.floor(period));
    if (!values.length) return [];
    const cached = emaCache.get(values);
    let out: number[];
    let startIdx: number;
    if (cached && cached.period === p && cached.out.length <= values.length) {
        out = cached.out;
        startIdx = out.length;
    } else {
        out = [values[0]!];
        startIdx = 1;
    }
    const k = 2 / (p + 1);
    for (let i = startIdx; i < values.length; i += 1) {
        out[i] = values[i]! * k + out[i - 1]! * (1 - k);
    }
    emaCache.set(values, { period: p, out });
    return out;
}

const atrCache = new WeakMap<ScalpCandle[], { period: number; out: number[]; tr: number[]; rolling: number }>();

export function computeAtrSeries(candles: ScalpCandle[], period: number): number[] {
    if (!Array.isArray(candles) || candles.length < 2) return [];
    const p = Math.max(1, Math.floor(period));
    const cached = atrCache.get(candles);
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
        const prevClose = close(candles[i - 1]!);
        const trVal = Math.max(
            high(candles[i]!) - low(candles[i]!),
            Math.abs(high(candles[i]!) - prevClose),
            Math.abs(low(candles[i]!) - prevClose),
        );
        tr[i] = trVal;
        rolling += trVal;
        if (i > p) rolling -= tr[i - p]!;
        const divisor = Math.min(i, p);
        out[i] = divisor > 0 ? rolling / divisor : 0;
    }
    if (out.length > 1 && out[0] === 0) out[0] = out[1] ?? 0;
    atrCache.set(candles, { period: p, out, tr, rolling });
    return out;
}

// Incremental ADX with WeakMap cache — avoids O(n) recomputation per tick.
const adxCache = new WeakMap<ScalpCandle[], {
  period: number;
  processedLen: number;
  trSmooth: number;
  plusSmooth: number;
  minusSmooth: number;
  initCount: number;
  dxRing: number[];
  dxRingIdx: number;
  value: number;
  prevCandle: ScalpCandle | null;
}>();

export function computeAdx(candles: ScalpCandle[], period: number): number {
    const p = Math.max(2, Math.floor(period));
    if (!Array.isArray(candles) || candles.length < p + 2) return 0;

    let state = adxCache.get(candles);
    let startIdx: number;

    if (state && state.period === p && state.processedLen <= candles.length) {
        startIdx = state.processedLen;
    } else {
        state = {
            period: p,
            processedLen: 0,
            trSmooth: 0,
            plusSmooth: 0,
            minusSmooth: 0,
            initCount: 0,
            dxRing: new Array(p).fill(0),
            dxRingIdx: 0,
            value: 0,
            prevCandle: null,
        };
        startIdx = 0;
    }

    for (let i = startIdx; i < candles.length; i += 1) {
        const candle = candles[i]!;
        if (state.prevCandle) {
            const upMove = high(candle) - high(state.prevCandle);
            const downMove = low(state.prevCandle) - low(candle);
            const pDm = upMove > downMove && upMove > 0 ? upMove : 0;
            const mDm = downMove > upMove && downMove > 0 ? downMove : 0;
            const trVal = Math.max(
                high(candle) - low(candle),
                Math.abs(high(candle) - close(state.prevCandle)),
                Math.abs(low(candle) - close(state.prevCandle)),
            );

            state.initCount += 1;
            if (state.initCount <= p) {
                state.trSmooth += trVal;
                state.plusSmooth += pDm;
                state.minusSmooth += mDm;
            } else {
                state.trSmooth = state.trSmooth - state.trSmooth / p + trVal;
                state.plusSmooth = state.plusSmooth - state.plusSmooth / p + pDm;
                state.minusSmooth = state.minusSmooth - state.minusSmooth / p + mDm;

                if (state.trSmooth > 0) {
                    const plusDi = (100 * state.plusSmooth) / state.trSmooth;
                    const minusDi = (100 * state.minusSmooth) / state.trSmooth;
                    const diSum = plusDi + minusDi;
                    if (diSum > 0) {
                        const dx = (100 * Math.abs(plusDi - minusDi)) / diSum;
                        if (Number.isFinite(dx)) {
                            state.dxRing[state.dxRingIdx % p] = dx;
                            state.dxRingIdx += 1;
                            const count = Math.min(state.dxRingIdx, p);
                            let sum = 0;
                            for (let j = 0; j < count; j += 1) sum += state.dxRing[j]!;
                            state.value = sum / count;
                        }
                    }
                }
            }
        }
        state.prevCandle = candle;
    }
    state.processedLen = candles.length;
    adxCache.set(candles, state);
    return state.value;
}

export function mean(values: number[]): number {
    if (!values.length) return 0;
    return values.reduce((acc, value) => acc + value, 0) / values.length;
}

export function median(values: number[]): number {
    if (!values.length) return 0;
    const sorted = values.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 0) return (sorted[mid - 1]! + sorted[mid]!) / 2;
    return sorted[mid]!;
}

export function stddev(values: number[]): number {
    if (values.length < 2) return 0;
    const m = mean(values);
    const variance = values.reduce((acc, value) => {
        const d = value - m;
        return acc + d * d;
    }, 0) / (values.length - 1);
    return variance > 0 ? Math.sqrt(variance) : 0;
}

function dedupeReasonCodes(codes: string[]): string[] {
    return Array.from(new Set(codes.map((code) => String(code || '').trim().toUpperCase()).filter((code) => code.length > 0)));
}

function latestTs(candles: ScalpCandle[]): number | null {
    const latest = candles.at(-1)?.[0];
    return Number.isFinite(Number(latest)) ? Number(latest) : null;
}

export function withLastProcessed(state: ScalpSessionState, input: ScalpStrategyPhaseInput['market']): ScalpSessionState {
    const next: ScalpSessionState = {
        ...state,
        lastProcessed: {
            ...state.lastProcessed,
        },
    };
    const baseTs = latestTs(input.baseCandles);
    const confirmTs = latestTs(input.confirmCandles);

    if (input.baseTf === 'M1') next.lastProcessed.m1ClosedTsMs = baseTs;
    if (input.baseTf === 'M3') next.lastProcessed.m3ClosedTsMs = baseTs;
    if (input.baseTf === 'M5') next.lastProcessed.m5ClosedTsMs = baseTs;
    if (input.baseTf === 'M15') next.lastProcessed.m15ClosedTsMs = baseTs;

    if (input.confirmTf === 'M1') next.lastProcessed.m1ClosedTsMs = confirmTs;
    if (input.confirmTf === 'M3') next.lastProcessed.m3ClosedTsMs = confirmTs;
    return next;
}

export function finalizePhase(params: {
    state: ScalpSessionState;
    reasonCodes: string[];
    entryIntent?: ScalpStrategyEntryIntent | null;
}): ScalpStrategyPhaseOutput {
    return {
        state: params.state,
        reasonCodes: dedupeReasonCodes(params.reasonCodes),
        entryIntent: params.entryIntent ?? null,
    };
}

export function toSyntheticSignalPhase(params: {
    state: ScalpSessionState;
    cfg: ScalpStrategyConfig;
    signal: SyntheticSignal;
    extraReasonCodes?: string[];
}): ScalpStrategyPhaseOutput {
    const signal = params.signal;
    const side = signal.direction === 'BULLISH' ? 'SELL_SIDE' : 'BUY_SIDE';
    const entry = Number(signal.entryPrice);
    const stop = Number(signal.stopAnchor);
    const zoneLow = Math.min(signal.zoneLow, signal.zoneHigh);
    const zoneHigh = Math.max(signal.zoneLow, signal.zoneHigh);
    if (!(Number.isFinite(entry) && entry > 0 && Number.isFinite(stop) && stop > 0)) {
        return finalizePhase({
            state: params.state,
            reasonCodes: ['SYNTHETIC_SIGNAL_INVALID_PRICES'],
        });
    }
    if (signal.direction === 'BULLISH' && !(stop < entry)) {
        return finalizePhase({
            state: params.state,
            reasonCodes: ['SYNTHETIC_SIGNAL_STOP_NOT_PROTECTIVE_LONG'],
        });
    }
    if (signal.direction === 'BEARISH' && !(stop > entry)) {
        return finalizePhase({
            state: params.state,
            reasonCodes: ['SYNTHETIC_SIGNAL_STOP_NOT_PROTECTIVE_SHORT'],
        });
    }
    if (!(Number.isFinite(zoneLow) && Number.isFinite(zoneHigh) && zoneHigh > zoneLow && zoneLow > 0)) {
        return finalizePhase({
            state: params.state,
            reasonCodes: ['SYNTHETIC_SIGNAL_INVALID_ZONE'],
        });
    }

    const next: ScalpSessionState = {
        ...params.state,
        state: 'WAITING_RETRACE',
        sweep: {
            side,
            sweepTsMs: signal.signalTsMs,
            sweepPrice: stop,
            bufferAbs: Math.max(1e-9, Math.abs(entry - stop) * 0.2),
            rejected: true,
            rejectedTsMs: signal.signalTsMs,
            reasonCodes: ['SYNTHETIC_SWEEP_PROXY'],
        },
        ifvg: {
            direction: signal.direction,
            low: zoneLow,
            high: zoneHigh,
            createdTsMs: signal.signalTsMs,
            expiresAtMs: signal.signalTsMs + Math.max(1, params.cfg.ifvg.ttlMinutes) * 60_000,
            entryMode: params.cfg.ifvg.entryMode,
            touched: true,
        },
        confirmation: {
            displacementDetected: true,
            displacementTsMs: signal.signalTsMs,
            structureShiftDetected: true,
            structureShiftTsMs: signal.signalTsMs,
            reasonCodes: ['SYNTHETIC_CONFIRM_PROXY'],
        },
    };

    return finalizePhase({
        state: next,
        reasonCodes: [
            ...signal.reasonCodes,
            ...(params.extraReasonCodes || []),
            'SYNTHETIC_IFVG_READY',
            'ENTRY_SIGNAL_READY',
        ],
        entryIntent: { model: 'ifvg_touch' },
    });
}
