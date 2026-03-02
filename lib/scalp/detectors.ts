import type {
    ScalpAsiaRangeSnapshot,
    ScalpCandle,
    ScalpConfirmationSnapshot,
    ScalpDirectionalBias,
    ScalpFvgEntryMode,
    ScalpIfvgZoneSnapshot,
    ScalpSessionWindows,
    ScalpStrategyConfig,
    ScalpSweepSnapshot,
} from './types';

function high(c: ScalpCandle): number {
    return c[2];
}

function low(c: ScalpCandle): number {
    return c[3];
}

function open(c: ScalpCandle): number {
    return c[1];
}

function close(c: ScalpCandle): number {
    return c[4];
}

function ts(c: ScalpCandle): number {
    return c[0];
}

function safeDiv(n: number, d: number): number {
    if (!(Number.isFinite(n) && Number.isFinite(d) && d !== 0)) return 0;
    return n / d;
}

function candleBody(c: ScalpCandle): number {
    return Math.abs(close(c) - open(c));
}

function candleRange(c: ScalpCandle): number {
    return Math.max(0, high(c) - low(c));
}

function upperWick(c: ScalpCandle): number {
    return Math.max(0, high(c) - Math.max(open(c), close(c)));
}

function lowerWick(c: ScalpCandle): number {
    return Math.max(0, Math.min(open(c), close(c)) - low(c));
}

function computeAtrSeries(candles: ScalpCandle[], period: number): number[] {
    if (!Array.isArray(candles) || candles.length < 2) return [];
    const p = Math.max(1, Math.floor(period));
    const trs: number[] = [];
    for (let i = 1; i < candles.length; i += 1) {
        const prevClose = close(candles[i - 1]!);
        const tr = Math.max(
            high(candles[i]!) - low(candles[i]!),
            Math.abs(high(candles[i]!) - prevClose),
            Math.abs(low(candles[i]!) - prevClose),
        );
        trs.push(Math.max(0, tr));
    }
    if (!trs.length) return [];

    const out: number[] = new Array(candles.length).fill(0);
    let sum = 0;
    for (let i = 0; i < trs.length; i += 1) {
        sum += trs[i]!;
        if (i >= p) sum -= trs[i - p]!;
        const atr = i >= p - 1 ? sum / p : sum / (i + 1);
        out[i + 1] = Number.isFinite(atr) ? atr : 0;
    }
    out[0] = out[1] ?? out[0] ?? 0;
    return out;
}

function filterWindow(candles: ScalpCandle[], startMs: number, endMs: number): ScalpCandle[] {
    return candles.filter((c) => ts(c) >= startMs && ts(c) < endMs);
}

function dedupeReasonCodes(codes: string[]): string[] {
    return Array.from(new Set(codes.filter((c) => Boolean(String(c || '').trim()))));
}

function closeNearHigh(c: ScalpCandle): number {
    return safeDiv(high(c) - close(c), candleRange(c));
}

function closeNearLow(c: ScalpCandle): number {
    return safeDiv(close(c) - low(c), candleRange(c));
}

export function computeAtr(candles: ScalpCandle[], period: number): number {
    const series = computeAtrSeries(candles, period);
    return series.length ? series[series.length - 1]! : 0;
}

export function buildAsiaRangeSnapshot(params: {
    nowMs: number;
    windows: ScalpSessionWindows;
    candles: ScalpCandle[];
    minCandles: number;
    sourceTf: ScalpAsiaRangeSnapshot['sourceTf'];
}): { snapshot: ScalpAsiaRangeSnapshot | null; reasonCodes: string[] } {
    if (params.nowMs < params.windows.asiaEndMs) {
        return { snapshot: null, reasonCodes: ['ASIA_WINDOW_NOT_CLOSED'] };
    }

    const asiaCandles = filterWindow(params.candles, params.windows.asiaStartMs, params.windows.asiaEndMs);
    if (asiaCandles.length < Math.max(1, params.minCandles)) {
        return {
            snapshot: null,
            reasonCodes: ['ASIA_RANGE_INSUFFICIENT_CANDLES'],
        };
    }

    const rangeHigh = Math.max(...asiaCandles.map((c) => high(c)));
    const rangeLow = Math.min(...asiaCandles.map((c) => low(c)));
    if (!(Number.isFinite(rangeHigh) && Number.isFinite(rangeLow) && rangeHigh > rangeLow)) {
        return {
            snapshot: null,
            reasonCodes: ['ASIA_RANGE_INVALID'],
        };
    }

    return {
        snapshot: {
            timezone: params.windows.timezone,
            sourceTf: params.sourceTf,
            startUtcIso: params.windows.asiaStartUtcIso,
            endUtcIso: params.windows.asiaEndUtcIso,
            high: rangeHigh,
            low: rangeLow,
            candleCount: asiaCandles.length,
            builtAtMs: params.nowMs,
        },
        reasonCodes: ['ASIA_RANGE_READY'],
    };
}

function expectedDirectionForSweep(sweep: ScalpSweepSnapshot['side']): ScalpDirectionalBias {
    return sweep === 'BUY_SIDE' ? 'BEARISH' : 'BULLISH';
}

function findSweepCandleIndex(candles: ScalpCandle[], sweep: ScalpSweepSnapshot): number {
    return candles.findIndex((c) => ts(c) === sweep.sweepTsMs);
}

export function detectSweepLifecycle(params: {
    existingSweep: ScalpSweepSnapshot | null;
    candles: ScalpCandle[];
    windows: ScalpSessionWindows;
    nowMs: number;
    asiaHigh: number;
    asiaLow: number;
    atrAbs: number;
    spreadAbs: number;
    pipSize: number;
    cfg: ScalpStrategyConfig['sweep'];
}): {
    sweep: ScalpSweepSnapshot | null;
    status: 'none' | 'pending' | 'rejected' | 'expired';
    direction: ScalpDirectionalBias | null;
    reasonCodes: string[];
} {
    const reasonCodes: string[] = [];
    const bufferAbs = Math.max(
        Math.max(0, params.cfg.bufferPips) * params.pipSize,
        Math.max(0, params.cfg.bufferAtrMult) * Math.max(0, params.atrAbs),
        Math.max(0, params.cfg.bufferSpreadMult) * Math.max(0, params.spreadAbs),
    );
    const insideAbs = Math.max(0, params.cfg.rejectInsidePips) * params.pipSize;
    const raidCandles = filterWindow(params.candles, params.windows.raidStartMs, params.windows.raidEndMs);

    let sweep = params.existingSweep ? { ...params.existingSweep, reasonCodes: params.existingSweep.reasonCodes.slice() } : null;

    if (!sweep) {
        if (!raidCandles.length) {
            reasonCodes.push('NO_RAID_CANDLES_YET');
            return { sweep: null, status: 'none', direction: null, reasonCodes };
        }
        for (const candle of raidCandles) {
            const buySweep = high(candle) >= params.asiaHigh + bufferAbs;
            const sellSweep = low(candle) <= params.asiaLow - bufferAbs;
            if (buySweep && sellSweep) {
                reasonCodes.push('SWEEP_AMBIGUOUS_DUAL_BREACH');
                continue;
            }
            if (buySweep) {
                sweep = {
                    side: 'BUY_SIDE',
                    sweepTsMs: ts(candle),
                    sweepPrice: high(candle),
                    bufferAbs,
                    rejected: false,
                    rejectedTsMs: null,
                    reasonCodes: ['SWEEP_BUY_SIDE_BREACH'],
                };
                break;
            }
            if (sellSweep) {
                sweep = {
                    side: 'SELL_SIDE',
                    sweepTsMs: ts(candle),
                    sweepPrice: low(candle),
                    bufferAbs,
                    rejected: false,
                    rejectedTsMs: null,
                    reasonCodes: ['SWEEP_SELL_SIDE_BREACH'],
                };
                break;
            }
        }
        if (!sweep) {
            reasonCodes.push('NO_SWEEP_DETECTED');
            return { sweep: null, status: 'none', direction: null, reasonCodes };
        }
    }

    if (sweep.rejected) {
        reasonCodes.push('SWEEP_ALREADY_REJECTED');
        return {
            sweep,
            status: 'rejected',
            direction: expectedDirectionForSweep(sweep.side),
            reasonCodes: dedupeReasonCodes([...reasonCodes, ...sweep.reasonCodes]),
        };
    }

    const sweepIndex = findSweepCandleIndex(params.candles, sweep);
    if (sweepIndex < 0) {
        reasonCodes.push('SWEEP_CANDLE_NOT_FOUND');
        return { sweep, status: 'pending', direction: expectedDirectionForSweep(sweep.side), reasonCodes };
    }

    const maxBars = Math.max(1, params.cfg.rejectMaxBars);
    const lastIndexForReject = Math.min(params.candles.length - 1, sweepIndex + maxBars - 1);
    for (let i = sweepIndex; i <= lastIndexForReject; i += 1) {
        const candle = params.candles[i]!;
        const body = candleBody(candle);
        const wickRatio =
            sweep.side === 'BUY_SIDE'
                ? safeDiv(upperWick(candle), Math.max(body, params.pipSize * 0.1))
                : safeDiv(lowerWick(candle), Math.max(body, params.pipSize * 0.1));

        const rejected =
            sweep.side === 'BUY_SIDE'
                ? close(candle) <= params.asiaHigh - insideAbs
                : close(candle) >= params.asiaLow + insideAbs;
        const wickOk = params.cfg.minWickBodyRatio <= 0 || wickRatio >= params.cfg.minWickBodyRatio;
        if (rejected && wickOk) {
            sweep.rejected = true;
            sweep.rejectedTsMs = ts(candle);
            sweep.reasonCodes = dedupeReasonCodes([...sweep.reasonCodes, 'SWEEP_REJECTION_CONFIRMED']);
            reasonCodes.push('SWEEP_REJECTION_CONFIRMED');
            return {
                sweep,
                status: 'rejected',
                direction: expectedDirectionForSweep(sweep.side),
                reasonCodes: dedupeReasonCodes([...reasonCodes, ...sweep.reasonCodes]),
            };
        }
    }

    if (params.candles.length - 1 > lastIndexForReject) {
        reasonCodes.push('SWEEP_REJECTION_TIMEOUT');
        sweep.reasonCodes = dedupeReasonCodes([...sweep.reasonCodes, 'SWEEP_REJECTION_TIMEOUT']);
        return {
            sweep,
            status: 'expired',
            direction: expectedDirectionForSweep(sweep.side),
            reasonCodes: dedupeReasonCodes([...reasonCodes, ...sweep.reasonCodes]),
        };
    }

    reasonCodes.push('SWEEP_PENDING_REJECTION');
    return {
        sweep,
        status: 'pending',
        direction: expectedDirectionForSweep(sweep.side),
        reasonCodes: dedupeReasonCodes([...reasonCodes, ...sweep.reasonCodes]),
    };
}

function detectDisplacementIndex(params: {
    candles: ScalpCandle[];
    atrSeries: number[];
    direction: ScalpDirectionalBias;
    bodyMult: number;
    rangeMult: number;
    closeInExtremePct: number;
}): number {
    for (let i = 0; i < params.candles.length; i += 1) {
        const c = params.candles[i]!;
        const atr = Math.max(params.atrSeries[i] || 0, Number.EPSILON);
        const bodyOk = candleBody(c) >= params.bodyMult * atr;
        const rangeOk = candleRange(c) >= params.rangeMult * atr;
        if (!(bodyOk && rangeOk)) continue;

        if (params.direction === 'BEARISH') {
            const dirOk = close(c) < open(c);
            const closeOk = closeNearLow(c) <= params.closeInExtremePct;
            if (dirOk && closeOk) return i;
        } else {
            const dirOk = close(c) > open(c);
            const closeOk = closeNearHigh(c) <= params.closeInExtremePct;
            if (dirOk && closeOk) return i;
        }
    }
    return -1;
}

function detectStructureShiftIndex(params: {
    candles: ScalpCandle[];
    atrSeries: number[];
    fromIndex: number;
    direction: ScalpDirectionalBias;
    lookback: number;
    bufferPipsAbs: number;
    bufferAtrMult: number;
}): number {
    const lookback = Math.max(2, Math.floor(params.lookback));
    for (let i = Math.max(lookback, params.fromIndex); i < params.candles.length; i += 1) {
        const window = params.candles.slice(Math.max(0, i - lookback), i);
        if (window.length < 2) continue;
        const atr = Math.max(params.atrSeries[i] || 0, Number.EPSILON);
        const breakBuffer = Math.max(params.bufferPipsAbs, params.bufferAtrMult * atr);
        if (params.direction === 'BULLISH') {
            const swingHigh = Math.max(...window.map((c) => high(c)));
            if (close(params.candles[i]!) >= swingHigh + breakBuffer) return i;
        } else {
            const swingLow = Math.min(...window.map((c) => low(c)));
            if (close(params.candles[i]!) <= swingLow - breakBuffer) return i;
        }
    }
    return -1;
}

export function detectConfirmation(params: {
    candles: ScalpCandle[];
    nowMs: number;
    rejectionTsMs: number;
    pipSize: number;
    atrPeriod: number;
    direction: ScalpDirectionalBias;
    cfg: ScalpStrategyConfig['confirm'];
}): {
    snapshot: ScalpConfirmationSnapshot;
    status: 'pending' | 'confirmed' | 'expired';
    displacementTsMs: number | null;
    structureShiftTsMs: number | null;
    reasonCodes: string[];
} {
    const startTs = params.rejectionTsMs;
    const endTs = params.rejectionTsMs + Math.max(1, params.cfg.ttlMinutes) * 60_000;
    const inWindow = params.candles.filter((c) => ts(c) >= startTs && ts(c) <= Math.min(params.nowMs, endTs));
    if (!inWindow.length) {
        return {
            snapshot: {
                displacementDetected: false,
                displacementTsMs: null,
                structureShiftDetected: false,
                structureShiftTsMs: null,
                reasonCodes: ['CONFIRM_WINDOW_EMPTY'],
            },
            status: params.nowMs > endTs ? 'expired' : 'pending',
            displacementTsMs: null,
            structureShiftTsMs: null,
            reasonCodes: [params.nowMs > endTs ? 'CONFIRM_WINDOW_EXPIRED' : 'CONFIRM_WAITING_CANDLES'],
        };
    }

    const atrSeries = computeAtrSeries(inWindow, params.atrPeriod);
    const displacementIndex = detectDisplacementIndex({
        candles: inWindow,
        atrSeries,
        direction: params.direction,
        bodyMult: params.cfg.displacementBodyAtrMult,
        rangeMult: params.cfg.displacementRangeAtrMult,
        closeInExtremePct: params.cfg.closeInExtremePct,
    });

    if (displacementIndex < 0) {
        const expired = params.nowMs > endTs;
        return {
            snapshot: {
                displacementDetected: false,
                displacementTsMs: null,
                structureShiftDetected: false,
                structureShiftTsMs: null,
                reasonCodes: [expired ? 'DISPLACEMENT_NOT_FOUND_EXPIRED' : 'DISPLACEMENT_PENDING'],
            },
            status: expired ? 'expired' : 'pending',
            displacementTsMs: null,
            structureShiftTsMs: null,
            reasonCodes: [expired ? 'DISPLACEMENT_NOT_FOUND_EXPIRED' : 'DISPLACEMENT_PENDING'],
        };
    }

    const shiftIndex = detectStructureShiftIndex({
        candles: inWindow,
        atrSeries,
        fromIndex: displacementIndex + 1,
        direction: params.direction,
        lookback: params.cfg.mssLookbackBars,
        bufferPipsAbs: Math.max(0, params.cfg.mssBreakBufferPips) * params.pipSize,
        bufferAtrMult: Math.max(0, params.cfg.mssBreakBufferAtrMult),
    });

    const displacementTsMs = ts(inWindow[displacementIndex]!);
    const structureShiftTsMs = shiftIndex >= 0 ? ts(inWindow[shiftIndex]!) : null;
    const expired = params.nowMs > endTs;

    const snapshot: ScalpConfirmationSnapshot = {
        displacementDetected: true,
        displacementTsMs,
        structureShiftDetected: shiftIndex >= 0,
        structureShiftTsMs,
        reasonCodes: dedupeReasonCodes([
            'DISPLACEMENT_CONFIRMED',
            shiftIndex >= 0 ? 'MSS_CONFIRMED' : expired ? 'MSS_NOT_FOUND_EXPIRED' : 'MSS_PENDING',
        ]),
    };

    return {
        snapshot,
        status: shiftIndex >= 0 ? 'confirmed' : expired ? 'expired' : 'pending',
        displacementTsMs,
        structureShiftTsMs,
        reasonCodes: snapshot.reasonCodes,
    };
}

function fvgZoneFromTriplet(direction: ScalpDirectionalBias, c1: ScalpCandle, c3: ScalpCandle): { low: number; high: number } | null {
    if (direction === 'BULLISH') {
        if (high(c1) < low(c3)) {
            return { low: high(c1), high: low(c3) };
        }
        return null;
    }
    if (low(c1) > high(c3)) {
        return { low: high(c3), high: low(c1) };
    }
    return null;
}

export function detectIfvg(params: {
    candles: ScalpCandle[];
    direction: ScalpDirectionalBias;
    displacementTsMs: number;
    structureShiftTsMs: number;
    nowMs: number;
    atrPeriod: number;
    cfg: ScalpStrategyConfig['ifvg'];
}): { zone: ScalpIfvgZoneSnapshot | null; reasonCodes: string[] } {
    const window = params.candles.filter((c) => ts(c) >= params.displacementTsMs && ts(c) <= params.nowMs);
    if (window.length < 3) return { zone: null, reasonCodes: ['IFVG_WINDOW_TOO_SHORT'] };

    const atrSeries = computeAtrSeries(window, params.atrPeriod);
    const minMult = Math.max(0, params.cfg.minAtrMult);
    const maxMult = Math.max(minMult, params.cfg.maxAtrMult);
    const candidates: ScalpIfvgZoneSnapshot[] = [];

    for (let i = 2; i < window.length; i += 1) {
        const c1 = window[i - 2]!;
        const c3 = window[i]!;
        const zone = fvgZoneFromTriplet(params.direction, c1, c3);
        if (!zone) continue;
        const sizeAbs = zone.high - zone.low;
        const atr = Math.max(atrSeries[i] || 0, Number.EPSILON);
        const minSize = minMult * atr;
        const maxSize = maxMult * atr;
        if (!(sizeAbs >= minSize && sizeAbs <= maxSize)) continue;
        const createdTsMs = ts(c3);
        if (createdTsMs < params.displacementTsMs) continue;
        if (createdTsMs < params.structureShiftTsMs) continue;
        candidates.push({
            direction: params.direction,
            low: zone.low,
            high: zone.high,
            createdTsMs,
            expiresAtMs: createdTsMs + Math.max(1, params.cfg.ttlMinutes) * 60_000,
            entryMode: params.cfg.entryMode,
            touched: false,
        });
    }

    if (candidates.length) {
        candidates.sort((a, b) => b.createdTsMs - a.createdTsMs);
        return {
            zone: candidates[0]!,
            reasonCodes: ['IFVG_QUALIFIED'],
        };
    }

    return { zone: null, reasonCodes: ['IFVG_NOT_FOUND'] };
}

function intersectsZone(c: ScalpCandle, zone: { low: number; high: number }): boolean {
    return low(c) <= zone.high && high(c) >= zone.low;
}

function isTouchByMode(c: ScalpCandle, zone: ScalpIfvgZoneSnapshot, mode: ScalpFvgEntryMode): boolean {
    if (mode === 'first_touch') {
        return intersectsZone(c, zone);
    }
    const mid = (zone.low + zone.high) / 2;
    if (mode === 'midline_touch') {
        return low(c) <= mid && high(c) >= mid;
    }
    if (zone.direction === 'BULLISH') {
        return low(c) <= zone.low;
    }
    return high(c) >= zone.high;
}

export function detectIfvgTouch(params: {
    candles: ScalpCandle[];
    ifvg: ScalpIfvgZoneSnapshot;
    nowMs: number;
}): { touched: boolean; touchedTsMs: number | null; expired: boolean; reasonCodes: string[] } {
    if (params.ifvg.touched) {
        return { touched: true, touchedTsMs: params.ifvg.createdTsMs, expired: false, reasonCodes: ['IFVG_ALREADY_TOUCHED'] };
    }
    const expired = params.nowMs > params.ifvg.expiresAtMs;
    const candidates = params.candles.filter((c) => ts(c) > params.ifvg.createdTsMs && ts(c) <= params.nowMs);
    for (const candle of candidates) {
        if (isTouchByMode(candle, params.ifvg, params.ifvg.entryMode)) {
            return {
                touched: true,
                touchedTsMs: ts(candle),
                expired: false,
                reasonCodes: ['IFVG_ENTRY_TOUCH_CONFIRMED'],
            };
        }
    }
    return {
        touched: false,
        touchedTsMs: null,
        expired,
        reasonCodes: [expired ? 'IFVG_EXPIRED' : 'IFVG_WAITING_RETRACE'],
    };
}
