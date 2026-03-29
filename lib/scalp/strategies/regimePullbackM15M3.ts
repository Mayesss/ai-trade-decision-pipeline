import type { ScalpBaseTimeframe, ScalpCandle, ScalpConfirmTimeframe, ScalpDirectionalBias, ScalpSessionState } from '../types';
import {
    inScalpEntrySessionProfileWindow,
    minuteOfDayInTimeZone,
    normalizeScalpEntrySessionProfile,
} from '../sessions';
import type { ScalpStrategyDefinition, ScalpStrategyEntryIntent, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';

type RegimeScan = {
    direction: ScalpDirectionalBias | null;
    adx: number;
    close: number;
    emaFast: number;
    emaSlow: number;
    emaSlowSlope: number;
    reasonCodes: string[];
};

type VolatilityScan = {
    ok: boolean;
    atrCurrent: number;
    atrThreshold: number | null;
    atrPercentile: number | null;
    reasonCodes: string[];
};

type SetupScan = {
    found: boolean;
    direction: ScalpDirectionalBias;
    impulseIndex: number;
    pullbackIndex: number;
    entryIndex: number;
    stopAnchor: number;
    zoneLow: number;
    zoneHigh: number;
    trigger: 'PULLBACK_SWING_BREAK' | 'EMA20_RECLAIM_STRONG';
    reasonCodes: string[];
};

type RegimePullbackStrategyOptions = {
    id: string;
    shortName: string;
    longName: string;
    allowPullbackSwingBreakTrigger: boolean;
    blockedBerlinEntryHours: number[];
    requiredBaseTf: ScalpBaseTimeframe;
    requiredConfirmTf: ScalpConfirmTimeframe;
};

const STRATEGY_CONST = {
    emaFast15: 50,
    emaSlow15: 200,
    adxLen15: 14,
    adxMin: 18,
    emaSlopeLookback15: 10,
    emaSlopeMin: 0,
    emaPullback3: 20,
    emaTrend3: 50,
    atrLen3: 14,
    atrPercentileEnabled: true,
    atrPercentileLookback: 200,
    atrMinPercentile: 40,
    impulseBreakoutLookback: 10,
    pullbackMaxBars: 12,
    retestToleranceAtr: 0.25,
    pullbackInvalidationAtr: 0.2,
    recentSwingLookbackBars: 8,
} as const;

function close(candle: ScalpCandle): number {
    return candle[4];
}

function high(candle: ScalpCandle): number {
    return candle[2];
}

function low(candle: ScalpCandle): number {
    return candle[3];
}

function open(candle: ScalpCandle): number {
    return candle[1];
}

function ts(candle: ScalpCandle): number {
    return candle[0];
}

function dedupeReasonCodes(codes: string[]): string[] {
    return Array.from(
        new Set(
            codes
                .map((code) => String(code || '').trim().toUpperCase())
                .filter((code) => code.length > 0),
        ),
    );
}

function latestTs(candles: ScalpCandle[]): number | null {
    const latest = candles.at(-1)?.[0];
    return Number.isFinite(Number(latest)) ? Number(latest) : null;
}

function withLastProcessed(state: ScalpSessionState, input: ScalpStrategyPhaseInput['market']): ScalpSessionState {
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

function finalizePhase(params: {
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

function candleRange(candle: ScalpCandle): number {
    return Math.max(0, high(candle) - low(candle));
}

function candleBody(candle: ScalpCandle): number {
    return Math.abs(close(candle) - open(candle));
}

function computeEmaSeries(values: number[], period: number): number[] {
    const p = Math.max(1, Math.floor(period));
    if (!values.length) return [];
    const out: number[] = [];
    const k = 2 / (p + 1);
    out[0] = values[0]!;
    for (let i = 1; i < values.length; i += 1) {
        out[i] = values[i]! * k + out[i - 1]! * (1 - k);
    }
    return out;
}

function computeAtrSeries(candles: ScalpCandle[], period: number): number[] {
    if (!Array.isArray(candles) || candles.length < 2) return [];
    const p = Math.max(1, Math.floor(period));
    const tr: number[] = new Array(candles.length).fill(0);
    for (let i = 1; i < candles.length; i += 1) {
        const prevClose = close(candles[i - 1]!);
        tr[i] = Math.max(
            high(candles[i]!) - low(candles[i]!),
            Math.abs(high(candles[i]!) - prevClose),
            Math.abs(low(candles[i]!) - prevClose),
        );
    }

    const out: number[] = new Array(candles.length).fill(0);
    let rolling = 0;
    for (let i = 1; i < candles.length; i += 1) {
        rolling += tr[i]!;
        if (i > p) rolling -= tr[i - p]!;
        const divisor = Math.min(i, p);
        out[i] = divisor > 0 ? rolling / divisor : 0;
    }
    out[0] = out[1] ?? 0;
    return out;
}

function percentile(values: number[], p: number): number | null {
    if (!values.length) return null;
    const sorted = values.slice().sort((a, b) => a - b);
    const pct = Math.max(0, Math.min(100, p));
    const rank = (pct / 100) * (sorted.length - 1);
    const lo = Math.floor(rank);
    const hi = Math.ceil(rank);
    if (lo === hi) return sorted[lo] ?? null;
    const left = sorted[lo]!;
    const right = sorted[hi]!;
    const w = rank - lo;
    return left * (1 - w) + right * w;
}

function percentileRank(values: number[], current: number): number | null {
    if (!values.length || !Number.isFinite(current)) return null;
    const sorted = values.slice().sort((a, b) => a - b);
    let count = 0;
    for (const value of sorted) {
        if (value <= current) count += 1;
    }
    return (count / sorted.length) * 100;
}

function computeAdx(candles: ScalpCandle[], period: number): number {
    const p = Math.max(2, Math.floor(period));
    if (!Array.isArray(candles) || candles.length < p + 2) return 0;

    const tr: number[] = new Array(candles.length).fill(0);
    const plusDm: number[] = new Array(candles.length).fill(0);
    const minusDm: number[] = new Array(candles.length).fill(0);

    for (let i = 1; i < candles.length; i += 1) {
        const upMove = high(candles[i]!) - high(candles[i - 1]!);
        const downMove = low(candles[i - 1]!) - low(candles[i]!);

        plusDm[i] = upMove > downMove && upMove > 0 ? upMove : 0;
        minusDm[i] = downMove > upMove && downMove > 0 ? downMove : 0;
        tr[i] = Math.max(
            high(candles[i]!) - low(candles[i]!),
            Math.abs(high(candles[i]!) - close(candles[i - 1]!)),
            Math.abs(low(candles[i]!) - close(candles[i - 1]!)),
        );
    }

    let trSmooth = 0;
    let plusSmooth = 0;
    let minusSmooth = 0;
    for (let i = 1; i <= p && i < candles.length; i += 1) {
        trSmooth += tr[i]!;
        plusSmooth += plusDm[i]!;
        minusSmooth += minusDm[i]!;
    }
    if (!(trSmooth > 0)) return 0;

    const dxValues: number[] = [];
    for (let i = p + 1; i < candles.length; i += 1) {
        trSmooth = trSmooth - trSmooth / p + tr[i]!;
        plusSmooth = plusSmooth - plusSmooth / p + plusDm[i]!;
        minusSmooth = minusSmooth - minusSmooth / p + minusDm[i]!;
        if (!(trSmooth > 0)) continue;
        const plusDi = (100 * plusSmooth) / trSmooth;
        const minusDi = (100 * minusSmooth) / trSmooth;
        const diSum = plusDi + minusDi;
        if (!(diSum > 0)) continue;
        const dx = (100 * Math.abs(plusDi - minusDi)) / diSum;
        if (Number.isFinite(dx)) dxValues.push(dx);
    }
    if (!dxValues.length) return 0;
    const take = Math.min(p, dxValues.length);
    return dxValues.slice(-take).reduce((acc, value) => acc + value, 0) / take;
}

function normalizeBerlinEntryHours(values: number[] | undefined): number[] {
    if (!Array.isArray(values) || values.length === 0) return [];
    return Array.from(
        new Set(
            values
                .map((value) => Math.floor(Number(value)))
                .filter((value) => Number.isFinite(value) && value >= 0 && value <= 23),
        ),
    ).sort((a, b) => a - b);
}

function blockedBerlinEntryHour(nowMs: number, blockedHours: number[]): number | null {
    if (!blockedHours.length) return null;
    const minuteOfDay = minuteOfDayInTimeZone(nowMs, 'Europe/Berlin');
    if (!(minuteOfDay >= 0)) return null;
    const hour = Math.floor(minuteOfDay / 60);
    if (blockedHours.includes(hour)) return hour;
    return null;
}

function scanRegime(baseCandles: ScalpCandle[]): RegimeScan {
    const reasonCodes: string[] = [];
    if (baseCandles.length < STRATEGY_CONST.emaSlow15 + STRATEGY_CONST.emaSlopeLookback15 + 5) {
        return {
            direction: null,
            adx: 0,
            close: 0,
            emaFast: 0,
            emaSlow: 0,
            emaSlowSlope: 0,
            reasonCodes: ['REGIME_INSUFFICIENT_M15_CANDLES'],
        };
    }

    const closes = baseCandles.map(close);
    const emaFastSeries = computeEmaSeries(closes, STRATEGY_CONST.emaFast15);
    const emaSlowSeries = computeEmaSeries(closes, STRATEGY_CONST.emaSlow15);
    const idx = baseCandles.length - 1;
    const slopeIdx = idx - STRATEGY_CONST.emaSlopeLookback15;
    const closeNow = closes[idx]!;
    const emaFastNow = emaFastSeries[idx]!;
    const emaSlowNow = emaSlowSeries[idx]!;
    const emaSlowPast = slopeIdx >= 0 ? emaSlowSeries[slopeIdx]! : emaSlowNow;
    const emaSlowSlope = emaSlowNow - emaSlowPast;
    const adx = computeAdx(baseCandles, STRATEGY_CONST.adxLen15);

    if (!(adx >= STRATEGY_CONST.adxMin)) {
        reasonCodes.push('REGIME_ADX_BELOW_MIN');
    }

    const bull =
        closeNow > emaSlowNow &&
        emaFastNow > emaSlowNow &&
        emaSlowSlope >= STRATEGY_CONST.emaSlopeMin &&
        adx >= STRATEGY_CONST.adxMin;
    const bear =
        closeNow < emaSlowNow &&
        emaFastNow < emaSlowNow &&
        emaSlowSlope <= -STRATEGY_CONST.emaSlopeMin &&
        adx >= STRATEGY_CONST.adxMin;

    if (bull) {
        reasonCodes.push('REGIME_BULL_CONFIRMED');
        return {
            direction: 'BULLISH',
            adx,
            close: closeNow,
            emaFast: emaFastNow,
            emaSlow: emaSlowNow,
            emaSlowSlope,
            reasonCodes,
        };
    }
    if (bear) {
        reasonCodes.push('REGIME_BEAR_CONFIRMED');
        return {
            direction: 'BEARISH',
            adx,
            close: closeNow,
            emaFast: emaFastNow,
            emaSlow: emaSlowNow,
            emaSlowSlope,
            reasonCodes,
        };
    }

    reasonCodes.push('REGIME_NEUTRAL');
    return {
        direction: null,
        adx,
        close: closeNow,
        emaFast: emaFastNow,
        emaSlow: emaSlowNow,
        emaSlowSlope,
        reasonCodes,
    };
}

function scanVolatility(confirmCandles: ScalpCandle[]): VolatilityScan {
    const atrSeries = computeAtrSeries(confirmCandles, STRATEGY_CONST.atrLen3);
    const atrCurrent = atrSeries.at(-1) ?? 0;
    if (!(atrCurrent > 0)) {
        return {
            ok: false,
            atrCurrent: 0,
            atrThreshold: null,
            atrPercentile: null,
            reasonCodes: ['VOL_FILTER_ATR_INVALID'],
        };
    }
    if (!STRATEGY_CONST.atrPercentileEnabled) {
        return {
            ok: true,
            atrCurrent,
            atrThreshold: null,
            atrPercentile: null,
            reasonCodes: ['VOL_FILTER_SKIPPED_PERCENTILE_DISABLED'],
        };
    }

    if (atrSeries.length < STRATEGY_CONST.atrPercentileLookback) {
        return {
            ok: false,
            atrCurrent,
            atrThreshold: null,
            atrPercentile: null,
            reasonCodes: ['VOL_FILTER_ATR_PERCENTILE_INSUFFICIENT_HISTORY'],
        };
    }
    const window = atrSeries.slice(-STRATEGY_CONST.atrPercentileLookback);
    const threshold = percentile(window, STRATEGY_CONST.atrMinPercentile);
    const rank = percentileRank(window, atrCurrent);
    if (!(Number.isFinite(threshold as number) && (threshold as number) > 0)) {
        return {
            ok: false,
            atrCurrent,
            atrThreshold: null,
            atrPercentile: rank,
            reasonCodes: ['VOL_FILTER_ATR_PERCENTILE_INVALID'],
        };
    }
    if (atrCurrent < (threshold as number)) {
        return {
            ok: false,
            atrCurrent,
            atrThreshold: threshold as number,
            atrPercentile: rank,
            reasonCodes: ['VOL_FILTER_ATR_PERCENTILE_BELOW_MIN'],
        };
    }
    return {
        ok: true,
        atrCurrent,
        atrThreshold: threshold as number,
        atrPercentile: rank,
        reasonCodes: ['VOL_FILTER_PASSED'],
    };
}

function latestExtreme(values: number[], fromIndex: number, toIndex: number, mode: 'min' | 'max'): number | null {
    if (fromIndex > toIndex || fromIndex < 0 || toIndex >= values.length) return null;
    const slice = values.slice(fromIndex, toIndex + 1);
    if (!slice.length) return null;
    return mode === 'min' ? Math.min(...slice) : Math.max(...slice);
}

function detectEntrySetup(params: {
    candles: ScalpCandle[];
    direction: ScalpDirectionalBias;
    ema20: number[];
    ema50: number[];
    atr: number[];
    allowPullbackSwingBreakTrigger: boolean;
}): SetupScan | null {
    const candles = params.candles;
    const lastIndex = candles.length - 1;
    if (lastIndex < STRATEGY_CONST.impulseBreakoutLookback + 3) return null;
    const highs = candles.map(high);
    const lows = candles.map(low);
    const closes = candles.map(close);

    for (let impulseIndex = lastIndex - 1; impulseIndex >= STRATEGY_CONST.impulseBreakoutLookback; impulseIndex -= 1) {
        const impulseCandle = candles[impulseIndex]!;
        const impulseRange = candleRange(impulseCandle);
        if (!(impulseRange > 0)) continue;
        const bodyRatio = candleBody(impulseCandle) / impulseRange;
        if (!(bodyRatio >= 0.4)) continue;

        const lookbackStart = Math.max(0, impulseIndex - STRATEGY_CONST.impulseBreakoutLookback);
        const prevHigh = latestExtreme(highs, lookbackStart, impulseIndex - 1, 'max');
        const prevLow = latestExtreme(lows, lookbackStart, impulseIndex - 1, 'min');
        if (prevHigh === null || prevLow === null) continue;

        const impulseBreakoutOk =
            params.direction === 'BULLISH' ? close(impulseCandle) > prevHigh : close(impulseCandle) < prevLow;
        if (!impulseBreakoutOk) continue;

        const maxEntryIndex = Math.min(lastIndex, impulseIndex + STRATEGY_CONST.pullbackMaxBars);
        if (lastIndex > maxEntryIndex) continue;
        const pullbackSearchStart = impulseIndex + 1;
        if (pullbackSearchStart > maxEntryIndex) continue;

        let pullbackIndex = -1;
        let invalidated = false;
        for (let idx = pullbackSearchStart; idx <= maxEntryIndex; idx += 1) {
            const atrAbs = params.atr[idx] ?? 0;
            const ema20 = params.ema20[idx] ?? NaN;
            const ema50 = params.ema50[idx] ?? NaN;
            if (!(Number.isFinite(ema20) && Number.isFinite(ema50) && atrAbs > 0)) {
                invalidated = true;
                break;
            }
            const invalidBreak =
                params.direction === 'BULLISH'
                    ? closes[idx]! < ema50 - STRATEGY_CONST.pullbackInvalidationAtr * atrAbs
                    : closes[idx]! > ema50 + STRATEGY_CONST.pullbackInvalidationAtr * atrAbs;
            if (invalidBreak) {
                invalidated = true;
                break;
            }

            const touchedZone =
                params.direction === 'BULLISH'
                    ? lows[idx]! <= ema20 || Math.abs(lows[idx]! - ema20) <= STRATEGY_CONST.retestToleranceAtr * atrAbs
                    : highs[idx]! >= ema20 || Math.abs(highs[idx]! - ema20) <= STRATEGY_CONST.retestToleranceAtr * atrAbs;
            if (touchedZone) {
                pullbackIndex = idx;
                break;
            }
        }
        if (invalidated || pullbackIndex < 0) continue;
        if (pullbackIndex >= lastIndex) continue;

        let postPullbackInvalid = false;
        for (let idx = pullbackIndex; idx <= lastIndex; idx += 1) {
            const atrAbs = params.atr[idx] ?? 0;
            const ema50 = params.ema50[idx] ?? NaN;
            if (!(Number.isFinite(ema50) && atrAbs > 0)) {
                postPullbackInvalid = true;
                break;
            }
            const invalidBreak =
                params.direction === 'BULLISH'
                    ? closes[idx]! < ema50 - STRATEGY_CONST.pullbackInvalidationAtr * atrAbs
                    : closes[idx]! > ema50 + STRATEGY_CONST.pullbackInvalidationAtr * atrAbs;
            if (invalidBreak) {
                postPullbackInvalid = true;
                break;
            }
        }
        if (postPullbackInvalid) continue;

        const pullbackSwingHigh = latestExtreme(highs, pullbackIndex, lastIndex - 1, 'max');
        const pullbackSwingLow = latestExtreme(lows, pullbackIndex, lastIndex - 1, 'min');
        if (pullbackSwingHigh === null || pullbackSwingLow === null) continue;

        const lastCandle = candles[lastIndex]!;
        const lastRange = candleRange(lastCandle);
        const lastBody = candleBody(lastCandle);
        const lastEma20 = params.ema20[lastIndex] ?? NaN;

        const trigger1Raw =
            params.direction === 'BULLISH'
                ? close(lastCandle) > pullbackSwingHigh
                : close(lastCandle) < pullbackSwingLow;
        const trigger1 = params.allowPullbackSwingBreakTrigger && trigger1Raw;
        const trigger2 =
            Number.isFinite(lastEma20) &&
            lastRange > 0 &&
            lastBody >= 0.5 * lastRange &&
            (params.direction === 'BULLISH'
                ? close(lastCandle) > lastEma20 && (high(lastCandle) - close(lastCandle)) / lastRange <= 0.3
                : close(lastCandle) < lastEma20 && (close(lastCandle) - low(lastCandle)) / lastRange <= 0.3);
        if (!(trigger1 || trigger2)) continue;

        const recentStart = Math.max(0, lastIndex - STRATEGY_CONST.recentSwingLookbackBars + 1);
        const recentLow = latestExtreme(lows, recentStart, lastIndex, 'min');
        const recentHigh = latestExtreme(highs, recentStart, lastIndex, 'max');
        if (recentLow === null || recentHigh === null) continue;

        const stopAnchor =
            params.direction === 'BULLISH'
                ? Math.min(pullbackSwingLow, recentLow)
                : Math.max(pullbackSwingHigh, recentHigh);
        const zoneLow = Math.min(params.ema20[lastIndex] ?? close(lastCandle), params.ema50[lastIndex] ?? close(lastCandle));
        const zoneHigh = Math.max(params.ema20[lastIndex] ?? close(lastCandle), params.ema50[lastIndex] ?? close(lastCandle));
        if (!(Number.isFinite(stopAnchor) && stopAnchor > 0 && Number.isFinite(zoneLow) && Number.isFinite(zoneHigh))) continue;

        return {
            found: true,
            direction: params.direction,
            impulseIndex,
            pullbackIndex,
            entryIndex: lastIndex,
            stopAnchor,
            zoneLow,
            zoneHigh: zoneHigh > zoneLow ? zoneHigh : zoneLow + Math.max(1e-9, (params.atr[lastIndex] ?? 0) * 0.01),
            trigger: trigger1 ? 'PULLBACK_SWING_BREAK' : 'EMA20_RECLAIM_STRONG',
            reasonCodes: [
                'SETUP_IMPULSE_DETECTED',
                'SETUP_PULLBACK_CONFIRMED',
                trigger1 ? 'ENTRY_TRIGGER_PULLBACK_SWING_BREAK' : 'ENTRY_TRIGGER_EMA20_RECLAIM_STRONG',
            ],
        };
    }

    return null;
}

function applyPhaseDetectorsWithOptions(
    input: ScalpStrategyPhaseInput,
    options: RegimePullbackStrategyOptions,
): ScalpStrategyPhaseOutput {
    let next = withLastProcessed(input.state, input.market);
    const reasonCodes: string[] = [];

    if (next.state === 'IN_TRADE' || next.state === 'COOLDOWN') {
        return finalizePhase({
            state: next,
            reasonCodes: ['STATE_SKIPPED_MANAGED_EXTERNALLY'],
        });
    }
    if (next.state === 'DONE') {
        return finalizePhase({
            state: next,
            reasonCodes: ['DAY_ALREADY_DONE'],
        });
    }

    if (input.market.baseTf !== options.requiredBaseTf || input.market.confirmTf !== options.requiredConfirmTf) {
        next.state = 'DONE';
        return finalizePhase({
            state: next,
            reasonCodes: [`STRATEGY_REQUIRES_${options.requiredBaseTf}_${options.requiredConfirmTf}_TIMEFRAMES`],
        });
    }

    const entrySessionProfile = normalizeScalpEntrySessionProfile(input.cfg.sessions.entrySessionProfile, 'berlin');
    const sessionProfileReason = `SESSION_PROFILE_${entrySessionProfile.toUpperCase()}`;
    if (!inScalpEntrySessionProfileWindow(input.nowMs, entrySessionProfile)) {
        next.state = 'IDLE';
        next.sweep = null;
        next.confirmation = null;
        next.ifvg = null;
        reasonCodes.push('SESSION_FILTER_OUTSIDE_ENTRY_PROFILE');
        reasonCodes.push(sessionProfileReason);
        // Legacy alias for existing UI/reporting compatibility.
        reasonCodes.push('SESSION_FILTER_OUTSIDE_BERLIN_WINDOWS');
        return finalizePhase({ state: next, reasonCodes });
    }
    reasonCodes.push('SESSION_FILTER_PASSED');
    reasonCodes.push(sessionProfileReason);
    const blockedHours = normalizeBerlinEntryHours(
        input.cfg.sessions.blockedBerlinEntryHours?.length ? input.cfg.sessions.blockedBerlinEntryHours : options.blockedBerlinEntryHours,
    );
    const blockedHour = blockedBerlinEntryHour(input.nowMs, blockedHours);
    if (blockedHour !== null) {
        next.state = 'IDLE';
        next.sweep = null;
        next.confirmation = null;
        next.ifvg = null;
        reasonCodes.push('SESSION_FILTER_BLOCKED_BERLIN_HOUR');
        reasonCodes.push(`SESSION_FILTER_BLOCKED_BERLIN_HOUR_${blockedHour}`);
        return finalizePhase({ state: next, reasonCodes });
    }

    const regime = scanRegime(input.market.baseCandles);
    reasonCodes.push(...regime.reasonCodes);
    if (!regime.direction) {
        next.state = 'IDLE';
        next.sweep = null;
        next.confirmation = null;
        next.ifvg = null;
        return finalizePhase({ state: next, reasonCodes });
    }

    const vol = scanVolatility(input.market.confirmCandles);
    reasonCodes.push(...vol.reasonCodes);
    if (!vol.ok) {
        next.state = 'IDLE';
        next.sweep = null;
        next.confirmation = null;
        next.ifvg = null;
        return finalizePhase({ state: next, reasonCodes });
    }

    const confirmCloses = input.market.confirmCandles.map(close);
    const ema20 = computeEmaSeries(confirmCloses, STRATEGY_CONST.emaPullback3);
    const ema50 = computeEmaSeries(confirmCloses, STRATEGY_CONST.emaTrend3);
    const atr = computeAtrSeries(input.market.confirmCandles, STRATEGY_CONST.atrLen3);
    const lastIndex = input.market.confirmCandles.length - 1;
    if (!(lastIndex >= 2)) {
        next.state = 'IDLE';
        return finalizePhase({
            state: next,
            reasonCodes: [...reasonCodes, 'SETUP_INSUFFICIENT_M3_CANDLES'],
        });
    }

    const ema20Last = ema20[lastIndex] ?? NaN;
    const ema50Last = ema50[lastIndex] ?? NaN;
    if (!(Number.isFinite(ema20Last) && Number.isFinite(ema50Last))) {
        next.state = 'IDLE';
        return finalizePhase({
            state: next,
            reasonCodes: [...reasonCodes, 'SETUP_EMA_INVALID'],
        });
    }

    const alignOk = regime.direction === 'BULLISH' ? ema20Last >= ema50Last : ema20Last <= ema50Last;
    if (!alignOk) {
        next.state = 'IDLE';
        return finalizePhase({
            state: next,
            reasonCodes: [...reasonCodes, 'M3_ALIGNMENT_FILTER_BLOCKED'],
        });
    }
    reasonCodes.push('M3_ALIGNMENT_FILTER_PASSED');

    const setup = detectEntrySetup({
        candles: input.market.confirmCandles,
        direction: regime.direction,
        ema20,
        ema50,
        atr,
        allowPullbackSwingBreakTrigger: input.cfg.confirm.allowPullbackSwingBreakTrigger ?? options.allowPullbackSwingBreakTrigger,
    });
    if (!setup?.found) {
        next.state = 'IDLE';
        next.sweep = null;
        next.confirmation = null;
        next.ifvg = null;
        reasonCodes.push('SETUP_NOT_READY');
        return finalizePhase({ state: next, reasonCodes });
    }

    reasonCodes.push(...setup.reasonCodes);
    const entryTsMs = ts(input.market.confirmCandles[setup.entryIndex]!);
    const direction = setup.direction;
    const sweepSide = direction === 'BULLISH' ? 'SELL_SIDE' : 'BUY_SIDE';
    const ifvgLow = Math.min(setup.zoneLow, setup.zoneHigh);
    const ifvgHigh = Math.max(setup.zoneLow, setup.zoneHigh);

    next.sweep = {
        side: sweepSide,
        sweepTsMs: entryTsMs,
        sweepPrice: setup.stopAnchor,
        bufferAbs: 0,
        rejected: true,
        rejectedTsMs: entryTsMs,
        reasonCodes: [setup.trigger],
    };
    next.confirmation = {
        displacementDetected: true,
        displacementTsMs: entryTsMs,
        structureShiftDetected: true,
        structureShiftTsMs: entryTsMs,
        reasonCodes: [setup.trigger],
    };
    next.ifvg = {
        direction,
        low: ifvgLow,
        high: ifvgHigh > ifvgLow ? ifvgHigh : ifvgLow + Math.max(1e-9, vol.atrCurrent * 0.01),
        createdTsMs: entryTsMs,
        expiresAtMs: entryTsMs + Math.max(1, input.cfg.ifvg.ttlMinutes) * 60_000,
        entryMode: input.cfg.ifvg.entryMode,
        touched: true,
    };
    next.state = 'WAITING_RETRACE';
    reasonCodes.push('ENTRY_SIGNAL_READY');

    return finalizePhase({
        state: next,
        reasonCodes,
        entryIntent: { model: 'ifvg_touch' },
    });
}

export const REGIME_PULLBACK_M15_M3_STRATEGY_ID = 'regime_pullback_m15_m3';

export function buildRegimePullbackM15M3Strategy(
    overrides: Partial<RegimePullbackStrategyOptions> = {},
): ScalpStrategyDefinition {
    const blockedBerlinEntryHours = normalizeBerlinEntryHours(overrides.blockedBerlinEntryHours);
    const options: RegimePullbackStrategyOptions = {
        id: REGIME_PULLBACK_M15_M3_STRATEGY_ID,
        shortName: 'Regime Pullback',
        longName: 'Regime-Filtered Trend Pullback Continuation (M15/M3)',
        allowPullbackSwingBreakTrigger: true,
        requiredBaseTf: 'M15',
        requiredConfirmTf: 'M3',
        ...overrides,
        blockedBerlinEntryHours,
    };
    return {
        id: options.id,
        shortName: options.shortName,
        longName: options.longName,
        preferredBaseTf: options.requiredBaseTf,
        preferredConfirmTf: options.requiredConfirmTf,
        applyPhaseDetectors(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput {
            return applyPhaseDetectorsWithOptions(input, options);
        },
    };
}

export const regimePullbackM15M3Strategy = buildRegimePullbackM15M3Strategy();
