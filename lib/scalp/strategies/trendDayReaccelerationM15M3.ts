import type { ScalpCandle, ScalpDirectionalBias, ScalpSessionState } from '../types';
import type { ScalpStrategyDefinition, ScalpStrategyEntryIntent, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';

type TrendDayReaccelerationStrategyOptions = {
    id: string;
    shortName: string;
    longName: string;
};

type TrendDaySetup = {
    found: boolean;
    direction: ScalpDirectionalBias;
    impulseTsMs: number;
    entryTsMs: number;
    stopAnchor: number;
    zoneLow: number;
    zoneHigh: number;
    reasonCodes: string[];
};

const STRATEGY_CONST = {
    sessionsBerlinMinutes: [
        [8 * 60, 12 * 60],
        [14 * 60, 18 * 60],
    ] as Array<[number, number]>,
    atrLen15: 14,
    impulseBreakLookback15: 5,
    impulseMinRangeAtrMult: 0.9,
    impulseMinBodyPct: 0.55,
    impulseCloseBufferAtrMult: 0.04,
    followThroughWindow15: 2,
    followThroughMinHoldPct: 0.55,
    retraceMinPct: 0.2,
    retraceMaxPct: 0.65,
    retraceInvalidPct: 0.8,
    pullbackMaxConfirmBars: 10,
    reclaimCloseBufferAtrMult: 0.03,
    reclaimMinBodyPct: 0.5,
} as const;

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

function candleRange(candle: ScalpCandle): number {
    return Math.max(0, high(candle) - low(candle));
}

function candleBody(candle: ScalpCandle): number {
    return Math.abs(close(candle) - open(candle));
}

function latestTs(candles: ScalpCandle[]): number | null {
    const latest = candles.at(-1)?.[0];
    return Number.isFinite(Number(latest)) ? Number(latest) : null;
}

function dedupeReasonCodes(codes: string[]): string[] {
    return Array.from(new Set(codes.map((code) => String(code || '').trim().toUpperCase()).filter((code) => code.length > 0)));
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

function minuteOfDayInTimeZone(tsMs: number, timeZone: string): number {
    const fmt = new Intl.DateTimeFormat('en-GB', {
        timeZone,
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
    });
    const parts = fmt.formatToParts(new Date(tsMs));
    const hh = Number(parts.find((part) => part.type === 'hour')?.value || '0');
    const mm = Number(parts.find((part) => part.type === 'minute')?.value || '0');
    if (!Number.isFinite(hh) || !Number.isFinite(mm)) return -1;
    return hh * 60 + mm;
}

function inBerlinSessionWindow(nowMs: number): boolean {
    const minuteOfDay = minuteOfDayInTimeZone(nowMs, 'Europe/Berlin');
    if (!(minuteOfDay >= 0)) return false;
    return STRATEGY_CONST.sessionsBerlinMinutes.some(([startMin, endMin]) => minuteOfDay >= startMin && minuteOfDay < endMin);
}

function extreme(values: number[], mode: 'min' | 'max'): number | null {
    if (!values.length) return null;
    return mode === 'min' ? Math.min(...values) : Math.max(...values);
}

function detectTrendDaySetup(params: {
    baseCandles: ScalpCandle[];
    confirmCandles: ScalpCandle[];
    nowMs: number;
}): TrendDaySetup | null {
    const atrSeries = computeAtrSeries(params.baseCandles, STRATEGY_CONST.atrLen15);
    if (params.baseCandles.length < STRATEGY_CONST.impulseBreakLookback15 + 3) return null;

    for (let impulseIndex = params.baseCandles.length - 2; impulseIndex >= STRATEGY_CONST.impulseBreakLookback15; impulseIndex -= 1) {
        const impulseCandle = params.baseCandles[impulseIndex]!;
        const atrAbs = atrSeries[impulseIndex] ?? 0;
        if (!(atrAbs > 0)) continue;

        const lookback = params.baseCandles.slice(impulseIndex - STRATEGY_CONST.impulseBreakLookback15, impulseIndex);
        if (lookback.length < STRATEGY_CONST.impulseBreakLookback15) continue;
        const priorHigh = extreme(lookback.map(high), 'max');
        const priorLow = extreme(lookback.map(low), 'min');
        if (priorHigh === null || priorLow === null) continue;

        const rangeAbs = candleRange(impulseCandle);
        const bodyAbs = candleBody(impulseCandle);
        const closeBuffer = STRATEGY_CONST.impulseCloseBufferAtrMult * atrAbs;
        const bullishImpulse =
            close(impulseCandle) > priorHigh + closeBuffer &&
            close(impulseCandle) > open(impulseCandle) &&
            rangeAbs >= STRATEGY_CONST.impulseMinRangeAtrMult * atrAbs &&
            bodyAbs >= STRATEGY_CONST.impulseMinBodyPct * rangeAbs;
        const bearishImpulse =
            close(impulseCandle) < priorLow - closeBuffer &&
            close(impulseCandle) < open(impulseCandle) &&
            rangeAbs >= STRATEGY_CONST.impulseMinRangeAtrMult * atrAbs &&
            bodyAbs >= STRATEGY_CONST.impulseMinBodyPct * rangeAbs;
        if (!(bullishImpulse || bearishImpulse)) continue;

        const direction: ScalpDirectionalBias = bullishImpulse ? 'BULLISH' : 'BEARISH';
        const impulseHigh = high(impulseCandle);
        const impulseLow = low(impulseCandle);
        const impulseRange = Math.max(impulseHigh - impulseLow, Number.EPSILON);
        const impulseTsMs = ts(impulseCandle);

        let followThroughOk = false;
        const followThroughWindow = params.baseCandles.slice(
            impulseIndex + 1,
            Math.min(params.baseCandles.length, impulseIndex + 1 + STRATEGY_CONST.followThroughWindow15),
        );
        for (const followCandle of followThroughWindow) {
            const holdPct =
                direction === 'BULLISH'
                    ? (close(followCandle) - impulseLow) / impulseRange
                    : (impulseHigh - close(followCandle)) / impulseRange;
            const extendsDirection =
                direction === 'BULLISH' ? high(followCandle) >= impulseHigh : low(followCandle) <= impulseLow;
            if (holdPct >= STRATEGY_CONST.followThroughMinHoldPct || extendsDirection) {
                followThroughOk = true;
                break;
            }
        }
        if (!followThroughOk) continue;

        const retraceLow = impulseHigh - impulseRange * STRATEGY_CONST.retraceMaxPct;
        const retraceHigh = impulseHigh - impulseRange * STRATEGY_CONST.retraceMinPct;
        const shortRetraceLow = impulseLow + impulseRange * STRATEGY_CONST.retraceMinPct;
        const shortRetraceHigh = impulseLow + impulseRange * STRATEGY_CONST.retraceMaxPct;
        const invalidationLevel =
            direction === 'BULLISH'
                ? impulseHigh - impulseRange * STRATEGY_CONST.retraceInvalidPct
                : impulseLow + impulseRange * STRATEGY_CONST.retraceInvalidPct;
        const confirmEndMs = Math.min(params.nowMs, impulseTsMs + STRATEGY_CONST.pullbackMaxConfirmBars * 3 * 60_000);
        const confirmWindow = params.confirmCandles.filter((c) => ts(c) > impulseTsMs && ts(c) <= confirmEndMs);
        if (!confirmWindow.length) continue;

        let touchedPullback = false;
        for (let i = 0; i < confirmWindow.length; i += 1) {
            const candle = confirmWindow[i]!;
            const invalidated =
                direction === 'BULLISH' ? low(candle) < invalidationLevel : high(candle) > invalidationLevel;
            if (invalidated) break;

            const touched =
                direction === 'BULLISH'
                    ? low(candle) <= retraceHigh && high(candle) >= retraceLow
                    : high(candle) >= shortRetraceLow && low(candle) <= shortRetraceHigh;
            if (touched) touchedPullback = true;
            if (!touchedPullback) continue;

            const range = candleRange(candle);
            const reclaim =
                direction === 'BULLISH'
                    ? close(candle) > open(candle) &&
                      close(candle) > retraceHigh + STRATEGY_CONST.reclaimCloseBufferAtrMult * atrAbs &&
                      candleBody(candle) >= STRATEGY_CONST.reclaimMinBodyPct * Math.max(range, atrAbs * 0.05)
                    : close(candle) < open(candle) &&
                      close(candle) < shortRetraceLow - STRATEGY_CONST.reclaimCloseBufferAtrMult * atrAbs &&
                      candleBody(candle) >= STRATEGY_CONST.reclaimMinBodyPct * Math.max(range, atrAbs * 0.05);
            if (!reclaim) continue;

            const seenCandles = confirmWindow.slice(0, i + 1);
            const stopAnchor =
                direction === 'BULLISH'
                    ? Math.min(impulseLow, ...seenCandles.map(low))
                    : Math.max(impulseHigh, ...seenCandles.map(high));
            const zoneLow = direction === 'BULLISH' ? retraceLow : shortRetraceLow;
            const zoneHigh = direction === 'BULLISH' ? retraceHigh : shortRetraceHigh;
            if (!(Number.isFinite(stopAnchor) && stopAnchor > 0 && Number.isFinite(zoneLow) && Number.isFinite(zoneHigh))) {
                continue;
            }

            return {
                found: true,
                direction,
                impulseTsMs,
                entryTsMs: ts(candle),
                stopAnchor,
                zoneLow: Math.min(zoneLow, zoneHigh),
                zoneHigh: Math.max(zoneLow, zoneHigh),
                reasonCodes: [
                    direction === 'BULLISH' ? 'TREND_DAY_BULL_IMPULSE_CONFIRMED' : 'TREND_DAY_BEAR_IMPULSE_CONFIRMED',
                    'TREND_DAY_FOLLOW_THROUGH_CONFIRMED',
                    'TREND_DAY_PULLBACK_RECLAIM_CONFIRMED',
                ],
            };
        }
    }

    return null;
}

function applyPhaseDetectorsWithOptions(
    input: ScalpStrategyPhaseInput,
    _options: TrendDayReaccelerationStrategyOptions,
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

    if (input.market.baseTf !== 'M15' || input.market.confirmTf !== 'M3') {
        next.state = 'DONE';
        return finalizePhase({
            state: next,
            reasonCodes: ['STRATEGY_REQUIRES_M15_M3_TIMEFRAMES'],
        });
    }

    if (!inBerlinSessionWindow(input.nowMs)) {
        next.state = 'IDLE';
        next.sweep = null;
        next.confirmation = null;
        next.ifvg = null;
        reasonCodes.push('SESSION_FILTER_OUTSIDE_BERLIN_WINDOWS');
        return finalizePhase({ state: next, reasonCodes });
    }
    reasonCodes.push('SESSION_FILTER_PASSED');

    const setup = detectTrendDaySetup({
        baseCandles: input.market.baseCandles,
        confirmCandles: input.market.confirmCandles,
        nowMs: input.nowMs,
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
    const sweepSide = setup.direction === 'BULLISH' ? 'SELL_SIDE' : 'BUY_SIDE';
    next.sweep = {
        side: sweepSide,
        sweepTsMs: setup.impulseTsMs,
        sweepPrice: setup.stopAnchor,
        bufferAbs: 0,
        rejected: true,
        rejectedTsMs: setup.entryTsMs,
        reasonCodes: setup.reasonCodes.slice(),
    };
    next.confirmation = {
        displacementDetected: true,
        displacementTsMs: setup.impulseTsMs,
        structureShiftDetected: true,
        structureShiftTsMs: setup.entryTsMs,
        reasonCodes: setup.reasonCodes.slice(),
    };
    next.ifvg = {
        direction: setup.direction,
        low: setup.zoneLow,
        high: setup.zoneHigh > setup.zoneLow ? setup.zoneHigh : setup.zoneLow + 1e-9,
        createdTsMs: setup.entryTsMs,
        expiresAtMs: setup.entryTsMs + Math.max(1, input.cfg.ifvg.ttlMinutes) * 60_000,
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

export const TREND_DAY_REACCELERATION_M15_M3_STRATEGY_ID = 'trend_day_reacceleration_m15_m3';

export function buildTrendDayReaccelerationM15M3Strategy(
    overrides: Partial<TrendDayReaccelerationStrategyOptions> = {},
): ScalpStrategyDefinition {
    const options: TrendDayReaccelerationStrategyOptions = {
        id: TREND_DAY_REACCELERATION_M15_M3_STRATEGY_ID,
        shortName: 'Trend Day Reaccel',
        longName: 'Trend Day Reacceleration (M15/M3)',
        ...overrides,
    };
    return {
        id: options.id,
        shortName: options.shortName,
        longName: options.longName,
        preferredBaseTf: 'M15',
        preferredConfirmTf: 'M3',
        applyPhaseDetectors(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput {
            return applyPhaseDetectorsWithOptions(input, options);
        },
    };
}

export const trendDayReaccelerationM15M3Strategy = buildTrendDayReaccelerationM15M3Strategy();
