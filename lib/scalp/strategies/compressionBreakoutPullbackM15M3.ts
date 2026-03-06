import type { ScalpCandle, ScalpDirectionalBias, ScalpSessionState } from '../types';
import type { ScalpStrategyDefinition, ScalpStrategyEntryIntent, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';

type CompressionBreakoutStrategyOptions = {
    id: string;
    shortName: string;
    longName: string;
};

type CompressionSetup = {
    found: boolean;
    direction: ScalpDirectionalBias;
    breakoutIndex: number;
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
    compressionLookback15: 5,
    compressionMaxRangeAtrMult: 1.2,
    compressionMaxAvgRangeAtrMult: 0.8,
    breakoutMinRangeAtrMult: 0.8,
    breakoutMinBodyPct: 0.55,
    breakoutCloseBufferAtrMult: 0.06,
    pullbackMaxConfirmBars: 8,
    retestToleranceAtrMult: 0.18,
    invalidationAtrMult: 0.2,
    reclaimCloseBufferAtrMult: 0.03,
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

function averageRange(candles: ScalpCandle[]): number {
    if (!candles.length) return 0;
    return candles.reduce((sum, candle) => sum + candleRange(candle), 0) / candles.length;
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

function detectCompressionBreakoutSetup(params: {
    baseCandles: ScalpCandle[];
    confirmCandles: ScalpCandle[];
}): CompressionSetup | null {
    const atrSeries = computeAtrSeries(params.baseCandles, STRATEGY_CONST.atrLen15);
    if (params.baseCandles.length < STRATEGY_CONST.compressionLookback15 + 2) return null;

    for (let breakoutIndex = params.baseCandles.length - 1; breakoutIndex >= STRATEGY_CONST.compressionLookback15; breakoutIndex -= 1) {
        const breakoutCandle = params.baseCandles[breakoutIndex]!;
        const atrAbs = atrSeries[breakoutIndex] ?? 0;
        if (!(atrAbs > 0)) continue;

        const compressionCandles = params.baseCandles.slice(
            breakoutIndex - STRATEGY_CONST.compressionLookback15,
            breakoutIndex,
        );
        if (compressionCandles.length < STRATEGY_CONST.compressionLookback15) continue;

        const compressionHigh = Math.max(...compressionCandles.map(high));
        const compressionLow = Math.min(...compressionCandles.map(low));
        const compressionRange = compressionHigh - compressionLow;
        const avgCompressionRange = averageRange(compressionCandles);
        if (
            !(compressionRange > 0) ||
            compressionRange > STRATEGY_CONST.compressionMaxRangeAtrMult * atrAbs ||
            avgCompressionRange > STRATEGY_CONST.compressionMaxAvgRangeAtrMult * atrAbs
        ) {
            continue;
        }

        const breakoutRange = candleRange(breakoutCandle);
        const breakoutBody = candleBody(breakoutCandle);
        const closeBuffer = STRATEGY_CONST.breakoutCloseBufferAtrMult * atrAbs;
        const bullish =
            close(breakoutCandle) > compressionHigh + closeBuffer &&
            close(breakoutCandle) > open(breakoutCandle) &&
            breakoutRange >= STRATEGY_CONST.breakoutMinRangeAtrMult * atrAbs &&
            breakoutBody >= STRATEGY_CONST.breakoutMinBodyPct * breakoutRange;
        const bearish =
            close(breakoutCandle) < compressionLow - closeBuffer &&
            close(breakoutCandle) < open(breakoutCandle) &&
            breakoutRange >= STRATEGY_CONST.breakoutMinRangeAtrMult * atrAbs &&
            breakoutBody >= STRATEGY_CONST.breakoutMinBodyPct * breakoutRange;
        if (!(bullish || bearish)) continue;

        const direction: ScalpDirectionalBias = bullish ? 'BULLISH' : 'BEARISH';
        const breakoutLevel = bullish ? compressionHigh : compressionLow;
        const toleranceAbs = STRATEGY_CONST.retestToleranceAtrMult * atrAbs;
        const invalidationAbs = STRATEGY_CONST.invalidationAtrMult * atrAbs;
        const reclaimBuffer = STRATEGY_CONST.reclaimCloseBufferAtrMult * atrAbs;
        const breakoutTsMs = ts(breakoutCandle);
        const maxConfirmTsMs = breakoutTsMs + STRATEGY_CONST.pullbackMaxConfirmBars * 3 * 60_000;
        const confirmWindow = params.confirmCandles.filter((c) => ts(c) > breakoutTsMs && ts(c) <= maxConfirmTsMs);
        if (!confirmWindow.length) continue;

        let touchedRetest = false;
        for (let i = 0; i < confirmWindow.length; i += 1) {
            const candle = confirmWindow[i]!;
            const invalidated =
                direction === 'BULLISH'
                    ? low(candle) < compressionLow - invalidationAbs
                    : high(candle) > compressionHigh + invalidationAbs;
            if (invalidated) break;

            const touched =
                direction === 'BULLISH'
                    ? low(candle) <= breakoutLevel + toleranceAbs && high(candle) >= breakoutLevel - toleranceAbs
                    : high(candle) >= breakoutLevel - toleranceAbs && low(candle) <= breakoutLevel + toleranceAbs;
            if (touched) touchedRetest = true;
            if (!touchedRetest) continue;

            const reclaimed =
                direction === 'BULLISH'
                    ? close(candle) > breakoutLevel + reclaimBuffer && close(candle) > open(candle)
                    : close(candle) < breakoutLevel - reclaimBuffer && close(candle) < open(candle);
            if (!reclaimed) continue;

            const seenCandles = confirmWindow.slice(0, i + 1);
            const stopAnchor =
                direction === 'BULLISH'
                    ? Math.min(compressionLow, ...seenCandles.map(low))
                    : Math.max(compressionHigh, ...seenCandles.map(high));
            const zoneLow = direction === 'BULLISH' ? breakoutLevel - toleranceAbs : breakoutLevel - Math.max(toleranceAbs, atrAbs * 0.04);
            const zoneHigh = direction === 'BULLISH' ? breakoutLevel + Math.max(toleranceAbs, atrAbs * 0.04) : breakoutLevel + toleranceAbs;
            if (!(Number.isFinite(stopAnchor) && stopAnchor > 0 && Number.isFinite(zoneLow) && Number.isFinite(zoneHigh))) {
                continue;
            }

            return {
                found: true,
                direction,
                breakoutIndex,
                entryTsMs: ts(candle),
                stopAnchor,
                zoneLow: Math.min(zoneLow, zoneHigh),
                zoneHigh: Math.max(zoneLow, zoneHigh),
                reasonCodes: [
                    'COMPRESSION_WINDOW_DETECTED',
                    bullish ? 'BREAKOUT_BULL_CONFIRMED' : 'BREAKOUT_BEAR_CONFIRMED',
                    'PULLBACK_RETEST_CONFIRMED',
                ],
            };
        }
    }

    return null;
}

function applyPhaseDetectorsWithOptions(
    input: ScalpStrategyPhaseInput,
    _options: CompressionBreakoutStrategyOptions,
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

    const setup = detectCompressionBreakoutSetup({
        baseCandles: input.market.baseCandles,
        confirmCandles: input.market.confirmCandles,
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
    const breakoutTsMs = ts(input.market.baseCandles[setup.breakoutIndex]!);
    const sweepSide = setup.direction === 'BULLISH' ? 'SELL_SIDE' : 'BUY_SIDE';

    next.sweep = {
        side: sweepSide,
        sweepTsMs: breakoutTsMs,
        sweepPrice: setup.stopAnchor,
        bufferAbs: 0,
        rejected: true,
        rejectedTsMs: setup.entryTsMs,
        reasonCodes: setup.reasonCodes.slice(),
    };
    next.confirmation = {
        displacementDetected: true,
        displacementTsMs: breakoutTsMs,
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

export const COMPRESSION_BREAKOUT_PULLBACK_M15_M3_STRATEGY_ID = 'compression_breakout_pullback_m15_m3';

export function buildCompressionBreakoutPullbackM15M3Strategy(
    overrides: Partial<CompressionBreakoutStrategyOptions> = {},
): ScalpStrategyDefinition {
    const options: CompressionBreakoutStrategyOptions = {
        id: COMPRESSION_BREAKOUT_PULLBACK_M15_M3_STRATEGY_ID,
        shortName: 'Compression Breakout',
        longName: 'Compression Breakout Pullback Continuation (M15/M3)',
        ...overrides,
    };
    return {
        id: options.id,
        shortName: options.shortName,
        longName: options.longName,
        applyPhaseDetectors(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput {
            return applyPhaseDetectorsWithOptions(input, options);
        },
    };
}

export const compressionBreakoutPullbackM15M3Strategy = buildCompressionBreakoutPullbackM15M3Strategy();
