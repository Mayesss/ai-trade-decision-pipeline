import type { ScalpBaseTimeframe, ScalpCandle, ScalpConfirmTimeframe, ScalpDirectionalBias, ScalpSessionState, ScalpSessionWindows } from '../types';
import type { ScalpStrategyDefinition, ScalpStrategyEntryIntent, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';

type OpeningRangeBreakoutStrategyOptions = {
    id: string;
    shortName: string;
    longName: string;
    requiredBaseTf: ScalpBaseTimeframe;
    requiredConfirmTf: ScalpConfirmTimeframe;
};

type OpeningRangeSnapshot = {
    high: number;
    low: number;
    startMs: number;
    endMs: number;
};

type OpeningRangeSetup = {
    found: boolean;
    direction: ScalpDirectionalBias;
    breakoutTsMs: number;
    entryTsMs: number;
    stopAnchor: number;
    breakoutLevel: number;
    zoneLow: number;
    zoneHigh: number;
    reasonCodes: string[];
};

const STRATEGY_CONST = {
    openingRangeBars5: 3,
    atrLen5: 14,
    breakoutMinRangeAtrMult: 0.75,
    breakoutMinBodyPct: 0.55,
    breakoutCloseBufferAtrMult: 0.04,
    retestMaxMinutes: 20,
    retestToleranceAtrMult: 0.15,
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

function buildOpeningRange(windows: ScalpSessionWindows, baseCandles: ScalpCandle[]): {
    snapshot: OpeningRangeSnapshot | null;
    reasonCodes: string[];
} {
    const rangeEndMs = windows.raidStartMs + STRATEGY_CONST.openingRangeBars5 * 5 * 60_000;
    const rangeCandles = baseCandles.filter((c) => ts(c) >= windows.raidStartMs && ts(c) < rangeEndMs);
    if (rangeCandles.length < STRATEGY_CONST.openingRangeBars5) {
        return {
            snapshot: null,
            reasonCodes: ['OPENING_RANGE_PENDING'],
        };
    }
    const rangeHigh = Math.max(...rangeCandles.map(high));
    const rangeLow = Math.min(...rangeCandles.map(low));
    if (!(Number.isFinite(rangeHigh) && Number.isFinite(rangeLow) && rangeHigh > rangeLow)) {
        return {
            snapshot: null,
            reasonCodes: ['OPENING_RANGE_INVALID'],
        };
    }
    return {
        snapshot: {
            high: rangeHigh,
            low: rangeLow,
            startMs: windows.raidStartMs,
            endMs: rangeEndMs,
        },
        reasonCodes: ['OPENING_RANGE_READY'],
    };
}

function detectOpeningRangeBreakoutSetup(params: {
    windows: ScalpSessionWindows;
    openingRange: OpeningRangeSnapshot;
    baseCandles: ScalpCandle[];
    confirmCandles: ScalpCandle[];
    nowMs: number;
}): OpeningRangeSetup | null {
    const atrSeries = computeAtrSeries(params.baseCandles, STRATEGY_CONST.atrLen5);
    const breakoutWindowEndMs = Math.min(params.nowMs, params.windows.raidEndMs);
    const breakoutCandidates = params.baseCandles.filter(
        (c) => ts(c) >= params.openingRange.endMs && ts(c) <= breakoutWindowEndMs,
    );
    if (!breakoutCandidates.length) return null;

    for (const breakoutCandle of breakoutCandidates) {
        const breakoutIndex = params.baseCandles.findIndex((c) => ts(c) === ts(breakoutCandle));
        if (breakoutIndex < 0) continue;
        const atrAbs = atrSeries[breakoutIndex] ?? 0;
        if (!(atrAbs > 0)) continue;

        const breakoutRange = candleRange(breakoutCandle);
        const breakoutBody = candleBody(breakoutCandle);
        const closeBuffer = STRATEGY_CONST.breakoutCloseBufferAtrMult * atrAbs;
        const bullishBreakout =
            close(breakoutCandle) > params.openingRange.high + closeBuffer &&
            close(breakoutCandle) > open(breakoutCandle) &&
            breakoutRange >= STRATEGY_CONST.breakoutMinRangeAtrMult * atrAbs &&
            breakoutBody >= STRATEGY_CONST.breakoutMinBodyPct * breakoutRange;
        const bearishBreakout =
            close(breakoutCandle) < params.openingRange.low - closeBuffer &&
            close(breakoutCandle) < open(breakoutCandle) &&
            breakoutRange >= STRATEGY_CONST.breakoutMinRangeAtrMult * atrAbs &&
            breakoutBody >= STRATEGY_CONST.breakoutMinBodyPct * breakoutRange;
        if (!(bullishBreakout || bearishBreakout)) continue;

        const direction: ScalpDirectionalBias = bullishBreakout ? 'BULLISH' : 'BEARISH';
        const breakoutLevel = bullishBreakout ? params.openingRange.high : params.openingRange.low;
        const breakoutTsMs = ts(breakoutCandle);
        const confirmEndMs = Math.min(
            params.nowMs,
            breakoutTsMs + STRATEGY_CONST.retestMaxMinutes * 60_000,
            params.windows.raidEndMs,
        );
        const confirmWindow = params.confirmCandles.filter((c) => ts(c) > breakoutTsMs && ts(c) <= confirmEndMs);
        if (!confirmWindow.length) continue;

        const toleranceAbs = STRATEGY_CONST.retestToleranceAtrMult * atrAbs;
        const invalidationAbs = STRATEGY_CONST.invalidationAtrMult * atrAbs;
        const reclaimBuffer = STRATEGY_CONST.reclaimCloseBufferAtrMult * atrAbs;
        let touchedRetest = false;

        for (let i = 0; i < confirmWindow.length; i += 1) {
            const candle = confirmWindow[i]!;
            const invalidated =
                direction === 'BULLISH'
                    ? low(candle) < params.openingRange.low - invalidationAbs
                    : high(candle) > params.openingRange.high + invalidationAbs;
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
                    ? Math.min(params.openingRange.low, ...seenCandles.map(low))
                    : Math.max(params.openingRange.high, ...seenCandles.map(high));
            const zoneLow = breakoutLevel - Math.max(toleranceAbs, atrAbs * 0.04);
            const zoneHigh = breakoutLevel + Math.max(toleranceAbs, atrAbs * 0.04);
            if (!(Number.isFinite(stopAnchor) && stopAnchor > 0 && Number.isFinite(zoneLow) && Number.isFinite(zoneHigh))) {
                continue;
            }

            return {
                found: true,
                direction,
                breakoutTsMs,
                entryTsMs: ts(candle),
                stopAnchor,
                breakoutLevel,
                zoneLow: Math.min(zoneLow, zoneHigh),
                zoneHigh: Math.max(zoneLow, zoneHigh),
                reasonCodes: [
                    'OPENING_RANGE_BREAKOUT_CONFIRMED',
                    bullishBreakout ? 'OPENING_RANGE_BREAKOUT_BULL' : 'OPENING_RANGE_BREAKOUT_BEAR',
                    'OPENING_RANGE_RETEST_RECLAIM_CONFIRMED',
                ],
            };
        }
    }

    return null;
}

function applyPhaseDetectorsWithOptions(
    input: ScalpStrategyPhaseInput,
    options: OpeningRangeBreakoutStrategyOptions,
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

    if (input.nowMs < input.windows.raidStartMs) {
        next.state = 'IDLE';
        reasonCodes.push('OPENING_RANGE_WAITING_SESSION_START');
        return finalizePhase({ state: next, reasonCodes });
    }

    const openingRange = buildOpeningRange(input.windows, input.market.baseCandles);
    reasonCodes.push(...openingRange.reasonCodes);
    if (!openingRange.snapshot) {
        next.state = 'IDLE';
        return finalizePhase({ state: next, reasonCodes });
    }

    if (!next.asiaRange) {
        next.asiaRange = {
            timezone: input.windows.timezone,
            sourceTf: input.market.baseTf,
            startUtcIso: new Date(openingRange.snapshot.startMs).toISOString(),
            endUtcIso: new Date(openingRange.snapshot.endMs).toISOString(),
            high: openingRange.snapshot.high,
            low: openingRange.snapshot.low,
            candleCount: STRATEGY_CONST.openingRangeBars5,
            builtAtMs: input.nowMs,
        };
    }

    const setup = detectOpeningRangeBreakoutSetup({
        windows: input.windows,
        openingRange: openingRange.snapshot,
        baseCandles: input.market.baseCandles,
        confirmCandles: input.market.confirmCandles,
        nowMs: input.nowMs,
    });
    if (!setup?.found) {
        next.state = input.nowMs > input.windows.raidEndMs ? 'DONE' : 'ASIA_RANGE_READY';
        reasonCodes.push(input.nowMs > input.windows.raidEndMs ? 'OPENING_RANGE_WINDOW_CLOSED_NO_SETUP' : 'SETUP_NOT_READY');
        return finalizePhase({ state: next, reasonCodes });
    }

    reasonCodes.push(...setup.reasonCodes);
    const sweepSide = setup.direction === 'BULLISH' ? 'SELL_SIDE' : 'BUY_SIDE';

    next.sweep = {
        side: sweepSide,
        sweepTsMs: setup.breakoutTsMs,
        sweepPrice: setup.stopAnchor,
        bufferAbs: 0,
        rejected: true,
        rejectedTsMs: setup.entryTsMs,
        reasonCodes: setup.reasonCodes.slice(),
    };
    next.confirmation = {
        displacementDetected: true,
        displacementTsMs: setup.breakoutTsMs,
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

export const OPENING_RANGE_BREAKOUT_RETEST_M5_M1_STRATEGY_ID = 'opening_range_breakout_retest_m5_m1';

export function buildOpeningRangeBreakoutRetestM5M1Strategy(
    overrides: Partial<OpeningRangeBreakoutStrategyOptions> = {},
): ScalpStrategyDefinition {
    const options: OpeningRangeBreakoutStrategyOptions = {
        id: OPENING_RANGE_BREAKOUT_RETEST_M5_M1_STRATEGY_ID,
        shortName: 'OR Breakout Retest',
        longName: 'Opening Range Breakout Retest (M5/M1)',
        requiredBaseTf: 'M5',
        requiredConfirmTf: 'M1',
        ...overrides,
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

export const openingRangeBreakoutRetestM5M1Strategy = buildOpeningRangeBreakoutRetestM5M1Strategy();
