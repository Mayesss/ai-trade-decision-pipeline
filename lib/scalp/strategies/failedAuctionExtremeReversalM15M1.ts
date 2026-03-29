import type { ScalpBaseTimeframe, ScalpCandle, ScalpConfirmTimeframe, ScalpDirectionalBias, ScalpSessionState } from '../types';
import { inScalpEntrySessionProfileWindow, normalizeScalpEntrySessionProfile } from '../sessions';
import type { ScalpStrategyDefinition, ScalpStrategyEntryIntent, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';

type FailedAuctionStrategyOptions = {
    id: string;
    shortName: string;
    longName: string;
    requiredBaseTf: ScalpBaseTimeframe;
    requiredConfirmTf: ScalpConfirmTimeframe;
};

type FailedAuctionSetup = {
    found: boolean;
    direction: ScalpDirectionalBias;
    rejectionTsMs: number;
    entryTsMs: number;
    stopAnchor: number;
    referenceHigh: number;
    referenceLow: number;
    zoneLow: number;
    zoneHigh: number;
    reasonCodes: string[];
};

const STRATEGY_CONST = {
    atrLen15: 14,
    recentExtremeLookbackBars: 4,
    rejectionMinRangeAtrMult: 0.8,
    rejectionMinWickBodyRatio: 1.2,
    breachBufferAtrMult: 0.05,
    reclaimCloseBufferAtrMult: 0.02,
    retestMaxMinutes: 20,
    retestToleranceAtrMult: 0.15,
    invalidationAtrMult: 0.18,
    confirmCloseBufferAtrMult: 0.03,
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

function upperWick(candle: ScalpCandle): number {
    return Math.max(0, high(candle) - Math.max(open(candle), close(candle)));
}

function lowerWick(candle: ScalpCandle): number {
    return Math.max(0, Math.min(open(candle), close(candle)) - low(candle));
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

function detectFailedAuctionSetup(params: {
    windows: ScalpStrategyPhaseInput['windows'];
    baseCandles: ScalpCandle[];
    confirmCandles: ScalpCandle[];
    nowMs: number;
}): FailedAuctionSetup | null {
    const atrSeries = computeAtrSeries(params.baseCandles, STRATEGY_CONST.atrLen15);
    if (params.baseCandles.length < STRATEGY_CONST.recentExtremeLookbackBars + 2) return null;

    for (let index = params.baseCandles.length - 1; index >= STRATEGY_CONST.recentExtremeLookbackBars; index -= 1) {
        const rejectionCandle = params.baseCandles[index]!;
        const rejectionTsMs = ts(rejectionCandle);
        if (rejectionTsMs < params.windows.raidStartMs || rejectionTsMs > params.windows.raidEndMs) continue;

        const referenceWindow = params.baseCandles.slice(index - STRATEGY_CONST.recentExtremeLookbackBars, index);
        if (referenceWindow.length < STRATEGY_CONST.recentExtremeLookbackBars) continue;

        const atrAbs = atrSeries[index] ?? 0;
        if (!(atrAbs > 0)) continue;

        const referenceHigh = Math.max(...referenceWindow.map(high));
        const referenceLow = Math.min(...referenceWindow.map(low));
        const rangeAbs = candleRange(rejectionCandle);
        const bodyAbs = Math.max(candleBody(rejectionCandle), atrAbs * 0.01);
        const breachBuffer = STRATEGY_CONST.breachBufferAtrMult * atrAbs;
        const reclaimBuffer = STRATEGY_CONST.reclaimCloseBufferAtrMult * atrAbs;

        const failedAuctionHigh =
            high(rejectionCandle) > referenceHigh + breachBuffer &&
            close(rejectionCandle) < referenceHigh - reclaimBuffer &&
            close(rejectionCandle) < open(rejectionCandle) &&
            rangeAbs >= STRATEGY_CONST.rejectionMinRangeAtrMult * atrAbs &&
            upperWick(rejectionCandle) / bodyAbs >= STRATEGY_CONST.rejectionMinWickBodyRatio;
        const failedAuctionLow =
            low(rejectionCandle) < referenceLow - breachBuffer &&
            close(rejectionCandle) > referenceLow + reclaimBuffer &&
            close(rejectionCandle) > open(rejectionCandle) &&
            rangeAbs >= STRATEGY_CONST.rejectionMinRangeAtrMult * atrAbs &&
            lowerWick(rejectionCandle) / bodyAbs >= STRATEGY_CONST.rejectionMinWickBodyRatio;
        if (!(failedAuctionHigh || failedAuctionLow)) continue;

        const direction: ScalpDirectionalBias = failedAuctionHigh ? 'BEARISH' : 'BULLISH';
        const extremeLevel = failedAuctionHigh ? referenceHigh : referenceLow;
        const confirmEndMs = Math.min(
            params.nowMs,
            rejectionTsMs + STRATEGY_CONST.retestMaxMinutes * 60_000,
            params.windows.raidEndMs,
        );
        const confirmWindow = params.confirmCandles.filter((c) => ts(c) > rejectionTsMs && ts(c) <= confirmEndMs);
        if (!confirmWindow.length) continue;

        const toleranceAbs = STRATEGY_CONST.retestToleranceAtrMult * atrAbs;
        const invalidationAbs = STRATEGY_CONST.invalidationAtrMult * atrAbs;
        const confirmBuffer = STRATEGY_CONST.confirmCloseBufferAtrMult * atrAbs;
        let touchedRetest = false;

        for (let i = 0; i < confirmWindow.length; i += 1) {
            const candle = confirmWindow[i]!;
            const invalidated =
                direction === 'BEARISH'
                    ? high(candle) > high(rejectionCandle) + invalidationAbs
                    : low(candle) < low(rejectionCandle) - invalidationAbs;
            if (invalidated) break;

            const touched =
                direction === 'BEARISH'
                    ? high(candle) >= extremeLevel - toleranceAbs
                    : low(candle) <= extremeLevel + toleranceAbs;
            if (touched) touchedRetest = true;
            if (!touchedRetest) continue;

            const confirmed =
                direction === 'BEARISH'
                    ? close(candle) < extremeLevel - confirmBuffer && close(candle) < open(candle)
                    : close(candle) > extremeLevel + confirmBuffer && close(candle) > open(candle);
            if (!confirmed) continue;

            const seenCandles = confirmWindow.slice(0, i + 1);
            const stopAnchor =
                direction === 'BEARISH'
                    ? Math.max(high(rejectionCandle), ...seenCandles.map(high))
                    : Math.min(low(rejectionCandle), ...seenCandles.map(low));
            const zoneLow = extremeLevel - Math.max(toleranceAbs, atrAbs * 0.04);
            const zoneHigh = extremeLevel + Math.max(toleranceAbs, atrAbs * 0.04);
            if (!(Number.isFinite(stopAnchor) && stopAnchor > 0 && Number.isFinite(zoneLow) && Number.isFinite(zoneHigh))) {
                continue;
            }

            return {
                found: true,
                direction,
                rejectionTsMs,
                entryTsMs: ts(candle),
                stopAnchor,
                referenceHigh,
                referenceLow,
                zoneLow: Math.min(zoneLow, zoneHigh),
                zoneHigh: Math.max(zoneLow, zoneHigh),
                reasonCodes: [
                    failedAuctionHigh ? 'FAILED_AUCTION_HIGH_CONFIRMED' : 'FAILED_AUCTION_LOW_CONFIRMED',
                    'FAILED_AUCTION_RETEST_CONFIRMED',
                ],
            };
        }
    }

    return null;
}

function applyPhaseDetectorsWithOptions(
    input: ScalpStrategyPhaseInput,
    options: FailedAuctionStrategyOptions,
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

    const setup = detectFailedAuctionSetup({
        windows: input.windows,
        baseCandles: input.market.baseCandles,
        confirmCandles: input.market.confirmCandles,
        nowMs: input.nowMs,
    });
    if (!setup?.found) {
        next.state = input.nowMs > input.windows.raidEndMs ? 'DONE' : 'IDLE';
        next.sweep = null;
        next.confirmation = null;
        next.ifvg = null;
        reasonCodes.push(input.nowMs > input.windows.raidEndMs ? 'FAILED_AUCTION_WINDOW_CLOSED_NO_SETUP' : 'SETUP_NOT_READY');
        return finalizePhase({ state: next, reasonCodes });
    }

    next.asiaRange = {
        timezone: input.windows.timezone,
        sourceTf: input.market.baseTf,
        startUtcIso: new Date(Math.max(input.windows.raidStartMs, setup.rejectionTsMs - STRATEGY_CONST.recentExtremeLookbackBars * 15 * 60_000)).toISOString(),
        endUtcIso: new Date(setup.rejectionTsMs).toISOString(),
        high: setup.referenceHigh,
        low: setup.referenceLow,
        candleCount: STRATEGY_CONST.recentExtremeLookbackBars,
        builtAtMs: input.nowMs,
    };

    reasonCodes.push(...setup.reasonCodes);
    const sweepSide = setup.direction === 'BULLISH' ? 'SELL_SIDE' : 'BUY_SIDE';
    next.sweep = {
        side: sweepSide,
        sweepTsMs: setup.rejectionTsMs,
        sweepPrice: setup.stopAnchor,
        bufferAbs: 0,
        rejected: true,
        rejectedTsMs: setup.entryTsMs,
        reasonCodes: setup.reasonCodes.slice(),
    };
    next.confirmation = {
        displacementDetected: true,
        displacementTsMs: setup.rejectionTsMs,
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

export const FAILED_AUCTION_EXTREME_REVERSAL_M15_M1_STRATEGY_ID = 'failed_auction_extreme_reversal_m15_m1';

export function buildFailedAuctionExtremeReversalM15M1Strategy(
    overrides: Partial<FailedAuctionStrategyOptions> = {},
): ScalpStrategyDefinition {
    const options: FailedAuctionStrategyOptions = {
        id: FAILED_AUCTION_EXTREME_REVERSAL_M15_M1_STRATEGY_ID,
        shortName: 'Failed Auction',
        longName: 'Failed Auction Extreme Reversal (M15/M1)',
        requiredBaseTf: 'M15',
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

export const failedAuctionExtremeReversalM15M1Strategy = buildFailedAuctionExtremeReversalM15M1Strategy();
