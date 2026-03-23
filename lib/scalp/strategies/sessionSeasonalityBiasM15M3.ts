import type { ScalpCandle } from '../types';
import type { ScalpStrategyDefinition, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';
import {
    close,
    computeAtrSeries,
    finalizePhase,
    high,
    low,
    mean,
    open,
    stddev,
    toSyntheticSignalPhase,
    ts,
    type SyntheticSignal,
    withLastProcessed,
} from './syntheticSignal';

const STRATEGY_CONST = {
    minConfirmCandles: 180,
    atrLen: 14,
    minSamples: 12,
    minHitRate: 0.58,
    minExpectancyAtr: 0.08,
    breakoutBufferAtrMult: 0.08,
    stopLookbackBars: 8,
    stopBufferAtrMult: 0.25,
    zoneHalfWidthAtrMult: 0.1,
} as const;

function minuteBucket(candleTsMs: number, intervalMinutes: number): number {
    const date = new Date(candleTsMs);
    const minute = date.getUTCMinutes();
    const bucket = Math.floor(minute / Math.max(1, intervalMinutes));
    return bucket;
}

function detectSessionSeasonalityBias(input: ScalpStrategyPhaseInput): SyntheticSignal | null {
    const confirm = input.market.confirmCandles;
    if (confirm.length < STRATEGY_CONST.minConfirmCandles) return null;
    const atrSeries = computeAtrSeries(confirm, STRATEGY_CONST.atrLen);
    const lastAtr = atrSeries[atrSeries.length - 1] ?? 0;
    if (!(lastAtr > 0)) return null;

    const intervalMinutes = input.market.confirmTf === 'M1' ? 1 : 3;
    const last = confirm[confirm.length - 1]!;
    const prev = confirm[confirm.length - 2]!;
    const currentBucket = minuteBucket(ts(last), intervalMinutes);

    const normalizedReturns: number[] = [];
    for (let i = 1; i < confirm.length - 1; i += 1) {
        const candle = confirm[i]!;
        if (minuteBucket(ts(candle), intervalMinutes) !== currentBucket) continue;
        const atr = atrSeries[i] ?? 0;
        if (!(atr > 0)) continue;
        const ret = (close(candle) - open(candle)) / atr;
        if (Number.isFinite(ret)) normalizedReturns.push(ret);
    }
    if (normalizedReturns.length < STRATEGY_CONST.minSamples) return null;

    const expectancy = mean(normalizedReturns);
    const volatility = stddev(normalizedReturns);
    const positiveHitRate = normalizedReturns.filter((value) => value > 0).length / normalizedReturns.length;
    const negativeHitRate = normalizedReturns.filter((value) => value < 0).length / normalizedReturns.length;
    const breakoutBuffer = STRATEGY_CONST.breakoutBufferAtrMult * lastAtr;
    const lookback = confirm.slice(-Math.max(STRATEGY_CONST.stopLookbackBars + 2, 10));
    const recentLow = Math.min(...lookback.map((candle) => low(candle)));
    const recentHigh = Math.max(...lookback.map((candle) => high(candle)));

    if (
        expectancy >= STRATEGY_CONST.minExpectancyAtr &&
        positiveHitRate >= STRATEGY_CONST.minHitRate &&
        close(last) > high(prev) + breakoutBuffer &&
        close(last) > open(last)
    ) {
        const entryPrice = close(last);
        const stopAnchor = recentLow - STRATEGY_CONST.stopBufferAtrMult * lastAtr;
        const zoneHalf = Math.max(lastAtr * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.00025);
        return {
            direction: 'BULLISH',
            signalTsMs: ts(last),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: [
                'SESSION_SEASONALITY_POSITIVE_BUCKET',
                `SESSION_SEASONALITY_EXPECTANCY_${expectancy.toFixed(3)}`,
                `SESSION_SEASONALITY_VOL_${volatility.toFixed(3)}`,
            ],
        };
    }

    if (
        expectancy <= -STRATEGY_CONST.minExpectancyAtr &&
        negativeHitRate >= STRATEGY_CONST.minHitRate &&
        close(last) < low(prev) - breakoutBuffer &&
        close(last) < open(last)
    ) {
        const entryPrice = close(last);
        const stopAnchor = recentHigh + STRATEGY_CONST.stopBufferAtrMult * lastAtr;
        const zoneHalf = Math.max(lastAtr * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.00025);
        return {
            direction: 'BEARISH',
            signalTsMs: ts(last),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: [
                'SESSION_SEASONALITY_NEGATIVE_BUCKET',
                `SESSION_SEASONALITY_EXPECTANCY_${expectancy.toFixed(3)}`,
                `SESSION_SEASONALITY_VOL_${volatility.toFixed(3)}`,
            ],
        };
    }

    return null;
}

function applyPhaseDetectors(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput {
    const next = withLastProcessed(input.state, input.market);
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

    const signal = detectSessionSeasonalityBias(input);
    if (!signal) {
        return finalizePhase({
            state: next,
            reasonCodes: ['SESSION_SEASONALITY_NO_SETUP'],
        });
    }
    return toSyntheticSignalPhase({
        state: next,
        cfg: input.cfg,
        signal,
    });
}

export const SESSION_SEASONALITY_BIAS_M15_M3_STRATEGY_ID = 'session_seasonality_bias_m15_m3';

export const sessionSeasonalityBiasM15M3Strategy: ScalpStrategyDefinition = {
    id: SESSION_SEASONALITY_BIAS_M15_M3_STRATEGY_ID,
    shortName: 'Session Seasonality',
    longName: 'Session Seasonality Bias Breakout (M15/M3)',
    preferredBaseTf: 'M15',
    preferredConfirmTf: 'M3',
    applyPhaseDetectors,
};
