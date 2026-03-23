import type { ScalpCandle } from '../types';
import type { ScalpStrategyDefinition, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';
import {
    candleRange,
    close,
    computeAtrSeries,
    finalizePhase,
    high,
    low,
    median,
    open,
    toSyntheticSignalPhase,
    ts,
    type SyntheticSignal,
    withLastProcessed,
} from './syntheticSignal';

const STRATEGY_CONST = {
    minBaseCandles: 60,
    atrLen: 14,
    spikeRangeAtrMult: 1.8,
    spikeVolumeMult: 1.6,
    closeNearExtremePct: 0.2,
    stopBufferAtrMult: 0.15,
    zoneHalfWidthAtrMult: 0.12,
} as const;

function volume(candle: ScalpCandle): number {
    const raw = Number(candle[5]);
    return Number.isFinite(raw) ? Math.max(0, raw) : 0;
}

function closeNearLowPct(candle: ScalpCandle): number {
    const range = Math.max(candleRange(candle), Number.EPSILON);
    return (close(candle) - low(candle)) / range;
}

function closeNearHighPct(candle: ScalpCandle): number {
    const range = Math.max(candleRange(candle), Number.EPSILON);
    return (high(candle) - close(candle)) / range;
}

function detectFundingOiExhaustionProxy(input: ScalpStrategyPhaseInput): SyntheticSignal | null {
    const base = input.market.baseCandles;
    const confirm = input.market.confirmCandles;
    if (base.length < STRATEGY_CONST.minBaseCandles || confirm.length < 2) return null;
    const atrSeries = computeAtrSeries(base, STRATEGY_CONST.atrLen);
    const spike = base[base.length - 2]!;
    const follow = base[base.length - 1]!;
    const atrSpike = atrSeries[atrSeries.length - 2] ?? 0;
    if (!(atrSpike > 0)) return null;

    const volumeLookback = base.slice(-42, -2).map((candle) => volume(candle)).filter((value) => value > 0);
    const medianVol = volumeLookback.length ? median(volumeLookback) : 0;
    if (!(medianVol > 0)) return null;
    const isVolumeSpike = volume(spike) >= STRATEGY_CONST.spikeVolumeMult * medianVol;
    const isRangeSpike = candleRange(spike) >= STRATEGY_CONST.spikeRangeAtrMult * atrSpike;
    if (!(isVolumeSpike && isRangeSpike)) return null;

    const confirmLast = confirm[confirm.length - 1]!;
    const followMid = (open(spike) + close(spike)) / 2;

    const longExhaustion =
        close(spike) < open(spike) &&
        closeNearLowPct(spike) <= STRATEGY_CONST.closeNearExtremePct &&
        close(follow) > followMid &&
        close(confirmLast) > open(confirmLast);
    if (longExhaustion) {
        const entryPrice = close(confirmLast);
        const stopAnchor = Math.min(low(spike), low(follow)) - STRATEGY_CONST.stopBufferAtrMult * atrSpike;
        const zoneHalf = Math.max(atrSpike * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.0003);
        return {
            direction: 'BULLISH',
            signalTsMs: ts(confirmLast),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: ['FUNDING_OI_PROXY_LONG_EXHAUSTION', 'CROWDING_FLUSH_REVERSAL_LONG'],
        };
    }

    const shortExhaustion =
        close(spike) > open(spike) &&
        closeNearHighPct(spike) <= STRATEGY_CONST.closeNearExtremePct &&
        close(follow) < followMid &&
        close(confirmLast) < open(confirmLast);
    if (shortExhaustion) {
        const entryPrice = close(confirmLast);
        const stopAnchor = Math.max(high(spike), high(follow)) + STRATEGY_CONST.stopBufferAtrMult * atrSpike;
        const zoneHalf = Math.max(atrSpike * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.0003);
        return {
            direction: 'BEARISH',
            signalTsMs: ts(confirmLast),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: ['FUNDING_OI_PROXY_SHORT_EXHAUSTION', 'CROWDING_FLUSH_REVERSAL_SHORT'],
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

    const signal = detectFundingOiExhaustionProxy(input);
    if (!signal) {
        return finalizePhase({
            state: next,
            reasonCodes: ['FUNDING_OI_PROXY_NO_SETUP'],
        });
    }
    return toSyntheticSignalPhase({
        state: next,
        cfg: input.cfg,
        signal,
    });
}

export const FUNDING_OI_EXHAUSTION_PROXY_M15_M3_STRATEGY_ID = 'funding_oi_exhaustion_proxy_m15_m3';

export const fundingOiExhaustionProxyM15M3Strategy: ScalpStrategyDefinition = {
    id: FUNDING_OI_EXHAUSTION_PROXY_M15_M3_STRATEGY_ID,
    shortName: 'Funding/OI Proxy',
    longName: 'Funding/OI Exhaustion Proxy Reversal (M15/M3)',
    preferredBaseTf: 'M15',
    preferredConfirmTf: 'M3',
    applyPhaseDetectors,
};
