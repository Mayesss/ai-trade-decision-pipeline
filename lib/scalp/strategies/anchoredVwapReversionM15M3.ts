import type { ScalpCandle } from '../types';
import type { ScalpStrategyDefinition, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';
import {
    close,
    computeAtrSeries,
    finalizePhase,
    high,
    low,
    open,
    toSyntheticSignalPhase,
    ts,
    type SyntheticSignal,
    withLastProcessed,
} from './syntheticSignal';

const STRATEGY_CONST = {
    minConfirmCandles: 80,
    minAnchorCandles: 20,
    atrLen: 14,
    deviationAtrMult: 1.15,
    reclaimBufferAtrMult: 0.1,
    stopLookbackBars: 8,
    stopBufferAtrMult: 0.2,
    zoneHalfWidthAtrMult: 0.1,
} as const;

function anchoredVwap(candles: ScalpCandle[]): number | null {
    if (!candles.length) return null;
    let pv = 0;
    let v = 0;
    for (const candle of candles) {
        const typical = (high(candle) + low(candle) + close(candle)) / 3;
        const volume = Number(candle[5]) > 0 ? Number(candle[5]) : 1;
        pv += typical * volume;
        v += volume;
    }
    if (!(v > 0)) return null;
    return pv / v;
}

function detectAnchoredVwapReversion(input: ScalpStrategyPhaseInput): SyntheticSignal | null {
    const confirm = input.market.confirmCandles;
    if (confirm.length < STRATEGY_CONST.minConfirmCandles) return null;
    const atrSeries = computeAtrSeries(confirm, STRATEGY_CONST.atrLen);
    const atr = atrSeries[atrSeries.length - 1] ?? 0;
    if (!(atr > 0)) return null;

    const anchorCandles = confirm.filter((candle) => ts(candle) >= input.windows.asiaStartMs && ts(candle) <= input.nowMs);
    const effectiveAnchor =
        anchorCandles.length >= STRATEGY_CONST.minAnchorCandles
            ? anchorCandles
            : confirm.slice(-Math.max(STRATEGY_CONST.minConfirmCandles, STRATEGY_CONST.minAnchorCandles));
    const vwap = anchoredVwap(effectiveAnchor);
    if (!(Number.isFinite(vwap) && (vwap as number) > 0)) return null;

    const last = confirm[confirm.length - 1]!;
    const prev = confirm[confirm.length - 2]!;
    const lookback = confirm.slice(-Math.max(STRATEGY_CONST.stopLookbackBars + 2, 12));
    const minRecent = Math.min(...lookback.map((candle) => low(candle)));
    const maxRecent = Math.max(...lookback.map((candle) => high(candle)));
    const lowerBand = (vwap as number) - STRATEGY_CONST.deviationAtrMult * atr;
    const upperBand = (vwap as number) + STRATEGY_CONST.deviationAtrMult * atr;
    const reclaimBuffer = STRATEGY_CONST.reclaimBufferAtrMult * atr;

    const bullishOvershoot = Math.min(...lookback.map((candle) => low(candle))) <= lowerBand;
    const bullishReclaim =
        close(prev) < lowerBand &&
        close(last) > lowerBand + reclaimBuffer &&
        close(last) > open(last);
    if (bullishOvershoot && bullishReclaim) {
        const entryPrice = close(last);
        const stopAnchor = minRecent - STRATEGY_CONST.stopBufferAtrMult * atr;
        const zoneHalf = Math.max(atr * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.00025);
        return {
            direction: 'BULLISH',
            signalTsMs: ts(last),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: ['ANCHORED_VWAP_LOWER_BAND_RECLAIM', 'ANCHORED_VWAP_MEAN_REVERSION_LONG'],
        };
    }

    const bearishOvershoot = Math.max(...lookback.map((candle) => high(candle))) >= upperBand;
    const bearishReclaim =
        close(prev) > upperBand &&
        close(last) < upperBand - reclaimBuffer &&
        close(last) < open(last);
    if (bearishOvershoot && bearishReclaim) {
        const entryPrice = close(last);
        const stopAnchor = maxRecent + STRATEGY_CONST.stopBufferAtrMult * atr;
        const zoneHalf = Math.max(atr * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.00025);
        return {
            direction: 'BEARISH',
            signalTsMs: ts(last),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: ['ANCHORED_VWAP_UPPER_BAND_RECLAIM', 'ANCHORED_VWAP_MEAN_REVERSION_SHORT'],
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

    const signal = detectAnchoredVwapReversion(input);
    if (!signal) {
        return finalizePhase({
            state: next,
            reasonCodes: ['ANCHORED_VWAP_NO_SETUP'],
        });
    }
    return toSyntheticSignalPhase({
        state: next,
        cfg: input.cfg,
        signal,
    });
}

export const ANCHORED_VWAP_REVERSION_M15_M3_STRATEGY_ID = 'anchored_vwap_reversion_m15_m3';

export const anchoredVwapReversionM15M3Strategy: ScalpStrategyDefinition = {
    id: ANCHORED_VWAP_REVERSION_M15_M3_STRATEGY_ID,
    shortName: 'Anchored VWAP Reversion',
    longName: 'Anchored VWAP Mean Reversion (M15/M3)',
    preferredBaseTf: 'M15',
    preferredConfirmTf: 'M3',
    applyPhaseDetectors,
};
