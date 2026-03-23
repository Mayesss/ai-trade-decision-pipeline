import type { ScalpStrategyDefinition, ScalpStrategyPhaseInput, ScalpStrategyPhaseOutput } from './types';
import {
    close,
    computeAtrSeries,
    computeEmaSeries,
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
    minBaseCandles: 140,
    atrLen: 14,
    emaSlowLen: 100,
    dislocationAtrMult: 2.2,
    reclaimBufferAtrMult: 0.08,
    stopLookbackBars: 10,
    stopBufferAtrMult: 0.2,
    zoneHalfWidthAtrMult: 0.1,
} as const;

function detectBasisDislocationProxy(input: ScalpStrategyPhaseInput): SyntheticSignal | null {
    const base = input.market.baseCandles;
    const confirm = input.market.confirmCandles;
    if (base.length < STRATEGY_CONST.minBaseCandles || confirm.length < 2) return null;

    const baseAtrSeries = computeAtrSeries(base, STRATEGY_CONST.atrLen);
    const atr = baseAtrSeries[baseAtrSeries.length - 1] ?? 0;
    if (!(atr > 0)) return null;
    const baseCloses = base.map((candle) => close(candle));
    const emaSlow = computeEmaSeries(baseCloses, STRATEGY_CONST.emaSlowLen);
    const emaNow = emaSlow[emaSlow.length - 1] ?? NaN;
    if (!(Number.isFinite(emaNow) && emaNow > 0)) return null;

    const lastBase = base[base.length - 1]!;
    const dislocationAtr = (close(lastBase) - emaNow) / atr;
    const confirmLast = confirm[confirm.length - 1]!;
    const confirmPrev = confirm[confirm.length - 2]!;
    const reclaimBuffer = STRATEGY_CONST.reclaimBufferAtrMult * atr;
    const recentConfirm = confirm.slice(-Math.max(STRATEGY_CONST.stopLookbackBars + 2, 12));
    const recentLow = Math.min(...recentConfirm.map((candle) => low(candle)));
    const recentHigh = Math.max(...recentConfirm.map((candle) => high(candle)));

    if (
        dislocationAtr <= -STRATEGY_CONST.dislocationAtrMult &&
        close(confirmLast) > high(confirmPrev) + reclaimBuffer &&
        close(confirmLast) > open(confirmLast)
    ) {
        const entryPrice = close(confirmLast);
        const stopAnchor = recentLow - STRATEGY_CONST.stopBufferAtrMult * atr;
        const zoneHalf = Math.max(atr * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.00025);
        return {
            direction: 'BULLISH',
            signalTsMs: ts(confirmLast),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: ['BASIS_PROXY_DISLOCATION_NEGATIVE_EXTREME', 'BASIS_PROXY_RECLAIM_LONG'],
        };
    }

    if (
        dislocationAtr >= STRATEGY_CONST.dislocationAtrMult &&
        close(confirmLast) < low(confirmPrev) - reclaimBuffer &&
        close(confirmLast) < open(confirmLast)
    ) {
        const entryPrice = close(confirmLast);
        const stopAnchor = recentHigh + STRATEGY_CONST.stopBufferAtrMult * atr;
        const zoneHalf = Math.max(atr * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.00025);
        return {
            direction: 'BEARISH',
            signalTsMs: ts(confirmLast),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: ['BASIS_PROXY_DISLOCATION_POSITIVE_EXTREME', 'BASIS_PROXY_RECLAIM_SHORT'],
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

    const signal = detectBasisDislocationProxy(input);
    if (!signal) {
        return finalizePhase({
            state: next,
            reasonCodes: ['BASIS_PROXY_NO_SETUP'],
        });
    }
    return toSyntheticSignalPhase({
        state: next,
        cfg: input.cfg,
        signal,
    });
}

export const BASIS_DISLOCATION_REVERSION_PROXY_M15_M3_STRATEGY_ID = 'basis_dislocation_reversion_proxy_m15_m3';

export const basisDislocationReversionProxyM15M3Strategy: ScalpStrategyDefinition = {
    id: BASIS_DISLOCATION_REVERSION_PROXY_M15_M3_STRATEGY_ID,
    shortName: 'Basis Proxy Reversion',
    longName: 'Perp Basis Dislocation Proxy Reversion (M15/M3)',
    preferredBaseTf: 'M15',
    preferredConfirmTf: 'M3',
    applyPhaseDetectors,
};
