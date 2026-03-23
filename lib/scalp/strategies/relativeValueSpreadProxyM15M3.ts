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
    minBaseCandles: 120,
    minConfirmCandles: 120,
    atrLen: 14,
    emaTrendLen: 50,
    baseLookbackBars: 10,
    confirmLookbackBars: 8,
    spreadZThreshold: 1.0,
    reclaimBufferAtrMult: 0.06,
    stopLookbackBars: 9,
    stopBufferAtrMult: 0.22,
    zoneHalfWidthAtrMult: 0.1,
} as const;

function detectRelativeValueSpreadProxy(input: ScalpStrategyPhaseInput): SyntheticSignal | null {
    const base = input.market.baseCandles;
    const confirm = input.market.confirmCandles;
    if (base.length < STRATEGY_CONST.minBaseCandles || confirm.length < STRATEGY_CONST.minConfirmCandles) return null;

    const baseAtrSeries = computeAtrSeries(base, STRATEGY_CONST.atrLen);
    const confirmAtrSeries = computeAtrSeries(confirm, STRATEGY_CONST.atrLen);
    const atrBase = baseAtrSeries[baseAtrSeries.length - 1] ?? 0;
    const atrConfirm = confirmAtrSeries[confirmAtrSeries.length - 1] ?? 0;
    if (!(atrBase > 0 && atrConfirm > 0)) return null;

    const baseCloses = base.map((candle) => close(candle));
    const emaTrend = computeEmaSeries(baseCloses, STRATEGY_CONST.emaTrendLen);
    const emaNow = emaTrend[emaTrend.length - 1] ?? NaN;
    const emaPrev = emaTrend[emaTrend.length - 4] ?? NaN;
    if (!(Number.isFinite(emaNow) && Number.isFinite(emaPrev))) return null;

    const baseLast = base[base.length - 1]!;
    const baseRef = base[Math.max(0, base.length - 1 - STRATEGY_CONST.baseLookbackBars)]!;
    const confirmLast = confirm[confirm.length - 1]!;
    const confirmPrev = confirm[confirm.length - 2]!;
    const confirmRef = confirm[Math.max(0, confirm.length - 1 - STRATEGY_CONST.confirmLookbackBars)]!;

    const baseNormalized = (close(baseLast) - close(baseRef)) / atrBase;
    const confirmNormalized = (close(confirmLast) - close(confirmRef)) / atrConfirm;
    const spread = confirmNormalized - baseNormalized;
    const trendUp = emaNow > emaPrev && close(baseLast) > emaNow;
    const trendDown = emaNow < emaPrev && close(baseLast) < emaNow;
    const reclaimBuffer = STRATEGY_CONST.reclaimBufferAtrMult * atrConfirm;
    const recent = confirm.slice(-Math.max(STRATEGY_CONST.stopLookbackBars + 2, 12));
    const recentLow = Math.min(...recent.map((candle) => low(candle)));
    const recentHigh = Math.max(...recent.map((candle) => high(candle)));

    if (
        trendUp &&
        spread <= -STRATEGY_CONST.spreadZThreshold &&
        close(confirmLast) > high(confirmPrev) + reclaimBuffer &&
        close(confirmLast) > open(confirmLast)
    ) {
        const entryPrice = close(confirmLast);
        const stopAnchor = recentLow - STRATEGY_CONST.stopBufferAtrMult * atrConfirm;
        const zoneHalf = Math.max(atrConfirm * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.00025);
        return {
            direction: 'BULLISH',
            signalTsMs: ts(confirmLast),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: ['RELATIVE_VALUE_PROXY_UNDERPERFORMANCE', `RELATIVE_VALUE_PROXY_SPREAD_${spread.toFixed(3)}`],
        };
    }

    if (
        trendDown &&
        spread >= STRATEGY_CONST.spreadZThreshold &&
        close(confirmLast) < low(confirmPrev) - reclaimBuffer &&
        close(confirmLast) < open(confirmLast)
    ) {
        const entryPrice = close(confirmLast);
        const stopAnchor = recentHigh + STRATEGY_CONST.stopBufferAtrMult * atrConfirm;
        const zoneHalf = Math.max(atrConfirm * STRATEGY_CONST.zoneHalfWidthAtrMult, entryPrice * 0.00025);
        return {
            direction: 'BEARISH',
            signalTsMs: ts(confirmLast),
            entryPrice,
            stopAnchor,
            zoneLow: entryPrice - zoneHalf,
            zoneHigh: entryPrice + zoneHalf,
            reasonCodes: ['RELATIVE_VALUE_PROXY_OVERPERFORMANCE', `RELATIVE_VALUE_PROXY_SPREAD_${spread.toFixed(3)}`],
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

    const signal = detectRelativeValueSpreadProxy(input);
    if (!signal) {
        return finalizePhase({
            state: next,
            reasonCodes: ['RELATIVE_VALUE_PROXY_NO_SETUP'],
        });
    }
    return toSyntheticSignalPhase({
        state: next,
        cfg: input.cfg,
        signal,
    });
}

export const RELATIVE_VALUE_SPREAD_PROXY_M15_M3_STRATEGY_ID = 'relative_value_spread_proxy_m15_m3';

export const relativeValueSpreadProxyM15M3Strategy: ScalpStrategyDefinition = {
    id: RELATIVE_VALUE_SPREAD_PROXY_M15_M3_STRATEGY_ID,
    shortName: 'Relative-Value Proxy',
    longName: 'Relative-Value Spread Proxy Reversion (M15/M3)',
    preferredBaseTf: 'M15',
    preferredConfirmTf: 'M3',
    applyPhaseDetectors,
};
