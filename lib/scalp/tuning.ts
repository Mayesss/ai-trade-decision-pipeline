import type { ScalpStrategyConfigOverride } from './config';

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function toPositiveNumber(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return null;
    return n;
}

function toNonNegativeNumber(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return null;
    return n;
}

function toPositiveInt(value: unknown): number | null {
    const n = toPositiveNumber(value);
    if (n === null) return null;
    return Math.max(1, Math.floor(n));
}

function compactObject(value: unknown): unknown {
    if (Array.isArray(value)) {
        return value.map((entry) => compactObject(entry));
    }
    if (!isRecord(value)) return value;
    const next: Record<string, unknown> = {};
    for (const [key, raw] of Object.entries(value)) {
        if (raw === undefined) continue;
        const compacted = compactObject(raw);
        if (isRecord(compacted) && Object.keys(compacted).length === 0) continue;
        next[key] = compacted;
    }
    return next;
}

function parseEntryMode(value: unknown): 'first_touch' | 'midline_touch' | 'full_fill' | null {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'first_touch' || normalized === 'midline_touch' || normalized === 'full_fill') {
        return normalized;
    }
    return null;
}

export function compactScalpStrategyConfigOverride(value: unknown): ScalpStrategyConfigOverride | null {
    const compacted = compactObject(value);
    if (!isRecord(compacted) || Object.keys(compacted).length === 0) return null;
    return compacted as ScalpStrategyConfigOverride;
}

export function buildScalpConfigOverrideFromEffectiveConfig(
    effectiveConfig: unknown,
    opts: { includeTimeframes?: boolean } = {},
): ScalpStrategyConfigOverride | null {
    if (!isRecord(effectiveConfig)) return null;
    const strategy = isRecord(effectiveConfig.strategy) ? effectiveConfig.strategy : {};

    const risk: Record<string, unknown> = {};
    const maxTrades = toPositiveInt(strategy.maxTradesPerDay);
    if (maxTrades !== null) risk.maxTradesPerSymbolPerDay = maxTrades;
    const riskPct = toPositiveNumber(strategy.riskPerTradePct);
    if (riskPct !== null) risk.riskPerTradePct = riskPct;
    const referenceEquityUsd = toPositiveNumber(strategy.referenceEquityUsd);
    if (referenceEquityUsd !== null) risk.referenceEquityUsd = referenceEquityUsd;
    const minNotionalUsd = toPositiveNumber(strategy.minNotionalUsd);
    if (minNotionalUsd !== null) risk.minNotionalUsd = minNotionalUsd;
    const maxNotionalUsd = toPositiveNumber(strategy.maxNotionalUsd);
    if (maxNotionalUsd !== null) risk.maxNotionalUsd = maxNotionalUsd;
    const takeProfitR = toPositiveNumber(strategy.takeProfitR);
    if (takeProfitR !== null) risk.takeProfitR = takeProfitR;
    const stopBufferPips = toNonNegativeNumber(strategy.stopBufferPips);
    if (stopBufferPips !== null) risk.stopBufferPips = stopBufferPips;
    const stopBufferSpreadMult = toNonNegativeNumber(strategy.stopBufferSpreadMult);
    if (stopBufferSpreadMult !== null) risk.stopBufferSpreadMult = stopBufferSpreadMult;
    const minStopDistancePips = toPositiveNumber(strategy.minStopDistancePips);
    if (minStopDistancePips !== null) risk.minStopDistancePips = minStopDistancePips;
    const breakEvenOffsetR = toNonNegativeNumber(strategy.breakEvenOffsetR);
    if (breakEvenOffsetR !== null) risk.breakEvenOffsetR = breakEvenOffsetR;
    const tp1R = toPositiveNumber(strategy.tp1R);
    if (tp1R !== null) risk.tp1R = tp1R;
    const tp1ClosePct = toNonNegativeNumber(strategy.tp1ClosePct);
    if (tp1ClosePct !== null) risk.tp1ClosePct = Math.max(0, Math.min(100, tp1ClosePct));
    const trailStartR = toPositiveNumber(strategy.trailStartR);
    if (trailStartR !== null) risk.trailStartR = trailStartR;
    const trailAtrMult = toPositiveNumber(strategy.trailAtrMult);
    if (trailAtrMult !== null) risk.trailAtrMult = trailAtrMult;
    const timeStopBars = toPositiveInt(strategy.timeStopBars);
    if (timeStopBars !== null) risk.timeStopBars = timeStopBars;
    const dailyLossLimitR = Number(strategy.dailyLossLimitR);
    if (Number.isFinite(dailyLossLimitR)) risk.dailyLossLimitR = dailyLossLimitR;
    const consecutiveLossPauseThreshold = toPositiveInt(strategy.consecutiveLossPauseThreshold);
    if (consecutiveLossPauseThreshold !== null) risk.consecutiveLossPauseThreshold = consecutiveLossPauseThreshold;
    const consecutiveLossCooldownBars = toNonNegativeNumber(strategy.consecutiveLossCooldownBars);
    if (consecutiveLossCooldownBars !== null) risk.consecutiveLossCooldownBars = Math.floor(consecutiveLossCooldownBars);

    const sweep: Record<string, unknown> = {};
    const sweepBufferPips = toNonNegativeNumber(strategy.sweepBufferPips);
    if (sweepBufferPips !== null) sweep.bufferPips = sweepBufferPips;
    const sweepBufferAtrMult = toNonNegativeNumber(strategy.sweepBufferAtrMult);
    if (sweepBufferAtrMult !== null) sweep.bufferAtrMult = sweepBufferAtrMult;
    const sweepBufferSpreadMult = toNonNegativeNumber(strategy.sweepBufferSpreadMult);
    if (sweepBufferSpreadMult !== null) sweep.bufferSpreadMult = sweepBufferSpreadMult;
    const sweepRejectInsidePips = toNonNegativeNumber(strategy.sweepRejectInsidePips);
    if (sweepRejectInsidePips !== null) sweep.rejectInsidePips = sweepRejectInsidePips;
    const sweepRejectMaxBars = toPositiveInt(strategy.sweepRejectMaxBars);
    if (sweepRejectMaxBars !== null) sweep.rejectMaxBars = sweepRejectMaxBars;
    const sweepMinWickBodyRatio = toNonNegativeNumber(strategy.sweepMinWickBodyRatio);
    if (sweepMinWickBodyRatio !== null) sweep.minWickBodyRatio = sweepMinWickBodyRatio;

    const confirm: Record<string, unknown> = {};
    const displacementBodyAtrMult = toNonNegativeNumber(strategy.displacementBodyAtrMult);
    if (displacementBodyAtrMult !== null) confirm.displacementBodyAtrMult = displacementBodyAtrMult;
    const displacementRangeAtrMult = toNonNegativeNumber(strategy.displacementRangeAtrMult);
    if (displacementRangeAtrMult !== null) confirm.displacementRangeAtrMult = displacementRangeAtrMult;
    const displacementCloseInExtremePct = toPositiveNumber(strategy.displacementCloseInExtremePct);
    if (displacementCloseInExtremePct !== null) confirm.closeInExtremePct = displacementCloseInExtremePct;
    const mssLookbackBars = toPositiveInt(strategy.mssLookbackBars);
    if (mssLookbackBars !== null) confirm.mssLookbackBars = mssLookbackBars;
    const mssBreakBufferPips = toNonNegativeNumber(strategy.mssBreakBufferPips);
    if (mssBreakBufferPips !== null) confirm.mssBreakBufferPips = mssBreakBufferPips;
    const mssBreakBufferAtrMult = toNonNegativeNumber(strategy.mssBreakBufferAtrMult);
    if (mssBreakBufferAtrMult !== null) confirm.mssBreakBufferAtrMult = mssBreakBufferAtrMult;
    const confirmTtlMinutes = toPositiveInt(strategy.confirmTtlMinutes);
    if (confirmTtlMinutes !== null) confirm.ttlMinutes = confirmTtlMinutes;

    const ifvg: Record<string, unknown> = {};
    const ifvgMinAtrMult = toNonNegativeNumber(strategy.ifvgMinAtrMult);
    if (ifvgMinAtrMult !== null) ifvg.minAtrMult = ifvgMinAtrMult;
    const ifvgMaxAtrMult = toPositiveNumber(strategy.ifvgMaxAtrMult);
    if (ifvgMaxAtrMult !== null) ifvg.maxAtrMult = ifvgMaxAtrMult;
    const ifvgTtlMinutes = toPositiveInt(strategy.ifvgTtlMinutes);
    if (ifvgTtlMinutes !== null) ifvg.ttlMinutes = ifvgTtlMinutes;
    const ifvgEntryMode = parseEntryMode(strategy.ifvgEntryMode);
    if (ifvgEntryMode) ifvg.entryMode = ifvgEntryMode;

    const override: Record<string, unknown> = {};
    if (Object.keys(risk).length) override.risk = risk;
    if (Object.keys(sweep).length) override.sweep = sweep;
    if (Object.keys(confirm).length) override.confirm = confirm;
    if (Object.keys(ifvg).length) override.ifvg = ifvg;

    if (opts.includeTimeframes) {
        const timeframes: Record<string, unknown> = {};
        const asiaBaseTf = String(strategy.asiaBaseTf || '')
            .trim()
            .toUpperCase();
        if (asiaBaseTf === 'M1' || asiaBaseTf === 'M3' || asiaBaseTf === 'M5' || asiaBaseTf === 'M15') {
            timeframes.asiaBase = asiaBaseTf;
        }
        const confirmTf = String(strategy.confirmTf || '')
            .trim()
            .toUpperCase();
        if (confirmTf === 'M1' || confirmTf === 'M3') {
            timeframes.confirm = confirmTf;
        }
        if (Object.keys(timeframes).length) override.timeframes = timeframes;
    }

    return compactScalpStrategyConfigOverride(override);
}
