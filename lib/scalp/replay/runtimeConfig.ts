import { applyScalpStrategyConfigOverride, getScalpStrategyConfig, type ScalpStrategyConfigOverride } from '../config';
import type { ScalpDeploymentRef } from '../types';
import { getScalpStrategyPreferredTimeframes } from '../strategies/registry';
import { resolveScalpExecutionStrategyId } from '../composer/composerExecution';
import { defaultScalpReplayConfig } from './harness';
import type { ScalpReplayRuntimeConfig } from './types';

export function buildScalpReplayRuntimeFromDeployment(params: {
    deployment: ScalpDeploymentRef;
    configOverride?: ScalpStrategyConfigOverride | null;
    baseRuntime?: ScalpReplayRuntimeConfig;
}): ScalpReplayRuntimeConfig {
    const base = params.baseRuntime || defaultScalpReplayConfig(params.deployment.symbol);
    const executionStrategyId = resolveScalpExecutionStrategyId({
        strategyId: params.deployment.strategyId,
        tuneId: params.deployment.tuneId,
    });
    const preferredTimeframes = getScalpStrategyPreferredTimeframes(
        executionStrategyId || params.deployment.strategyId,
    );
    const cfg = applyScalpStrategyConfigOverride(
        getScalpStrategyConfig(),
        (params.configOverride || undefined) as ScalpStrategyConfigOverride | undefined,
    );

    return {
        ...base,
        symbol: params.deployment.symbol,
        strategyId: params.deployment.strategyId,
        tuneId: params.deployment.tuneId,
        deploymentId: params.deployment.deploymentId,
        tuneLabel: params.deployment.tuneLabel,
        configOverride: params.configOverride
            ? (JSON.parse(JSON.stringify(params.configOverride)) as ScalpStrategyConfigOverride)
            : null,
        executeMinutes: cfg.cadence.executeMinutes,
        strategy: {
            ...base.strategy,
            sessionClockMode: cfg.sessions.clockMode,
            entrySessionProfile: cfg.sessions.entrySessionProfile,
            asiaWindowLocal: cfg.sessions.asiaWindowLocal,
            raidWindowLocal: cfg.sessions.raidWindowLocal,
            blockedBerlinEntryHours: cfg.sessions.blockedBerlinEntryHours,
            // Temporal-filter fields. `base.strategy` was built from the
            // pre-override global config so it carries the global default
            // (usually undefined). Re-copy the post-override values from
            // cfg.sessions so deployment-specific slot/weekday/hour
            // restrictions (set via configOverride) actually reach the
            // replay harness, which reads these straight off runtime.strategy
            // at lib/scalp/replay/harness.ts:123-127.
            allowedSessionWindowSlots: cfg.sessions.allowedSessionWindowSlots,
            sessionSlotMinutes: cfg.sessions.sessionSlotMinutes,
            allowedWeekdaysLocal: cfg.sessions.allowedWeekdaysLocal,
            allowedUtcHours: cfg.sessions.allowedUtcHours,
            entryBlockReasonCodes: cfg.sessions.entryBlockReasonCodes,
            asiaBaseTf: preferredTimeframes?.asiaBaseTf ?? cfg.timeframes.asiaBase,
            confirmTf: preferredTimeframes?.confirmTf ?? cfg.timeframes.confirm,
            maxTradesPerDay: cfg.risk.maxTradesPerSymbolPerDay,
            riskPerTradePct: cfg.risk.riskPerTradePct,
            referenceEquityUsd: cfg.risk.referenceEquityUsd,
            minNotionalUsd: cfg.risk.minNotionalUsd,
            maxNotionalUsd: cfg.risk.maxNotionalUsd,
            takeProfitR: cfg.risk.takeProfitR,
            stopBufferPips: cfg.risk.stopBufferPips,
            stopBufferSpreadMult: cfg.risk.stopBufferSpreadMult,
            breakEvenOffsetR: cfg.risk.breakEvenOffsetR,
            tp1R: cfg.risk.tp1R,
            tp1ClosePct: cfg.risk.tp1ClosePct,
            trailStartR: cfg.risk.trailStartR,
            trailAtrMult: cfg.risk.trailAtrMult,
            timeStopBars: cfg.risk.timeStopBars,
            dailyLossLimitR: cfg.risk.dailyLossLimitR,
            consecutiveLossPauseThreshold: cfg.risk.consecutiveLossPauseThreshold,
            consecutiveLossCooldownBars: cfg.risk.consecutiveLossCooldownBars,
            minStopDistancePips: cfg.risk.minStopDistancePips,
            sweepBufferPips: cfg.sweep.bufferPips,
            sweepBufferAtrMult: cfg.sweep.bufferAtrMult,
            sweepBufferSpreadMult: cfg.sweep.bufferSpreadMult,
            sweepRejectInsidePips: cfg.sweep.rejectInsidePips,
            sweepRejectMaxBars: cfg.sweep.rejectMaxBars,
            sweepMinWickBodyRatio: cfg.sweep.minWickBodyRatio,
            displacementBodyAtrMult: cfg.confirm.displacementBodyAtrMult,
            displacementRangeAtrMult: cfg.confirm.displacementRangeAtrMult,
            displacementCloseInExtremePct: cfg.confirm.closeInExtremePct,
            mssLookbackBars: cfg.confirm.mssLookbackBars,
            mssBreakBufferPips: cfg.confirm.mssBreakBufferPips,
            mssBreakBufferAtrMult: cfg.confirm.mssBreakBufferAtrMult,
            confirmTtlMinutes: cfg.confirm.ttlMinutes,
            allowPullbackSwingBreakTrigger: cfg.confirm.allowPullbackSwingBreakTrigger,
            ifvgMinAtrMult: cfg.ifvg.minAtrMult,
            ifvgMaxAtrMult: cfg.ifvg.maxAtrMult,
            ifvgTtlMinutes: cfg.ifvg.ttlMinutes,
            ifvgEntryMode: cfg.ifvg.entryMode,
            atrPeriod: cfg.data.atrPeriod,
            minAsiaCandles: cfg.data.minAsiaCandles,
            minBaseCandles: cfg.data.minBaseCandles,
            minConfirmCandles: cfg.data.minConfirmCandles,
        },
    };
}
