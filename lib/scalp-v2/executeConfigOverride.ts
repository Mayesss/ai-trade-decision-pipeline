import type { ScalpStrategyConfigOverride } from "../scalp/config";

import type { EntryTriggerOverrides } from "./entryTriggerPresets";
import type { ExitRuleOverrides } from "./exitRulePresets";
import type { RiskRuleReplayOverrides } from "./riskRulePresets";
import type { StateMachineOverrides } from "./stateMachinePresets";
import type { ScalpV2RiskProfile, ScalpV2Session } from "./types";

function isDefined<T>(value: T | undefined): value is T {
  return value !== undefined;
}

export function buildScalpV2ExecuteConfigOverride(params: {
  entrySessionProfile: ScalpV2Session;
  riskProfile: ScalpV2RiskProfile;
  entryTriggerOverrides?: EntryTriggerOverrides;
  exitRuleOverrides?: ExitRuleOverrides;
  riskRuleReplayOverrides?: RiskRuleReplayOverrides;
  stateMachineOverrides: StateMachineOverrides;
}): ScalpStrategyConfigOverride {
  const entry = params.entryTriggerOverrides || {};
  const exit = params.exitRuleOverrides || {};
  const riskRule = params.riskRuleReplayOverrides || {};
  const sm = params.stateMachineOverrides || {};
  const dailyLossLimitR =
    sm.dailyLossLimitR ?? riskRule.dailyLossLimitR ?? params.riskProfile.autoPauseDailyR;
  const maxTradesPerSymbolPerDay =
    sm.maxTradesPerSymbolPerDay ?? riskRule.maxTradesPerDay;

  return {
    // Always bind execution to the deployment session profile.
    sessions: {
      entrySessionProfile: params.entrySessionProfile,
    },
    risk: {
      riskPerTradePct: params.riskProfile.riskPerTradePct ?? riskRule.riskPerTradePct,
      maxOpenPositionsPerSymbol: params.riskProfile.maxOpenPositionsPerSymbol,
      dailyLossLimitR,
      ...(isDefined(maxTradesPerSymbolPerDay) && {
        maxTradesPerSymbolPerDay,
      }),
      ...(isDefined(exit.takeProfitR) && {
        takeProfitR: exit.takeProfitR,
      }),
      ...(isDefined(exit.tp1R) && {
        tp1R: exit.tp1R,
      }),
      ...(isDefined(exit.tp1ClosePct) && {
        tp1ClosePct: exit.tp1ClosePct,
      }),
      ...(isDefined(exit.breakEvenOffsetR) && {
        breakEvenOffsetR: exit.breakEvenOffsetR,
      }),
      ...(isDefined(exit.trailStartR) && {
        trailStartR: exit.trailStartR,
      }),
      ...(isDefined(exit.trailAtrMult) && {
        trailAtrMult: exit.trailAtrMult,
      }),
      ...(isDefined(exit.timeStopBars) && {
        timeStopBars: exit.timeStopBars,
      }),
      ...(sm.consecutiveLossPauseThreshold !== undefined && {
        consecutiveLossPauseThreshold: sm.consecutiveLossPauseThreshold,
      }),
      ...(sm.consecutiveLossCooldownBars !== undefined && {
        consecutiveLossCooldownBars: sm.consecutiveLossCooldownBars,
      }),
    },
    confirm: {
      ...(isDefined(entry.displacementBodyAtrMult) && {
        displacementBodyAtrMult: entry.displacementBodyAtrMult,
      }),
      ...(isDefined(entry.displacementRangeAtrMult) && {
        displacementRangeAtrMult: entry.displacementRangeAtrMult,
      }),
      ...(isDefined(entry.mssLookbackBars) && {
        mssLookbackBars: entry.mssLookbackBars,
      }),
      ...(isDefined(entry.confirmTtlMinutes) && {
        ttlMinutes: entry.confirmTtlMinutes,
      }),
      ...(isDefined(entry.allowPullbackSwingBreakTrigger) && {
        allowPullbackSwingBreakTrigger: entry.allowPullbackSwingBreakTrigger,
      }),
      ...(sm.confirmTtlMinutes !== undefined && {
        ttlMinutes: sm.confirmTtlMinutes,
      }),
    },
    ifvg: {
      ...(isDefined(entry.ifvgMinAtrMult) && {
        minAtrMult: entry.ifvgMinAtrMult,
      }),
      ...(isDefined(entry.ifvgMaxAtrMult) && {
        maxAtrMult: entry.ifvgMaxAtrMult,
      }),
      ...(isDefined(entry.ifvgTtlMinutes) && {
        ttlMinutes: entry.ifvgTtlMinutes,
      }),
      ...(isDefined(entry.ifvgEntryMode) && {
        entryMode: entry.ifvgEntryMode,
      }),
    },
    sweep: {
      ...(isDefined(entry.sweepBufferPips) && {
        bufferPips: entry.sweepBufferPips,
      }),
      ...(isDefined(entry.sweepBufferAtrMult) && {
        bufferAtrMult: entry.sweepBufferAtrMult,
      }),
      ...(isDefined(entry.sweepRejectMaxBars) && {
        rejectMaxBars: entry.sweepRejectMaxBars,
      }),
      ...(isDefined(entry.sweepMinWickBodyRatio) && {
        minWickBodyRatio: entry.sweepMinWickBodyRatio,
      }),
      ...(sm.sweepRejectMaxBars !== undefined && {
        rejectMaxBars: sm.sweepRejectMaxBars,
      }),
    },
  };
}
