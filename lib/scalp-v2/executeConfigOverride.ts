import type { ScalpStrategyConfigOverride } from "../scalp/config";

import type { StateMachineOverrides } from "./stateMachinePresets";
import type { ScalpV2RiskProfile, ScalpV2Session } from "./types";

export function buildScalpV2ExecuteConfigOverride(params: {
  entrySessionProfile: ScalpV2Session;
  riskProfile: ScalpV2RiskProfile;
  stateMachineOverrides: StateMachineOverrides;
}): ScalpStrategyConfigOverride {
  const sm = params.stateMachineOverrides || {};

  return {
    // Always bind execution to the deployment session profile.
    sessions: {
      entrySessionProfile: params.entrySessionProfile,
    },
    risk: {
      riskPerTradePct: params.riskProfile.riskPerTradePct,
      maxOpenPositionsPerSymbol: params.riskProfile.maxOpenPositionsPerSymbol,
      ...(sm.consecutiveLossPauseThreshold !== undefined && {
        consecutiveLossPauseThreshold: sm.consecutiveLossPauseThreshold,
      }),
      ...(sm.consecutiveLossCooldownBars !== undefined && {
        consecutiveLossCooldownBars: sm.consecutiveLossCooldownBars,
      }),
      ...(sm.dailyLossLimitR !== undefined && {
        dailyLossLimitR: sm.dailyLossLimitR,
      }),
      ...(sm.maxTradesPerSymbolPerDay !== undefined && {
        maxTradesPerSymbolPerDay: sm.maxTradesPerSymbolPerDay,
      }),
    },
    confirm: {
      ...(sm.confirmTtlMinutes !== undefined && {
        ttlMinutes: sm.confirmTtlMinutes,
      }),
    },
    sweep: {
      ...(sm.sweepRejectMaxBars !== undefined && {
        rejectMaxBars: sm.sweepRejectMaxBars,
      }),
    },
  };
}
