import {
  resolveModelGuidedComposerExecutionPlanFromTuneId,
  MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
} from "../../scalp-v2/composerExecution";

import { adaptiveMetaSelectorM15M3Strategy } from "./adaptiveMetaSelectorM15M3";
import { anchoredVwapReversionM15M3Strategy } from "./anchoredVwapReversionM15M3";
import { basisDislocationReversionProxyM15M3Strategy } from "./basisDislocationReversionProxyM15M3";
import { compressionBreakoutPullbackM15M3Strategy } from "./compressionBreakoutPullbackM15M3";
import { failedAuctionExtremeReversalM15M1Strategy } from "./failedAuctionExtremeReversalM15M1";
import { hssIctM15M3GuardedStrategy } from "./hssIctM15M3Guarded";
import { openingRangeBreakoutRetestM5M1Strategy } from "./openingRangeBreakoutRetestM5M1";
import { pdhPdlReclaimM15M3Strategy } from "./pdhPdlReclaimM15M3";
import { regimePullbackM15M3Strategy } from "./regimePullbackM15M3";
import { relativeValueSpreadProxyM15M3Strategy } from "./relativeValueSpreadProxyM15M3";
import { sessionSeasonalityBiasM15M3Strategy } from "./sessionSeasonalityBiasM15M3";
import { trendDayReaccelerationM15M3Strategy } from "./trendDayReaccelerationM15M3";
import type {
  ScalpStrategyDefinition,
  ScalpStrategyPhaseInput,
  ScalpStrategyPhaseOutput,
} from "./types";

const DELEGATE_BY_STRATEGY_ID: Record<string, ScalpStrategyDefinition> =
  Object.freeze({
    [regimePullbackM15M3Strategy.id]: regimePullbackM15M3Strategy,
    [compressionBreakoutPullbackM15M3Strategy.id]:
      compressionBreakoutPullbackM15M3Strategy,
    [openingRangeBreakoutRetestM5M1Strategy.id]:
      openingRangeBreakoutRetestM5M1Strategy,
    [failedAuctionExtremeReversalM15M1Strategy.id]:
      failedAuctionExtremeReversalM15M1Strategy,
    [trendDayReaccelerationM15M3Strategy.id]: trendDayReaccelerationM15M3Strategy,
    [anchoredVwapReversionM15M3Strategy.id]: anchoredVwapReversionM15M3Strategy,
    [basisDislocationReversionProxyM15M3Strategy.id]:
      basisDislocationReversionProxyM15M3Strategy,
    [relativeValueSpreadProxyM15M3Strategy.id]:
      relativeValueSpreadProxyM15M3Strategy,
    [sessionSeasonalityBiasM15M3Strategy.id]:
      sessionSeasonalityBiasM15M3Strategy,
    [pdhPdlReclaimM15M3Strategy.id]: pdhPdlReclaimM15M3Strategy,
    [hssIctM15M3GuardedStrategy.id]: hssIctM15M3GuardedStrategy,
    [adaptiveMetaSelectorM15M3Strategy.id]: adaptiveMetaSelectorM15M3Strategy,
  });

function dedupeReasonCodes(values: string[]): string[] {
  return Array.from(
    new Set(
      (values || [])
        .map((row) => String(row || "").trim())
        .filter(Boolean),
    ),
  );
}

function resolveDelegate(
  input: ScalpStrategyPhaseInput,
): {
  plan: ReturnType<typeof resolveModelGuidedComposerExecutionPlanFromTuneId>;
  strategy: ScalpStrategyDefinition;
} {
  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(
    input.state.tuneId,
  );
  const strategy =
    DELEGATE_BY_STRATEGY_ID[plan.strategyId] || regimePullbackM15M3Strategy;
  return { plan, strategy };
}

function applyDelegate(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput {
  const { plan, strategy } = resolveDelegate(input);
  const phase = strategy.applyPhaseDetectors(input);
  return {
    ...phase,
    reasonCodes: dedupeReasonCodes([
      "MODEL_GUIDED_COMPOSER_ACTIVE",
      `MODEL_GUIDED_ARM_${plan.armId.toUpperCase()}`,
      `MODEL_GUIDED_DELEGATE_${strategy.id.toUpperCase()}`,
      ...phase.reasonCodes,
    ]),
    meta: {
      ...(phase.meta || {}),
      modelGuidedComposer: {
        armId: plan.armId,
        delegateStrategyId: strategy.id,
        source: plan.source,
      },
    },
  };
}

export const modelGuidedComposerV2Strategy: ScalpStrategyDefinition = {
  id: MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
  shortName: "Model Guided Composer",
  longName: "Model-Guided Composer V2",
  preferredBaseTf: "M15",
  preferredConfirmTf: "M3",
  applyPhaseDetectors: applyDelegate,
};
