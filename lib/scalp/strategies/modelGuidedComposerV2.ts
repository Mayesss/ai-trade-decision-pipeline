import {
  resolveModelGuidedComposerExecutionPlanFromTuneId,
  MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
} from "../../scalp-v2/composerExecution";
import { resolveRegimeGateRule } from "../../scalp-v2/regimeGatePresets";
import {
  inScalpEntrySessionProfileWindow,
  normalizeScalpEntrySessionProfile,
} from "../sessions";
import type { ScalpCandle } from "../types";

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
import { computeAtrSeries } from "./syntheticSignal";
import { trendDayReaccelerationM15M3Strategy } from "./trendDayReaccelerationM15M3";
import { TIMEFRAME_VARIANT_STRATEGIES_BY_ID } from "./timeframeVariants";
import type {
  ScalpStrategyDefinition,
  ScalpStrategyPhaseInput,
  ScalpStrategyPhaseOutput,
} from "./types";

const BASE_DELEGATES: Record<string, ScalpStrategyDefinition> = {
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
};

// Merge timeframe variant strategies into the delegate map
const DELEGATE_BY_STRATEGY_ID: Record<string, ScalpStrategyDefinition> =
  Object.freeze({ ...BASE_DELEGATES, ...TIMEFRAME_VARIANT_STRATEGIES_BY_ID });
const REGIME_GATE_ATR_PERIOD = 14;

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

function computeAtrPercentileRank(
  candles: ScalpCandle[],
  atrLookback: number,
): number | null {
  const lookback = Math.max(20, Math.floor(atrLookback));
  const atrSeries = computeAtrSeries(candles || [], REGIME_GATE_ATR_PERIOD).filter(
    (value) => Number.isFinite(value) && value > 0,
  );
  if (atrSeries.length < lookback) return null;
  const window = atrSeries.slice(-lookback);
  const current = window[window.length - 1];
  if (!(Number.isFinite(current) && current > 0)) return null;
  let count = 0;
  for (const value of window) {
    if (value <= current) count += 1;
  }
  return (count / window.length) * 100;
}

function applyDelegate(input: ScalpStrategyPhaseInput): ScalpStrategyPhaseOutput {
  const { plan, strategy } = resolveDelegate(input);

  // Central session gate: ensure ALL delegate strategies respect the assigned
  // session window. Some delegates already check this themselves — the
  // redundancy is harmless and guarantees no strategy can trade outside its
  // assigned session.
  const entrySessionProfile = normalizeScalpEntrySessionProfile(
    input.cfg.sessions.entrySessionProfile,
    "berlin",
  );
  if (!inScalpEntrySessionProfileWindow(input.nowMs, entrySessionProfile)) {
    return {
      state: { ...input.state, state: "IDLE" },
      reasonCodes: dedupeReasonCodes([
        "MODEL_GUIDED_COMPOSER_ACTIVE",
        `MODEL_GUIDED_ARM_${plan.armId.toUpperCase()}`,
        `MODEL_GUIDED_DELEGATE_${strategy.id.toUpperCase()}`,
        "SESSION_FILTER_OUTSIDE_ENTRY_PROFILE",
        `SESSION_PROFILE_${entrySessionProfile.toUpperCase()}`,
      ]),
      entryIntent: null,
      meta: {
        modelGuidedComposer: {
          armId: plan.armId,
          delegateStrategyId: strategy.id,
          source: plan.source,
          sessionGated: true,
          regimeGateId: plan.regimeGateBlockId,
        },
      },
    };
  }

  const phase = strategy.applyPhaseDetectors(input);
  const regimeGateId = plan.regimeGateBlockId || null;
  const regimeRule = resolveRegimeGateRule(regimeGateId);
  let blockedByRegime = false;
  let regimeReasonCodes: string[] = [];
  let atrPercentile: number | null = null;

  if (regimeRule) {
    const regimeGateReasonCode = regimeGateId
      ? `REGIME_GATE_${regimeGateId.toUpperCase()}`
      : "REGIME_GATE_UNSPECIFIED";
    atrPercentile = computeAtrPercentileRank(
      input.market.baseCandles,
      regimeRule.atrLookback,
    );
    const hasEntryIntent = Boolean(phase.entryIntent);
    if (hasEntryIntent) {
      if (atrPercentile === null) {
        blockedByRegime = true;
        regimeReasonCodes = [
          "REGIME_GATE_ENTRY_BLOCKED",
          "REGIME_GATE_INSUFFICIENT_HISTORY",
          regimeGateReasonCode,
        ];
      } else if (
        atrPercentile < regimeRule.minPercentile ||
        atrPercentile > regimeRule.maxPercentile
      ) {
        blockedByRegime = true;
        regimeReasonCodes = [
          "REGIME_GATE_ENTRY_BLOCKED",
          "REGIME_GATE_OUT_OF_RANGE",
          regimeGateReasonCode,
          `REGIME_ATR_PCTL_${Math.round(atrPercentile)}`,
        ];
      }
    }
  }

  return {
    ...phase,
    reasonCodes: dedupeReasonCodes([
      "MODEL_GUIDED_COMPOSER_ACTIVE",
      `MODEL_GUIDED_ARM_${plan.armId.toUpperCase()}`,
      `MODEL_GUIDED_DELEGATE_${strategy.id.toUpperCase()}`,
      ...phase.reasonCodes,
      ...regimeReasonCodes,
    ]),
    entryIntent: blockedByRegime ? null : phase.entryIntent,
    meta: {
      ...(phase.meta || {}),
      modelGuidedComposer: {
        armId: plan.armId,
        delegateStrategyId: strategy.id,
        source: plan.source,
        regimeGateId,
        regimeGate:
          regimeRule
            ? {
                atrLookback: regimeRule.atrLookback,
                minPercentile: regimeRule.minPercentile,
                maxPercentile: regimeRule.maxPercentile,
                atrPercentile,
                blockedEntry: blockedByRegime,
              }
            : null,
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
