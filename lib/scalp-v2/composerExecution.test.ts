import assert from "node:assert/strict";
import test from "node:test";

import {
  MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
  buildModelGuidedComposerTuneId,
  isModelGuidedComposerStrategyId,
  resolveModelGuidedComposerExecutionPlan,
  resolveModelGuidedComposerExecutionPlanFromBlocks,
  resolveModelGuidedComposerExecutionPlanFromTuneId,
  resolveScalpExecutionStrategyId,
} from "./composerExecution";

test("model-guided strategy id guard works", () => {
  assert.equal(
    isModelGuidedComposerStrategyId(MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID),
    true,
  );
  assert.equal(isModelGuidedComposerStrategyId("regime_pullback_m15_m3"), false);
});

test("pattern blocks resolve to expected execution strategy (default TF variant)", () => {
  const compression = resolveModelGuidedComposerExecutionPlanFromBlocks({
    pattern: ["pattern_compression_breakout"],
  });
  assert.equal(compression.armId, "compress_m15_m3");
  assert.equal(compression.baseArm, "compress");
  assert.equal(compression.tfVariant, "m15_m3");
  assert.equal(
    compression.strategyId,
    "compression_breakout_pullback_m15_m3",
  );
  assert.equal(compression.source, "pattern_block");
});

test("pattern blocks resolve with explicit TF variant", () => {
  const compression = resolveModelGuidedComposerExecutionPlanFromBlocks(
    { pattern: ["pattern_compression_breakout"] },
    "m5_m1",
  );
  assert.equal(compression.armId, "compress_m5_m1");
  assert.equal(compression.baseArm, "compress");
  assert.equal(compression.tfVariant, "m5_m1");
  assert.equal(compression.strategyId, "compression_breakout_pullback_m5_m1");
});

test("tune id round-trip preserves arm selection (new format)", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "orb_m5_m1",
    digest: "abcdef1234567890",
  });
  assert.equal(tuneId.startsWith("mdl_orb_m5_m1_"), true);
  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.armId, "orb_m5_m1");
  assert.equal(plan.strategyId, "opening_range_breakout_retest_m5_m1");
  assert.equal(plan.source, "tune_prefix");
});

test("tune id round-trip with m5_m3 variant", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "regime_m5_m3",
    digest: "abcdef1234567890",
  });
  assert.equal(tuneId.startsWith("mdl_regime_m5_m3_"), true);
  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.armId, "regime_m5_m3");
  assert.equal(plan.strategyId, "regime_pullback_m5_m3");
});

test("backward compat: legacy tune id (bare arm) resolves to default TF variant", () => {
  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId("mdl_orb_abcd");
  assert.equal(plan.armId, "orb_m5_m1");
  assert.equal(plan.strategyId, "opening_range_breakout_retest_m5_m1");

  const plan2 = resolveModelGuidedComposerExecutionPlanFromTuneId("mdl_regime_abcd");
  assert.equal(plan2.armId, "regime_m15_m3");
  assert.equal(plan2.strategyId, "regime_pullback_m15_m3");
});

test("composite resolver prefers blocks then tune then fallback", () => {
  const fromBlocks = resolveModelGuidedComposerExecutionPlan({
    tuneId: "mdl_orb_m5_m1_abcd",
    blocksByFamily: { pattern: ["pattern_hss_ict_structure"] },
  });
  assert.equal(fromBlocks.armId, "hss_m15_m3");
  assert.equal(fromBlocks.strategyId, "hss_ict_m15_m3_guarded");
  assert.equal(fromBlocks.source, "pattern_block");

  const fromTune = resolveModelGuidedComposerExecutionPlan({
    tuneId: "mdl_vwap_m15_m3_1111",
    blocksByFamily: {},
  });
  assert.equal(fromTune.armId, "vwap_m15_m3");
  assert.equal(fromTune.strategyId, "anchored_vwap_reversion_m15_m3");
  assert.equal(fromTune.source, "tune_prefix");

  const fallback = resolveModelGuidedComposerExecutionPlan({
    tuneId: "mdl_unknown_zzzz",
    blocksByFamily: { pattern: [] },
  });
  assert.equal(fallback.armId, "regime_m15_m3");
  assert.equal(fallback.strategyId, "regime_pullback_m15_m3");
});

test("execution strategy resolver maps model-guided deployments to delegate arm", () => {
  const strategyId = resolveScalpExecutionStrategyId({
    strategyId: MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
    tuneId: "mdl_trend_m15_m3_12abcdef34",
  });
  assert.equal(strategyId, "trend_day_reacceleration_m15_m3");

  // Legacy tune id
  const legacyStrategyId = resolveScalpExecutionStrategyId({
    strategyId: MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
    tuneId: "mdl_trend_12abcdef34",
  });
  assert.equal(legacyStrategyId, "trend_day_reacceleration_m15_m3");

  // M5/M1 variant
  const m5m1StrategyId = resolveScalpExecutionStrategyId({
    strategyId: MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
    tuneId: "mdl_trend_m5_m1_12abcdef34",
  });
  assert.equal(m5m1StrategyId, "trend_day_reacceleration_m5_m1");

  const untouched = resolveScalpExecutionStrategyId({
    strategyId: "regime_pullback_m15_m3",
    tuneId: "default",
  });
  assert.equal(untouched, "regime_pullback_m15_m3");
});
