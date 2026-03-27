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

test("pattern blocks resolve to expected execution strategy", () => {
  const compression = resolveModelGuidedComposerExecutionPlanFromBlocks({
    pattern: ["pattern_compression_breakout"],
  });
  assert.equal(compression.armId, "compress");
  assert.equal(
    compression.strategyId,
    "compression_breakout_pullback_m15_m3",
  );
  assert.equal(compression.source, "pattern_block");
});

test("tune id round-trip preserves arm selection", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "orb",
    digest: "abcdef1234567890",
  });
  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(tuneId.startsWith("mdl_orb_"), true);
  assert.equal(plan.armId, "orb");
  assert.equal(plan.strategyId, "opening_range_breakout_retest_m5_m1");
  assert.equal(plan.source, "tune_prefix");
});

test("composite resolver prefers blocks then tune then fallback", () => {
  const fromBlocks = resolveModelGuidedComposerExecutionPlan({
    tuneId: "mdl_orb_abcd",
    blocksByFamily: { pattern: ["pattern_hss_ict_structure"] },
  });
  assert.equal(fromBlocks.armId, "hss");
  assert.equal(fromBlocks.strategyId, "hss_ict_m15_m3_guarded");
  assert.equal(fromBlocks.source, "pattern_block");

  const fromTune = resolveModelGuidedComposerExecutionPlan({
    tuneId: "mdl_vwap_1111",
    blocksByFamily: {},
  });
  assert.equal(fromTune.armId, "vwap");
  assert.equal(fromTune.strategyId, "anchored_vwap_reversion_m15_m3");
  assert.equal(fromTune.source, "tune_prefix");

  const fallback = resolveModelGuidedComposerExecutionPlan({
    tuneId: "mdl_unknown_zzzz",
    blocksByFamily: { pattern: [] },
  });
  assert.equal(fallback.armId, "regime");
  assert.equal(fallback.strategyId, "regime_pullback_m15_m3");
});

test("execution strategy resolver maps model-guided deployments to delegate arm", () => {
  const strategyId = resolveScalpExecutionStrategyId({
    strategyId: MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
    tuneId: "mdl_trend_12abcdef34",
  });
  assert.equal(strategyId, "trend_day_reacceleration_m15_m3");

  const untouched = resolveScalpExecutionStrategyId({
    strategyId: "regime_pullback_m15_m3",
    tuneId: "default",
  });
  assert.equal(untouched, "regime_pullback_m15_m3");
});
