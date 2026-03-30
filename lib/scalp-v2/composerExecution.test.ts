import assert from "node:assert/strict";
import test from "node:test";

import {
  MODEL_GUIDED_COMPOSER_V2_STRATEGY_ID,
  buildModelGuidedComposerTuneId,
  isModelGuidedComposerStrategyId,
  parseEntryTriggerFromTuneId,
  parseExitRuleFromTuneId,
  parseRiskRuleFromTuneId,
  parseStateMachineFromTuneId,
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

test("tune id with exit rule encodes and parses correctly", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "regime_m15_m3",
    digest: "abcdef1234567890",
    exitRuleId: "exit_tp1_then_trail",
  });
  assert.equal(tuneId, "mdl_regime_m15_m3_xtt_abcdef1234");

  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.armId, "regime_m15_m3");
  assert.equal(plan.strategyId, "regime_pullback_m15_m3");
  assert.equal(plan.exitRuleBlockId, "exit_tp1_then_trail");

  const exitRule = parseExitRuleFromTuneId(tuneId);
  assert.equal(exitRule, "exit_tp1_then_trail");
});

test("tune id without exit rule parses exitRuleBlockId as null", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "vwap_m5_m1",
    digest: "abcdef1234567890",
  });
  assert.equal(tuneId, "mdl_vwap_m5_m1_abcdef1234");

  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.exitRuleBlockId, null);

  const exitRule = parseExitRuleFromTuneId(tuneId);
  assert.equal(exitRule, null);
});

test("tune id with exit + entry encodes and parses correctly", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "regime_m15_m3",
    digest: "abcdef1234567890",
    exitRuleId: "exit_trailing_atr",
    entryTriggerId: "entry_sweep_reclaim",
  });
  assert.equal(tuneId, "mdl_regime_m15_m3_xatr_esr_abcdef1234");

  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.armId, "regime_m15_m3");
  assert.equal(plan.exitRuleBlockId, "exit_trailing_atr");
  assert.equal(plan.entryTriggerBlockId, "entry_sweep_reclaim");

  const exitRule = parseExitRuleFromTuneId(tuneId);
  assert.equal(exitRule, "exit_trailing_atr");
  const entryTrigger = parseEntryTriggerFromTuneId(tuneId);
  assert.equal(entryTrigger, "entry_sweep_reclaim");
});

test("tune id with exit only parses entryTriggerBlockId as null", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "vwap_m5_m1",
    digest: "abcdef1234567890",
    exitRuleId: "exit_time_stop",
  });
  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.exitRuleBlockId, "exit_time_stop");
  assert.equal(plan.entryTriggerBlockId, null);
});

test("all exit rule short codes round-trip", () => {
  const codes = ["exit_fixed_r_take_profit", "exit_trailing_atr", "exit_time_stop"];
  for (const exitRuleId of codes) {
    const tuneId = buildModelGuidedComposerTuneId({
      armId: "compress_m5_m3",
      digest: "1111111111",
      exitRuleId,
    });
    const parsed = parseExitRuleFromTuneId(tuneId);
    assert.equal(parsed, exitRuleId, `round-trip failed for ${exitRuleId}`);
  }
});

test("all entry trigger short codes round-trip", () => {
  const triggers = [
    "entry_sweep_reclaim",
    "entry_mss_break_trigger",
    "entry_ifvg_touch",
    "entry_displacement_confirm",
    "entry_vwap_snapback",
    "entry_extreme_reversal",
  ];
  for (const entryTriggerId of triggers) {
    const tuneId = buildModelGuidedComposerTuneId({
      armId: "regime_m15_m3",
      digest: "2222222222",
      exitRuleId: "exit_tp1_then_trail",
      entryTriggerId,
    });
    const parsed = parseEntryTriggerFromTuneId(tuneId);
    assert.equal(parsed, entryTriggerId, `round-trip failed for ${entryTriggerId}`);
    // Exit should still parse correctly alongside entry
    const exitParsed = parseExitRuleFromTuneId(tuneId);
    assert.equal(exitParsed, "exit_tp1_then_trail", `exit parse broken for ${entryTriggerId}`);
  }
});

test("tune id with exit + entry + risk encodes and parses correctly", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "compress_m5_m3",
    digest: "abcdef1234567890",
    exitRuleId: "exit_trailing_atr",
    entryTriggerId: "entry_displacement_confirm",
    riskRuleId: "risk_leverage_cap",
  });
  assert.equal(tuneId, "mdl_compress_m5_m3_xatr_edisp_rlev_abcdef1234");

  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.armId, "compress_m5_m3");
  assert.equal(plan.exitRuleBlockId, "exit_trailing_atr");
  assert.equal(plan.entryTriggerBlockId, "entry_displacement_confirm");
  assert.equal(plan.riskRuleBlockId, "risk_leverage_cap");
});

test("tune id without risk rule parses riskRuleBlockId as null", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "regime_m15_m3",
    digest: "abcdef1234567890",
    exitRuleId: "exit_tp1_then_trail",
    entryTriggerId: "entry_sweep_reclaim",
  });
  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.riskRuleBlockId, null);
  assert.equal(plan.exitRuleBlockId, "exit_tp1_then_trail");
  assert.equal(plan.entryTriggerBlockId, "entry_sweep_reclaim");
});

test("risk rule short code round-trips", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "vwap_m15_m3",
    digest: "3333333333",
    exitRuleId: "exit_time_stop",
    entryTriggerId: "entry_vwap_snapback",
    riskRuleId: "risk_leverage_cap",
  });
  assert.equal(parseRiskRuleFromTuneId(tuneId), "risk_leverage_cap");
  assert.equal(parseExitRuleFromTuneId(tuneId), "exit_time_stop");
  assert.equal(parseEntryTriggerFromTuneId(tuneId), "entry_vwap_snapback");
});

test("tune id with all 4 codes encodes and parses correctly", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "regime_m15_m3",
    digest: "abcdef1234567890",
    exitRuleId: "exit_tp1_then_trail",
    entryTriggerId: "entry_sweep_reclaim",
    riskRuleId: "risk_leverage_cap",
    stateMachineId: "state_consecutive_loss_pause",
  });
  assert.equal(tuneId, "mdl_regime_m15_m3_xtt_esr_rlev_sclp_abcdef1234");

  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.armId, "regime_m15_m3");
  assert.equal(plan.exitRuleBlockId, "exit_tp1_then_trail");
  assert.equal(plan.entryTriggerBlockId, "entry_sweep_reclaim");
  assert.equal(plan.riskRuleBlockId, "risk_leverage_cap");
  assert.equal(plan.stateMachineBlockId, "state_consecutive_loss_pause");
});

test("state machine code round-trips independently", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "trend_m5_m1",
    digest: "4444444444",
    exitRuleId: "exit_trailing_atr",
    stateMachineId: "state_adaptive_confidence_gate",
  });
  assert.equal(parseStateMachineFromTuneId(tuneId), "state_adaptive_confidence_gate");
  assert.equal(parseExitRuleFromTuneId(tuneId), "exit_trailing_atr");
  assert.equal(parseEntryTriggerFromTuneId(tuneId), null);
  assert.equal(parseRiskRuleFromTuneId(tuneId), null);
});

test("tune id without state machine parses stateMachineBlockId as null", () => {
  const tuneId = buildModelGuidedComposerTuneId({
    armId: "basis_m5_m3",
    digest: "5555555555",
    exitRuleId: "exit_fixed_r_take_profit",
    entryTriggerId: "entry_displacement_confirm",
    riskRuleId: "risk_leverage_cap",
  });
  const plan = resolveModelGuidedComposerExecutionPlanFromTuneId(tuneId);
  assert.equal(plan.stateMachineBlockId, null);
  assert.equal(plan.riskRuleBlockId, "risk_leverage_cap");
});
