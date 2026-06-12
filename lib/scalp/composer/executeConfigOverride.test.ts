import assert from "node:assert/strict";
import test from "node:test";

import { buildScalpV2ExecuteConfigOverride } from "./executeConfigOverride";

test("execute config override always applies deployment entry session profile", () => {
  const override = buildScalpV2ExecuteConfigOverride({
    entrySessionProfile: "sydney",
    riskProfile: {
      riskPerTradePct: 0.5,
      maxOpenPositionsPerSymbol: 1,
      autoPauseDailyR: -3,
      autoPause30dR: -10,
    },
    stateMachineOverrides: {},
  });

  assert.equal(override.sessions?.entrySessionProfile, "sydney");
  assert.equal(override.risk?.riskPerTradePct, 0.5);
  assert.equal(override.risk?.maxOpenPositionsPerSymbol, 1);
});

test("execute config override merges state-machine overrides while preserving session binding", () => {
  const override = buildScalpV2ExecuteConfigOverride({
    entrySessionProfile: "tokyo",
    riskProfile: {
      riskPerTradePct: 0.35,
      maxOpenPositionsPerSymbol: 2,
      autoPauseDailyR: -2,
      autoPause30dR: -8,
    },
    stateMachineOverrides: {
      consecutiveLossPauseThreshold: 2,
      consecutiveLossCooldownBars: 4,
      dailyLossLimitR: -1.5,
      maxTradesPerSymbolPerDay: 1,
      confirmTtlMinutes: 20,
      sweepRejectMaxBars: 8,
    },
  });

  assert.equal(override.sessions?.entrySessionProfile, "tokyo");
  assert.equal(override.risk?.consecutiveLossPauseThreshold, 2);
  assert.equal(override.risk?.consecutiveLossCooldownBars, 4);
  assert.equal(override.risk?.dailyLossLimitR, -1.5);
  assert.equal(override.risk?.maxTradesPerSymbolPerDay, 1);
  assert.equal(override.confirm?.ttlMinutes, 20);
  assert.equal(override.sweep?.rejectMaxBars, 8);
});

test("execute config override maps promoted DSL presets to live strategy config", () => {
  const override = buildScalpV2ExecuteConfigOverride({
    entrySessionProfile: "berlin",
    riskProfile: {
      riskPerTradePct: 0.4,
      maxOpenPositionsPerSymbol: 1,
      autoPauseDailyR: -2,
      autoPause30dR: -8,
    },
    entryTriggerOverrides: {
      sweepBufferPips: 0.12,
      sweepRejectMaxBars: 20,
      displacementBodyAtrMult: 0.06,
      mssLookbackBars: 1,
      ifvgMinAtrMult: 0,
      ifvgMaxAtrMult: 4,
      ifvgTtlMinutes: 120,
      ifvgEntryMode: "midline_touch",
    },
    exitRuleOverrides: {
      takeProfitR: 0.8,
      tp1R: 999,
      tp1ClosePct: 0,
      breakEvenOffsetR: 0,
      trailStartR: 999,
      trailAtrMult: 0,
      timeStopBars: 20,
    },
    riskRuleReplayOverrides: {
      dailyLossLimitR: -1.5,
      maxTradesPerDay: 1,
    },
    stateMachineOverrides: {},
  });

  assert.equal(override.sessions?.entrySessionProfile, "berlin");
  assert.equal(override.risk?.riskPerTradePct, 0.4);
  assert.equal(override.risk?.dailyLossLimitR, -1.5);
  assert.equal(override.risk?.maxTradesPerSymbolPerDay, 1);
  assert.equal(override.risk?.takeProfitR, 0.8);
  assert.equal(override.risk?.timeStopBars, 20);
  assert.equal(override.confirm?.displacementBodyAtrMult, 0.06);
  assert.equal(override.confirm?.mssLookbackBars, 1);
  assert.equal(override.sweep?.bufferPips, 0.12);
  assert.equal(override.sweep?.rejectMaxBars, 20);
  assert.equal(override.ifvg?.minAtrMult, 0);
  assert.equal(override.ifvg?.maxAtrMult, 4);
  assert.equal(override.ifvg?.ttlMinutes, 120);
  assert.equal(override.ifvg?.entryMode, "midline_touch");
});

test("execute config override lets state-machine presets win on overlapping fields", () => {
  const override = buildScalpV2ExecuteConfigOverride({
    entrySessionProfile: "newyork",
    riskProfile: {
      riskPerTradePct: 0.35,
      maxOpenPositionsPerSymbol: 1,
      autoPauseDailyR: -3,
      autoPause30dR: -8,
    },
    entryTriggerOverrides: {
      confirmTtlMinutes: 60,
      sweepRejectMaxBars: 20,
    },
    riskRuleReplayOverrides: {
      dailyLossLimitR: -2,
      maxTradesPerDay: 2,
    },
    exitRuleOverrides: {},
    stateMachineOverrides: {
      confirmTtlMinutes: 20,
      sweepRejectMaxBars: 8,
      dailyLossLimitR: -1,
      maxTradesPerSymbolPerDay: 1,
    },
  });

  assert.equal(override.confirm?.ttlMinutes, 20);
  assert.equal(override.sweep?.rejectMaxBars, 8);
  assert.equal(override.risk?.dailyLossLimitR, -1);
  assert.equal(override.risk?.maxTradesPerSymbolPerDay, 1);
});
