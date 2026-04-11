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
