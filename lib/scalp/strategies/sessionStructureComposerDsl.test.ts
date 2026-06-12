import assert from "node:assert/strict";
import test from "node:test";

import {
  SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
  buildScalpComposerSessionStructureComposerGrid,
  buildSessionStructureComposerTuneId,
  isSessionStructureComposerStrategyId,
  parseSessionStructureComposerTuneId,
  validateSessionStructureCompatibility,
} from "../composer/sessionStructureComposer";
import { buildSessionStructureAdaptivePriorsFromRows } from "../composer/sessionStructureAdaptiveSearch";

function composerFamilyKey(row: ReturnType<typeof buildScalpComposerSessionStructureComposerGrid>[number]): string {
  const level = row.sessionComposerPlan.levelId.startsWith("opening_range_")
    ? "opening_range"
    : row.sessionComposerPlan.levelId;
  const trigger = row.sessionComposerPlan.triggerId.startsWith("breakout_retest_hold")
    ? "breakout_retest"
    : row.sessionComposerPlan.triggerId;
  return `${level}|${trigger}`;
}

test("session structure composer tune id build/parse round-trips", () => {
  const tuneId = buildSessionStructureComposerTuneId({
    contextId: "m30_session_momentum",
    levelId: "session_vwap",
    triggerId: "vwap_pullback_continuation",
    confirmationId: "m15_close_acceptance",
    managementId: "fixed_2r_time_4h",
    digest: "abcdef123456",
  });
  assert.equal(tuneId, "ssc_m30mom_svwap_vwpb_m15acc_fix2r4h_abcdef1234");
  assert.deepEqual(parseSessionStructureComposerTuneId(tuneId), {
    contextId: "m30_session_momentum",
    levelId: "session_vwap",
    triggerId: "vwap_pullback_continuation",
    confirmationId: "m15_close_acceptance",
    managementId: "fixed_2r_time_4h",
    digest: "abcdef1234",
  });
  assert.equal(isSessionStructureComposerStrategyId(SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID), true);
});

test("session structure composer V1.1 tune blocks round-trip", () => {
  const tuneId = buildSessionStructureComposerTuneId({
    contextId: "atr_low_chop_avoid",
    levelId: "opening_range_45m",
    triggerId: "breakout_retest_hold_tight",
    confirmationId: "retest_wick_rejection",
    managementId: "trail_after_0_8r_time_3h",
    digest: "1234567890abcdef",
  });
  assert.equal(tuneId, "ssc_atrnochop_orb45_brktit_wickrej_trl08r3h_1234567890");
  assert.deepEqual(parseSessionStructureComposerTuneId(tuneId), {
    contextId: "atr_low_chop_avoid",
    levelId: "opening_range_45m",
    triggerId: "breakout_retest_hold_tight",
    confirmationId: "retest_wick_rejection",
    managementId: "trail_after_0_8r_time_3h",
    digest: "1234567890",
  });
});

test("session structure compatibility rejects invalid no-op combinations", () => {
  assert.equal(
    validateSessionStructureCompatibility({
      contextId: "m30_session_momentum",
      levelId: "prior_day_hl",
      triggerId: "vwap_pullback_continuation",
      confirmationId: "m15_close_acceptance",
      managementId: "fixed_2r_time_4h",
    }).compatible,
    false,
  );
  assert.equal(
    validateSessionStructureCompatibility({
      contextId: "m30_session_momentum",
      levelId: "session_vwap",
      triggerId: "sweep_reclaim",
      confirmationId: "m15_close_acceptance",
      managementId: "fixed_2r_time_4h",
    }).compatible,
    false,
  );
  assert.equal(
    validateSessionStructureCompatibility({
      contextId: "m30_session_momentum",
      levelId: "session_vwap",
      triggerId: "vwap_pullback_continuation",
      confirmationId: "m30_close_acceptance",
      managementId: "fixed_2r_time_4h",
    }).compatible,
    true,
  );
  assert.equal(
    validateSessionStructureCompatibility({
      contextId: "atr_low_chop_avoid",
      levelId: "opening_range_15m",
      triggerId: "breakout_retest_hold_tight",
      confirmationId: "retest_wick_rejection",
      managementId: "fixed_2r_time_4h",
    }).compatible,
    true,
  );
});

test("session structure grid is deterministic, capped, and deduped by behavior fingerprint", () => {
  const a = buildScalpComposerSessionStructureComposerGrid({
    venue: "capital",
    symbol: "EURUSD",
    entrySessionProfile: "berlin",
    maxCandidates: 60,
    generatedAtMs: 1,
  });
  const b = buildScalpComposerSessionStructureComposerGrid({
    venue: "capital",
    symbol: "EURUSD",
    entrySessionProfile: "berlin",
    maxCandidates: 60,
    generatedAtMs: 1,
  });
  assert.equal(a.length, 60);
  assert.deepEqual(a.map((row) => row.tuneId), b.map((row) => row.tuneId));
  assert.equal(new Set(a.map((row) => row.behaviorFingerprint)).size, a.length);
  assert.ok(a.every((row) => row.tuneId.startsWith("ssc_")));
  assert.ok(a.every((row) => row.model.version === "session_structure_composer_v1"));
  assert.ok(new Set(a.map((row) => row.sessionComposerPlan.contextId)).has("atr_low_chop_avoid"));
  assert.ok(new Set(a.map((row) => row.sessionComposerPlan.levelId)).has("opening_range_45m"));
  assert.ok(new Set(a.map((row) => row.sessionComposerPlan.triggerId)).has("breakout_retest_hold_tight"));
  const familyCounts = new Map<string, number>();
  for (const row of a) {
    const key = composerFamilyKey(row);
    familyCounts.set(key, (familyCounts.get(key) || 0) + 1);
  }
  assert.ok(Math.max(...familyCounts.values()) <= 11);
  assert.ok(a.every((row) => row.novelty?.strategyFamilyKey));
});

test("session structure adaptive priors bias the next generated grid", () => {
  const winningPlan = {
    contextId: "ny_continuation",
    levelId: "opening_range_60m",
    triggerId: "breakout_retest_hold_tight",
    confirmationId: "retest_wick_rejection",
    managementId: "trail_after_0_8r_time_3h",
    digest: "abc1234567",
  } as const;
  const rows = Array.from({ length: 12 }, () => ({
    venue: "capital" as const,
    symbol: "EURUSD",
    session: "berlin" as const,
    metadata: {
      sessionComposerPlan: winningPlan,
      worker: {
        windowToTs: 1,
        stageA: { passed: true, netR: 2, trades: 18 },
        stageB: { passed: true, netR: 3, trades: 28 },
        stageC: { passed: true, netR: 5, trades: 42 },
      },
    },
  }));
  const priors = buildSessionStructureAdaptivePriorsFromRows({
    rows,
    windowToTs: 1,
    nowMs: 2,
    minSamples: 4,
  });
  const adaptiveGrid = buildScalpComposerSessionStructureComposerGrid({
    venue: "capital",
    symbol: "EURUSD",
    entrySessionProfile: "berlin",
    maxCandidates: 60,
    generatedAtMs: 1,
    adaptivePriors: priors,
  });
  const nearNeighbor = adaptiveGrid.find((row) =>
    row.sessionComposerPlan.levelId.startsWith("opening_range_") &&
    row.sessionComposerPlan.triggerId.startsWith("breakout_retest_hold") &&
    (row.adaptivePrior?.matchedKeys || []).some((key) => key === "level_family:opening_range")
  );
  assert.ok(nearNeighbor);
  assert.ok((nearNeighbor.adaptivePrior?.adjustment || 0) > 0);
  const lanes = new Map<string, number>();
  const familyCounts = new Map<string, number>();
  for (const row of adaptiveGrid) {
    lanes.set(row.novelty?.lane || "none", (lanes.get(row.novelty?.lane || "none") || 0) + 1);
    const key = composerFamilyKey(row);
    familyCounts.set(key, (familyCounts.get(key) || 0) + 1);
  }
  assert.ok((lanes.get("exploit") || 0) > 0);
  assert.ok((lanes.get("explore") || 0) >= 10);
  assert.ok(Math.max(...familyCounts.values()) <= 11);
});
