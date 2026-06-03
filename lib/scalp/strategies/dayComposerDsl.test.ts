import assert from "node:assert/strict";
import test from "node:test";

import {
  DAY_MODEL_GUIDED_COMPOSER_V1_STRATEGY_ID,
  buildDayComposerTuneId,
  buildScalpV2DayModelComposerGrid,
  isDayModelGuidedComposerStrategyId,
  parseDayComposerTuneId,
  validateDayComposerCompatibility,
} from "../../scalp-v2/dayComposer";

test("day composer tune id build/parse round-trips", () => {
  const tuneId = buildDayComposerTuneId({
    contextId: "h1_trend_d1_bias",
    levelId: "prior_day_hl",
    triggerId: "sweep_reclaim",
    confirmationId: "m15_close_acceptance",
    managementId: "fixed_2r_time_6h",
    digest: "abcdef123456",
  });
  assert.equal(tuneId, "dtc_h1td1_pdhpdl_sweep_m15acc_fix2r6h_abcdef1234");
  assert.deepEqual(parseDayComposerTuneId(tuneId), {
    contextId: "h1_trend_d1_bias",
    levelId: "prior_day_hl",
    triggerId: "sweep_reclaim",
    confirmationId: "m15_close_acceptance",
    managementId: "fixed_2r_time_6h",
    digest: "abcdef1234",
  });
  assert.equal(isDayModelGuidedComposerStrategyId(DAY_MODEL_GUIDED_COMPOSER_V1_STRATEGY_ID), true);
});

test("day composer compatibility rejects invalid no-op combinations", () => {
  assert.equal(
    validateDayComposerCompatibility({
      contextId: "session_momentum",
      levelId: "prior_day_hl",
      triggerId: "sweep_reclaim",
      confirmationId: "m15_close_acceptance",
      managementId: "target_pdh_pdl",
    }).compatible,
    true,
  );
  assert.equal(
    validateDayComposerCompatibility({
      contextId: "session_momentum",
      levelId: "weekly_open",
      triggerId: "sweep_reclaim",
      confirmationId: "m15_close_acceptance",
      managementId: "target_pdh_pdl",
    }).compatible,
    false,
  );
  assert.equal(
    validateDayComposerCompatibility({
      contextId: "session_momentum",
      levelId: "prior_day_hl",
      triggerId: "vwap_pullback_continuation",
      confirmationId: "m15_close_acceptance",
      managementId: "target_pdh_pdl",
    }).compatible,
    false,
  );
});

test("day composer grid is deterministic and deduped by behavior fingerprint", () => {
  const a = buildScalpV2DayModelComposerGrid({
    venue: "capital",
    symbol: "EURUSD",
    entrySessionProfile: "berlin",
    maxCandidates: 120,
    generatedAtMs: 1,
  });
  const b = buildScalpV2DayModelComposerGrid({
    venue: "capital",
    symbol: "EURUSD",
    entrySessionProfile: "berlin",
    maxCandidates: 120,
    generatedAtMs: 1,
  });
  assert.equal(a.length, 120);
  assert.deepEqual(
    a.map((row) => row.tuneId),
    b.map((row) => row.tuneId),
  );
  assert.equal(
    new Set(a.map((row) => row.behaviorFingerprint)).size,
    a.length,
  );
  assert.ok(a.every((row) => row.tuneId.startsWith("dtc_")));
});
