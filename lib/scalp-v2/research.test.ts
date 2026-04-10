import assert from "node:assert/strict";
import test from "node:test";

import {
  buildScalpV2CandidateDslGrid,
  buildScalpV2ModelGuidedComposerGrid,
  listScalpV2StrategyPrimitiveReferences,
  resolveScalpV2CandidateEvaluationWindow,
  strategyPrimitiveCoverageSummary,
  toScalpV2ResearchCursorKey,
} from "./research";
import {
  parseRegimeGateFromTuneId,
  resolveModelGuidedComposerExecutionPlanFromTuneId,
} from "./composerExecution";
import { listScalpV2CatalogStrategyIds } from "./strategyCatalog";

function withEnv<T>(
  patch: Record<string, string | null>,
  run: () => T,
): T {
  const prev: Record<string, string | undefined> = {};
  for (const [key, value] of Object.entries(patch)) {
    prev[key] = process.env[key];
    if (value === null) delete process.env[key];
    else process.env[key] = value;
  }
  try {
    return run();
  } finally {
    for (const [key, value] of Object.entries(prev)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
}

test("strategy primitive references cover all v2 catalog strategy ids", () => {
  const strategyIds = new Set(listScalpV2CatalogStrategyIds());
  const references = listScalpV2StrategyPrimitiveReferences();
  const referencedIds = new Set(references.map((row) => row.strategyId));

  assert.equal(references.length, strategyIds.size);
  for (const strategyId of strategyIds) {
    assert.equal(
      referencedIds.has(strategyId),
      true,
      `missing primitive reference for ${strategyId}`,
    );
  }
});

test("candidate DSL grid is bounded and deterministic for same context", () => {
  const a = buildScalpV2CandidateDslGrid({
    venue: "bitget",
    symbol: "BTCUSDT",
    entrySessionProfile: "berlin",
    maxCandidates: 20,
  });
  const b = buildScalpV2CandidateDslGrid({
    venue: "bitget",
    symbol: "BTCUSDT",
    entrySessionProfile: "berlin",
    maxCandidates: 20,
  });

  assert.equal(a.length, 20);
  assert.equal(b.length, 20);
  assert.deepEqual(
    a.map((row) => row.candidateId),
    b.map((row) => row.candidateId),
  );
  assert.deepEqual(
    a.map((row) => row.blocksByFamily.session_filter),
    b.map((row) => row.blocksByFamily.session_filter),
  );
});

test("candidate DSL grid novelty quota changes candidate mix deterministically", () => {
  const noNovelty = withEnv(
    {
      SCALP_V2_NOVELTY_PATTERN_POOL_SIZE: "4",
      SCALP_V2_NOVELTY_QUOTA_PCT: "0",
      SCALP_V2_NOVELTY_MIN_SLOTS: "0",
    },
    () =>
      buildScalpV2CandidateDslGrid({
        venue: "bitget",
        symbol: "BTCUSDT",
        entrySessionProfile: "berlin",
        maxCandidates: 40,
      }),
  );
  const withNovelty = withEnv(
    {
      SCALP_V2_NOVELTY_PATTERN_POOL_SIZE: "4",
      SCALP_V2_NOVELTY_QUOTA_PCT: "0.5",
      SCALP_V2_NOVELTY_MIN_SLOTS: "2",
    },
    () =>
      buildScalpV2CandidateDslGrid({
        venue: "bitget",
        symbol: "BTCUSDT",
        entrySessionProfile: "berlin",
        maxCandidates: 40,
      }),
  );

  assert.equal(noNovelty.length, 40);
  assert.equal(withNovelty.length, 40);
  assert.notDeepEqual(
    noNovelty.map((row) => row.candidateId),
    withNovelty.map((row) => row.candidateId),
  );
});

test("candidate DSL grid enforces requested session as primary filter block", () => {
  const rows = buildScalpV2CandidateDslGrid({
    venue: "capital",
    symbol: "EURUSD",
    entrySessionProfile: "newyork",
    maxCandidates: 10,
  });
  assert.equal(rows.length, 10);
  for (const row of rows) {
    assert.equal(row.blocksByFamily.session_filter.includes("session_newyork_window"), true);
    assert.equal(row.symbol, "EURUSD");
    assert.equal(row.entrySessionProfile, "newyork");
  }
});

test("model-guided composer grid is deterministic and bounded", () => {
  const a = buildScalpV2ModelGuidedComposerGrid({
    venue: "bitget",
    symbol: "BTCUSDT",
    entrySessionProfile: "berlin",
    maxCandidates: 16,
  });
  const b = buildScalpV2ModelGuidedComposerGrid({
    venue: "bitget",
    symbol: "BTCUSDT",
    entrySessionProfile: "berlin",
    maxCandidates: 16,
  });

  // After base-arm dedup × 3 TF × 4 exits × compat entries × 2 risk profiles,
  // theoretical max is 12 × 3 × 4 × 4 × 2 = 1152.
  assert.equal(a.length > 0, true);
  assert.equal(a.length <= 1152, true);
  assert.equal(b.length, a.length);
  assert.deepEqual(
    a.map((row) => row.candidateId),
    b.map((row) => row.candidateId),
  );
  assert.deepEqual(
    a.map((row) => row.model.compositeScore),
    b.map((row) => row.model.compositeScore),
  );
});

test("model-guided composer scores stay in [0,1] and preserve session filter intent", () => {
  const rows = buildScalpV2ModelGuidedComposerGrid({
    venue: "capital",
    symbol: "EURUSD",
    entrySessionProfile: "newyork",
    maxCandidates: 20,
  });
  // After base-arm dedup × 3 TF × 4 exits × compat entries × 2 risk profiles.
  assert.equal(rows.length > 0, true);
  assert.equal(rows.length <= 1152, true);
  for (const row of rows) {
    assert.equal(
      row.blocksByFamily.session_filter.includes("session_newyork_window"),
      true,
    );
    assert.equal(row.model.interpretableScore >= 0, true);
    assert.equal(row.model.interpretableScore <= 1, true);
    assert.equal(row.model.treeScore >= 0, true);
    assert.equal(row.model.treeScore <= 1, true);
    assert.equal(row.model.sequenceScore >= 0, true);
    assert.equal(row.model.sequenceScore <= 1, true);
    assert.equal(row.model.compositeScore >= 0, true);
    assert.equal(row.model.compositeScore <= 1, true);
    assert.equal(row.model.confidence >= 0, true);
    assert.equal(row.model.confidence <= 1, true);
    assert.equal(row.model.version.startsWith("composer_v2_"), true);
  }
});

test("model-guided composer regime gate variants are quota-bounded by top base arms", () => {
  const rows = withEnv(
    {
      SCALP_V2_REGIME_GATE_ENABLED: "true",
      SCALP_V2_REGIME_GATE_TOP_BASE_ARMS: "1",
    },
    () =>
      buildScalpV2ModelGuidedComposerGrid({
        venue: "bitget",
        symbol: "BTCUSDT",
        entrySessionProfile: "berlin",
        maxCandidates: 24,
      }),
  );
  const gatedRows = rows.filter((row) => Boolean(row.regimeGateId));
  assert.equal(gatedRows.length > 0, true);
  const baseArms = new Set(
    gatedRows.map((row) =>
      resolveModelGuidedComposerExecutionPlanFromTuneId(row.tuneId).baseArm,
    ),
  );
  assert.equal(baseArms.size, 1);
  for (const row of gatedRows) {
    assert.equal(parseRegimeGateFromTuneId(row.tuneId) !== null, true);
  }
});

test("model-guided composer can disable regime gate variants entirely", () => {
  const rows = withEnv(
    {
      SCALP_V2_REGIME_GATE_ENABLED: "false",
      SCALP_V2_REGIME_GATE_TOP_BASE_ARMS: "12",
    },
    () =>
      buildScalpV2ModelGuidedComposerGrid({
        venue: "capital",
        symbol: "EURUSD",
        entrySessionProfile: "newyork",
        maxCandidates: 24,
      }),
  );
  assert.equal(rows.length > 0, true);
  for (const row of rows) {
    assert.equal(row.regimeGateId ?? null, null);
    assert.equal(parseRegimeGateFromTuneId(row.tuneId), null);
  }
});

test("research cursor key is stable and normalized", () => {
  const key = toScalpV2ResearchCursorKey({
    venue: "capital",
    symbol: " eur/usd ",
    entrySessionProfile: "newyork",
  });
  assert.equal(key, "v2:capital:EURUSD:newyork");
});

test("coverage summary reports non-empty primitive inventory", () => {
  const summary = strategyPrimitiveCoverageSummary();
  assert.equal(summary.strategyCount >= 1, true);
  assert.equal(summary.strategiesCovered >= 1, true);
  assert.equal(summary.primitiveBlocks >= 20, true);
  assert.equal(summary.families.pattern > 0, true);
  assert.equal(summary.families.entry_trigger > 0, true);
});

test("candidate evaluation window rotates deterministically by cursor offset", () => {
  const outA = resolveScalpV2CandidateEvaluationWindow({
    candidates: ["a", "b", "c", "d"],
    maxCandidates: 2,
    startOffset: 0,
  });
  assert.deepEqual(outA.selectedCandidates, ["a", "b"]);
  assert.equal(outA.nextOffset, 2);

  const outB = resolveScalpV2CandidateEvaluationWindow({
    candidates: ["a", "b", "c", "d"],
    maxCandidates: 2,
    startOffset: outA.nextOffset,
  });
  assert.deepEqual(outB.selectedCandidates, ["c", "d"]);
  assert.equal(outB.nextOffset, 0);
});

test("candidate evaluation window evaluates full pool when max exceeds pool", () => {
  const out = resolveScalpV2CandidateEvaluationWindow({
    candidates: ["x", "y", "z"],
    maxCandidates: 10,
    startOffset: 2,
  });
  assert.deepEqual(out.selectedCandidates, ["z", "x", "y"]);
  assert.equal(out.poolSize, 3);
  assert.equal(out.evaluatedCount, 3);
  assert.equal(out.nextOffset, 2);
});
