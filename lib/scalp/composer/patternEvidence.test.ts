import assert from "node:assert/strict";
import test from "node:test";

import {
  SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED,
  aggregateScalpV2PatternEdges,
  buildScalpV2PatternKey,
  selectScalpV2PatternRepresentativeCandidates,
  type ScalpV2PatternCandidateSummary,
  type ScalpV2PatternTradeVector,
} from "./patternEvidence";

function candidate(
  id: number,
  overrides: Partial<ScalpV2PatternCandidateSummary> = {},
): ScalpV2PatternCandidateSummary {
  return {
    candidateId: id,
    venue: "capital",
    symbol: "EURUSD",
    session: "berlin",
    behaviorFingerprint: "ctx|level|trigger|confirm|manage",
    windowToTs: 1_800_000_000_000,
    stageCLowerBoundR: 0,
    stageCNetR: 0,
    stageCTrades: 0,
    ...overrides,
  };
}

function trade(
  symbol: string,
  bucketStartTs: number,
  rMultiple: number,
  index: number,
): ScalpV2PatternTradeVector {
  const behaviorFingerprint = "ctx|level|trigger|confirm|manage";
  return {
    candidateId: index,
    venue: "capital",
    symbol,
    strategyId: "session_structure_composer_v1",
    tuneId: `tune_${index}`,
    session: "berlin",
    windowToTs: 1_800_000_000_000,
    stageId: "c",
    replayTradeIndex: index,
    behaviorFingerprint,
    patternKey: buildScalpV2PatternKey({
      venue: "capital",
      session: "berlin",
      behaviorFingerprint,
    }),
    entryTs: bucketStartTs,
    exitTs: bucketStartTs + 60_000,
    bucketStartTs,
    side: "BUY",
    exitReason: "TP",
    rMultiple,
    feeR: null,
    grossRMultiple: null,
  };
}

test("selectScalpV2PatternRepresentativeCandidates keeps best clone per symbol pattern window", () => {
  const rows = [
    candidate(1, { stageCLowerBoundR: -0.01, stageCNetR: 8, stageCTrades: 40 }),
    candidate(2, { stageCLowerBoundR: 0.03, stageCNetR: 3, stageCTrades: 30 }),
    candidate(3, {
      symbol: "GBPUSD",
      stageCLowerBoundR: -0.02,
      stageCNetR: 10,
      stageCTrades: 50,
    }),
  ];

  const reps = selectScalpV2PatternRepresentativeCandidates(rows);

  assert.equal(reps.length, 2);
  assert.equal(reps.find((row) => row.symbol === "EURUSD")?.candidateId, 2);
  assert.equal(reps.find((row) => row.symbol === "GBPUSD")?.candidateId, 3);
});

test("aggregateScalpV2PatternEdges uses 1h bucket observations as authoritative sample", () => {
  const bucket = Date.UTC(2026, 0, 1, 8);
  const rows = [
    trade("EURUSD", bucket, 0.5, 1),
    trade("GBPUSD", bucket, 0.4, 2),
    trade("USDJPY", bucket, -0.1, 3),
    trade("EURUSD", bucket + 60 * 60_000, 0.3, 4),
  ];

  const [edge] = aggregateScalpV2PatternEdges({
    trades: rows,
    candidateCount: 4,
    representativeCandidateCount: 3,
    bucketMinutes: 60,
  });

  assert.equal(edge.rawTrades, 4);
  assert.equal(edge.bucketCount, 2);
  assert.equal(edge.bucketNetR.toFixed(6), edge.rawNetR.toFixed(6));
  assert.equal(edge.scoreJson.authoritativeMetric, "bucketLowerBoundR");
  assert.ok(edge.bucketLowerBoundR !== edge.rawLowerBoundR);
});

test("aggregateScalpV2PatternEdges computes symbol breadth and leave-one-symbol-out lower bound", () => {
  const bucket = Date.UTC(2026, 0, 1, 8);
  const rows = [
    trade("EURUSD", bucket, 2, 1),
    trade("EURUSD", bucket + 60 * 60_000, 2, 2),
    trade("GBPUSD", bucket, -0.2, 3),
    trade("USDJPY", bucket + 60 * 60_000, 0.1, 4),
  ];

  const [edge] = aggregateScalpV2PatternEdges({
    trades: rows,
    candidateCount: 3,
    representativeCandidateCount: 3,
    bucketMinutes: 60,
  });

  assert.equal(edge.symbolCount, 3);
  assert.equal(edge.positiveSymbolCount, 2);
  assert.equal(edge.topSymbol, "EURUSD");
  assert.ok(edge.topSymbolConcentrationPct > 90);
  assert.notEqual(edge.leaveOneSymbolOutBucketLowerBoundR, null);
  assert.ok(edge.leaveOneSymbolOutBucketLowerBoundR! < edge.bucketLowerBoundR);
});

test("aggregateScalpV2PatternEdges labels stage-C-passed survivor-only evidence", () => {
  const [edge] = aggregateScalpV2PatternEdges({
    trades: [trade("EURUSD", Date.UTC(2026, 0, 1, 8), 1, 1)],
    candidateCount: 1,
    representativeCandidateCount: 1,
    populationScope: SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED,
  });

  assert.equal(edge.populationScope, SCALP_V2_PATTERN_EVIDENCE_POPULATION_STAGE_C_PASSED);
  assert.equal(edge.scoreJson.survivorOnly, true);
  assert.match(String(edge.scoreJson.warning), /Survivor-only/);
});
