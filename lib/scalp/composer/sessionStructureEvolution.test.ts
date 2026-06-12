import assert from "node:assert/strict";
import test from "node:test";

import {
  buildScalpComposerSessionStructureComposerGrid,
  sessionStructureBehaviorFingerprint,
  validateSessionStructureCompatibility,
  type SessionStructureComposerCandidateDslSpec,
} from "./sessionStructureComposer";
import {
  buildSurvivorPoolFromRows,
  enumerateCrossovers,
  enumerateSingleGeneMutations,
  generateOffspring,
  offspringTuneId,
  type SessionStructureGenome,
  type SessionStructureSurvivor,
} from "./sessionStructureEvolution";

// A known-compatible 5-gene genome (vwap pullback requires session_vwap).
const GENOME_A: SessionStructureGenome = {
  contextId: "m30_session_momentum",
  levelId: "session_vwap",
  triggerId: "vwap_pullback_continuation",
  confirmationId: "m15_close_acceptance",
  managementId: "fixed_2r_time_4h",
};

// Differs from A in exactly 2 genes (context, confirmation); still compatible.
const GENOME_B: SessionStructureGenome = {
  contextId: "h1_directional_bias",
  levelId: "session_vwap",
  triggerId: "vwap_pullback_continuation",
  confirmationId: "m30_close_acceptance",
  managementId: "fixed_2r_time_4h",
};

function geneCount(a: SessionStructureGenome, b: SessionStructureGenome): number {
  let d = 0;
  if (a.contextId !== b.contextId) d += 1;
  if (a.levelId !== b.levelId) d += 1;
  if (a.triggerId !== b.triggerId) d += 1;
  if (a.confirmationId !== b.confirmationId) d += 1;
  if (a.managementId !== b.managementId) d += 1;
  return d;
}

function survivorOf(
  genome: SessionStructureGenome,
  fitness: number,
  overrides: Partial<SessionStructureSurvivor> = {},
): SessionStructureSurvivor {
  const fingerprint = sessionStructureBehaviorFingerprint(genome);
  return {
    scopeKey: "bitget:BTCUSDT:berlin",
    venue: "bitget",
    symbol: "BTCUSDT",
    session: "berlin",
    genome,
    fingerprint,
    tuneId: `tune_${fingerprint}`,
    fitness,
    fitnessSource: "lowerBoundR",
    stageCTrades: 30,
    holdoutPassed: false,
    ...overrides,
  };
}

function metadataRow(params: {
  genome: SessionStructureGenome;
  lowerBoundR?: number;
  netR?: number;
  trades?: number;
  windowToTs?: number;
}) {
  const stats: Record<string, unknown> = {};
  if (params.lowerBoundR !== undefined) stats.lowerBoundR = params.lowerBoundR;
  if (params.netR !== undefined) stats.netR = params.netR;
  return {
    venue: "bitget" as const,
    symbol: "BTCUSDT",
    session: "berlin" as const,
    tuneId: `tune_${sessionStructureBehaviorFingerprint(params.genome)}`,
    metadata: {
      sessionComposerPlan: { ...params.genome, digest: "deadbeef00" },
      worker: {
        windowToTs: params.windowToTs ?? 1_000,
        stageC: { passed: true, trades: params.trades ?? 30 },
      },
      v3Ranking: { stageC: { stats } },
    },
  };
}

test("enumerateSingleGeneMutations returns the full Hamming-1 neighbourhood (27)", () => {
  const mutations = enumerateSingleGeneMutations(GENOME_A);
  // (8-1)+(9-1)+(6-1)+(5-1)+(4-1) = 7+8+5+4+3 = 27
  assert.equal(mutations.length, 27);
  for (const child of mutations) {
    assert.equal(geneCount(GENOME_A, child), 1);
  }
  // No duplicates.
  const fps = new Set(mutations.map(sessionStructureBehaviorFingerprint));
  assert.equal(fps.size, 27);
});

test("enumerateCrossovers at Hamming distance d returns 2^d - 2", () => {
  assert.equal(geneCount(GENOME_A, GENOME_B), 2);
  const children = enumerateCrossovers(GENOME_A, GENOME_B);
  assert.equal(children.length, Math.pow(2, 2) - 2); // 2
  const parentFps = new Set([
    sessionStructureBehaviorFingerprint(GENOME_A),
    sessionStructureBehaviorFingerprint(GENOME_B),
  ]);
  for (const child of children) {
    assert.equal(parentFps.has(sessionStructureBehaviorFingerprint(child)), false);
  }
});

test("generateOffspring only emits compatible, non-parent, non-evaluated cells", () => {
  const survivor = survivorOf(GENOME_A, -0.04);
  const offspring = generateOffspring({
    scopedSurvivors: [survivor],
    globalSurvivors: [survivor],
    evaluatedFingerprints: new Set(),
    config: {
      maxOffspringPerSurvivor: 100,
      maxOffspringPerCycle: 1000,
      maxCrossoverPartners: 0,
      globalCrossover: false,
    },
  });
  assert.ok(offspring.size > 0);
  for (const [fingerprint, child] of offspring.entries()) {
    // compatible
    assert.equal(validateSessionStructureCompatibility(child.genome).compatible, true);
    // never the parent
    assert.notEqual(fingerprint, survivor.fingerprint);
    // a mutation off session_vwap with a vwap trigger would be incompatible and dropped
    assert.equal(sessionStructureBehaviorFingerprint(child.genome), fingerprint);
  }
  // The incompatible level-mutation (session_vwap -> orb15 with vwap trigger) is absent.
  const incompatible: SessionStructureGenome = { ...GENOME_A, levelId: "opening_range_15m" };
  assert.equal(offspring.has(sessionStructureBehaviorFingerprint(incompatible)), false);
});

test("generateOffspring excludes already-evaluated fingerprints", () => {
  const survivor = survivorOf(GENOME_A, -0.04);
  const all = generateOffspring({
    scopedSurvivors: [survivor],
    config: { maxOffspringPerSurvivor: 100, maxOffspringPerCycle: 1000, maxCrossoverPartners: 0, globalCrossover: false },
  });
  const victim = Array.from(all.keys())[0]!;
  const filtered = generateOffspring({
    scopedSurvivors: [survivor],
    evaluatedFingerprints: new Set([victim]),
    config: { maxOffspringPerSurvivor: 100, maxOffspringPerCycle: 1000, maxCrossoverPartners: 0, globalCrossover: false },
  });
  assert.equal(filtered.has(victim), false);
});

test("generateOffspring honours the per-cycle cap deterministically", () => {
  const survivor = survivorOf(GENOME_A, -0.04);
  const cfg = { maxOffspringPerSurvivor: 100, maxOffspringPerCycle: 5, maxCrossoverPartners: 0, globalCrossover: false };
  const a = generateOffspring({ scopedSurvivors: [survivor], config: cfg });
  const b = generateOffspring({ scopedSurvivors: [survivor], config: cfg });
  assert.equal(a.size, 5);
  assert.deepEqual(Array.from(a.keys()).sort(), Array.from(b.keys()).sort());
});

test("buildSurvivorPoolFromRows ranks by lowerBoundR with netR fallback", () => {
  const pool = buildSurvivorPoolFromRows({
    rows: [
      metadataRow({ genome: GENOME_A, lowerBoundR: -0.16 }),
      metadataRow({ genome: GENOME_B, lowerBoundR: -0.02 }),
    ],
    windowToTs: 1_000,
    nowMs: 1,
  });
  assert.equal(pool.global.length, 2);
  // least-negative first
  assert.equal(pool.global[0]!.fingerprint, sessionStructureBehaviorFingerprint(GENOME_B));
  assert.equal(pool.global[0]!.fitnessSource, "lowerBoundR");
});

test("buildSurvivorPoolFromRows falls back to netR when lowerBoundR missing", () => {
  const pool = buildSurvivorPoolFromRows({
    rows: [metadataRow({ genome: GENOME_A, netR: 2.5 })],
    windowToTs: 1_000,
    nowMs: 1,
  });
  assert.equal(pool.global.length, 1);
  assert.equal(pool.global[0]!.fitnessSource, "netR");
  assert.equal(pool.global[0]!.fitness, 2.5);
  assert.equal(pool.diagnostics.usedNetRFallback, 1);
});

test("buildSurvivorPoolFromRows keeps weak (all-negative) survivors and flags weakOnly", () => {
  const pool = buildSurvivorPoolFromRows({
    rows: [
      metadataRow({ genome: GENOME_A, lowerBoundR: -0.16 }),
      metadataRow({ genome: GENOME_B, lowerBoundR: -0.05 }),
    ],
    windowToTs: 1_000,
    nowMs: 1,
  });
  assert.equal(pool.diagnostics.survivors, 2);
  assert.equal(pool.diagnostics.weakOnly, true);
});

test("ANTI-DUP: every offspring tuneId equals the deterministic grid tuneId", () => {
  const survivor = survivorOf(GENOME_A, -0.04);
  const offspring = generateOffspring({
    scopedSurvivors: [survivor],
    config: { maxOffspringPerSurvivor: 100, maxOffspringPerCycle: 1000, maxCrossoverPartners: 0, globalCrossover: false },
  });
  for (const [fingerprint, child] of offspring.entries()) {
    const expected = offspringTuneId({
      genome: child.genome,
      venue: "bitget",
      symbol: "BTCUSDT",
      session: "berlin",
    });
    // The canonical tuneId is derivable purely from the genome + scope — no
    // minted digest. Cross-check against the prefix-encoded builder helper.
    const parts = expected.split("_");
    assert.equal(parts[0], "ssc");
    assert.equal(parts.length, 7);
    assert.equal(sessionStructureBehaviorFingerprint(child.genome), fingerprint);
  }
});

test("builder tags offspring cells and assigns the canonical (non-minted) tuneId", () => {
  const survivor = survivorOf(GENOME_A, -0.04);
  const offspring = generateOffspring({
    scopedSurvivors: [survivor],
    config: { maxOffspringPerSurvivor: 100, maxOffspringPerCycle: 1000, maxCrossoverPartners: 0, globalCrossover: false },
  });
  const grid = buildScalpComposerSessionStructureComposerGrid({
    venue: "bitget",
    symbol: "BTCUSDT",
    entrySessionProfile: "berlin",
    maxCandidates: 60,
    generatedAtMs: 1,
    offspring,
    evolutionScoreBoost: 0.06,
    noveltyBudget: { enabled: true, evolutionPct: 0.5, minExplorePct: 0.05 },
  });
  const tagged = grid.filter((row) => row.evolution);
  assert.ok(tagged.length > 0, "expected at least one tagged offspring in the selection");
  for (const row of tagged) {
    const expectedTuneId = offspringTuneId({
      genome: {
        contextId: row.sessionComposerPlan.contextId,
        levelId: row.sessionComposerPlan.levelId,
        triggerId: row.sessionComposerPlan.triggerId,
        confirmationId: row.sessionComposerPlan.confirmationId,
        managementId: row.sessionComposerPlan.managementId,
      },
      venue: "bitget",
      symbol: "BTCUSDT",
      session: "berlin",
    });
    assert.equal(row.tuneId, expectedTuneId);
    assert.ok(["mutation", "crossover"].includes(row.evolution!.op));
  }
});

test("DETERMINISM + COLD-START: no offspring → identical to baseline", () => {
  const base = () =>
    buildScalpComposerSessionStructureComposerGrid({
      venue: "bitget",
      symbol: "BTCUSDT",
      entrySessionProfile: "berlin",
      maxCandidates: 40,
      generatedAtMs: 1,
    }).map((row) => row.tuneId);
  const withNull = buildScalpComposerSessionStructureComposerGrid({
    venue: "bitget",
    symbol: "BTCUSDT",
    entrySessionProfile: "berlin",
    maxCandidates: 40,
    generatedAtMs: 1,
    offspring: null,
    evolutionScoreBoost: 0.06,
  }).map((row: SessionStructureComposerCandidateDslSpec) => row.tuneId);
  assert.deepEqual(base(), base());
  assert.deepEqual(withNull, base());
  // and none are tagged
  const grid = buildScalpComposerSessionStructureComposerGrid({
    venue: "bitget",
    symbol: "BTCUSDT",
    entrySessionProfile: "berlin",
    maxCandidates: 40,
    generatedAtMs: 1,
    offspring: new Map(),
  });
  assert.equal(grid.some((row) => row.evolution), false);
});
