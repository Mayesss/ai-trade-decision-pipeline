import assert from "node:assert/strict";
import test from "node:test";

import {
  predictSessionStructureStageAPass,
  type SessionStructureStageAPrediction,
} from "./sessionStructureAdaptiveSearch";
import type {
  SessionStructureAdaptivePriorEntry,
  SessionStructureAdaptivePriorSet,
} from "./sessionStructureComposer";
import {
  applySurrogatePrescreen,
  type SessionStructureSurrogateConfig,
} from "./sessionStructureSurrogate";

const PLAN = {
  contextId: "atr_expansion" as const,
  levelId: "opening_range_30m" as const,
  triggerId: "breakout_retest_hold" as const,
  confirmationId: "m15_close_acceptance" as const,
  managementId: "fixed_2r_time_4h" as const,
};

function entry(samples: number, passes: number): SessionStructureAdaptivePriorEntry {
  return { score: 0, samples, stageAPass: passes, stageBPass: 0, stageCPass: 0 };
}

function priorSet(
  globalKeys: Record<string, SessionStructureAdaptivePriorEntry>,
  baseRate = 0.11,
): SessionStructureAdaptivePriorSet {
  return {
    version: "test",
    generatedAtMs: 1,
    windowToTs: 1,
    minSamples: 8,
    stageABaseRate: baseRate,
    global: globalKeys,
    scoped: {},
    diagnostics: { rows: 0, scoredKeys: 0, stageAPass: 0, stageBPass: 0, stageCPass: 0 },
  };
}

function predictWith(globalKeys: Record<string, SessionStructureAdaptivePriorEntry>, plan = PLAN) {
  return predictSessionStructureStageAPass({
    plan,
    venue: "bitget",
    symbol: "BTCUSDT",
    session: "berlin",
    priors: priorSet(globalKeys),
  });
}

test("predict: high-pass trigger marginal pushes blended P above base (not skip-eligible)", () => {
  const high = predictWith({ "trigger:breakout_retest_hold": entry(400, 220) }); // 55%
  assert.ok(high.probability > 0.11, `expected >0.11, got ${high.probability}`);
  assert.equal(high.source, "blend");
  assert.equal(high.samples, 0, "marginal blend must be prioritise-only (0 samples)");
});

test("predict: well-sampled exact combo is trusted directly (behavior source)", () => {
  // behavior key matches the full fingerprint of PLAN.
  const fp =
    "atr_expansion|opening_range_30m|breakout_retest_hold|m15_close_acceptance|fixed_2r_time_4h";
  const b = predictWith({ [`behavior:${fp}`]: entry(60, 36) }); // 60% over 60 samples
  assert.equal(b.source, "behavior");
  assert.equal(b.samples, 60);
  // shrunk toward base but clearly elevated, and skip-eligible (real evidence).
  assert.ok(b.probability > 0.3, `expected elevated, got ${b.probability}`);
});

test("predict: exact-combo failure is trusted directly (skippable low P)", () => {
  const fp =
    "atr_expansion|opening_range_30m|breakout_retest_hold|m15_close_acceptance|fixed_2r_time_4h";
  const b = predictWith({ [`behavior:${fp}`]: entry(200, 1) }); // 0.5% over 200 samples
  assert.equal(b.source, "behavior");
  assert.ok(b.probability < 0.05, `expected hopeless, got ${b.probability}`);
  assert.ok(b.samples >= 40, "skip-eligible evidence");
});

test("predict: hopeless trigger pulls P well below base", () => {
  const low = predictWith({ "trigger:breakout_retest_hold": entry(400, 8) }); // 2%
  assert.ok(low.probability < 0.11, `expected <0.11, got ${low.probability}`);
});

test("predict: discriminative trigger is not washed out by a high-volume low-signal dim", () => {
  // context near base (huge sample) + strongly positive trigger → P stays elevated.
  const p = predictWith({
    "context:atr_expansion": entry(15000, 1500), // 10% ~ base
    "trigger:breakout_retest_hold": entry(400, 220), // 55%
  });
  assert.ok(p.probability > 0.3, `trigger signal lost: ${p.probability}`);
});

test("predict: never-seen pattern → base prob, zero evidence (not skip-eligible)", () => {
  const p = predictWith({});
  assert.equal(p.samples, 0);
  assert.equal(p.source, "base");
  assert.equal(p.probability, 0.11);
});

test("predict: thin evidence shrinks toward base", () => {
  const thin = predictWith({ "trigger:breakout_retest_hold": entry(2, 2) }); // 100% raw, 2 samples
  const strong = predictWith({ "trigger:breakout_retest_hold": entry(400, 400) }); // 100% raw, 400 samples
  assert.ok(thin.probability < strong.probability);
  assert.ok(thin.probability < 0.5, "2-sample 100% should shrink hard toward base");
});

test("predict: null priors → base 0", () => {
  const p = predictSessionStructureStageAPass({
    plan: PLAN,
    venue: "bitget",
    symbol: "BTCUSDT",
    session: "berlin",
    priors: null,
  });
  assert.equal(p.probability, 0);
  assert.equal(p.source, "base");
});

// --- applySurrogatePrescreen ---

type C = { id: string; p: number; n: number; explore?: boolean };
const cfg = (over: Partial<SessionStructureSurrogateConfig> = {}): SessionStructureSurrogateConfig => ({
  enabled: true,
  prioritize: true,
  skipProb: 0.02,
  skipMinSamples: 40,
  maxSkipPct: 0.5,
  ...over,
});
const predictC = (c: C): SessionStructureStageAPrediction => ({
  probability: c.p,
  samples: c.n,
  source: "blend",
});
const run = (candidates: C[], config = cfg()) =>
  applySurrogatePrescreen({
    candidates,
    predict: predictC,
    config,
    isExploration: (c) => Boolean(c.explore),
    tieBreak: (a, b) => a.id.localeCompare(b.id),
  });

test("prescreen: skips only high-evidence very-low-P, non-exploration", () => {
  const out = run([
    { id: "hopeless", p: 0.01, n: 100 }, // skip
    { id: "low_eviden", p: 0.01, n: 10 }, // low evidence → keep
    { id: "decent", p: 0.4, n: 100 }, // good → keep
    { id: "explore", p: 0.01, n: 100, explore: true }, // exploration → keep
  ]);
  assert.deepEqual(out.skipped.map((s) => (s.candidate as C).id), ["hopeless"]);
  assert.equal(out.ordered.length, 3);
});

test("prescreen: skip cap respects maxSkipPct and drops lowest-P first", () => {
  const candidates: C[] = Array.from({ length: 10 }, (_, i) => ({
    id: `c${i}`,
    p: 0.001 * i, // all <= skipProb for i<=20; all very low
    n: 100,
  }));
  const out = run(candidates, cfg({ maxSkipPct: 0.3, skipProb: 0.02 }));
  assert.equal(out.skipped.length, 3); // floor(10 * 0.3)
  // lowest P skipped first
  assert.deepEqual(out.skipped.map((s) => (s.candidate as C).id), ["c0", "c1", "c2"]);
});

test("prescreen: prioritise sorts survivors by P desc deterministically", () => {
  const out = run([
    { id: "a", p: 0.2, n: 5 },
    { id: "b", p: 0.6, n: 5 },
    { id: "c", p: 0.4, n: 5 },
  ]);
  assert.deepEqual(out.ordered.map((c) => (c as C).id), ["b", "c", "a"]);
});

test("prescreen: disabled → identity (no skip, original order)", () => {
  const candidates: C[] = [
    { id: "a", p: 0.01, n: 100 },
    { id: "b", p: 0.5, n: 100 },
  ];
  const out = run(candidates, cfg({ enabled: false }));
  assert.equal(out.skipped.length, 0);
  assert.deepEqual(out.ordered.map((c) => (c as C).id), ["a", "b"]);
});

test("prescreen: prioritise off → survivors keep input order", () => {
  const out = run(
    [
      { id: "a", p: 0.2, n: 5 },
      { id: "b", p: 0.6, n: 5 },
    ],
    cfg({ prioritize: false }),
  );
  assert.deepEqual(out.ordered.map((c) => (c as C).id), ["a", "b"]);
});
