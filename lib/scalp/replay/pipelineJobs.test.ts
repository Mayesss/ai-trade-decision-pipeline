import assert from "node:assert/strict";
import test from "node:test";

import {
  applyPromotionHysteresis,
  buildPipelineJobDiagnostics,
  buildDiscoverSymbolSyncPlan,
  computeStagedEvaluationScoreForTasks,
  enforceSingleEnabledPerSymbolStrategy,
  filterScalpSundayCandles,
  listScalpDurationTimelineRuns,
  planPrepareOverflowNonEnabledVariants,
  resolveLoadCandlesFetchUpperBoundMs,
  resolveLifecycleTuneFamily,
  resolveStageSurvivorCount,
  resolveStagedRebucket,
  selectPrepareTuneVariantsForStrategy,
  selectPromotionWinnerRowsWithExploration,
  startOfBerlinWeekMonday,
  type PromotionSelectionRow,
} from "../pipelineJobs";
import type { ScalpPromotionForwardValidationCandidate } from "../promotionPolicy";

function makeCandidate(params: {
  deploymentId: string;
  symbol: string;
  strategyId?: string;
  tuneId?: string;
  selectionScore: number;
}): ScalpPromotionForwardValidationCandidate {
  const strategyId = params.strategyId || "regime_pullback_m15_m3";
  const tuneId = params.tuneId || "base";
  return {
    deploymentId: params.deploymentId,
    symbol: params.symbol,
    strategyId,
    tuneId,
    rollCount: 14,
    profitableWindowPct: 62,
    profitableWindows: 9,
    meanExpectancyR: 0.11,
    trimmedMeanExpectancyR: 0.1,
    medianExpectancyR: 0.1,
    meanProfitFactor: 1.25,
    maxDrawdownR: 1.2,
    topWindowPnlConcentrationPct: 42,
    selectionScore: params.selectionScore,
    minTradesPerWindow: 2,
    totalTrades: 38,
    selectionWindowDays: 91,
    forwardWindowDays: 7,
    forwardValidation: {
      rollCount: 14,
      profitableWindowPct: 62,
      meanExpectancyR: 0.11,
      meanProfitFactor: 1.25,
      maxDrawdownR: 1.2,
      minTradesPerWindow: 2,
      selectionWindowDays: 91,
      forwardWindowDays: 7,
    },
  };
}

test("buildDiscoverSymbolSyncPlan computes active/catalog adds and removals", () => {
  const plan = buildDiscoverSymbolSyncPlan({
    existingRows: [
      { symbol: "btcusdt", active: true },
      { symbol: "ethusdt", active: true },
      { symbol: "xrpusdt", active: false },
    ],
    activeSymbols: ["ETHUSDT", "SOLUSDT"],
    catalogSymbols: ["ETHUSDT", "SOLUSDT", "ADAUSDT"],
  });

  assert.deepEqual(plan.addedActiveSymbols, ["SOLUSDT"]);
  assert.deepEqual(plan.removedActiveSymbols, ["BTCUSDT"]);
  assert.deepEqual(plan.catalogAddedSymbols, ["SOLUSDT", "ADAUSDT"]);
});

test("applyPromotionHysteresis applies immediate enable/disable decisions", () => {
  const nowMs = Date.UTC(2026, 2, 19, 20, 0, 0);

  const firstFail = applyPromotionHysteresis({
    currentlyEnabled: true,
    shouldEnableNow: false,
    previous: null,
    nowMs,
  });
  assert.equal(firstFail.enabled, false);
  assert.equal(firstFail.transition, "disabled");
  assert.equal(firstFail.hysteresis.failStreak, 1);

  const firstPass = applyPromotionHysteresis({
    currentlyEnabled: firstFail.enabled,
    shouldEnableNow: true,
    previous: firstFail.hysteresis,
    nowMs: nowMs + 120_000,
  });
  assert.equal(firstPass.enabled, true);
  assert.equal(firstPass.transition, "enabled");
  assert.equal(firstPass.hysteresis.passStreak, 1);
});

test("applyPromotionHysteresis keeps currently enabled deployment on when lockEnabled is true", () => {
  const nowMs = Date.UTC(2026, 2, 19, 21, 0, 0);
  const out = applyPromotionHysteresis({
    currentlyEnabled: true,
    shouldEnableNow: false,
    previous: {
      passStreak: 0,
      failStreak: 1,
      lastStateChangeAtMs: nowMs - 60_000,
      lastDecision: "hold",
    },
    nowMs,
    lockEnabled: true,
  });
  assert.equal(out.enabled, true);
  assert.equal(out.transition, "held");
  assert.equal(out.hysteresis.failStreak, 1);
});

test("resolveStageSurvivorCount keeps configured share with min survivor floor", () => {
  assert.equal(
    resolveStageSurvivorCount({
      cohortSize: 5,
      keepShare: 0.4,
      minSurvivors: 1,
    }),
    2,
  );
  assert.equal(
    resolveStageSurvivorCount({
      cohortSize: 3,
      keepShare: 0.1,
      minSurvivors: 1,
    }),
    1,
  );
  assert.equal(
    resolveStageSurvivorCount({
      cohortSize: 7,
      keepShare: 0.35,
      minSurvivors: 2,
    }),
    3,
  );
  assert.equal(
    resolveStageSurvivorCount({
      cohortSize: 0,
      keepShare: 0.4,
      minSurvivors: 1,
    }),
    0,
  );
});

test("resolveStagedRebucket maps completed-week ranges into A/B/C windows", () => {
  const cfg = { stageAWeeks: 2, stageBWeeks: 6, stageCWeeks: 12 };
  assert.deepEqual(
    resolveStagedRebucket({ completedWeeks: 1, ...cfg }),
    { stage: "A", targetWeeks: 2 },
  );
  assert.deepEqual(
    resolveStagedRebucket({ completedWeeks: 5, ...cfg }),
    { stage: "A", targetWeeks: 2 },
  );
  assert.deepEqual(
    resolveStagedRebucket({ completedWeeks: 6, ...cfg }),
    { stage: "B", targetWeeks: 6 },
  );
  assert.deepEqual(
    resolveStagedRebucket({ completedWeeks: 11, ...cfg }),
    { stage: "B", targetWeeks: 6 },
  );
  assert.deepEqual(
    resolveStagedRebucket({ completedWeeks: 12, ...cfg }),
    { stage: "C", targetWeeks: 12 },
  );
});

test("computeStagedEvaluationScoreForTasks ranks stronger short-window performance higher", () => {
  const stronger = computeStagedEvaluationScoreForTasks([
    {
      deploymentId: "dep_a",
      symbol: "BTCUSDT",
      strategyId: "regime_pullback_m15_m3",
      tuneId: "default",
      status: "completed",
      windowFromTs: 1,
      windowToTs: 2,
      result: { expectancyR: 0.12, netR: 0.6, maxDrawdownR: 0.8 },
    } as any,
    {
      deploymentId: "dep_a",
      symbol: "BTCUSDT",
      strategyId: "regime_pullback_m15_m3",
      tuneId: "default",
      status: "completed",
      windowFromTs: 2,
      windowToTs: 3,
      result: { expectancyR: 0.1, netR: 0.4, maxDrawdownR: 0.9 },
    } as any,
  ]);
  const weaker = computeStagedEvaluationScoreForTasks([
    {
      deploymentId: "dep_b",
      symbol: "BTCUSDT",
      strategyId: "regime_pullback_m15_m3",
      tuneId: "default",
      status: "completed",
      windowFromTs: 1,
      windowToTs: 2,
      result: { expectancyR: -0.02, netR: -0.1, maxDrawdownR: 1.5 },
    } as any,
    {
      deploymentId: "dep_b",
      symbol: "BTCUSDT",
      strategyId: "regime_pullback_m15_m3",
      tuneId: "default",
      status: "completed",
      windowFromTs: 2,
      windowToTs: 3,
      result: { expectancyR: 0.01, netR: 0.05, maxDrawdownR: 1.4 },
    } as any,
  ]);
  assert.equal(Number.isFinite(stronger), true);
  assert.equal(stronger > weaker, true);
  assert.equal(
    computeStagedEvaluationScoreForTasks([] as any),
    Number.NEGATIVE_INFINITY,
  );
});

test("selectPromotionWinnerRowsWithExploration enforces 40% exploration split when possible", () => {
  const rows: PromotionSelectionRow[] = [
    {
      deploymentId: "inc_1",
      symbol: "BTCUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "inc_1",
        symbol: "BTCUSDT",
        selectionScore: 10,
      }),
    },
    {
      deploymentId: "inc_2",
      symbol: "ETHUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "inc_2",
        symbol: "ETHUSDT",
        selectionScore: 9,
      }),
    },
    {
      deploymentId: "inc_3",
      symbol: "XAUUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "inc_3",
        symbol: "XAUUSDT",
        selectionScore: 8,
      }),
    },
    {
      deploymentId: "chal_1",
      symbol: "SOLUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "chal_1",
        symbol: "SOLUSDT",
        selectionScore: 7,
      }),
    },
    {
      deploymentId: "chal_2",
      symbol: "ADAUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "chal_2",
        symbol: "ADAUSDT",
        selectionScore: 6,
      }),
    },
    {
      deploymentId: "chal_3",
      symbol: "UNIUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "chal_3",
        symbol: "UNIUSDT",
        selectionScore: 5,
      }),
    },
  ];

  const out = selectPromotionWinnerRowsWithExploration({
    rows,
    explorationShare: 0.4,
    maxSymbols: 10,
    maxPerSymbol: 2,
    maxDeployments: 5,
  });

  assert.equal(out.exploitSlots, 3);
  assert.equal(out.explorationSlots, 2);
  assert.equal(out.exploitSelected, 3);
  assert.equal(out.explorationSelected, 2);
  assert.equal(out.selectedRows.length, 5);
  assert.equal(out.winnerIds.has("inc_1"), true);
  assert.equal(out.winnerIds.has("inc_2"), true);
  assert.equal(out.winnerIds.has("inc_3"), true);
  assert.equal(out.winnerIds.has("chal_1"), true);
  assert.equal(out.winnerIds.has("chal_2"), true);
});

test("selectPromotionWinnerRowsWithExploration respects per-symbol cap", () => {
  const rows: PromotionSelectionRow[] = [
    {
      deploymentId: "btc_inc",
      symbol: "BTCUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "btc_inc",
        symbol: "BTCUSDT",
        selectionScore: 10,
      }),
    },
    {
      deploymentId: "btc_chal",
      symbol: "BTCUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "btc_chal",
        symbol: "BTCUSDT",
        selectionScore: 9,
      }),
    },
    {
      deploymentId: "eth_inc",
      symbol: "ETHUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "eth_inc",
        symbol: "ETHUSDT",
        selectionScore: 8,
      }),
    },
  ];

  const out = selectPromotionWinnerRowsWithExploration({
    rows,
    explorationShare: 0.4,
    maxSymbols: 5,
    maxPerSymbol: 1,
    maxDeployments: 3,
  });

  const btcRows = out.selectedRows.filter((row) => row.symbol === "BTCUSDT");
  assert.equal(btcRows.length, 1);
});

test("selectPromotionWinnerRowsWithExploration keeps one row per incumbent symbol when capacity allows", () => {
  const rows: PromotionSelectionRow[] = [
    {
      deploymentId: "btc_inc_low",
      symbol: "BTCUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "btc_inc_low",
        symbol: "BTCUSDT",
        selectionScore: 2,
      }),
    },
    {
      deploymentId: "eth_inc_low",
      symbol: "ETHUSDT",
      incumbent: true,
      candidate: makeCandidate({
        deploymentId: "eth_inc_low",
        symbol: "ETHUSDT",
        selectionScore: 1.5,
      }),
    },
    {
      deploymentId: "sol_chal_high",
      symbol: "SOLUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "sol_chal_high",
        symbol: "SOLUSDT",
        selectionScore: 10,
      }),
    },
    {
      deploymentId: "ada_chal_high",
      symbol: "ADAUSDT",
      incumbent: false,
      candidate: makeCandidate({
        deploymentId: "ada_chal_high",
        symbol: "ADAUSDT",
        selectionScore: 9,
      }),
    },
  ];

  const out = selectPromotionWinnerRowsWithExploration({
    rows,
    explorationShare: 0.4,
    maxSymbols: 10,
    maxPerSymbol: 2,
    maxDeployments: 3,
  });

  const selectedSymbols = new Set(out.selectedRows.map((row) => row.symbol));
  assert.equal(selectedSymbols.has("BTCUSDT"), true);
  assert.equal(selectedSymbols.has("ETHUSDT"), true);
});

test("enforceSingleEnabledPerSymbolStrategy keeps only shortlisted enabled deployment", () => {
  const out = enforceSingleEnabledPerSymbolStrategy({
    rows: [
      {
        deploymentId: "dep_a",
        symbol: "FETUSDT",
        strategyId: "compression_breakout_pullback_m15_m3",
        tuneId: "auto_tr1p5",
        enabled: true,
        shortlistIncluded: false,
      },
      {
        deploymentId: "dep_b",
        symbol: "FETUSDT",
        strategyId: "compression_breakout_pullback_m15_m3",
        tuneId: "auto_tr1p6",
        enabled: true,
        shortlistIncluded: true,
      },
    ],
  });

  assert.equal(out.primaryEnabledIds.has("dep_b"), true);
  assert.equal(out.demotedIds.has("dep_a"), true);
  assert.equal(out.demotedIds.has("dep_b"), false);
});

test("enforceSingleEnabledPerSymbolStrategy falls back to candidate ranking when shortlist is absent", () => {
  const out = enforceSingleEnabledPerSymbolStrategy({
    rows: [
      {
        deploymentId: "dep_low",
        symbol: "FETUSDT",
        strategyId: "compression_breakout_pullback_m15_m3",
        tuneId: "auto_tr1p5",
        enabled: true,
        shortlistIncluded: false,
        candidate: makeCandidate({
          deploymentId: "dep_low",
          symbol: "FETUSDT",
          strategyId: "compression_breakout_pullback_m15_m3",
          tuneId: "auto_tr1p5",
          selectionScore: 0.1,
        }),
      },
      {
        deploymentId: "dep_high",
        symbol: "FETUSDT",
        strategyId: "compression_breakout_pullback_m15_m3",
        tuneId: "auto_tr1p6",
        enabled: true,
        shortlistIncluded: false,
        candidate: makeCandidate({
          deploymentId: "dep_high",
          symbol: "FETUSDT",
          strategyId: "compression_breakout_pullback_m15_m3",
          tuneId: "auto_tr1p6",
          selectionScore: 0.9,
        }),
      },
    ],
  });

  assert.equal(out.primaryEnabledIds.has("dep_high"), true);
  assert.equal(out.demotedIds.has("dep_low"), true);
});

test("resolveLifecycleTuneFamily normalizes tune id families", () => {
  assert.equal(resolveLifecycleTuneFamily("default"), "base");
  assert.equal(resolveLifecycleTuneFamily("auto_tr1p6"), "auto_tr");
  assert.equal(resolveLifecycleTuneFamily("auto_mix_tr1p4_ts18"), "auto_mix");
  assert.equal(resolveLifecycleTuneFamily("AUTO_SP_BERLIN"), "auto_sp");
});

test("prepare variant selection warms up with a small seed before wider expansion", () => {
  const out = selectPrepareTuneVariantsForStrategy({
    symbol: "FETUSDT",
    strategyId: "regime_pullback_m15_m3",
    nowMs: Date.UTC(2026, 2, 21, 9, 0, 0),
    maxSelected: 4,
    seedTarget: 2,
    maxVariantPool: 32,
    maxNewPerRun: 1,
    winnerNeighborRadius: 1,
    existingByKey: new Map(),
  } as any);

  assert.equal(out.length, 2);
  assert.equal(out[0]?.tuneId, "default");
});

test("prepare variant selection does not expand loser tracks with negative expectancy", () => {
  const existingByKey = new Map([
    [
      "regime_pullback_m15_m3::default",
      {
        deploymentId: "dep_loser_default",
        strategyId: "regime_pullback_m15_m3",
        tuneId: "default",
        enabled: false,
        inUniverse: false,
        promotionGate: {
          forwardValidation: {
            medianExpectancyR: -0.08,
            meanProfitFactor: 0.85,
          },
        },
        createdAtMs: 10,
        updatedAtMs: 10,
      },
    ],
  ]);

  const out = selectPrepareTuneVariantsForStrategy({
    symbol: "FETUSDT",
    strategyId: "regime_pullback_m15_m3",
    nowMs: Date.UTC(2026, 2, 21, 10, 0, 0),
    maxSelected: 4,
    seedTarget: 2,
    maxVariantPool: 32,
    maxNewPerRun: 2,
    winnerNeighborRadius: 1,
    existingByKey,
  } as any);

  assert.equal(out.length, 1);
  assert.equal(out[0]?.tuneId, "default");
});

test("prepare variant selection can expand around anchored tracks", () => {
  const existingByKey = new Map([
    [
      "regime_pullback_m15_m3::default",
      {
        deploymentId: "dep_winner_default",
        strategyId: "regime_pullback_m15_m3",
        tuneId: "default",
        enabled: false,
        inUniverse: true,
        promotionGate: {
          forwardValidation: {
            medianExpectancyR: 0.06,
            meanProfitFactor: 1.25,
          },
        },
        createdAtMs: 20,
        updatedAtMs: 20,
      },
    ],
  ]);

  const out = selectPrepareTuneVariantsForStrategy({
    symbol: "FETUSDT",
    strategyId: "regime_pullback_m15_m3",
    nowMs: Date.UTC(2026, 2, 21, 10, 30, 0),
    maxSelected: 4,
    seedTarget: 2,
    maxVariantPool: 32,
    maxNewPerRun: 2,
    winnerNeighborRadius: 1,
    existingByKey,
  } as any);

  assert.equal(out.length >= 2, true);
  assert.equal(out.some((row) => row.tuneId === "default"), true);
});

test("prepare variant selection can disable new variants for Sunday backfill mode", () => {
  const outWithoutExisting = selectPrepareTuneVariantsForStrategy({
    symbol: "FETUSDT",
    strategyId: "regime_pullback_m15_m3",
    nowMs: Date.UTC(2026, 2, 22, 9, 0, 0),
    maxSelected: 4,
    seedTarget: 2,
    maxVariantPool: 32,
    maxNewPerRun: 2,
    winnerNeighborRadius: 1,
    allowNewVariants: false,
    existingByKey: new Map(),
  } as any);
  assert.equal(outWithoutExisting.length, 0);

  const existingByKey = new Map([
    [
      "regime_pullback_m15_m3::default",
      {
        deploymentId: "dep_existing_default",
        strategyId: "regime_pullback_m15_m3",
        tuneId: "default",
        enabled: true,
        inUniverse: true,
        promotionGate: null,
        createdAtMs: 10,
        updatedAtMs: 10,
      },
    ],
  ]);
  const outWithExisting = selectPrepareTuneVariantsForStrategy({
    symbol: "FETUSDT",
    strategyId: "regime_pullback_m15_m3",
    nowMs: Date.UTC(2026, 2, 22, 9, 0, 0),
    maxSelected: 4,
    seedTarget: 2,
    maxVariantPool: 32,
    maxNewPerRun: 2,
    winnerNeighborRadius: 1,
    allowNewVariants: false,
    existingByKey,
  } as any);
  assert.deepEqual(
    outWithExisting.map((row) => row.tuneId),
    ["default"],
  );
});

test("resolveLoadCandlesFetchUpperBoundMs clamps Sunday UTC to Saturday close", () => {
  const sundayNoonUtc = Date.UTC(2026, 2, 22, 12, 0, 0); // Sunday
  const outSunday = resolveLoadCandlesFetchUpperBoundMs(sundayNoonUtc);
  assert.equal(outSunday, Date.UTC(2026, 2, 22, 0, 0, 0) - 1);

  const mondayMorningUtc = Date.UTC(2026, 2, 23, 8, 15, 0); // Monday
  const outMonday = resolveLoadCandlesFetchUpperBoundMs(mondayMorningUtc);
  assert.equal(outMonday, mondayMorningUtc);
});

test("filterScalpSundayCandles removes UTC Sunday candles", () => {
  const saturday = Date.UTC(2026, 2, 21, 23, 59, 0);
  const sunday = Date.UTC(2026, 2, 22, 10, 0, 0);
  const monday = Date.UTC(2026, 2, 23, 0, 1, 0);
  const out = filterScalpSundayCandles([
    [saturday, 1, 2, 1, 1.5, 10],
    [sunday, 1, 2, 1, 1.5, 11],
    [monday, 1, 2, 1, 1.5, 12],
  ] as any);
  assert.deepEqual(
    out.map((row) => Number(row[0])),
    [saturday, monday],
  );
});

test("prepare overflow planning keeps protected/enabled rows and prunes oldest non-enabled overflow", () => {
  const out = planPrepareOverflowNonEnabledVariants({
    hardCap: 3,
    protectedDeploymentIds: ["dep_4"],
    rows: [
      {
        deploymentId: "dep_1",
        enabled: false,
        inUniverse: true,
        promotionGate: { forwardValidation: { meanExpectancyR: 0.01 } },
        createdAtMs: 10,
        updatedAtMs: 10,
      },
      {
        deploymentId: "dep_2",
        enabled: true,
        inUniverse: true,
        promotionGate: { forwardValidation: { meanExpectancyR: 0.05 } },
        createdAtMs: 20,
        updatedAtMs: 20,
      },
      {
        deploymentId: "dep_3",
        enabled: false,
        inUniverse: true,
        promotionGate: { forwardValidation: { meanExpectancyR: 0.08 } },
        createdAtMs: 30,
        updatedAtMs: 30,
      },
      {
        deploymentId: "dep_4",
        enabled: false,
        inUniverse: true,
        promotionGate: null,
        createdAtMs: 40,
        updatedAtMs: 40,
      },
      {
        deploymentId: "dep_5",
        enabled: false,
        inUniverse: false,
        promotionGate: { forwardValidation: { meanExpectancyR: -0.2 } },
        createdAtMs: 50,
        updatedAtMs: 50,
      },
    ],
  });

  assert.equal(out.keepIds.includes("dep_2"), true);
  assert.equal(out.keepIds.includes("dep_4"), true);
  assert.deepEqual(out.pruneIds, ["dep_1", "dep_5"]);
});

test("prepare overflow planning protects recently demoted rows from pruning", () => {
  const nowMs = Date.UTC(2026, 2, 23, 0, 5, 0);
  const out = planPrepareOverflowNonEnabledVariants({
    hardCap: 2,
    nowMs,
    protectRecentDemotedMs: 7 * 24 * 60 * 60_000,
    rows: [
      {
        deploymentId: "dep_enabled",
        enabled: true,
        inUniverse: true,
        promotionGate: null,
        createdAtMs: 10,
        updatedAtMs: 10,
      },
      {
        deploymentId: "dep_recently_demoted",
        enabled: false,
        inUniverse: true,
        promotionGate: {
          lifecycle: {
            state: "graduated",
            lastSeatReleaseAtMs: nowMs - 60_000,
          },
        },
        createdAtMs: 20,
        updatedAtMs: 20,
      },
      {
        deploymentId: "dep_old_non_enabled",
        enabled: false,
        inUniverse: true,
        promotionGate: null,
        createdAtMs: 5,
        updatedAtMs: 5,
      },
    ],
  });

  assert.equal(out.keepIds.includes("dep_enabled"), true);
  assert.equal(out.keepIds.includes("dep_recently_demoted"), true);
  assert.deepEqual(out.pruneIds, ["dep_old_non_enabled"]);
});

test("prepare overflow planning protects suspended and freshness-incomplete rows from pruning", () => {
  const out = planPrepareOverflowNonEnabledVariants({
    hardCap: 1,
    rows: [
      {
        deploymentId: "dep_suspended",
        enabled: false,
        inUniverse: true,
        promotionGate: { lifecycle: { state: "suspended" } },
        createdAtMs: 10,
        updatedAtMs: 10,
      },
      {
        deploymentId: "dep_freshness_incomplete",
        enabled: false,
        inUniverse: true,
        promotionGate: { reason: "fresh_weeks_incomplete" },
        createdAtMs: 20,
        updatedAtMs: 20,
      },
      {
        deploymentId: "dep_regular_old",
        enabled: false,
        inUniverse: false,
        promotionGate: null,
        createdAtMs: 5,
        updatedAtMs: 5,
      },
    ],
  });

  assert.equal(out.keepIds.includes("dep_suspended"), true);
  assert.equal(out.keepIds.includes("dep_freshness_incomplete"), true);
  assert.deepEqual(out.pruneIds, ["dep_regular_old"]);
});

test("startOfBerlinWeekMonday resolves Monday boundary in Berlin timezone", () => {
  const tsMs = Date.UTC(2026, 2, 18, 12, 0, 0); // 2026-03-18T12:00:00Z
  const weekStartMs = startOfBerlinWeekMonday(tsMs);
  assert.equal(weekStartMs, Date.UTC(2026, 2, 15, 23, 0, 0));
});

test("buildPipelineJobDiagnostics computes duration for completed runs", () => {
  const out = buildPipelineJobDiagnostics(1_000, 1_380);
  assert.equal(out.startedAtMs, 1_000);
  assert.equal(out.finishedAtMs, 1_380);
  assert.equal(out.durationMs, 380);
});

test("buildPipelineJobDiagnostics handles failed/invalid timing bounds", () => {
  const out = buildPipelineJobDiagnostics(2_000, 1_500);
  assert.equal(out.startedAtMs, 2_000);
  assert.equal(out.finishedAtMs, 1_500);
  assert.equal(out.durationMs, null);
});

test("buildPipelineJobDiagnostics handles busy/no-run states", () => {
  const out = buildPipelineJobDiagnostics(null, null);
  assert.equal(out.startedAtMs, null);
  assert.equal(out.finishedAtMs, null);
  assert.equal(out.durationMs, null);
});

test("listScalpDurationTimelineRuns returns empty list when PG is not configured", async () => {
  const pgEnvKeys = [
    "SCALP_PG_CONNECTION_STRING",
    "NEON__DATABASE_URL",
    "NEON__POSTGRES_PRISMA_URL",
    "NEON__POSTGRES_URL",
    "DATABASE_URL",
    "POSTGRES_PRISMA_URL",
    "POSTGRES_URL",
    "PRISMA_CONNECTION_STRING",
    "PRISMA_PG_POSTGRES_URL",
  ] as const;
  const originalValues: Partial<Record<(typeof pgEnvKeys)[number], string>> =
    {};
  for (const key of pgEnvKeys) {
    const value = process.env[key];
    if (value !== undefined) originalValues[key] = value;
    delete process.env[key];
  }
  try {
    const runs = await listScalpDurationTimelineRuns({
      source: "all",
      jobKind: "all",
      limit: 50,
    });
    assert.deepEqual(runs, []);
  } finally {
    for (const key of pgEnvKeys) {
      const value = originalValues[key];
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
  }
});
