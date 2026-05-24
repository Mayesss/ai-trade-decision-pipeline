import assert from "node:assert/strict";
import test from "node:test";

import type { ScalpReplayTrade } from "../scalp/replay/types";
import {
  buildScalpV5CellEvidence,
  evaluateScalpV5PromotionEvidence,
  resolveScalpV5EntryBlock,
  resolveScalpV5EvidenceFreshness,
  SCALP_V5_VERSION,
  tagTradesWithCells,
} from "./index";
import {
  isScalpV5RemovedBitgetSymbolError,
  resolveScalpV5PreflightWeek,
  summarizeScalpV5CandleCoverage,
} from "./candlePreflight";
import { shouldRunScalpV5EvaluationCandlePreflight } from "./evaluator";
import { rankScalpV5StageCRefillCandidates, type ScalpV5StageCRefillCandidate } from "./pg";

const CELL_A = "vol=mid|trend=trending_up|risk=risk_on";
const CELL_B = "vol=high|trend=choppy|risk=risk_off";

const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const WEEK_MS = 7 * ONE_DAY_MS;

// 2026-04-06 is a Monday. Use it as a stable anchor for the test windows.
const ANCHOR = Date.UTC(2026, 3, 6);

function cellStat(params: {
  trades: number;
  netR: number;
  expectancyR: number;
  wins: number;
  losses: number;
}) {
  return {
    ...params,
    weeklyNetR: new Array(12).fill(0),
    weeklyTrades: new Array(12).fill(0),
    weeklyWins: new Array(12).fill(0),
    weeklyLosses: new Array(12).fill(0),
  };
}

function tradeAt(weeksFromAnchor: number, rMultiple: number): ScalpReplayTrade {
  const entryTs = ANCHOR + weeksFromAnchor * WEEK_MS + 12 * 60 * 60 * 1000; // mid-week
  return {
    id: `t-${weeksFromAnchor}-${rMultiple}`,
    dayKey: new Date(entryTs).toISOString().slice(0, 10),
    side: "BUY",
    entryTs,
    exitTs: entryTs + 60_000,
    holdMinutes: 1,
    entryPrice: 100,
    stopPrice: 99,
    takeProfitPrice: 102,
    exitPrice: 100 + rMultiple,
    exitReason: rMultiple >= 0 ? "TP" : "STOP",
    riskAbs: 1,
    riskUsd: 10,
    notionalUsd: 1000,
    rMultiple,
    pnlUsd: rMultiple * 10,
  };
}

test("tagTradesWithCells maps each trade to its week-start cell", () => {
  const snapshots = new Map([
    [ANCHOR, CELL_A as any],
    [ANCHOR + WEEK_MS, CELL_B as any],
  ]);
  const tagged = tagTradesWithCells({
    trades: [tradeAt(0, 0.5), tradeAt(1, -0.2), tradeAt(2, 0.1)],
    snapshotsByWeekStart: snapshots,
  });
  assert.equal(tagged[0]!.cellId, CELL_A);
  assert.equal(tagged[1]!.cellId, CELL_B);
  // Week 2 anchor has no snapshot → null
  assert.equal(tagged[2]!.cellId, null);
});

test("buildScalpV5CellEvidence aggregates per cell and marks eligible cells", () => {
  const tagged = tagTradesWithCells({
    trades: [
      // CELL_A: 3 trades, netR +1.5, expectancy 0.5
      tradeAt(0, 0.5),
      tradeAt(0, 0.4),
      tradeAt(0, 0.6),
      // CELL_B: 4 trades, netR -1.0, expectancy -0.25
      tradeAt(1, -0.3),
      tradeAt(1, -0.4),
      tradeAt(1, -0.2),
      tradeAt(1, -0.1),
    ],
    snapshotsByWeekStart: new Map([
      [ANCHOR, CELL_A as any],
      [ANCHOR + WEEK_MS, CELL_B as any],
    ]),
  });
  const evidence = buildScalpV5CellEvidence({
    tagged,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR + 12 * WEEK_MS,
    holdoutFromMs: ANCHOR,
    holdoutToMs: ANCHOR + 12 * WEEK_MS,
    minTradesPerCell: 3,
  });
  assert.equal(evidence.version, SCALP_V5_VERSION);
  assert.equal(evidence.cells[CELL_A]?.trades, 3);
  assert.equal(evidence.cells[CELL_A]?.netR, 1.5);
  assert.equal(evidence.cells[CELL_A]?.expectancyR.toFixed(3), "0.500");
  assert.equal(evidence.cells[CELL_B]?.trades, 4);
  assert.equal(evidence.cells[CELL_B]?.expectancyR.toFixed(3), "-0.250");
  // CELL_A meets minTrades=3 AND expectancy>0 → eligible.
  // CELL_B has trades >= 3 but expectancy < 0 → not eligible.
  assert.deepEqual(evidence.eligibleCells, [CELL_A]);
});

test("buildScalpV5CellEvidence populates per-cell weeklyNetR aligned to the holdout window", () => {
  // 12-week holdout window starting at ANCHOR. Cell A has trades in weeks 0
  // and 2 only; cell B has a trade in week 1 only.
  const tagged = tagTradesWithCells({
    trades: [
      tradeAt(0, 0.5),
      tradeAt(0, 0.3),
      tradeAt(2, -0.2),
      tradeAt(1, 0.8),
    ],
    snapshotsByWeekStart: new Map([
      [ANCHOR, CELL_A as any],
      [ANCHOR + WEEK_MS, CELL_B as any],
      [ANCHOR + 2 * WEEK_MS, CELL_A as any],
    ]),
  });
  const evidence = buildScalpV5CellEvidence({
    tagged,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR + 12 * WEEK_MS,
    holdoutFromMs: ANCHOR,
    holdoutToMs: ANCHOR + 12 * WEEK_MS,
    minTradesPerCell: 1,
  });
  const aWeekly = evidence.cells[CELL_A]?.weeklyNetR ?? [];
  const bWeekly = evidence.cells[CELL_B]?.weeklyNetR ?? [];
  assert.equal(aWeekly.length, 12);
  assert.equal(bWeekly.length, 12);
  assert.equal(aWeekly[0]?.toFixed(3), "0.800"); // 0.5 + 0.3
  assert.equal(aWeekly[1], 0);
  assert.equal(aWeekly[2]?.toFixed(3), "-0.200");
  assert.equal(bWeekly[0], 0);
  assert.equal(bWeekly[1]?.toFixed(3), "0.800");
  assert.equal(bWeekly[2], 0);
});

test("buildScalpV5CellEvidence respects the minTradesPerCell threshold", () => {
  const tagged = tagTradesWithCells({
    trades: [
      tradeAt(0, 0.5),
      tradeAt(0, 0.5),
      // CELL_B only has 1 trade; should not be eligible even though profitable.
      tradeAt(1, 1.0),
    ],
    snapshotsByWeekStart: new Map([
      [ANCHOR, CELL_A as any],
      [ANCHOR + WEEK_MS, CELL_B as any],
    ]),
  });
  const evidence = buildScalpV5CellEvidence({
    tagged,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR + 12 * WEEK_MS,
    holdoutFromMs: ANCHOR,
    holdoutToMs: ANCHOR + 12 * WEEK_MS,
    minTradesPerCell: 3,
  });
  // Neither cell meets minTrades=3 → no eligible cells.
  assert.deepEqual(evidence.eligibleCells, []);
});

test("resolveScalpV5EntryBlock allows when current cell is profitable", () => {
  const evidence = {
    version: SCALP_V5_VERSION,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR,
    holdoutFromMs: ANCHOR - 12 * WEEK_MS,
    holdoutToMs: ANCHOR,
    minTradesPerCell: 8,
    cells: {
      [CELL_A]: cellStat({ trades: 20, netR: 4, expectancyR: 0.2, wins: 12, losses: 8 }),
      [CELL_B]: cellStat({ trades: 15, netR: -3, expectancyR: -0.2, wins: 5, losses: 10 }),
    },
    eligibleCells: [CELL_A],
  };
  const gate = resolveScalpV5EntryBlock({
    enabled: true,
    hardGate: true,
    evidence,
    currentCellId: CELL_A as any,
    stale: false,
    minTradesPerCell: 8,
  });
  assert.equal(gate.blocked, false);
  assert.equal(gate.shadowOnly, false);
  assert.equal(gate.matchedCellId, CELL_A);
  assert.equal(gate.evidence?.trades, 20);
  assert.deepEqual(gate.reasonCodes, []);
});

test("resolveScalpV5EntryBlock allows positive thin cell for consistency exception rows", () => {
  const evidence = {
    version: SCALP_V5_VERSION,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR,
    holdoutFromMs: ANCHOR - 12 * WEEK_MS,
    holdoutToMs: ANCHOR,
    minTradesPerCell: 8,
    cells: {
      [CELL_A]: cellStat({ trades: 3, netR: 1.5, expectancyR: 0.5, wins: 2, losses: 1 }),
    },
    eligibleCells: [],
  };
  const withoutException = resolveScalpV5EntryBlock({
    enabled: true,
    hardGate: true,
    evidence,
    currentCellId: CELL_A as any,
    stale: false,
    minTradesPerCell: 8,
  });
  assert.equal(withoutException.blocked, true);
  assert.ok(withoutException.reasonCodes.includes("V5_CELL_INSUFFICIENT_TRADES"));

  const withException = resolveScalpV5EntryBlock({
    enabled: true,
    hardGate: true,
    evidence,
    currentCellId: CELL_A as any,
    stale: false,
    minTradesPerCell: 8,
    allowThinPositiveCell: true,
    minThinCellTrades: 3,
  });
  assert.equal(withException.blocked, false);
  assert.deepEqual(withException.reasonCodes, []);
});

test("resolveScalpV5EntryBlock blocks when current cell is unprofitable (hard gate)", () => {
  const evidence = {
    version: SCALP_V5_VERSION,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR,
    holdoutFromMs: ANCHOR - 12 * WEEK_MS,
    holdoutToMs: ANCHOR,
    minTradesPerCell: 8,
    cells: {
      [CELL_B]: cellStat({ trades: 15, netR: -3, expectancyR: -0.2, wins: 5, losses: 10 }),
    },
    eligibleCells: [],
  };
  const gate = resolveScalpV5EntryBlock({
    enabled: true,
    hardGate: true,
    evidence,
    currentCellId: CELL_B as any,
    stale: false,
    minTradesPerCell: 8,
  });
  assert.equal(gate.blocked, true);
  assert.ok(gate.reasonCodes.includes("V5_CELL_NEGATIVE_EXPECTANCY"));
});

test("resolveScalpV5EntryBlock soft-blocks (shadow) when hard gate is off", () => {
  const evidence = {
    version: SCALP_V5_VERSION,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR,
    holdoutFromMs: ANCHOR - 12 * WEEK_MS,
    holdoutToMs: ANCHOR,
    minTradesPerCell: 8,
    cells: {},
    eligibleCells: [],
  };
  const gate = resolveScalpV5EntryBlock({
    enabled: true,
    hardGate: false,
    evidence,
    currentCellId: CELL_A as any,
    stale: false,
    minTradesPerCell: 8,
  });
  assert.equal(gate.blocked, false);
  assert.equal(gate.shadowOnly, true);
  assert.ok(gate.reasonCodes.includes("V5_CELL_NOT_IN_EVIDENCE_SHADOW"));
});

test("resolveScalpV5EntryBlock does not block when evidence is missing (evaluator hasn't run)", () => {
  const gate = resolveScalpV5EntryBlock({
    enabled: true,
    hardGate: true,
    evidence: null,
    currentCellId: CELL_A as any,
    stale: false,
    minTradesPerCell: 8,
  });
  assert.equal(gate.blocked, false);
  assert.equal(gate.shadowOnly, false);
  assert.deepEqual(gate.reasonCodes, ["V5_CELL_EVIDENCE_MISSING"]);
});

test("resolveScalpV5EntryBlock blocks when regime data is stale", () => {
  const evidence = {
    version: SCALP_V5_VERSION,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR,
    holdoutFromMs: ANCHOR - 12 * WEEK_MS,
    holdoutToMs: ANCHOR,
    minTradesPerCell: 8,
    cells: {
      [CELL_A]: cellStat({ trades: 20, netR: 4, expectancyR: 0.2, wins: 12, losses: 8 }),
    },
    eligibleCells: [CELL_A],
  };
  const gate = resolveScalpV5EntryBlock({
    enabled: true,
    hardGate: true,
    evidence,
    currentCellId: null,
    stale: true,
    minTradesPerCell: 8,
  });
  assert.equal(gate.blocked, true);
  assert.ok(gate.reasonCodes.includes("V5_CELL_DATA_STALE"));
});

test("resolveScalpV5EntryBlock returns blocked=false when v5 is disabled", () => {
  const gate = resolveScalpV5EntryBlock({
    enabled: false,
    hardGate: true,
    evidence: null,
    currentCellId: CELL_A as any,
    stale: false,
    minTradesPerCell: 8,
  });
  assert.equal(gate.blocked, false);
  assert.equal(gate.shadowOnly, false);
  assert.deepEqual(gate.reasonCodes, []);
});

function promotionEvidence(params: {
  weekly: number[];
  trades?: number;
}) {
  const trades = params.trades ?? Math.max(1, params.weekly.length * 5);
  const netR = params.weekly.reduce((acc, value) => acc + value, 0);
  return {
    version: SCALP_V5_VERSION,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR,
    holdoutFromMs: ANCHOR - 12 * WEEK_MS,
    holdoutToMs: ANCHOR,
    minTradesPerCell: 8,
    cells: {
      [CELL_A]: {
        trades,
        netR,
        expectancyR: netR / trades,
        wins: Math.max(0, Math.floor(trades / 2)),
        losses: Math.max(0, Math.floor(trades / 3)),
        weeklyNetR: params.weekly,
        weeklyTrades: new Array(params.weekly.length).fill(Math.floor(trades / params.weekly.length)),
        weeklyWins: new Array(params.weekly.length).fill(1),
        weeklyLosses: new Array(params.weekly.length).fill(0),
      },
    },
    eligibleCells: [CELL_A],
  };
}

const PROMOTION_THRESHOLDS = {
  minTotalNetR: 4,
  minTotalTrades: 60,
  minPositiveWeeks: 8,
  minWorstWeekR: 3,
  minTrailing4wNetR: 4,
};

test("evaluateScalpV5PromotionEvidence qualifies v5 evidence without v2 promotion_gate", () => {
  const evaluation = evaluateScalpV5PromotionEvidence({
    evidence: promotionEvidence({
      trades: 72,
      weekly: [0.5, 0.4, -0.2, 0.7, 0.6, -0.5, 0.4, 0.5, 1.2, 1.1, 1.0, 0.9],
    }),
    thresholds: PROMOTION_THRESHOLDS,
  });
  assert.equal(evaluation.qualified, true);
  assert.equal(evaluation.reason, "v5_strict_passed");
  assert.equal(evaluation.metrics.positiveWeeks, 10);
  assert.equal(evaluation.metrics.trailing4wNetR.toFixed(1), "4.2");
});

test("evaluateScalpV5PromotionEvidence allows exceptional low-sample consistency", () => {
  const evaluation = evaluateScalpV5PromotionEvidence({
    evidence: {
      ...promotionEvidence({
        trades: 29,
        weekly: [1.4, 2.8, 0.9, 1.8, 0.9, 0.9, 2.7, 2.5, 0.9, 1.9, 0.9, 0],
      }),
      cells: {
        [CELL_A]: {
          trades: 29,
          netR: 17.6,
          expectancyR: 17.6 / 29,
          wins: 26,
          losses: 3,
          weeklyNetR: [1.4, 2.8, 0.9, 1.8, 0.9, 0.9, 2.7, 2.5, 0.9, 1.9, 0.9, 0],
          weeklyTrades: new Array(12).fill(2),
          weeklyWins: new Array(12).fill(1),
          weeklyLosses: new Array(12).fill(0),
        },
        [CELL_B]: {
          trades: 3,
          netR: 1.5,
          expectancyR: 0.5,
          wins: 2,
          losses: 1,
          weeklyNetR: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.5],
          weeklyTrades: new Array(11).fill(0).concat(3),
          weeklyWins: new Array(11).fill(0).concat(2),
          weeklyLosses: new Array(11).fill(0).concat(1),
        },
      },
      eligibleCells: [CELL_A, CELL_B],
    },
    thresholds: PROMOTION_THRESHOLDS,
  });
  assert.equal(evaluation.qualified, true);
  assert.equal(evaluation.reason, "v5_consistency_exception_passed");
  assert.equal(evaluation.metrics.totalTrades, 32);
  assert.equal(evaluation.metrics.activeCells, 2);
});

test("evaluateScalpV5PromotionEvidence reports the binding v5 threshold failure", () => {
  assert.equal(
    evaluateScalpV5PromotionEvidence({
      evidence: promotionEvidence({ trades: 72, weekly: [0.1, 0.1, 0.1, 0.1] }),
      thresholds: PROMOTION_THRESHOLDS,
    }).reason,
    "v5_total_net_r_below_threshold",
  );
  assert.equal(
    evaluateScalpV5PromotionEvidence({
      evidence: promotionEvidence({
        trades: 20,
        weekly: [0.5, 0.4, -0.2, 0.7, 0.6, -0.5, 0.4, 0.5, 1.2, 1.1, 1.0, 0.9],
      }),
      thresholds: PROMOTION_THRESHOLDS,
    }).reason,
    "v5_total_trades_below_threshold",
  );
  assert.equal(
    evaluateScalpV5PromotionEvidence({
      evidence: promotionEvidence({
        trades: 72,
        weekly: [2, -0.1, 2, -0.1, 2, -0.1, 2, -0.1, 2, -0.1, 2, -0.1],
      }),
      thresholds: PROMOTION_THRESHOLDS,
    }).reason,
    "v5_positive_weeks_below_threshold",
  );
  assert.equal(
    evaluateScalpV5PromotionEvidence({
      evidence: promotionEvidence({
        trades: 72,
        weekly: [2, 1, 1, 1, 1, 1, 1, 1, 1, -4, 1, 1],
      }),
      thresholds: PROMOTION_THRESHOLDS,
    }).reason,
    "v5_worst_week_below_threshold",
  );
  assert.equal(
    evaluateScalpV5PromotionEvidence({
      evidence: promotionEvidence({
        trades: 72,
        weekly: [1, 1, -0.2, 1, 1, -0.2, 1, 1, 0.2, 0.2, 0.2, 0.2],
      }),
      thresholds: PROMOTION_THRESHOLDS,
    }).reason,
    "v5_trailing_4w_net_r_below_threshold",
  );
});

test("resolveScalpV5EvidenceFreshness marks stale and fresh evidence", () => {
  const nowMs = ANCHOR + 20 * ONE_DAY_MS;
  const staleOlderThanMs = 14 * ONE_DAY_MS;
  assert.equal(
    resolveScalpV5EvidenceFreshness({
      evaluatedAtMs: nowMs - 15 * ONE_DAY_MS,
      nowMs,
      staleOlderThanMs,
    }).stale,
    true,
  );
  assert.equal(
    resolveScalpV5EvidenceFreshness({
      evaluatedAtMs: nowMs - 2 * ONE_DAY_MS,
      nowMs,
      staleOlderThanMs,
    }).stale,
    false,
  );
  assert.equal(
    resolveScalpV5EvidenceFreshness({ evaluatedAtMs: null, nowMs, staleOlderThanMs }).stale,
    true,
  );
});

test("resolveScalpV5PreflightWeek targets the just-completed week on Sunday", () => {
  const sunday = Date.UTC(2026, 4, 24, 7, 30, 0);
  const out = resolveScalpV5PreflightWeek({ nowMs: sunday, holdoutWeeks: 12 });
  assert.equal(out.targetWeekStartMs, Date.UTC(2026, 4, 18));
  assert.equal(out.targetWeekEndMs, Date.UTC(2026, 4, 25));
  assert.equal(out.holdoutFromMs, Date.UTC(2026, 2, 2));
  assert.equal(out.holdoutToMs, Date.UTC(2026, 4, 25));
});

test("summarizeScalpV5CandleCoverage flags missing and partial weekly buckets", () => {
  const failures = summarizeScalpV5CandleCoverage({
    scopes: [
      { venue: "bitget", symbol: "BTCUSDT" },
      { venue: "capital", symbol: "EURUSD" },
      { venue: "bitget", symbol: "SOLUSDT" },
    ],
    coverageRows: [
      { symbol: "BTCUSDT", candleCount: 7_999, firstTsMs: 1, lastTsMs: 2 },
      { symbol: "EURUSD", candleCount: 6_000, firstTsMs: 1, lastTsMs: 2 },
    ],
    minCandles: { bitget: 8_000, capital: 6_000 },
  });
  assert.deepEqual(
    failures.map((row) => ({ symbol: row.symbol, reason: row.reason, candles: row.candles })),
    [
      { symbol: "BTCUSDT", reason: "insufficient_week_candles", candles: 7_999 },
      { symbol: "SOLUSDT", reason: "missing_week_bucket", candles: 0 },
    ],
  );
});

test("removed Bitget symbols are classified from provider errors", () => {
  assert.equal(
    isScalpV5RemovedBitgetSymbolError(
      "bitget_history_request_failed:MKRUSDT: Bitget error 40309: The symbol has been removed",
    ),
    true,
  );
  assert.equal(isScalpV5RemovedBitgetSymbolError("capital timeout"), false);
});

test("v5 evaluation candle preflight runs only on Sunday unless forced", () => {
  assert.equal(
    shouldRunScalpV5EvaluationCandlePreflight({
      nowMs: Date.UTC(2026, 4, 24, 7, 0, 0),
    }),
    true,
  );
  assert.equal(
    shouldRunScalpV5EvaluationCandlePreflight({
      nowMs: Date.UTC(2026, 4, 25, 7, 0, 0),
    }),
    false,
  );
  assert.equal(
    shouldRunScalpV5EvaluationCandlePreflight({
      nowMs: Date.UTC(2026, 4, 25, 7, 0, 0),
      forcePreflight: true,
    }),
    true,
  );
  assert.equal(
    shouldRunScalpV5EvaluationCandlePreflight({
      nowMs: Date.UTC(2026, 4, 24, 7, 0, 0),
      preflightCandles: false,
    }),
    false,
  );
});

function makeRefillCandidate(
  id: number,
  overrides: Partial<ScalpV5StageCRefillCandidate> = {},
): ScalpV5StageCRefillCandidate {
  const base: ScalpV5StageCRefillCandidate = {
    id,
    venue: "bitget",
    symbol: `T${id}USDT`,
    strategyId: "model_guided_composer_v2",
    tuneId: `tune_${id}`,
    entrySessionProfile: "tokyo",
    score: 10,
    status: "evaluated",
    metadata: {},
    stageCNetR: 8,
    stageCTrades: 50,
    deploymentId: `bitget:T${id}USDT~model_guided_composer_v2~tune_${id}__sp_tokyo`,
    inRuntimeScope: true,
  };
  return { ...base, ...overrides };
}

test("rankScalpV5StageCRefillCandidates filters inactive quality candidates and orders by edge", () => {
  const selected = rankScalpV5StageCRefillCandidates({
    targetNewSeats: 3,
    minStageCNetR: 4,
    minStageCTrades: 30,
    candidates: [
      makeRefillCandidate(1, { stageCNetR: 12, stageCTrades: 35, score: 9 }),
      makeRefillCandidate(2, { stageCNetR: 20, stageCTrades: 31, score: 1 }),
      makeRefillCandidate(3, { stageCNetR: 20, stageCTrades: 60, score: 1 }),
      makeRefillCandidate(4, { stageCNetR: 30, stageCTrades: 80, alreadyActive: true }),
      makeRefillCandidate(5, { stageCNetR: 30, stageCTrades: 80, scopeRemoved: true }),
      makeRefillCandidate(6, { stageCNetR: 30, stageCTrades: 80, inRuntimeScope: false }),
      makeRefillCandidate(7, { stageCNetR: 3.9, stageCTrades: 80 }),
      makeRefillCandidate(8, { stageCNetR: 30, stageCTrades: 29 }),
      makeRefillCandidate(9, { stageCNetR: 30, stageCTrades: 80, status: "discovered" }),
    ],
  });
  assert.deepEqual(selected.map((row) => row.id), [3, 2, 1]);
});
