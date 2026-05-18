import assert from "node:assert/strict";
import test from "node:test";

import type { ScalpReplayTrade } from "../scalp/replay/types";
import {
  buildScalpV5CellEvidence,
  resolveScalpV5EntryBlock,
  SCALP_V5_VERSION,
  tagTradesWithCells,
} from "./index";

const CELL_A = "vol=mid|trend=trending_up|risk=risk_on";
const CELL_B = "vol=high|trend=choppy|risk=risk_off";

const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const WEEK_MS = 7 * ONE_DAY_MS;

// 2026-04-06 is a Monday. Use it as a stable anchor for the test windows.
const ANCHOR = Date.UTC(2026, 3, 6);

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
      [CELL_A]: { trades: 20, netR: 4, expectancyR: 0.2, wins: 12, losses: 8 },
      [CELL_B]: { trades: 15, netR: -3, expectancyR: -0.2, wins: 5, losses: 10 },
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

test("resolveScalpV5EntryBlock blocks when current cell is unprofitable (hard gate)", () => {
  const evidence = {
    version: SCALP_V5_VERSION,
    classifierVersion: "scalp_v4_macro_weekly_r1",
    evaluatedAtMs: ANCHOR,
    holdoutFromMs: ANCHOR - 12 * WEEK_MS,
    holdoutToMs: ANCHOR,
    minTradesPerCell: 8,
    cells: {
      [CELL_B]: { trades: 15, netR: -3, expectancyR: -0.2, wins: 5, losses: 10 },
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
      [CELL_A]: { trades: 20, netR: 4, expectancyR: 0.2, wins: 12, losses: 8 },
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
