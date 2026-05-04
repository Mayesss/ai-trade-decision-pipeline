import assert from "node:assert/strict";
import test from "node:test";

import {
  deriveCloseTypeFromReasonCodes,
  enforceCandidateBudgets,
  enforceDeploymentBudget,
  isScalpV2SundayUtc,
  isScalpV2DiscoverSymbolAllowed,
  mapV1LedgerRowToV2,
  normalizeReasonCodes,
  toLedgerCloseTypeFromEvent,
} from "./logic";
import type { ScalpV2Candidate, ScalpV2Deployment } from "./types";

function candidate(
  id: number,
  symbol: string,
  score: number,
  updatedAtMs: number,
): ScalpV2Candidate {
  return {
    id,
    venue: "bitget",
    symbol,
    strategyId: "compression_breakout_pullback_m15_m3",
    tuneId: "default",
    entrySessionProfile: "berlin",
    score,
    status: "evaluated",
    reasonCodes: [],
    metadata: {},
    createdAtMs: updatedAtMs - 1_000,
    updatedAtMs,
  };
}

function deployment(
  deploymentId: string,
  enabled: boolean,
  liveMode: "shadow" | "live",
  updatedAtMs: number,
): ScalpV2Deployment {
  return {
    deploymentId,
    candidateId: null,
    venue: "bitget",
    symbol: "BTCUSDT",
    strategyId: "compression_breakout_pullback_m15_m3",
    tuneId: "default",
    entrySessionProfile: "berlin",
    enabled,
    liveMode,
    promotionGate: {},
    riskProfile: {
      riskPerTradePct: 0.35,
      maxOpenPositionsPerSymbol: 1,
      autoPauseDailyR: -3,
      autoPause30dR: -8,
    },
    createdAtMs: updatedAtMs - 1_000,
    updatedAtMs,
  };
}

test("enforceCandidateBudgets applies per-symbol and global caps", () => {
  const input = [
    candidate(1, "BTCUSDT", 90, 10),
    candidate(2, "BTCUSDT", 80, 20),
    candidate(3, "BTCUSDT", 70, 30),
    candidate(4, "ETHUSDT", 95, 40),
    candidate(5, "ETHUSDT", 85, 50),
  ];
  const out = enforceCandidateBudgets({
    candidates: input,
    budgets: {
      maxCandidatesTotal: 3,
      maxCandidatesPerSymbol: 2,
      maxEnabledDeployments: 12,
    },
  });

  assert.equal(out.kept.length, 3);
  assert.equal(out.dropped.length, 2);
  assert.deepEqual(
    out.kept.map((row) => row.id),
    [4, 1, 5],
  );
});

test("enforceDeploymentBudget keeps only capped enabled rows in live set", () => {
  const out = enforceDeploymentBudget({
    deployments: [
      deployment("d1", true, "live", 100),
      deployment("d2", true, "live", 90),
      deployment("d3", true, "live", 80),
      deployment("d4", false, "shadow", 70),
    ],
    budgets: {
      maxCandidatesTotal: 200,
      maxCandidatesPerSymbol: 4,
      maxEnabledDeployments: 2,
    },
  });

  assert.equal(out.live.length, 2);
  assert.equal(out.shadow.length, 2);
  assert.equal(out.shadow.some((row) => row.deploymentId === "d3"), true);
  assert.equal(
    out.shadow.find((row) => row.deploymentId === "d3")?.enabled,
    false,
  );
});

test("event and reason close mapping includes stop-loss and liquidation", () => {
  assert.equal(toLedgerCloseTypeFromEvent("stop_loss", []), "stop_loss");
  assert.equal(toLedgerCloseTypeFromEvent("liquidation", []), "liquidation");
  assert.equal(toLedgerCloseTypeFromEvent("order_submitted", []), null);
  assert.equal(
    deriveCloseTypeFromReasonCodes(["broker_liquidation", "risk"]),
    "liquidation",
  );
  assert.equal(
    deriveCloseTypeFromReasonCodes(["sl_hit", "risk"]),
    "stop_loss",
  );
});

test("normalizeReasonCodes keeps execution result codes beyond early signal context", () => {
  const codes = Array.from({ length: 20 }, (_, idx) => `CODE_${idx + 1}`);
  codes.push("ENTRY_EXECUTE_ATTEMPT", "ENTRY_PLACED");

  const out = normalizeReasonCodes(codes);

  assert.equal(out.length, 22);
  assert.equal(out.includes("ENTRY_EXECUTE_ATTEMPT"), true);
  assert.equal(out.includes("ENTRY_PLACED"), true);
});

test("mapV1LedgerRowToV2 preserves identity and derives close type", () => {
  const out = mapV1LedgerRowToV2({
    id: "abc",
    exitAtMs: 1700000000000,
    deploymentId: "bitget:BTCUSDT~x~y__sp_berlin",
    symbol: "btcusdt",
    strategyId: "X",
    tuneId: "Y",
    rMultiple: -1.5,
    reasonCodes: ["SL_HIT"],
  });
  assert.equal(out.id, "abc");
  assert.equal(out.symbol, "BTCUSDT");
  assert.equal(out.strategyId, "x");
  assert.equal(out.tuneId, "y");
  assert.equal(out.closeType, "stop_loss");
  assert.equal(out.rMultiple, -1.5);
});

test("v2 discovery enforces bitget crypto-only and capital non-crypto", () => {
  assert.equal(isScalpV2DiscoverSymbolAllowed("bitget", "BTCUSDT"), true);
  assert.equal(isScalpV2DiscoverSymbolAllowed("bitget", "XAUUSDT"), false);
  assert.equal(isScalpV2DiscoverSymbolAllowed("bitget", "AAPLUSDT"), false);

  assert.equal(isScalpV2DiscoverSymbolAllowed("capital", "EURUSD"), true);
  assert.equal(isScalpV2DiscoverSymbolAllowed("capital", "BTCUSD"), false);
  assert.equal(isScalpV2DiscoverSymbolAllowed("capital", "ETHUSD"), false);
});

test("sunday guard uses UTC day", () => {
  const sundayUtc = Date.UTC(2026, 2, 29, 9, 0, 0); // Sunday, March 29, 2026 UTC
  const mondayUtc = Date.UTC(2026, 2, 30, 9, 0, 0); // Monday, March 30, 2026 UTC
  assert.equal(isScalpV2SundayUtc(sundayUtc), true);
  assert.equal(isScalpV2SundayUtc(mondayUtc), false);
});
