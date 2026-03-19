import assert from "node:assert/strict";
import test from "node:test";

import {
  applyScalpStrategyConfigOverride,
  getScalpStrategyConfig,
} from "../config";
import { executeScalpEntryPlan } from "../execution";
import { createInitialScalpSessionState } from "../stateMachine";

test("executeScalpEntryPlan maps broker throw to entry rejection reason codes", async () => {
  const nowMs = Date.UTC(2026, 2, 18, 9, 0, 0, 0);
  const state = createInitialScalpSessionState({
    symbol: "XANUSDT",
    dayKey: "2026-03-18",
    nowMs,
    killSwitchActive: false,
  });
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    execution: {
      liveEnabled: true,
    },
  });

  const out = await executeScalpEntryPlan({
    state,
    plan: {
      setupId: "test-xan-entry",
      dealReference: "test-xan-ref",
      side: "BUY",
      orderType: "MARKET",
      limitLevel: null,
      entryReferencePrice: 1.23,
      stopPrice: 1.2,
      takeProfitPrice: 1.29,
      riskAbs: 0.03,
      riskUsd: 10,
      notionalUsd: 100,
      leverage: 20,
    },
    cfg,
    dryRun: false,
    nowMs,
    adapter: {
      broker: {
        async executeScalpEntry() {
          throw new Error("Bitget error 40762: The order amount exceeds the balance");
        },
      },
    } as any,
  });

  assert.equal(out.state.trade, null);
  assert.equal(out.state.state, "IDLE");
  assert.ok(out.reasonCodes.includes("ENTRY_NOT_PLACED"));
  assert.ok(out.reasonCodes.includes("ENTRY_EXECUTION_ERROR"));
  assert.ok(out.reasonCodes.includes("ENTRY_REJECT_BITGET_40762"));
  assert.ok(out.reasonCodes.includes("ENTRY_REJECT_INSUFFICIENT_BALANCE"));
});

test("executeScalpEntryPlan forwards riskUsd to broker entry adapter", async () => {
  const nowMs = Date.UTC(2026, 2, 18, 9, 5, 0, 0);
  const state = createInitialScalpSessionState({
    symbol: "XANUSDT",
    dayKey: "2026-03-18",
    nowMs,
    killSwitchActive: false,
  });
  const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), {
    execution: {
      liveEnabled: true,
    },
  });

  let capturedRiskUsd: number | null = null;
  const out = await executeScalpEntryPlan({
    state,
    plan: {
      setupId: "test-xan-entry-risk-forward",
      dealReference: "test-xan-ref-risk-forward",
      side: "BUY",
      orderType: "MARKET",
      limitLevel: null,
      entryReferencePrice: 1.23,
      stopPrice: 1.2,
      takeProfitPrice: 1.29,
      riskAbs: 0.03,
      riskUsd: 250,
      notionalUsd: 1_000,
      leverage: 4,
    },
    cfg,
    dryRun: false,
    nowMs,
    adapter: {
      broker: {
        async executeScalpEntry(params: any) {
          capturedRiskUsd =
            Number.isFinite(Number(params?.riskUsd)) && Number(params.riskUsd) > 0
              ? Number(params.riskUsd)
              : null;
          return {
            placed: true,
            dryRun: false,
            orderId: "bitget-order-1",
            dealId: "XANUSDT:long",
            dealReference: String(params?.clientOid || "test-deal-ref"),
            clientOid: String(params?.clientOid || "test-client-oid"),
            symbol: "XANUSDT",
            direction: "BUY",
            notionalUsd: Number(params?.notionalUsd || 0),
            leverage: Number(params?.leverage || 1),
            orderType: "MARKET",
            size: 100,
            epic: "XANUSDT",
            dealStatus: "ACCEPTED",
            confirmStatus: "SUBMITTED",
            rejectReason: null,
          };
        },
      },
    } as any,
  });

  assert.equal(capturedRiskUsd, 250);
  assert.equal(out.state.state, "IN_TRADE");
  assert.ok(out.reasonCodes.includes("ENTRY_PLACED"));
});
