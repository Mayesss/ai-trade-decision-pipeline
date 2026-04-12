import assert from "node:assert/strict";
import test from "node:test";

import { runScalpExecuteCycle } from "../engine";
import { getDefaultScalpStrategy } from "../strategies/registry";

test("runScalpExecuteCycle uses injected persistence adapter", async () => {
  const strategyId = getDefaultScalpStrategy().id;
  const calls = {
    loadRuntimeSnapshot: 0,
    appendJournal: 0,
    tryAcquireRunLock: 0,
  };

  const result = await runScalpExecuteCycle({
    symbol: "EURUSD",
    dryRun: true,
    persistence: {
      loadRuntimeSnapshot: async () => {
        calls.loadRuntimeSnapshot += 1;
        return {
          defaultStrategyId: strategyId,
          strategyId,
          strategy: {
            strategyId,
            shortName: strategyId,
            longName: strategyId,
            enabled: false,
            envEnabled: true,
            kvEnabled: false,
            updatedAtMs: Date.now(),
            updatedBy: "test",
          },
          strategies: [
            {
              strategyId,
              shortName: strategyId,
              longName: strategyId,
              enabled: false,
              envEnabled: true,
              kvEnabled: false,
              updatedAtMs: Date.now(),
              updatedBy: "test",
            },
          ],
        };
      },
      loadSessionState: async () => null,
      saveSessionState: async () => undefined,
      appendJournal: async () => {
        calls.appendJournal += 1;
      },
      appendTradeLedgerEntry: async () => undefined,
      tryAcquireRunLock: async () => {
        calls.tryAcquireRunLock += 1;
        return true;
      },
      releaseRunLock: async () => undefined,
    },
  });

  assert.equal(calls.loadRuntimeSnapshot, 1);
  assert.equal(calls.appendJournal, 1);
  assert.equal(calls.tryAcquireRunLock, 0);
  assert.equal(result.runLockAcquired, false);
  assert.equal(
    result.reasonCodes.includes("SCALP_STRATEGY_DISABLED_BY_KV"),
    true,
  );
});
