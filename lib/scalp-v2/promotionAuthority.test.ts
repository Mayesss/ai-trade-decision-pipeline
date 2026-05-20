import assert from "node:assert/strict";
import test from "node:test";

import {
  isScalpV2PromoteEnabled,
  isScalpV5OwnedPromotionGate,
  resolveScalpV2ExecuteDryRunForDeployment,
  runScalpV2PromoteJob,
  shouldRunScalpV2ExecuteCycleForDeployment,
} from "./pipeline";

function withEnv<T>(key: string, value: string | undefined, fn: () => T): T {
  const previous = process.env[key];
  if (value === undefined) delete process.env[key];
  else process.env[key] = value;
  try {
    return fn();
  } finally {
    if (previous === undefined) delete process.env[key];
    else process.env[key] = previous;
  }
}

async function withEnvAsync<T>(
  key: string,
  value: string | undefined,
  fn: () => Promise<T>,
): Promise<T> {
  const previous = process.env[key];
  if (value === undefined) delete process.env[key];
  else process.env[key] = value;
  try {
    return await fn();
  } finally {
    if (previous === undefined) delete process.env[key];
    else process.env[key] = previous;
  }
}

test("SCALP_V2_PROMOTE_ENABLED defaults to false", () => {
  withEnv("SCALP_V2_PROMOTE_ENABLED", undefined, () => {
    assert.equal(isScalpV2PromoteEnabled(), false);
  });
});

test("SCALP_V2_PROMOTE_ENABLED=true restores the legacy promote path", () => {
  withEnv("SCALP_V2_PROMOTE_ENABLED", "true", () => {
    assert.equal(isScalpV2PromoteEnabled(), true);
  });
});

test("runScalpV2PromoteJob skips before taking the v2 promote lock by default", async () => {
  await withEnvAsync("SCALP_V2_PROMOTE_ENABLED", undefined, async () => {
    const job = await runScalpV2PromoteJob();
    assert.equal(job.ok, true);
    assert.equal(job.busy, false);
    assert.equal(job.processed, 0);
    assert.equal(job.details.reason, "scalp_v2_promote_disabled");
  });
});

test("isScalpV5OwnedPromotionGate detects v5-owned promotion markers", () => {
  assert.equal(isScalpV5OwnedPromotionGate({ source: "v5_cell_evidence" }), true);
  assert.equal(isScalpV5OwnedPromotionGate({ v5Promotion: { promotedAtMs: 1 } }), true);
  assert.equal(isScalpV5OwnedPromotionGate({ source: "v2_forward_evidence" }), false);
  assert.equal(isScalpV5OwnedPromotionGate(null), false);
});

test("v5-owned live deployments bypass SCALP_V2_LIVE_ENABLED in execute dry-run resolution", () => {
  assert.equal(
    resolveScalpV2ExecuteDryRunForDeployment({
      effectiveDryRun: false,
      runtimeLiveEnabled: false,
      deploymentLiveMode: "live",
      v5Owned: true,
    }),
    false,
  );
  assert.equal(
    resolveScalpV2ExecuteDryRunForDeployment({
      effectiveDryRun: false,
      runtimeLiveEnabled: false,
      deploymentLiveMode: "live",
      v5Owned: false,
    }),
    true,
  );
  assert.equal(
    resolveScalpV2ExecuteDryRunForDeployment({
      effectiveDryRun: true,
      runtimeLiveEnabled: false,
      deploymentLiveMode: "live",
      v5Owned: true,
    }),
    true,
  );
  assert.equal(
    resolveScalpV2ExecuteDryRunForDeployment({
      effectiveDryRun: false,
      runtimeLiveEnabled: false,
      deploymentLiveMode: "shadow",
      v5Owned: true,
    }),
    true,
  );
});

test("execute cycle skips entry-blocked flat deployments but still manages open positions", () => {
  assert.equal(
    shouldRunScalpV2ExecuteCycleForDeployment({
      entryBlocked: false,
      hasOpenPosition: false,
    }),
    true,
  );
  assert.equal(
    shouldRunScalpV2ExecuteCycleForDeployment({
      entryBlocked: true,
      hasOpenPosition: false,
    }),
    false,
  );
  assert.equal(
    shouldRunScalpV2ExecuteCycleForDeployment({
      entryBlocked: true,
      hasOpenPosition: true,
    }),
    true,
  );
});
