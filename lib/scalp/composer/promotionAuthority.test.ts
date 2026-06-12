import assert from "node:assert/strict";
import test from "node:test";

import {
  isScalpComposerPromoteEnabled,
  isScalpResearchOwnedPromotionGate,
  resolveScalpComposerExecuteDryRunForDeployment,
  runScalpComposerPromoteJob,
  shouldRunScalpComposerExecuteCycleForDeployment,
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

test("SCALP_COMPOSER_PROMOTE_ENABLED defaults to false", () => {
  withEnv("SCALP_COMPOSER_PROMOTE_ENABLED", undefined, () => {
    assert.equal(isScalpComposerPromoteEnabled(), false);
  });
});

test("SCALP_COMPOSER_PROMOTE_ENABLED=true restores the legacy promote path", () => {
  withEnv("SCALP_COMPOSER_PROMOTE_ENABLED", "true", () => {
    assert.equal(isScalpComposerPromoteEnabled(), true);
  });
});

test("runScalpComposerPromoteJob skips before taking the v2 promote lock by default", async () => {
  await withEnvAsync("SCALP_COMPOSER_PROMOTE_ENABLED", undefined, async () => {
    const job = await runScalpComposerPromoteJob();
    assert.equal(job.ok, true);
    assert.equal(job.busy, false);
    assert.equal(job.processed, 0);
    assert.equal(job.details.reason, "scalp_v2_promote_disabled");
  });
});

test("isScalpResearchOwnedPromotionGate detects v5-owned promotion markers", () => {
  assert.equal(isScalpResearchOwnedPromotionGate({ source: "v5_cell_evidence" }), true);
  assert.equal(isScalpResearchOwnedPromotionGate({ v5Promotion: { promotedAtMs: 1 } }), true);
  assert.equal(isScalpResearchOwnedPromotionGate({ source: "v2_forward_evidence" }), false);
  assert.equal(isScalpResearchOwnedPromotionGate(null), false);
});

test("v5-owned live deployments bypass SCALP_COMPOSER_LIVE_ENABLED in execute dry-run resolution", () => {
  assert.equal(
    resolveScalpComposerExecuteDryRunForDeployment({
      effectiveDryRun: false,
      runtimeLiveEnabled: false,
      deploymentLiveMode: "live",
      v5Owned: true,
    }),
    false,
  );
  assert.equal(
    resolveScalpComposerExecuteDryRunForDeployment({
      effectiveDryRun: false,
      runtimeLiveEnabled: false,
      deploymentLiveMode: "live",
      v5Owned: false,
    }),
    true,
  );
  assert.equal(
    resolveScalpComposerExecuteDryRunForDeployment({
      effectiveDryRun: true,
      runtimeLiveEnabled: false,
      deploymentLiveMode: "live",
      v5Owned: true,
    }),
    true,
  );
  assert.equal(
    resolveScalpComposerExecuteDryRunForDeployment({
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
    shouldRunScalpComposerExecuteCycleForDeployment({
      entryBlocked: false,
      hasOpenPosition: false,
    }),
    true,
  );
  assert.equal(
    shouldRunScalpComposerExecuteCycleForDeployment({
      entryBlocked: true,
      hasOpenPosition: false,
    }),
    false,
  );
  assert.equal(
    shouldRunScalpComposerExecuteCycleForDeployment({
      entryBlocked: true,
      hasOpenPosition: true,
    }),
    true,
  );
});
