import assert from "node:assert/strict";
import test from "node:test";

import { resolveScalpV1ResearchHardCaps } from "../v1CostBrake";

test("hard caps resolve from env with sane clamps", { concurrency: false }, () => {
  const oldCandidateCap = process.env.SCALP_V1_RESEARCH_MAX_CANDIDATES_CAP;
  const oldHopCap = process.env.SCALP_V1_RESEARCH_MAX_SELF_HOPS_CAP;
  try {
    process.env.SCALP_V1_RESEARCH_MAX_CANDIDATES_CAP = "55";
    process.env.SCALP_V1_RESEARCH_MAX_SELF_HOPS_CAP = "2";
    const caps = resolveScalpV1ResearchHardCaps();
    assert.equal(caps.maxCandidates, 55);
    assert.equal(caps.maxSelfHops, 2);
  } finally {
    if (oldCandidateCap === undefined) delete process.env.SCALP_V1_RESEARCH_MAX_CANDIDATES_CAP;
    else process.env.SCALP_V1_RESEARCH_MAX_CANDIDATES_CAP = oldCandidateCap;
    if (oldHopCap === undefined) delete process.env.SCALP_V1_RESEARCH_MAX_SELF_HOPS_CAP;
    else process.env.SCALP_V1_RESEARCH_MAX_SELF_HOPS_CAP = oldHopCap;
  }
});
