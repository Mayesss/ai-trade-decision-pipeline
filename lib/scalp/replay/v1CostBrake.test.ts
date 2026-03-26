import assert from "node:assert/strict";
import test from "node:test";

import {
  maybeRespondScalpV1ResearchPaused,
  resolveScalpV1ResearchHardCaps,
} from "../v1CostBrake";

type MockRes = {
  statusCode: number;
  body: unknown;
  status: (code: number) => MockRes;
  json: (payload: unknown) => MockRes;
};

function createRes(): MockRes {
  const res: MockRes = {
    statusCode: 200,
    body: null,
    status(code: number) {
      this.statusCode = code;
      return this;
    },
    json(payload: unknown) {
      this.body = payload;
      return this;
    },
  };
  return res;
}

test("v1 cost brake pauses by default", { concurrency: false }, () => {
  const oldPaused = process.env.SCALP_V1_RESEARCH_PAUSED;
  const oldForce = process.env.SCALP_V1_RESEARCH_ALLOW_FORCE_RUN;
  try {
    delete process.env.SCALP_V1_RESEARCH_PAUSED;
    delete process.env.SCALP_V1_RESEARCH_ALLOW_FORCE_RUN;
    const req = { query: {}, url: "/api/scalp/cron/worker" } as any;
    const res = createRes();
    const handled = maybeRespondScalpV1ResearchPaused({
      req,
      res: res as any,
      routeId: "worker",
    });
    assert.equal(handled, true);
    assert.equal(res.statusCode, 200);
    assert.equal((res.body as any)?.paused, true);
  } finally {
    if (oldPaused === undefined) delete process.env.SCALP_V1_RESEARCH_PAUSED;
    else process.env.SCALP_V1_RESEARCH_PAUSED = oldPaused;
    if (oldForce === undefined) delete process.env.SCALP_V1_RESEARCH_ALLOW_FORCE_RUN;
    else process.env.SCALP_V1_RESEARCH_ALLOW_FORCE_RUN = oldForce;
  }
});

test("forceRun query bypasses pause only when force is explicitly allowed", { concurrency: false }, () => {
  const oldPaused = process.env.SCALP_V1_RESEARCH_PAUSED;
  const oldForce = process.env.SCALP_V1_RESEARCH_ALLOW_FORCE_RUN;
  try {
    process.env.SCALP_V1_RESEARCH_PAUSED = "true";
    process.env.SCALP_V1_RESEARCH_ALLOW_FORCE_RUN = "true";
    const req = { query: { forceRun: "1" }, url: "/api/scalp/cron/promotion" } as any;
    const res = createRes();
    const handled = maybeRespondScalpV1ResearchPaused({
      req,
      res: res as any,
      routeId: "promotion",
    });
    assert.equal(handled, false);
    assert.equal(res.body, null);
  } finally {
    if (oldPaused === undefined) delete process.env.SCALP_V1_RESEARCH_PAUSED;
    else process.env.SCALP_V1_RESEARCH_PAUSED = oldPaused;
    if (oldForce === undefined) delete process.env.SCALP_V1_RESEARCH_ALLOW_FORCE_RUN;
    else process.env.SCALP_V1_RESEARCH_ALLOW_FORCE_RUN = oldForce;
  }
});

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
