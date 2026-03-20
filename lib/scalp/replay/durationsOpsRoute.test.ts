import assert from "node:assert/strict";
import test from "node:test";

import durationsHandler from "../../../pages/api/scalp/ops/durations";

type MockReq = {
  method: string;
  url: string;
  headers: Record<string, string>;
  query: Record<string, string | string[] | undefined>;
};

type MockRes = {
  statusCode: number;
  headers: Record<string, string>;
  body: unknown;
  setHeader: (name: string, value: string) => void;
  status: (code: number) => MockRes;
  json: (payload: unknown) => MockRes;
};

function createReq(
  pathname: string,
  query: Record<string, string>,
  opts: { method?: string; headers?: Record<string, string> } = {},
): MockReq {
  const search = new URLSearchParams(query).toString();
  return {
    method: opts.method || "GET",
    url: search ? `${pathname}?${search}` : pathname,
    headers: opts.headers || {},
    query,
  };
}

function createRes(): MockRes {
  return {
    statusCode: 200,
    headers: {},
    body: null,
    setHeader(name: string, value: string) {
      this.headers[String(name || "").toLowerCase()] = String(value || "");
    },
    status(code: number) {
      this.statusCode = Math.floor(Number(code) || 200);
      return this;
    },
    json(payload: unknown) {
      this.body = payload;
      return this;
    },
  };
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

test("scalp durations route enforces admin auth header when configured", async () => {
  const originalSecret = process.env.ADMIN_ACCESS_SECRET;
  try {
    process.env.ADMIN_ACCESS_SECRET = "secret_test_value";
    const req = createReq("/api/scalp/ops/durations", {});
    const res = createRes();
    await durationsHandler(req as any, res as any);
    assert.equal(res.statusCode, 401);
    const body = asRecord(res.body);
    assert.equal(body.error, "Unauthorized");
  } finally {
    if (originalSecret === undefined) delete process.env.ADMIN_ACCESS_SECRET;
    else process.env.ADMIN_ACCESS_SECRET = originalSecret;
  }
});

test("scalp durations route validates query filters and supports limit bounds", async () => {
  const req = createReq("/api/scalp/ops/durations", {
    source: "pipeline",
    jobKind: "prepare",
    limit: "999",
  });
  const res = createRes();
  await durationsHandler(req as any, res as any);

  assert.equal(res.statusCode, 200);
  const body = asRecord(res.body);
  assert.equal(body.ok, true);
  const filters = asRecord(body.filters);
  assert.equal(filters.source, "pipeline");
  assert.equal(filters.jobKind, "prepare");
  assert.equal(filters.limit, 500);
  assert.equal(Array.isArray(body.runs), true);
});

test("scalp durations route rejects invalid source and job kind", async () => {
  const invalidSourceReq = createReq("/api/scalp/ops/durations", {
    source: "bad_source",
  });
  const invalidSourceRes = createRes();
  await durationsHandler(invalidSourceReq as any, invalidSourceRes as any);
  assert.equal(invalidSourceRes.statusCode, 400);
  assert.equal(asRecord(invalidSourceRes.body).error, "invalid_source");

  const invalidJobKindReq = createReq("/api/scalp/ops/durations", {
    source: "all",
    jobKind: "invalid_kind",
  });
  const invalidJobKindRes = createRes();
  await durationsHandler(invalidJobKindReq as any, invalidJobKindRes as any);
  assert.equal(invalidJobKindRes.statusCode, 400);
  assert.equal(asRecord(invalidJobKindRes.body).error, "invalid_job_kind");
});

test("scalp durations route only accepts GET", async () => {
  const req = createReq(
    "/api/scalp/ops/durations",
    {},
    {
      method: "POST",
    },
  );
  const res = createRes();
  await durationsHandler(req as any, res as any);
  assert.equal(res.statusCode, 405);
});
