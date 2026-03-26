import assert from "node:assert/strict";
import test from "node:test";

import handler from "../../../pages/api/scalp/ops/promotion-diagnostics";

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

test("promotion diagnostics route does not require admin auth header", async () => {
  const originalSecret = process.env.ADMIN_ACCESS_SECRET;
  try {
    process.env.ADMIN_ACCESS_SECRET = "secret_test_value";
    const req = createReq("/api/scalp/ops/promotion-diagnostics", {
      scope: "actionable",
      session: "berlin",
    });
    const res = createRes();
    await handler(req as any, res as any);
    assert.notEqual(res.statusCode, 401);
    assert.notEqual(asRecord(res.body).error, "Unauthorized");
  } finally {
    if (originalSecret === undefined) delete process.env.ADMIN_ACCESS_SECRET;
    else process.env.ADMIN_ACCESS_SECRET = originalSecret;
  }
});

test("promotion diagnostics route validates scope and session filters", async () => {
  const invalidScopeReq = createReq("/api/scalp/ops/promotion-diagnostics", {
    scope: "bad",
    session: "berlin",
  });
  const invalidScopeRes = createRes();
  await handler(invalidScopeReq as any, invalidScopeRes as any);
  assert.equal(invalidScopeRes.statusCode, 400);
  assert.equal(asRecord(invalidScopeRes.body).error, "invalid_scope");

  const invalidSessionReq = createReq("/api/scalp/ops/promotion-diagnostics", {
    scope: "actionable",
    session: "invalid",
  });
  const invalidSessionRes = createRes();
  await handler(invalidSessionReq as any, invalidSessionRes as any);
  assert.equal(invalidSessionRes.statusCode, 400);
  assert.equal(asRecord(invalidSessionRes.body).error, "invalid_session");
});

test("promotion diagnostics route only accepts GET", async () => {
  const req = createReq(
    "/api/scalp/ops/promotion-diagnostics",
    {},
    { method: "POST" },
  );
  const res = createRes();
  await handler(req as any, res as any);
  assert.equal(res.statusCode, 405);
});
