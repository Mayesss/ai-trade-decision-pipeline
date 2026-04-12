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

function createReq(pathname: string): MockReq {
  return {
    method: "GET",
    url: pathname,
    headers: {},
    query: {},
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

test("legacy promotion diagnostics ops route is retired in v2 cutover", async () => {
  const req = createReq("/api/scalp/ops/promotion-diagnostics");
  const res = createRes();
  await handler(req as any, res as any);
  const body = asRecord(res.body);
  assert.equal(res.statusCode, 410);
  assert.equal(body.error, "scalp_legacy_retired");
  assert.equal(body.migrationPath, "/api/scalp/v2/*");
});
