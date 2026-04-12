import assert from "node:assert/strict";
import test from "node:test";

import strategyCatalogHandler from "../../../pages/api/scalp/v2/ops/strategy-catalog";

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
  const headers: Record<string, string> = {};
  const adminSecret = String(process.env.ADMIN_ACCESS_SECRET || "").trim();
  if (adminSecret) headers["x-admin-access-secret"] = adminSecret;
  return {
    method: "GET",
    url: pathname,
    headers,
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

test("v2 strategy catalog returns strategy options for backtest selector", async () => {
  const req = createReq("/api/scalp/v2/ops/strategy-catalog");
  const res = createRes();
  await strategyCatalogHandler(req as any, res as any);
  const body = asRecord(res.body);
  assert.equal(res.statusCode, 200);
  assert.equal(body.ok, true);
  assert.equal(body.mode, "scalp_v2");
  assert.equal(typeof body.defaultStrategyId, "string");
  const strategies = Array.isArray(body.strategies) ? body.strategies : [];
  assert.equal(strategies.length > 0, true);
});
