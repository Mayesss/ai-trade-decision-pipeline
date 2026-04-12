import assert from "node:assert/strict";
import test from "node:test";

import canonicalizeDeploymentsHandler from "../../../pages/api/scalp/cron/canonicalize-deployments";
import discoverSymbolsHandler from "../../../pages/api/scalp/cron/discover-symbols";
import executeDeploymentsHandler from "../../../pages/api/scalp/cron/execute-deployments";
import fullResetHandler from "../../../pages/api/scalp/cron/full-reset";
import housekeepingHandler from "../../../pages/api/scalp/cron/housekeeping";
import liveGuardrailMonitorHandler from "../../../pages/api/scalp/cron/live-guardrail-monitor";
import loadCandlesHandler from "../../../pages/api/scalp/cron/load-candles";
import prepareHandler from "../../../pages/api/scalp/cron/prepare";
import promotionHandler from "../../../pages/api/scalp/cron/promotion";
import workerHandler from "../../../pages/api/scalp/cron/worker";
import dashboardSummaryHandler from "../../../pages/api/scalp/dashboard/summary";
import deploymentsRegistryHandler from "../../../pages/api/scalp/deployments/registry";
import durationsHandler from "../../../pages/api/scalp/ops/durations";
import panicStopHandler from "../../../pages/api/scalp/ops/panic-stop";
import pipelineStateV2Handler from "../../../pages/api/scalp/ops/pipeline-state-v2";
import promotionDiagnosticsHandler from "../../../pages/api/scalp/ops/promotion-diagnostics";
import researchUniverseHandler from "../../../pages/api/scalp/research/universe";
import strategyControlHandler from "../../../pages/api/scalp/strategy/control";

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

test("all legacy scalp v1 routes return 410 after v2 cutover", async () => {
  const routes: Array<{
    path: string;
    handler: (req: any, res: any) => Promise<any>;
  }> = [
    {
      path: "/api/scalp/cron/canonicalize-deployments",
      handler: canonicalizeDeploymentsHandler,
    },
    { path: "/api/scalp/cron/discover-symbols", handler: discoverSymbolsHandler },
    {
      path: "/api/scalp/cron/execute-deployments",
      handler: executeDeploymentsHandler,
    },
    { path: "/api/scalp/cron/full-reset", handler: fullResetHandler },
    { path: "/api/scalp/cron/housekeeping", handler: housekeepingHandler },
    {
      path: "/api/scalp/cron/live-guardrail-monitor",
      handler: liveGuardrailMonitorHandler,
    },
    { path: "/api/scalp/cron/load-candles", handler: loadCandlesHandler },
    { path: "/api/scalp/cron/prepare", handler: prepareHandler },
    { path: "/api/scalp/cron/promotion", handler: promotionHandler },
    { path: "/api/scalp/cron/worker", handler: workerHandler },
    { path: "/api/scalp/ops/durations", handler: durationsHandler },
    { path: "/api/scalp/ops/panic-stop", handler: panicStopHandler },
    { path: "/api/scalp/ops/pipeline-state-v2", handler: pipelineStateV2Handler },
    {
      path: "/api/scalp/ops/promotion-diagnostics",
      handler: promotionDiagnosticsHandler,
    },
    { path: "/api/scalp/strategy/control", handler: strategyControlHandler },
    { path: "/api/scalp/dashboard/summary", handler: dashboardSummaryHandler },
    {
      path: "/api/scalp/deployments/registry",
      handler: deploymentsRegistryHandler,
    },
    { path: "/api/scalp/research/universe", handler: researchUniverseHandler },
  ];

  for (const route of routes) {
    const req = createReq(route.path);
    const res = createRes();
    await route.handler(req as any, res as any);
    const body = asRecord(res.body);
    assert.equal(res.statusCode, 410, `expected 410 for ${route.path}`);
    assert.equal(body.error, "scalp_legacy_retired");
    assert.equal(body.migrationPath, "/api/scalp/v2/*");
  }
});
