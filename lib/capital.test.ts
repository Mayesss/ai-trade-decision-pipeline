import assert from "node:assert/strict";
import test from "node:test";

import {
  executeCapitalDecision,
  resolveCapitalEpic,
  resolveCapitalForexBaseUnitUsdFromQuotes,
  resolveCapitalScalpOrderSizing,
} from "./capital";

test("resolveCapitalEpic maps internal gold symbols to classic Capital gold CFD", () => {
  assert.deepEqual(resolveCapitalEpic("XAUUSDT"), {
    ticker: "XAUUSDT",
    epic: "GOLD",
    source: "default",
  });
  assert.deepEqual(resolveCapitalEpic("XAUUSD"), {
    ticker: "XAUUSD",
    epic: "GOLD",
    source: "default",
  });
  assert.deepEqual(resolveCapitalEpic("GOLD"), {
    ticker: "GOLD",
    epic: "GOLD",
    source: "default",
  });
});

test("resolveCapitalEpic maps internal silver symbols to classic Capital silver CFD", () => {
  assert.deepEqual(resolveCapitalEpic("XAGUSDT"), {
    ticker: "XAGUSDT",
    epic: "SILVER",
    source: "default",
  });
  assert.deepEqual(resolveCapitalEpic("XAGUSD"), {
    ticker: "XAGUSD",
    epic: "SILVER",
    source: "default",
  });
  assert.deepEqual(resolveCapitalEpic("SILVER"), {
    ticker: "SILVER",
    epic: "SILVER",
    source: "default",
  });
});

test("resolveCapitalEpic preserves existing crypto CFD mapping", () => {
  assert.deepEqual(resolveCapitalEpic("BTCUSDT"), {
    ticker: "BTCUSDT",
    epic: "BTCUSD",
    source: "default",
  });
});

test("Capital forex conversion resolves USD value for direct and inverse USD legs", () => {
  assert.deepEqual(
    resolveCapitalForexBaseUnitUsdFromQuotes({
      symbol: "EURUSD",
      pairPrice: 1.08,
      quotes: {},
    }),
    {
      baseUnitUsd: 1.08,
      reasonCodes: ["CAPITAL_FX_USD_CONVERSION"],
    },
  );
  assert.deepEqual(
    resolveCapitalForexBaseUnitUsdFromQuotes({
      symbol: "USDJPY",
      pairPrice: 156,
      quotes: {},
    }),
    {
      baseUnitUsd: 1,
      reasonCodes: ["CAPITAL_FX_USD_CONVERSION"],
    },
  );
});

test("Capital forex conversion resolves cross-pair base USD value", () => {
  assert.deepEqual(
    resolveCapitalForexBaseUnitUsdFromQuotes({
      symbol: "AUDNZD",
      pairPrice: 1.225,
      quotes: {
        AUDUSD: 0.66,
        NZDUSD: 0.61,
      },
    }),
    {
      baseUnitUsd: 0.66,
      reasonCodes: ["CAPITAL_FX_USD_CONVERSION"],
    },
  );
  const eurJpy = resolveCapitalForexBaseUnitUsdFromQuotes({
    symbol: "EURJPY",
    pairPrice: 185,
    quotes: {
      USDJPY: 156,
    },
  });
  assert.equal(eurJpy.reasonCodes.includes("CAPITAL_FX_USD_CONVERSION"), true);
  assert.ok(Math.abs((eurJpy.baseUnitUsd ?? 0) - 185 / 156) < 1e-12);
});

test("Capital forex conversion reports unavailable cross-pair conversion", () => {
  assert.deepEqual(
    resolveCapitalForexBaseUnitUsdFromQuotes({
      symbol: "EURJPY",
      pairPrice: 185,
      quotes: {},
    }),
    {
      baseUnitUsd: null,
      reasonCodes: ["CAPITAL_FX_CONVERSION_UNAVAILABLE"],
    },
  );
});

test("Capital EURJPY sizing uses base USD value instead of JPY quote price", () => {
  const sizing = resolveCapitalScalpOrderSizing({
    symbol: "EURJPY",
    assetCategory: "forex",
    requestedNotionalUsd: 500,
    referencePrice: 185,
    minDealSize: 100,
    sizeDecimals: 0,
    availableMarginUsd: 127,
    leverageCap: 30,
    forexBaseUnitUsd: 1.08,
  });

  assert.equal(sizing.accepted, true);
  assert.equal(sizing.size, 400);
  assert.equal(sizing.minNotionalUsd, 108);
  assert.equal(sizing.orderNotionalUsd, 432);
  assert.equal(sizing.rejectReason, null);
  assert.ok(sizing.reasonCodes.includes("CAPITAL_FX_USD_CONVERSION"));
  assert.ok(sizing.reasonCodes.includes("CAPITAL_MIN_SIZE_APPLIED"));
});

test("Capital forex sizing rejects only when available margin cannot cover quantized minimum", () => {
  const sizing = resolveCapitalScalpOrderSizing({
    symbol: "EURJPY",
    assetCategory: "forex",
    requestedNotionalUsd: 500,
    referencePrice: 185,
    minDealSize: 100,
    sizeDecimals: 0,
    availableMarginUsd: 2,
    leverageCap: 30,
    forexBaseUnitUsd: 1.08,
  });

  assert.equal(sizing.accepted, false);
  assert.equal(sizing.size, 100);
  assert.equal(sizing.minNotionalUsd, 108);
  assert.equal(sizing.maxNotionalUsd, 60);
  assert.equal(sizing.rejectReason, "INSUFFICIENT_AVAILABLE_MARGIN");
});

test("Capital forex sizing blocks cross pairs when USD conversion is unavailable", () => {
  const sizing = resolveCapitalScalpOrderSizing({
    symbol: "EURJPY",
    assetCategory: "forex",
    requestedNotionalUsd: 500,
    referencePrice: 185,
    minDealSize: 100,
    sizeDecimals: 0,
    availableMarginUsd: 127,
    leverageCap: 30,
    forexBaseUnitUsd: null,
  });

  assert.equal(sizing.accepted, false);
  assert.equal(sizing.rejectReason, "CAPITAL_FX_CONVERSION_UNAVAILABLE");
  assert.ok(sizing.reasonCodes.includes("CAPITAL_FX_CONVERSION_UNAVAILABLE"));
});

test("Capital partial close uses opposing reduce order instead of full close endpoint", async (t) => {
  const originalFetch = globalThis.fetch;
  const originalEnv = {
    CAPITAL_API_KEY: process.env.CAPITAL_API_KEY,
    CAPITAL_IDENTIFIER: process.env.CAPITAL_IDENTIFIER,
    CAPITAL_PASSWORD: process.env.CAPITAL_PASSWORD,
  };
  const requests: Array<{
    method: string;
    path: string;
    body: any;
  }> = [];

  process.env.CAPITAL_API_KEY = "test-key";
  process.env.CAPITAL_IDENTIFIER = "test-user";
  process.env.CAPITAL_PASSWORD = "test-pass";
  t.after(() => {
    globalThis.fetch = originalFetch;
    if (originalEnv.CAPITAL_API_KEY === undefined) {
      delete process.env.CAPITAL_API_KEY;
    } else {
      process.env.CAPITAL_API_KEY = originalEnv.CAPITAL_API_KEY;
    }
    if (originalEnv.CAPITAL_IDENTIFIER === undefined) {
      delete process.env.CAPITAL_IDENTIFIER;
    } else {
      process.env.CAPITAL_IDENTIFIER = originalEnv.CAPITAL_IDENTIFIER;
    }
    if (originalEnv.CAPITAL_PASSWORD === undefined) {
      delete process.env.CAPITAL_PASSWORD;
    } else {
      process.env.CAPITAL_PASSWORD = originalEnv.CAPITAL_PASSWORD;
    }
  });

  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    const method = String(init?.method || "GET").toUpperCase();
    const body =
      typeof init?.body === "string" && init.body
        ? JSON.parse(init.body)
        : null;
    requests.push({ method, path: url.pathname, body });

    if (method === "POST" && url.pathname === "/api/v1/session") {
      return new Response("{}", {
        status: 200,
        headers: {
          CST: "test-cst",
          "X-SECURITY-TOKEN": "test-token",
        },
      });
    }
    if (method === "GET" && url.pathname === "/api/v1/markets/PARTIALTEST") {
      return Response.json({
        market: {
          dealingRules: {
            minDealSize: { value: 1 },
          },
        },
      });
    }
    if (method === "GET" && url.pathname === "/api/v1/positions") {
      return Response.json({
        positions: [
          {
            market: { epic: "PARTIALTEST" },
            position: {
              dealId: "deal-1",
              direction: "BUY",
              size: 10,
            },
          },
        ],
      });
    }
    if (method === "POST" && url.pathname === "/api/v1/positions") {
      return Response.json({ dealReference: "partial-close-ref" });
    }
    if (method === "DELETE" && url.pathname === "/api/v1/positions/deal-1") {
      assert.fail("partial close must not call the full close endpoint");
    }

    return Response.json(
      { errorCode: `unexpected ${method} ${url.pathname}` },
      { status: 500 },
    );
  }) as typeof fetch;

  const result = await executeCapitalDecision(
    "PARTIALTEST",
    100,
    {
      action: "CLOSE",
      summary: "trim",
      reason: "test",
      exit_size_pct: 50,
    },
    false,
  );

  assert.equal(result.placed, true);
  assert.equal(result.partial, true);
  assert.equal(result.partialClosePct, 50);
  const partialClose = requests.find(
    (request) =>
      request.method === "POST" && request.path === "/api/v1/positions",
  );
  assert.ok(partialClose);
  assert.deepEqual(partialClose.body, {
    epic: "PARTIALTEST",
    direction: "SELL",
    size: 5,
    orderType: "MARKET",
    currencyCode: "USD",
    forceOpen: false,
    dealReference: result.clientOid,
  });
  assert.equal(
    requests.some(
      (request) =>
        request.method === "DELETE" &&
        request.path === "/api/v1/positions/deal-1",
    ),
    false,
  );
});

test("Capital full close uses documented dealId close endpoint", async (t) => {
  const originalFetch = globalThis.fetch;
  const originalEnv = {
    CAPITAL_API_KEY: process.env.CAPITAL_API_KEY,
    CAPITAL_IDENTIFIER: process.env.CAPITAL_IDENTIFIER,
    CAPITAL_PASSWORD: process.env.CAPITAL_PASSWORD,
  };
  const requests: Array<{
    method: string;
    path: string;
    body: any;
  }> = [];

  process.env.CAPITAL_API_KEY = "test-key";
  process.env.CAPITAL_IDENTIFIER = "test-user";
  process.env.CAPITAL_PASSWORD = "test-pass";
  t.after(() => {
    globalThis.fetch = originalFetch;
    if (originalEnv.CAPITAL_API_KEY === undefined) {
      delete process.env.CAPITAL_API_KEY;
    } else {
      process.env.CAPITAL_API_KEY = originalEnv.CAPITAL_API_KEY;
    }
    if (originalEnv.CAPITAL_IDENTIFIER === undefined) {
      delete process.env.CAPITAL_IDENTIFIER;
    } else {
      process.env.CAPITAL_IDENTIFIER = originalEnv.CAPITAL_IDENTIFIER;
    }
    if (originalEnv.CAPITAL_PASSWORD === undefined) {
      delete process.env.CAPITAL_PASSWORD;
    } else {
      process.env.CAPITAL_PASSWORD = originalEnv.CAPITAL_PASSWORD;
    }
  });

  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input));
    const method = String(init?.method || "GET").toUpperCase();
    const body =
      typeof init?.body === "string" && init.body
        ? JSON.parse(init.body)
        : null;
    requests.push({ method, path: url.pathname, body });

    if (method === "POST" && url.pathname === "/api/v1/session") {
      return new Response("{}", {
        status: 200,
        headers: {
          CST: "test-cst",
          "X-SECURITY-TOKEN": "test-token",
        },
      });
    }
    if (method === "GET" && url.pathname === "/api/v1/markets/FULLTEST") {
      return Response.json({});
    }
    if (method === "GET" && url.pathname === "/api/v1/positions") {
      return Response.json({
        positions: [
          {
            market: { epic: "FULLTEST" },
            position: {
              dealId: "deal-2",
              direction: "SELL",
              size: 10,
            },
          },
        ],
      });
    }
    if (method === "DELETE" && url.pathname === "/api/v1/positions/deal-2") {
      return Response.json({ dealReference: "full-close-ref" });
    }

    return Response.json(
      { errorCode: `unexpected ${method} ${url.pathname}` },
      { status: 500 },
    );
  }) as typeof fetch;

  const result = await executeCapitalDecision(
    "FULLTEST",
    100,
    {
      action: "CLOSE",
      summary: "exit",
      reason: "test",
    },
    false,
  );

  assert.equal(result.placed, true);
  assert.equal(result.partial, false);
  assert.equal(
    requests.some(
      (request) =>
        request.method === "DELETE" &&
        request.path === "/api/v1/positions/deal-2",
    ),
    true,
  );
  assert.equal(
    requests.some(
      (request) =>
        request.method === "DELETE" && request.path === "/api/v1/positions",
    ),
    false,
  );
});
