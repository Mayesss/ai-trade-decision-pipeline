import assert from "node:assert/strict";
import test from "node:test";

import { resolveCapitalEpic } from "./capital";

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
