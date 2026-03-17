import assert from "node:assert/strict";
import test from "node:test";

import { getScalpVenueFeeSchedule } from "../fees";

test("bitget venue fee schedule uses fixed 0.06% taker fee for USDT-M", () => {
  const fee = getScalpVenueFeeSchedule("bitget");
  assert.equal(fee.model, "fixed_taker_pct");
  assert.equal(fee.takerFeeRate, 0.0006);
  assert.equal(fee.feeCurrency, "USDT");
});

test("unknown venue fee schedule falls back to capital defaults", () => {
  const fee = getScalpVenueFeeSchedule("unknown_venue");
  assert.equal(fee.model, "embedded_spread_or_broker");
  assert.equal(fee.takerFeeRate, null);
  assert.equal(fee.feeCurrency, "USD");
});
