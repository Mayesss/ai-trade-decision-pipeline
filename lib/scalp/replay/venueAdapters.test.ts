import assert from "node:assert/strict";
import test from "node:test";

import {
  getScalpVenueAdapter,
  isScalpVenueAdapterSupported,
  supportedScalpVenues,
} from "../adapters";

test("bitget scalp adapter is registered in venue registry", () => {
  assert.equal(isScalpVenueAdapterSupported("bitget"), true);
  assert.ok(supportedScalpVenues().includes("bitget"));
  const adapter = getScalpVenueAdapter("bitget");
  assert.equal(adapter.venue, "bitget");
});

test("bitget scalp adapter exposes fixed 0.06% taker fee metadata", () => {
  const adapter = getScalpVenueAdapter("bitget");
  assert.equal(adapter.fees.model, "fixed_taker_pct");
  assert.equal(adapter.fees.takerFeeRate, 0.0006);
  assert.equal(adapter.fees.feeCurrency, "USDT");
});
