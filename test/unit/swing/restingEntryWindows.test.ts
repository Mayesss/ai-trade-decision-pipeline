import assert from 'node:assert/strict';
import { test } from 'vitest';

import { buildRestingEntryWindows } from '../../../lib/swing/restingEntryWindows';

const MIN = 60_000;
const NOW = Date.UTC(2026, 8, 2, 18, 0, 0);

const decision = (minutesAgo: number, aiDecision: Record<string, unknown>) => ({
  timestamp: NOW - minutesAgo * MIN,
  dryRun: false,
  aiDecision,
});

test('a resting entry that filled hours later spans until the fill', () => {
  // SELL limit at 100.3 issued 20h ago, re-issued 15min later, then nothing but
  // HOLDs every 15min — a HOLD leaves the order resting — until it filled 13h ago.
  const holds = Array.from({ length: 24 }, (_, i) => decision(20 * 60 - 30 - i * 15, { action: 'HOLD' }));
  const windows = buildRestingEntryWindows({
    nowMs: NOW,
    history: [
      decision(20 * 60, { action: 'SELL', entry_limit_price: 100.3 }),
      decision(20 * 60 - 15, { action: 'SELL', entry_limit_price: 100.3 }),
      ...holds,
    ],
    positions: [{ side: 'short', entryTime: (NOW - 13 * 60 * MIN) / 1000, entryPrice: 100.31 }],
    pendingOrders: [],
  });
  assert.equal(windows.length, 1);
  assert.equal(windows[0].side, 'sell');
  assert.equal(windows[0].price, 100.3);
  assert.equal(windows[0].fromTime, (NOW - 20 * 60 * MIN) / 1000);
  assert.equal(windows[0].toTime, (NOW - 13 * 60 * MIN) / 1000);
  assert.equal(windows[0].filled, true);
});

test('an unfilled resting entry ends at the decision that superseded it, not the next tick', () => {
  const windows = buildRestingEntryWindows({
    nowMs: NOW,
    history: [
      decision(300, { action: 'BUY', entry_limit_price: 98 }),
      decision(285, { action: 'HOLD' }),
      decision(270, { action: 'HOLD' }),
      decision(255, { action: 'BUY', entry_limit_price: 97 }),
    ],
    positions: [],
    pendingOrders: [],
  });
  assert.equal(windows.length, 2);
  assert.equal(windows[0].price, 98);
  assert.equal(windows[0].toTime, (NOW - 255 * MIN) / 1000);
  assert.equal(windows[0].filled, false);
  // The supersede opens the next chain, which rests on through the HOLD-free tail.
  assert.equal(windows[1].price, 97);
  assert.equal(windows[1].toTime, NOW / 1000);
});

test('withdraw_resting_entry on a flat HOLD ends the window', () => {
  const windows = buildRestingEntryWindows({
    nowMs: NOW,
    history: [
      decision(120, { action: 'SELL', entry_stop_price: 105 }),
      decision(105, { action: 'HOLD' }),
      decision(90, { action: 'HOLD', withdraw_resting_entry: true }),
      decision(75, { action: 'HOLD' }),
    ],
    positions: [],
    pendingOrders: [],
  });
  assert.equal(windows.length, 1);
  // A stop entry rests exactly like a limit and draws the same way.
  assert.equal(windows[0].price, 105);
  assert.equal(windows[0].toTime, (NOW - 90 * MIN) / 1000);
  assert.equal(windows[0].filled, false);
});

test('an entry on the other side, or far from the resting price, is not the fill', () => {
  const history = [decision(120, { action: 'BUY', entry_limit_price: 100 }), decision(60, { action: 'HOLD' })];
  const wrongSide = buildRestingEntryWindows({
    nowMs: NOW,
    history,
    positions: [{ side: 'short', entryTime: (NOW - 90 * MIN) / 1000, entryPrice: 100 }],
    pendingOrders: [],
  });
  assert.equal(wrongSide[0].filled, false);
  // Not its fill — but nothing rests through an open position, so the window
  // still ends where that position opened.
  assert.equal(wrongSide[0].toTime, (NOW - 90 * MIN) / 1000);
  const farPrice = buildRestingEntryWindows({
    nowMs: NOW,
    history,
    positions: [{ side: 'long', entryTime: (NOW - 90 * MIN) / 1000, entryPrice: 108 }],
    pendingOrders: [],
  });
  assert.equal(farPrice[0].filled, false);
});

test('a live resting order extends its chain to now', () => {
  const windows = buildRestingEntryWindows({
    nowMs: NOW,
    history: [
      decision(200, { action: 'BUY', entry_limit_price: 95.5 }),
      decision(185, { action: 'HOLD' }),
      decision(170, { action: 'BUY', entry_limit_price: 94 }),
    ],
    positions: [],
    pendingOrders: [{ side: 'buy', price: 94, createdAtMs: NOW - 170 * MIN }],
  });
  assert.equal(windows.length, 2);
  assert.equal(windows[1].price, 94);
  assert.equal(windows[1].toTime, NOW / 1000);
  assert.equal(windows[1].filled, false);
});

test('a resting entry with no cancel is still capped by the age backstop', () => {
  const windows = buildRestingEntryWindows({
    nowMs: NOW,
    history: [decision(72 * 60, { action: 'SELL', entry_limit_price: 120 })],
    positions: [],
    pendingOrders: [],
  });
  assert.equal(windows.length, 1);
  assert.equal(windows[0].toTime, (NOW - 24 * 60 * MIN) / 1000);
});

test('a gate withdrawal in the tick log ends the window there and marks it withdrawn (2026-09-16 EURUSD)', () => {
  // SELL issued 50h ago, withdrawn by a session window 4h later, no decision
  // row ever recorded the cancel. Without the tick it would draw to the 48h
  // backstop (2h ago); with it, it ends at the withdrawal.
  const withdrawnAt = NOW - 46 * 60 * MIN;
  const windows = buildRestingEntryWindows({
    nowMs: NOW,
    history: [decision(50 * 60, { action: 'SELL', entry_limit_price: 1.1577 })],
    positions: [],
    pendingOrders: [],
    withdrawalsMs: [withdrawnAt],
  });
  assert.equal(windows.length, 1);
  assert.equal(windows[0].toTime, withdrawnAt / 1000);
  assert.equal(windows[0].withdrawn, true);
  assert.equal(windows[0].filled, false);
});

test('a withdrawal before the issue or after the natural end does not clip, and a fill wins over a later withdrawal', () => {
  const issuedAt = NOW - 10 * 60 * MIN;
  const filledAt = NOW - 8 * 60 * MIN;
  const windows = buildRestingEntryWindows({
    nowMs: NOW,
    history: [decision(10 * 60, { action: 'BUY', entry_stop_price: 77750 })],
    positions: [{ side: 'long', entryTime: filledAt / 1000, entryPrice: 77760 }],
    pendingOrders: [],
    // one before the issue, one after the fill
    withdrawalsMs: [issuedAt - 60 * MIN, NOW - 6 * 60 * MIN],
  });
  assert.equal(windows.length, 1);
  assert.equal(windows[0].filled, true);
  assert.equal(windows[0].withdrawn, undefined);
  assert.equal(windows[0].toTime, filledAt / 1000);
});

test('a live resting order does not revive a withdrawn chain at the same price; it opens a fresh segment', () => {
  const withdrawnAt = NOW - 5 * 60 * MIN;
  const windows = buildRestingEntryWindows({
    nowMs: NOW,
    history: [decision(8 * 60, { action: 'SELL', entry_limit_price: 100 })],
    positions: [],
    pendingOrders: [{ side: 'sell', price: 100, createdAtMs: NOW - 60 * MIN }],
    withdrawalsMs: [withdrawnAt],
  });
  assert.equal(windows.length, 2);
  assert.equal(windows[0].toTime, withdrawnAt / 1000);
  assert.equal(windows[0].withdrawn, true);
  assert.equal(windows[1].fromTime, (NOW - 60 * MIN) / 1000);
  assert.equal(windows[1].toTime, NOW / 1000);
});
