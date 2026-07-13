import assert from 'node:assert/strict';
import test from 'node:test';

import {
  capitalTransactionToPositionWindow,
  deriveCapitalPnlPct,
  enrichCapitalCloseFromHistory,
  mergeCapitalCloseWindows,
} from './capitalClosedPositions';

test('enriches a Capital cash close from its placed entry and derives leveraged PnL percent', () => {
  const close = capitalTransactionToPositionWindow({
    reference: 'tlt-close',
    instrumentName: 'TLT',
    dateUtcMs: 2_000_000,
    pnlNet: -1.04,
    status: 'PROCESSED',
    transactionType: 'TRADE',
    note: 'Trade closed',
    currency: 'EUR',
    raw: {},
  }, 'TLT');
  assert.ok(close);
  const enriched = enrichCapitalCloseFromHistory(close, [{
    timestamp: 1_000_000,
    aiDecision: { action: 'SELL', leverage: 5 },
    execResult: { placed: true, notionalUsd: 100 },
    snapshot: { price: 84.2 },
  }]);
  assert.equal(enriched.entryTimestamp, 1_000_000);
  assert.equal(enriched.side, 'short');
  assert.equal(enriched.notional, 100);
  assert.equal(enriched.leverage, 5);
  assert.equal(enriched.pnlPct, -5.2);
});

test('does not fabricate percentage PnL without both notional and leverage', () => {
  assert.equal(deriveCapitalPnlPct({
    id: 'copper-close',
    symbol: 'COPPER',
    side: 'long',
    pnlNet: 0.79,
    notional: null,
    leverage: 10,
  }), null);
});

test('merges a history close with its cash transaction but preserves separate cash legs', () => {
  const rows = mergeCapitalCloseWindows([
    { id: 'history', symbol: 'COPPER', side: 'short', exitTimestamp: 1_000_000, pnlPct: 5.45, pnlNet: null },
    { id: 'cash-a', symbol: 'COPPER', side: null, exitTimestamp: 1_001_000, pnlNet: 0.58 },
    { id: 'cash-b', symbol: 'COPPER', side: null, exitTimestamp: 1_002_000, pnlNet: 0.79 },
  ]);
  assert.equal(rows.length, 2);
  assert.equal(rows[0].pnlPct, 5.45);
  assert.equal(rows[0].pnlNet, 0.58);
  assert.equal(rows[1].pnlNet, 0.79);
});
