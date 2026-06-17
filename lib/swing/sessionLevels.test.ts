import assert from 'node:assert/strict';
import test from 'node:test';

import { buildForexSessionLevelsContext } from './sessionLevels';

function candle(iso: string, open: number, high: number, low: number, close: number) {
  return [Date.parse(iso), open, high, low, close, 100];
}

test('buildForexSessionLevelsContext detects prior day low sweep and reclaim', () => {
  const candles = [
    candle('2026-06-16T00:00:00Z', 160.0, 160.2, 159.9, 160.1),
    candle('2026-06-16T01:00:00Z', 160.1, 160.3, 159.8, 160.2),
    candle('2026-06-16T07:00:00Z', 160.2, 160.4, 160.0, 160.3),
    candle('2026-06-16T13:00:00Z', 160.3, 160.5, 160.1, 160.4),
    candle('2026-06-16T20:00:00Z', 160.4, 160.45, 160.2, 160.3),
    candle('2026-06-17T00:00:00Z', 160.3, 160.35, 159.75, 159.85),
    candle('2026-06-17T01:00:00Z', 159.85, 160.15, 159.82, 160.05),
    candle('2026-06-17T02:00:00Z', 160.05, 160.25, 160.0, 160.12),
    candle('2026-06-17T03:00:00Z', 160.12, 160.2, 160.02, 160.08),
    candle('2026-06-17T04:00:00Z', 160.08, 160.18, 160.01, 160.1),
    candle('2026-06-17T05:00:00Z', 160.1, 160.22, 160.05, 160.18),
    candle('2026-06-17T06:00:00Z', 160.18, 160.24, 160.1, 160.2),
  ];

  const context = buildForexSessionLevelsContext({
    symbol: 'USDJPY',
    candles,
    nowMs: Date.parse('2026-06-17T06:30:00Z'),
  });

  assert.ok(context);
  assert.equal(context.priorDay?.sweptLow, true);
  assert.equal(context.priorDay?.reclaimedLow, true);
  assert.equal(context.signals.bullishLiquidityReclaim, true);
  assert.equal(context.signals.sweptPriorDayLow, true);
});

test('buildForexSessionLevelsContext detects current session sweep of last completed session high', () => {
  const candles = [
    candle('2026-06-17T00:00:00Z', 160.0, 160.1, 159.9, 160.0),
    candle('2026-06-17T01:00:00Z', 160.0, 160.12, 159.92, 160.02),
    candle('2026-06-17T02:00:00Z', 160.02, 160.15, 159.94, 160.08),
    candle('2026-06-17T03:00:00Z', 160.08, 160.18, 160.0, 160.1),
    candle('2026-06-17T04:00:00Z', 160.1, 160.2, 160.05, 160.12),
    candle('2026-06-17T05:00:00Z', 160.12, 160.22, 160.08, 160.18),
    candle('2026-06-17T06:00:00Z', 160.18, 160.25, 160.1, 160.2),
    candle('2026-06-17T07:00:00Z', 160.2, 160.35, 160.05, 160.18),
    candle('2026-06-17T08:00:00Z', 160.18, 160.28, 160.08, 160.16),
    candle('2026-06-17T09:00:00Z', 160.16, 160.24, 160.1, 160.14),
    candle('2026-06-17T10:00:00Z', 160.14, 160.22, 160.06, 160.12),
    candle('2026-06-17T11:00:00Z', 160.12, 160.2, 160.02, 160.1),
  ];

  const context = buildForexSessionLevelsContext({
    symbol: 'USDJPY',
    candles,
    nowMs: Date.parse('2026-06-17T11:30:00Z'),
  });

  assert.ok(context);
  assert.equal(context.currentSession?.name, 'london');
  assert.equal(context.lastCompletedSession?.name, 'asia');
  assert.equal(context.currentSession?.sweptLastSessionHigh, true);
  assert.equal(context.currentSession?.reclaimedLastSessionHigh, true);
  assert.equal(context.signals.bearishLiquidityRejection, true);
});
