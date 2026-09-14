import assert from 'node:assert/strict';
import { test } from 'vitest';

import { resolveRealizedRiskUsd, summarizeRStats } from '../../../lib/swing/rStats';

import type { SwingClosedRiskRow } from '../../../lib/swing/pg';

// A row measured the preferred way: the fill's own notional against the stop
// the placing decision shipped. entry 100 / stop 98 / notional 30 → $0.60 risk.
const row = (pnlNet: number | null, over: Partial<SwingClosedRiskRow> = {}): SwingClosedRiskRow => ({
    platform: 'bitget',
    symbol: 'BTCUSDT',
    exitTsMs: 1,
    pnlNet,
    budgetRiskUsd: 10,
    effectiveRiskUsd: null,
    notionalUsd: 30,
    entryPrice: 100,
    stopPrice: 98,
    ...over,
});

const unmeasurable = (pnlNet: number): SwingClosedRiskRow =>
    row(pnlNet, { notionalUsd: null, entryPrice: null, stopPrice: null, effectiveRiskUsd: null });

test('R divides by the risk actually taken, not the budget', () => {
    // The budget says $10; the fill only ever risked $0.60. A -$0.60 loss is a
    // full -1R, not the -0.06R the budget would have reported.
    const s = summarizeRStats([row(-0.6), row(1.8), row(-0.66)], 200);
    assert.equal(s.measured, 3);
    assert.equal(s.sources.position_stop, 3);
    assert.equal(s.avgR, 0.3);
    assert.equal(s.sumR, 0.9); // -1 + 3 - 1.1
    assert.equal(s.minR, -1.1);
    assert.equal(s.avgWinR, 3);
    assert.equal(s.avgLossR, -1.05);
    assert.equal(s.payoff, 2.86);
    assert.deepEqual(s.curve, [-1, 2, 0.9]);
});

test('a clean stop-out reads as -1R, which is what makes the -1.3R slippage tripwire fire', () => {
    const clean = summarizeRStats([row(-0.6)], 200);
    assert.equal(clean.avgR, -1);
    // Same trade, but the stop slipped to a -$0.84 fill.
    const slipped = summarizeRStats([row(-0.84)], 200);
    assert.equal(slipped.minR, -1.4);
});

test('rows with no resolvable denominator are closed but NOT measured (never budget-divided)', () => {
    const s = summarizeRStats([row(-0.6), unmeasurable(2.0), row(null)], 200);
    assert.equal(s.closed, 3);
    assert.equal(s.measured, 1); // the unmeasurable +2.0 must not reach the mean
    assert.equal(s.avgR, -1);
});

test('falls back to the recorded effective risk when the fill lacks a stop', () => {
    const s = summarizeRStats([row(-0.5, { stopPrice: null, effectiveRiskUsd: 0.5 })], 200);
    assert.equal(s.measured, 1);
    assert.equal(s.sources.effective, 1);
    assert.equal(s.sources.position_stop, 0);
    assert.equal(s.avgR, -1);
});

test('empty and wholly unmeasurable windows produce nulls, never NaN', () => {
    const empty = summarizeRStats([], 200);
    assert.equal(empty.closed, 0);
    assert.equal(empty.avgR, null);
    assert.equal(empty.payoff, null);
    assert.deepEqual(empty.curve, []);
    const legacy = summarizeRStats([unmeasurable(1), unmeasurable(-1)], 200);
    assert.equal(legacy.closed, 2);
    assert.equal(legacy.measured, 0);
    assert.equal(legacy.winRate, null);
});

test('all losers: payoff is null, avgR is the mean loss', () => {
    const s = summarizeRStats([row(-0.6), row(-0.72)], 50);
    assert.equal(s.payoff, null);
    assert.equal(s.avgR, -1.1);
    assert.equal(s.winRate, 0);
});

test('resolveRealizedRiskUsd rejects degenerate and implausible stop distances', () => {
    // Stop sitting on top of entry: below MIN_SIZEABLE_STOP_PCT.
    assert.equal(resolveRealizedRiskUsd(row(-1, { stopPrice: 100.001 })), null);
    // Stop further than 100% of entry: a unit/inversion error, not a wide stop.
    assert.equal(resolveRealizedRiskUsd(row(-1, { stopPrice: 250 })), null);
    // ...and a degenerate stop still yields to a recorded effective risk.
    assert.deepEqual(resolveRealizedRiskUsd(row(-1, { stopPrice: 100.001, effectiveRiskUsd: 2 })), {
        riskUsd: 2,
        source: 'effective',
    });
});

test('short entries measure identically (stop above entry)', () => {
    const s = summarizeRStats([row(-0.6, { entryPrice: 100, stopPrice: 102 })], 200);
    assert.equal(s.avgR, -1);
});
