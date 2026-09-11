import assert from 'node:assert/strict';
import { test } from 'vitest';

import { summarizeRStats } from '../../../lib/swing/rStats';

import type { SwingClosedRiskRow } from '../../../lib/swing/pg';

const row = (pnlNet: number | null, riskUsd: number | null): SwingClosedRiskRow => ({
    platform: 'bitget',
    symbol: 'BTCUSDT',
    exitTsMs: 1,
    pnlNet,
    riskUsd,
});

test('R = pnl / budget; unbudgeted rows count as closed but are not measured', () => {
    const s = summarizeRStats([row(-0.6, 0.6), row(1.8, 0.6), row(-0.66, 0.6), row(2.0, null), row(null, 0.6)], 200);
    assert.equal(s.closed, 5);
    assert.equal(s.measured, 3);
    assert.equal(s.target, 200);
    assert.equal(s.sumR, 0.9); // -1 + 3 - 1.1
    assert.equal(s.avgR, 0.3);
    assert.equal(s.winRate, 0.3333);
    assert.equal(s.avgWinR, 3);
    assert.equal(s.avgLossR, -1.05);
    assert.equal(s.payoff, 2.86);
    assert.equal(s.minR, -1.1);
    assert.deepEqual(s.curve, [-1, 2, 0.9]);
});

test('empty and all-unbudgeted windows produce nulls, never NaN', () => {
    const empty = summarizeRStats([], 200);
    assert.equal(empty.closed, 0);
    assert.equal(empty.avgR, null);
    assert.equal(empty.payoff, null);
    assert.deepEqual(empty.curve, []);
    const legacy = summarizeRStats([row(1, null), row(-1, 0)], 200);
    assert.equal(legacy.closed, 2);
    assert.equal(legacy.measured, 0);
    assert.equal(legacy.winRate, null);
});

test('all losers: payoff is null, avgR is the mean loss', () => {
    const s = summarizeRStats([row(-1, 1), row(-1.2, 1)], 50);
    assert.equal(s.payoff, null);
    assert.equal(s.avgR, -1.1);
    assert.equal(s.winRate, 0);
});
