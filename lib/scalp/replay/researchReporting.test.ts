import assert from 'node:assert/strict';
import test from 'node:test';

import {
    buildRecentMonthKeys,
    computeTradeWindowPerformance,
    pearsonCorrelation,
    type ScalpTradeWindowPerformance,
} from '../researchReporting';
import type { ScalpTradeLedgerEntry } from '../types';

function makeTrade(params: {
    id: string;
    exitAtMs: number;
    deploymentId?: string;
    rMultiple: number;
    dryRun?: boolean;
}): ScalpTradeLedgerEntry {
    return {
        id: params.id,
        timestampMs: params.exitAtMs,
        exitAtMs: params.exitAtMs,
        symbol: 'BTCUSDT',
        strategyId: 'compression_breakout_pullback_m15_m3',
        tuneId: 'default',
        deploymentId: params.deploymentId || 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
        side: 'BUY',
        dryRun: Boolean(params.dryRun),
        rMultiple: params.rMultiple,
        reasonCodes: [],
    };
}

test('computeTradeWindowPerformance summarizes netR/expectancy/win-rate/drawdown correctly', () => {
    const nowMs = Date.UTC(2026, 2, 7, 12, 0, 0, 0);
    const startMs = nowMs - 7 * 24 * 60 * 60_000;
    const trades: ScalpTradeLedgerEntry[] = [
        makeTrade({ id: 'a', exitAtMs: nowMs - 5 * 24 * 60 * 60_000, rMultiple: 1 }),
        makeTrade({ id: 'b', exitAtMs: nowMs - 4 * 24 * 60 * 60_000, rMultiple: -0.5 }),
        makeTrade({ id: 'c', exitAtMs: nowMs - 3 * 24 * 60 * 60_000, rMultiple: -1 }),
        makeTrade({ id: 'd', exitAtMs: nowMs - 2 * 24 * 60 * 60_000, rMultiple: 2 }),
        makeTrade({ id: 'e', exitAtMs: nowMs - 1 * 24 * 60 * 60_000, rMultiple: 999, dryRun: true }),
    ];

    const out: ScalpTradeWindowPerformance = computeTradeWindowPerformance(trades, startMs, nowMs);

    assert.equal(out.trades, 4);
    assert.equal(out.wins, 2);
    assert.equal(out.losses, 2);
    assert.equal(out.netR, 1.5);
    assert.equal(out.expectancyR, 0.375);
    assert.equal(out.winRatePct, 50);
    assert.equal(out.maxDrawdownR, 1.5);
    assert.equal(out.lastTradeAtMs, nowMs - 2 * 24 * 60 * 60_000);
});

test('buildRecentMonthKeys returns ascending recent month keys', () => {
    const nowMs = Date.UTC(2026, 2, 7, 12, 0, 0, 0);
    const keys = buildRecentMonthKeys(nowMs, 5);
    assert.deepEqual(keys, ['2025-11', '2025-12', '2026-01', '2026-02', '2026-03']);
});

test('pearsonCorrelation handles aligned and degenerate vectors', () => {
    assert.equal(pearsonCorrelation([1, 2, 3], [2, 4, 6]), 1);
    assert.equal(pearsonCorrelation([1, 2, 3], [6, 4, 2]), -1);
    assert.equal(pearsonCorrelation([1, 1, 1], [2, 3, 4]), null);
    assert.equal(pearsonCorrelation([1], [1]), null);
});
