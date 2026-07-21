import assert from 'node:assert/strict';
import test from 'node:test';

import { resolveRiskBasedSizing } from './riskSizing';

test('1% of equity at a 2% stop: notional = risk / stop distance, margin = notional / leverage', () => {
    const out = resolveRiskBasedSizing({
        entryPrice: 100,
        stopPrice: 98,
        equityUsd: 1000,
        leverage: 5,
        riskEquityPct: 1,
    });
    assert.ok(out);
    assert.equal(out.riskUsd, 10);
    assert.equal(out.stopDistancePct, 0.02);
    assert.equal(out.notionalUsd, 500);
    assert.equal(out.marginUsd, 100);
    assert.equal(out.source, 'equity_pct');
});

test('short entries size identically (stop above entry)', () => {
    const out = resolveRiskBasedSizing({
        entryPrice: 100,
        stopPrice: 102,
        equityUsd: 1000,
        leverage: 10,
        riskEquityPct: 1,
    });
    assert.ok(out);
    assert.equal(out.notionalUsd, 500);
    assert.equal(out.marginUsd, 50);
});

test('no equity reading falls back to the fixed risk amount', () => {
    const out = resolveRiskBasedSizing({
        entryPrice: 100,
        stopPrice: 99,
        equityUsd: null,
        leverage: null,
        fallbackRiskUsd: 5,
    });
    assert.ok(out);
    assert.equal(out.riskUsd, 5);
    assert.equal(out.notionalUsd, 500);
    assert.equal(out.marginUsd, 500); // leverage null → 1
    assert.equal(out.source, 'fallback_fixed');
});

test('tight stops cannot blow exposure past 2x equity', () => {
    const out = resolveRiskBasedSizing({
        entryPrice: 100,
        stopPrice: 99.9, // 0.1% stop → uncapped notional would be 10,000
        equityUsd: 1000,
        leverage: 5,
        riskEquityPct: 1,
    });
    assert.ok(out);
    assert.equal(out.notionalUsd, 2000);
    assert.equal(out.marginUsd, 400);
});

test('invalid entry/stop or a stop on top of entry returns null (caller keeps legacy sizing)', () => {
    assert.equal(resolveRiskBasedSizing({ entryPrice: NaN, stopPrice: 98, equityUsd: 1000, leverage: 5 }), null);
    assert.equal(resolveRiskBasedSizing({ entryPrice: 100, stopPrice: 0, equityUsd: 1000, leverage: 5 }), null);
    assert.equal(
        resolveRiskBasedSizing({ entryPrice: 100, stopPrice: 100.0001, equityUsd: 1000, leverage: 5 }),
        null,
    );
});
