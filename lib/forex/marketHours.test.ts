import assert from 'node:assert/strict';
import test from 'node:test';

import { evaluateForexMarketGate } from './marketHours';

function ts(isoUtc: string): number {
    return Date.parse(isoUtc);
}

test('market gate is closed on Saturday UTC', () => {
    const out = evaluateForexMarketGate(ts('2026-02-21T12:00:00.000Z'));
    assert.equal(out.marketClosed, true);
    assert.equal(out.reasonCode, 'MARKET_CLOSED_WEEKEND');
    assert.ok(Number.isFinite(out.reopensAtMs as number));
});

test('market gate is closed on Sunday before open hour', () => {
    const out = evaluateForexMarketGate(ts('2026-02-22T18:00:00.000Z'));
    assert.equal(out.marketClosed, true);
    assert.equal(out.reasonCode, 'MARKET_CLOSED_WEEKEND');
});

test('market gate is open on Sunday after open hour', () => {
    const out = evaluateForexMarketGate(ts('2026-02-22T22:30:00.000Z'));
    assert.equal(out.marketClosed, false);
    assert.equal(out.reasonCode, 'MARKET_OPEN');
});

test('market gate is closed on Friday after close hour', () => {
    const out = evaluateForexMarketGate(ts('2026-02-20T22:30:00.000Z'));
    assert.equal(out.marketClosed, true);
    assert.equal(out.reasonCode, 'MARKET_CLOSED_WEEKEND');
});

test('market gate is open during normal weekday session', () => {
    const out = evaluateForexMarketGate(ts('2026-02-19T10:00:00.000Z'));
    assert.equal(out.marketClosed, false);
    assert.equal(out.reasonCode, 'MARKET_OPEN');
});
