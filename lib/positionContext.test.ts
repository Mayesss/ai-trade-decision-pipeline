import assert from 'node:assert/strict';
import test from 'node:test';

import { composePositionContext } from './positionContext';
import type { PositionInfo } from './analytics';

const openPosition = {
    status: 'open',
    holdSide: 'long',
    entryPrice: 100,
    total: 1,
    entryTimestamp: Date.now() - 60 * 60_000,
} as unknown as PositionInfo;

test('composePositionContext: opening decision + partial closes flow into the context', () => {
    const opening = {
        action: 'BUY',
        ts: '2026-07-01T10:00:00.000Z',
        price: 97.85,
        summary: 'breakout retest held',
        reason: 'primary breakout confirmed with micro turn up off retest',
    };
    const trim = {
        action: 'CLOSE',
        ts: '2026-07-03T14:00:00.000Z',
        price: 103.4,
        reason: 'into context resistance, banking partial',
        exit_size_pct: 40,
    };
    const ctx = composePositionContext({
        position: openPosition,
        pnlPct: 1.2,
        openingDecision: opening,
        partialCloses: [trim],
    });
    assert.ok(ctx);
    assert.deepEqual(ctx?.opening_decision, opening);
    assert.deepEqual(ctx?.partial_closes, [trim]);
});

test('composePositionContext: thesis fields are omitted entirely when absent', () => {
    const ctx = composePositionContext({
        position: openPosition,
        pnlPct: 0,
        openingDecision: null,
        partialCloses: [],
    });
    assert.ok(ctx);
    assert.ok(!('opening_decision' in (ctx as object)));
    assert.ok(!('partial_closes' in (ctx as object)));
});
