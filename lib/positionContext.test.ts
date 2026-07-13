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

test('composePositionContext: core fields + standing bracket flow into the context', () => {
    const ctx = composePositionContext({
        position: openPosition,
        pnlPct: 1.2,
        takeProfitPrice: 106.5,
        stopLossPrice: 97.25,
    });
    assert.ok(ctx);
    assert.equal(ctx?.side, 'long');
    assert.equal(ctx?.entry_price, 100);
    assert.equal(ctx?.unrealized_pnl_pct, 1.2);
    assert.equal(ctx?.take_profit_price, 106.5);
    assert.equal(ctx?.stop_loss_price, 97.25);
});

test('composePositionContext: missing bracket legs surface as null, closed position as null context', () => {
    const ctx = composePositionContext({ position: openPosition, pnlPct: 0 });
    assert.ok(ctx);
    assert.equal(ctx?.take_profit_price, null);
    assert.equal(ctx?.stop_loss_price, null);

    const closed = composePositionContext({
        position: { ...openPosition, status: 'closed' } as unknown as PositionInfo,
        pnlPct: 0,
    });
    assert.equal(closed, null);
});
