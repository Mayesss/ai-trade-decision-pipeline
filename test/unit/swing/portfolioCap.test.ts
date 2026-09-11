import assert from 'node:assert/strict';
import { test } from 'vitest';

import { evaluatePortfolioCapGate, type PortfolioOccupant } from '../../../lib/swing/portfolioCap';

const occ = (symbol: string, category: string | null, status: PortfolioOccupant['status'] = 'in_position', platform = 'bitget'): PortfolioOccupant => ({
    platform,
    symbol,
    category,
    status,
});

const self = { platform: 'bitget', symbol: 'BTCUSDT', category: 'crypto' };

test('empty book: nothing blocks', () => {
    const v = evaluatePortfolioCapGate({ self, occupants: [], maxOpen: 4, onePerClass: true });
    assert.equal(v.blocked, false);
});

test('own thread never blocks its own tick, even over the cap', () => {
    const occupants = [occ('BTCUSDT', 'crypto'), occ('EURUSD', 'forex', 'in_position', 'capital'), occ('GOLD', 'commodity', 'in_position', 'capital'), occ('US500', 'index', 'in_position', 'capital'), occ('GBPUSD', 'forex', 'pending_entry', 'capital')];
    const v = evaluatePortfolioCapGate({ self, occupants, maxOpen: 2, onePerClass: true });
    assert.equal(v.blocked, false);
    assert.equal(!v.blocked && v.selfOccupied, true);
});

test('same asset class standing (position or resting entry) blocks a sibling', () => {
    const open = evaluatePortfolioCapGate({ self, occupants: [occ('ETHUSDT', 'crypto')], maxOpen: 4, onePerClass: true });
    assert.equal(open.blocked, true);
    assert.equal(open.blocked && open.stage, 'asset_class_occupied');
    assert.equal(open.blocked && open.reason, 'asset_class_occupied:crypto:ETHUSDT');

    const resting = evaluatePortfolioCapGate({ self, occupants: [occ('ETHUSDT', 'crypto', 'pending_entry')], maxOpen: 4, onePerClass: true });
    assert.equal(resting.blocked && resting.reason, 'asset_class_occupied:crypto:ETHUSDT(resting)');
});

test('a different class does not block below the cap', () => {
    const v = evaluatePortfolioCapGate({ self, occupants: [occ('EURUSD', 'forex', 'in_position', 'capital')], maxOpen: 4, onePerClass: true });
    assert.equal(v.blocked, false);
});

test('the total cap binds regardless of class', () => {
    const occupants = [occ('EURUSD', 'forex', 'in_position', 'capital'), occ('GOLD', 'commodity', 'in_position', 'capital'), occ('US500', 'index', 'pending_entry', 'capital')];
    const v = evaluatePortfolioCapGate({ self, occupants, maxOpen: 3, onePerClass: true });
    assert.equal(v.blocked, true);
    assert.equal(v.blocked && v.stage, 'position_cap');
    assert.equal(v.blocked && v.reason, 'position_cap:3_of_3_committed');
    // One more slot → passes (different classes).
    assert.equal(evaluatePortfolioCapGate({ self, occupants, maxOpen: 4, onePerClass: true }).blocked, false);
});

test('flags: cap 0 disables the count, onePerClass=false disables the class rule', () => {
    const occupants = [occ('ETHUSDT', 'crypto'), occ('EURUSD', 'forex', 'in_position', 'capital')];
    assert.equal(evaluatePortfolioCapGate({ self, occupants, maxOpen: 0, onePerClass: false }).blocked, false);
    assert.equal(evaluatePortfolioCapGate({ self, occupants, maxOpen: 0, onePerClass: true }).blocked, true);
    assert.equal(evaluatePortfolioCapGate({ self, occupants, maxOpen: 1, onePerClass: false }).blocked, true);
});

test('unknown category on either side skips the class rule for that pair', () => {
    assert.equal(evaluatePortfolioCapGate({ self: { ...self, category: null }, occupants: [occ('ETHUSDT', 'crypto')], maxOpen: 4, onePerClass: true }).blocked, false);
    assert.equal(evaluatePortfolioCapGate({ self, occupants: [occ('XYZUSDT', null)], maxOpen: 4, onePerClass: true }).blocked, false);
});
