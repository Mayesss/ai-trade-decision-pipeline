import assert from 'node:assert/strict';
import { test } from 'vitest';

import { estimateEntryMarginNeedUsd, evaluateSpendableMarginGate, shouldDedupeBoundaryLook } from '../../../lib/swing/flatGates';

test('spendable margin: the cap-sized entry at max leverage is the smallest compliant margin', () => {
    // 60 USDT equity, cap 2× → notional 120; at 10× that is 12 of margin.
    assert.equal(estimateEntryMarginNeedUsd({ equityUsd: 60, maxLeverage: 10 }), 12);
    // The measured week: median spendable 8.4 against a median need of 23 (5×) — blocked even at 10×.
    assert.equal(evaluateSpendableMarginGate({ equityUsd: 59.7, availableUsd: 8.4, maxLeverage: 10 }).blocked, true);
    // 0.16 spendable (four positions open) — blocked.
    assert.equal(evaluateSpendableMarginGate({ equityUsd: 59, availableUsd: 0.16, maxLeverage: 10 }).blocked, true);
    // Enough to post the cap-sized position at 10× (need 11.94, have 13) — not blocked; the model may still pick 5× and be dropped post-AI.
    assert.equal(evaluateSpendableMarginGate({ equityUsd: 59.7, availableUsd: 13, maxLeverage: 10 }).blocked, false);
    // Headroom: 12 needed, 12 available is 12 > 11.76 → blocked; 12.3 available passes.
    assert.equal(evaluateSpendableMarginGate({ equityUsd: 60, availableUsd: 12, maxLeverage: 10 }).blocked, true);
    assert.equal(evaluateSpendableMarginGate({ equityUsd: 60, availableUsd: 12.3, maxLeverage: 10 }).blocked, false);
    // Test-world account: 10000 equity / 9500 available — never blocks.
    assert.equal(evaluateSpendableMarginGate({ equityUsd: 10000, availableUsd: 9500, maxLeverage: 10 }).blocked, false);
    // Missing readings fail open.
    assert.equal(evaluateSpendableMarginGate({ equityUsd: null, availableUsd: 0, maxLeverage: 10 }).blocked, false);
    assert.equal(evaluateSpendableMarginGate({ equityUsd: 60, availableUsd: null, maxLeverage: 10 }).blocked, false);
});

const base = {
    lastFlat: { action: 'HOLD', ageMin: 240, priceMoveAtr: 0.2, actionabilityReason: 'at_primary_level_boxed', leftPlan: false },
    currentActionabilityReason: 'at_primary_level_boxed',
    restingEntryStanding: false,
    dedupedLastBar: false,
    maxMoveAtr: 0.5,
    maxAgeMin: 260,
};

test('boundary dedupe: an unchanged setup after a plain HOLD is skipped', () => {
    assert.equal(shouldDedupeBoundaryLook(base), true);
});

test('boundary dedupe: anything that could change the answer re-asks the model', () => {
    // Price moved past the band.
    assert.equal(shouldDedupeBoundaryLook({ ...base, lastFlat: { ...base.lastFlat, priceMoveAtr: 0.6 } }), false);
    // A different admitting door.
    assert.equal(shouldDedupeBoundaryLook({ ...base, currentActionabilityReason: 'channel_low' }), false);
    // The last call left a wake band — the band owns the re-look.
    assert.equal(shouldDedupeBoundaryLook({ ...base, lastFlat: { ...base.lastFlat, leftPlan: true } }), false);
    // The last call was an entry, or too old (two bars back).
    assert.equal(shouldDedupeBoundaryLook({ ...base, lastFlat: { ...base.lastFlat, action: 'BUY' } }), false);
    assert.equal(shouldDedupeBoundaryLook({ ...base, lastFlat: { ...base.lastFlat, ageMin: 500 } }), false);
    // A resting order stands.
    assert.equal(shouldDedupeBoundaryLook({ ...base, restingEntryStanding: true }), false);
    // One-bar limit: the previous bar was already deduped.
    assert.equal(shouldDedupeBoundaryLook({ ...base, dedupedLastBar: true }), false);
    // Unknown move or door, or gate disabled.
    assert.equal(shouldDedupeBoundaryLook({ ...base, lastFlat: { ...base.lastFlat, priceMoveAtr: null } }), false);
    assert.equal(shouldDedupeBoundaryLook({ ...base, lastFlat: { ...base.lastFlat, actionabilityReason: null }, currentActionabilityReason: null }), false);
    assert.equal(shouldDedupeBoundaryLook({ ...base, maxMoveAtr: 0 }), false);
});
