import assert from 'node:assert/strict';
import { test } from 'vitest';

import { evaluateActionability } from '../../../lib/swing/signals';
import type { ActionabilityInputs } from '../../../lib/swing/decisionConfig';

// Defaults chosen so nothing is confirmed and nothing is near a level; each
// case overrides just what it exercises. Wall default = 0.5 ATR (2026-07-08
// re-validation), NEAR = 0.6, ROOM = 1.5.
const base: ActionabilityInputs = {
    primaryBreakoutConfirmed: false,
    primaryBreakdownConfirmed: false,
    primaryBreakoutRetestOk: false,
    primaryBreakoutRetestDir: null,
    primaryBos: false,
    primaryBosDir: null,
    primaryBreakState: 'inside',
    primarySupportDistAtr: 1.0,
    primaryResistanceDistAtr: 1.0,
    microBreakoutRetestOk: false,
    microBreakoutRetestDir: null,
    microBos: false,
    microBosDir: null,
    microBreakState: 'inside',
    contextSupportDistAtr: 3.0,
    contextSupportState: 'rejected',
    contextResistanceDistAtr: 3.0,
    contextResistanceState: 'rejected',
};

// micro_entry_ok is deliberately absent from ActionabilityInputs. It was a hard
// prerequisite here until 2026-08-30, on the premise "you must take the market
// at this price" — which stopped being true once the model could rest an order
// at a better one. It is now a measurement in the prompt, not a gate, so there
// is nothing left to assert about it.

test('confirmed primary structure is actionable with the context wall far away', () => {
    const out = evaluateActionability({ ...base, primaryBreakoutConfirmed: true, primaryBreakState: 'above' });
    assert.deepEqual(out, { actionable: true, reason: 'confirmed_primary_structure' });
});

// Walls no longer gate: they are location information the model weighs, and
// with resting entries available a wall is a tradeable place, not a veto. The
// branch stays NAMED so the decision trail remains queryable.
test('confirmed breakout pressing into a near unbroken context wall is reported, not rejected', () => {
    const out = evaluateActionability({
        ...base,
        primaryBreakoutConfirmed: true,
        primaryBreakState: 'above',
        contextResistanceDistAtr: 0.45,
        contextResistanceState: 'approaching',
    });
    assert.deepEqual(out, { actionable: true, reason: 'confirmed_primary_structure_into_context_wall' });
});

test('the same wall just beyond 0.5 ATR does not block', () => {
    const out = evaluateActionability({
        ...base,
        primaryBreakoutConfirmed: true,
        primaryBreakState: 'above',
        contextResistanceDistAtr: 0.55,
        contextResistanceState: 'approaching',
    });
    assert.equal(out.actionable, true);
});

test('a broken/retesting context level is not a wall', () => {
    const out = evaluateActionability({
        ...base,
        primaryBreakdownConfirmed: true,
        primaryBreakState: 'below',
        contextSupportDistAtr: 0.2,
        contextSupportState: 'broken',
    });
    assert.deepEqual(out, { actionable: true, reason: 'confirmed_primary_structure' });
});

test('bounce long: at support, room above, micro turning up', () => {
    const out = evaluateActionability({
        ...base,
        primarySupportDistAtr: 0.3,
        primaryResistanceDistAtr: 2.0,
        microBos: true,
        microBosDir: 'up',
    });
    assert.deepEqual(out, { actionable: true, reason: 'bounce_long' });
});

test('bounce long into a near unbroken context resistance is reported, not rejected', () => {
    const out = evaluateActionability({
        ...base,
        primarySupportDistAtr: 0.3,
        primaryResistanceDistAtr: 2.0,
        microBos: true,
        microBosDir: 'up',
        contextResistanceDistAtr: 0.4,
        contextResistanceState: 'approaching',
    });
    assert.deepEqual(out, { actionable: true, reason: 'bounce_long_into_context_wall' });
});

test('bounce short into a near unbroken context support is reported, not rejected', () => {
    const out = evaluateActionability({
        ...base,
        primaryResistanceDistAtr: 0.3,
        primarySupportDistAtr: 2.0,
        microBos: true,
        microBosDir: 'down',
        contextSupportDistAtr: 0.35,
        contextSupportState: 'at_level',
    });
    assert.deepEqual(out, { actionable: true, reason: 'bounce_short_into_context_wall' });
});

test('bounce survives a near context wall that is already broken', () => {
    const out = evaluateActionability({
        ...base,
        primarySupportDistAtr: 0.3,
        primaryResistanceDistAtr: 2.0,
        microBos: true,
        microBosDir: 'up',
        contextResistanceDistAtr: 0.4,
        contextResistanceState: 'retesting',
    });
    assert.deepEqual(out, { actionable: true, reason: 'bounce_long' });
});

// ---------------------------------------------------------------------------
// Doors (c) anchor and (d) geometry, added 2026-09-03. Doors (a) and (b) both
// key on a move that has already happened; these key on whether there is a risk
// anchor near price, which is what the location-first strategies need.
// ---------------------------------------------------------------------------

test('the symmetric box is actionable — this is the range fade, not still water', () => {
    // Was the headline `boxed_or_unconfirmed` case until the anchor door: both
    // levels inside NEAR, so the bounce door's ROOM requirement can never be met
    // however the micro turns. Exactly the trade dropping ENTRY_TP_MIN_ATR was
    // meant to make expressible.
    const out = evaluateActionability({
        ...base,
        primarySupportDistAtr: 0.3,
        primaryResistanceDistAtr: 0.4,
        microBos: true,
        microBosDir: 'up',
    });
    assert.deepEqual(out, { actionable: true, reason: 'at_primary_level_boxed' });
});

test('at one level with no room and no micro turn is actionable, and names its side', () => {
    // The bounce door needs BOTH room and a micro turn; the anchor door needs
    // neither — the level alone defines invalidation.
    const atSup = evaluateActionability({ ...base, primarySupportDistAtr: 0.4, primaryResistanceDistAtr: 1.0 });
    assert.deepEqual(atSup, { actionable: true, reason: 'at_primary_support' });
    const atRes = evaluateActionability({ ...base, primarySupportDistAtr: 1.0, primaryResistanceDistAtr: 0.5 });
    assert.deepEqual(atRes, { actionable: true, reason: 'at_primary_resistance' });
});

test('the confirmed and bounce doors still win over the anchor door', () => {
    // Ordering matters for the skip/decision trail: a tick that is BOTH at a
    // level and confirmed must keep reporting the stronger reason.
    const confirmed = evaluateActionability({
        ...base,
        primarySupportDistAtr: 0.3,
        primaryResistanceDistAtr: 0.4,
        primaryBreakoutConfirmed: true,
        primaryBreakState: 'above',
    });
    assert.equal(confirmed.reason, 'confirmed_primary_structure');
    const bounce = evaluateActionability({
        ...base,
        primarySupportDistAtr: 0.3,
        primaryResistanceDistAtr: 2.0,
        microBos: true,
        microBosDir: 'up',
    });
    assert.equal(bounce.reason, 'bounce_long');
});

test('deep pullback to the channel floor is actionable with no level in reach', () => {
    // primaryBreakState flips to 'inside' once price comes back through the last
    // swing extreme, so without the geometry door the DEEP pullback — the better
    // entry — is dropped while the shallow one passes.
    const low = evaluateActionability({ ...base, primaryChannelPos: 0.1 });
    assert.deepEqual(low, { actionable: true, reason: 'channel_low' });
    const high = evaluateActionability({ ...base, primaryChannelPos: 0.92 });
    assert.deepEqual(high, { actionable: true, reason: 'channel_high' });
});

test('mid-channel is not a geometry door', () => {
    assert.equal(evaluateActionability({ ...base, primaryChannelPos: 0.5 }).actionable, false);
    assert.equal(evaluateActionability({ ...base, primaryChannelPos: 0.7 }).actionable, false);
});

test('a trendline within NEAR_ATR is an anchor even with no swing level near', () => {
    const sup = evaluateActionability({ ...base, primarySupportTrendlineDistAtr: 0.3 });
    assert.deepEqual(sup, { actionable: true, reason: 'at_support_trendline' });
    const res = evaluateActionability({ ...base, primaryResistanceTrendlineDistAtr: 0.55 });
    assert.deepEqual(res, { actionable: true, reason: 'at_resistance_trendline' });
    const far = evaluateActionability({ ...base, primarySupportTrendlineDistAtr: 1.2 });
    assert.equal(far.actionable, false);
});

test('still water is still the only skip: no break, no level, no edge, no trendline', () => {
    const out = evaluateActionability({
        ...base,
        primarySupportDistAtr: 1.0,
        primaryResistanceDistAtr: 1.0,
        primaryChannelPos: 0.5,
        primarySupportTrendlineDistAtr: 2.0,
        primaryResistanceTrendlineDistAtr: 2.2,
    });
    assert.deepEqual(out, { actionable: false, reason: 'boxed_or_unconfirmed' });
});

test('absent geometry never opens a door on its own', () => {
    // Short candle history → computeWaveGeometry returns null → every geometry
    // field arrives null. That must read as "not measured", not "at the edge".
    const out = evaluateActionability({
        ...base,
        primaryChannelPos: null,
        primarySupportTrendlineDistAtr: null,
        primaryResistanceTrendlineDistAtr: null,
    });
    assert.deepEqual(out, { actionable: false, reason: 'boxed_or_unconfirmed' });
});
