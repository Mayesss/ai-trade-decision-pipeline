import assert from 'node:assert/strict';
import test from 'node:test';

import { evaluateActionability } from './ai';
import type { ActionabilityInputs } from './ai';

// Defaults chosen so nothing is confirmed and nothing is near a level; each
// case overrides just what it exercises. Wall default = 0.5 ATR (2026-07-08
// re-validation), NEAR = 0.6, ROOM = 1.5.
const base: ActionabilityInputs = {
    microEntryOk: true,
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

test('micro entry gate is a hard prerequisite', () => {
    const out = evaluateActionability({ ...base, microEntryOk: false, primaryBreakoutConfirmed: true });
    assert.deepEqual(out, { actionable: false, reason: 'micro_entry_ok_false' });
});

test('confirmed primary structure is actionable with the context wall far away', () => {
    const out = evaluateActionability({ ...base, primaryBreakoutConfirmed: true, primaryBreakState: 'above' });
    assert.deepEqual(out, { actionable: true, reason: 'confirmed_primary_structure' });
});

test('confirmed breakout pressing into a near unbroken context wall is rejected at 0.5 ATR', () => {
    const out = evaluateActionability({
        ...base,
        primaryBreakoutConfirmed: true,
        primaryBreakState: 'above',
        contextResistanceDistAtr: 0.45,
        contextResistanceState: 'approaching',
    });
    assert.deepEqual(out, { actionable: false, reason: 'into_context_wall' });
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

test('bounce long into a near unbroken context resistance is rejected', () => {
    const out = evaluateActionability({
        ...base,
        primarySupportDistAtr: 0.3,
        primaryResistanceDistAtr: 2.0,
        microBos: true,
        microBosDir: 'up',
        contextResistanceDistAtr: 0.4,
        contextResistanceState: 'approaching',
    });
    assert.deepEqual(out, { actionable: false, reason: 'bounce_into_context_wall' });
});

test('bounce short into a near unbroken context support is rejected', () => {
    const out = evaluateActionability({
        ...base,
        primaryResistanceDistAtr: 0.3,
        primarySupportDistAtr: 2.0,
        microBos: true,
        microBosDir: 'down',
        contextSupportDistAtr: 0.35,
        contextSupportState: 'at_level',
    });
    assert.deepEqual(out, { actionable: false, reason: 'bounce_into_context_wall' });
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

test('sandwiched with no break stays non-actionable', () => {
    const out = evaluateActionability({
        ...base,
        primarySupportDistAtr: 0.3,
        primaryResistanceDistAtr: 0.4,
        microBos: true,
        microBosDir: 'up',
    });
    assert.deepEqual(out, { actionable: false, reason: 'boxed_or_unconfirmed' });
});
