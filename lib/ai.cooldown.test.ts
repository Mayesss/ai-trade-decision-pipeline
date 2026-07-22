import assert from 'node:assert/strict';
import test from 'node:test';

import { computeSwingState, sanitizeHoldCooldown, HOLD_COOLDOWN_MAX_MINUTES, HOLD_COOLDOWN_MIN_MINUTES } from './ai';

const base = { action: 'HOLD', positionOpen: false, price: 100 };

test('sanitizeHoldCooldown: valid flat HOLD request passes with bands intact', () => {
    const out = sanitizeHoldCooldown({ ...base, cooldownMinutes: 480, wakeAbove: 105, wakeBelow: 95 });
    assert.equal(out.cooldownMinutes, 480);
    assert.equal(out.wakeAbove, 105);
    assert.equal(out.wakeBelow, 95);
    assert.deepEqual(out.notes, []);
});

test('sanitizeHoldCooldown: wake note survives alongside a valid band, trimmed and capped', () => {
    const out = sanitizeHoldCooldown({
        ...base,
        cooldownMinutes: 480,
        wakeAbove: 105,
        wakeBelow: null,
        wakeNote: `  retest of broken 105 for long entry ${'x'.repeat(300)}  `,
    });
    assert.equal(out.wakeAbove, 105);
    assert.ok(out.wakeNote?.startsWith('retest of broken 105 for long entry'));
    assert.equal(out.wakeNote?.length, 200);
});

test('sanitizeHoldCooldown: wake note is dropped when no band survives', () => {
    // Both bands wrong-side → dropped; a note is a plan attached to a band,
    // so it must not persist bandless.
    const out = sanitizeHoldCooldown({
        ...base,
        cooldownMinutes: 600,
        wakeAbove: 99,
        wakeBelow: 101,
        wakeNote: 'retest plan',
    });
    assert.equal(out.wakeNote, null);
    assert.ok(out.notes.includes('wake_note_dropped_no_band'));
});

test('sanitizeHoldCooldown: non-string / empty wake notes coerce to null', () => {
    for (const wakeNote of [null, undefined, 42, {}, '   ']) {
        const out = sanitizeHoldCooldown({ ...base, cooldownMinutes: 480, wakeAbove: 105, wakeBelow: null, wakeNote });
        assert.equal(out.wakeNote, null);
        assert.ok(!out.notes.includes('wake_note_dropped_no_band'));
    }
});

test('sanitizeHoldCooldown: sub-bar requests clamp UP to the 6h floor (4H cadence)', () => {
    // Anything shorter than one primary bar would expire before the next
    // evaluation and suppress nothing — the floor makes every cooldown real.
    const out = sanitizeHoldCooldown({ ...base, cooldownMinutes: 120, wakeAbove: null, wakeBelow: null });
    assert.equal(out.cooldownMinutes, HOLD_COOLDOWN_MIN_MINUTES);
    assert.equal(HOLD_COOLDOWN_MIN_MINUTES, 360);
});

test('sanitizeHoldCooldown: only flat HOLD is eligible', () => {
    for (const params of [
        { ...base, action: 'BUY' },
        { ...base, action: 'CLOSE' },
        { ...base, positionOpen: true },
    ]) {
        const out = sanitizeHoldCooldown({ ...params, cooldownMinutes: 60, wakeAbove: 105, wakeBelow: 95 });
        assert.equal(out.cooldownMinutes, null);
        assert.equal(out.wakeAbove, null);
        assert.equal(out.wakeBelow, null);
    }
});

test('sanitizeHoldCooldown: minutes clamp to [min, max]', () => {
    assert.equal(sanitizeHoldCooldown({ ...base, cooldownMinutes: 1, wakeAbove: null, wakeBelow: null }).cooldownMinutes, HOLD_COOLDOWN_MIN_MINUTES);
    assert.equal(
        sanitizeHoldCooldown({ ...base, cooldownMinutes: 100000, wakeAbove: null, wakeBelow: null }).cooldownMinutes,
        HOLD_COOLDOWN_MAX_MINUTES,
    );
    assert.equal(sanitizeHoldCooldown({ ...base, cooldownMinutes: null, wakeAbove: 105, wakeBelow: null }).cooldownMinutes, null);
});

test('sanitizeHoldCooldown: wrong-side wake bands are dropped, cooldown survives', () => {
    const out = sanitizeHoldCooldown({ ...base, cooldownMinutes: 600, wakeAbove: 99, wakeBelow: 101 });
    assert.equal(out.cooldownMinutes, 600);
    assert.equal(out.wakeAbove, null);
    assert.equal(out.wakeBelow, null);
    assert.ok(out.notes.includes('wake_above_dropped_not_above_price'));
    assert.ok(out.notes.includes('wake_below_dropped_not_below_price'));
});

test('sanitizeHoldCooldown: unknown price drops bands but keeps the cooldown', () => {
    const out = sanitizeHoldCooldown({ ...base, price: null, cooldownMinutes: 600, wakeAbove: 105, wakeBelow: 95 });
    assert.equal(out.cooldownMinutes, 600);
    assert.equal(out.wakeAbove, null);
    assert.equal(out.wakeBelow, null);
    assert.ok(out.notes.includes('wake_bands_dropped_price_unknown'));
});

// ---- wake-band trigger context (market.cooldown_wake) ----

const NOW_MS = 1_750_000_000_000;
const wakeBundle = { ticker: [{ lastPr: '100', change24h: '0' }], candles: [] };
const wakeIndicators: any = {
    micro: '',
    macro: '',
    primary: { summary: '', timeframe: '4h' },
    context: { summary: '', timeframe: '1d' },
    microTimeFrame: '1h',
    macroTimeFrame: '1d',
    sr: {},
    rawCandles: {},
};
const wakeMomentum: any = { microExtensionInAtr: 0, info: { microEntryOk: false } };

const buildWakeUserPrompt = (cooldownWake: any) => {
    const state = computeSwingState(
        'BTCUSDT',
        '4h',
        wakeBundle,
        {},
        'none',
        null,
        null,
        wakeIndicators,
        {},
        null,
        wakeMomentum,
        [],
        null,
        true,
        5,
        undefined,
        'crypto',
        'bitget',
        null,
        NOW_MS,
        null,
        cooldownWake,
    );
    return state.assemble(null, []).user;
};

test('computeSwingState: a crossed wake band surfaces as market.cooldown_wake with its age', () => {
    const user = buildWakeUserPrompt({ crossed: 'above', level: 105, setAtMs: NOW_MS - 76 * 60_000 });
    const market = JSON.parse(user.slice(user.indexOf('MARKET (raw inputs):') + 'MARKET (raw inputs):'.length).split('\n\nTASKS:')[0]);
    assert.deepEqual(market.cooldown_wake, { crossed: 'above', level: 105, set_minutes_ago: 76 });
});

test('computeSwingState: the wake note is echoed back in market.cooldown_wake', () => {
    const user = buildWakeUserPrompt({
        crossed: 'above',
        level: 105,
        setAtMs: NOW_MS - 30 * 60_000,
        note: '  acceptance above 105 → breakout check  ',
    });
    const market = JSON.parse(user.slice(user.indexOf('MARKET (raw inputs):') + 'MARKET (raw inputs):'.length).split('\n\nTASKS:')[0]);
    assert.deepEqual(market.cooldown_wake, {
        crossed: 'above',
        level: 105,
        set_minutes_ago: 30,
        note: 'acceptance above 105 → breakout check',
    });
});

test('computeSwingState: unknown set time yields set_minutes_ago null', () => {
    const user = buildWakeUserPrompt({ crossed: 'below', level: 95, setAtMs: null });
    const market = JSON.parse(user.slice(user.indexOf('MARKET (raw inputs):') + 'MARKET (raw inputs):'.length).split('\n\nTASKS:')[0]);
    assert.deepEqual(market.cooldown_wake, { crossed: 'below', level: 95, set_minutes_ago: null });
});

test('computeSwingState: no wake trigger → market.cooldown_wake absent', () => {
    const user = buildWakeUserPrompt(null);
    assert.ok(!user.includes('cooldown_wake"'));
});
