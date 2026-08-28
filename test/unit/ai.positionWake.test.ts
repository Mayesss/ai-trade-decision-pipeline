import assert from 'node:assert/strict';
import { test } from 'vitest';

import { computeSwingState, postprocessDecision, sanitizePositionWake, POSITION_WAKE_MIN_ATR } from '../../lib/ai';
import type { PromptDecisionContext } from '../../lib/ai';
import { isCooldownBandDecision, isPositionWakeBandDecision, isPositionWakeEntry } from '../../lib/history';

// In-position wake bands: sanitizePositionWake is pure (the
// ENABLE_POSITION_WAKE_BANDS flag gates the postprocessDecision routing at
// CALL time — mirroring ENABLE_CRYPTO_MARGIN_RECYCLE — and the analyze-route
// persistence, never the sanitizer itself).

// Long position: price 100, bracket SL 90 / TP 120, primary ATR 10 → min band
// distance = POSITION_WAKE_MIN_ATR × 10 = 3.
const longBase = {
    action: 'HOLD',
    positionOpen: true,
    exitSizePct: null,
    price: 100,
    primaryAtr: 10,
    takeProfitPrice: 120,
    stopLossPrice: 90,
};

test('sanitizePositionWake: valid bands inside the bracket pass untouched', () => {
    const out = sanitizePositionWake({ ...longBase, wakeAbove: 110, wakeBelow: 95 });
    assert.equal(out.wakeAbove, 110);
    assert.equal(out.wakeBelow, 95);
    assert.deepEqual(out.notes, []);
});

test('sanitizePositionWake: wrong-side bands are dropped (incl. a restated just-fired band)', () => {
    // A band the price has already crossed sits on the wrong side — this is
    // what auto-consumes a restated just-fired band without churn.
    const out = sanitizePositionWake({ ...longBase, wakeAbove: 99, wakeBelow: 101 });
    assert.equal(out.wakeAbove, null);
    assert.equal(out.wakeBelow, null);
    assert.ok(out.notes.includes('wake_above_dropped_not_above_price'));
    assert.ok(out.notes.includes('wake_below_dropped_not_below_price'));
});

test('sanitizePositionWake: bands at/beyond the bracket are unreachable and dropped', () => {
    // The venue bracket fires there first; a band at the level adds nothing.
    for (const wakeAbove of [120, 125]) {
        const out = sanitizePositionWake({ ...longBase, wakeAbove, wakeBelow: null });
        assert.equal(out.wakeAbove, null, `above=${wakeAbove}`);
        assert.ok(out.notes.includes('wake_above_dropped_beyond_bracket'));
    }
    for (const wakeBelow of [90, 85]) {
        const out = sanitizePositionWake({ ...longBase, wakeAbove: null, wakeBelow });
        assert.equal(out.wakeBelow, null, `below=${wakeBelow}`);
        assert.ok(out.notes.includes('wake_below_dropped_beyond_bracket'));
    }
});

test('sanitizePositionWake: short position mirrors the bracket bounds', () => {
    // Short: SL above price, TP below. Bands still must sit strictly between
    // price and the bracket leg on their side.
    const shortBase = { ...longBase, takeProfitPrice: 85, stopLossPrice: 110 };
    const ok = sanitizePositionWake({ ...shortBase, wakeAbove: 106, wakeBelow: 87 });
    assert.equal(ok.wakeAbove, 106);
    assert.equal(ok.wakeBelow, 87);
    const beyond = sanitizePositionWake({ ...shortBase, wakeAbove: 112, wakeBelow: 84 });
    assert.equal(beyond.wakeAbove, null);
    assert.equal(beyond.wakeBelow, null);
    assert.ok(beyond.notes.includes('wake_above_dropped_beyond_bracket'));
    assert.ok(beyond.notes.includes('wake_below_dropped_beyond_bracket'));
});

test('sanitizePositionWake: bands glued to price are dropped (churn guard)', () => {
    const out = sanitizePositionWake({ ...longBase, wakeAbove: 102, wakeBelow: 98 });
    assert.equal(out.wakeAbove, null);
    assert.equal(out.wakeBelow, null);
    assert.ok(out.notes.includes('wake_above_dropped_too_close'));
    assert.ok(out.notes.includes('wake_below_dropped_too_close'));
    assert.equal(POSITION_WAKE_MIN_ATR, 0.3);
});

test('sanitizePositionWake: unknown ATR fails open on distance (side/bracket still bound)', () => {
    const out = sanitizePositionWake({ ...longBase, primaryAtr: null, wakeAbove: 101, wakeBelow: null });
    assert.equal(out.wakeAbove, 101);
    assert.ok(out.notes.includes('wake_min_dist_unverified'));
});

test('sanitizePositionWake: unknown price drops the bands', () => {
    const out = sanitizePositionWake({ ...longBase, price: null, wakeAbove: 110, wakeBelow: 95 });
    assert.equal(out.wakeAbove, null);
    assert.equal(out.wakeBelow, null);
    assert.ok(out.notes.includes('wake_bands_dropped_price_unknown'));
});

test('sanitizePositionWake: a missing bracket leg only leaves side+distance checks', () => {
    const out = sanitizePositionWake({ ...longBase, takeProfitPrice: null, wakeAbove: 130, wakeBelow: null });
    assert.equal(out.wakeAbove, 130);
    assert.deepEqual(out.notes, []);
});

test('sanitizePositionWake: eligibility — in-position HOLD or partial CLOSE only', () => {
    for (const params of [
        { ...longBase, positionOpen: false }, // flat
        { ...longBase, action: 'BUY' }, // entry actions ineligible (v1)
        { ...longBase, action: 'REVERSE' },
        { ...longBase, action: 'CLOSE', exitSizePct: null }, // full close
        { ...longBase, action: 'CLOSE', exitSizePct: 100 },
    ]) {
        const out = sanitizePositionWake({ ...params, wakeAbove: 110, wakeBelow: 95 });
        assert.equal(out.wakeAbove, null);
        assert.equal(out.wakeBelow, null);
    }
    const partial = sanitizePositionWake({ ...longBase, action: 'CLOSE', exitSizePct: 40, wakeAbove: 110, wakeBelow: 95 });
    assert.equal(partial.wakeAbove, 110);
    assert.equal(partial.wakeBelow, 95);
});

test('sanitizePositionWake: note survives with a band, dropped bandless, trimmed and capped', () => {
    const kept = sanitizePositionWake({
        ...longBase,
        wakeAbove: 110,
        wakeBelow: null,
        wakeNote: `  trail vs take at 110 ${'x'.repeat(300)}  `,
    });
    assert.ok(kept.wakeNote?.startsWith('trail vs take at 110'));
    assert.equal(kept.wakeNote?.length, 200);
    const dropped = sanitizePositionWake({ ...longBase, wakeAbove: 99, wakeBelow: null, wakeNote: 'plan' });
    assert.equal(dropped.wakeNote, null);
    assert.ok(dropped.notes.includes('wake_note_dropped_no_band'));
});

// ---- postprocessDecision routing (flag read at call time) ----

const openContext: PromptDecisionContext = {
    signal_strength: 'MEDIUM',
    micro_bias_calc: 'UP',
    primary_bias: 'UP',
    macro_bias: 'UP',
    context_bias: 'UP',
    primary_trend_up: true,
    primary_trend_down: false,
    primary_breakdown_confirmed: false,
    primary_breakout_confirmed: true,
    micro_entry_ok: true,
    aligned_driver_count: 4,
    regime_alignment: 1,
    location_confluence_score: 1,
    micro_extension_atr: 0,
    primary_extension_atr: 0,
    breakout_retest_ok_primary: true,
    breakout_retest_dir_primary: 'up',
} as PromptDecisionContext;

const gatesOk = { spread_ok: true, liquidity_ok: true, atr_ok: true, slippage_ok: true };

function decide(decision: Record<string, unknown>, positionOpen: boolean, flagOn: boolean) {
    const prev = process.env.ENABLE_POSITION_WAKE_BANDS;
    process.env.ENABLE_POSITION_WAKE_BANDS = flagOn ? 'true' : 'false';
    try {
        return postprocessDecision({
            decision,
            context: openContext,
            gates: gatesOk,
            positionOpen,
            recentActions: [],
            positionContext: null,
            policy: 'balanced',
        });
    } finally {
        if (prev === undefined) delete process.env.ENABLE_POSITION_WAKE_BANDS;
        else process.env.ENABLE_POSITION_WAKE_BANDS = prev;
    }
}

test('routing: flag on + in-position HOLD keeps wake bands, cooldown_minutes stays flat-only', () => {
    const out = decide(
        { action: 'HOLD', cooldown_minutes: 480, cooldown_wake_above: 110, cooldown_wake_below: 95, cooldown_wake_note: 'plan' },
        true,
        true,
    );
    assert.equal(out.cooldown_minutes, null);
    assert.equal(out.cooldown_wake_above, 110);
    assert.equal(out.cooldown_wake_below, 95);
    assert.equal(out.cooldown_wake_note, 'plan');
});

test('routing: flag on + partial CLOSE keeps wake bands', () => {
    const out = decide(
        { action: 'CLOSE', exit_size_pct: 40, cooldown_wake_above: 110, cooldown_wake_below: null },
        true,
        true,
    );
    assert.equal(out.action, 'CLOSE');
    assert.equal(out.cooldown_wake_above, 110);
});

test('routing: flag on + full CLOSE nulls wake bands', () => {
    const out = decide(
        { action: 'CLOSE', exit_size_pct: 100, cooldown_wake_above: 110, cooldown_wake_below: 95 },
        true,
        true,
    );
    assert.equal(out.cooldown_wake_above, null);
    assert.equal(out.cooldown_wake_below, null);
});

test('routing: flag off nulls in-position wake bands', () => {
    const out = decide({ action: 'HOLD', cooldown_wake_above: 110, cooldown_wake_below: 95 }, true, false);
    assert.equal(out.cooldown_wake_above, null);
    assert.equal(out.cooldown_wake_below, null);
});

test('routing: flat HOLD cooldown fields are untouched by the flag', () => {
    for (const flag of [true, false]) {
        const out = decide(
            { action: 'HOLD', cooldown_minutes: 480, cooldown_wake_above: 110, cooldown_wake_below: 95 },
            false,
            flag,
        );
        assert.equal(out.cooldown_minutes, 480);
        assert.equal(out.cooldown_wake_above, 110);
        assert.equal(out.cooldown_wake_below, 95);
    }
});

// ---- market.position_wake / market.position_wake_armed injection ----

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

const buildUserPrompt = (positionWake: any) => {
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
        null,
        null,
        positionWake,
    );
    return state.assemble(null, []).user;
};

const parseMarket = (user: string) =>
    JSON.parse(user.slice(user.indexOf('MARKET (raw inputs):') + 'MARKET (raw inputs):'.length).split('\n\nTASKS:')[0]);

test('computeSwingState: a fired band surfaces as market.position_wake with age and note', () => {
    const market = parseMarket(
        buildUserPrompt({
            fired: { crossed: 'below', level: 95, setAtMs: NOW_MS - 42 * 60_000, note: '  losing 95 = thesis dead  ' },
            armed: { above: 110, below: 95, note: 'x', setAtMs: NOW_MS },
        }),
    );
    assert.deepEqual(market.position_wake, {
        crossed: 'below',
        level: 95,
        set_minutes_ago: 42,
        note: 'losing 95 = thesis dead',
    });
    // fired suppresses the armed echo — the bands are being consumed this tick
    assert.equal(market.position_wake_armed, undefined);
});

test('computeSwingState: armed bands echo as market.position_wake_armed on quiet looks', () => {
    const market = parseMarket(
        buildUserPrompt({ fired: null, armed: { above: 110, below: 95, note: 'plan', setAtMs: NOW_MS - 60 * 60_000 } }),
    );
    assert.deepEqual(market.position_wake_armed, {
        above: 110,
        below: 95,
        set_minutes_ago: 60,
        note: 'plan',
    });
    assert.equal(market.position_wake, undefined);
});

test('computeSwingState: no bands → neither key present', () => {
    const user = buildUserPrompt({ fired: null, armed: null });
    assert.ok(!user.includes('position_wake'));
});

// ---- chart-index predicates (lib/history.ts) ----

test('index predicates: bands-without-minutes discriminates in-position from flat cooldown rows', () => {
    const inPosHold = { action: 'HOLD', cooldown_minutes: null, cooldown_wake_above: 110 };
    const inPosTrim = { action: 'CLOSE', exit_size_pct: 40, cooldown_minutes: null, cooldown_wake_below: 95 };
    const flatCooldown = { action: 'HOLD', cooldown_minutes: 480, cooldown_wake_above: 110 };
    assert.equal(isPositionWakeBandDecision(inPosHold), true);
    assert.equal(isPositionWakeBandDecision(inPosTrim), true);
    // A flat cooldown row belongs to the OTHER predicate — never both.
    assert.equal(isPositionWakeBandDecision(flatCooldown), false);
    assert.equal(isCooldownBandDecision(flatCooldown), true);
    assert.equal(isCooldownBandDecision(inPosHold), false);
    // No band / wrong action → neither.
    assert.equal(isPositionWakeBandDecision({ action: 'HOLD' }), false);
    assert.equal(isPositionWakeBandDecision({ action: 'BUY', cooldown_wake_above: 110 }), false);
});

test('index predicates: fired position-wake rows are recognized via snapshot.positionWake', () => {
    assert.equal(isPositionWakeEntry({ snapshot: { positionWake: { crossed: 'below' } } }), true);
    assert.equal(isPositionWakeEntry({ snapshot: { cooldownWake: { crossed: 'below' } } }), false);
    assert.equal(isPositionWakeEntry({ snapshot: {} }), false);
    assert.equal(isPositionWakeEntry(null), false);
});
