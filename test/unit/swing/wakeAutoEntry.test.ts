import assert from 'node:assert/strict';
import { test } from 'vitest';

import {
    buildWakeAutoEntryDecision,
    WAKE_AUTO_ENTRY_MAX_EXTENSION_ATR,
    WAKE_AUTO_ENTRY_SL_ATR,
    WAKE_AUTO_ENTRY_TP_ATR,
} from '../../../lib/swing/wakeAutoEntry';
import { postprocessDecision } from '../../../lib/ai';
import type { PromptDecisionContext } from '../../../lib/ai';

test('builder: confirmed breakout above -> BUY with ATR-anchored bracket and failed-break trigger at the level', () => {
    const d = buildWakeAutoEntryDecision({
        crossed: 'above',
        level: 100,
        note: 'Above 100 accept breakout long',
        sustainedMinutes: 11,
        breakExtensionAtr: null,
        price: 100.4,
        primaryAtr: 2,
    });
    assert.ok(d);
    assert.equal(d.action, 'BUY');
    assert.equal(d.stop_loss_price, 100.4 - WAKE_AUTO_ENTRY_SL_ATR * 2);
    assert.equal(d.take_profit_price, 100.4 + WAKE_AUTO_ENTRY_TP_ATR * 2);
    assert.equal(d.entry_trigger_price, 100); // arms the failed-break watch
    assert.equal(d.entry_limit_price, null); // market entry, no resting limit
    assert.equal(d.cooldown_minutes, null);
    assert.match(String(d.reason), /wake_auto_entry/);
    assert.match(String(d.reason), /held 11m/);
});

test('builder: confirmed breakdown below -> SELL mirrored', () => {
    const d = buildWakeAutoEntryDecision({
        crossed: 'below',
        level: 100,
        note: null,
        sustainedMinutes: null,
        breakExtensionAtr: 0.62,
        price: 99.5,
        primaryAtr: 2,
    });
    assert.ok(d);
    assert.equal(d.action, 'SELL');
    assert.equal(d.stop_loss_price, 99.5 + WAKE_AUTO_ENTRY_SL_ATR * 2);
    assert.equal(d.take_profit_price, 99.5 - WAKE_AUTO_ENTRY_TP_ATR * 2);
    assert.equal(d.entry_trigger_price, 100);
    assert.match(String(d.reason), /extension 0\.62 ATR/);
});

test('builder: chase guard — refuses a fire discovered too far past the level', () => {
    const base = {
        crossed: 'above' as const,
        level: 100,
        note: null,
        sustainedMinutes: 12,
        breakExtensionAtr: null,
        price: 100.5,
        primaryAtr: 2,
    };
    // At the cap (extension exactly 1.0 ATR = price 102): still builds — the
    // stop lands 0.5 ATR inside the level, the last sweep-resistant placement.
    const atCap = buildWakeAutoEntryDecision({ ...base, price: 100 + WAKE_AUTO_ENTRY_MAX_EXTENSION_ATR * 2 });
    assert.ok(atCap);
    // Beyond the cap: refuse — the caller falls back to the AI (retest protocol).
    assert.equal(buildWakeAutoEntryDecision({ ...base, price: 102.1 }), null);
    // Mirrored for a breakdown short.
    assert.equal(buildWakeAutoEntryDecision({ ...base, crossed: 'below', price: 97.9 }), null);
    assert.ok(buildWakeAutoEntryDecision({ ...base, crossed: 'below', price: 98.5 }));
});

test('builder: refuses to build without a usable ATR/price/level (caller falls back to the AI)', () => {
    const base = {
        crossed: 'above' as const,
        level: 100,
        note: null,
        sustainedMinutes: 10,
        breakExtensionAtr: null,
        price: 100.4,
        primaryAtr: 2,
    };
    assert.equal(buildWakeAutoEntryDecision({ ...base, primaryAtr: null }), null);
    assert.equal(buildWakeAutoEntryDecision({ ...base, primaryAtr: 0 }), null);
    assert.equal(buildWakeAutoEntryDecision({ ...base, price: NaN }), null);
    assert.equal(buildWakeAutoEntryDecision({ ...base, level: 0 }), null);
    // stop would land <= 0 on a tiny price with a huge ATR
    assert.equal(buildWakeAutoEntryDecision({ ...base, price: 1, primaryAtr: 2 }), null);
});

// ---------------------------------------------------------------------------
// postprocessDecision: confirmedWakeEntry bypasses ONLY the micro_entry_ok
// timing block. Trend guard and base gates still coerce.
// ---------------------------------------------------------------------------
const wakeContext: PromptDecisionContext = {
    micro_bias_calc: 'UP',
    primary_bias: 'up',
    macro_bias: 'UP',
    context_bias: 'UP',
    primary_trend_up: false,
    primary_trend_down: false,
    primary_breakdown_confirmed: false,
    primary_breakout_confirmed: false,
    // The block under test: entry timing not ok, no exception evidence.
    micro_entry_ok: false,
    aligned_driver_count: 0,
    regime_alignment: 0,
    location_confluence_score: 0,
    micro_extension_atr: 0,
    primary_extension_atr: 0,
    breakout_retest_ok_primary: false,
    breakout_retest_dir_primary: null,
};
const gatesOk = { spread_ok: true, liquidity_ok: true, atr_ok: true, slippage_ok: true };

function post(action: 'BUY' | 'SELL', opts: { confirmedWakeEntry?: boolean; context?: PromptDecisionContext; gates?: typeof gatesOk }) {
    return postprocessDecision({
        decision: { action },
        context: opts.context ?? wakeContext,
        gates: opts.gates ?? gatesOk,
        positionOpen: false,
        recentActions: [],
        positionContext: null,
        policy: 'strict',
        lastClosedPosition: null,
        confirmedWakeEntry: opts.confirmedWakeEntry,
    });
}

test('postprocess: micro_entry_ok=false coerces a normal entry to HOLD but passes a confirmed wake entry', () => {
    assert.equal(post('BUY', {}).action, 'HOLD');
    assert.equal(post('BUY', { confirmedWakeEntry: true }).action, 'BUY');
});

test('postprocess: confirmed wake entry still respects the trend guard', () => {
    const counterTrend: PromptDecisionContext = {
        ...wakeContext,
        micro_bias_calc: 'UP',
        primary_trend_up: true,
        micro_entry_ok: false,
    };
    // SELL against aligned primary+micro UP stays blocked even for the wake path.
    assert.equal(post('SELL', { confirmedWakeEntry: true, context: counterTrend }).action, 'HOLD');
});

test('postprocess: confirmed wake entry still respects base gates', () => {
    const gatesBad = { ...gatesOk, spread_ok: false };
    assert.equal(post('BUY', { confirmedWakeEntry: true, gates: gatesBad }).action, 'HOLD');
});
