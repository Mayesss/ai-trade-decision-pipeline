import assert from 'node:assert/strict';
import { test } from 'vitest';

import { postprocessDecision } from '../../../lib/swing/decisionRules';
import type { PromptDecisionContext } from '../../../lib/swing/decisionConfig';

// A trim asked for as HOLD. The venue only trims on a partial CLOSE, but the
// model reliably sets exit_size_pct while labelling the action HOLD; that
// request used to be nulled by the CLOSE/REVERSE-only exit_size_pct rule and
// the trim vanished silently (2026-09-07: a 40% ADAUSDT and a 35% BTCUSDT trim
// dropped a minute apart). postprocessDecision now relabels it as the partial
// CLOSE it describes, and marks the rewrite.

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

function decide(decision: Record<string, unknown>, positionOpen = true) {
    return postprocessDecision({
        decision,
        context: openContext,
        gates: gatesOk,
        positionOpen,
        positionContext: null,
        policy: 'balanced',
    });
}

test('HOLD carrying exit_size_pct becomes a partial CLOSE, marked as coerced', () => {
    const out = decide({ action: 'HOLD', exit_size_pct: 40 });
    assert.equal(out.action, 'CLOSE');
    assert.equal(out.exit_size_pct, 40);
    assert.equal((out as Record<string, unknown>).trim_coerced_from_hold, true);
});

test('an explicit partial CLOSE is untouched and NOT marked as coerced', () => {
    const out = decide({ action: 'CLOSE', exit_size_pct: 40 });
    assert.equal(out.action, 'CLOSE');
    assert.equal(out.exit_size_pct, 40);
    assert.equal((out as Record<string, unknown>).trim_coerced_from_hold, undefined);
});

test('a plain HOLD stays HOLD', () => {
    const out = decide({ action: 'HOLD' });
    assert.equal(out.action, 'HOLD');
    assert.equal(out.exit_size_pct, null);
    assert.equal((out as Record<string, unknown>).trim_coerced_from_hold, undefined);
});

test('exit_size_pct 100 on a HOLD is NOT coerced, but IS recorded', () => {
    const out = decide({ action: 'HOLD', exit_size_pct: 100 });
    assert.equal(out.action, 'HOLD');
    assert.equal(out.exit_size_pct, null);
    assert.equal((out as Record<string, unknown>).trim_dropped, '100');
});

test('a nonsensical exit_size_pct on a HOLD is ignored, and recorded rather than silent', () => {
    for (const pct of [0, -10, 140, Number.NaN, 'forty']) {
        const out = decide({ action: 'HOLD', exit_size_pct: pct });
        assert.equal(out.action, 'HOLD', `pct=${String(pct)} must not trim`);
        assert.equal(
            typeof (out as Record<string, unknown>).trim_dropped,
            'string',
            `pct=${String(pct)} must be recorded`,
        );
    }
});

test('a plain HOLD with no trim field records nothing', () => {
    const out = decide({ action: 'HOLD', exit_size_pct: null });
    assert.equal((out as Record<string, unknown>).trim_dropped, undefined);
});

test('flat: exit_size_pct on a HOLD never invents an exit with no position', () => {
    const out = decide({ action: 'HOLD', exit_size_pct: 40 }, false);
    assert.equal(out.action, 'HOLD');
    assert.equal(out.exit_size_pct, null);
});
