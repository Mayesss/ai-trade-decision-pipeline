import assert from 'node:assert/strict';
import test from 'node:test';

import { postprocessDecision } from './ai';
import type { PromptDecisionContext } from './ai';

// Margin-recycle field coercion in postprocessDecision. The flag is read at
// CALL time (process.env.ENABLE_CRYPTO_MARGIN_RECYCLE), so tests toggle it
// around each call. Execution owns the [current, symbol max] clamp — here we
// only assert the action/flag gating and type coercion.

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

function decide(decision: Record<string, unknown>, flagOn: boolean) {
    const prev = process.env.ENABLE_CRYPTO_MARGIN_RECYCLE;
    process.env.ENABLE_CRYPTO_MARGIN_RECYCLE = flagOn ? 'true' : 'false';
    try {
        return postprocessDecision({
            decision,
            context: openContext,
            gates: gatesOk,
            positionOpen: true,
            recentActions: [],
            positionContext: null,
            policy: 'balanced',
        });
    } finally {
        if (prev === undefined) delete process.env.ENABLE_CRYPTO_MARGIN_RECYCLE;
        else process.env.ENABLE_CRYPTO_MARGIN_RECYCLE = prev;
    }
}

test('flag off: manage fields coerce to null/false even on an eligible HOLD', () => {
    const out = decide({ action: 'HOLD', raise_leverage_to: 25, move_stop_to_be: true }, false);
    assert.equal(out.raise_leverage_to, null);
    assert.equal(out.move_stop_to_be, false);
});

test('flag on + HOLD: raise passes through rounded, move_stop_to_be honored', () => {
    const out = decide({ action: 'HOLD', raise_leverage_to: 24.6, move_stop_to_be: true }, true);
    assert.equal(out.raise_leverage_to, 25);
    assert.equal(out.move_stop_to_be, true);
});

test('flag on + partial CLOSE: eligible (the trim maneuver decision shape)', () => {
    const out = decide(
        { action: 'CLOSE', exit_size_pct: 40, raise_leverage_to: 30, move_stop_to_be: true },
        true,
    );
    assert.equal(out.action, 'CLOSE');
    assert.equal(out.raise_leverage_to, 30);
    assert.equal(out.move_stop_to_be, true);
});

test('flag on + full CLOSE: nothing left to manage, fields nulled', () => {
    const out = decide(
        { action: 'CLOSE', exit_size_pct: 100, raise_leverage_to: 30, move_stop_to_be: true },
        true,
    );
    assert.equal(out.raise_leverage_to, null);
    assert.equal(out.move_stop_to_be, false);
});

test('flag on + non-positive or non-numeric raise coerces to null', () => {
    assert.equal(decide({ action: 'HOLD', raise_leverage_to: 0 }, true).raise_leverage_to, null);
    assert.equal(decide({ action: 'HOLD', raise_leverage_to: 'max' }, true).raise_leverage_to, null);
});
