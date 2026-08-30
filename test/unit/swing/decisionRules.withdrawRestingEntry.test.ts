import assert from 'node:assert/strict';
import { test } from 'vitest';

import { postprocessDecision } from '../../../lib/swing/decisionRules';
import type { PromptDecisionContext } from '../../../lib/swing/decisionConfig';

// A resting entry SURVIVES evaluations, so silence leaves it standing.
// withdraw_resting_entry is the one way to take it back without trading — and
// it is only meaningful on a flat HOLD: an entry action already supersedes
// whatever rests, and in a position nothing rests.

const context: PromptDecisionContext = {
    micro_bias_calc: 'UP',
    primary_bias: 'up',
    macro_bias: 'UP',
    context_bias: 'UP',
    primary_trend_up: false,
    primary_trend_down: false,
    primary_breakdown_confirmed: false,
    primary_breakout_confirmed: true,
    micro_entry_ok: true,
    aligned_driver_count: 5,
    regime_alignment: 1,
    location_confluence_score: 1,
    micro_extension_atr: 0,
    primary_extension_atr: 0,
    breakout_retest_ok_primary: true,
    breakout_retest_dir_primary: 'up',
};

const gatesOk = { spread_ok: true, liquidity_ok: true, atr_ok: true, slippage_ok: true };

const run = (decision: Record<string, unknown>, positionOpen = false) =>
    postprocessDecision({
        decision,
        context,
        gates: gatesOk,
        positionOpen,
        recentActions: [],
        positionContext: positionOpen ? { side: 'long', hold_minutes: 60 } : null,
    }) as Record<string, unknown>;

test('silence leaves the standing order alone', () => {
    // The whole point of the persistence model: doing nothing is a real choice
    // and must not read as "cancel it".
    assert.equal(run({ action: 'HOLD' }).withdraw_resting_entry, false);
    assert.equal(run({ action: 'HOLD', withdraw_resting_entry: null }).withdraw_resting_entry, false);
});

test('a flat HOLD can withdraw the standing order', () => {
    assert.equal(run({ action: 'HOLD', withdraw_resting_entry: true }).withdraw_resting_entry, true);
});

test('an entry action never withdraws — it supersedes', () => {
    // BUY/SELL already cancels-then-places in the route, so honouring a withdraw
    // here would be a second, contradictory instruction on the same order.
    assert.equal(run({ action: 'BUY', withdraw_resting_entry: true }).withdraw_resting_entry, false);
    assert.equal(run({ action: 'SELL', withdraw_resting_entry: true }).withdraw_resting_entry, false);
});

test('in a position the field is inert — nothing rests', () => {
    assert.equal(run({ action: 'HOLD', withdraw_resting_entry: true }, true).withdraw_resting_entry, false);
    assert.equal(run({ action: 'CLOSE', exit_size_pct: 100, withdraw_resting_entry: true }, true).withdraw_resting_entry, false);
});

test('a withdraw on a HOLD coerced from an entry still applies', () => {
    // The trend guard demotes this BUY to HOLD. The model asked to withdraw and
    // is now flat-HOLD, so the withdraw is honoured on the coerced action rather
    // than silently lost with the entry.
    const demoted = run({
        action: 'BUY',
        withdraw_resting_entry: true,
    });
    assert.equal(demoted.action, 'BUY');
    // ...and when the guard actually fires, the action is HOLD and withdraw holds.
    const guarded = postprocessDecision({
        decision: { action: 'BUY', withdraw_resting_entry: true },
        context: { ...context, primary_trend_down: true, micro_bias_calc: 'DOWN' },
        gates: gatesOk,
        positionOpen: false,
        recentActions: [],
        positionContext: null,
    }) as Record<string, unknown>;
    assert.equal(guarded.action, 'HOLD');
    assert.equal(guarded.withdraw_resting_entry, true);
});
