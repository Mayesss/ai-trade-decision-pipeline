import assert from 'node:assert/strict';
import test from 'node:test';

import { postprocessDecision, resolveReentryCooldown, REENTRY_COOLDOWN_MIN } from './ai';
import type { PromptDecisionContext } from './ai';

// A context that leaves BUY/SELL untouched by the other hard constraints, so the
// tests isolate the re-entry cooldown coercion.
const openContext: PromptDecisionContext = {
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

function decide(action: 'BUY' | 'SELL', lastClosedPosition: Parameters<typeof resolveReentryCooldown>[0]) {
    return postprocessDecision({
        decision: { action },
        context: openContext,
        gates: gatesOk,
        positionOpen: false,
        recentActions: [],
        positionContext: null,
        policy: 'strict',
        lastClosedPosition,
    });
}

test('resolveReentryCooldown: active inside the window, inactive after it', () => {
    const now = Date.now();
    const inside = resolveReentryCooldown({ side: 'long', exitTsMs: now - 60_000 }, now);
    assert.equal(inside?.blockedSide, 'long');
    assert.ok((inside?.minutesLeft ?? 0) > 0 && (inside?.minutesLeft ?? 0) <= REENTRY_COOLDOWN_MIN);

    const after = resolveReentryCooldown({ side: 'long', exitTsMs: now - (REENTRY_COOLDOWN_MIN + 1) * 60_000 }, now);
    assert.equal(after, null);

    assert.equal(resolveReentryCooldown(null, now), null);
});

test('postprocessDecision: same-direction re-entry inside cooldown is coerced to HOLD', () => {
    const recentLongClose = { side: 'long' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal(decide('BUY', recentLongClose).action, 'HOLD');
    // opposite direction stays allowed — a reversal thesis is a new trade
    assert.equal(decide('SELL', recentLongClose).action, 'SELL');
});

test('postprocessDecision: entries pass once the cooldown has expired or with no prior close', () => {
    const staleClose = { side: 'long' as const, exitTsMs: Date.now() - (REENTRY_COOLDOWN_MIN + 5) * 60_000 };
    assert.equal(decide('BUY', staleClose).action, 'BUY');
    assert.equal(decide('BUY', null).action, 'BUY');
});
