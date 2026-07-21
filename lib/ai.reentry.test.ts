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

function decide(
    action: 'BUY' | 'SELL',
    lastClosedPosition: Parameters<typeof resolveReentryCooldown>[0],
    context: PromptDecisionContext = openContext,
    sessionOffenseEnabled = true, // reclaim-exception cases pin the day-trade flag ON
) {
    return postprocessDecision({
        decision: { action },
        context,
        gates: gatesOk,
        positionOpen: false,
        recentActions: [],
        positionContext: null,
        policy: 'strict',
        lastClosedPosition,
        sessionOffenseEnabled,
    });
}

// Session-signals variant: only the two reclaim flags matter to the coercion.
const withSignals = (signals: Partial<{ bullishLiquidityReclaim: boolean; bearishLiquidityRejection: boolean }>) =>
    ({
        ...openContext,
        forex_session_context: {
            signals: {
                sweptLastSessionHigh: false,
                sweptLastSessionLow: false,
                sweptPriorDayHigh: false,
                sweptPriorDayLow: false,
                bullishLiquidityReclaim: false,
                bearishLiquidityRejection: false,
                midSessionRange: false,
                ...signals,
            },
        },
    }) as PromptDecisionContext;

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

test('sweep-reclaim exception: bullishLiquidityReclaim lifts the block for a long re-entry only', () => {
    const recentLongClose = { side: 'long' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal(decide('BUY', recentLongClose, withSignals({ bullishLiquidityReclaim: true })).action, 'BUY');
    // The bullish reclaim is not a pass for a blocked SHORT re-entry.
    const recentShortClose = { side: 'short' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal(decide('SELL', recentShortClose, withSignals({ bullishLiquidityReclaim: true })).action, 'HOLD');
});

test('sweep-reclaim exception: bearishLiquidityRejection lifts the block for a short re-entry only', () => {
    const recentShortClose = { side: 'short' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal(decide('SELL', recentShortClose, withSignals({ bearishLiquidityRejection: true })).action, 'SELL');
    const recentLongClose = { side: 'long' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal(decide('BUY', recentLongClose, withSignals({ bearishLiquidityRejection: true })).action, 'HOLD');
});

test('sweep-reclaim exception: absent or all-false signals leave the block intact', () => {
    const recentLongClose = { side: 'long' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal(decide('BUY', recentLongClose, withSignals({})).action, 'HOLD');
});

test('session offense OFF (swing default): a live reclaim signal no longer lifts the block', () => {
    const recentLongClose = { side: 'long' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal(
        decide('BUY', recentLongClose, withSignals({ bullishLiquidityReclaim: true }), false).action,
        'HOLD',
    );
    const recentShortClose = { side: 'short' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal(
        decide('SELL', recentShortClose, withSignals({ bearishLiquidityRejection: true }), false).action,
        'HOLD',
    );
});
