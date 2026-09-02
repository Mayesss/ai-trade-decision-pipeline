// The re-entry cooldown defaults to OFF (SWING_REENTRY_COOLDOWN_MIN = 0 since
// 2026-09-02), so decisionRules.reentry.test.ts pins the DISABLED behaviour.
// This file pins the mechanism as it behaves when switched back ON — the state
// the restore note in decisionConfig.ts points at.
//
// REENTRY_COOLDOWN_MIN is captured into a module-level const at import time, so
// the env has to be set BEFORE the modules load: vi.stubEnv in a test body is
// too late. Hence a dedicated file (the same module-state trap the
// swing-fixtures skill warns about) and a dynamic import after the stub.

import assert from 'node:assert/strict';
import { beforeAll, test, vi } from 'vitest';

import type { PromptDecisionContext } from '../../../lib/swing/decisionConfig';

const COOLDOWN_MIN = 240;

let postprocessDecision: typeof import('../../../lib/swing/decisionRules').postprocessDecision;
let resolveReentryCooldown: typeof import('../../../lib/swing/signals').resolveReentryCooldown;
let REENTRY_COOLDOWN_MIN: number;

beforeAll(async () => {
    vi.stubEnv('SWING_REENTRY_COOLDOWN_MIN', String(COOLDOWN_MIN));
    vi.resetModules();
    ({ postprocessDecision } = await import('../../../lib/swing/decisionRules'));
    ({ resolveReentryCooldown } = await import('../../../lib/swing/signals'));
    ({ REENTRY_COOLDOWN_MIN } = await import('../../../lib/swing/decisionConfig'));
    // Guard the whole file: if the stub ever stops reaching the module constant
    // these tests would silently pass against the disabled path instead.
    assert.equal(REENTRY_COOLDOWN_MIN, COOLDOWN_MIN);
});

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

const decide = (
    action: 'BUY' | 'SELL',
    lastClosedPosition: { side: 'long' | 'short'; exitTsMs: number } | null,
    context: PromptDecisionContext = openContext,
    sessionOffenseEnabled = true, // reclaim-exception cases pin the day-trade flag ON
) =>
    postprocessDecision({
        decision: { action },
        context,
        gates: gatesOk,
        positionOpen: false,
        positionContext: null,
        policy: 'strict',
        lastClosedPosition,
        sessionOffenseEnabled,
    }) as Record<string, unknown>;

const justClosed = (side: 'long' | 'short') => ({ side, exitTsMs: Date.now() - 60_000 });

test('resolveReentryCooldown: active inside the window, inactive after it', () => {
    const now = Date.now();
    const inside = resolveReentryCooldown({ side: 'long', exitTsMs: now - 60_000 }, now);
    assert.equal(inside?.blockedSide, 'long');
    assert.ok((inside?.minutesLeft ?? 0) > 0 && (inside?.minutesLeft ?? 0) <= COOLDOWN_MIN);

    const after = resolveReentryCooldown({ side: 'long', exitTsMs: now - (COOLDOWN_MIN + 1) * 60_000 }, now);
    assert.equal(after, null);
    assert.equal(resolveReentryCooldown(null, now), null);
});

test('same-direction re-entry inside the cooldown is coerced to HOLD', () => {
    assert.equal(decide('BUY', justClosed('long')).action, 'HOLD');
    // Opposite direction stays allowed — a reversal thesis is a new trade.
    assert.equal(decide('SELL', justClosed('long')).action, 'SELL');
});

test('entries pass once the cooldown has expired, or with no prior close', () => {
    const stale = { side: 'long' as const, exitTsMs: Date.now() - (COOLDOWN_MIN + 5) * 60_000 };
    assert.equal(decide('BUY', stale).action, 'BUY');
    assert.equal(decide('BUY', null).action, 'BUY');
});

test('sweep-reclaim exception lifts the block for the matching side only', () => {
    assert.equal(decide('BUY', justClosed('long'), withSignals({ bullishLiquidityReclaim: true })).action, 'BUY');
    assert.equal(decide('SELL', justClosed('short'), withSignals({ bullishLiquidityReclaim: true })).action, 'HOLD');

    assert.equal(decide('SELL', justClosed('short'), withSignals({ bearishLiquidityRejection: true })).action, 'SELL');
    assert.equal(decide('BUY', justClosed('long'), withSignals({ bearishLiquidityRejection: true })).action, 'HOLD');
});

test('absent or all-false signals leave the block intact', () => {
    assert.equal(decide('BUY', justClosed('long'), withSignals({})).action, 'HOLD');
});

test('session offense OFF (the swing default): a live reclaim no longer lifts the block', () => {
    assert.equal(decide('BUY', justClosed('long'), withSignals({ bullishLiquidityReclaim: true }), false).action, 'HOLD');
    assert.equal(
        decide('SELL', justClosed('short'), withSignals({ bearishLiquidityRejection: true }), false).action,
        'HOLD',
    );
});
