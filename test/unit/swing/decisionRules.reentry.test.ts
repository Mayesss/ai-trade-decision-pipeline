import assert from 'node:assert/strict';
import { test } from 'vitest';

import { resolveReentryCooldown } from '../../../lib/swing/signals';
import { postprocessDecision } from '../../../lib/swing/decisionRules';
import type { PromptDecisionContext } from '../../../lib/swing/decisionConfig';

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
    sessionOffenseEnabled = true, // inert while the window is 0; exercised in decisionRules.reentryEnabled
) {
    return postprocessDecision({
        decision: { action },
        context,
        gates: gatesOk,
        positionOpen: false,
        positionContext: null,
        policy: 'strict',
        lastClosedPosition,
        sessionOffenseEnabled,
    });
}

// The re-entry cooldown is OFF by default since 2026-09-02
// (SWING_REENTRY_COOLDOWN_MIN defaults to 0). The mechanism is kept so it can be
// switched back on from env if churn reappears; these tests pin the disabled
// behaviour and the still-pure cooldown resolver. The ENABLED path — the
// coercion itself and both sweep-reclaim exceptions — lives in
// decisionRules.reentryEnabled.test.ts, which must be a separate file because
// REENTRY_COOLDOWN_MIN is captured at import time.
test('resolveReentryCooldown returns null while the window is 0 (default off)', () => {
    const justClosed = { side: 'long' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal(resolveReentryCooldown(justClosed, Date.now()), null);
});

test('a same-direction re-entry right after a close is no longer coerced', () => {
    const justClosed = { side: 'long' as const, exitTsMs: Date.now() - 60_000 };
    assert.equal((decide('BUY', justClosed) as Record<string, unknown>).action, 'BUY');
    assert.equal((decide('SELL', justClosed) as Record<string, unknown>).action, 'SELL');
});

test('no prior close is still a clean entry', () => {
    assert.equal((decide('BUY', null) as Record<string, unknown>).action, 'BUY');
});
