import assert from 'node:assert/strict';
import test from 'node:test';

import {
    evaluateScalpDeploymentGuardrail,
    resolveScalpLiveGuardrailThresholds,
    type ScalpLiveGuardrailThresholds,
} from '../liveGuardrail';

function thresholds(overrides: Partial<ScalpLiveGuardrailThresholds> = {}): ScalpLiveGuardrailThresholds {
    return {
        minTrades30d: 8,
        minExpectancyR30d: -0.1,
        maxDrawdownR30d: 5,
        maxExpectancyDriftFromForward: 0.15,
        maxTradesPerDay30d: 0.6,
        minForwardProfitableWindowPct: 45,
        autoPause: true,
        ...overrides,
    };
}

test('evaluateScalpDeploymentGuardrail reports soft low-sample breach before hard checks', () => {
    const out = evaluateScalpDeploymentGuardrail(
        {
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
            perf30d: {
                trades: 4,
                wins: 3,
                losses: 1,
                netR: 1,
                expectancyR: 0.25,
                winRatePct: 75,
                maxDrawdownR: 1,
                lastTradeAtMs: Date.now(),
            },
            forwardValidation: null,
        },
        thresholds(),
    );

    assert.equal(out.hardBreachCount, 0);
    assert.equal(out.softBreachCount, 1);
    assert.equal(out.breaches[0]?.code, 'GUARDRAIL_LOW_SAMPLE_30D');
});

test('evaluateScalpDeploymentGuardrail emits hard breaches for expectancy/dd/churn/drift', () => {
    const out = evaluateScalpDeploymentGuardrail(
        {
            deploymentId: 'BTCUSDT~compression_breakout_pullback_m15_m3~default',
            perf30d: {
                trades: 30,
                wins: 8,
                losses: 22,
                netR: -9,
                expectancyR: -0.3,
                winRatePct: 26.6,
                maxDrawdownR: 7,
                lastTradeAtMs: Date.now(),
            },
            forwardValidation: {
                rollCount: 18,
                profitableWindowPct: 40,
                meanExpectancyR: 0.2,
                meanProfitFactor: 1.1,
                maxDrawdownR: 6,
                minTradesPerWindow: 3,
                selectionWindowDays: 90,
                forwardWindowDays: 28,
            },
        },
        thresholds(),
    );

    const codes = new Set(out.breaches.map((row) => row.code));
    assert.equal(out.hardBreachCount >= 4, true);
    assert.equal(codes.has('GUARDRAIL_EXPECTANCY_BELOW_FLOOR_30D'), true);
    assert.equal(codes.has('GUARDRAIL_DRAWDOWN_ABOVE_CAP_30D'), true);
    assert.equal(codes.has('GUARDRAIL_CHURN_TRADES_PER_DAY_ABOVE_CAP_30D'), true);
    assert.equal(codes.has('GUARDRAIL_EXPECTANCY_DRIFT_BELOW_FORWARD_BAND'), true);
    assert.equal(codes.has('GUARDRAIL_FORWARD_PROFITABLE_PCT_BELOW_MIN'), true);
});

test('resolveScalpLiveGuardrailThresholds applies explicit overrides', () => {
    const out = resolveScalpLiveGuardrailThresholds({
        minTrades30d: 12,
        autoPause: false,
    });
    assert.equal(out.minTrades30d, 12);
    assert.equal(out.autoPause, false);
});
