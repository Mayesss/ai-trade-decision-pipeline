import assert from 'node:assert/strict';
import test from 'node:test';

import {
    computePositionProgress,
    isPacketStale,
    packetAgeMinutes,
    resolveReentryLockMinutes,
    shouldTimeStopMaxHold,
    shouldTimeStopNoFollowThrough,
} from './engine';
import type { ForexRegimePacket } from './types';

const basePacket: ForexRegimePacket = {
    pair: 'EURUSD',
    generatedAtMs: Date.UTC(2026, 1, 20, 10, 0, 0),
    regime: 'trend_up',
    permission: 'long_only',
    allowed_modules: ['pullback'],
    risk_state: 'normal',
    confidence: 0.7,
    htf_context: {
        nearest_support: 1.09,
        nearest_resistance: 1.11,
        distance_to_support_atr1h: 0.5,
        distance_to_resistance_atr1h: 0.8,
    },
    notes_codes: [],
};

test('packet stale gate flips only after threshold', () => {
    const nowAtThreshold = basePacket.generatedAtMs + 120 * 60_000;
    const nowAfterThreshold = nowAtThreshold + 1;
    assert.equal(packetAgeMinutes(basePacket, nowAtThreshold), 120);
    assert.equal(isPacketStale(basePacket, nowAtThreshold, 120), false);
    assert.equal(isPacketStale(basePacket, nowAfterThreshold, 120), true);
});

test('computePositionProgress returns deterministic age bars and MFE in R', () => {
    const openedAtMs = Date.UTC(2026, 1, 20, 10, 0, 0);
    const nowMs = openedAtMs + 18 * 5 * 60_000;
    const candles = Array.from({ length: 19 }).map((_, i) => {
        const ts = openedAtMs + i * 5 * 60_000;
        const high = 1.1000 + i * 0.0001;
        const low = 1.0990 - i * 0.00002;
        return [ts, 1.1, high, low, 1.1, 100];
    });

    const out = computePositionProgress({
        side: 'BUY',
        entryPrice: 1.1,
        initialStopPrice: 1.095,
        openedAtMs,
        nowMs,
        candles5m: candles,
    });

    assert.equal(out.ageBars5m, 18);
    assert.equal(Math.round(out.rValue * 1_000_000), 5000);
    assert.ok(out.mfeR > 0);
});

test('no-follow-through time-stop triggers exactly at threshold when MFE < min R', () => {
    assert.equal(
        shouldTimeStopNoFollowThrough({
            ageBars5m: 17,
            mfeR: 0.2,
            thresholdBars: 18,
            minFollowR: 0.3,
        }),
        false,
    );
    assert.equal(
        shouldTimeStopNoFollowThrough({
            ageBars5m: 18,
            mfeR: 0.2999,
            thresholdBars: 18,
            minFollowR: 0.3,
        }),
        true,
    );
});

test('max-hold time-stop respects trend+trailing exception', () => {
    assert.equal(
        shouldTimeStopMaxHold({
            ageHours: 10,
            maxHoldHours: 10,
            trendAligned: true,
            trailingActive: true,
        }),
        false,
    );
    assert.equal(
        shouldTimeStopMaxHold({
            ageHours: 10,
            maxHoldHours: 10,
            trendAligned: false,
            trailingActive: true,
        }),
        true,
    );
});

test('resolveReentryLockMinutes maps lock duration by exit context', () => {
    const reentry = {
        lockMinutes: 5,
        lockMinutesTimeStop: 5,
        lockMinutesRegimeFlip: 10,
        lockMinutesEventRisk: 20,
        lockMinutesStopInvalidated: 0,
        lockMinutesStopInvalidatedStress: 0,
    };

    assert.equal(
        resolveReentryLockMinutes({
            reasonCodes: ['EVENT_HIGH_FORCE_CLOSE'],
            reentry,
            executeMinutes: 5,
        }),
        20,
    );
    assert.equal(
        resolveReentryLockMinutes({
            reasonCodes: ['REGIME_FLIP_CLOSE'],
            reentry,
            executeMinutes: 5,
        }),
        10,
    );
    assert.equal(
        resolveReentryLockMinutes({
            reasonCodes: ['CLOSE_TIME_STOP_NO_PROGRESS'],
            reentry,
            executeMinutes: 5,
        }),
        5,
    );
    assert.equal(
        resolveReentryLockMinutes({
            reasonCodes: ['STOP_INVALIDATED_LONG'],
            reentry,
            executeMinutes: 5,
        }),
        null,
    );

    assert.equal(
        resolveReentryLockMinutes({
            reasonCodes: ['STOP_INVALIDATED_SHORT'],
            reentry: {
                ...reentry,
                lockMinutesStopInvalidated: 30,
                lockMinutesStopInvalidatedStress: 60,
            },
            executeMinutes: 5,
        }),
        30,
    );
    assert.equal(
        resolveReentryLockMinutes({
            reasonCodes: ['STOP_INVALIDATED_SHORT'],
            reentry: {
                ...reentry,
                lockMinutesStopInvalidated: 30,
                lockMinutesStopInvalidatedStress: 60,
            },
            executeMinutes: 5,
            stopInvalidationStressActive: true,
        }),
        60,
    );
});
