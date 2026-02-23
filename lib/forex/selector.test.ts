import assert from 'node:assert/strict';
import test from 'node:test';

import { evaluatePairEligibility, isWithinSelectorTopPercentile, selectorTopRankCutoff } from './selector';
import type { NormalizedForexEconomicEvent } from './types';

const baseMetrics = {
    pair: 'EURUSD',
    epic: 'EURUSD',
    sessionTag: 'LONDON',
    price: 1.09,
    spreadAbs: 0.0002,
    spreadPips: 2,
    spreadToAtr1h: 0.08,
    atr1h: 0.0025,
    atr4h: 0.005,
    atr1hPercent: 0.002,
    trendStrength: 0.9,
    chopScore: 0.25,
    shockFlag: false,
    timestampMs: Date.now(),
} as const;

function eventAt(iso: string, currency = 'USD'): NormalizedForexEconomicEvent {
    return {
        id: `${currency}-${iso}`,
        timestamp_utc: iso,
        currency,
        impact: 'HIGH',
        event_name: 'Test Event',
        actual: null,
        forecast: null,
        previous: null,
        source: 'forexfactory',
    };
}

test('evaluatePairEligibility returns eligible pair with good metrics', () => {
    const out = evaluatePairEligibility({
        pair: 'EURUSD',
        metrics: { ...baseMetrics },
        staleEvents: false,
        events: [],
    });

    assert.equal(out.eligible, true);
    assert.ok(out.score > 0);
});

test('evaluatePairEligibility blocks pair when spread_to_atr is too high', () => {
    const out = evaluatePairEligibility({
        pair: 'EURUSD',
        metrics: { ...baseMetrics, spreadToAtr1h: 0.5 },
        staleEvents: false,
        events: [],
    });

    assert.equal(out.eligible, false);
    assert.ok(out.reasons.includes('SPREAD_TO_ATR_TOO_HIGH'));
});

test('evaluatePairEligibility blocks on active high-impact event window', () => {
    const nowIso = new Date(Date.now() + 10 * 60_000).toISOString();
    const out = evaluatePairEligibility({
        pair: 'EURUSD',
        metrics: { ...baseMetrics },
        staleEvents: false,
        events: [eventAt(nowIso, 'USD')],
    });

    assert.equal(out.eligible, false);
    assert.ok(out.reasons.includes('EVENT_WINDOW_ACTIVE_BLOCK'));
});

test('evaluatePairEligibility applies stricter spread-to-ATR cap around session transitions', () => {
    const transitionNowMs = Date.UTC(2026, 1, 23, 7, 0, 0);
    const out = evaluatePairEligibility({
        pair: 'EURUSD',
        metrics: { ...baseMetrics, spreadToAtr1h: 0.1 },
        staleEvents: false,
        events: [],
        nowMs: transitionNowMs,
    });

    assert.equal(out.eligible, false);
    assert.ok(out.reasons.includes('SESSION_TRANSITION_SPREAD_STRESS'));
    assert.ok(out.reasons.includes('SPREAD_TO_ATR_TRANSITION_CAP_EXCEEDED'));
});

test('evaluatePairEligibility keeps pair eligible outside session transition buffer when base cap is respected', () => {
    const normalNowMs = Date.UTC(2026, 1, 23, 9, 0, 0);
    const out = evaluatePairEligibility({
        pair: 'EURUSD',
        metrics: { ...baseMetrics, spreadToAtr1h: 0.1 },
        staleEvents: false,
        events: [],
        nowMs: normalNowMs,
    });

    assert.equal(out.eligible, true);
    assert.ok(!out.reasons.includes('SESSION_TRANSITION_SPREAD_STRESS'));
});

test('selector top percentile gate keeps only top configured slice', () => {
    assert.equal(selectorTopRankCutoff(10, 40), 4);
    assert.equal(
        isWithinSelectorTopPercentile({
            rank: 4,
            totalRows: 10,
            topPercent: 40,
        }),
        true,
    );
    assert.equal(
        isWithinSelectorTopPercentile({
            rank: 5,
            totalRows: 10,
            topPercent: 40,
        }),
        false,
    );
});
