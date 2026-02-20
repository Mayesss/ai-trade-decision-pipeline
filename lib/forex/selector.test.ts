import assert from 'node:assert/strict';
import test from 'node:test';

import { evaluatePairEligibility } from './selector';
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
