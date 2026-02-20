import assert from 'node:assert/strict';
import test from 'node:test';

import { evaluateForexEventGate, listPairEventMatches, pairCurrencies } from './gate';
import type { NormalizedForexEconomicEvent } from '../types';

const NOW_MS = Date.parse('2026-02-20T12:00:00.000Z');

function eventAt(minutesFromNow: number, currency: string, impact: 'HIGH' | 'MEDIUM' | 'LOW'): NormalizedForexEconomicEvent {
    const ts = new Date(NOW_MS + minutesFromNow * 60_000).toISOString();
    return {
        id: `${currency}-${minutesFromNow}`,
        timestamp_utc: ts,
        currency,
        impact,
        event_name: `${currency} Event`,
        actual: null,
        forecast: null,
        previous: null,
        source: 'forexfactory',
    };
}

test('pairCurrencies extracts major FX currencies from plain pair symbols', () => {
    assert.deepEqual(pairCurrencies('EURUSD'), ['EUR', 'USD']);
    assert.deepEqual(pairCurrencies('gbpjpy'), ['GBP', 'JPY']);
    assert.deepEqual(pairCurrencies('BAD'), []);
});

test('listPairEventMatches matches affected currencies and blocked impacts', () => {
    const events = [eventAt(10, 'USD', 'HIGH'), eventAt(10, 'CAD', 'HIGH'), eventAt(10, 'EUR', 'LOW')];
    const matches = listPairEventMatches({
        pair: 'EURUSD',
        events,
        nowMs: NOW_MS,
        blockedImpacts: ['HIGH'],
    });

    assert.equal(matches.length, 1);
    assert.equal(matches[0]?.event.currency, 'USD');
    assert.equal(matches[0]?.activeWindow, true);
});

test('evaluateForexEventGate blocks entry when active event window exists', () => {
    const events = [eventAt(5, 'USD', 'HIGH')];
    const decision = evaluateForexEventGate({
        pair: 'EURUSD',
        events,
        staleData: false,
        riskState: 'normal',
        nowMs: NOW_MS,
        blockedImpacts: ['HIGH'],
    });

    assert.equal(decision.allowNewEntries, false);
    assert.equal(decision.blockNewEntries, true);
    assert.equal(decision.allowRiskReduction, true);
    assert.ok(decision.reasonCodes.includes('EVENT_WINDOW_ACTIVE_BLOCK'));
    assert.deepEqual(decision.activeImpactLevels, ['HIGH']);
});

test('evaluateForexEventGate reports active impact levels for medium filters', () => {
    const events = [eventAt(5, 'USD', 'MEDIUM')];
    const decision = evaluateForexEventGate({
        pair: 'EURUSD',
        events,
        staleData: false,
        riskState: 'normal',
        nowMs: NOW_MS,
        blockedImpacts: ['MEDIUM'],
    });

    assert.equal(decision.blockNewEntries, true);
    assert.deepEqual(decision.activeImpactLevels, ['MEDIUM']);
});

test('evaluateForexEventGate enforces exact T-30 and T+15 event window boundaries', () => {
    const atMinus30 = eventAt(30, 'USD', 'HIGH');
    const atPlus15 = eventAt(-15, 'USD', 'HIGH');

    const minusBoundary = evaluateForexEventGate({
        pair: 'EURUSD',
        events: [atMinus30],
        staleData: false,
        riskState: 'normal',
        nowMs: NOW_MS,
        blockedImpacts: ['HIGH'],
    });

    const plusBoundary = evaluateForexEventGate({
        pair: 'EURUSD',
        events: [atPlus15],
        staleData: false,
        riskState: 'normal',
        nowMs: NOW_MS,
        blockedImpacts: ['HIGH'],
    });

    assert.equal(minusBoundary.blockNewEntries, true);
    assert.equal(plusBoundary.blockNewEntries, true);
});

test('evaluateForexEventGate stale policy allows normal risk and blocks elevated risk', () => {
    const normalRisk = evaluateForexEventGate({
        pair: 'EURUSD',
        events: [],
        staleData: true,
        riskState: 'normal',
        nowMs: NOW_MS,
        blockedImpacts: ['HIGH'],
    });

    const elevatedRisk = evaluateForexEventGate({
        pair: 'EURUSD',
        events: [],
        staleData: true,
        riskState: 'elevated',
        nowMs: NOW_MS,
        blockedImpacts: ['HIGH'],
    });

    assert.equal(normalRisk.allowNewEntries, true);
    assert.equal(normalRisk.blockNewEntries, false);
    assert.ok(normalRisk.reasonCodes.includes('EVENT_DATA_STALE_ALLOW_NORMAL_RISK'));

    assert.equal(elevatedRisk.allowNewEntries, false);
    assert.equal(elevatedRisk.blockNewEntries, true);
    assert.ok(elevatedRisk.reasonCodes.includes('EVENT_DATA_STALE_BLOCK_NON_NORMAL_RISK'));
});

test('evaluateForexEventGate defaults missing riskState to elevated for stale safety', () => {
    const decision = evaluateForexEventGate({
        pair: 'EURUSD',
        events: [],
        staleData: true,
        nowMs: NOW_MS,
        blockedImpacts: ['HIGH'],
    });

    assert.equal(decision.riskStateApplied, 'elevated');
    assert.equal(decision.allowNewEntries, false);
});
