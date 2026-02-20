import assert from 'node:assert/strict';
import test from 'node:test';

import {
    normalizeForexFactoryEventRow,
    parseEventTimestampUtc,
    resolveCountryCurrency,
    shouldWarnCallBudget,
} from './forexFactory';

test('parseEventTimestampUtc parses ForexFactory timestamp into ISO UTC', () => {
    const iso = parseEventTimestampUtc('2026-02-20T09:45:00-05:00');
    assert.equal(iso, '2026-02-20T14:45:00.000Z');
});

test('resolveCountryCurrency maps country aliases to major FX currencies', () => {
    assert.equal(resolveCountryCurrency('United States'), 'USD');
    assert.equal(resolveCountryCurrency('Euro Area'), 'EUR');
    assert.equal(resolveCountryCurrency('Japan'), 'JPY');
});

test('normalizeForexFactoryEventRow returns normalized economic event schema', () => {
    const normalized = normalizeForexFactoryEventRow({
        date: '2026-02-20T09:45:00-05:00',
        country: 'USD',
        title: 'Flash Manufacturing PMI',
        impact: 'High',
        forecast: '52.4',
        previous: '51.9',
    });

    assert.ok(normalized);
    assert.equal(normalized?.currency, 'USD');
    assert.equal(normalized?.impact, 'HIGH');
    assert.equal(normalized?.event_name, 'Flash Manufacturing PMI');
    assert.equal(normalized?.actual, null);
    assert.equal(normalized?.forecast, 52.4);
    assert.equal(normalized?.previous, 51.9);
    assert.equal(normalized?.source, 'forexfactory');
    assert.ok(normalized?.id.length);
});

test('shouldWarnCallBudget warns only above threshold', () => {
    assert.equal(shouldWarnCallBudget(180, 180), false);
    assert.equal(shouldWarnCallBudget(181, 180), true);
});
