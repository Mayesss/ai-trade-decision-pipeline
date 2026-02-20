import assert from 'node:assert/strict';
import test from 'node:test';

import { normalizeFmpEventRow, parseEventTimestampUtc, resolveCountryCurrency, shouldWarnCallBudget } from './fmp';

test('parseEventTimestampUtc parses FMP-style timestamp into ISO UTC', () => {
    const iso = parseEventTimestampUtc('2026-03-18 13:30:00');
    assert.equal(iso, '2026-03-18T13:30:00.000Z');
});

test('resolveCountryCurrency maps country aliases to major FX currencies', () => {
    assert.equal(resolveCountryCurrency('United States'), 'USD');
    assert.equal(resolveCountryCurrency('Euro Area'), 'EUR');
    assert.equal(resolveCountryCurrency('Japan'), 'JPY');
});

test('normalizeFmpEventRow returns normalized economic event schema', () => {
    const normalized = normalizeFmpEventRow({
        date: '2026-03-18 13:30:00',
        country: 'United States',
        event: 'CPI y/y',
        impact: 'High',
        actual: '3.1',
        estimate: '3.0',
        previous: '2.9',
    });

    assert.ok(normalized);
    assert.equal(normalized?.currency, 'USD');
    assert.equal(normalized?.impact, 'HIGH');
    assert.equal(normalized?.event_name, 'CPI y/y');
    assert.equal(normalized?.actual, 3.1);
    assert.equal(normalized?.forecast, 3.0);
    assert.equal(normalized?.previous, 2.9);
    assert.equal(normalized?.source, 'fmp');
    assert.ok(normalized?.id.length);
});

test('shouldWarnCallBudget warns only above threshold', () => {
    assert.equal(shouldWarnCallBudget(180, 180), false);
    assert.equal(shouldWarnCallBudget(181, 180), true);
});
