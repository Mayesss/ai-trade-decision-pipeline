import assert from 'node:assert/strict';
import test from 'node:test';

import {
    inScalpEntrySessionProfileWindow,
    isScalpSundayEntryBlocked,
    isScalpSundayForClockMode,
    listScalpEntrySessionProfiles,
    normalizeScalpEntrySessionProfile,
    scalpEntrySessionProfileDistance,
} from '../sessions';

test('entry session profiles include tokyo/berlin/newyork/sydney', () => {
    const profiles = listScalpEntrySessionProfiles();
    assert.deepEqual(profiles, ['tokyo', 'berlin', 'newyork', 'sydney']);
});

test('entry session profile normalization falls back to berlin for unknown values', () => {
    assert.equal(normalizeScalpEntrySessionProfile('london_overlap', 'berlin'), 'berlin');
    assert.equal(normalizeScalpEntrySessionProfile('SYDNEY', 'berlin'), 'sydney');
});

test('entry session profile distance follows configured ordering', () => {
    assert.equal(scalpEntrySessionProfileDistance('berlin', 'berlin'), 0);
    assert.equal(scalpEntrySessionProfileDistance('tokyo', 'berlin'), 1);
    assert.equal(scalpEntrySessionProfileDistance('tokyo', 'newyork'), 2);
});

test('entry session profile windows evaluate timestamps in profile-local timezone', () => {
    const berlinInside = Date.UTC(2026, 0, 5, 8, 30, 0, 0); // 09:30 Berlin (CET)
    const berlinOutside = Date.UTC(2026, 0, 5, 12, 30, 0, 0); // 13:30 Berlin (CET)
    assert.equal(inScalpEntrySessionProfileWindow(berlinInside, 'berlin'), true);
    assert.equal(inScalpEntrySessionProfileWindow(berlinOutside, 'berlin'), false);

    const tokyoInside = Date.UTC(2026, 0, 5, 1, 30, 0, 0); // 10:30 Tokyo (JST)
    const newYorkInside = Date.UTC(2026, 0, 5, 14, 0, 0, 0); // 09:00 New York (EST)
    const sydneyInside = Date.UTC(2026, 0, 4, 22, 0, 0, 0); // 09:00 Sydney (AEDT)
    assert.equal(inScalpEntrySessionProfileWindow(tokyoInside, 'tokyo'), true);
    assert.equal(inScalpEntrySessionProfileWindow(newYorkInside, 'newyork'), true);
    assert.equal(inScalpEntrySessionProfileWindow(sydneyInside, 'sydney'), true);
});

test('sunday gate defaults to enabled and can be turned off via env', () => {
    const prev = process.env.SCALP_BLOCK_SUNDAY_ENTRIES;
    try {
        delete process.env.SCALP_BLOCK_SUNDAY_ENTRIES;
        assert.equal(
            isScalpSundayEntryBlocked({
                nowMs: Date.UTC(2026, 0, 4, 10, 0, 0, 0), // Sunday
                clockMode: 'UTC_FIXED',
            }),
            true,
        );

        process.env.SCALP_BLOCK_SUNDAY_ENTRIES = 'false';
        assert.equal(
            isScalpSundayEntryBlocked({
                nowMs: Date.UTC(2026, 0, 4, 10, 0, 0, 0), // Sunday
                clockMode: 'UTC_FIXED',
            }),
            false,
        );
    } finally {
        if (prev === undefined) delete process.env.SCALP_BLOCK_SUNDAY_ENTRIES;
        else process.env.SCALP_BLOCK_SUNDAY_ENTRIES = prev;
    }
});

test('sunday detection follows configured session clock mode', () => {
    const ts = Date.UTC(2026, 5, 7, 23, 30, 0, 0); // Sunday UTC; Monday in London during DST
    assert.equal(isScalpSundayForClockMode(ts, 'UTC_FIXED'), true);
    assert.equal(isScalpSundayForClockMode(ts, 'LONDON_TZ'), false);
});
