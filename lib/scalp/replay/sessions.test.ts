import assert from 'node:assert/strict';
import test from 'node:test';

import {
    inScalpEntrySessionProfileWindow,
    listScalpEntrySessionProfiles,
    normalizeScalpEntrySessionProfile,
    scalpEntrySessionProfileDistance,
} from '../sessions';

test('entry session profiles include tokyo/berlin/newyork and exclude sydney', () => {
    const profiles = listScalpEntrySessionProfiles();
    assert.deepEqual(profiles, ['tokyo', 'berlin', 'newyork']);
    assert.ok(!profiles.includes('sydney' as any));
});

test('entry session profile normalization falls back to berlin for unknown values', () => {
    assert.equal(normalizeScalpEntrySessionProfile('london_overlap', 'berlin'), 'berlin');
    assert.equal(normalizeScalpEntrySessionProfile('SYDNEY', 'berlin'), 'berlin');
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
    assert.equal(inScalpEntrySessionProfileWindow(tokyoInside, 'tokyo'), true);
    assert.equal(inScalpEntrySessionProfileWindow(newYorkInside, 'newyork'), true);
});
