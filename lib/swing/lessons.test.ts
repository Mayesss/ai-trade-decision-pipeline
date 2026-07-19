import assert from 'node:assert/strict';
import test from 'node:test';

import type { SwingLessonRow } from './pg';
import { MAX_PROMPT_LESSONS, selectPromptLessons } from './lessons';

const row = (extra: Partial<SwingLessonRow>): SwingLessonRow => ({
    id: 1,
    scope: 'global',
    symbol: null,
    assetClass: null,
    lesson: 'Some lesson.',
    confidence: 0.5,
    supportCount: 1,
    sourcePostmortemIds: [1],
    status: 'active',
    updatedAtMs: 0,
    ...extra,
});

test('selectPromptLessons: confidence-sorted, support/recency tiebreaks, capped', () => {
    const rows = [
        row({ id: 1, confidence: 0.6, supportCount: 1, updatedAtMs: 100 }),
        row({ id: 2, confidence: 0.9, lesson: 'Top lesson.' }),
        row({ id: 3, confidence: 0.6, supportCount: 3, updatedAtMs: 50, lesson: 'Backed lesson.' }),
        row({ id: 4, confidence: 0.6, supportCount: 1, updatedAtMs: 200, lesson: 'Newer lesson.' }),
    ];
    const picked = selectPromptLessons(rows, 3);
    assert.equal(picked.length, 3);
    assert.equal(picked[0].lesson, 'Top lesson.'); // highest confidence
    assert.equal(picked[1].lesson, 'Backed lesson.'); // support_count beats recency
    assert.equal(picked[2].lesson, 'Newer lesson.'); // recency last tiebreak
});

test('selectPromptLessons: retired and empty lessons excluded; default cap holds', () => {
    const rows = [
        row({ id: 1, status: 'retired', confidence: 1 }),
        row({ id: 2, lesson: '   ' }),
        ...Array.from({ length: 10 }, (_, i) =>
            row({ id: 10 + i, confidence: 0.5 + i / 100, lesson: `L${i}` }),
        ),
    ];
    const picked = selectPromptLessons(rows);
    assert.equal(picked.length, MAX_PROMPT_LESSONS);
    assert.ok(picked.every((p) => p.lesson.startsWith('L')));
    assert.equal(picked[0].lesson, 'L9');
});

test('selectPromptLessons: carries scope for the [scope] prompt tag', () => {
    const picked = selectPromptLessons([
        row({ id: 1, scope: 'symbol', symbol: 'ETHUSDT', lesson: 'Symbol quirk.' }),
    ]);
    assert.deepEqual(picked, [{ scope: 'symbol', lesson: 'Symbol quirk.' }]);
});
