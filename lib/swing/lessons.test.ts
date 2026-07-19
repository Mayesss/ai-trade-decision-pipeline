import assert from 'node:assert/strict';
import test from 'node:test';

import type { SwingLessonRow } from './pg';
import { MAX_PROMPT_LESSONS, resolveLessonDecision, selectPromptLessons } from './lessons';

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

test('resolveLessonDecision: new lesson with text → add with scope/confidence', () => {
    const d = resolveLessonDecision(
        { lesson_action: 'new', lesson: 'Do X.', lesson_scope: 'asset_class', confidence: 0.7 },
        [],
    );
    assert.deepEqual(d, { kind: 'add', scope: 'asset_class', text: 'Do X.', confidence: 0.7 });
});

test('resolveLessonDecision: reinforce with valid id merges; confidence never drops', () => {
    const shown = [row({ id: 7, lesson: 'Existing wording.', confidence: 0.8 })];
    const withText = resolveLessonDecision(
        { lesson_action: 'reinforce', reinforce_lesson_id: 7, lesson: 'Reworded.', confidence: 0.6 },
        shown,
    );
    assert.deepEqual(withText, { kind: 'merge', targetId: 7, text: 'Reworded.', confidence: 0.8 });
    const withoutText = resolveLessonDecision(
        { lesson_action: 'reinforce', reinforce_lesson_id: 7, lesson: null, confidence: 0.9 },
        shown,
    );
    assert.deepEqual(withoutText, { kind: 'merge', targetId: 7, text: 'Existing wording.', confidence: 0.9 });
});

test('resolveLessonDecision: reinforce with hallucinated id degrades to add (text) or none (no text)', () => {
    const withText = resolveLessonDecision(
        { lesson_action: 'reinforce', reinforce_lesson_id: 999, lesson: 'Do Y.', lesson_scope: 'global', confidence: 0.5 },
        [row({ id: 7 })],
    );
    assert.equal(withText.kind, 'add');
    const withoutText = resolveLessonDecision(
        { lesson_action: 'reinforce', reinforce_lesson_id: 999, lesson: null },
        [row({ id: 7 })],
    );
    assert.equal(withoutText.kind, 'none');
});

test('resolveLessonDecision: none / missing text / bad scope defaults', () => {
    assert.equal(resolveLessonDecision({ lesson_action: 'none', lesson: 'ignored' }, []).kind, 'none');
    assert.equal(resolveLessonDecision({ lesson_action: 'new', lesson: '   ' }, []).kind, 'none');
    const badScope = resolveLessonDecision({ lesson_action: 'new', lesson: 'Do Z.', lesson_scope: 'universe' }, []);
    assert.deepEqual(badScope, { kind: 'add', scope: 'symbol', text: 'Do Z.', confidence: 0.5 });
});
