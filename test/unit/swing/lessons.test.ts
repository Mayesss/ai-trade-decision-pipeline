import assert from 'node:assert/strict';
import { test } from 'vitest';

import type { SwingLessonRow } from '../../../lib/swing/pg';
import {
    lessonOriginLabel,
    PROMPT_LESSON_SCOPE_CAPS,
    promotedScopeOnReinforce,
    resolveLessonDecision,
    selectPromptLessons,
} from '../../../lib/swing/lessons';

const row = (extra: Partial<SwingLessonRow>): SwingLessonRow => ({
    id: 1,
    scope: 'global',
    symbol: null,
    assetClass: null,
    lesson: 'Some lesson.',
    confidence: 0.5,
    supportCount: 1,
    sourcePostmortemIds: [1],
    originCounts: {},
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
    const picked = selectPromptLessons(rows, { global: 3 });
    assert.equal(picked.length, 3);
    assert.equal(picked[0].lesson, 'Top lesson.'); // highest confidence
    assert.equal(picked[1].lesson, 'Backed lesson.'); // support_count beats recency
    assert.equal(picked[2].lesson, 'Newer lesson.'); // recency last tiebreak
});

test('selectPromptLessons: retired and empty lessons excluded; per-scope default caps hold', () => {
    const cap = PROMPT_LESSON_SCOPE_CAPS.global;
    const rows = [
        row({ id: 1, status: 'retired', confidence: 1 }),
        row({ id: 2, lesson: '   ' }),
        ...Array.from({ length: cap + 2 }, (_, i) =>
            row({ id: 10 + i, confidence: 0.5 + i / 100, lesson: `L${i}` }),
        ),
    ];
    const picked = selectPromptLessons(rows);
    assert.equal(picked.length, cap);
    assert.ok(picked.every((p) => p.lesson.startsWith('L')));
    assert.equal(picked[0].lesson, `L${cap + 1}`);
});

test('selectPromptLessons: scope buckets are independent and ordered symbol-first', () => {
    const symCap = PROMPT_LESSON_SCOPE_CAPS.symbol;
    const rows = [
        // Fill global beyond its cap with max-confidence rows...
        ...Array.from({ length: PROMPT_LESSON_SCOPE_CAPS.global + 3 }, (_, i) =>
            row({ id: 100 + i, scope: 'global', confidence: 1, lesson: `G${i}` }),
        ),
        // ...they must not crowd out low-confidence narrow-scope lessons.
        ...Array.from({ length: symCap + 1 }, (_, i) =>
            row({ id: 200 + i, scope: 'symbol', symbol: 'ETHUSDT', confidence: 0.1 + i / 100, lesson: `S${i}` }),
        ),
        row({ id: 300, scope: 'asset_class', assetClass: 'crypto', confidence: 0.2, lesson: 'A0' }),
    ];
    const picked = selectPromptLessons(rows);
    assert.equal(picked.length, symCap + 1 + PROMPT_LESSON_SCOPE_CAPS.global);
    // Most specific scope renders first, capped at its own bucket size.
    assert.deepEqual(
        picked.slice(0, symCap).map((p) => p.scope),
        Array(symCap).fill('symbol'),
    );
    assert.equal(picked[0].lesson, `S${symCap}`); // best symbol lesson leads
    assert.equal(picked[symCap].scope, 'asset_class');
    assert.ok(picked.slice(symCap + 1).every((p) => p.scope === 'global'));
});

test('selectPromptLessons: carries scope + origin label for the prompt tag', () => {
    const picked = selectPromptLessons([
        row({ id: 1, scope: 'symbol', symbol: 'ETHUSDT', lesson: 'Symbol quirk.' }),
    ]);
    assert.deepEqual(picked, [{ scope: 'symbol', lesson: 'Symbol quirk.', originLabel: null }]);
});

test('resolveLessonDecision: bad_luck verdict never yields a lesson, even if the analyst emits one', () => {
    const shown = [row({ id: 7, lesson: 'Existing wording.', confidence: 0.8 })];
    const asNew = resolveLessonDecision(
        { verdict: 'bad_luck', lesson_action: 'new', lesson: 'Do X.', lesson_scope: 'global', confidence: 0.9 },
        [],
    );
    assert.deepEqual(asNew, { kind: 'none', reason: 'bad_luck_no_lesson' });
    const asReinforce = resolveLessonDecision(
        { verdict: 'bad_luck', lesson_action: 'reinforce', reinforce_lesson_id: 7, confidence: 0.9 },
        shown,
    );
    assert.deepEqual(asReinforce, { kind: 'none', reason: 'bad_luck_no_lesson' });
});

test('resolveLessonDecision: new lessons ALWAYS enter at symbol scope (analyst scope is advisory)', () => {
    for (const advisory of ['asset_class', 'global', 'symbol']) {
        const d = resolveLessonDecision(
            { lesson_action: 'new', lesson: 'Do X.', lesson_scope: advisory, confidence: 0.7 },
            [],
        );
        assert.deepEqual(d, { kind: 'add', scope: 'symbol', text: 'Do X.', confidence: 0.7 });
    }
});

test('resolveLessonDecision: reinforce with valid id merges; confidence never drops; target facts carried', () => {
    const shown = [
        row({ id: 7, scope: 'symbol', symbol: 'ETHUSDT', assetClass: 'crypto', lesson: 'Existing wording.', confidence: 0.8 }),
    ];
    const withText = resolveLessonDecision(
        { lesson_action: 'reinforce', reinforce_lesson_id: 7, lesson: 'Reworded.', confidence: 0.6 },
        shown,
    );
    assert.deepEqual(withText, {
        kind: 'merge',
        targetId: 7,
        text: 'Reworded.',
        confidence: 0.8,
        targetScope: 'symbol',
        targetSymbol: 'ETHUSDT',
        targetAssetClass: 'crypto',
    });
    const withoutText = resolveLessonDecision(
        { lesson_action: 'reinforce', reinforce_lesson_id: 7, lesson: null, confidence: 0.9 },
        shown,
    );
    assert.equal(withoutText.kind, 'merge');
    assert.equal((withoutText as any).text, 'Existing wording.');
    assert.equal((withoutText as any).confidence, 0.9);
});

test('resolveLessonDecision: revise rewrites a SHOWN lesson (scope move optional); unshown target → none', () => {
    const shown = [row({ id: 5, scope: 'global', lesson: 'Old universal veto.', confidence: 0.9 })];
    const revised = resolveLessonDecision(
        { lesson_action: 'revise', reinforce_lesson_id: 5, lesson: 'Bounded version.', lesson_scope: 'symbol', confidence: 0.5 },
        shown,
    );
    assert.deepEqual(revised, { kind: 'revise', targetId: 5, text: 'Bounded version.', confidence: 0.5, scope: 'symbol' });
    const noScope = resolveLessonDecision(
        { lesson_action: 'revise', reinforce_lesson_id: 5, lesson: 'Bounded version.', confidence: 0.5 },
        shown,
    );
    assert.equal((noScope as any).scope, null);
    assert.equal(
        resolveLessonDecision({ lesson_action: 'revise', reinforce_lesson_id: 99, lesson: 'X.' }, shown).kind,
        'none',
    );
    assert.equal(
        resolveLessonDecision({ lesson_action: 'revise', reinforce_lesson_id: 5, lesson: null }, shown).kind,
        'none',
    );
});

test('resolveLessonDecision: retire a shown lesson; unshown target → none', () => {
    const shown = [row({ id: 5 })];
    assert.deepEqual(resolveLessonDecision({ lesson_action: 'retire', reinforce_lesson_id: 5 }, shown), {
        kind: 'retire',
        targetId: 5,
    });
    assert.equal(resolveLessonDecision({ lesson_action: 'retire', reinforce_lesson_id: 6 }, shown).kind, 'none');
});

test('resolveLessonDecision: refusal gates — right_to_skip only reinforces, unclear teaches nothing', () => {
    const shown = [row({ id: 5, lesson: 'Blocking lesson.', confidence: 0.8 })];
    // right_to_skip may reinforce the lesson that earned the skip...
    const reinforce = resolveLessonDecision(
        { verdict: 'right_to_skip', lesson_action: 'reinforce', reinforce_lesson_id: 5, confidence: 0.7 },
        shown,
        { kind: 'refusal' },
    );
    assert.equal(reinforce.kind, 'merge');
    // ...but may NEVER create or revise (no new restrictions from skips that worked).
    for (const action of ['new', 'revise', 'retire']) {
        const d = resolveLessonDecision(
            { verdict: 'right_to_skip', lesson_action: action, reinforce_lesson_id: 5, lesson: 'X bound 0.5 ATR.' },
            shown,
            { kind: 'refusal' },
        );
        assert.equal(d.kind, 'none', `right_to_skip must block ${action}`);
    }
    assert.equal(
        resolveLessonDecision(
            { verdict: 'unclear', lesson_action: 'revise', reinforce_lesson_id: 5, lesson: 'X.' },
            shown,
            { kind: 'refusal' },
        ).kind,
        'none',
    );
    // wrong_to_skip unlocks the corrective actions.
    const revise = resolveLessonDecision(
        { verdict: 'wrong_to_skip', lesson_action: 'revise', reinforce_lesson_id: 5, lesson: 'Bounded.', confidence: 0.6 },
        shown,
        { kind: 'refusal' },
    );
    assert.equal(revise.kind, 'revise');
});

test('promotedScopeOnReinforce: the ladder — cross-symbol promotes, cross-class globalizes, same origin holds', () => {
    const ethLesson = { scope: 'symbol' as const, symbol: 'ETHUSDT', assetClass: 'crypto' };
    // Same symbol: no move.
    assert.equal(promotedScopeOnReinforce(ethLesson, { symbol: 'ETHUSDT', assetClass: 'crypto' }), null);
    // Different symbol, same class: → asset_class.
    assert.deepEqual(promotedScopeOnReinforce(ethLesson, { symbol: 'SOLUSDT', assetClass: 'crypto' }), {
        scope: 'asset_class',
        assetClass: 'crypto',
    });
    // Different class straight from symbol scope: → global.
    assert.deepEqual(promotedScopeOnReinforce(ethLesson, { symbol: 'GOLD', assetClass: 'commodity' }), {
        scope: 'global',
        assetClass: null,
    });
    // asset_class lesson reinforced from another class: → global.
    assert.deepEqual(
        promotedScopeOnReinforce(
            { scope: 'asset_class', symbol: 'ETHUSDT', assetClass: 'crypto' },
            { symbol: 'US100', assetClass: 'index' },
        ),
        { scope: 'global', assetClass: null },
    );
    // Same class: holds.
    assert.equal(
        promotedScopeOnReinforce(
            { scope: 'asset_class', symbol: 'ETHUSDT', assetClass: 'crypto' },
            { symbol: 'SOLUSDT', assetClass: 'crypto' },
        ),
        null,
    );
    // Global never moves.
    assert.equal(
        promotedScopeOnReinforce({ scope: 'global', symbol: null, assetClass: null }, { symbol: 'GOLD', assetClass: 'commodity' }),
        null,
    );
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

test('resolveLessonDecision: win gates — lucky_win teaches nothing, wins never retire', () => {
    const shown = [row({ id: 5, lesson: 'A lesson.', confidence: 0.8 })];
    // lucky_win: hard none, whatever the analyst tried.
    for (const action of ['new', 'reinforce', 'revise', 'retire']) {
        const d = resolveLessonDecision(
            { verdict: 'lucky_win', lesson_action: action, reinforce_lesson_id: 5, lesson: 'X within 0.5 ATR.' },
            shown,
            { kind: 'win' },
        );
        assert.equal(d.kind, 'none', `lucky_win must block ${action}`);
    }
    // earned_win: reinforce/revise/new flow through...
    assert.equal(
        resolveLessonDecision(
            { verdict: 'earned_win', lesson_action: 'reinforce', reinforce_lesson_id: 5, confidence: 0.7 },
            shown,
            { kind: 'win' },
        ).kind,
        'merge',
    );
    assert.equal(
        resolveLessonDecision(
            { verdict: 'earned_win', lesson_action: 'revise', reinforce_lesson_id: 5, lesson: 'Loosened bound.', confidence: 0.6 },
            shown,
            { kind: 'win' },
        ).kind,
        'revise',
    );
    assert.equal(
        resolveLessonDecision(
            { verdict: 'exit_flaw', lesson_action: 'new', lesson: 'Trail after +1.5R within 0.5 ATR of target.', confidence: 0.6 },
            shown,
            { kind: 'win' },
        ).kind,
        'add',
    );
    // ...but retire is blocked for every win verdict.
    assert.equal(
        resolveLessonDecision(
            { verdict: 'earned_win', lesson_action: 'retire', reinforce_lesson_id: 5 },
            shown,
            { kind: 'win' },
        ).kind,
        'none',
    );
});

test('lessonOriginLabel + selectPromptLessons: provenance renders as counts, empty stays null', () => {
    assert.equal(lessonOriginLabel({ loss: 2, refusal: 1 }), '2 losses, 1 missed entry');
    assert.equal(lessonOriginLabel({ win: 1 }), '1 win');
    assert.equal(lessonOriginLabel({ loss: 1, win: 2, refusal: 3 }), '1 loss, 2 wins, 3 missed entries');
    assert.equal(lessonOriginLabel({}), null);
    const picked = selectPromptLessons([
        row({ id: 1, scope: 'symbol', symbol: 'ETHUSDT', lesson: 'Tagged.', originCounts: { loss: 1, win: 1 } }),
        row({ id: 2, scope: 'symbol', symbol: 'ETHUSDT', lesson: 'Legacy.', originCounts: {} }),
    ]);
    assert.equal(picked.find((p) => p.lesson === 'Tagged.')?.originLabel, '1 loss, 1 win');
    assert.equal(picked.find((p) => p.lesson === 'Legacy.')?.originLabel, null);
});
