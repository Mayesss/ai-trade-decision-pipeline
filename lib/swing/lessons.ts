// Lesson library (phase 3). Two halves:
//
// CURATION (write side): after a post-mortem succeeds, its 1-2 line lesson is
// NOT appended raw — a curator AI call sees the library slice relevant to the
// instrument and decides: add a genuinely new lesson, merge-reformulate it
// into an existing one (that row's support grows), or discard a duplicate /
// non-generalizable one. The curator also finalizes the audience scope
// (symbol | asset_class | global; the post-mortem analyst's suggestion is a
// hint, not a verdict).
//
// INJECTION (read side): flat/managed ticks load the active lessons for
// (symbol ∪ its asset class ∪ global), confidence-sorted, capped at
// MAX_PROMPT_LESSONS, and render them as a cautionary block in the USER
// prompt (the cached system prefix stays byte-stable). SWING_LESSONS_MODE=off
// disables injection; curation keeps building the library regardless.
import { callSwingDecision } from '../aiProvider';
import type { AnalysisPlatform } from '../platform';
import { resolveSwingCategory } from './category';
import {
    insertSwingLesson,
    loadActiveSwingLessons,
    mergeSwingLesson,
    type SwingLessonRow,
    type SwingLessonScope,
    type SwingPostmortemRow,
} from './pg';

export const MAX_PROMPT_LESSONS = 5;

export type SwingLessonsMode = 'on' | 'off';

export function resolveSwingLessonsMode(): SwingLessonsMode {
    const raw = String(process.env.SWING_LESSONS_MODE || '')
        .trim()
        .toLowerCase();
    return raw === 'off' ? 'off' : 'on';
}

export type PromptLesson = { scope: SwingLessonScope; lesson: string };

// Pure selection half (tested): confidence first, then how many post-mortems
// back the lesson, then recency. Cap for the prompt.
export function selectPromptLessons(rows: SwingLessonRow[], max: number = MAX_PROMPT_LESSONS): PromptLesson[] {
    return [...rows]
        .filter((r) => r.status === 'active' && r.lesson.trim().length > 0)
        .sort(
            (a, b) =>
                b.confidence - a.confidence ||
                b.supportCount - a.supportCount ||
                b.updatedAtMs - a.updatedAtMs,
        )
        .slice(0, Math.max(0, max))
        .map((r) => ({ scope: r.scope, lesson: r.lesson.trim() }));
}

// Read side used by the tick: [] when injection is off / no library yet /
// PG hiccup — the prompt block simply doesn't render then.
export async function loadPromptLessons(symbol: string, assetClass: string | null): Promise<PromptLesson[]> {
    if (resolveSwingLessonsMode() === 'off') return [];
    try {
        const rows = await loadActiveSwingLessons({ symbol, assetClass });
        return selectPromptLessons(rows);
    } catch (err) {
        console.warn(`lesson load failed for ${symbol}:`, err);
        return [];
    }
}

export const LESSON_CURATION_SCHEMA = {
    name: 'swing_lesson_curation',
    schema: {
        type: 'object',
        additionalProperties: false,
        required: ['action', 'scope', 'target_lesson_id', 'lesson', 'confidence', 'rationale'],
        properties: {
            action: { type: 'string', enum: ['add', 'merge', 'discard'] },
            scope: { type: 'string', enum: ['symbol', 'asset_class', 'global'] },
            // Required (null) so strict structured-output accepts the schema;
            // an id only makes sense on merge.
            target_lesson_id: { type: ['integer', 'null'] },
            lesson: { type: ['string', 'null'] },
            confidence: { type: ['number', 'null'], minimum: 0, maximum: 1 },
            rationale: { type: 'string' },
        },
    },
} as const;

const LESSON_CURATION_SYSTEM_PROMPT = `You curate a small library of trading lessons for an automated swing-trading AI. Each lesson is a 1-2 sentence imperative instruction (max ~220 chars) distilled from a post-mortem of a losing trade. The library is injected into future trading prompts, so it must stay SMALL, NON-REDUNDANT and GENERALIZABLE — a bloated or repetitive library dilutes attention and is worse than none.

You receive ONE candidate lesson (with its post-mortem verdict, confidence and suggested audience scope) plus the existing active lessons already covering this instrument. Decide:
- "merge": the candidate substantially overlaps an existing lesson → set target_lesson_id to that lesson and write the REFORMULATED text that covers both (keep it ≤220 chars, imperative, no symbol-specific price levels). Set confidence to your updated belief given the additional supporting case (usually equal or higher than the existing row's).
- "add": the candidate teaches something the library does not already say → write the final text (you may tighten the wording) and its confidence (start from the post-mortem's confidence).
- "discard": the candidate is redundant AND adds no strength, is too situational to ever generalize, or is not actionable. lesson/confidence may be null.
- scope: who should see this lesson — "symbol" (a quirk of this one instrument), "asset_class" (applies to this class, e.g. all crypto), "global" (any instrument). Most structural/entry-mechanics lessons generalize: prefer asset_class or global unless the lesson genuinely hinges on this instrument's behavior. On merge, scope applies to the merged row's audience and normally keeps the existing row's scope unless the new case proves it generalizes wider.

Respond with strict JSON per the schema.`;

export type LessonCurationResult =
    | { action: 'add'; lessonId: number | null }
    | { action: 'merge'; lessonId: number }
    | { action: 'discard' }
    | { action: 'skipped'; reason: string };

// Best-effort — a curation failure never fails the post-mortem it followed.
export async function curateLessonFromPostmortem(pm: SwingPostmortemRow): Promise<LessonCurationResult> {
    try {
        if (!pm.lesson || !pm.verdict) return { action: 'skipped', reason: 'no_lesson' };
        const assetClass = resolveSwingCategory({
            symbol: pm.symbol,
            platform: pm.platform as AnalysisPlatform,
        });
        const existing = await loadActiveSwingLessons({ symbol: pm.symbol, assetClass });
        const user = [
            `INSTRUMENT: ${pm.symbol} (platform=${pm.platform}, asset_class=${assetClass ?? 'unknown'})`,
            '',
            'CANDIDATE LESSON (from a new post-mortem):',
            JSON.stringify(
                {
                    lesson: pm.lesson,
                    verdict: pm.verdict,
                    confidence: pm.report?.confidence ?? null,
                    suggested_scope: pm.lessonScope ?? null,
                    pnl_pct: pm.pnlPct,
                },
                null,
                1,
            ),
            '',
            `EXISTING ACTIVE LESSONS covering this instrument (${existing.length}):`,
            JSON.stringify(
                existing.map((l) => ({
                    id: l.id,
                    scope: l.scope,
                    lesson: l.lesson,
                    confidence: l.confidence,
                    support_count: l.supportCount,
                })),
                null,
                1,
            ),
        ].join('\n');

        const { json } = await callSwingDecision({
            system: LESSON_CURATION_SYSTEM_PROMPT,
            user,
            schema: LESSON_CURATION_SCHEMA as unknown as { name: string; schema: Record<string, unknown> },
        });
        const action = String(json?.action || '');
        const scope: SwingLessonScope = ['symbol', 'asset_class', 'global'].includes(json?.scope)
            ? json.scope
            : (pm.lessonScope as SwingLessonScope) || 'symbol';
        const text = typeof json?.lesson === 'string' ? json.lesson.trim().slice(0, 300) : null;
        const confidence = Number.isFinite(Number(json?.confidence))
            ? Math.max(0, Math.min(1, Number(json.confidence)))
            : Number(pm.report?.confidence) || 0.5;

        if (action === 'discard') return { action: 'discard' };
        if (action === 'merge') {
            const targetId = Number(json?.target_lesson_id);
            const target = existing.find((l) => l.id === targetId);
            if (!target || !text) {
                // Hallucinated id / missing text degrades to add — losing the
                // lesson entirely is worse than a rare near-duplicate.
                if (!text) return { action: 'skipped', reason: 'merge_without_text' };
                const id = await insertSwingLesson({
                    scope,
                    symbol: pm.symbol,
                    assetClass,
                    lesson: text,
                    confidence,
                    sourcePostmortemId: pm.id,
                });
                return { action: 'add', lessonId: id };
            }
            await mergeSwingLesson({ id: target.id, lesson: text, confidence, sourcePostmortemId: pm.id });
            return { action: 'merge', lessonId: target.id };
        }
        if (action === 'add' && text) {
            const id = await insertSwingLesson({
                scope,
                symbol: pm.symbol,
                assetClass,
                lesson: text,
                confidence,
                sourcePostmortemId: pm.id,
            });
            return { action: 'add', lessonId: id };
        }
        return { action: 'skipped', reason: `unusable_curation_${action || 'empty'}` };
    } catch (err: any) {
        console.warn(`lesson curation failed for postmortem #${pm.id}:`, err);
        return { action: 'skipped', reason: err?.message || String(err) };
    }
}
