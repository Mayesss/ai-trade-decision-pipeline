// Lesson library (phase 3). Two halves:
//
// WRITE side: the post-mortem analyst itself is library-aware — the dossier
// shows it the active lessons already covering the instrument, and its report
// carries a lesson_action: 'new' (a failure mode the library doesn't cover),
// 'reinforce' (an existing lesson applied — support grows, optionally
// reformulated to absorb the new case), or 'none' (bad luck / already covered
// with nothing to add — losing WITH the process is not a lesson). This module
// resolves that decision against the library (pure) and applies it (DB); no
// separate curator AI call.
//
// INJECTION (read side): flat/managed ticks load the active lessons for
// (symbol ∪ its asset class ∪ global), confidence-sorted, capped at
// MAX_PROMPT_LESSONS, and render them as a cautionary block in the USER
// prompt (the cached system prefix stays byte-stable). SWING_LESSONS_MODE=off
// disables injection; the library keeps building regardless.
import {
    insertSwingLesson,
    loadActiveSwingLessons,
    mergeSwingLesson,
    type SwingLessonRow,
    type SwingLessonScope,
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

// ---------------------------------------------------------------------------
// Applying the analyst's lesson decision (report.lesson_action)
// ---------------------------------------------------------------------------
export type LessonDecision =
    | { kind: 'add'; scope: SwingLessonScope; text: string; confidence: number }
    | { kind: 'merge'; targetId: number; text: string; confidence: number }
    | { kind: 'none'; reason: string };

// Pure resolution of the post-mortem report's lesson fields against the
// library slice that was SHOWN to the analyst (tested). Defensive on model
// output: a reinforce pointing at an id that was never shown degrades to add
// (losing the lesson entirely is worse than a rare near-duplicate); a
// reinforce without any usable text keeps the existing row's wording.
export function resolveLessonDecision(
    report: {
        lesson?: string | null;
        lesson_action?: string | null;
        lesson_scope?: string | null;
        reinforce_lesson_id?: number | null;
        confidence?: number | null;
    },
    shownLessons: SwingLessonRow[],
): LessonDecision {
    const action = String(report?.lesson_action || '').toLowerCase();
    const text = typeof report?.lesson === 'string' && report.lesson.trim() ? report.lesson.trim().slice(0, 300) : null;
    const scope: SwingLessonScope = ['symbol', 'asset_class', 'global'].includes(report?.lesson_scope as string)
        ? (report!.lesson_scope as SwingLessonScope)
        : 'symbol';
    const confidence = Number.isFinite(Number(report?.confidence))
        ? Math.max(0, Math.min(1, Number(report?.confidence)))
        : 0.5;

    if (action === 'none' || (!text && action !== 'reinforce')) {
        return { kind: 'none', reason: action === 'none' ? 'analyst_none' : 'no_lesson_text' };
    }
    if (action === 'reinforce') {
        const target = shownLessons.find((l) => l.id === Number(report?.reinforce_lesson_id));
        if (target) {
            return {
                kind: 'merge',
                targetId: target.id,
                text: text ?? target.lesson,
                // Reinforcement never weakens a lesson that just proved itself.
                confidence: Math.max(confidence, target.confidence),
            };
        }
        if (text) return { kind: 'add', scope, text, confidence };
        return { kind: 'none', reason: 'reinforce_unresolvable' };
    }
    if (action === 'new' && text) {
        return { kind: 'add', scope, text, confidence };
    }
    return { kind: 'none', reason: `unusable_action_${action || 'empty'}` };
}

// DB half — best-effort; a library write failure never fails the post-mortem.
export async function applyLessonDecision(
    decision: LessonDecision,
    ctx: { postmortemId: number; symbol: string; assetClass: string | null },
): Promise<{ applied: LessonDecision['kind']; lessonId?: number | null }> {
    try {
        if (decision.kind === 'add') {
            const id = await insertSwingLesson({
                scope: decision.scope,
                symbol: ctx.symbol,
                assetClass: ctx.assetClass,
                lesson: decision.text,
                confidence: decision.confidence,
                sourcePostmortemId: ctx.postmortemId,
            });
            return { applied: 'add', lessonId: id };
        }
        if (decision.kind === 'merge') {
            await mergeSwingLesson({
                id: decision.targetId,
                lesson: decision.text,
                confidence: decision.confidence,
                sourcePostmortemId: ctx.postmortemId,
            });
            return { applied: 'merge', lessonId: decision.targetId };
        }
        return { applied: 'none' };
    } catch (err) {
        console.warn(`lesson apply failed for postmortem #${ctx.postmortemId}:`, err);
        return { applied: 'none' };
    }
}
