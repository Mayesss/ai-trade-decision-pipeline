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
// (symbol ∪ its asset class ∪ global), confidence-sorted, capped per scope
// bucket (PROMPT_LESSON_SCOPE_CAPS), and render them as a cautionary block in
// the USER prompt (the cached system prefix stays byte-stable).
// SWING_LESSONS_MODE=off disables injection; the library keeps building
// regardless.
import {
    insertSwingLesson,
    loadActiveSwingLessons,
    mergeSwingLesson,
    retireSwingLesson,
    reviseSwingLesson,
    type SwingLessonRow,
    type SwingLessonScope,
} from './pg';

// Per-scope prompt buckets: each scope gets its own guaranteed slots, so a
// wall of high-confidence globals can never crowd out the lesson written
// specifically for this symbol (narrow scopes accrue support slower and would
// systematically lose a single confidence-sorted pool). Worst case
// 3+5+10 = 18 lessons ≈ ~1.4k tokens in the user turn.
export const PROMPT_LESSON_SCOPE_CAPS: Record<SwingLessonScope, number> = {
    symbol: 3,
    asset_class: 5,
    global: 10,
};

// Prompt render order: most trade-specific first.
const PROMPT_SCOPE_ORDER: SwingLessonScope[] = ['symbol', 'asset_class', 'global'];

export type SwingLessonsMode = 'on' | 'off';

export function resolveSwingLessonsMode(): SwingLessonsMode {
    const raw = String(process.env.SWING_LESSONS_MODE || '')
        .trim()
        .toLowerCase();
    return raw === 'off' ? 'off' : 'on';
}

export type PromptLesson = { scope: SwingLessonScope; lesson: string };

// Pure selection half (tested): within each scope bucket, confidence first,
// then how many post-mortems back the lesson, then recency; each bucket capped
// independently. Output ordered symbol → asset_class → global.
export function selectPromptLessons(
    rows: SwingLessonRow[],
    caps: Partial<Record<SwingLessonScope, number>> = PROMPT_LESSON_SCOPE_CAPS,
): PromptLesson[] {
    const sorted = [...rows]
        .filter((r) => r.status === 'active' && r.lesson.trim().length > 0)
        .sort(
            (a, b) =>
                b.confidence - a.confidence ||
                b.supportCount - a.supportCount ||
                b.updatedAtMs - a.updatedAtMs,
        );
    return PROMPT_SCOPE_ORDER.flatMap((scope) =>
        sorted
            .filter((r) => r.scope === scope)
            .slice(0, Math.max(0, caps[scope] ?? 0))
            .map((r) => ({ scope: r.scope, lesson: r.lesson.trim() })),
    );
}

// Read side used by the tick: [] when injection is off / no library yet /
// PG hiccup — the prompt block simply doesn't render then.
export async function loadPromptLessons(symbol: string, assetClass: string | null): Promise<PromptLesson[]> {
    if (resolveSwingLessonsMode() === 'off') return [];
    try {
        // limit 100: the DB query is confidence-ordered across ALL scopes, so a
        // tight limit could truncate low-confidence narrow-scope rows before the
        // per-scope buckets ever see them.
        const rows = await loadActiveSwingLessons({ symbol, assetClass, limit: 100 });
        return selectPromptLessons(rows);
    } catch (err) {
        console.warn(`lesson load failed for ${symbol}:`, err);
        return [];
    }
}

// ---------------------------------------------------------------------------
// Applying the analyst's lesson decision (report.lesson_action)
// ---------------------------------------------------------------------------
// Scope is CODE-owned, not analyst opinion (the analyst's lesson_scope field
// is advisory at best): every new lesson enters at SYMBOL scope and earns
// promotion mechanically — reinforced from a different symbol in the same
// class → asset_class; from a different class → global. This is the ladder
// that keeps a two-loss BTC pattern from becoming a universal veto again.
export type LessonDecision =
    | { kind: 'add'; scope: 'symbol'; text: string; confidence: number }
    | {
          kind: 'merge';
          targetId: number;
          text: string;
          confidence: number;
          // Target row facts the ladder needs at apply time (from the SHOWN slice).
          targetScope: SwingLessonScope;
          targetSymbol: string | null;
          targetAssetClass: string | null;
      }
    // Revise: reword a shown lesson, tighten/loosen its bound, or move its
    // scope (both directions) — the analyst's tool for contradictions and
    // over-restrictive rules. Confidence is taken as given (a revise may
    // deliberately weaken).
    | { kind: 'revise'; targetId: number; text: string; confidence: number; scope: SwingLessonScope | null }
    | { kind: 'retire'; targetId: number }
    | { kind: 'none'; reason: string };

// Pure resolution of the post-mortem report's lesson fields against the
// library slice that was SHOWN to the analyst (tested). Defensive on model
// output: a reinforce pointing at an id that was never shown degrades to add
// (losing the lesson entirely is worse than a rare near-duplicate); revise/
// retire pointing at an unshown id degrade to none (mutating an unseen lesson
// is never safe); a reinforce without usable text keeps the existing wording.
export function resolveLessonDecision(
    report: {
        verdict?: string | null;
        lesson?: string | null;
        lesson_action?: string | null;
        lesson_scope?: string | null;
        reinforce_lesson_id?: number | null;
        confidence?: number | null;
    },
    shownLessons: SwingLessonRow[],
    opts: { kind?: 'loss' | 'refusal' | 'win' } = {},
): LessonDecision {
    const verdict = String(report?.verdict || '').toLowerCase();
    // Hard gates, not just prompt guidance:
    // - loss + bad_luck: the process was sound, the loss was variance — nothing
    //   may enter or reinforce (variance proves nothing about a lesson).
    // - refusal + right_to_skip: the skip was CORRECT — the only permitted
    //   library effect is reinforcing the lesson that earned it ('none'
    //   otherwise). New restrictions must never be born from skips that worked.
    // - refusal + unclear: teaches nothing either way.
    // - win + lucky_win: bad_luck's mirror — profit despite a process flaw is
    //   variance, never doctrine (must not weaken the violated lesson, must
    //   not mint "the violation worked").
    // - wins never retire: removing a rule takes negative evidence about the
    //   rule, not one profitable trade.
    if (verdict === 'bad_luck') {
        return { kind: 'none', reason: 'bad_luck_no_lesson' };
    }
    const action = String(report?.lesson_action || '').toLowerCase();
    if (opts.kind === 'refusal') {
        if (verdict === 'unclear') return { kind: 'none', reason: 'unclear_no_lesson' };
        if (verdict === 'right_to_skip' && action !== 'reinforce' && action !== 'none') {
            return { kind: 'none', reason: 'right_to_skip_only_reinforces' };
        }
    }
    if (opts.kind === 'win') {
        if (verdict === 'lucky_win') return { kind: 'none', reason: 'lucky_win_no_lesson' };
        if (action === 'retire') return { kind: 'none', reason: 'wins_never_retire' };
    }
    const text = typeof report?.lesson === 'string' && report.lesson.trim() ? report.lesson.trim().slice(0, 300) : null;
    const confidence = Number.isFinite(Number(report?.confidence))
        ? Math.max(0, Math.min(1, Number(report?.confidence)))
        : 0.5;
    const target = shownLessons.find((l) => l.id === Number(report?.reinforce_lesson_id));

    if (action === 'retire') {
        if (!target) return { kind: 'none', reason: 'retire_target_not_shown' };
        return { kind: 'retire', targetId: target.id };
    }
    if (action === 'revise') {
        if (!target) return { kind: 'none', reason: 'revise_target_not_shown' };
        if (!text) return { kind: 'none', reason: 'revise_without_text' };
        const scope: SwingLessonScope | null = ['symbol', 'asset_class', 'global'].includes(
            report?.lesson_scope as string,
        )
            ? (report!.lesson_scope as SwingLessonScope)
            : null;
        return { kind: 'revise', targetId: target.id, text, confidence, scope };
    }
    if (action === 'none' || (!text && action !== 'reinforce')) {
        return { kind: 'none', reason: action === 'none' ? 'analyst_none' : 'no_lesson_text' };
    }
    if (action === 'reinforce') {
        if (target) {
            return {
                kind: 'merge',
                targetId: target.id,
                text: text ?? target.lesson,
                // Reinforcement never weakens a lesson that just proved itself.
                confidence: Math.max(confidence, target.confidence),
                targetScope: target.scope,
                targetSymbol: target.symbol ?? null,
                targetAssetClass: target.assetClass ?? null,
            };
        }
        if (text) return { kind: 'add', scope: 'symbol', text, confidence };
        return { kind: 'none', reason: 'reinforce_unresolvable' };
    }
    if (action === 'new' && text) {
        // Ladder rule: new lessons ALWAYS enter at symbol scope, whatever the
        // analyst suggested — promotion is earned by cross-symbol evidence.
        return { kind: 'add', scope: 'symbol', text, confidence };
    }
    return { kind: 'none', reason: `unusable_action_${action || 'empty'}` };
}

// The ladder's promotion half (pure, tested): a reinforce coming from a
// different symbol than the lesson's origin promotes symbol → asset_class; a
// reinforce from a different asset class promotes asset_class → global.
// Same-origin reinforcement never moves scope.
export function promotedScopeOnReinforce(
    target: { scope: SwingLessonScope; symbol: string | null; assetClass: string | null },
    ctx: { symbol: string; assetClass: string | null },
): { scope: SwingLessonScope; assetClass: string | null } | null {
    const ctxSymbol = String(ctx.symbol || '').toUpperCase();
    if (target.scope === 'symbol') {
        const originSymbol = String(target.symbol || '').toUpperCase();
        if (originSymbol && ctxSymbol && originSymbol !== ctxSymbol) {
            const sameClass =
                target.assetClass && ctx.assetClass && target.assetClass === ctx.assetClass;
            return sameClass
                ? { scope: 'asset_class', assetClass: target.assetClass }
                : { scope: 'global', assetClass: null };
        }
        return null;
    }
    if (target.scope === 'asset_class') {
        if (target.assetClass && ctx.assetClass && target.assetClass !== ctx.assetClass) {
            return { scope: 'global', assetClass: null };
        }
        return null;
    }
    return null;
}

// DB half — best-effort; a library write failure never fails the post-mortem.
export async function applyLessonDecision(
    decision: LessonDecision,
    ctx: { postmortemId: number; symbol: string; assetClass: string | null },
): Promise<{ applied: LessonDecision['kind']; lessonId?: number | null; promotedTo?: string | null }> {
    try {
        if (decision.kind === 'add') {
            const id = await insertSwingLesson({
                scope: 'symbol',
                symbol: ctx.symbol,
                assetClass: ctx.assetClass,
                lesson: decision.text,
                confidence: decision.confidence,
                sourcePostmortemId: ctx.postmortemId,
            });
            return { applied: 'add', lessonId: id };
        }
        if (decision.kind === 'merge') {
            // The ladder's promotion half: cross-symbol evidence widens scope.
            const promotion = promotedScopeOnReinforce(
                { scope: decision.targetScope, symbol: decision.targetSymbol, assetClass: decision.targetAssetClass },
                { symbol: ctx.symbol, assetClass: ctx.assetClass },
            );
            await mergeSwingLesson({
                id: decision.targetId,
                lesson: decision.text,
                confidence: decision.confidence,
                sourcePostmortemId: ctx.postmortemId,
                promoteTo: promotion,
            });
            return { applied: 'merge', lessonId: decision.targetId, promotedTo: promotion?.scope ?? null };
        }
        if (decision.kind === 'revise') {
            await reviseSwingLesson({
                id: decision.targetId,
                lesson: decision.text,
                confidence: decision.confidence,
                scope: decision.scope,
                sourcePostmortemId: ctx.postmortemId,
            });
            return { applied: 'revise', lessonId: decision.targetId };
        }
        if (decision.kind === 'retire') {
            await retireSwingLesson(decision.targetId);
            return { applied: 'retire', lessonId: decision.targetId };
        }
        return { applied: 'none' };
    } catch (err) {
        console.warn(`lesson apply failed for postmortem #${ctx.postmortemId}:`, err);
        return { applied: 'none' };
    }
}
