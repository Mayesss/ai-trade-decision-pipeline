// Post-loss trade post-mortems (phase 2). When a closed position lands in
// swing.positions the close-persistence paths call maybeEnqueueSwingPostmortem;
// losses (default — SWING_POSTMORTEM_MODE=all|off widens/kills the filter) get
// one row in swing.postmortems and a detached worker request. The worker
// (pages/api/swing/postmortem.ts) rebuilds the trade's full tick series from
// swing.decisions (prompts included) + swing.tick_log, feeds the dossier to the
// swing AI provider with a forensic prompt, and persists the report plus a 1-2
// line lesson. Lessons are STORED for later prompt injection (phase 3) — no
// code feeds them back to the trading AI yet.
import { callSwingDecision } from '../aiProvider';
import type { PositionWindow } from '../analytics';
import type { AnalysisPlatform } from '../platform';
import { resolveSwingCategory } from './category';
import { applyLessonDecision, resolveLessonDecision } from './lessons';
import {
    completeSwingPostmortem,
    enqueueSwingPostmortem,
    failSwingPostmortem,
    loadActiveSwingLessons,
    loadSwingDecisionWindow,
    loadSwingTickLog,
    type SwingDecisionFullRow,
    type SwingLessonRow,
    type SwingPostmortemRow,
    type SwingPostmortemTrigger,
    type SwingTickLogRow,
} from './pg';

export type SwingPostmortemMode = 'loss' | 'all' | 'off';

export function resolveSwingPostmortemMode(): SwingPostmortemMode {
    const raw = String(process.env.SWING_POSTMORTEM_MODE || '')
        .trim()
        .toLowerCase();
    if (raw === 'all' || raw === 'off' || raw === 'loss') return raw;
    return 'loss';
}

// Sign is all that matters; prefer net over gross, absolute over pct only in
// the order the mirrors actually populate them.
export function postmortemPnl(w: Pick<PositionWindow, 'pnlNet' | 'pnlPct' | 'pnlGross' | 'pnlGrossPct'>): number | null {
    for (const v of [w.pnlNet, w.pnlPct, w.pnlGross, w.pnlGrossPct]) {
        if (typeof v === 'number' && Number.isFinite(v)) return v;
    }
    return null;
}

export function shouldEnqueuePostmortem(w: PositionWindow, mode: SwingPostmortemMode): boolean {
    if (mode === 'off') return false;
    if (!w?.symbol || !w.exitTimestamp) return false;
    if (mode === 'all') return true;
    const pnl = postmortemPnl(w);
    // Unknown PnL ≠ loss: Bitget closes can land before realized ROI does —
    // the next re-sync carries the PnL and re-calls this (idempotent insert).
    return pnl != null && pnl < 0;
}

// Best-effort enqueue + worker kick, called from every close-persistence path
// (Bitget broker-merge sync, Capital reconcile, Capital AI-close snapshot).
// Never throws into the caller; returns the new row id or null (filtered out /
// already enqueued / PG unconfigured).
export async function maybeEnqueueSwingPostmortem(
    platform: string,
    window: PositionWindow,
    trigger: SwingPostmortemTrigger = 'close',
): Promise<number | null> {
    try {
        // Manual/backfill triggers bypass the mode filter — an operator asking
        // for a post-mortem gets one, wins included.
        if (trigger === 'close' && !shouldEnqueuePostmortem(window, resolveSwingPostmortemMode())) return null;
        const id = await enqueueSwingPostmortem({
            platform,
            symbol: window.symbol,
            positionKey: String(window.id || `${window.symbol}-${window.entryTimestamp ?? 'nots'}`),
            trigger,
            side: window.side ?? null,
            entryTsMs: window.entryTimestamp ?? null,
            exitTsMs: window.exitTimestamp ?? null,
            entryPrice: window.entryPrice ?? null,
            exitPrice: window.exitPrice ?? null,
            pnlPct: window.pnlPct ?? window.pnlGrossPct ?? null,
            pnlNet: window.pnlNet ?? null,
        });
        if (id != null) triggerSwingPostmortemWorker(id);
        return id;
    } catch (err) {
        console.warn(`postmortem enqueue failed for ${window?.symbol}:`, err);
        return null;
    }
}

function postmortemBaseUrl(): string | null {
    const explicit = String(process.env.SWING_POSTMORTEM_BASE_URL || '').trim();
    if (explicit) return explicit.replace(/\/+$/, '');
    const vercel = String(process.env.VERCEL_URL || '').trim();
    return vercel ? `https://${vercel}` : null;
}

// Detached worker kick (evaluate.ts pattern, but from lib code: auth headers
// come from env, not a forwarded request). Fire-and-forget — if the runtime
// drops the request, the row stays 'queued' and ?drain=true sweeps it up.
export function triggerSwingPostmortemWorker(id: number): void {
    const base = postmortemBaseUrl();
    if (!base) {
        console.log(`[postmortem] #${id} queued (no base URL in env — run ?drain=true to process)`);
        return;
    }
    const headers: Record<string, string> = {};
    if (process.env.ADMIN_ACCESS_SECRET) headers['x-admin-access-secret'] = process.env.ADMIN_ACCESS_SECRET;
    if (process.env.VERCEL_AUTOMATION_BYPASS_SECRET)
        headers['x-vercel-protection-bypass'] = process.env.VERCEL_AUTOMATION_BYPASS_SECRET;
    void fetch(`${base}/api/swing/postmortem?id=${encodeURIComponent(id)}&execute=true`, {
        method: 'GET',
        headers,
        cache: 'no-store',
    })
        .then((res) => {
            if (!res.ok) console.error(`postmortem worker trigger for #${id} returned HTTP ${res.status}`);
        })
        .catch((err) => console.error(`postmortem worker trigger for #${id} failed:`, err));
}

// ---------------------------------------------------------------------------
// Dossier assembly (pure — fed by rows, returns the stored dossier + the AI
// user message). The stored dossier holds DIGESTS only: full prompts already
// live in swing.decisions, so persisting them again would double prompt
// storage for zero information.
// ---------------------------------------------------------------------------
export const POSTMORTEM_LOOKBACK_BEFORE_ENTRY_MS = 24 * 60 * 60 * 1000;
export const POSTMORTEM_TAIL_AFTER_EXIT_MS = 60 * 60 * 1000;
// Fallback when the position row has no entry timestamp (Capital transaction
// imports occasionally lack it): assume at most this much position lifetime.
export const POSTMORTEM_MAX_LIFETIME_MS = 48 * 60 * 60 * 1000;

const MAX_AI_USER_CHARS = 250_000;
const MAX_FULL_PROMPT_CHARS = 20_000;
const MAX_SKIP_ROWS = 400;
const MAX_CALL_ROWS = 250;

type DecisionDigest = {
    ts: number;
    iso: string;
    action: string | null;
    summary: string | null;
    reason: string | null;
    take_profit_price: number | null;
    stop_loss_price: number | null;
    entry_limit_price: number | null;
    exit_size_pct: number | null;
    cooldown_minutes: number | null;
    exec: Record<string, any> | null;
    model: string | null;
    tokens: { in: number; out: number; cached: number | null } | null;
    decision_id: number;
};

type SkipDigest = {
    ts: number;
    iso: string;
    stage: string;
    reason: string | null;
    metrics: Record<string, any> | null;
};

export type PostmortemDossier = {
    position: Record<string, any>;
    window: { from_utc: string; to_utc: string };
    counts: { ai_calls: number; skipped_ticks: number; dropped_ai_calls: number; dropped_skips: number; full_prompts: number };
    ai_calls: DecisionDigest[];
    skipped_ticks: SkipDigest[];
    // Which decisions got their full prompt shown to the analyst (by id) —
    // the UI can fetch the texts from swing.decisions on demand.
    pivotal_decision_ids: number[];
    // The active lesson-library slice shown to the analyst (dedup/adherence
    // context) — provenance for reinforce decisions.
    lessons_shown?: Array<{ id: number; scope: string; lesson: string }>;
};

const numOrNull = (v: unknown): number | null => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
};

function isSkipDecision(d: SwingDecisionFullRow): boolean {
    const ai = d.aiDecision as any;
    return ai?.decision_source === 'pre_ai_skip' || ai?.promptSkipped === true;
}

function digestDecision(d: SwingDecisionFullRow): DecisionDigest {
    const ai = (d.aiDecision ?? {}) as any;
    const exec = (d.execResult ?? {}) as any;
    const usage = ai.ai_usage as any;
    const execDigest: Record<string, any> = {};
    for (const key of ['placed', 'closed', 'reversed', 'reason', 'orderId', 'leverage', 'targetLeverage']) {
        if (exec[key] !== undefined && exec[key] !== null) execDigest[key] = exec[key];
    }
    if (exec.tpsl) execDigest.tpsl = exec.tpsl;
    return {
        ts: d.decidedAtMs,
        iso: new Date(d.decidedAtMs).toISOString(),
        action: d.action ?? ai.action ?? null,
        summary: typeof ai.summary === 'string' ? ai.summary : null,
        reason: typeof ai.reason === 'string' ? ai.reason : null,
        take_profit_price: numOrNull(ai.take_profit_price),
        stop_loss_price: numOrNull(ai.stop_loss_price),
        entry_limit_price: numOrNull(ai.entry_limit_price),
        exit_size_pct: numOrNull(ai.exit_size_pct),
        cooldown_minutes: numOrNull(ai.cooldown_minutes),
        exec: Object.keys(execDigest).length ? execDigest : null,
        model: typeof ai.ai_model === 'string' ? ai.ai_model : null,
        tokens: usage
            ? {
                  in: Number(usage.input_tokens) || 0,
                  out: Number(usage.output_tokens) || 0,
                  cached: numOrNull(usage.cache_read_input_tokens),
              }
            : null,
        decision_id: d.id,
    };
}

// Pivotal = the ticks whose full prompt the analyst should read: everything
// that acted (entries/exits/reversals), everything that amended the bracket,
// plus the first and last AI call in the window. Returns ids ranked by
// importance, capped by the caller.
export function pickPivotalDecisions(calls: SwingDecisionFullRow[]): SwingDecisionFullRow[] {
    const scored = calls.map((d, idx) => {
        const ai = (d.aiDecision ?? {}) as any;
        const exec = (d.execResult ?? {}) as any;
        const action = String(d.action ?? ai.action ?? '').toUpperCase();
        let score = 0;
        if (action === 'BUY' || action === 'SELL' || action === 'REVERSE') score += 100;
        if (action === 'CLOSE') score += 90;
        if (exec.placed === true) score += 40;
        const tpsl = exec.tpsl as any;
        if (tpsl?.takeProfit?.applied === true || tpsl?.stopLoss?.applied === true || tpsl?.updated === true) score += 30;
        if (numOrNull(ai.exit_size_pct) != null && Number(ai.exit_size_pct) > 0) score += 30;
        if (idx === calls.length - 1) score += 25; // last call before/at close
        if (idx === 0) score += 15;
        return { d, score };
    });
    return scored
        .filter((s) => s.score > 0 && s.d.prompt?.user)
        .sort((a, b) => b.score - a.score || a.d.decidedAtMs - b.d.decidedAtMs)
        .map((s) => s.d);
}

function digestTickSkip(t: SwingTickLogRow): SkipDigest {
    return {
        ts: t.tsMs,
        iso: new Date(t.tsMs).toISOString(),
        stage: t.stage,
        reason: t.reason,
        metrics: t.metrics ?? null,
    };
}

function digestDecisionSkip(d: SwingDecisionFullRow): SkipDigest {
    const ai = (d.aiDecision ?? {}) as any;
    const snap = (d.snapshot ?? {}) as any;
    return {
        ts: d.decidedAtMs,
        iso: new Date(d.decidedAtMs).toISOString(),
        stage: String(ai.skipStage ?? snap.skipStage ?? 'skip'),
        reason: typeof ai.reason === 'string' ? ai.reason : (snap.skipReason ?? null),
        metrics: snap.metrics ?? null,
    };
}

// Skips exist in two stores with overlap: hourly skips as swing.decisions rows
// (since forever) and ALL skips as swing.tick_log rows (since phase 1). Merge
// on a minute+stage bucket, preferring tick_log (it carries gate metrics).
export function mergeSkips(tickSkips: SkipDigest[], decisionSkips: SkipDigest[]): SkipDigest[] {
    const byBucket = new Map<string, SkipDigest>();
    for (const s of decisionSkips) byBucket.set(`${Math.floor(s.ts / 60_000)}:${s.stage}`, s);
    for (const s of tickSkips) byBucket.set(`${Math.floor(s.ts / 60_000)}:${s.stage}`, s);
    return Array.from(byBucket.values()).sort((a, b) => a.ts - b.ts);
}

// Middle-out truncation: post-mortems care most about how a window STARTS
// (context building up) and ENDS (what preceded the outcome).
export function truncateMiddle<T>(rows: T[], max: number): { rows: T[]; dropped: number } {
    if (rows.length <= max) return { rows, dropped: 0 };
    const head = Math.ceil(max / 2);
    const tail = max - head;
    return { rows: [...rows.slice(0, head), ...rows.slice(rows.length - tail)], dropped: rows.length - max };
}

export function buildPostmortemDossier(input: {
    position: Record<string, any>;
    fromMs: number;
    toMs: number;
    decisions: SwingDecisionFullRow[];
    ticks: SwingTickLogRow[];
    // Active lesson library covering this instrument — shown to the analyst
    // so it can judge adherence and never emit duplicates.
    library?: SwingLessonRow[];
}): { dossier: PostmortemDossier; aiUserMessage: string } {
    const calls = input.decisions.filter((d) => !isSkipDecision(d) && !d.dryRun);
    const decisionSkips = input.decisions.filter((d) => isSkipDecision(d) && !d.dryRun).map(digestDecisionSkip);
    const tickSkips = input.ticks.filter((t) => t.kind === 'skip' && !t.dryRun).map(digestTickSkip);
    const skipsAll = mergeSkips(tickSkips, decisionSkips);

    const callDigestsAll = calls.map(digestDecision);
    const { rows: callDigests, dropped: droppedCalls } = truncateMiddle(callDigestsAll, MAX_CALL_ROWS);
    const { rows: skips, dropped: droppedSkips } = truncateMiddle(skipsAll, MAX_SKIP_ROWS);

    const pivotalRanked = pickPivotalDecisions(calls);
    const systemPrompt =
        pivotalRanked.find((d) => typeof d.prompt?.system === 'string' && d.prompt.system)?.prompt?.system ??
        calls.find((d) => typeof d.prompt?.system === 'string' && d.prompt.system)?.prompt?.system ??
        null;

    const lessonsShown = (input.library ?? []).map((l) => ({ id: l.id, scope: l.scope, lesson: l.lesson }));

    // Shrink the full-prompt set until the assembled message fits the budget.
    for (const maxFull of [12, 6, 3, 1, 0]) {
        const pivotal = pivotalRanked.slice(0, maxFull).sort((a, b) => a.decidedAtMs - b.decidedAtMs);
        const dossier: PostmortemDossier = {
            position: input.position,
            window: { from_utc: new Date(input.fromMs).toISOString(), to_utc: new Date(input.toMs).toISOString() },
            counts: {
                ai_calls: callDigestsAll.length,
                skipped_ticks: skipsAll.length,
                dropped_ai_calls: droppedCalls,
                dropped_skips: droppedSkips,
                full_prompts: pivotal.length,
            },
            ai_calls: callDigests,
            skipped_ticks: skips,
            pivotal_decision_ids: pivotal.map((d) => d.id),
            ...(lessonsShown.length ? { lessons_shown: lessonsShown } : {}),
        };
        const aiUserMessage = renderPostmortemUserMessage(dossier, pivotal, systemPrompt);
        if (aiUserMessage.length <= MAX_AI_USER_CHARS || maxFull === 0) {
            return { dossier, aiUserMessage };
        }
    }
    throw new Error('unreachable: dossier budget loop always returns at maxFull=0');
}

function renderPostmortemUserMessage(
    dossier: PostmortemDossier,
    pivotal: SwingDecisionFullRow[],
    systemPrompt: string | null,
): string {
    const parts: string[] = [];
    parts.push('## POSITION (closed — subject of this post-mortem)');
    parts.push(JSON.stringify(dossier.position, null, 1));
    parts.push(`## ANALYSIS WINDOW\n${dossier.window.from_utc} → ${dossier.window.to_utc}`);
    parts.push(
        `## COUNTS\n${JSON.stringify(dossier.counts)}${
            dossier.counts.dropped_ai_calls || dossier.counts.dropped_skips
                ? '\nNOTE: middle rows were dropped to fit; start and end of the window are complete.'
                : ''
        }`,
    );
    if (dossier.lessons_shown?.length) {
        parts.push(
            '## ACTIVE LESSON LIBRARY (already injected into the trading AI\'s prompts for this instrument — check adherence, never duplicate)',
        );
        parts.push(JSON.stringify(dossier.lessons_shown, null, 1));
    }
    parts.push('## AI CALL TIMELINE (chronological digests; tokens/model per call)');
    parts.push(JSON.stringify(dossier.ai_calls, null, 1));
    parts.push('## SKIPPED TICKS (gates blocked the AI; stage/reason + gate measurements)');
    parts.push(JSON.stringify(dossier.skipped_ticks, null, 1));
    if (systemPrompt) {
        parts.push('## TRADING-AI SYSTEM PROMPT (shared by all calls above)');
        parts.push(systemPrompt.slice(0, MAX_FULL_PROMPT_CHARS));
    }
    if (pivotal.length) {
        parts.push('## FULL USER PROMPTS AT PIVOTAL TICKS (exact market state the trading AI saw)');
        for (const d of pivotal) {
            const user = String(d.prompt?.user ?? '');
            parts.push(
                `--- decision_id=${d.id} ${new Date(d.decidedAtMs).toISOString()} action=${d.action ?? '?'} ---\n` +
                    user.slice(0, MAX_FULL_PROMPT_CHARS) +
                    (user.length > MAX_FULL_PROMPT_CHARS ? '\n[...prompt truncated]' : ''),
            );
        }
    }
    return parts.join('\n\n');
}

// ---------------------------------------------------------------------------
// The forensic analyst call
// ---------------------------------------------------------------------------
export const POSTMORTEM_SCHEMA = {
    name: 'swing_postmortem',
    schema: {
        type: 'object',
        additionalProperties: false,
        required: [
            'verdict',
            'confidence',
            'timeline_analysis',
            'what_went_wrong',
            'missed_signals',
            'gate_impact',
            'suggestions',
            'lesson_adherence',
            'lesson_action',
            'reinforce_lesson_id',
            'lesson',
            'lesson_scope',
        ],
        properties: {
            verdict: {
                type: 'string',
                enum: ['entry_flaw', 'management_flaw', 'stop_placement', 'exit_timing', 'bad_luck', 'process_gap'],
            },
            confidence: { type: 'number', minimum: 0, maximum: 1 },
            timeline_analysis: { type: 'string' },
            what_went_wrong: { type: 'array', items: { type: 'string' } },
            missed_signals: {
                type: 'array',
                items: {
                    type: 'object',
                    additionalProperties: false,
                    required: ['ts_utc', 'description', 'visible_in'],
                    properties: {
                        ts_utc: { type: 'string' },
                        description: { type: 'string' },
                        visible_in: { type: 'string', enum: ['ai_call', 'skipped_tick', 'post_exit'] },
                    },
                },
            },
            gate_impact: { type: ['string', 'null'] },
            suggestions: { type: 'array', items: { type: 'string' } },
            // Was an ACTIVE LESSON LIBRARY entry applicable to this trade, and
            // was it followed or violated? null = no applicable lesson existed.
            lesson_adherence: { type: ['string', 'null'] },
            // 'new' = the library lacks this failure mode; 'reinforce' = an
            // existing lesson applied (reinforce_lesson_id, optional reformulated
            // text in `lesson`); 'none' = nothing to teach (bad luck / already
            // covered with nothing to add).
            lesson_action: { type: 'string', enum: ['new', 'reinforce', 'none'] },
            reinforce_lesson_id: { type: ['integer', 'null'] },
            lesson: { type: ['string', 'null'] },
            lesson_scope: { type: ['string', 'null'] },
        },
    },
} as const;

const POSTMORTEM_SYSTEM_PROMPT = `You are a forensic trade post-mortem analyst for an automated swing-trading pipeline. You receive the complete recorded lifecycle of ONE closed position: the position outcome, a chronological digest of every AI decision call, every SKIPPED tick (where a pre-AI gate blocked the model from even looking, with the gate's measurements), the trading AI's system prompt, and the exact user prompts it saw at the pivotal ticks.

Your job: determine what actually went wrong and how to avoid it — measurements over narratives.

Rules:
- NO hindsight bias. Only fault a decision if information AVAILABLE AT THAT TIMESTAMP (in its prompt, the gate metrics, or earlier ticks) contradicted it. Price going the wrong way afterwards is not by itself an error — that verdict is 'bad_luck'.
- Anchor every claim to a timestamp from the dossier. Do not invent data that is not present.
- Explicitly examine the SKIPPED ticks: did a cooldown/dedupe/quarter-tick gate hide actionable information while the position was moving against its thesis? If yes, describe it in gate_impact; if no, set gate_impact to null.
- Judge the bracket geometry: was the stop at a level the recorded volatility (ATR fields in the prompts/metrics) made likely to be swept? Was the TP realistic for the holding window?
- Judge entry mechanics: market vs pullback limit, and whether a resting limit filled into momentum against the position (adverse selection).
- what_went_wrong: concrete defects, each one sentence. suggestions: concrete, implementable changes (gate thresholds, prompt wording, bracket sizing rules) — no platitudes.
- verdict: the SINGLE dominant failure. confidence below 0.5 means the data did not clearly separate the hypotheses — say so in timeline_analysis.
- ACTIVE LESSON LIBRARY (section in the dossier, when present): lessons distilled from PREVIOUS post-mortems that are already injected into the trading AI's prompts for this instrument. Handle it in three steps:
  1. Adherence: if a library lesson applied to this trade, state in lesson_adherence whether the trading AI FOLLOWED it or VIOLATED it (cite the tick). A violated lesson is an adherence failure, not a missing lesson. No applicable lesson → lesson_adherence null.
  2. lesson_action: 'new' ONLY for a failure mode the library does not yet cover (write the lesson text). 'reinforce' when an existing lesson covers this failure — set reinforce_lesson_id to its id; optionally put a reformulated text in the lesson field that absorbs the new case (≤220 chars), or null to keep its current wording. 'none' when there is nothing to teach: the loss happened DESPITE a sound process (verdict bad_luck), or the library already covers it and this case adds nothing. Never emit a duplicate of a library lesson as 'new'.
  3. Losing while following the process and the library is often just variance — do not invent a lesson to have something to say. A library bloated with near-duplicates and noise dilutes the trading AI's attention.
- lesson (when lesson_action='new' or a reformulation on 'reinforce'): 1-2 sentences, max ~220 characters, imperative voice, GENERALIZABLE (no symbol-specific price levels; ATR-relative or structural phrasing). It is shown to the trading AI before similar setups, so write it as an instruction to a trader, not commentary.
- lesson_scope (with a lesson; null otherwise): the audience — 'symbol' (a behavioral quirk of this one instrument), 'asset_class' (applies to this whole class, e.g. all crypto or all commodities), 'global' (sound for any instrument). Entry-mechanics and structure lessons usually generalize; pick 'symbol' only when the failure genuinely hinged on this instrument's specific behavior.

Respond with strict JSON per the provided schema.`;

export type PostmortemRunResult = {
    id: number;
    status: 'succeeded' | 'failed';
    verdict?: string | null;
    lesson?: string | null;
    error?: string;
};

// Runs one CLAIMED post-mortem row end-to-end and persists the outcome.
// The caller owns claiming (claimSwingPostmortemById / claimQueuedSwingPostmortems).
export async function runSwingPostmortem(row: SwingPostmortemRow): Promise<PostmortemRunResult> {
    try {
        const exitMs = row.exitTsMs ?? Date.now();
        const entryMs = row.entryTsMs ?? exitMs - POSTMORTEM_MAX_LIFETIME_MS;
        const fromMs = entryMs - POSTMORTEM_LOOKBACK_BEFORE_ENTRY_MS;
        const toMs = exitMs + POSTMORTEM_TAIL_AFTER_EXIT_MS;
        const assetClass = resolveSwingCategory({
            symbol: row.symbol,
            platform: row.platform as AnalysisPlatform,
        });
        const [decisions, ticks, library] = await Promise.all([
            loadSwingDecisionWindow({ symbol: row.symbol, platform: row.platform, fromMs, toMs }),
            loadSwingTickLog({ symbol: row.symbol, platform: row.platform, fromMs, toMs, limit: 3000 }),
            // Library slice shown to the analyst: adherence check + dedup —
            // fails open to [] (the analyst then simply can't reinforce).
            loadActiveSwingLessons({ symbol: row.symbol, assetClass }).catch(() => [] as SwingLessonRow[]),
        ]);
        if (!decisions.length && !ticks.length) {
            throw new Error(`no recorded ticks/decisions in window ${new Date(fromMs).toISOString()}..${new Date(toMs).toISOString()}`);
        }
        const position = {
            platform: row.platform,
            symbol: row.symbol,
            position_key: row.positionKey,
            side: row.side,
            entry_utc: row.entryTsMs ? new Date(row.entryTsMs).toISOString() : null,
            exit_utc: row.exitTsMs ? new Date(row.exitTsMs).toISOString() : null,
            entry_price: row.entryPrice,
            exit_price: row.exitPrice,
            pnl_pct: row.pnlPct,
            pnl_net: row.pnlNet,
        };
        const { dossier, aiUserMessage } = buildPostmortemDossier({ position, fromMs, toMs, decisions, ticks, library });
        const { json: report, model, usage } = await callSwingDecision({
            system: POSTMORTEM_SYSTEM_PROMPT,
            user: aiUserMessage,
            schema: POSTMORTEM_SCHEMA as unknown as { name: string; schema: Record<string, unknown> },
        });
        const verdict = typeof report?.verdict === 'string' ? report.verdict : null;
        if (!verdict) throw new Error('postmortem report missing verdict');
        // The analyst is library-aware: resolve its lesson_action (new /
        // reinforce / none) against the slice it was actually shown. 'none'
        // is a legitimate outcome — bad luck or an already-covered failure
        // teaches nothing new.
        const decision = resolveLessonDecision(report, library);
        // Row keeps the per-trade record: the (re)formulated text on new or
        // reinforce; null when there was nothing to teach.
        const lesson = decision.kind === 'none' ? null : decision.text;
        const lessonScope =
            lesson && ['symbol', 'asset_class', 'global'].includes(report?.lesson_scope)
                ? String(report.lesson_scope)
                : null;
        await completeSwingPostmortem(row.id, { verdict, lesson, lessonScope, report, dossier, model, usage });
        // Library write AFTER the row is safe — apply is best-effort anyway.
        const applied = await applyLessonDecision(decision, {
            postmortemId: row.id,
            symbol: row.symbol,
            assetClass,
        });
        console.log(
            `[postmortem] #${row.id} lesson decision: ${JSON.stringify({ kind: decision.kind, ...applied })}`,
        );
        return { id: row.id, status: 'succeeded', verdict, lesson };
    } catch (err: any) {
        const message = err?.message || String(err);
        await failSwingPostmortem(row.id, message).catch(() => undefined);
        return { id: row.id, status: 'failed', error: message };
    }
}
