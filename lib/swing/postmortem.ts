// Post-loss trade post-mortems (phase 2). When a closed position lands in
// swing.positions the close-persistence paths call maybeEnqueueSwingPostmortem;
// losses (default — SWING_POSTMORTEM_MODE=all|off widens/kills the filter) get
// one row in swing.postmortems and a detached worker request. The worker
// (pages/api/swing/postmortem.ts) rebuilds the trade's full tick series from
// swing.decisions (prompts included) + swing.tick_log, feeds the dossier to the
// swing AI provider with a forensic prompt, and persists the report plus a 1-2
// line lesson. Lessons are STORED for later prompt injection (phase 3) — no
// code feeds them back to the trading AI yet.
import { AiCallError } from '../aiError';
import { callSwingDecision } from '../aiProvider';
import type { PositionWindow } from '../analytics';
import { bitgetFetch, resolveProductType } from '../bitget';
import { fetchCapitalCandlesByEpicDateRange, resolveCapitalEpic } from '../capital';
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
    requeueSwingPostmortem,
    skipSwingPostmortem,
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
    // 'all' since win evaluations (docs/win-evaluation.md): losses get the
    // loss analyst, wins the win-evaluation analyst — the runner branches by
    // PnL sign. 'loss' restores the old losses-only behavior.
    return 'all';
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
    const pnl = postmortemPnl(w);
    // Unknown PnL is never enqueued (in ANY mode): Bitget closes can land
    // before realized ROI does — the next re-sync carries the PnL and
    // re-calls this (idempotent insert). The runner picks the analyst by the
    // stored PnL's sign, so a row must not be born before the sign is known.
    if (pnl == null) return false;
    if (mode === 'all') return true;
    return pnl < 0;
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
        if (id != null) {
            // Close-triggered rows wait out the post-close delay so the dossier
            // tail has real recorded data; the drain cron picks them up once
            // mature. Manual/backfill triggers (operator intent, usually old
            // closes) keep the immediate worker kick.
            const matureAtMs = (window.exitTimestamp ?? Date.now()) + resolveSwingPostmortemDelayMs();
            if (trigger !== 'close' || Date.now() >= matureAtMs) {
                triggerSwingPostmortemWorker(id);
            } else {
                console.log(
                    `[postmortem] #${id} queued for ${window.symbol}; matures ${new Date(matureAtMs).toISOString()} (post-close delay — drain cron runs it)`,
                );
            }
        }
        return id;
    } catch (err) {
        console.warn(`postmortem enqueue failed for ${window?.symbol}:`, err);
        return null;
    }
}

// Refusal investigations: a flat HOLD on a wake evaluation is a refusal with a
// well-defined counterfactual (the model itself chose the level and wrote the
// plan). Enqueue it into the SAME pipeline as loss post-mortems — one row,
// matured by the drain after the standard delay (12h), never kicked
// immediately: the whole point is judging the refusal AFTER the market showed
// what the declined trade would have done. The dossier is rebuilt from
// swing.decisions at run time (the refused evaluation's full prompt carries
// the band level, side, note and reason), so nothing extra is stored here.
export async function maybeEnqueueSwingRefusalInvestigation(params: {
    platform: string;
    symbol: string;
    decidedAtMs: number;
    priceAtEval?: number | null;
}): Promise<number | null> {
    try {
        if (resolveSwingPostmortemMode() === 'off') return null;
        const id = await enqueueSwingPostmortem({
            platform: params.platform,
            symbol: params.symbol,
            positionKey: `refusal:${String(params.symbol).toUpperCase()}:${Math.floor(params.decidedAtMs)}`,
            trigger: 'refusal',
            // exitTsMs is the drain's maturity anchor (runs at exit + delay);
            // for a refusal both ends of the "trade" are the refusal moment.
            entryTsMs: Math.floor(params.decidedAtMs),
            exitTsMs: Math.floor(params.decidedAtMs),
            entryPrice: params.priceAtEval ?? null,
        });
        if (id != null) {
            console.log(
                `[postmortem] refusal investigation #${id} queued for ${params.symbol}; drain runs it after the ${Math.round(
                    resolveSwingPostmortemDelayMs() / 3_600_000,
                )}h counterfactual window`,
            );
        }
        return id;
    } catch (err) {
        console.warn(`refusal investigation enqueue failed for ${params?.symbol}:`, err);
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

// Post-mortems are deliberately DELAYED past the close: the analyst needs to
// see what the market did after the exit (price reversing right past a swept
// stop → misplaced SL; continuation through the original target after an AI
// CLOSE → premature exit; continued adverse movement → the exit was right) to
// separate exit-quality defects from a genuinely broken thesis. Close-triggered
// rows only become claimable at exit + delay; the dossier tail spans the same
// window so it is fully recorded by the time the row runs.
export function resolveSwingPostmortemDelayMs(): number {
    const raw = Number(process.env.SWING_POSTMORTEM_DELAY_MINUTES);
    // 12h: long enough that both a close's aftermath AND a refusal's
    // counterfactual (winners historically resolve in 12-30h) carry real
    // signal by analysis time.
    const minutes = Number.isFinite(raw) && raw >= 0 ? raw : 720;
    return Math.floor(minutes * 60 * 1000);
}

export function postmortemTailAfterExitMs(): number {
    // Never below the pre-delay tail (1h) so a zero-delay config keeps the
    // original window semantics.
    return Math.max(60 * 60 * 1000, resolveSwingPostmortemDelayMs());
}

// ---------------------------------------------------------------------------
// Post-exit market summary: what price did AFTER the close, as measurements
// (high/low/last vs exit price) — the analyst judges premature-close /
// misplaced-SL from these, we don't pre-chew a verdict.
// ---------------------------------------------------------------------------
const POST_EXIT_CANDLE_TIMEFRAME = '15m';

// Pure over venue candle arrays ([ts, open, high, low, close, ...] — the shape
// both bitget and the Capital normalizer emit). Exported for tests.
export function summarizePostExitBars(params: {
    exitPrice: number | null | undefined;
    exitMs: number;
    toMs: number;
    bars: unknown[];
}): Record<string, unknown> | null {
    const exitPrice = Number(params.exitPrice);
    if (!(Number.isFinite(exitPrice) && exitPrice > 0)) return null;
    const rows = (Array.isArray(params.bars) ? params.bars : [])
        .map((b: any) => ({
            ts: Number(b?.[0]),
            high: Number(b?.[2]),
            low: Number(b?.[3]),
            close: Number(b?.[4]),
        }))
        .filter(
            (b) =>
                Number.isFinite(b.ts) &&
                b.ts >= params.exitMs &&
                b.ts <= params.toMs &&
                Number.isFinite(b.high) &&
                Number.isFinite(b.low) &&
                b.high > 0 &&
                b.low > 0,
        )
        .sort((a, b) => a.ts - b.ts);
    if (!rows.length) return null;
    const high = Math.max(...rows.map((b) => b.high));
    const low = Math.min(...rows.map((b) => b.low));
    const lastClose = rows[rows.length - 1].close;
    const pct = (p: number) => Number((((p - exitPrice) / exitPrice) * 100).toFixed(3));
    return {
        timeframe: POST_EXIT_CANDLE_TIMEFRAME,
        from_utc: new Date(params.exitMs).toISOString(),
        to_utc: new Date(rows[rows.length - 1].ts).toISOString(),
        bars: rows.length,
        exit_price: exitPrice,
        high,
        low,
        last_close: Number.isFinite(lastClose) && lastClose > 0 ? lastClose : null,
        max_up_from_exit_pct: pct(high),
        max_down_from_exit_pct: pct(low),
        last_from_exit_pct: Number.isFinite(lastClose) && lastClose > 0 ? pct(lastClose) : null,
    };
}

// Venue fetch wrapper. Best-effort: any failure returns null and the dossier
// simply omits the section (the post-mortem must never fail on candle reads).
async function buildPostExitMarketSummary(params: {
    platform: string;
    symbol: string;
    exitPrice: number | null | undefined;
    exitMs: number;
    toMs: number;
}): Promise<Record<string, unknown> | null> {
    const toMs = Math.min(params.toMs, Date.now());
    if (!(toMs > params.exitMs)) return null;
    try {
        let bars: unknown[] = [];
        if (String(params.platform).toLowerCase() === 'capital') {
            const epic = resolveCapitalEpic(params.symbol).epic;
            bars = await fetchCapitalCandlesByEpicDateRange(
                epic,
                POST_EXIT_CANDLE_TIMEFRAME,
                params.exitMs,
                toMs,
            );
        } else {
            bars = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
                symbol: params.symbol,
                productType: resolveProductType() as string,
                granularity: POST_EXIT_CANDLE_TIMEFRAME,
                startTime: String(params.exitMs),
                endTime: String(toMs),
                limit: '200',
            });
        }
        return summarizePostExitBars({
            exitPrice: params.exitPrice,
            exitMs: params.exitMs,
            toMs,
            bars: Array.isArray(bars) ? bars : [],
        });
    } catch (err) {
        console.warn(`[postmortem] post-exit candle read failed for ${params.platform}:${params.symbol}:`, err);
        return null;
    }
}
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
    // Price path recorded AFTER the close (the run is delayed past the exit so
    // this window exists) — exit-quality evidence for the analyst.
    post_exit_market?: Record<string, unknown>;
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
    // Post-exit price summary (buildPostExitMarketSummary) — optional, omitted
    // when candles were unavailable.
    postExitMarket?: Record<string, unknown> | null;
    // Refusal investigations: force the decision at this timestamp (±2 min) to
    // the FRONT of the pivotal ranking — the refused evaluation is the subject
    // and its full prompt must reach the analyst, but as a HOLD it would rank
    // last under the action-based scoring.
    focusTsMs?: number | null;
    // Section label for the subject block ("POSITION (closed …)" by default).
    subjectLabel?: string;
}): { dossier: PostmortemDossier; aiUserMessage: string } {
    const calls = input.decisions.filter((d) => !isSkipDecision(d) && !d.dryRun);
    const decisionSkips = input.decisions.filter((d) => isSkipDecision(d) && !d.dryRun).map(digestDecisionSkip);
    const tickSkips = input.ticks.filter((t) => t.kind === 'skip' && !t.dryRun).map(digestTickSkip);
    const skipsAll = mergeSkips(tickSkips, decisionSkips);

    const callDigestsAll = calls.map(digestDecision);
    const { rows: callDigests, dropped: droppedCalls } = truncateMiddle(callDigestsAll, MAX_CALL_ROWS);
    const { rows: skips, dropped: droppedSkips } = truncateMiddle(skipsAll, MAX_SKIP_ROWS);

    let pivotalRanked = pickPivotalDecisions(calls);
    if (input.focusTsMs) {
        const isFocus = (d: SwingDecisionFullRow) => Math.abs(d.decidedAtMs - Number(input.focusTsMs)) <= 120_000;
        pivotalRanked = [...calls.filter(isFocus), ...pivotalRanked.filter((d) => !isFocus(d))];
    }
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
            ...(input.postExitMarket ? { post_exit_market: input.postExitMarket } : {}),
        };
        const aiUserMessage = renderPostmortemUserMessage(
            dossier,
            pivotal,
            systemPrompt,
            input.subjectLabel ?? 'POSITION (closed — subject of this post-mortem)',
        );
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
    subjectLabel: string,
): string {
    const parts: string[] = [];
    parts.push(`## ${subjectLabel}`);
    parts.push(JSON.stringify(dossier.position, null, 1));
    parts.push(`## ANALYSIS WINDOW\n${dossier.window.from_utc} → ${dossier.window.to_utc}`);
    if (dossier.post_exit_market) {
        parts.push(
            '## POST-EXIT MARKET (price path recorded AFTER the close — exit-quality evidence only)',
        );
        parts.push(JSON.stringify(dossier.post_exit_market, null, 1));
    }
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
            // text in `lesson`); 'revise' = correct a shown lesson (rewrite its
            // text/bound, optionally move its scope) — the contradiction tool;
            // 'retire' = a shown lesson is wrong, remove it; 'none' = nothing to
            // teach. revise/retire also target via reinforce_lesson_id.
            lesson_action: { type: 'string', enum: ['new', 'reinforce', 'revise', 'retire', 'none'] },
            reinforce_lesson_id: { type: ['integer', 'null'] },
            lesson: { type: ['string', 'null'] },
            lesson_scope: { type: ['string', 'null'] },
        },
    },
} as const;

// Refusal investigations: the mirror analyst. Judges a DECLINED wake entry
// against what the market actually did afterwards. Unlike loss post-mortems,
// the counterfactual outcome is ADMISSIBLE evidence here — measuring it is the
// entire point of the 12h delay.
export const REFUSAL_INVESTIGATION_SCHEMA = {
    name: 'swing_refusal_investigation',
    schema: {
        type: 'object',
        additionalProperties: false,
        required: [
            'verdict',
            'confidence',
            'timeline_analysis',
            'counterfactual_outcome',
            'skip_reason_quality',
            'lesson_adherence',
            'lesson_action',
            'reinforce_lesson_id',
            'lesson',
            'lesson_scope',
        ],
        properties: {
            verdict: { type: 'string', enum: ['wrong_to_skip', 'right_to_skip', 'unclear'] },
            confidence: { type: 'number', minimum: 0, maximum: 1 },
            timeline_analysis: { type: 'string' },
            // What the declined trade would have done (levels, excursions, in
            // ATR terms where possible) — grounded in the post-refusal market
            // section, never invented.
            counterfactual_outcome: { type: 'string' },
            // Was the trading AI's stated refusal reason valid ON ITS OWN
            // TERMS (bounds quoted correctly, plan honored/overridden with
            // cause), independent of outcome?
            skip_reason_quality: { type: 'string' },
            lesson_adherence: { type: ['string', 'null'] },
            lesson_action: { type: 'string', enum: ['new', 'reinforce', 'revise', 'retire', 'none'] },
            reinforce_lesson_id: { type: ['integer', 'null'] },
            lesson: { type: ['string', 'null'] },
            lesson_scope: { type: ['string', 'null'] },
        },
    },
} as const;

// Win evaluations (docs/win-evaluation.md): the positive-polarity analyst.
// Same forensic discipline, flipped question — and the mirror hazard: a win
// must not retroactively validate a bad process (lucky_win = bad_luck's twin).
export const WIN_EVALUATION_SCHEMA = {
    name: 'swing_win_evaluation',
    schema: {
        type: 'object',
        additionalProperties: false,
        required: [
            'verdict',
            'confidence',
            'timeline_analysis',
            'what_worked',
            'exit_quality',
            'lesson_adherence',
            'lesson_action',
            'reinforce_lesson_id',
            'lesson',
            'lesson_scope',
        ],
        properties: {
            verdict: { type: 'string', enum: ['earned_win', 'lucky_win', 'exit_flaw'] },
            confidence: { type: 'number', minimum: 0, maximum: 1 },
            timeline_analysis: { type: 'string' },
            // The measurable conditions that made the trade work — each one
            // sentence, anchored to dossier timestamps/values.
            what_worked: { type: 'array', items: { type: 'string' } },
            // Judged against the post-exit price path: premature / well-timed /
            // late, with the numbers that say so.
            exit_quality: { type: 'string' },
            lesson_adherence: { type: ['string', 'null'] },
            lesson_action: { type: 'string', enum: ['new', 'reinforce', 'revise', 'retire', 'none'] },
            reinforce_lesson_id: { type: ['integer', 'null'] },
            lesson: { type: ['string', 'null'] },
            lesson_scope: { type: ['string', 'null'] },
        },
    },
} as const;

const WIN_EVALUATION_SYSTEM_PROMPT = `You are a forensic trade analyst for an automated swing-trading pipeline, running a WIN EVALUATION: the position under review CLOSED IN PROFIT. You receive its complete recorded lifecycle — the position outcome, every AI decision call, every skipped tick with gate measurements, the trading AI's system prompt, the exact prompts at pivotal ticks, and the price path recorded AFTER the exit.

Your job: decide whether this win was EARNED or LUCKY, judge the exit against what price did next, and feed what is repeatable back into the lesson library.

Rules:
- NO survivor bias — the mirror of hindsight bias: a profitable outcome does not retroactively validate the process. Only credit a decision if the information AVAILABLE AT ITS TIMESTAMP supported it. A trade that violated the library or its own plan and got paid anyway is 'lucky_win' — the most dangerous outcome in the dataset, because it teaches overconfidence if mishandled.
- verdict: 'earned_win' when entry, management and exit were each defensible on their own timestamps. 'lucky_win' when the profit arrived DESPITE a process flaw (violated an applicable lesson, chased an extended entry, overrode its own written plan without cause, or was rescued by news/variance). 'exit_flaw' when the win was real but the exit demonstrably leaked money — use the POST-EXIT MARKET section: continuation well past the exit price toward the original target = premature exit; most of the recorded in-trade MFE given back before the close = late exit; reversal shortly after the close = well-timed (that alone is not exit_flaw).
- what_worked: the repeatable, MEASURABLE conditions behind the win (each one sentence, anchored to dossier values). exit_quality: the exit judgment with its numbers.
- Anchor every claim to a timestamp or measured value from the dossier. Do not invent data.
- ACTIVE LESSON LIBRARY handling:
  1. Adherence: if a library lesson applied, state in lesson_adherence whether it was FOLLOWED or VIOLATED (cite the tick).
  2. lesson_action gates (code-enforced): on 'lucky_win' you MUST use 'none' — a lesson violated by a winning trade is NOT weakened by one lucky outcome, and "the violation worked" must never become doctrine; record the violation in lesson_adherence only. On 'earned_win': 'reinforce' the lesson whose condition shaped the win (reinforce_lesson_id — positive evidence counts like negative), or 'revise' a shown lesson whose bound ALMOST blocked this good trade (the win is evidence the bound is a notch too wide — corrected text with the adjusted number), or 'new' ONLY when the win hinged on a repeatable measurable condition the library does not cover. Before emitting 'new', check the candidate against EVERY shown lesson: if it would contradict one (opposite prescription for overlapping measured conditions), do not add it — 'revise' the existing lesson instead so the two reconcile into one rule with partitioned bounds. On 'exit_flaw': 'new'/'reinforce'/'revise' for exit-mechanics lessons. Never 'retire' from a win.
  3. Do not invent a lesson to have something to say — most earned wins are the process working and teach nothing new ('none').
- lesson (when writing/rewriting): 1-2 sentences, ≤220 chars, imperative, generalizable (ATR-relative/structural, no absolute price levels), and it MUST carry a numeric applicability bound. Positive-playbook lessons are allowed ("Prefer X when Y is within Z primary-ATR") but never platitudes.
- lesson_scope is ADVISORY on 'new' (code starts every lesson at this symbol and promotes on cross-symbol evidence); on 'revise' it moves the corrected lesson's scope, null keeps it.

Respond with strict JSON per the provided schema.`;

const POSTMORTEM_SYSTEM_PROMPT = `You are a forensic trade post-mortem analyst for an automated swing-trading pipeline. You receive the complete recorded lifecycle of ONE closed position: the position outcome, a chronological digest of every AI decision call, every SKIPPED tick (where a pre-AI gate blocked the model from even looking, with the gate's measurements), the trading AI's system prompt, and the exact user prompts it saw at the pivotal ticks.

Your job: determine what actually went wrong and how to avoid it — measurements over narratives.

Rules:
- NO hindsight bias. Only fault a decision if information AVAILABLE AT THAT TIMESTAMP (in its prompt, the gate metrics, or earlier ticks) contradicted it. Price going the wrong way afterwards is not by itself an error — that verdict is 'bad_luck'.
- Anchor every claim to a timestamp from the dossier. Do not invent data that is not present.
- Explicitly examine the SKIPPED ticks: did a gate (cooldown, dedupe, off-boundary bar-close cadence, quiet-position threshold) hide actionable information while the position was moving against its thesis? Remember the pipeline BY DESIGN only consults the model on primary bar closes and fences positions with the exchange bracket in between — a skip is only a defect if an ON-CADENCE look or a wider/different bracket could have acted on what the skip hid. If yes, describe it in gate_impact; if no, set gate_impact to null.
- Judge the bracket geometry: was the stop at a level the recorded volatility (ATR fields in the prompts/metrics) made likely to be swept? Was the TP realistic for the holding window?
- POST-EXIT MARKET (section, when present): the run is deliberately delayed past the close so you can see the price path AFTER the exit. Use it to judge EXIT quality only: a stop swept immediately before a reversal well past the stop level points to misplaced SL geometry; continuation through the original target after an AI CLOSE points to a premature exit; continued adverse movement CONFIRMS the exit was right. It must never retroactively fault the ENTRY or any in-trade decision — the no-hindsight rule above still governs those; this section exists solely so exit-mechanics verdicts rest on measurements instead of guesses.
- Judge entry mechanics: market vs pullback limit (resting limits only exist on trades from when that tool was enabled), and whether a resting limit filled into momentum against the position (adverse selection).
- what_went_wrong: concrete defects, each one sentence. suggestions: concrete, implementable changes (gate thresholds, prompt wording, bracket sizing rules) — no platitudes.
- verdict: the SINGLE dominant failure. confidence below 0.5 means the data did not clearly separate the hypotheses — say so in timeline_analysis.
- ACTIVE LESSON LIBRARY (section in the dossier, when present): lessons distilled from PREVIOUS post-mortems that are already injected into the trading AI's prompts for this instrument. Handle it in three steps:
  1. Adherence: if a library lesson applied to this trade, state in lesson_adherence whether the trading AI FOLLOWED it or VIOLATED it (cite the tick). A violated lesson is an adherence failure, not a missing lesson. No applicable lesson → lesson_adherence null.
  2. lesson_action: 'new' ONLY for a failure mode the library does not yet cover (write the lesson text). 'reinforce' when an existing lesson covers this failure — set reinforce_lesson_id to its id; optionally put a reformulated text in the lesson field that absorbs the new case (≤220 chars), or null to keep its current wording. 'revise' when a SHOWN lesson is wrong as written — too broad, missing its numeric bound, or CONTRADICTING another shown lesson: set reinforce_lesson_id to the lesson being corrected and put the full corrected text in lesson (you may also move its scope via lesson_scope, up or down). 'retire' when a shown lesson is simply wrong and unfixable — set reinforce_lesson_id. 'none' when there is nothing to teach: the loss happened DESPITE a sound process (verdict bad_luck), or the library already covers it and this case adds nothing. Never emit a duplicate of a library lesson as 'new'. Before emitting 'new', check the candidate against EVERY shown lesson: if it would contradict one (opposite prescription for overlapping measured conditions), do not add it — 'revise' the existing lesson instead so the two reconcile into one rule with partitioned bounds.
  3. Losing while following the process and the library is often just variance — do not invent a lesson to have something to say. A library bloated with near-duplicates and noise dilutes the trading AI's attention.
- lesson (when lesson_action='new', or the reformulated/corrected text on 'reinforce'/'revise'): 1-2 sentences, max ~220 characters, imperative voice, GENERALIZABLE (no symbol-specific price levels; ATR-relative or structural phrasing). It is shown to the trading AI before similar setups, so write it as an instruction to a trader, not commentary. Every lesson MUST carry a numeric applicability bound (an ATR distance, a time window, a count) that makes it checkable against measurements — an unbounded "do not X near Y" reads as a universal veto and WILL be applied far beyond your intent (a bound like "within 0.5 primary-ATR" both blocks the failure and clears the setups it never meant to touch).
- lesson_scope: ADVISORY on 'new' — code assigns every new lesson to this symbol and promotes it mechanically when later post-mortems reinforce it from other symbols or asset classes, so do not agonize over the audience. On 'revise' it moves the corrected lesson's scope (e.g. demote an over-generalized global back to its origin symbol); null keeps the current scope.

Respond with strict JSON per the provided schema.`;

const REFUSAL_INVESTIGATION_SYSTEM_PROMPT = `You are a forensic analyst for an automated swing-trading pipeline, conducting an INVESTIGATION of ONE REFUSED ENTRY: the trading AI was woken at a price level it had itself chosen to watch (a wake band with an attached plan note), evaluated the setup, and declined (HOLD). You receive the refused evaluation's EXACT prompt (market state, the plan note, active lessons), the AI's stated refusal reason, the surrounding tick timeline, and the price path recorded AFTER the refusal.

Your job: decide whether declining was right — and correct the rulebook when it was not.

Rules:
- Unlike a loss post-mortem, the OUTCOME IS ADMISSIBLE EVIDENCE here: the run is delayed ~12h precisely so the counterfactual is measurable. Describe it in counterfactual_outcome with numbers (how far past the level in primary-ATR terms, whether a stop just beyond the level would have been hit first, how long the move took) — read them from the POST-EXIT MARKET section (here: the post-REFUSAL path); never invent bars.
- verdict:
  - wrong_to_skip: the declined trade worked (roughly ≥1R before any sweep back through the level) AND the refusal reason does not hold up — it misapplied a lesson outside its stated numeric bound, re-demanded confirmation the wake had already established, or overrode the AI's own written plan without naming a genuine structural change.
  - right_to_skip: the declined trade failed or went nowhere, OR the refusal reasoning was sound on the evidence at the time even though the move later worked (a good decision can miss).
  - unclear: the counterfactual is genuinely unresolved at analysis time.
- skip_reason_quality: judge the refusal reason ON ITS OWN TERMS, outcome aside — were lesson bounds quoted with real measured values, was the plan note honored or overridden with a named cause?
- Library handling (the lever this analysis exists for):
  - right_to_skip: the only permitted library actions are 'reinforce' (credit the exact lesson whose bound correctly blocked this — reinforce_lesson_id) or 'none'. NEVER write a new restriction from a skip that worked; code enforces this.
  - wrong_to_skip: identify WHAT blocked the entry. If a shown lesson was misapplied or is too broad, 'revise' it — corrected text with a tighter/looser numeric bound, or a scope demotion (lesson_scope) back toward its origin; 'retire' it if it is simply wrong. If two shown lessons contradict each other, revise the one that is wrong. 'new' is rare here and reserved for a repeatable missed-entry failure mode (e.g. "re-demanding acceptance a confirmed wake already established") — same ≤220-char, imperative, numerically-bounded format as loss lessons. Before emitting 'new', check the candidate against EVERY shown lesson: if it would contradict one (opposite prescription for overlapping measured conditions), do not add it — 'revise' the existing lesson instead so the two reconcile into one rule with partitioned bounds.
  - Only lessons in the ACTIVE LESSON LIBRARY section may be reinforced/revised/retired; code drops actions on unlisted ids.
- lesson_scope is ADVISORY on 'new' (code starts every lesson at this symbol and promotes on cross-symbol evidence); on 'revise' it moves the corrected lesson's scope, null keeps it.
- Anchor every claim to a timestamp or measured value from the dossier. Do not judge trades or symbols other than this refusal.

Respond with strict JSON per the provided schema.`;

export type PostmortemRunResult = {
    id: number;
    status: 'succeeded' | 'failed' | 'skipped' | 'requeued';
    verdict?: string | null;
    lesson?: string | null;
    error?: string;
};

// A transient AI failure (rate limit, 5xx) is retried by requeueing, but not
// forever — a row that keeps bouncing eventually fails for good. Billing/
// config failures requeue WITHOUT a cap: they are gated at the drain (health
// flag), so attempts only grow at the outage edges, and the whole point is
// that the backlog runs itself once the subscription is paid.
const POSTMORTEM_MAX_TRANSIENT_ATTEMPTS = 5;

// Runs one CLAIMED post-mortem row end-to-end and persists the outcome.
// The caller owns claiming (claimSwingPostmortemById / claimQueuedSwingPostmortems).
// opts.force (manual regenerate): also analyze positions with an AI coverage
// gap — the default is to SKIP those (see below).
export async function runSwingPostmortem(
    row: SwingPostmortemRow,
    opts: { force?: boolean } = {},
): Promise<PostmortemRunResult> {
    try {
        const isRefusal = row.trigger === 'refusal';
        // Analyst selection by PnL sign (docs/win-evaluation.md): profitable
        // closes get the win-evaluation analyst. Enqueue guarantees the sign
        // is known before a row is born (unknown PnL is never enqueued).
        const rowPnl = [row.pnlNet, row.pnlPct].find((v) => typeof v === 'number' && Number.isFinite(v)) ?? null;
        const isWin = !isRefusal && rowPnl !== null && rowPnl > 0;
        const exitMs = row.exitTsMs ?? Date.now();
        const entryMs = row.entryTsMs ?? exitMs - POSTMORTEM_MAX_LIFETIME_MS;
        const fromMs = entryMs - POSTMORTEM_LOOKBACK_BEFORE_ENTRY_MS;
        const toMs = exitMs + postmortemTailAfterExitMs();
        const assetClass = resolveSwingCategory({
            symbol: row.symbol,
            platform: row.platform as AnalysisPlatform,
        });
        const [decisions, ticks, library, postExitMarket] = await Promise.all([
            loadSwingDecisionWindow({ symbol: row.symbol, platform: row.platform, fromMs, toMs }),
            loadSwingTickLog({ symbol: row.symbol, platform: row.platform, fromMs, toMs, limit: 3000 }),
            // Library slice shown to the analyst: adherence check + dedup —
            // fails open to [] (the analyst then simply can't reinforce).
            loadActiveSwingLessons({ symbol: row.symbol, assetClass }).catch(() => [] as SwingLessonRow[]),
            // What price did after the close (for refusals: after the refusal
            // moment, anchored at the price the model declined at) — the
            // reason the run is delayed.
            buildPostExitMarketSummary({
                platform: row.platform,
                symbol: row.symbol,
                exitPrice: isRefusal ? row.entryPrice : row.exitPrice,
                exitMs,
                toMs,
            }),
        ]);
        if (!decisions.length && !ticks.length) {
            throw new Error(`no recorded ticks/decisions in window ${new Date(fromMs).toISOString()}..${new Date(toMs).toISOString()}`);
        }
        // AI coverage gap: if in-position ticks died with ai_unavailable (AI
        // outage — quota lapse, key problem), the AI never fully managed this
        // trade. A post-mortem would judge a counterfactual — with the AI
        // present it might have closed the loser sooner — so skip it for good
        // instead of distilling misleading lessons. Only real (non-dryRun)
        // ticks inside the position's actual lifetime count. (Refusals are a
        // single evaluated moment — the gap concept does not apply.)
        if (!opts.force && !isRefusal) {
            const gapTicks = ticks.filter(
                (t) =>
                    t.stage === 'ai_unavailable' &&
                    !t.dryRun &&
                    t.tsMs >= entryMs &&
                    t.tsMs <= exitMs,
            );
            if (gapTicks.length) {
                const note = `skipped: ${gapTicks.length} ai_unavailable tick(s) during position (first ${new Date(gapTicks[0].tsMs).toISOString()}) — AI did not fully manage this trade`;
                await skipSwingPostmortem(row.id, note);
                console.log(`[postmortem] #${row.id} ${note}`);
                return { id: row.id, status: 'skipped', error: note };
            }
        }
        const position = isRefusal
            ? {
                  platform: row.platform,
                  symbol: row.symbol,
                  refusal_key: row.positionKey,
                  refused_at_utc: row.exitTsMs ? new Date(row.exitTsMs).toISOString() : null,
                  price_at_refusal: row.entryPrice,
              }
            : {
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
        const { dossier, aiUserMessage } = buildPostmortemDossier({
            position,
            fromMs,
            toMs,
            decisions,
            ticks,
            library,
            postExitMarket,
            // The refused evaluation is the subject: its full prompt must lead
            // the pivotal set (a HOLD would otherwise rank last).
            focusTsMs: isRefusal ? exitMs : null,
            subjectLabel: isRefusal
                ? 'REFUSED ENTRY INVESTIGATION (the declined wake — subject of this investigation; the POST-EXIT MARKET section shows the price path AFTER this refusal)'
                : isWin
                  ? 'POSITION (closed IN PROFIT — subject of this WIN EVALUATION)'
                  : undefined,
        });
        const { json: report, model, usage } = await callSwingDecision({
            system: isRefusal
                ? REFUSAL_INVESTIGATION_SYSTEM_PROMPT
                : isWin
                  ? WIN_EVALUATION_SYSTEM_PROMPT
                  : POSTMORTEM_SYSTEM_PROMPT,
            user: aiUserMessage,
            schema: (isRefusal
                ? REFUSAL_INVESTIGATION_SCHEMA
                : isWin
                  ? WIN_EVALUATION_SCHEMA
                  : POSTMORTEM_SCHEMA) as unknown as {
                name: string;
                schema: Record<string, unknown>;
            },
        });
        const verdict = typeof report?.verdict === 'string' ? report.verdict : null;
        if (!verdict) throw new Error('postmortem report missing verdict');
        // The analyst is library-aware: resolve its lesson_action (new /
        // reinforce / revise / retire / none) against the slice it was actually
        // shown. 'none' is a legitimate outcome — bad luck, a lucky win, a
        // correct skip, or an already-covered failure teaches nothing new.
        const decision = resolveLessonDecision(report, library, {
            kind: isRefusal ? 'refusal' : isWin ? 'win' : 'loss',
        });
        // Row keeps the per-trade record: the (re)formulated text on new or
        // reinforce; null when there was nothing to teach.
        const lesson = decision.kind === 'none' || decision.kind === 'retire' ? null : decision.text;
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
            originKind: isRefusal ? 'refusal' : isWin ? 'win' : 'loss',
        });
        console.log(
            `[postmortem] #${row.id} lesson decision: ${JSON.stringify({ kind: decision.kind, ...applied })}`,
        );
        return { id: row.id, status: 'succeeded', verdict, lesson };
    } catch (err: any) {
        const message = err?.message || String(err);
        // AI provider failures are retryable — the trade data is fine, only
        // the analyst was unreachable. Requeue instead of terminally failing
        // so the drain picks the row up again once the provider recovers
        // (billing: after the subscription is paid; transient: next pass,
        // capped so a persistent mystery error still lands in 'failed').
        if (err instanceof AiCallError) {
            const retryable =
                err.kind === 'billing' ||
                err.kind === 'config' ||
                (err.kind === 'transient' && row.attempts < POSTMORTEM_MAX_TRANSIENT_ATTEMPTS);
            if (retryable) {
                await requeueSwingPostmortem(row.id, message).catch(() => undefined);
                return { id: row.id, status: 'requeued', error: message };
            }
        }
        await failSwingPostmortem(row.id, message).catch(() => undefined);
        return { id: row.id, status: 'failed', error: message };
    }
}
