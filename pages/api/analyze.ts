// api/analyze.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { requireAdminAccess } from '../../lib/admin';

import {
    fetchMarketBundle as fetchBitgetMarketBundle,
    computeAnalytics,
    evaluateBitgetMinSizeAffordability,
    fetchBitgetAccountAvailableMarginUsd,
    fetchBitgetAccountEquityUsd,
    fetchPositionInfo as fetchBitgetPositionInfo,
    fetchRealizedRoi as fetchBitgetRealizedRoi,
    type PositionInfo,
} from '../../lib/analytics';
import { calculateMultiTFIndicators as calculateBitgetMultiTFIndicators } from '../../lib/indicators';
import { fetchNewsWithHeadlines, type Sentiment } from '../../lib/news';
import {
    calculateCapitalMultiTFIndicators,
    evaluateCapitalMinSizeAffordability,
    executeCapitalDecision,
    fetchCapitalAccountEquityUsd,
    getCapitalCategoryLeverage,
    fetchCapitalMarketBundle,
    fetchCapitalMarketTradeability,
    fetchCapitalPositionInfo,
    fetchCapitalRealizedRoi,
    cancelCapitalPendingEntryOrders,
    listCapitalPendingEntryOrders,
    resolveCapitalEpic,
    resolveCapitalEpicRuntime,
    type CapitalMarketTradeability,
} from '../../lib/capital';
import { resolveAnalysisPlatform, resolveInstrumentId, resolveNewsSource, type AnalysisPlatform } from '../../lib/platform';
import { resolveSwingCategory } from '../../lib/swing/category';
import { loadSwingCronControlState } from '../../lib/swing/cronControl';
import { recordSwingLastScan } from '../../lib/swing/lastScan';
import { buildEventReactionContext, swingEventReactionEnabled } from '../../lib/swing/eventReaction';
import { loadBtcContext } from '../../lib/swing/btcContext';
import { loadPerplexityContext } from '../../lib/swing/perplexity';
import { loadFearGreedContext } from '../../lib/swing/fearGreed';
import { runAiBouncer, swingAiBouncerEnabled, type AiBouncerVerdict } from '../../lib/swing/aiBouncer';
import { computeNanoContext } from '../../lib/swing/waveGeometry';
import { loadForexEventContext } from '../../lib/swing/forexEvents';
import { buildForexSessionLevelsContext } from '../../lib/swing/sessionLevels';
import { buildVenueSessionEvents } from '../../lib/swing/sessionEvents';

import { POSITION_WAKE_ENABLED, REENTRY_COOLDOWN_MIN, RESTING_ENTRY_MAX_AGE_MINUTES, resolveDecisionPolicy, resolveExtensionThresholds } from '../../lib/swing/decisionConfig';
import { SWING_DECISION_SCHEMA, SWING_DECISION_SCHEMA_NO_LEVERAGE } from '../../lib/swing/decisionSchema';
import { computeMomentumSignals, resolveReentryCooldown } from '../../lib/swing/signals';
import { computeSwingState } from '../../lib/swing/prompt';
import { postprocessDecision, sanitizeRestingEntry, sanitizeEntryTrigger, sanitizeExchangeTpSl, sanitizeHoldCooldown, sanitizePositionWake } from '../../lib/swing/decisionRules';
import type { DecisionPolicy, LastClosedPosition, MomentumSignals } from '../../lib/swing/decisionConfig';
import { AiCallError } from '../../lib/aiError';
import { callSwingDecision, resolveSwingAiProvider } from '../../lib/aiProvider';
import { truncateClaudeTranscript } from '../../lib/claudeAi';
import { getGates } from '../../lib/gates';

import {
    cancelPendingEntryOrders,
    fetchPendingEntryOrders,
    classifyPendingEntrySweep,
    executeDecision,
    fetchPositionTpsl,
    getTargetLeverage,
    getTradeProductType,
    type TradeDecision,
} from '../../lib/trading';
import { composePositionContext } from '../../lib/positionContext';
import { updatePositionExtrema } from '../../lib/positionExtrema';
import { appendDecisionHistory, loadDecisionHistory, type DecisionSnapshot } from '../../lib/history';
import { recordSwingAccountSnapshot } from '../../lib/swing/sync';
import { resolveRiskBasedSizing, RISK_EQUITY_PCT } from '../../lib/swing/riskSizing';
import {
    breakTriggerFailed,
    flatWakePlanStale,
    lastClosedBar,
    reclaimWakeEligible,
    RECLAIM_WAKE_FRESH_MINUTES,
    sessionLevelsRefKey,
    sessionSweepEventKey,
    timeframeToMs,
    type SessionLevelsRef,
    type SessionSweepEvent,
    wakeBandCrossed,
    wakeBreakConfirmAtr,
    wakeWatchRefKey,
    type WakeWatchRef,
} from '../../lib/swing/wakeWatch';
import { kvDel, kvGetJson, kvSetJson } from '../../lib/kv';
import { maybeEnqueueSwingPostmortem, maybeEnqueueSwingRefusalInvestigation } from '../../lib/swing/postmortem';
import { loadPromptLessons } from '../../lib/swing/lessons';
import {
    claimSwingAiCooldown,
    claimSwingReclaimLook,
    clearSwingAiCooldown,
    clearSwingBreakTrigger,
    endSwingAiThread,
    getSwingAiCooldown,
    getSwingAiThread,
    getSwingBreakTrigger,
    insertSwingTickLog,
    loadClosedSwingPositions,
    markSwingAiThreadInPosition,
    setSwingThreadWake,
    setSwingWakeTouch,
    upsertSwingAiCooldown,
    upsertSwingAiThread,
    upsertSwingBreakTrigger,
    upsertSwingPosition,
    type SwingWakeSweep,
} from '../../lib/swing/pg';
import {
    attachRecentActionOutcomes,
    collapseLimitReissues,
    type PositionForOutcome,
} from '../../lib/swing/recentActions';
import { invalidateSwingSummaryCache } from '../../lib/swing/summaryCache';
import { markSwingWarmDone, recordSwingAnalyzeFinished, swingWarmCycleId } from '../../lib/swing/warmLatch';
import { warmAllSwingSummaries } from './dashboard/summary';
import { warmChartCandlesFromAnalyze } from '../../lib/swing/chartCache';
import {
    invalidatePositionOverlayCache,
    warmPositionOverlayCacheFromAnalyze,
} from '../../lib/swing/positionOverlayCache';
import { reconcileCapitalClosedPositions } from '../../lib/swing/capitalWindows';
import {
    CONTEXT_TIMEFRAME,
    DEFAULT_NOTIONAL_USDT,
    MACRO_TIMEFRAME,
    MICRO_TIMEFRAME,
    NANO_TIMEFRAME,
    PRIMARY_TIMEFRAME,
} from '../../lib/constants';

// ------------------------------------------------------------------
// Small utilities
// ------------------------------------------------------------------
function parsePnlPct(p: string | undefined): number {
    if (!p) return 0;
    const m = String(p).match(/-?\d+(\.\d+)?/);
    return m ? Number(m[0]) : 0;
}

function safeNum(x: unknown, def = 0): number {
    const n = Number(x);
    return Number.isFinite(n) ? n : def;
}

// Structural view over the venue execution results — only the fields this
// file reads (both venues' result unions are assignable to it).
type ExecResultView = {
    placed?: unknown;
    closed?: unknown;
    reversed?: unknown;
    orderId?: unknown;
    clientOid?: unknown;
    tpsl?: {
        takeProfit?: { applied?: unknown } | null;
        stopLoss?: { applied?: unknown } | null;
        updated?: unknown;
        profitLevel?: unknown;
        stopLevel?: unknown;
    } | null;
};

async function persistCapitalClosedPositionSnapshot(params: {
    symbol: string;
    positionInfo: PositionInfo;
    execRes: ExecResultView | null;
    exitPrice: number | null;
    closedAtMs: number;
}) {
    if (params.positionInfo.status !== 'open') return;
    if (!params.execRes?.placed) return;
    if (!(params.execRes?.closed === true || params.execRes?.reversed === true)) return;

    const entryPrice = Number(params.positionInfo.entryPrice);
    const exitPrice = Number(params.exitPrice);
    const pnlPct = parsePnlPct(params.positionInfo.currentPnl);
    const entryTimestamp = Number(params.positionInfo.entryTimestamp);
    const positionKey = [
        'capital',
        params.symbol.toUpperCase(),
        Number.isFinite(entryTimestamp) && entryTimestamp > 0 ? Math.floor(entryTimestamp) : 'nots',
        Math.floor(params.closedAtMs),
        String(params.execRes.orderId || params.execRes.clientOid || 'close'),
    ].join(':');

    const window = {
        id: positionKey,
        symbol: params.symbol.toUpperCase(),
        side: (params.positionInfo.holdSide ?? null) as 'long' | 'short' | null,
        entryTimestamp: Number.isFinite(entryTimestamp) && entryTimestamp > 0 ? entryTimestamp : null,
        exitTimestamp: params.closedAtMs,
        entryPrice: Number.isFinite(entryPrice) && entryPrice > 0 ? entryPrice : null,
        exitPrice: Number.isFinite(exitPrice) && exitPrice > 0 ? exitPrice : null,
        pnlPct: Number.isFinite(pnlPct) ? pnlPct : null,
        pnlGrossPct: Number.isFinite(pnlPct) ? pnlPct : null,
        pnlNet: null,
        pnlGross: null,
        leverage:
            Number.isFinite(params.positionInfo.leverage as number) && (params.positionInfo.leverage as number) > 0
                ? (params.positionInfo.leverage as number)
                : null,
        notional: null,
    };
    try {
        await upsertSwingPosition('capital', { ...window, status: 'closed', leverageSource: 'captured' });
        // AI-initiated Capital closes never flow through the broker-merge sync,
        // so this is their only post-mortem enqueue point.
        await maybeEnqueueSwingPostmortem('capital', window);
    } catch (err) {
        console.warn(`Could not persist Capital closed position for ${params.symbol}:`, err);
    }
}

function timeframeToMinutes(tf: string): number {
    const match = String(tf).trim().toLowerCase().match(/^(\d+)\s*(m|h|d)$/);
    if (!match) return 0;
    const value = Number(match[1]);
    if (!Number.isFinite(value) || value <= 0) return 0;
    const unit = match[2];
    return unit === 'm' ? value : unit === 'h' ? value * 60 : value * 1440;
}

function isPrimaryCloseTime(tf: string, now = new Date(), toleranceMinutes = 2): boolean {
    const minutes = timeframeToMinutes(tf);
    if (!minutes) return true;
    const totalMinutes = now.getUTCHours() * 60 + now.getUTCMinutes();
    const remainder = totalMinutes % minutes;
    return remainder === 0 || remainder <= toleranceMinutes || remainder >= minutes - toleranceMinutes;
}

function firstHeaderValue(value: string | string[] | undefined): string {
    if (typeof value === 'string') return value.trim();
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim();
    return '';
}

function isAutomationCronRequest(req: NextApiRequest): boolean {
    const cronHeader = firstHeaderValue(req.headers['x-vercel-cron']);
    if (cronHeader) return true;
    const userAgent = firstHeaderValue(req.headers['user-agent']).toLowerCase();
    return userAgent.includes('vercel-cron');
}

// Crons fire every 15 minutes (see vercel.json); the :15/:30/:45 firings are
// "quarter ticks". FLAT symbols scan for entry windows 4x/hour (cheap: the full
// gate stack runs before any AI call, plus a no-new-information dedupe below).
// IN-POSITION quarter ticks are event-driven: the exchange-side TP/SL bracket
// already fences the position whenever the venue is trading (24/7 on Bitget;
// session hours on Capital), so the AI is only asked mid-hour when price has
// moved enough since its last look to plausibly change the answer — quiet
// quarter ticks keep the current bracket until the hourly tick. Tolerance
// mirrors isPrimaryCloseTime (cron jitter around the hour).
function isQuarterHourTick(now = new Date(), toleranceMinutes = 2): boolean {
    const minute = now.getUTCMinutes();
    return minute > toleranceMinutes && minute < 60 - toleranceMinutes;
}

// Flat quarter-tick dedupe: skip the AI call when price has barely moved since
// the last flat AI call that already answered HOLD for the SAME actionability
// branch under an hour ago. The 15m flat cadence exists to catch NEW entry
// windows early, not to re-ask about a standing "sitting on support" setup 4x
// an hour. 55min ceiling means hourly ticks are never deduped; fails open.
const FLAT_DEDUPE_MAX_AGE_MIN = 55;
const FLAT_DEDUPE_MAX_MOVE_ATR = 0.25;
// In-position quarter ticks run the full evaluation only when price has moved
// at least this many primary ATR from the last AI call's snapshot price
// (fallback: entry price). Below it, the standing TP/SL keeps managing.
const IN_POSITION_QUARTER_MOVE_ATR = (() => {
    const n = Number(process.env.SWING_INPOS_QUARTER_MOVE_ATR);
    return Number.isFinite(n) && n > 0 ? n : 0.5;
})();

// Real-swing cadence (default ON): cron ticks consult the AI only on primary
// (4H) bar closes — flat scans AND in-position management. Between closes the
// 15-min cron is a code-only watcher (bracket/thread reconcile, pending-entry
// sweep, chart warm, tick_log) and the exchange-side TP/SL bracket owns the
// position. Exceptions that still reach the AI off-boundary: a crossed flat
// wake band (the model explicitly asked to be woken at that level), an
// in-position move ≥ SWING_INPOS_EMERGENCY_MOVE_ATR primary ATR since the
// AI's last look, a swept resting entry that needs a re-issue decision, and
// manual/API calls. Off = the legacy 15-min AI cadence (churn mode).
const EVAL_PRIMARY_CLOSE_ONLY = (() => {
    const raw = String(process.env.SWING_EVAL_PRIMARY_CLOSE_ONLY ?? '')
        .trim()
        .toLowerCase();
    return !['0', 'false', 'no', 'off'].includes(raw);
})();
// Off-boundary in-position wake threshold under the 4H cadence. Deliberately
// far wider than IN_POSITION_QUARTER_MOVE_ATR: this is an emergency look
// ("something structural may have happened"), not routine management.
const IN_POSITION_EMERGENCY_MOVE_ATR = (() => {
    const n = Number(process.env.SWING_INPOS_EMERGENCY_MOVE_ATR);
    return Number.isFinite(n) && n > 0 ? n : 1.5;
})();

// ------------------------------------------------------------------
// In-memory position tracking for best-effort hold timing (resets on cold start).
// ------------------------------------------------------------------
type PersistState = {
    enteredAt?: number;
    lastSide?: 'long' | 'short';
};
const persist = new Map<string, PersistState>();

function touchPersist(key: string): PersistState {
    const existing = persist.get(key);
    if (existing) return existing;
    const created: PersistState = {};
    persist.set(key, created);
    return created;
}

const ATR_ACTIVE_MIN_PCT = 0.0007; // ~0.07%
const STALE_TRADE_MINUTES = 15;

// Tape rows arrive either as objects (ts/tradeTime fields) or positional
// arrays ([ts, ...]) — this is the minimal shape the timestamp read needs.
type TradeTapeRow = { ts?: unknown; tradeTime?: unknown; [index: number]: unknown };

function readLatestTradeTimestamp(trades: TradeTapeRow[]): number | null {
    if (!Array.isArray(trades) || trades.length === 0) return null;
    let latest = 0;
    for (const t of trades) {
        const tsRaw = Number(t?.ts ?? t?.tradeTime ?? t?.[0]);
        if (!Number.isFinite(tsRaw) || tsRaw <= 0) continue;
        const tsMs = tsRaw > 1e12 ? tsRaw : tsRaw * 1000;
        if (tsMs > latest) latest = tsMs;
    }
    return latest > 0 ? latest : null;
}

function shouldSkipMomentumCall(params: {
    signals: MomentumSignals;
    price: number;
    trades: TradeTapeRow[];
    enforceRecentTape?: boolean;
}) {
    const { signals, price, trades, enforceRecentTape = true } = params;
    const extensionActive = Math.abs(signals.microExtensionInAtr ?? 0) > 0.5;
    const primaryAtr = Number(signals.primaryAtr ?? 0);
    const atrPct = price > 0 && primaryAtr > 0 ? primaryAtr / price : 0;
    const atrActive = atrPct > ATR_ACTIVE_MIN_PCT;
    const momentumActive = extensionActive || atrActive;
    const latestTradeTs = readLatestTradeTimestamp(trades);
    const minutesSinceLastTrade = latestTradeTs ? (Date.now() - latestTradeTs) / 60000 : Infinity;
    const tapeInactive =
        !Array.isArray(trades) || trades.length === 0 || !Number.isFinite(minutesSinceLastTrade) || minutesSinceLastTrade > STALE_TRADE_MINUTES;
    if (!enforceRecentTape) return !momentumActive;
    return tapeInactive || !momentumActive;
}

/**
 * robustCvdFlip
 * - Requires sign flip AGAINST the position
 * - Requires magnitude + confirmation (mid return or OB imbalance)
 * - Requires persistence over >= 2 consecutive ticks
 * - Honors min-hold (ignore for first bar after entry)
 */

// ------------------------------------------------------------------
// Handler
// ------------------------------------------------------------------
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    // Non-null once this request is identified as a swing cron invocation; the
    // finally block below then counts it toward the cycle's warm latch.
    let swingWarmLatchCycleId: number | null = null;
    // Set once the tick is identified — lets the catch-all below leave a
    // durable tick_log row for a mid-flight crash. Without it a crashed run is
    // invisible (no tick, no decision): that's how the AVAX 2026-07-23 lost
    // wake stayed undiagnosed for two hours.
    let tickErrorContext: {
        symbol: string;
        platform: string;
        cadence: 'hourly' | 'quarter' | 'manual';
        dryRun: boolean;
    } | null = null;
    try {
        if (req.method !== 'GET') {
            return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
        }
        if (!requireAdminAccess(req, res)) return;

        const body = req.query ?? {};
        const requestPath = String(req.url || '/api/analyze').split('?')[0] || '/api/analyze';
        const symbolParam = Array.isArray(body.symbol) ? body.symbol[0] : body.symbol;
        const symbol = String(symbolParam || 'ETHUSDT').toUpperCase();
        const platformParam = Array.isArray(body.platform) ? body.platform[0] : body.platform;
        const platform: AnalysisPlatform = resolveAnalysisPlatform(platformParam as string | undefined);
        const newsSourceParam = Array.isArray(body.newsSource) ? body.newsSource[0] : body.newsSource;
        const newsSource = resolveNewsSource(platform, newsSourceParam as string | undefined);
        const categoryParam = Array.isArray(body.category) ? body.category[0] : body.category;
        const parseBoolParam = (value: string | string[] | undefined, fallback: boolean) => {
            if (value === undefined) return fallback;
            const v = Array.isArray(value) ? value[0] : value;
            if (v === undefined) return fallback;
            const normalized = String(v).toLowerCase();
            if (['false', '0', 'no', 'off'].includes(normalized)) return false;
            if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
            return fallback;
        };
        const timeFrame = PRIMARY_TIMEFRAME;
        const microTimeFrame = MICRO_TIMEFRAME;
        const macroTimeFrame = MACRO_TIMEFRAME;
        const contextTimeFrame = CONTEXT_TIMEFRAME;
        const dryRun = parseBoolParam(body.dryRun as string | string[] | undefined, false);
        const decisionPolicyParam = Array.isArray(body.decisionPolicy) ? body.decisionPolicy[0] : body.decisionPolicy;
        const decisionPolicy: DecisionPolicy = resolveDecisionPolicy(decisionPolicyParam as string | undefined);
        // The 4H-close cadence is normally controlled by the env flag
        // SWING_EVAL_PRIMARY_CLOSE_ONLY (default ON — see EVAL_PRIMARY_CLOSE_ONLY
        // above). This request param forces the gate on for a single call even
        // when the env flag is off (debug / manual boundary checks).
        const enforcePrimaryCloseGate = parseBoolParam(body.enforcePrimaryCloseGate as string | string[] | undefined, false);
        const debugGates = parseBoolParam(body.debugGates as string | string[] | undefined, false);
        const sideSizeUSDT = Number(body.notional ?? DEFAULT_NOTIONAL_USDT);
        const emitGateDebug = (stage: string, payload: Record<string, unknown>) => {
            if (!debugGates) return;
            try {
                console.log(
                    `[swing_gate_debug] ${JSON.stringify({
                        symbol,
                        platform,
                        stage,
                        route: requestPath,
                        ...payload,
                    })}`,
                );
            } catch {
                console.log(`[swing_gate_debug] symbol=${symbol} stage=${stage}`);
            }
        };

        const fetchMarketBundle = platform === 'capital' ? fetchCapitalMarketBundle : fetchBitgetMarketBundle;
        const calculateMultiTFIndicators =
            platform === 'capital' ? calculateCapitalMultiTFIndicators : calculateBitgetMultiTFIndicators;
        const fetchPositionInfo = platform === 'capital' ? fetchCapitalPositionInfo : fetchBitgetPositionInfo;
        const fetchRealizedRoi = platform === 'capital' ? fetchCapitalRealizedRoi : fetchBitgetRealizedRoi;
        let instrumentId =
            platform === 'capital' ? resolveCapitalEpic(symbol).epic : resolveInstrumentId(symbol, platform);
        let category = resolveSwingCategory({
            category: categoryParam as string | undefined,
            symbol,
            platform,
            instrumentId,
        });
        const automationCron = isAutomationCronRequest(req);
        // Set by the 1-minute wake-watcher on the analyze calls it fires
        // (wake=1). Those calls carry no Vercel cron headers, so without this
        // marker they'd be indistinguishable from manual operator ticks — and
        // would bypass the swing-cron hard-deactivation kill switch below.
        // Deliberately NOT folded into automationCron: wake fires must skip
        // the cadence gates (that is their purpose) and must not join the
        // warm-latch cycle count.
        const wakeFireRequest = parseBoolParam(body.wake as string | string[] | undefined, false);
        // Arm the warm latch as early as possible so EVERY swing cron invocation
        // counts — including gate skips, venue-closed exits and hard-deactivation
        // returns. If any early-return path bypassed the increment, the cycle's
        // count would never reach the cron total and the latch warm wouldn't fire.
        if (requestPath === '/api/swing/analyze' && automationCron) {
            swingWarmLatchCycleId = swingWarmCycleId(Date.now());
        }
        // Quarter ticks (:15/:30/:45, automation crons only) exist to scan FLAT
        // symbols for new entry windows; manual/API calls are never quarter
        // ticks and always run the full path.
        const quarterTick = automationCron && isQuarterHourTick();
        // Freshness marker for the dashboard: quarter-tick scans don't persist
        // decision rows, so this is the only evidence the 15m cadence ran.
        // AWAITED (not fire-and-forget): on serverless a void'd promise gets
        // dropped when the response ends first, which erased timeline dots.
        // recordSwingLastScan never throws, so this can't fail the tick.
        if (automationCron) await recordSwingLastScan(platform, symbol);
        const tickCadence = automationCron ? (quarterTick ? 'quarter' : 'hourly') : 'manual';
        tickErrorContext = { symbol, platform, cadence: tickCadence, dryRun };
        // Durable per-tick outcome (swing.tick_log): EVERY tick that ends —
        // gate skip or real AI call — leaves one Postgres row with the stage,
        // reason and gate measurements. Quarter-tick and cooldown skips never
        // reach swing.decisions and the KV scan-tick ring buffer only holds
        // ~2 days, so this is what lets a post-loss post-mortem reconstruct
        // the full tick series around a trade. kvMarker additionally stages
        // the skip on the KV last-scan marker (what the dashboard timeline
        // reads today); call sites keep their existing KV behavior. Both
        // writes are best-effort and never fail the tick.
        const recordTickOutcome = async (info: {
            kind: 'skip' | 'ai_call';
            stage: string;
            reason?: string;
            gates?: Record<string, unknown> | null;
            metrics?: Record<string, unknown> | null;
            kvMarker?: boolean;
        }) => {
            if (info.kvMarker !== false) {
                await recordSwingLastScan(platform, symbol, { stage: info.stage, reason: info.reason });
            }
            await insertSwingTickLog({
                tsMs: Date.now(),
                symbol,
                platform,
                kind: info.kind,
                stage: info.stage,
                reason: info.reason ?? null,
                cadence: tickCadence,
                dryRun,
                gates: info.gates ?? null,
                metrics: info.metrics ?? null,
            });
        };
        const persistPreAiSkip = async (params: {
            stage: string;
            decision: TradeDecision & Record<string, unknown>;
            execResult: Record<string, unknown>;
            gates?: Record<string, unknown>;
            metrics?: Record<string, unknown>;
            usedTape?: boolean;
            snapshot?: DecisionSnapshot & Record<string, unknown>;
        }) => {
            const reason = typeof params.decision?.reason === 'string' ? params.decision.reason : params.stage;
            await recordTickOutcome({
                kind: 'skip',
                stage: params.stage,
                reason,
                gates: params.gates ?? null,
                metrics: params.metrics ?? null,
                // Quarter ticks stage the skip on the KV marker (the timeline's
                // only view of them); hourly/manual skips surface via their
                // decision row below, same as before the tick log existed.
                kvMarker: quarterTick,
            });
            // Quarter ticks don't persist skip DECISION ROWS: gate short-circuits
            // already get recorded on the hourly tick, and 3 more identical
            // rows/hour/symbol would only be noise. The durable tick_log row
            // above still captures them. Real AI calls (past all gates) always
            // persist.
            if (quarterTick) return;
            await appendDecisionHistory({
                timestamp: Date.now(),
                symbol,
                category: category ?? undefined,
                platform,
                instrumentId,
                newsSource,
                timeFrame,
                dryRun,
                prompt: null,
                aiDecision: {
                    ...params.decision,
                    decision_source: 'pre_ai_skip',
                    promptSkipped: true,
                    skipStage: params.stage,
                },
                execResult: params.execResult,
                snapshot: {
                    category: category ?? undefined,
                    platform,
                    newsSource,
                    instrumentId,
                    promptSkipped: true,
                    skipStage: params.stage,
                    skipReason: reason,
                    usedTape: Boolean(params.usedTape),
                    gates: params.gates,
                    metrics: params.metrics,
                    ...(params.snapshot ?? {}),
                },
                biasTimeframes: {
                    context: contextTimeFrame,
                    macro: macroTimeFrame,
                    primary: timeFrame,
                    micro: microTimeFrame,
                },
            });
            // A new decision was recorded → bust the dashboard summary cache so the
            // next load reflects it. Best-effort; never blocks the trading path.
            await invalidateSwingSummaryCache();
        };
        // The kill switch binds every AUTOMATED invocation: the 15-min crons
        // AND wake-watcher fires (wake=1). Only genuine manual operator calls
        // (dashboard button, direct API hit) stay exempt.
        const isSwingCronAnalyzeRequest = requestPath === '/api/swing/analyze' && (automationCron || wakeFireRequest);
        if (isSwingCronAnalyzeRequest) {
            const swingCronControl = await loadSwingCronControlState();
            if (swingCronControl.hardDeactivated) {
                emitGateDebug('swing_cron_hard_deactivated', {
                    gate: 'SWING_CRON_HARD_DEACTIVATED',
                    hardDeactivated: true,
                    updatedAtMs: swingCronControl.updatedAtMs,
                    updatedBy: swingCronControl.updatedBy,
                    reason: swingCronControl.reason,
                });
                const decision: TradeDecision & Record<string, unknown> = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'swing_cron_hard_deactivated',
                    reason: 'swing_cron_hard_deactivated',
                };
                const execRes = { placed: false, orderId: null, clientOid: null, reason: 'swing_cron_hard_deactivated' };
                // Quarter ticks skip persistence: with 15m crons this branch would
                // otherwise write 4 identical skip rows/hour/symbol while deactivated.
                if (!quarterTick) {
                    await persistPreAiSkip({
                        stage: 'swing_cron_hard_deactivated',
                        decision,
                        execResult: execRes,
                        snapshot: {
                            cronControl: {
                                hardDeactivated: true,
                                updatedAtMs: swingCronControl.updatedAtMs,
                                updatedBy: swingCronControl.updatedBy,
                                reason: swingCronControl.reason,
                            },
                        },
                    });
                }
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision,
                    execRes,
                    usedTape: false,
                    promptSkipped: true,
                    cronControl: swingCronControl,
                    ...(debugGates
                        ? {
                              gateDebug: {
                                  blockedBy: 'SWING_CRON_HARD_DEACTIVATED',
                                  hardDeactivated: true,
                                  updatedAtMs: swingCronControl.updatedAtMs,
                                  updatedBy: swingCronControl.updatedBy,
                                  reason: swingCronControl.reason,
                              },
                          }
                        : {}),
                });
            }
        }

        // Skip the (expensive) AI call entirely when the Capital.com market is
        // closed — orders can't execute anyway. Bitget crypto trades 24/7 so
        // this gate only applies to the Capital platform. The fetched market
        // info (session timing, overnight funding) is kept for the prompt's
        // venue-session/costs context further down.
        let capitalMarketInfo: CapitalMarketTradeability | null = null;
        if (platform === 'capital') {
            const tradeability = await fetchCapitalMarketTradeability(symbol);
            capitalMarketInfo = tradeability;
            if (!tradeability.tradeable) {
                emitGateDebug('capital_market_closed', {
                    gate: 'CAPITAL_MARKET_CLOSED',
                    marketStatus: tradeability.status,
                });
                const decision: TradeDecision & Record<string, unknown> = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'capital_market_closed',
                    reason: `capital_market_closed:${tradeability.status ?? 'unknown'}`,
                };
                const execRes = { placed: false, orderId: null, clientOid: null, reason: 'capital_market_closed' };
                // Quarter ticks skip persistence: with 15m crons a closed market
                // (nights/weekends) would otherwise write 4 skip rows/hour/symbol.
                if (!quarterTick) {
                    await persistPreAiSkip({
                        stage: 'capital_market_closed',
                        decision,
                        execResult: execRes,
                        snapshot: { marketStatus: tradeability.status },
                    });
                }
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision,
                    execRes,
                    usedTape: false,
                    promptSkipped: true,
                    marketStatus: tradeability.status,
                    ...(debugGates
                        ? {
                              gateDebug: {
                                  blockedBy: 'CAPITAL_MARKET_CLOSED',
                                  marketStatus: tradeability.status,
                              },
                          }
                        : {}),
                });
            }
        }

        const positionInfo = await fetchPositionInfo(symbol);
        const positionOpen = positionInfo.status === 'open';
        // Post-open warmup gate (Capital only; crypto never closes): the first
        // minutes after a session opens are auction noise — spreads are wide
        // and every indicator still reflects the prior session plus one
        // distorted gap candle, so fresh-entry judgment is at its worst exactly
        // when overnight gaps cross wake bands and fire early looks. Hard-skip
        // FLAT ticks of EVERY cadence (wake-fired calls are classified manual,
        // and they're the ones that hit right at open) until the session is
        // SWING_CAPITAL_OPEN_WARMUP_MINUTES old. In-position ticks pass: the
        // first look at a position that gapped over the closure is the look
        // that matters most. No schedule data (openedAtMs null) fails open.
        const openWarmupMinutes = (() => {
            const raw = Number(process.env.SWING_CAPITAL_OPEN_WARMUP_MINUTES ?? 30);
            return Number.isFinite(raw) && raw > 0 ? Math.floor(raw) : 0;
        })();
        const sessionOpenedAtMs = capitalMarketInfo?.session?.openedAtMs ?? null;
        if (platform === 'capital' && !positionOpen && openWarmupMinutes > 0 && sessionOpenedAtMs !== null) {
            const minutesSinceOpen = (Date.now() - sessionOpenedAtMs) / 60_000;
            if (minutesSinceOpen >= 0 && minutesSinceOpen < openWarmupMinutes) {
                emitGateDebug('open_warmup_gate', {
                    gate: 'OPEN_WARMUP',
                    minutesSinceOpen: Math.round(minutesSinceOpen),
                    warmupMinutes: openWarmupMinutes,
                });
                const reason = `open_warmup:${Math.round(minutesSinceOpen)}m_of_${openWarmupMinutes}m_since_open`;
                const decision: TradeDecision & Record<string, unknown> = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'open_warmup_gate',
                    reason,
                };
                const execRes = { placed: false, orderId: null, clientOid: null, reason: 'open_warmup_gate' };
                // Quarter ticks skip persistence (same policy as the closed-
                // market gate) — the warmup window would otherwise write a
                // burst of skip rows every session open.
                if (!quarterTick) {
                    await persistPreAiSkip({
                        stage: 'open_warmup_gate',
                        decision,
                        execResult: execRes,
                        snapshot: { sessionOpenedAtMs, warmupMinutes: openWarmupMinutes },
                    });
                }
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision,
                    execRes,
                    usedTape: false,
                    promptSkipped: true,
                    ...(debugGates
                        ? {
                              gateDebug: {
                                  blockedBy: 'OPEN_WARMUP',
                                  minutesSinceOpen: Math.round(minutesSinceOpen),
                                  warmupMinutes: openWarmupMinutes,
                              },
                          }
                        : {}),
                });
            }
        }
        // Bounded account-leverage history: one snapshot per HOURLY tick per
        // symbol (quarter ticks skip it so the flat 15m cadence doesn't 4x the
        // table). Best-effort; never blocks the trading path on failure.
        if (!quarterTick) {
            // Account equity is NOT in positionInfo (per-symbol) — one broker
            // call per hourly snapshot buys the durable equity curve the weekly
            // digest draws down/drawdown from. Best-effort: null on failure.
            const snapshotEquityUsd =
                platform === 'capital'
                    ? await fetchCapitalAccountEquityUsd().catch(() => null)
                    : await fetchBitgetAccountEquityUsd().catch(() => null);
            await recordSwingAccountSnapshot({
                platform,
                symbol,
                capturedAtMs: Date.now(),
                positionInfo,
                equityUsd: snapshotEquityUsd,
            });
        }
        const primaryCloseTime = isPrimaryCloseTime(timeFrame);
        // 4H-close cadence active for this tick? Env flag governs cron ticks;
        // the request param forces it for manual calls. The gate itself sits
        // AFTER the watcher surface below (bracket read, thread reconcile,
        // pending-entry sweep, chart warm) so off-boundary ticks still do all
        // code-level upkeep — they only skip the AI.
        const primaryCloseCadence =
            (EVAL_PRIMARY_CLOSE_ONLY && automationCron) || enforcePrimaryCloseGate;
        const offBoundaryTick = primaryCloseCadence && !primaryCloseTime;
        if (offBoundaryTick) {
            emitGateDebug('primary_close_off_boundary', {
                gate: 'PRIMARY_CLOSE_TIME',
                primaryCloseTime,
                positionOpen,
                enforcePrimaryCloseGate,
                evalPrimaryCloseOnly: EVAL_PRIMARY_CLOSE_ONLY,
                timeFrame,
            });
        }

        // Margin-aware pre-skip (flat only): if the account can't cover the
        // smallest tradeable size, opening is impossible — skip before spending
        // the AI call and record it as an intentional skip rather than letting
        // it surface later as a venue rejection (Capital
        // INSUFFICIENT_AVAILABLE_MARGIN / Bitget 40762). Open positions are
        // exempt: HOLD/CLOSE still need to run to manage them. Fails open.
        if (!positionOpen) {
            const afford =
                platform === 'capital'
                    ? await evaluateCapitalMinSizeAffordability(symbol).catch(() => null)
                    : await evaluateBitgetMinSizeAffordability().catch(() => null);
            if (afford && afford.affordable === false) {
                const need =
                    typeof afford.requiredMarginUsd === 'number' ? Math.ceil(afford.requiredMarginUsd) : null;
                const have =
                    typeof afford.availableMarginUsd === 'number' ? Math.floor(afford.availableMarginUsd) : null;
                const reason =
                    need !== null && have !== null
                        ? `insufficient_margin_min_size:need≈${need} have≈${have}`
                        : 'insufficient_margin_min_size';
                emitGateDebug('insufficient_margin', {
                    gate: 'INSUFFICIENT_MARGIN',
                    availableMarginUsd: afford.availableMarginUsd,
                    requiredMarginUsd: afford.requiredMarginUsd,
                    minNotionalUsd: afford.minNotionalUsd,
                    minDealSize: afford.minDealSize,
                    leverage: afford.leverage,
                });
                const decision: TradeDecision & Record<string, unknown> = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'insufficient_margin',
                    reason,
                };
                const execRes = { placed: false, orderId: null, clientOid: null, reason: 'insufficient_margin' };
                await persistPreAiSkip({
                    stage: 'insufficient_margin',
                    decision,
                    execResult: execRes,
                    snapshot: { margin: afford },
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision,
                    execRes,
                    usedTape: false,
                    promptSkipped: true,
                    ...(debugGates
                        ? {
                              gateDebug: {
                                  blockedBy: 'INSUFFICIENT_MARGIN',
                                  reason,
                                  availableMarginUsd: afford.availableMarginUsd,
                                  requiredMarginUsd: afford.requiredMarginUsd,
                                  minNotionalUsd: afford.minNotionalUsd,
                                  leverage: afford.leverage,
                              },
                          }
                        : {}),
                });
            }
        }

        // 1) Product & parallel baseline fetches (fast)
        const productType = platform === 'bitget' ? getTradeProductType() : null;

        // Standing exchange-side bracket: fed into the prompt (so TP/SL
        // amendments are made against the actual resting levels) AND into the
        // chart overlay warm below (drawn as TP/SL lines in the UI). Capital
        // exposes it on the position row; Bitget resting TP/SL live as plan
        // orders and need their own read. Best-effort — a failure omits them.
        let currentTakeProfit: number | null = null;
        let currentStopLoss: number | null = null;
        if (positionOpen) {
            if (platform === 'bitget' && productType) {
                try {
                    const standingTpsl = await fetchPositionTpsl(symbol, productType);
                    currentTakeProfit = standingTpsl.takeProfit?.price ?? null;
                    currentStopLoss = standingTpsl.stopLoss?.price ?? null;
                } catch (err) {
                    console.warn(`Could not read standing TP/SL plans for ${symbol}:`, err);
                }
            } else if (positionInfo.status === 'open') {
                currentTakeProfit = positionInfo.takeProfitPrice ?? null;
                currentStopLoss = positionInfo.stopLossPrice ?? null;
            }
        }

        // Responses-API thread (per-order conversation chain). A thread starts
        // when an entry order is placed, survives a resting-entry fill into
        // position management AND unfilled-limit re-evaluations (sweep +
        // re-issue keeps the same conversation), and ends when the entry is
        // dropped without a re-issue or the position closes. Reconciled here
        // against broker reality:
        //   in_position + no open position → closed since last tick (TP/SL fill,
        //     manual close, executed CLOSE) → conversation over;
        //   pending_entry + open position → the limit filled → same conversation
        //     now manages the position;
        //   pending_entry + flat → the limit is (or just was) resting → this
        //     tick's evaluation chains onto the order's conversation ("market
        //     moved since you placed this — still valid?"). The sweep below
        //     still cancels the order and deletes the row; a re-issue upserts a
        //     new head that CONTINUES the same OpenAI chain via this id.
        // Best-effort: a thread hiccup degrades the tick to stateless, never fails it.
        let aiThreadResponseId: string | null = null;
        // Which provider wrote the thread row + the Claude transcript (captured
        // in memory here so it survives the sweep deleting the row mid-tick —
        // same semantics as the OpenAI chain head above). A provider mismatch
        // (row written by the other model family) degrades the CONVERSATION to
        // stateless at the call site, but thread lifecycle (pending-entry flag,
        // sweeps) is provider-independent and keeps using the row as-is.
        let aiThreadProvider: string | null = null;
        let aiThreadTranscript: unknown[] | null = null;
        // In-position wake bands armed on this thread by a previous management
        // look (ENABLE_POSITION_WAKE_BANDS) — checked against live price below
        // and echoed to the prompt; replaced after every real AI decision.
        let threadWake: { above: number | null; below: number | null; note: string | null; setAtMs: number | null } | null =
            null;
        // The thread row claims a resting entry is live while we are flat —
        // cross-checked against what the hourly sweep actually finds below.
        let aiThreadWasPendingEntry = false;
        if (!dryRun) {
            try {
                const aiThread = await getSwingAiThread(platform, symbol);
                if (aiThread) {
                    aiThreadWasPendingEntry = aiThread.status === 'pending_entry' && !positionOpen;
                    if (positionOpen) {
                        if (aiThread.status === 'pending_entry') {
                            await markSwingAiThreadInPosition(platform, symbol);
                        }
                        aiThreadResponseId = aiThread.lastResponseId;
                        aiThreadProvider = aiThread.provider;
                        aiThreadTranscript = aiThread.transcript;
                        if (POSITION_WAKE_ENABLED && (aiThread.wakeAbove !== null || aiThread.wakeBelow !== null)) {
                            threadWake = {
                                above: aiThread.wakeAbove,
                                below: aiThread.wakeBelow,
                                note: aiThread.wakeNote,
                                setAtMs: aiThread.wakeSetAtMs,
                            };
                        }
                    } else if (aiThread.status === 'in_position') {
                        await endSwingAiThread(platform, symbol);
                        // The previous tick had a position, now flat with no AI CLOSE
                        // in between ⇒ the venue closed it (TP/SL bracket, stop-out,
                        // manual). No execution path persisted that close, so pull it
                        // from Capital's transaction history now — otherwise it stays
                        // invisible to the chart and PnL until a dashboard-summary
                        // load happens to reconcile it. (Bitget needs no equivalent:
                        // its chart/summary reads always broker-merge recent windows.)
                        // Best-effort: reconcile never throws, and a cache drop only
                        // follows an actual write.
                        if (platform === 'capital') {
                            const persisted = await reconcileCapitalClosedPositions(symbol);
                            if (persisted > 0) {
                                await invalidatePositionOverlayCache({ symbol, platform });
                            }
                        }
                    } else {
                        aiThreadResponseId = aiThread.lastResponseId;
                        aiThreadProvider = aiThread.provider;
                        aiThreadTranscript = aiThread.transcript;
                    }
                }
            } catch (err) {
                console.warn(`AI thread load failed for ${symbol}:`, err);
            }
        }

        // Resting entries. A standing entry SURVIVES evaluations, so the sweep
        // is no longer something a tick does by default — it runs only when the
        // age backstop fires (below) or when the decision itself supersedes or
        // withdraws the order (after the AI call). What every flat tick does do
        // is READ, so the model can see what stands and decide its fate.
        // If a cancel fails because the order just FILLED, the tick stops:
        // a position now exists and the next tick manages it.
        // Any OTHER unclean sweep (helper threw, pending-orders fetch failed,
        // cancel failed without a fill) also stops the tick — the previous
        // order may still be live on the venue, and a fresh entry on top of it
        // stacks exposure (fail closed; the DE40 double fill 2026-07-13).
        // What the sweep found, normalized for the prompt: the AI decides fresh
        // each evaluation, but it should KNOW its previous resting entry rested
        // without filling (re-issue vs chase vs drop is its call to make).
        let sweptPendingEntry: { side: 'BUY' | 'SELL' | null; price: number | null; age_min: number | null } | null =
            null;
        // What is resting on the venue RIGHT NOW, read without cancelling.
        //
        // A resting entry is a standing commitment and survives evaluations: the
        // model sees it as state.position.resting_entry and decides what happens
        // to it (HOLD leaves it, a fresh BUY/SELL supersedes it, a market entry
        // or withdraw_resting_entry cancels it). Before 2026-08-30 the tick
        // cancelled unconditionally BEFORE asking, so the model could only ever
        // see the corpse (cancelled_pending_entry) and silence destroyed the
        // commitment — the opposite of the in-position bracket, where null means
        // "leave the standing leg alone".
        //
        // Returns null on a read failure, which the caller treats as fail-closed:
        // not knowing what rests means we must not place anything on top of it.
        const readPendingEntries = async () => {
            try {
                const orders =
                    platform === 'capital' || productType === null
                        ? await listCapitalPendingEntryOrders(symbol)
                        : await fetchPendingEntryOrders(symbol, productType);
                return orders;
            } catch (err) {
                console.warn(`pending entry read failed for ${symbol}:`, err);
                return null;
            }
        };
        const describeRestingEntry = (first: { createdAtMs?: number | null } & Record<string, unknown>) => {
            const sideRaw = String(('side' in first ? first.side : first.direction) ?? '').toUpperCase();
            const price = Number('price' in first ? first.price : first.level);
            const createdAtMs = Number(first.createdAtMs);
            return {
                side: sideRaw === 'BUY' || sideRaw === 'SELL' ? (sideRaw as 'BUY' | 'SELL') : null,
                price: Number.isFinite(price) && price > 0 ? price : null,
                age_min:
                    Number.isFinite(createdAtMs) && createdAtMs > 0
                        ? Math.max(0, Math.round((Date.now() - createdAtMs) / 60_000))
                        : null,
            };
        };
        const sweepPendingEntries = async () => {
            try {
                const result =
                    platform === 'capital' || productType === null
                        ? await cancelCapitalPendingEntryOrders(symbol)
                        : await cancelPendingEntryOrders(symbol, productType);
                const first = result.orders?.[0];
                if (first) sweptPendingEntry = describeRestingEntry(first);
                // Resting entry cancelled without filling → delete the thread
                // ROW (no order is resting anymore, so the pendingEntry flag
                // must drop) — but the CONVERSATION survives: this tick's AI
                // call chains via the head captured above, and a re-issue
                // upserts a new row continuing the same OpenAI chain. A cancel
                // that raced a fill (cancelled < found) is handled by the caller
                // via pendingEntryFilledMidTick.
                if (!dryRun && result.found > 0 && result.cancelled >= result.found) {
                    await endSwingAiThread(platform, symbol).catch((err) =>
                        console.warn(`AI thread end failed for ${symbol}:`, err),
                    );
                }
                return result;
            } catch (err) {
                console.warn(`pending entry sweep failed for ${symbol}:`, err);
                return null;
            }
        };
        const pendingEntryFilledMidTick = async (
            sweep: Awaited<ReturnType<typeof sweepPendingEntries>>,
        ): Promise<boolean> => {
            if (!sweep || sweep.found === 0 || sweep.cancelled >= sweep.found) return false;
            const recheck = await fetchPositionInfo(symbol).catch(() => null);
            const filled = recheck?.status === 'open';
            // Limit filled while we were cancelling it → a position exists; the
            // pending-entry conversation now manages it from the next tick on.
            if (filled && !dryRun) {
                await markSwingAiThreadInPosition(platform, symbol).catch((err) =>
                    console.warn(`AI thread fill-transition failed for ${symbol}:`, err),
                );
            }
            return filled;
        };
        // What is standing right now, and whether it has outlived its backstop.
        // Populated on every flat tick; drives state.position.resting_entry and
        // the post-decision cancel below.
        let standingEntry: {
            kind: 'limit' | 'stop' | null;
            side: 'BUY' | 'SELL' | null;
            price: number | null;
            age_min: number | null;
        } | null = null;
        let restingEntryReadFailed = false;
        if (!positionOpen) {
            const orders = await readPendingEntries();
            if (orders === null) {
                restingEntryReadFailed = true;
            } else if (orders.length) {
                const first = orders[0] as Record<string, unknown> & { createdAtMs?: number | null };
                // Each venue reports its own kind: Bitget by which order book the
                // row came from, Capital by the working order's LIMIT/STOP type.
                // Neither is inferred from price — that would be guessing at the
                // model's intent from geometry.
                const kind: 'limit' | 'stop' | null =
                    'planOrder' in first
                        ? first.planOrder
                            ? 'stop'
                            : 'limit'
                        : ((first.restingKind as 'limit' | 'stop' | null) ?? null);
                standingEntry = { kind, ...describeRestingEntry(first) };
            }
        }
        // Age backstop: a resting order now survives evaluations, so nothing
        // else bounds its life if this pipeline stops running. An entry idea
        // older than this is stale regardless of what the model last thought,
        // and a stop firing days later on a dead thesis is the failure mode
        // this guards. Not a policy the model reasons about — a safety net
        // under our own outages.
        const restingEntryAgedOut =
            standingEntry !== null &&
            standingEntry.age_min !== null &&
            standingEntry.age_min > RESTING_ENTRY_MAX_AGE_MINUTES;
        if (!positionOpen && !quarterTick && restingEntryAgedOut) {
            const sweep = await sweepPendingEntries();
            if (await pendingEntryFilledMidTick(sweep)) {
                // No KV marker (never had one — the fill surfaces as a position
                // next tick); the durable row keeps the post-mortem trail whole.
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'pending_entry_filled',
                    reason: 'pending_entry_filled_during_ttl_sweep',
                    kvMarker: false,
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision: {
                        action: 'HOLD',
                        bias: 'NEUTRAL',
                        signal_strength: 'LOW',
                        summary: 'pending_entry_filled',
                        reason: 'pending_entry_filled_during_ttl_sweep',
                    },
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'pending_entry_filled' },
                    usedTape: false,
                    promptSkipped: true,
                });
            }
            const sweepFailure = classifyPendingEntrySweep(sweep);
            if (sweepFailure) {
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'pending_entry_sweep_failed',
                    reason: sweepFailure,
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision: {
                        action: 'HOLD',
                        bias: 'NEUTRAL',
                        signal_strength: 'LOW',
                        summary: 'pending_entry_sweep_failed',
                        reason: `entry_blocked_${sweepFailure}`,
                    },
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'pending_entry_sweep_failed' },
                    usedTape: false,
                    promptSkipped: true,
                });
            }
            // Aged out and cancelled — nothing stands any more, so the model
            // decides this tick as if it never rested one.
            standingEntry = null;
        }
        // Fail closed on a read failure. Not knowing what rests is exactly when
        // placing a fresh entry stacks exposure on an order still live at the
        // venue (the DE40 double fill, 2026-07-13); the previous
        // cancel-first design failed closed here for the same reason.
        if (!positionOpen && restingEntryReadFailed) {
            await recordTickOutcome({
                kind: 'skip',
                stage: 'pending_entry_read_failed',
                reason: 'entry_blocked_pending_entry_read_failed',
            });
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                category,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision: {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'pending_entry_read_failed',
                    reason: 'entry_blocked_pending_entry_read_failed',
                },
                execRes: { placed: false, orderId: null, clientOid: null, reason: 'pending_entry_read_failed' },
                usedTape: false,
                promptSkipped: true,
            });
        }
        // Stale-thread reconcile: the row says an entry is resting but the venue
        // has nothing (filled and closed, expired, weekend purge, manual
        // cancel). Without this a vanished order leaves the dashboard's
        // pendingEntry flag latched through every gate-skipped tick (EURUSD sat
        // stale for 25h, 2026-07-15). Now keyed on the READ rather than on a
        // sweep, since ticks no longer sweep by default.
        if (!positionOpen && !dryRun && standingEntry === null && aiThreadWasPendingEntry) {
            await endSwingAiThread(platform, symbol).catch((err) =>
                console.warn(`stale pending-entry thread cleanup failed for ${symbol}:`, err),
            );
        }

        // News is the AI's ONLY consumer — defer fetching it until we know the AI
        // will actually be called (past the signal-strength gate), so flat sub-MEDIUM
        // ticks don't hit the news API. Assigned just before callAI below.
        let newsBundle: { sentiment: Sentiment | null; headlines: string[] } | null = null;
        const [bundleLight, indicators] = await Promise.all([
            // Light bundle: skip tape (fills)
            fetchMarketBundle(symbol, timeFrame, { includeTrades: false }),
            calculateMultiTFIndicators(symbol, {
                primary: timeFrame,
                micro: microTimeFrame,
                macro: macroTimeFrame,
                context: contextTimeFrame,
            }),
        ]);
        if (platform === 'capital') {
            const bundleEpic = 'epic' in bundleLight ? bundleLight.epic : null;
            instrumentId =
                typeof bundleEpic === 'string' && bundleEpic
                    ? bundleEpic
                    : (await resolveCapitalEpicRuntime(symbol)).epic;
            category = resolveSwingCategory({
                category: categoryParam as string | undefined,
                symbol,
                platform,
                instrumentId,
            });
        }

        // Warm the dashboard chart caches before any decision gates can return:
        // candles reuse indicator data (plus one 15m fetch), and overlays use the
        // persisted closed-position mirror plus the open position already fetched
        // above. Best-effort; never blocks trading.
        try {
            const nowMs = Date.now();
            await Promise.all([
                warmChartCandlesFromAnalyze({
                    symbol,
                    platform,
                    nowMs,
                    rawCandlesByTf: indicators.rawCandles,
                    fetch15m: async () => {
                        const b = await fetchMarketBundle(symbol, '15m', { includeTrades: false, candleLimit: 106 });
                        return b?.candles ?? [];
                    },
                }),
                warmPositionOverlayCacheFromAnalyze({
                    symbol,
                    platform,
                    nowMs,
                    openPositionInfo: positionInfo,
                    openTakeProfitPrice: currentTakeProfit,
                    openStopLossPrice: currentStopLoss,
                }),
            ]);
        } catch (err) {
            console.warn(`chart cache warm failed for ${symbol}:`, err);
        }

        // In-position off-boundary ticks are event-driven: the resting TP/SL
        // bracket fences the position between AI evaluations, so the AI is only
        // asked early when price has moved enough since its last look to
        // plausibly change the answer. Under the 4H-close cadence that means
        // EVERY cron tick between bar closes, gated by the wide emergency
        // threshold (IN_POSITION_EMERGENCY_MOVE_ATR); in legacy 15-min mode it
        // is the old quarter-tick quiet skip (IN_POSITION_QUARTER_MOVE_ATR).
        // Reference = last real AI call's snapshot price (any tick — the entry
        // decision counts), falling back to entry price. Missing price/ATR fails
        // OPEN (call the AI rather than fly blind).
        // Failed-break watch: if this position entered on a breakout/breakdown
        // trigger (swing.break_triggers row, armed at entry) and a primary bar
        // has since CLOSED back through it, the break has failed — surface it
        // to the model as market.failed_break and let the tick through even
        // when quiet (the whole point is not waiting out the quiet skip on a
        // sub-emergency drift back through the trigger). Detection runs on
        // EVERY in-position tick, so the regular close-boundary call and a
        // watcher-fired early call both catch it; the row is consumed on
        // surfacing (one explicit exit decision per trigger, no per-bar
        // nagging). Best-effort: a store hiccup just skips the check.
        let failedBreak: {
            side: 'long' | 'short';
            triggerPrice: number;
            barClose: number;
            barClosedAtMs: number | null;
        } | null = null;
        if (positionOpen && !dryRun) {
            try {
                const breakTrigger = await getSwingBreakTrigger(platform, symbol);
                if (breakTrigger) {
                    const posSide = (positionInfo.holdSide ?? null) as 'long' | 'short' | null;
                    if (posSide && posSide !== breakTrigger.side) {
                        // Position flipped since the trigger was armed (e.g. a
                        // REVERSE) — the old trigger no longer describes this
                        // exposure.
                        await clearSwingBreakTrigger(platform, symbol);
                    } else {
                        const tfMs = timeframeToMs(breakTrigger.timeFrame) ?? timeframeToMs(timeFrame);
                        const bar = tfMs
                            ? lastClosedBar(indicators?.rawCandles?.[timeFrame], tfMs, Date.now())
                            : null;
                        if (
                            bar &&
                            bar.closeTs > breakTrigger.entryAtMs &&
                            breakTriggerFailed(breakTrigger.side, breakTrigger.triggerPrice, bar.close)
                        ) {
                            failedBreak = {
                                side: breakTrigger.side,
                                triggerPrice: breakTrigger.triggerPrice,
                                barClose: bar.close,
                                barClosedAtMs: bar.closeTs,
                            };
                            await clearSwingBreakTrigger(platform, symbol);
                            emitGateDebug('failed_break_detected', {
                                gate: 'FAILED_BREAK',
                                side: breakTrigger.side,
                                triggerPrice: breakTrigger.triggerPrice,
                                barClose: bar.close,
                            });
                        }
                    }
                }
            } catch (err) {
                console.warn(`Failed-break check failed for ${symbol}:`, err);
            }
        }

        // In-position wake band (ENABLE_POSITION_WAKE_BANDS): price at/beyond a
        // band the model set on a previous management look (stored on the AI
        // thread). Detection runs on EVERY in-position tick — the 1-min
        // watcher's fired call and the regular close-boundary tick both catch
        // it — and lets the tick through the quiet skip below (the whole point
        // is an early look at a sub-emergency move onto the model's own
        // level). NOT cleared here: bands are replaced only after a real AI
        // decision is durably recorded, so a run that dies mid-AI leaves them
        // armed and the watcher re-fires instead of losing the wake.
        let positionWakeFired: {
            crossed: 'above' | 'below';
            level: number;
            setAtMs: number | null;
            note: string | null;
        } | null = null;
        if (positionOpen && threadWake) {
            const tickerLight = Array.isArray(bundleLight?.ticker) ? bundleLight.ticker[0] : bundleLight?.ticker;
            const priceNow = Number(
                tickerLight?.lastPr ?? tickerLight?.last ?? tickerLight?.close ?? tickerLight?.price,
            );
            const crossed = wakeBandCrossed(priceNow, threadWake.above, threadWake.below);
            if (crossed) {
                positionWakeFired = {
                    crossed,
                    level: (crossed === 'above' ? threadWake.above : threadWake.below) as number,
                    setAtMs: threadWake.setAtMs,
                    note: threadWake.note,
                };
                emitGateDebug('position_wake_detected', {
                    gate: 'POSITION_WAKE',
                    crossed,
                    level: positionWakeFired.level,
                    price: priceNow,
                });
            }
        }

        const inPositionOffCadenceTick = primaryCloseCadence ? offBoundaryTick : quarterTick;
        const inPositionMoveThresholdAtr = primaryCloseCadence
            ? IN_POSITION_EMERGENCY_MOVE_ATR
            : IN_POSITION_QUARTER_MOVE_ATR;
        if (positionOpen && inPositionOffCadenceTick && !failedBreak && !positionWakeFired) {
            const tickerLight = Array.isArray(bundleLight?.ticker) ? bundleLight.ticker[0] : bundleLight?.ticker;
            const priceNow = Number(
                tickerLight?.lastPr ?? tickerLight?.last ?? tickerLight?.close ?? tickerLight?.price,
            );
            const atrNow = Number(indicators?.metrics?.[timeFrame]?.atr);
            let refPrice: number | null = null;
            try {
                const recent = await loadDecisionHistory(symbol, 5, platform);
                // loadDecisionHistory is newest-first, so find() already returns
                // the LATEST AI call (a .reverse() here compared against the
                // oldest call in the window — a stale reference price).
                const lastAiCall = recent.find((h) => {
                    const d = h.aiDecision;
                    return d && d.decision_source !== 'pre_ai_skip' && !d.promptSkipped;
                });
                const p = Number(lastAiCall?.snapshot?.price);
                if (Number.isFinite(p) && p > 0) refPrice = p;
            } catch (err) {
                console.warn(`Could not load last AI-call price for ${symbol}:`, err);
            }
            if (refPrice == null && positionInfo.status === 'open') {
                const entry = Number(positionInfo.entryPrice);
                if (Number.isFinite(entry) && entry > 0) refPrice = entry;
            }
            const moveAtr =
                refPrice != null && Number.isFinite(priceNow) && priceNow > 0 && Number.isFinite(atrNow) && atrNow > 0
                    ? Math.abs(priceNow - refPrice) / atrNow
                    : null;
            if (moveAtr != null && moveAtr < inPositionMoveThresholdAtr) {
                emitGateDebug('in_position_quiet_skip', {
                    gate: 'IN_POSITION_QUIET',
                    moveAtr: Number(moveAtr.toFixed(3)),
                    thresholdAtr: inPositionMoveThresholdAtr,
                    refPrice,
                    priceNow,
                });
                const decision = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'quiet_position',
                    reason: `in_position_skip_quiet_tick_move_${moveAtr.toFixed(2)}atr`,
                };
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'quiet_position',
                    reason: decision.reason,
                    metrics: {
                        moveAtr: Number(moveAtr.toFixed(3)),
                        thresholdAtr: inPositionMoveThresholdAtr,
                        refPrice,
                        priceNow,
                    },
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision,
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'quiet_position' },
                    usedTape: false,
                    promptSkipped: true,
                });
            }
            emitGateDebug('in_position_quiet_triggered', {
                gate: 'IN_POSITION_QUIET_TRIGGERED',
                moveAtr: moveAtr != null ? Number(moveAtr.toFixed(3)) : null,
                thresholdAtr: inPositionMoveThresholdAtr,
            });
        }

        // Flat off-boundary ticks under the 4H-close cadence: no AI call unless
        // (a) this tick just swept a resting entry — the model owes
        // itself a re-issue/switch/drop decision — or (b) a flat wake band is
        // crossed. The band is PEEKED here without touching the cooldown row:
        // the full cooldown handler below re-reads it, claims the row (lease)
        // and surfaces the crossing to the prompt as market.cooldown_wake; the
        // row is deleted only after the decision is recorded. Peek failure
        // fails CLOSED (skip): missing a wake by one bar is recoverable, while
        // failing open would re-create the 15-min churn cadence on every store
        // hiccup.
        if (!positionOpen && offBoundaryTick && !sweptPendingEntry) {
            let wakeBandCrossed = false;
            if (!dryRun) {
                try {
                    const cooldown = await getSwingAiCooldown(platform, symbol);
                    if (cooldown) {
                        const tickerLight = Array.isArray(bundleLight?.ticker)
                            ? bundleLight.ticker[0]
                            : bundleLight?.ticker;
                        const priceNow = Number(
                            tickerLight?.lastPr ?? tickerLight?.last ?? tickerLight?.close ?? tickerLight?.price,
                        );
                        // Deliberately the RAW crossing, even for sustained
                        // bands: an unconfirmed crossing passes the peek, the
                        // cooldown handler below then skips quietly but arms/
                        // refreshes the touch backstop — a few no-AI ticks
                        // while a touch confirms is the price of the fallback.
                        wakeBandCrossed =
                            Number.isFinite(priceNow) &&
                            priceNow > 0 &&
                            ((cooldown.wakeAbove !== null && priceNow >= cooldown.wakeAbove) ||
                                (cooldown.wakeBelow !== null && priceNow <= cooldown.wakeBelow));
                    }
                } catch (err) {
                    console.warn(`wake-band peek failed for ${symbol}:`, err);
                }
            }
            if (!wakeBandCrossed) {
                emitGateDebug('primary_close_gate_blocked', {
                    gate: 'PRIMARY_CLOSE_TIME',
                    primaryCloseTime,
                    positionOpen,
                    enforcePrimaryCloseGate,
                    evalPrimaryCloseOnly: EVAL_PRIMARY_CLOSE_ONLY,
                    timeFrame,
                });
                const decision = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'not_primary_close',
                    reason: 'flat_skip_until_primary_close',
                };
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'primary_close_gate',
                    reason: decision.reason,
                    metrics: { primaryCloseTime, timeFrame },
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision,
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'not_primary_close' },
                    usedTape: false,
                    promptSkipped: true,
                    ...(debugGates
                        ? {
                              gateDebug: {
                                  blockedBy: 'PRIMARY_CLOSE_TIME',
                                  reason: 'flat_skip_until_primary_close',
                                  primaryCloseTime,
                                  enforcePrimaryCloseGate,
                                  evalPrimaryCloseOnly: EVAL_PRIMARY_CLOSE_ONLY,
                                  positionOpen,
                                  timeFrame,
                              },
                          }
                        : {}),
                });
            }
            emitGateDebug('primary_close_gate_woken', {
                gate: 'PRIMARY_CLOSE_TIME',
                wakeBandCrossed: true,
                primaryCloseTime,
            });
        }

        const positionForPrompt =
            positionOpen
                ? `${positionInfo.holdSide}, entryPrice: ${positionInfo.entryPrice}, currentPnl=${positionInfo.currentPnl}`
                : 'none';

        // Store/refresh entry timestamp for min-hold (best-effort, in-memory)
        const persistKey = `${symbol}:${timeFrame}`;
        const pstate = touchPersist(persistKey);
        if (positionOpen) {
            const entryTimestamp = typeof positionInfo.entryTimestamp === 'number' ? positionInfo.entryTimestamp : undefined;
            if (pstate.lastSide !== positionInfo.holdSide) {
                pstate.enteredAt = entryTimestamp ?? Date.now();
                pstate.lastSide = positionInfo.holdSide;
            } else if (!pstate.enteredAt) {
                pstate.enteredAt = entryTimestamp ?? Date.now();
            }
        } else {
            // clear on flat
            persist.delete(persistKey);
        }

        // 2) Analytics from light bundle (no tape)
        const analyticsLight = computeAnalytics({ ...bundleLight, trades: [] });
        const atrFloorScale = 1;

        // Capital applies a fixed per-asset-class leverage, so the swing position
        // opens at notional = sideSizeUSDT * that leverage — gates must vet the
        // leveraged size. (Bitget leverages by the model's chosen value, which is
        // only known after the AI call, so its base gate stays at sideSizeUSDT and
        // is re-checked post-decision below.)
        const capitalLeverage = platform === 'capital' ? getCapitalCategoryLeverage(symbol) : null;
        const baseNotionalUSDT = sideSizeUSDT * (capitalLeverage ?? 1);

        // 3) Gates on light data (orderbook/ATR based)
        const gatesOut = getGates({
            symbol,
            bundle: bundleLight,
            analytics: analyticsLight,
            indicators,
            notionalUSDT: baseNotionalUSDT,
            positionOpen,
            disableSymbolExclusions: platform === 'capital',
            atrFloorScale,
            marketCategory: category,
        });

        // 3b) Short-circuit if no trade allowed and no open position
        if (gatesOut.preDecision && !positionOpen) {
            const execRes = { placed: false, orderId: null, clientOid: null, reason: 'gates_short_circuit' };
            await persistPreAiSkip({
                stage: 'base_gates_short_circuit',
                decision: gatesOut.preDecision,
                execResult: execRes,
                gates: gatesOut.gates,
                metrics: gatesOut.metrics,
            });
            emitGateDebug('base_gates_short_circuit', {
                gate: 'BASE_GATES',
                preDecisionReason: gatesOut.preDecision.reason,
                preDecisionSummary: gatesOut.preDecision.summary,
                gates: gatesOut.gates,
                spreadBpsNow: safeNum(gatesOut.metrics?.spreadBpsNow, NaN),
                expectedSlippageBps: safeNum(gatesOut.metrics?.expectedSlippageBps, NaN),
                atrPctNow: safeNum(gatesOut.metrics?.atrPctNow, NaN),
            });
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                category,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision: gatesOut.preDecision,
                execRes,
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                usedTape: false,
                promptSkipped: true,
                ...(debugGates
                    ? {
                          gateDebug: {
                              blockedBy: 'BASE_GATES',
                              reason: gatesOut.preDecision.reason,
                              summary: gatesOut.preDecision.summary,
                              gates: gatesOut.gates,
                              metrics: gatesOut.metrics,
                          },
                      }
                    : {}),
            });
        }

        // 4) Decide whether we need tape; if yes, fetch with tight budgets
        const needTape = positionOpen || gatesOut.allowed_actions.some((a) => a === 'BUY' || a === 'SELL');

        let bundle = bundleLight;
        let analytics = analyticsLight;
        let usedTape = false;

        if (needTape) {
            const bundleFull = await fetchMarketBundle(symbol, timeFrame, {
                includeTrades: true,
                tradeMinutes: 60, // 30–60 typical
                tradeMaxMs: 2500, // time budget (ms)
                tradeMaxPages: 6, // pagination budget
                tradeMaxTrades: 1200, // cap number of trades
            });
            bundle = bundleFull;
            analytics = computeAnalytics(bundleFull);
            usedTape = true;
        }

        const tickerData = Array.isArray(bundle?.ticker) ? bundle.ticker[0] : bundle?.ticker;
        const lastPrice = Number(tickerData?.lastPr ?? tickerData?.last ?? tickerData?.close ?? tickerData?.price);
        const effectivePrice = Number.isFinite(lastPrice) ? lastPrice : safeNum(analytics.last, 0);

        // AI-requested flat cooldown ("nothing to do here for N minutes unless
        // price crosses a wake band"): the previous flat HOLD asked not to be
        // re-evaluated. Applies to fresh flat scans only — never to in-position
        // ticks or resting-limit re-evaluations (those carry a thread) — and to
        // hourly ticks too: suppressing the backstop is the point of the feature.
        // Checked EARLY — before the momentum/actionability/signal-strength/
        // extension gates — because a crossed wake band must reach the model even
        // when those gates would skip the tick: signal_strength is typically
        // still LOW in the first hour of a breakout, which is exactly when the
        // band fires (2026-07-18 BTC: two wake_above crossings both died at the
        // signal-strength gate and the AI was woken ~1h late, at the crest). A
        // crossed band sets cooldownWake, which bypasses the flat QUALITY gates
        // below and reaches the prompt as market.cooldown_wake; the HARD gates
        // (base executability, event blackout) still apply — a hard-gate skip
        // keeps the lease, so the wake retries after lease expiry instead of
        // being swallowed. Expiry without a crossing consumes the row and
        // proceeds as a normal scan, no bypass.
        // Best-effort: a store hiccup fails open (evaluate rather than trust a
        // stale quiet period).
        let cooldownWake: {
            crossed: 'above' | 'below';
            level: number;
            setAtMs: number | null;
            note: string | null;
            // Band crossed past its plan horizon (cooldown end + one primary
            // candle of grace — lib/swing/wakeWatch.flatWakePlanStale): the
            // note reaches the prompt flagged expired (an idea to re-derive,
            // not a schedule to execute) and does NOT bypass the flat quality
            // gates below.
            expired: boolean;
            // Sustained band only: minutes the cross had held when it woke.
            sustainedMinutes?: number | null;
            // Set when the wake confirmed by EXTENSION (≥ wakeBreakConfirmAtr
            // primary-ATRs beyond the level) rather than by holding the window.
            breakExtensionAtr?: number | null;
        } | null = null;
        // True when THIS run holds the claim lease on a triggered wake row.
        // The row is deleted only after the wake's decision is durably
        // recorded (post-decision consume below) — delete-at-detection lost
        // the wake forever when the AI-bearing run died mid-flight (AVAX
        // 2026-07-23: 2h blind spot through the exact move the band watched).
        let cooldownRowClaimed = false;
        // Sustained-confirmation extras: how long the crossing has held when a
        // sustained band wakes (→ market.cooldown_wake.sustained_minutes), how
        // far it extended when confirmed by force instead of time (→
        // market.cooldown_wake.break_extension_atr), and the row's
        // failed-touch history (→ market.wake_band_sweeps).
        let cooldownWakeSustainedMinutes: number | null = null;
        let cooldownWakeExtensionAtr: number | null = null;
        let wakeBandSweeps: SwingWakeSweep[] | null = null;
        // Reclaim wake: a fresh sweep of the standing sustained band claimed
        // its one-shot AI look — THIS tick is the bounce moment. Mutually
        // exclusive with cooldownWake by construction (only reachable when no
        // band is crossed). READ-ONLY wrt the cooldown row: the band's plan
        // stays armed; only a placed entry supersedes it.
        let reclaimWake: {
            side: 'above' | 'below';
            level: number;
            extreme: number | null;
            touchedAtMs: number;
            reclaimedAtMs: number;
            atr: number | null;
            note: string | null;
        } | null = null;
        // Session-reclaim look (phase 2): the watcher detected a deep sweep-
        // and-reclaim of a session/prior-day liquidity pool and left the event
        // in KV (one-shot per pool, watcher-side budget). Same semantics as a
        // band reclaim look: quality-gate bypass, judgment only, READ-ONLY wrt
        // any standing cooldown row. A fired band or band-reclaim look takes
        // precedence (cleared below) — one wake context per evaluation.
        let sessionReclaim: SessionSweepEvent | null = null;
        if (!positionOpen && !dryRun && !aiThreadResponseId) {
            try {
                const ev = await kvGetJson<SessionSweepEvent>(sessionSweepEventKey(platform, symbol));
                if (
                    ev &&
                    Number.isFinite(Number(ev.level)) &&
                    Number(ev.level) > 0 &&
                    Date.now() - Number(ev.reclaimedAtMs) <= RECLAIM_WAKE_FRESH_MINUTES * 60_000
                ) {
                    sessionReclaim = ev;
                }
            } catch (err) {
                console.warn(`session-reclaim event read failed for ${symbol}:`, err);
            }
        }
        if (!positionOpen && !dryRun && !aiThreadResponseId) {
            try {
                const cooldown = await getSwingAiCooldown(platform, symbol);
                if (cooldown) {
                    let wokenAbove = cooldown.wakeAbove !== null && effectivePrice >= cooldown.wakeAbove;
                    let wokenBelow = cooldown.wakeBelow !== null && effectivePrice <= cooldown.wakeBelow;
                    const expired = Date.now() >= cooldown.untilMs;
                    // Failed touches of this row's band (sustained bands only)
                    // — surfaced as market.wake_band_sweeps on whichever look
                    // consumes or fires the row.
                    if (cooldown.sweeps.length > 0) wakeBandSweeps = cooldown.sweeps;
                    // Sustained band: a raw crossing only WAKES once confirmed
                    // — by TIME (held beyond the band for the model's window;
                    // wake_touch_* is stamped by the 1-min watcher) or by
                    // EXTENSION (price ≥ wakeBreakConfirmAtr primary-ATRs past
                    // the level: force proves the break before the clock). An
                    // unconfirmed crossing stays a quiet cooldown tick — but
                    // arm the touch state as a backstop so time confirmation
                    // still accrues on cron ticks alone if the watcher is down.
                    if (cooldown.confirmMinutes && (wokenAbove || wokenBelow)) {
                        const side: 'above' | 'below' = wokenAbove ? 'above' : 'below';
                        const level = wokenAbove ? cooldown.wakeAbove : cooldown.wakeBelow;
                        const heldMs =
                            cooldown.touchSide === side && cooldown.touchStartedMs
                                ? Date.now() - cooldown.touchStartedMs
                                : null;
                        const extensionAtr =
                            level !== null && cooldown.atr !== null && cooldown.atr > 0
                                ? ((effectivePrice - level) * (side === 'above' ? 1 : -1)) / cooldown.atr
                                : null;
                        const confirmedByTime = heldMs !== null && heldMs >= cooldown.confirmMinutes * 60_000;
                        const confirmedByExtension =
                            extensionAtr !== null && extensionAtr >= wakeBreakConfirmAtr();
                        if (confirmedByTime || confirmedByExtension) {
                            if (heldMs !== null) {
                                cooldownWakeSustainedMinutes = Math.max(1, Math.round(heldMs / 60_000));
                            }
                            if (confirmedByExtension) cooldownWakeExtensionAtr = extensionAtr;
                        } else {
                            wokenAbove = false;
                            wokenBelow = false;
                            if (cooldown.touchSide !== side || !cooldown.touchStartedMs) {
                                await setSwingWakeTouch(platform, symbol, {
                                    side,
                                    startedMs: Date.now(),
                                    extreme: effectivePrice,
                                }).catch((err) =>
                                    console.warn(`wake touch backstop failed for ${symbol}:`, err),
                                );
                            }
                        }
                    }
                    if (!expired && !wokenAbove && !wokenBelow) {
                        // Reclaim wake: no band crossed, but the newest sweep
                        // of this row's band is fresh, deep enough, and the
                        // row's one-shot look budget is unspent — claim it
                        // atomically and proceed as the bounce-moment look
                        // instead of the quiet-cooldown skip. Claim losers
                        // (another run already looking) fall through to the
                        // ordinary skip below.
                        const newestSweep = cooldown.sweeps[cooldown.sweeps.length - 1] ?? null;
                        if (
                            reclaimWakeEligible({
                                sweep: newestSweep,
                                atr: cooldown.atr,
                                reclaimLookedAtMs: cooldown.reclaimLookedAtMs,
                                nowMs: Date.now(),
                            })
                        ) {
                            const won = await claimSwingReclaimLook(platform, symbol).catch((err) => {
                                console.warn(`reclaim-look claim failed for ${symbol}:`, err);
                                return false;
                            });
                            if (won && newestSweep) {
                                reclaimWake = {
                                    side: newestSweep.side,
                                    level: newestSweep.level,
                                    extreme: newestSweep.extreme,
                                    touchedAtMs: newestSweep.touchedAtMs,
                                    reclaimedAtMs: newestSweep.reclaimedAtMs,
                                    atr: cooldown.atr,
                                    note: cooldown.wakeNote,
                                };
                                emitGateDebug('flat_cooldown_reclaim_look', {
                                    gate: 'AI_COOLDOWN',
                                    side: newestSweep.side,
                                    level: newestSweep.level,
                                    extreme: newestSweep.extreme,
                                    price: effectivePrice,
                                });
                            }
                        }
                    }
                    if (!expired && !wokenAbove && !wokenBelow && !reclaimWake && !sessionReclaim) {
                        const minutesLeft = Math.max(1, Math.round((cooldown.untilMs - Date.now()) / 60_000));
                        emitGateDebug('flat_cooldown_active', {
                            gate: 'AI_COOLDOWN',
                            minutesLeft,
                            wakeAbove: cooldown.wakeAbove,
                            wakeBelow: cooldown.wakeBelow,
                        });
                        const decision = {
                            action: 'HOLD',
                            bias: 'NEUTRAL',
                            signal_strength: 'LOW',
                            summary: 'ai_requested_cooldown',
                            reason: `flat_skip_cooldown_active_${minutesLeft}m_left`,
                        };
                        await recordTickOutcome({
                            kind: 'skip',
                            stage: 'flat_cooldown',
                            reason: decision.reason,
                            gates: gatesOut.gates,
                            metrics: {
                                ...gatesOut.metrics,
                                cooldown: { minutesLeft, wakeAbove: cooldown.wakeAbove, wakeBelow: cooldown.wakeBelow },
                            },
                        });
                        return res.status(200).json({
                            symbol,
                            platform,
                            newsSource,
                            category,
                            instrumentId,
                            timeFrame,
                            dryRun,
                            decisionPolicy,
                            decision,
                            execRes: { placed: false, orderId: null, clientOid: null, reason: 'flat_cooldown' },
                            gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                            usedTape,
                            promptSkipped: true,
                        });
                    }
                    if ((wokenAbove || wokenBelow) && flatWakePlanStale(cooldown.untilMs, cooldown.setAtMs, Date.now())) {
                        // Crossed PAST the plan horizon (cooldown end + one
                        // primary candle of grace): the scheduled check never
                        // ran while it was fresh (venue closure, AI outage).
                        // Consume the row now — no lease, no retry, and no
                        // quality-gate bypass — and hand the note to the prompt
                        // flagged expired, so the model treats it as an old
                        // idea to re-derive from current structure rather than
                        // a standing order to execute (GOLD 2026-07-27).
                        await clearSwingAiCooldown(platform, symbol);
                        cooldownWake = {
                            crossed: wokenAbove ? 'above' : 'below',
                            level: (wokenAbove ? cooldown.wakeAbove : cooldown.wakeBelow) as number,
                            setAtMs: cooldown.setAtMs > 0 ? cooldown.setAtMs : null,
                            note: cooldown.wakeNote,
                            expired: true,
                        };
                        emitGateDebug('flat_cooldown_woken_stale', {
                            gate: 'AI_COOLDOWN',
                            crossed: cooldownWake.crossed,
                            level: cooldownWake.level,
                            price: effectivePrice,
                            untilMs: cooldown.untilMs,
                            setAtMs: cooldown.setAtMs,
                        });
                    } else if (wokenAbove || wokenBelow) {
                        // Lease the row instead of deleting it (see
                        // cooldownRowClaimed above). A held lease means another
                        // analyze run — usually the wake-watcher's fired call —
                        // is already mid-flight on this exact wake: end this
                        // tick quietly rather than double-calling the AI.
                        const claimed = await claimSwingAiCooldown(platform, symbol);
                        if (!claimed) {
                            const decision = {
                                action: 'HOLD',
                                bias: 'NEUTRAL',
                                signal_strength: 'LOW',
                                summary: 'cooldown_wake_in_flight',
                                reason: 'flat_skip_cooldown_wake_claim_held',
                            };
                            await recordTickOutcome({
                                kind: 'skip',
                                stage: 'flat_cooldown',
                                reason: decision.reason,
                                gates: gatesOut.gates,
                                metrics: {
                                    ...gatesOut.metrics,
                                    cooldown: { wakeAbove: cooldown.wakeAbove, wakeBelow: cooldown.wakeBelow },
                                },
                            });
                            return res.status(200).json({
                                symbol,
                                platform,
                                newsSource,
                                category,
                                instrumentId,
                                timeFrame,
                                dryRun,
                                decisionPolicy,
                                decision,
                                execRes: { placed: false, orderId: null, clientOid: null, reason: 'flat_cooldown' },
                                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                                usedTape,
                                promptSkipped: true,
                            });
                        }
                        cooldownRowClaimed = true;
                        cooldownWake = {
                            crossed: wokenAbove ? 'above' : 'below',
                            level: (wokenAbove ? cooldown.wakeAbove : cooldown.wakeBelow) as number,
                            setAtMs: cooldown.setAtMs > 0 ? cooldown.setAtMs : null,
                            // The model's own plan for this band — echoed back in
                            // market.cooldown_wake so the (stateless) wake scan
                            // knows why it was scheduled.
                            note: cooldown.wakeNote,
                            expired: false,
                            // Non-null only when this wake carried a sustained-
                            // confirmation window: how long the cross has held,
                            // and how far it extended if force confirmed it.
                            sustainedMinutes: cooldownWakeSustainedMinutes,
                            breakExtensionAtr: cooldownWakeExtensionAtr,
                        };
                        emitGateDebug('flat_cooldown_woken', {
                            gate: 'AI_COOLDOWN',
                            crossed: cooldownWake.crossed,
                            level: cooldownWake.level,
                            price: effectivePrice,
                            expired,
                        });
                    } else if (!reclaimWake) {
                        // Bare expiry, no crossing: nothing to protect — consume
                        // the row now and proceed as a normal scan (no bypass).
                        // (A claimed reclaim look never lands here — it is only
                        // reachable pre-expiry — but guard it anyway: the row
                        // must survive a reclaim look untouched.)
                        await clearSwingAiCooldown(platform, symbol);
                    }
                }
            } catch (err) {
                console.warn(`AI cooldown check failed for ${symbol}:`, err);
            }
        }

        // Only a FRESH wake earns the flat quality-gate bypass below. An
        // expired one (crossed past its plan horizon) rides to the prompt as
        // context if the tick passes the gates on its own merits — a stale
        // plan is re-evaluated as an idea, never fast-tracked as a schedule.
        // One wake context per evaluation: a fired band or a claimed band-
        // reclaim look outranks the KV session event (which stays TTL'd for
        // a later tick or expires unused).
        if (cooldownWake || reclaimWake) sessionReclaim = null;
        // A claimed reclaim look (band or session pool) earns the same flat
        // quality-gate bypass a fired band does: signal strength is
        // definitionally LOW at the reclaim minute — the reclaim IS the
        // actionability. Hard gates (base executability, event blackout,
        // margin) still apply below.
        const cooldownWakeActive =
            (cooldownWake !== null && !cooldownWake.expired) || reclaimWake !== null || sessionReclaim !== null;
        const momentumSignals = computeMomentumSignals({
            price: effectivePrice,
            indicators,
            gates: gatesOut.gates,
            primaryTimeframe: timeFrame,
        });

        const calmMarket =
            !positionOpen &&
            shouldSkipMomentumCall({
                signals: momentumSignals,
                price: effectivePrice,
                trades: Array.isArray(bundle?.trades) ? bundle.trades : [],
                enforceRecentTape: platform !== 'capital',
            });

        if (!positionOpen && calmMarket && !cooldownWakeActive) {
            const decision: TradeDecision & Record<string, unknown> = {
                action: 'HOLD',
                bias: 'NEUTRAL',
                signal_strength: 'LOW',
                summary: 'calm_market',
                reason: 'conditions_below_momentum_thresholds_or_no_recent_trades',
            };
            const execRes = { placed: false, orderId: null, clientOid: null, reason: 'calm_market' };
            await persistPreAiSkip({
                stage: 'calm_market_short_circuit',
                decision,
                execResult: execRes,
                gates: gatesOut.gates,
                metrics: gatesOut.metrics,
                usedTape,
                snapshot: {
                    price: effectivePrice,
                    momentumSignals,
                },
            });
            emitGateDebug('calm_market_short_circuit', {
                gate: 'MOMENTUM_FILTER',
                reason: 'conditions_below_momentum_thresholds_or_no_recent_trades',
                usedTape,
                microExtensionInAtr: safeNum(momentumSignals.microExtensionInAtr, NaN),
                primaryAtr: safeNum(momentumSignals.primaryAtr, NaN),
                tradeCount: Array.isArray(bundle?.trades) ? bundle.trades.length : 0,
            });
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                category,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision,
                execRes,
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                usedTape,
                promptSkipped: true,
                ...(debugGates
                    ? {
                          gateDebug: {
                              blockedBy: 'MOMENTUM_FILTER',
                              reason: 'conditions_below_momentum_thresholds_or_no_recent_trades',
                              momentumSignals,
                              usedTape,
                          },
                      }
                    : {}),
            });
        }

        // 5) PnL for downstream context
        let pnlPct = 0;

        if (positionOpen) {
            pnlPct = parsePnlPct(positionInfo.currentPnl);
        }

        const positionExtrema = positionOpen
            ? await updatePositionExtrema({
                  symbol,
                  timeFrame,
                  position: positionInfo,
                  pnlPct,
              })
            : {};

        // Entry thesis + management history now live in the position's Responses-API
        // conversation thread (previous_response_id) — no re-feed from Postgres.
        const positionContext = composePositionContext({
            position: positionInfo,
            pnlPct,
            maxDrawdownPct: positionExtrema.maxDrawdownPct,
            maxProfitPct: positionExtrema.maxProfitPct,
            enteredAt: pstate.enteredAt,
            takeProfitPrice: currentTakeProfit,
            stopLossPrice: currentStopLoss,
            // Capital CFDs are commission-free — a 0 fee keeps breakeven_price at
            // the entry price instead of shifting it by the Bitget taker default.
            takerFeeRate: platform === 'capital' ? 0 : undefined,
        });
        // Re-entry cooldown input: most recent close on this symbol (AI close,
        // auto-close or broker stop — swing.positions records them all). Only needed
        // when flat; enforcement happens in postprocessDecision, the prompt just
        // states the constraint. Fails open on read errors.
        let lastClosedPosition: LastClosedPosition | null = null;
        if (!positionOpen && REENTRY_COOLDOWN_MIN > 0) {
            try {
                const recentClosed = await loadClosedSwingPositions({
                    platform,
                    symbol,
                    fromMs: Date.now() - REENTRY_COOLDOWN_MIN * 60_000,
                    limit: 10,
                });
                const last = recentClosed.at(-1);
                if (last?.side && Number.isFinite(last.exitTimestamp as number)) {
                    lastClosedPosition = { side: last.side, exitTsMs: Number(last.exitTimestamp) };
                }
            } catch (err) {
                console.warn(`Could not load recent closed positions for ${symbol}:`, err);
            }
        }

        // Session/day/week levels and the macro-event calendar are both valuable for any
        // session-traded, fiat-macro-sensitive Capital.com instrument (forex, metals,
        // indices). Events resolve to the instrument's macro currency (e.g. USD for gold).
        // Crypto is excluded from session levels (24/7, no session boundaries) but DOES
        // get the USD macro calendar: BTC/ETH react to CPI/FOMC/NFP like any USD asset,
        // so the blackout gate and post-event reaction measurements apply there too.
        const SESSION_LEVEL_CATEGORIES = new Set(['forex', 'commodity', 'index']);
        const EVENT_CALENDAR_CATEGORIES = new Set([...SESSION_LEVEL_CATEGORIES, 'crypto']);
        // Venue liquidity clock (cash opens/closes, lunch breaks, Globex halts,
        // weekly thin reopen) — pure schedule math, no fetch, so it is computed
        // BEFORE the quarter-tick cooldown skip: sweep windows must stay live.
        const venueEvents =
            platform === 'capital' && category && SESSION_LEVEL_CATEGORIES.has(category)
                ? buildVenueSessionEvents({ symbol, category, nowMs: Date.now() })
                : null;

        // Quarter-tick cooldown skip: while the re-entry cooldown is active the
        // AI can only HOLD (same side blocked) or open the opposite side — a
        // call the hourly tick makes just as well. Skipping the 15m cadence
        // caps the burn at one call/hour for the cooldown window; the hourly
        // tick stays the backstop, so an opposite-direction reversal is
        // delayed by at most 45 min. Quarter-only: hourly ticks still evaluate.
        // Does NOT apply with a position open (skip is flat-only) or while a
        // resting entry is live (aiThreadResponseId set on a flat tick ⇔
        // pending-entry conversation) — a resting opposite-direction entry
        // must keep being re-validated against the moving market.
        // Also does NOT apply during sweep windows (opening_drive / thin_reopen):
        // the cooldown's sweep-reclaim exception (postprocessDecision) can only
        // fire on ticks that actually run, and reclaims resolve in minutes.
        const sweepWindow =
            venueEvents?.liquidity_phase === 'opening_drive' || venueEvents?.liquidity_phase === 'thin_reopen';
        if (!positionOpen && quarterTick && !aiThreadResponseId && !sweepWindow && !cooldownWakeActive) {
            const cooldownNow = resolveReentryCooldown(lastClosedPosition);
            if (cooldownNow) {
                const decision = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'reentry_cooldown_quarter_tick',
                    reason: `flat_skip_reentry_cooldown_blocked_${cooldownNow.blockedSide}_${cooldownNow.minutesLeft}min_left`,
                };
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'reentry_cooldown',
                    reason: decision.reason,
                    metrics: { blockedSide: cooldownNow.blockedSide, minutesLeft: cooldownNow.minutesLeft },
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision,
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'reentry_cooldown' },
                    promptSkipped: true,
                });
            }
        }

        // Depth 12 (not 5): skip rows dominate the history, and limit re-issue
        // chains need enough rows to collapse into one entry below.
        const recentHistory = await loadDecisionHistory(symbol, 12, platform);
        const recentActionsRaw = recentHistory
            .filter((h) => h.aiDecision?.decision_source !== 'pre_ai_skip' && !h.aiDecision?.promptSkipped)
            .map((h) => {
                const d = h.aiDecision;
                // Preserve the close size so a partial trim (e.g. 30%) is distinguishable
                // from a full exit in the model's recent-actions feedback. Kept as a
                // separate field so the raw `action` string stays clean for the
                // anti-flip guard in postprocessDecision (which matches on `action`).
                const rawPct = d?.exit_size_pct ?? d?.close_size_pct ?? d?.partial_close_pct;
                const pctNum = Number(rawPct);
                const closePct = Number.isFinite(pctNum) ? Math.max(0, Math.min(100, pctNum)) : null;
                // Resting entry: either tool, reported with the kind so the
                // model sees WHICH order it rested, not just at what price.
                // Rows written before entry_stop_price existed carry only a
                // limit — the kind falls back accordingly.
                const restingKind: 'limit' | 'stop' = d?.resting_entry_kind === 'stop' ? 'stop' : 'limit';
                const restNum = Number(restingKind === 'stop' ? d?.entry_stop_price : d?.entry_limit_price);
                const entryLimitPrice = Number.isFinite(restNum) && restNum > 0 ? restNum : null;
                const strategy = typeof d?.strategy === 'string' && d.strategy ? d.strategy : null;
                return {
                    action: d?.action,
                    timestamp: h.timestamp,
                    closePct,
                    entryLimitPrice,
                    restingEntryKind: entryLimitPrice != null ? restingKind : null,
                    strategy,
                };
            })
            .filter((a) => a.action)
            // loadDecisionHistory returns newest-first; everything downstream
            // (prompt slice(-N), anti-flip slice(-lookback), outcome windows)
            // assumes oldest-first — make the order explicit.
            .sort((a, b) => a.timestamp - b.timestamp);
        // Outcome enrichment: join the actions to what actually happened
        // (never_filled / still_open / closed pnl) so a resting limit that
        // never filled stops masquerading as a completed trade in the prompt.
        // One bounded Neon read, and only when there is a trade action to
        // explain — all-HOLD histories (the common quiet case) pay nothing.
        const collapsedActions = collapseLimitReissues(recentActionsRaw);
        let outcomePositions: PositionForOutcome[] = [];
        if (collapsedActions.some((a) => a.action !== 'HOLD')) {
            try {
                outcomePositions = await loadClosedSwingPositions({
                    platform,
                    symbol,
                    fromMs: Math.min(...collapsedActions.map((a) => a.firstTimestamp ?? a.timestamp)) - 5 * 60_000,
                    toMs: Date.now(),
                    limit: 20,
                });
            } catch (err) {
                console.warn(`Could not load positions for recent-action outcomes for ${symbol}:`, err);
            }
        }
        const recentActions = attachRecentActionOutcomes(collapsedActions, {
            positions: outcomePositions,
            openPosition: positionOpen
                ? {
                      side: positionInfo.holdSide ?? null,
                      entryTimestamp:
                          typeof positionInfo.entryTimestamp === 'number' ? positionInfo.entryTimestamp : null,
                  }
                : null,
            nowMs: Date.now(),
        });
        const forexEventContext =
            category && EVENT_CALENDAR_CATEGORIES.has(category)
                ? await loadForexEventContext({
                      symbol,
                      instrumentId,
                      category,
                  })
                : null;
        let forexSessionContext = null;
        if (category && SESSION_LEVEL_CATEGORIES.has(category)) {
            try {
                const sessionBundle =
                    String(microTimeFrame) === String(timeFrame)
                        ? bundle
                        : await fetchMarketBundle(symbol, microTimeFrame, {
                              includeTrades: false,
                              candleLimit: 120,
                          });
                const sessionCandles = sessionBundle?.candles;
                forexSessionContext = buildForexSessionLevelsContext({
                    symbol,
                    candles: Array.isArray(sessionCandles) ? sessionCandles : [],
                    sourceTimeframe: microTimeFrame,
                });
            } catch (err) {
                console.warn(`Could not build forex session levels for ${symbol}:`, err);
            }
        }
        // Session-levels ref for the watcher's sweep detection (reclaim-wake
        // phase 2): the pools plus their own validity horizons — last-session
        // levels are superseded when the current session completes, prior-day
        // levels when the UTC date rolls. Best-effort fire-and-forget.
        if (!dryRun && forexSessionContext) {
            const refAtrRaw = Number(indicators?.metrics?.[timeFrame]?.atr);
            const nowRef = Date.now();
            const utcMidnightMs = Date.UTC(
                new Date(nowRef).getUTCFullYear(),
                new Date(nowRef).getUTCMonth(),
                new Date(nowRef).getUTCDate() + 1,
            );
            const currentSessionEndMs = Date.parse(String(forexSessionContext.currentSession?.endUtc ?? ''));
            kvSetJson(
                sessionLevelsRefKey(platform, symbol),
                {
                    levels: {
                        last_session_high: forexSessionContext.lastCompletedSession?.high ?? null,
                        last_session_low: forexSessionContext.lastCompletedSession?.low ?? null,
                        prior_day_high: forexSessionContext.priorDay?.high ?? null,
                        prior_day_low: forexSessionContext.priorDay?.low ?? null,
                    },
                    atr: Number.isFinite(refAtrRaw) && refAtrRaw > 0 ? refAtrRaw : null,
                    ts: nowRef,
                    lastSessionValidUntilMs: Number.isFinite(currentSessionEndMs) ? currentSessionEndMs : utcMidnightMs,
                    priorDayValidUntilMs: utcMidnightMs,
                } satisfies SessionLevelsRef,
                26 * 3600,
            ).catch((err: unknown) => console.warn(`session-levels ref stamp failed for ${symbol}:`, err));
        }

        // 6a) Event-proximity gate (HARD risk rule). When flat and inside a
        // high/medium-impact event blackout window (pre/post-event minutes +
        // blocked impacts are env-configured in lib/swing/forexEvents), block NEW
        // entries — opening a fresh position into CPI/NFP/FOMC is exactly the risk
        // this prevents, and we don't leave it to model discretion. Exits are
        // unaffected: in-position ticks fall through and the AI still runs. Skipping
        // here also avoids the prompt assembly, news fetch and AI call.
        if (!positionOpen && forexEventContext?.status === 'active') {
            const reasonCodes = Array.isArray(forexEventContext.reasonCodes) ? forexEventContext.reasonCodes : [];
            const decision: TradeDecision & Record<string, unknown> = {
                action: 'HOLD',
                bias: 'NEUTRAL',
                summary: 'event_blackout',
                reason: `flat_skip_event_blackout_${reasonCodes.join('|') || 'active'}`,
            };
            const execRes = { placed: false, orderId: null, clientOid: null, reason: 'event_blackout' };
            await persistPreAiSkip({
                stage: 'event_blackout_gate',
                decision,
                execResult: execRes,
                gates: gatesOut.gates,
                metrics: gatesOut.metrics,
                usedTape,
                snapshot: { price: effectivePrice, forexEventContext, momentumSignals },
            });
            emitGateDebug('event_blackout_gate', {
                gate: 'FOREX_EVENT_BLACKOUT',
                status: forexEventContext.status,
                reasonCodes,
                activeEvents: forexEventContext.activeEvents,
                positionOpen,
            });
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                category,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision,
                execRes,
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                forexEventContext,
                usedTape,
                promptSkipped: true,
            });
        }

        // 6) Build prompt with allowed_actions and gates
        const roiRes = await fetchRealizedRoi(symbol, 24);

        // Venue market context for the prompt (Capital only). Session timing is
        // emitted only when the schedule confirms the venue is CURRENTLY inside a
        // session (this code runs post market-closed gate, so an isOpen=false/null
        // schedule means the schedule disagrees with marketStatus or is unreadable —
        // feeding its timestamps would mislabel the NEXT session's close as the
        // current one). Timestamps are ISO UTC; durations in minutes. The prompt
        // prose in lib/ai.ts is conditional on each field being present.
        const capitalNowMs = Date.now();
        const venueSession =
            platform === 'capital' &&
            capitalMarketInfo?.session?.isOpen === true &&
            Number.isFinite(capitalMarketInfo.session.closesAtMs as number)
                ? {
                      closes_at_utc: new Date(capitalMarketInfo.session.closesAtMs as number).toISOString(),
                      minutes_to_close: Math.max(
                          0,
                          Math.round(((capitalMarketInfo.session.closesAtMs as number) - capitalNowMs) / 60_000),
                      ),
                      reopens_at_utc: Number.isFinite(capitalMarketInfo.session.nextOpenAtMs as number)
                          ? new Date(capitalMarketInfo.session.nextOpenAtMs as number).toISOString()
                          : null,
                      // Session age: lets the model treat a just-opened venue
                      // (gap candles, wide spreads, indicators still digesting)
                      // differently from a mature session. In-position ticks
                      // are the main consumers — flat ticks inside the warmup
                      // window never reach the prompt (open_warmup_gate).
                      minutes_since_open: Number.isFinite(capitalMarketInfo.session.openedAtMs as number)
                          ? Math.max(
                                0,
                                Math.round((capitalNowMs - (capitalMarketInfo.session.openedAtMs as number)) / 60_000),
                            )
                          : null,
                  }
                : null;
        const capitalMarketContext =
            platform === 'capital'
                ? {
                      venue_session: venueSession,
                      venue_events: venueEvents,
                      overnight_fee_pct_per_day: capitalMarketInfo?.overnightFeePctPerDay ?? null,
                  }
                : null;

        // Derive signal_strength + context WITHOUT assembling the prompt or fetching
        // news — both are deferred until we know the AI will be called (past the gate),
        // so flat sub-MEDIUM ticks skip the expensive assembly entirely.
        const swingState = computeSwingState(
            symbol, // e.g. "BTCUSDT"
            timeFrame, // e.g. "45m"
            bundle, // from fetchMarketBundle(...)
            analytics, // from computeAnalytics(bundle)
            positionForPrompt, // "none" | JSON string like 'open long @ ...'
            forexEventContext,
            forexSessionContext,
            indicators, // from calculateMultiTFIndicators(symbol)
            gatesOut.gates, // from getGates(...)
            positionContext,
            momentumSignals,
            recentActions,
            roiRes.lastNetPct,
            dryRun,
            Number(gatesOut.metrics?.spreadBpsNow),
            decisionPolicy,
            category,
            platform,
            lastClosedPosition,
            capitalNowMs,
            capitalMarketContext,
            cooldownWake,
            failedBreak,
            // Fired band → market.position_wake; armed-but-quiet bands →
            // market.position_wake_armed (fired suppresses armed inside).
            { fired: positionWakeFired, armed: threadWake },
            // Failed sustained-band touches → market.wake_band_sweeps.
            wakeBandSweeps,
            // Claimed one-shot bounce-moment look → market.reclaim_wake.
            reclaimWake,
            // Session/prior-day pool sweep-and-reclaim → market.session_reclaim.
            sessionReclaim,
        );
        const { context, actionability } = swingState;

        // 6b) Actionability gate (replaces the old signal_strength + micro-entry gates).
        // Derived from the decision history, not hand-tuned: the AI opens a flat position
        // only on a confirmed primary structure break, or a bounce off a level with room
        // to run and micro turning that way — and HOLDs when sandwiched between nearby
        // S/R with no break. So we spend the (expensive) AI call + news fetch only when a
        // trade is plausible. Backtest: 100% recall on real opens, ~76% fewer calls than
        // the old signal_strength≥MEDIUM gate. Flat entries only — in-position ticks
        // always proceed (exits/trims can be needed regardless). Predicate:
        // evaluateActionability in lib/ai.ts.
        if (!positionOpen && !actionability.actionable && !cooldownWakeActive) {
            const decision: TradeDecision & Record<string, unknown> = {
                action: 'HOLD',
                bias: 'NEUTRAL',
                summary: 'not_actionable',
                reason: `flat_skip_not_actionable_${actionability.reason}`,
            };
            const execRes = { placed: false, orderId: null, clientOid: null, reason: 'not_actionable' };
            await persistPreAiSkip({
                stage: 'actionability_gate',
                decision,
                execResult: execRes,
                gates: gatesOut.gates,
                metrics: gatesOut.metrics,
                usedTape,
                // Session signals + venue phase ride along so "how many reclaim
                // moments died at this gate" stays a SQL query over skip rows —
                // the measurement that decides whether the entry gates need a
                // sweep-reclaim branch.
                snapshot: {
                    price: effectivePrice,
                    actionability,
                    momentumSignals,
                    forexSessionSignals: forexSessionContext?.signals ?? null,
                    venueLiquidityPhase: venueEvents?.liquidity_phase ?? null,
                },
            });
            emitGateDebug('actionability_gate', {
                gate: 'ACTIONABILITY',
                reason: actionability.reason,
                positionOpen,
            });
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                category,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision,
                execRes,
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                usedTape,
                promptSkipped: true,
            });
        }

        // 6c) Signal-strength hard gate: flat + sub-MEDIUM strength → no AI call,
        // even when structurally actionable (6b). signal_strength is code-owned
        // (computeSignalStrength, never shown to the model), so this is a pure
        // budget gate stacked on actionability: both must pass before we spend the
        // call. Flat entries only — in-position ticks always proceed (exits/trims
        // can be needed regardless).
        if (!positionOpen && context.signal_strength === 'LOW' && !cooldownWakeActive) {
            const decision: TradeDecision & Record<string, unknown> = {
                action: 'HOLD',
                bias: 'NEUTRAL',
                summary: 'weak_signal',
                reason: 'flat_skip_signal_strength_low',
            };
            const execRes = { placed: false, orderId: null, clientOid: null, reason: 'weak_signal' };
            await persistPreAiSkip({
                stage: 'signal_strength_gate',
                decision,
                execResult: execRes,
                gates: gatesOut.gates,
                metrics: gatesOut.metrics,
                usedTape,
                // Session signals + venue phase ride along so "how many reclaim
                // moments died at this gate" stays a SQL query over skip rows —
                // the measurement that decides whether the entry gates need a
                // sweep-reclaim branch.
                snapshot: {
                    price: effectivePrice,
                    actionability,
                    momentumSignals,
                    forexSessionSignals: forexSessionContext?.signals ?? null,
                    venueLiquidityPhase: venueEvents?.liquidity_phase ?? null,
                },
            });
            emitGateDebug('signal_strength_gate', {
                gate: 'SIGNAL_STRENGTH',
                reason: 'below_medium',
                signalStrength: context.signal_strength,
                positionOpen,
            });
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                category,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision,
                execRes,
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                usedTape,
                promptSkipped: true,
            });
        }

        // 6d) Extension hard gate: flat + price extremely extended from EMA20 → no
        // AI call. The prompt already tells the model to avoid fresh entries beyond
        // these thresholds (same numbers via resolveExtensionThresholds), and it
        // complies: every extension-flavored flat HOLD observed (micro |ext| 2.7–5.5
        // ATR, RSI 12–29 / 69–79) was the AI re-deriving this rule — a wasted call.
        // Flat entries only — in-position ticks always proceed (exits/trims can be
        // needed regardless, and extension often argues FOR taking profit).
        const extThresholds = resolveExtensionThresholds(decisionPolicy);
        const microExtAtr = Number(context.micro_extension_atr);
        const primaryExtAtr = Number(context.primary_extension_atr);
        const microOverextended = Number.isFinite(microExtAtr) && Math.abs(microExtAtr) >= extThresholds.microAvoid;
        const primaryOverextended =
            Number.isFinite(primaryExtAtr) && Math.abs(primaryExtAtr) >= extThresholds.primaryAvoid;
        if (!positionOpen && (microOverextended || primaryOverextended) && !cooldownWakeActive) {
            const extDetail = [
                microOverextended ? `micro_${microExtAtr.toFixed(2)}atr` : null,
                primaryOverextended ? `primary_${primaryExtAtr.toFixed(2)}atr` : null,
            ]
                .filter(Boolean)
                .join('_');
            const decision: TradeDecision & Record<string, unknown> = {
                action: 'HOLD',
                bias: 'NEUTRAL',
                summary: 'overextended',
                reason: `flat_skip_overextended_${extDetail}`,
            };
            const execRes = { placed: false, orderId: null, clientOid: null, reason: 'overextended' };
            await persistPreAiSkip({
                stage: 'extension_gate',
                decision,
                execResult: execRes,
                gates: gatesOut.gates,
                metrics: gatesOut.metrics,
                usedTape,
                // Session signals + venue phase ride along so "how many reclaim
                // moments died at this gate" stays a SQL query over skip rows —
                // the measurement that decides whether the entry gates need a
                // sweep-reclaim branch.
                snapshot: {
                    price: effectivePrice,
                    actionability,
                    momentumSignals,
                    forexSessionSignals: forexSessionContext?.signals ?? null,
                    venueLiquidityPhase: venueEvents?.liquidity_phase ?? null,
                },
            });
            emitGateDebug('extension_gate', {
                gate: 'EXTENSION',
                reason: extDetail,
                microExtensionAtr: microExtAtr,
                primaryExtensionAtr: primaryExtAtr,
                positionOpen,
            });
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                category,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision,
                execRes,
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                usedTape,
                promptSkipped: true,
            });
        }

        // Flat quarter-tick dedupe: actionable-but-HOLD configurations ("sitting
        // on support") persist for hours; without this, the 15m flat cadence
        // would re-ask the AI about the same standing setup 4x an hour. If the
        // last flat AI call is under an hour old, answered HOLD and price has
        // barely moved since, this tick adds no information — skip without
        // persisting. Deliberately does NOT require the same actionability
        // branch: on 2026-07-08 GOLD flapped between confirmed_primary and
        // bounce_* on near-identical prices and burned 7 quarter-tick AI calls
        // in one evening — a branch flip on an unmoved price is the same
        // standing setup, and a genuinely fresh break moves price past the
        // 0.25-ATR gate anyway (the hourly tick is the backstop regardless).
        // 55min ceiling means hourly ticks are never deduped; missing inputs
        // fail open.
        if (!positionOpen && quarterTick && !cooldownWakeActive) {
            // recentHistory is newest-first, so find() already returns the most
            // recent flat AI call (the old .reverse() picked the OLDEST in the
            // window, deduping against a stale price reference).
            const lastFlatAiCall = recentHistory.find((h) => {
                const d = h.aiDecision;
                if (!d || d.decision_source === 'pre_ai_skip' || d.promptSkipped) return false;
                return !h.snapshot?.positionContext;
            });
            const lastSnap = lastFlatAiCall?.snapshot ?? null;
            const lastAction = String(lastFlatAiCall?.aiDecision?.action || '').toUpperCase();
            const ageMin = lastFlatAiCall ? (Date.now() - Number(lastFlatAiCall.timestamp)) / 60_000 : Infinity;
            const lastPrice = Number(lastSnap?.price);
            const dedupeAtr = Number(indicators?.metrics?.[timeFrame]?.atr);
            const priceMoveAtr =
                Number.isFinite(lastPrice) && lastPrice > 0 && Number.isFinite(dedupeAtr) && dedupeAtr > 0
                    ? Math.abs(effectivePrice - lastPrice) / dedupeAtr
                    : null;
            if (
                ageMin < FLAT_DEDUPE_MAX_AGE_MIN &&
                lastAction === 'HOLD' &&
                priceMoveAtr != null &&
                priceMoveAtr <= FLAT_DEDUPE_MAX_MOVE_ATR
            ) {
                emitGateDebug('flat_quarter_tick_dedupe', {
                    gate: 'FLAT_DEDUPE',
                    ageMin: Number(ageMin.toFixed(1)),
                    priceMoveAtr: Number(priceMoveAtr.toFixed(3)),
                    actionabilityReason: actionability.reason,
                });
                const decision = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'no_new_information',
                    reason: `flat_skip_dedupe_same_setup_${actionability.reason}`,
                };
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'flat_dedupe',
                    reason: decision.reason,
                    gates: gatesOut.gates,
                    metrics: {
                        ...gatesOut.metrics,
                        dedupe: {
                            ageMin: Number(ageMin.toFixed(1)),
                            priceMoveAtr: Number(priceMoveAtr.toFixed(3)),
                            actionabilityReason: actionability.reason,
                        },
                    },
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision,
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'flat_dedupe' },
                    gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                    usedTape,
                    promptSkipped: true,
                });
            }
        }

        // 6z) ai-bouncer soft gate (flat entry scans ONLY): a cheap triage
        // model decides whether the expensive decision call is worth making.
        // It may only SKIP work — never unlocks what hard gates blocked — and
        // is HARD-bypassed on open-position calls (skipping management risks a
        // missed exit), wake calls (the expensive model armed that band
        // itself), and swept-entry re-evaluations (the resting order was
        // already cancelled upstream and demands a full re-decision).
        // Fail-open: null verdict (disabled/error) → proceed to the full call.
        // Placed BEFORE the supersede sweep below on purpose: the sweep's
        // premise is "this tick reaches a fresh AI evaluation", so a bouncer
        // skip must leave the previous tick's resting order untouched.
        let aiBouncerVerdict: AiBouncerVerdict | null = null;
        if (!positionOpen && !cooldownWakeActive && !sweptPendingEntry && swingAiBouncerEnabled()) {
            aiBouncerVerdict = await runAiBouncer({
                symbol,
                platform,
                category: category ?? null,
                price: Number.isFinite(effectivePrice) ? effectivePrice : null,
                change_24h_pct: Number.isFinite(Number(tickerData?.change24h))
                    ? Number(tickerData?.change24h)
                    : null,
                signal_strength: context.signal_strength ?? null,
                micro_bias_calc: context.micro_bias_calc ?? null,
                primary_bias: context.primary_bias ?? null,
                macro_bias: context.macro_bias ?? null,
                context_bias: context.context_bias ?? null,
                primary_trend_up: Boolean(context.primary_trend_up),
                primary_trend_down: Boolean(context.primary_trend_down),
                primary_breakout_confirmed: Boolean(context.primary_breakout_confirmed),
                primary_breakdown_confirmed: Boolean(context.primary_breakdown_confirmed),
                micro_entry_ok: Boolean(context.micro_entry_ok),
                aligned_driver_count: context.aligned_driver_count ?? null,
                regime_alignment: context.regime_alignment ?? null,
                location_confluence_score: context.location_confluence_score ?? null,
                micro_extension_atr: context.micro_extension_atr ?? null,
                primary_extension_atr: context.primary_extension_atr ?? null,
                breakout_retest_ok_primary: context.breakout_retest_ok_primary ?? null,
                breakout_retest_dir_primary: context.breakout_retest_dir_primary ?? null,
                actionability_branch: actionability?.reason ?? null,
            });
            if (aiBouncerVerdict && !aiBouncerVerdict.proceed) {
                const reasonSlug =
                    aiBouncerVerdict.reason
                        .toLowerCase()
                        .replace(/[^a-z0-9]+/g, '_')
                        .replace(/^_+|_+$/g, '')
                        .slice(0, 60) || 'not_worth_the_call';
                const decision: TradeDecision & Record<string, unknown> = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    summary: 'ai_bouncer_skip',
                    reason: `flat_skip_ai_bouncer_${reasonSlug}`,
                };
                const execRes = { placed: false, orderId: null, clientOid: null, reason: 'ai_bouncer_skip' };
                await persistPreAiSkip({
                    stage: 'ai_bouncer',
                    decision,
                    execResult: execRes,
                    gates: gatesOut.gates,
                    metrics: gatesOut.metrics,
                    usedTape,
                    snapshot: {
                        price: effectivePrice,
                        actionability,
                        aiBouncer: aiBouncerVerdict,
                    },
                });
                emitGateDebug('ai_bouncer', {
                    gate: 'AI_BOUNCER',
                    reason: aiBouncerVerdict.reason,
                    confidence: aiBouncerVerdict.confidence,
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision,
                    execRes,
                    gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                    usedTape,
                    promptSkipped: true,
                });
            }
        }

        // (No quarter-tick supersede sweep. A resting entry is a standing
        // commitment now, so a fresh evaluation no longer implies cancelling it —
        // the reconcile after the decision cancels only when the model actually
        // supersedes or withdraws.)

        // Past the gates → the AI will be called. Fetch its remaining inputs
        // together: news (its only consumer is the prompt), the nano (15m)
        // candles for wave/entry-timing geometry, and — for non-BTC crypto —
        // the BTC regime context (measured correlation/beta + BTC state). All
        // deferred to here so gated ticks never pay for them; each fails open
        // (prompt just omits the block).
        const [newsBundleRes, nanoRes, btcContext, promptLessons, perplexityContext, fearGreedContext] = await Promise.all([
            fetchNewsWithHeadlines(symbol, { platform, source: newsSource, category }),
            (async () => {
                try {
                    const nanoBundle = await fetchMarketBundle(symbol, NANO_TIMEFRAME, {
                        includeTrades: false,
                        candleLimit: 110,
                    });
                    const nanoCandlesRaw = nanoBundle?.candles;
                    const nanoCandles: unknown[] = Array.isArray(nanoCandlesRaw) ? nanoCandlesRaw : [];
                    return { nanoContext: computeNanoContext(nanoCandles), nanoCandles };
                } catch (err) {
                    console.warn(`Could not build nano (15m) context for ${symbol}:`, err);
                    return { nanoContext: null, nanoCandles: [] as unknown[] };
                }
            })(),
            // BTC regime context for non-BTC crypto (loadBtcContext itself also
            // no-ops on BTCUSDT and honors SWING_BTC_CONTEXT_ENABLED). Bitget
            // only: the measurements come from Bitget perp candles.
            platform === 'bitget' && category === 'crypto'
                ? loadBtcContext(symbol)
                : Promise.resolve(null),
            // Curated post-mortem lessons for this symbol / its asset class /
            // global (max 5, confidence-sorted). SWING_LESSONS_MODE=off or an
            // empty library returns [] — the prompt block just doesn't render.
            loadPromptLessons(symbol, category),
            // Fresh search-grounded news+social digest (Perplexity sonar via
            // the AI gateway, KV-cached). SWING_PERPLEXITY_ENABLED opt-in;
            // fails open to null like the rest of the bundle.
            loadPerplexityContext(symbol, { platform, category }),
            // Daily crypto Fear & Greed index (alternative.me, KV-cached 1h,
            // market-wide so one value serves every symbol). Crypto only;
            // default-on with SWING_FEAR_GREED_ENABLED as kill switch; fails
            // open to null like the rest of the bundle.
            category === 'crypto' ? loadFearGreedContext() : Promise.resolve(null),
        ]);
        newsBundle = newsBundleRes;
        // Nano (15m) geometry is an ENTRY-TIMING tool: injected into the prompt
        // only when flat. In-position ticks manage against primary (4H)
        // structure — feeding 15m wave position there produced intraday exit
        // narratives ("nano crest") that cut swing winners at +0.36R while the
        // planned targets sat 3.5R away. The candles are still fetched every AI
        // tick because event-reaction measurements reuse them below.
        const { nanoContext: nanoContextRaw, nanoCandles } = nanoRes;
        const nanoContext = positionOpen ? null : nanoContextRaw;
        // Post-event reaction measurements: only when a high-impact release is in
        // the recent lookback (forexEventContext.recentEvents), quantified from the
        // nano 15m candles already fetched above — zero extra I/O. Fails open like
        // nano: null just omits the prompt block.
        const eventReaction = swingEventReactionEnabled()
            ? buildEventReactionContext({
                  recentEvents: forexEventContext?.recentEvents,
                  candles: nanoCandles,
              })
            : null;
        const { system, user, userCompact } = swingState.assemble(
            newsBundle?.sentiment ?? null,
            newsBundle?.headlines ?? [],
            nanoContext,
            sweptPendingEntry,
            standingEntry,
            eventReaction,
            btcContext,
            promptLessons,
            perplexityContext,
            fearGreedContext,
        );

        // 7) Query AI via the provider switch (SWING_AI_PROVIDER; post-parse
        // enforces allowed_actions + close_conditions). Capital decides leverage
        // by asset class, so it uses the leverage-free schema. Ticks with a live
        // conversation chain onto it: in-position ticks manage the trade with
        // memory of the entry thesis and every prior management tick, and flat
        // ticks re-evaluating a resting entry remember why they placed
        // it ("market moved — is this entry still valid?"). Fresh flat scans
        // carry no thread and stay stateless.
        // Conversation context is provider-scoped: both providers chain through
        // the stored transcript, but the formats differ (Claude MessageParam
        // turns vs OpenAI plain text turns), so a thread row written by the
        // OTHER provider (mid-position cutover/rollback) degrades this tick to
        // stateless — the prompt's "position adopted mid-life" branch covers it —
        // and this tick's persist below re-anchors the thread on the active
        // provider.
        const activeChainProvider = resolveSwingAiProvider();
        const chainedTranscript = aiThreadProvider === activeChainProvider ? aiThreadTranscript : null;
        // A CONFIRMED wake fire — the band held for the model's own confirm
        // window, or the break extended beyond it by force. This used to skip
        // the AI and execute a synthetic entry (wakeAutoEntry, deleted
        // 2026-08-30): a stop order IS "enter when price crosses X", so
        // emulating one in application code was redundant once the model could
        // place the real thing. Commitment now lives in orders; a wake always
        // wakes.
        //
        // This once needed a timing bypass too: micro_entry_ok reads false in
        // the first minutes of a genuine break, and the entry-timing constraint
        // would have coerced the model's BUY straight to HOLD. That constraint
        // is gone (demoted to a measurement), so nothing needs waiving — the
        // flag now only labels the cohort for measurement.
        const confirmedWakeFire =
            !positionOpen &&
            !!cooldownWake &&
            !cooldownWake.expired &&
            (cooldownWake.sustainedMinutes != null || cooldownWake.breakExtensionAtr != null);
        const {
            json: decisionRaw,
            responseId: aiResponseId,
            provider: aiCallProvider,
            model: aiCallModel,
            usage: aiCallUsage,
            appendTurns: aiAppendTurns,
        } = await callSwingDecision({
            system,
            user,
            schema: platform === 'capital' ? SWING_DECISION_SCHEMA_NO_LEVERAGE : SWING_DECISION_SCHEMA,
            thread: { transcript: chainedTranscript },
            // The model gets the full turn; the THREAD keeps the abbreviated
            // one, so a multi-day hold stops resending a stale tape per
            // management tick (see computeSwingState userCompact).
            userForTranscript: userCompact,
        });
        const decision = postprocessDecision({
            decision: decisionRaw,
            context,
            gates: gatesOut.gates,
            positionOpen,
            recentActions,
            positionContext,
            policy: decisionPolicy,
            lastClosedPosition,
        });

        // The profit-lock margin-recycle maneuver is crypto/Bitget only (set-leverage
        // + position TP/SL amend). Null the fields on any other venue so they never
        // reach execution or the decision history for a non-crypto instrument.
        if (platform !== 'bitget') {
            decision.raise_leverage_to = null;
            decision.move_stop_to_be = false;
        }
        // Nano (15m) bias measured at decision time — persisted on the decision
        // so the dashboard can render a Nano chip next to the other TF biases.
        decision.nano_bias = nanoContext?.bias ?? null;
        // Provider message id of this call (gateway gen_..., Claude msg_...) —
        // persisted in ai_decision_json so every decision row maps to its turn
        // in the conversation. The previous id (null on stateless calls) lets
        // the dashboard link chained decisions on the timeline; chaining itself
        // runs through the stored transcript, the id only fills the linkage slot
        // and is meaningful only when this tick actually chained.
        decision.response_id = aiResponseId;
        decision.previous_response_id =
            aiThreadProvider === activeChainProvider ? aiThreadResponseId : null;
        // Which provider/model served this call and what it cost (cache activity
        // included) — rides in ai_decision_json next to response_id, so every
        // decision row is self-describing for post-mortems and token audits.
        decision.ai_provider = aiCallProvider;
        decision.ai_model = aiCallModel;
        decision.ai_usage = aiCallUsage;

        // Pullback entry limit first (its price anchors everything downstream):
        // validate the model's limit against live price + ATR — too far clamps;
        // wrong side / inside the noise band / unverifiable DROPS the entry for
        // this tick (no silent market fallback — the model asked for a patience
        // price, and null is its way to request market).
        const tpslAtrRaw = Number(indicators?.metrics?.[timeFrame]?.atr);
        const primaryAtrSane = Number.isFinite(tpslAtrRaw) && tpslAtrRaw > 0 ? tpslAtrRaw : null;
        const marketAnchor = Number.isFinite(lastPrice) ? lastPrice : effectivePrice;

        // Last-AI-look reference for the 1-minute wake-watcher: price + primary
        // ATR at the moment the model actually saw this market. The watcher
        // compares the live price against it to decide an in-position emergency
        // look (≥ N ATR move) without fetching candles per minute. Best-effort;
        // never blocks the decision path.
        if (!dryRun && Number.isFinite(marketAnchor) && (marketAnchor as number) > 0) {
            kvSetJson(
                wakeWatchRefKey(platform, symbol),
                { price: marketAnchor, atr: primaryAtrSane, ts: Date.now() } satisfies WakeWatchRef,
                7 * 24 * 3600,
            ).catch((err: unknown) => console.warn(`wake-watch ref stamp failed for ${symbol}:`, err));
        }
        const restingEntry = sanitizeRestingEntry({
            action: decision.action,
            positionOpen,
            price: marketAnchor,
            primaryAtr: primaryAtrSane,
            entryLimitPrice: decision.entry_limit_price ?? null,
            entryStopPrice: decision.entry_stop_price ?? null,
            platform,
        });
        // Both legs are rewritten from the sanitized result so exactly one (or
        // neither) survives — the executor picks its order type off `kind`.
        decision.entry_limit_price = restingEntry.kind === 'limit' ? restingEntry.price : null;
        decision.entry_stop_price = restingEntry.kind === 'stop' ? restingEntry.price : null;
        decision.resting_entry_kind = restingEntry.kind;
        if (restingEntry.dropEntry && (decision.action === 'BUY' || decision.action === 'SELL')) {
            decision.action = 'HOLD';
            decision.reason =
                `${String(decision.reason ?? '')} [entry dropped: ${restingEntry.notes.join(',')}]`.trim();
        }
        // Bracket anchor: for a resting entry the protective stop and TP must be
        // sized from the RESTING price (where the position would actually open),
        // not from the current price.
        const bracketAnchor = restingEntry.price ?? marketAnchor;

        // Exchange-side TP/SL: validate the model's price targets against the
        // bracket anchor + primary ATR (correct side of price, min/max distance;
        // stop never wider than the catastrophe distance, amendments tighten-only
        // vs the standing stop), with a 3×ATR fallback TP on entries so every
        // entry ships with a resting TP.
        const exchangeTpsl = sanitizeExchangeTpSl({
            action: decision.action,
            positionOpen,
            side: positionInfo.status === 'open' ? positionInfo.holdSide : null,
            price: bracketAnchor,
            primaryAtr: primaryAtrSane,
            takeProfitPrice: decision.take_profit_price ?? null,
            stopLossPrice: decision.stop_loss_price ?? null,
            exitSizePct: decision.exit_size_pct ?? null,
            standingStopLossPrice: currentStopLoss,
        });
        decision.take_profit_price = exchangeTpsl.takeProfitPrice;
        decision.stop_loss_price = exchangeTpsl.stopLossPrice;

        // Flat-HOLD cooldown request: clamp minutes and validate wake-band sides
        // against live price; write the SANITIZED values back onto the decision
        // so history/dashboard show what was actually armed. Persisted after
        // execution (below) — only a flat HOLD ever carries non-null values.
        // The cooldown_wake_* fields carry FLAT cooldown bands or IN-POSITION
        // wake bands depending on position state (normalizeDecision routes
        // eligibility); read the raw values once, run the state's sanitizer,
        // and write the surviving values back so history shows what was armed.
        const rawWakeAbove = decision.cooldown_wake_above;
        const rawWakeBelow = decision.cooldown_wake_below;
        const rawWakeNote = decision.cooldown_wake_note;
        const holdCooldownRaw = sanitizeHoldCooldown({
            action: decision.action,
            positionOpen,
            price: marketAnchor,
            cooldownMinutes: decision.cooldown_minutes,
            wakeAbove: rawWakeAbove,
            wakeBelow: rawWakeBelow,
            wakeNote: rawWakeNote,
            wakeConfirmMinutes: decision.cooldown_wake_confirm_minutes,
        });
        // Wake re-arm ratchet guard: a fresh wake fire that still ends in HOLD
        // may not re-arm a band on the SAME side it just fired on — that is
        // the measured goalpost-move loop (60d replay: 70 consecutive-wake
        // chains, 51 never entered; US100 was woken 21 times in 4 days at
        // ever-moving same-side levels). The OPPOSITE side survives: a retest
        // band at a broken level after declining a confirmed break is the
        // retest protocol, not the ratchet. The cooldown itself (quiet period)
        // also survives — only the same-side band is dropped, so the loop
        // ends at the normal cadence.
        const holdCooldown = (() => {
            // Reclaim look is READ-ONLY wrt the cooldown row: the band's plan
            // did not fire and stays armed exactly as it was — any cooldown_*
            // fields the model outputs on a non-entry are ignored (an entry
            // nulls them anyway), so the look can never rewrite or shorten
            // the standing plan.
            if (reclaimWake || sessionReclaim) {
                return {
                    cooldownMinutes: null,
                    wakeAbove: null,
                    wakeBelow: null,
                    wakeNote: null,
                    confirmMinutes: null,
                    notes: [...holdCooldownRaw.notes, 'reclaim_look_read_only'],
                };
            }
            if (positionOpen || !cooldownWake || cooldownWake.expired) {
                return holdCooldownRaw;
            }
            if (String(decision.action || '').toUpperCase() !== 'HOLD') return holdCooldownRaw;
            const dropAbove = cooldownWake.crossed === 'above' && holdCooldownRaw.wakeAbove !== null;
            const dropBelow = cooldownWake.crossed === 'below' && holdCooldownRaw.wakeBelow !== null;
            if (!dropAbove && !dropBelow) return holdCooldownRaw;
            const wakeAbove = dropAbove ? null : holdCooldownRaw.wakeAbove;
            const wakeBelow = dropBelow ? null : holdCooldownRaw.wakeBelow;
            const anyBand = wakeAbove !== null || wakeBelow !== null;
            return {
                ...holdCooldownRaw,
                // The AI requested this quiet period believing a band was
                // watching it. When the guard strips the ONLY band, keeping
                // the bare cooldown would leave the symbol blind for hours on
                // terms the AI never signed — drop the cooldown with it (full
                // fold: back to the normal cadence).
                cooldownMinutes: anyBand ? holdCooldownRaw.cooldownMinutes : null,
                wakeAbove,
                wakeBelow,
                wakeNote: anyBand ? holdCooldownRaw.wakeNote : null,
                confirmMinutes: anyBand ? holdCooldownRaw.confirmMinutes : null,
                notes: [...holdCooldownRaw.notes, 'wake_rearm_same_side_dropped'],
            };
        })();
        // In-position wake bands: side vs live price, strictly inside the
        // bracket as it will actually rest (this tick's sanitized amend, else
        // the standing leg), min ATR distance (churn guard). All-null when
        // flat, when the flag is off, or on a non-HOLD/non-partial action.
        const positionWakeBands = sanitizePositionWake({
            action: decision.action,
            positionOpen,
            exitSizePct: decision.exit_size_pct ?? null,
            price: marketAnchor,
            primaryAtr: primaryAtrSane,
            takeProfitPrice: exchangeTpsl.takeProfitPrice ?? currentTakeProfit,
            stopLossPrice: exchangeTpsl.stopLossPrice ?? currentStopLoss,
            wakeAbove: rawWakeAbove,
            wakeBelow: rawWakeBelow,
            wakeNote: rawWakeNote,
        });
        decision.cooldown_minutes = holdCooldown.cooldownMinutes;
        decision.cooldown_wake_above = positionOpen ? positionWakeBands.wakeAbove : holdCooldown.wakeAbove;
        decision.cooldown_wake_below = positionOpen ? positionWakeBands.wakeBelow : holdCooldown.wakeBelow;
        decision.cooldown_wake_note = positionOpen ? positionWakeBands.wakeNote : holdCooldown.wakeNote;
        // Sustained confirmation is a flat-band concept only (in-position
        // bands stay instant by design).
        decision.cooldown_wake_confirm_minutes = positionOpen ? null : holdCooldown.confirmMinutes;
        const wakeNotes = [...holdCooldown.notes, ...positionWakeBands.notes];
        if (wakeNotes.length) {
            decision.cooldown_notes = wakeNotes;
        }

        // Failed-break watch trigger: side-validate and write the sanitized
        // value back so history shows what was actually armed.
        // Persisted after execution (below) — only a placed flat entry arms it.
        // Anchored at the resting price for the same reason the bracket is: the
        // trigger must sit behind the position once it EXISTS, not behind live
        // price now. On a resting STOP the entry opens beyond the level it is
        // breaking, so at placement time that level is still ahead of price —
        // validating against `marketAnchor` would drop the failed-break watch on
        // exactly the entries whose whole thesis is a break.
        const entryTrigger = sanitizeEntryTrigger({
            action: decision.action,
            positionOpen,
            price: bracketAnchor,
            triggerPrice: decision.entry_trigger_price,
        });
        decision.entry_trigger_price = entryTrigger.triggerPrice;
        if (entryTrigger.notes.length) {
            decision.entry_trigger_notes = entryTrigger.notes;
        }

        // 8) Execute (dry run unless explicitly disabled), using leveraged notional for gates
        const execLeverage = capitalLeverage ?? getTargetLeverage(decision);
        const execNotionalUSDT = sideSizeUSDT * (execLeverage ?? 1);
        const gatesForExec =
            execNotionalUSDT !== baseNotionalUSDT
                ? getGates({
                      symbol,
                      bundle,
                      analytics,
                      indicators,
                      notionalUSDT: execNotionalUSDT,
                      positionOpen,
                      disableSymbolExclusions: platform === 'capital',
                      atrFloorScale,
                      marketCategory: category,
                  })
                : gatesOut;

        if (
            !positionOpen &&
            (decision.action === 'BUY' || decision.action === 'SELL') &&
            gatesForExec.preDecision
        ) {
            emitGateDebug('entry_blocked_after_ai', {
                gate: 'BASE_GATES_EXEC_NOTIONAL',
                action: decision.action,
                preDecisionReason: gatesForExec.preDecision.reason,
                gates: gatesForExec.gates,
                spreadBpsNow: safeNum(gatesForExec.metrics?.spreadBpsNow, NaN),
                expectedSlippageBps: safeNum(gatesForExec.metrics?.expectedSlippageBps, NaN),
                atrPctNow: safeNum(gatesForExec.metrics?.atrPctNow, NaN),
            });
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                category,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision,
                execRes: { placed: false, orderId: null, clientOid: null, reason: 'gates_short_circuit' },
                gates: { ...gatesForExec.gates, metrics: gatesForExec.metrics },
                forexEventContext: forexEventContext,
                forexSessionContext,
                usedTape,
                ...(debugGates
                    ? {
                          gateDebug: {
                              blockedBy: 'BASE_GATES_EXEC_NOTIONAL',
                              reason: gatesForExec.preDecision.reason,
                              action: decision.action,
                              gates: gatesForExec.gates,
                              metrics: gatesForExec.metrics,
                          },
                      }
                    : {}),
            });
        }

        // Entry protective stop: the model's structural invalidation stop when it
        // survived sanitation (sanitizeExchangeTpSl: protective side, 1–3×ATR
        // from the bracket anchor), otherwise the deliberately WIDE ATR-based
        // catastrophe stop — a circuit breaker bounding the position during the
        // gap between AI evaluations, not a tactical exit.
        const CATASTROPHE_STOP_ATR_MULT = 3;
        let stopLossPrice: number | null = null;
        // Any action that opens fresh exposure gets a protective stop —
        // including REVERSE, whose new position is the OPPOSITE of the current
        // side (it previously opened unprotected; gap closed 2026-07-08).
        const bracketEntrySide: 'long' | 'short' | null =
            decision.action === 'BUY'
                ? 'long'
                : decision.action === 'SELL'
                  ? 'short'
                  : decision.action === 'REVERSE' && positionInfo.status === 'open'
                    ? positionInfo.holdSide === 'long'
                        ? 'short'
                        : 'long'
                    : null;
        if (bracketEntrySide) {
            if (exchangeTpsl.stopLossPrice != null) {
                stopLossPrice = exchangeTpsl.stopLossPrice;
            } else {
                const primaryAtr = Number(indicators?.metrics?.[timeFrame]?.atr);
                // Anchored at the resting price when an entry is resting — the stop
                // protects the position from where it would actually open.
                const anchor = bracketAnchor;
                if (Number.isFinite(primaryAtr) && primaryAtr > 0 && Number.isFinite(anchor) && anchor > 0) {
                    const dist = CATASTROPHE_STOP_ATR_MULT * primaryAtr;
                    const raw = bracketEntrySide === 'long' ? anchor - dist : anchor + dist;
                    stopLossPrice = raw > 0 ? raw : null;
                }
            }
        }

        // Fixed-fractional risk sizing: one full stop-out costs RISK_EQUITY_PCT
        // of account equity, so the finalized stop distance (structural or
        // catastrophe) decides the notional and the margin follows from it at
        // the execution leverage. Failure modes fail SMALL: no equity reading →
        // fixed fallback risk inside resolveRiskBasedSizing; no stop/anchor →
        // legacy fixed sideSizeUSDT. If the risk budget cannot buy the venue's
        // minimum size, the entry is dropped — sizing UP would silently breach
        // the risk budget, which is how the old fixed notional produced −$12
        // outliers next to −$0.30 losers.
        let execSideSizeUSDT = sideSizeUSDT;
        if (bracketEntrySide && stopLossPrice != null) {
            const equityUsd =
                platform === 'capital'
                    ? await fetchCapitalAccountEquityUsd().catch(() => null)
                    : await fetchBitgetAccountEquityUsd();
            const riskSizing = resolveRiskBasedSizing({
                entryPrice: bracketAnchor,
                stopPrice: stopLossPrice,
                equityUsd,
                leverage: execLeverage ?? null,
            });
            if (riskSizing) {
                let minNotionalUsd: number | null = platform === 'bitget' ? 5 : null;
                if (platform === 'capital') {
                    const afford = await evaluateCapitalMinSizeAffordability(symbol).catch(() => null);
                    const affordMin = afford?.minNotionalUsd;
                    minNotionalUsd = typeof affordMin === 'number' && Number.isFinite(affordMin) ? affordMin : null;
                }
                emitGateDebug('risk_sizing', {
                    gate: 'RISK_SIZING',
                    riskUsd: Number(riskSizing.riskUsd.toFixed(2)),
                    notionalUsd: Number(riskSizing.notionalUsd.toFixed(2)),
                    marginUsd: Number(riskSizing.marginUsd.toFixed(2)),
                    stopDistancePct: Number((riskSizing.stopDistancePct * 100).toFixed(3)),
                    equityUsd,
                    source: riskSizing.source,
                    riskEquityPct: RISK_EQUITY_PCT,
                    minNotionalUsd,
                });
                // Venue-side balance check (Bitget): the exchange rejects any
                // order whose margin exceeds the spendable balance with error
                // 40762 — which used to escape as a tick-killing handler_error
                // (6× on 2026-07-24). Capital has its own exec-time sizing
                // gate. 2% headroom for fees/price drift between check and
                // fill; fails open (null read) — the venue still backstops.
                let bitgetAvailableUsd: number | null = null;
                if (platform === 'bitget' && !dryRun) {
                    bitgetAvailableUsd = await fetchBitgetAccountAvailableMarginUsd();
                }
                if (minNotionalUsd !== null && riskSizing.notionalUsd < minNotionalUsd) {
                    decision.action = 'HOLD';
                    decision.reason =
                        `${String(decision.reason ?? '')} [entry dropped: risk_budget_below_min_size ` +
                        `notional≈${riskSizing.notionalUsd.toFixed(0)} min≈${minNotionalUsd.toFixed(0)}]`.trim();
                } else if (bitgetAvailableUsd !== null && riskSizing.marginUsd > bitgetAvailableUsd * 0.98) {
                    decision.action = 'HOLD';
                    decision.reason =
                        `${String(decision.reason ?? '')} [entry dropped: insufficient_available_margin ` +
                        `need≈${riskSizing.marginUsd.toFixed(2)} have≈${bitgetAvailableUsd.toFixed(2)}]`.trim();
                } else {
                    execSideSizeUSDT = riskSizing.marginUsd;
                }
                // Persisted with the decision row so post-mortems can audit the
                // realized risk against the budget.
                decision.risk_sizing = {
                    risk_usd: Number(riskSizing.riskUsd.toFixed(2)),
                    notional_usd: Number(riskSizing.notionalUsd.toFixed(2)),
                    margin_usd: Number(riskSizing.marginUsd.toFixed(2)),
                    stop_distance_pct: Number((riskSizing.stopDistancePct * 100).toFixed(3)),
                    equity_usd: equityUsd,
                    source: riskSizing.source,
                };
            }
        }

        // Resting-entry reconcile — the standing commitment's fate, decided by
        // what the model actually said this tick:
        //
        //   HOLD (no withdraw)  -> leave it resting. The default, and the whole
        //                          point: silence preserves the commitment.
        //   BUY/SELL            -> supersede. Cancel first, then place, so the
        //                          new order never stacks on the old one.
        //   withdraw_resting_entry -> cancel, no position.
        //
        // A cancel that races a fill is caught the same way it always was: the
        // position surfaces and the pending-entry conversation manages it.
        const supersedesRestingEntry = decision.action === 'BUY' || decision.action === 'SELL';
        const withdrawsRestingEntry = Boolean((decision as Record<string, unknown>).withdraw_resting_entry);
        if (!positionOpen && standingEntry && (supersedesRestingEntry || withdrawsRestingEntry) && !dryRun) {
            const sweep = await sweepPendingEntries();
            if (await pendingEntryFilledMidTick(sweep)) {
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'pending_entry_filled',
                    reason: 'pending_entry_filled_during_supersede',
                    kvMarker: false,
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision: { ...decision, action: 'HOLD', summary: 'pending_entry_filled' },
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'pending_entry_filled' },
                    usedTape,
                    promptSkipped: true,
                });
            }
            const sweepFailure = classifyPendingEntrySweep(sweep);
            if (sweepFailure) {
                // The old order may still be live — placing now would stack.
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'pending_entry_sweep_failed',
                    reason: sweepFailure,
                });
                return res.status(200).json({
                    symbol,
                    platform,
                    newsSource,
                    category,
                    instrumentId,
                    timeFrame,
                    dryRun,
                    decisionPolicy,
                    decision: { ...decision, action: 'HOLD', summary: 'pending_entry_sweep_failed' },
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'pending_entry_sweep_failed' },
                    usedTape,
                    promptSkipped: true,
                });
            }
            standingEntry = null;
        }
        // Withdraw is not an entry: the order is gone and this tick ends flat.
        if (withdrawsRestingEntry && decision.action !== 'BUY' && decision.action !== 'SELL') {
            decision.reason = `${String(decision.reason ?? '')} [resting entry withdrawn]`.trim();
        }

        const execRes =
            platform === 'capital' || productType === null
                ? await executeCapitalDecision(
                      symbol,
                      execSideSizeUSDT,
                      decision,
                      dryRun,
                      stopLossPrice,
                      true,
                      exchangeTpsl.takeProfitPrice,
                  )
                : await executeDecision(
                      symbol,
                      execSideSizeUSDT,
                      decision,
                      productType,
                      dryRun,
                      stopLossPrice,
                      exchangeTpsl.takeProfitPrice,
                  );
        const executedAtMs = Date.now();

        // Thread lifecycle bookkeeping. An entry that actually placed an order
        // STARTS or CONTINUES a conversation (pending_entry while a resting
        // limit rests; straight to in_position on a market entry — the upsert
        // replaces any stale row while previous_response_id keeps a re-issued
        // limit on its original conversation). An in-position tick ADVANCES the
        // chain head (HOLD / partial CLOSE / REVERSE — the reversal keeps its
        // conversation). A full CLOSE that executed ends it, and so does a flat
        // tick that chained onto a resting limit's conversation but did NOT
        // re-issue (entry dropped → conversation over; also cleans up a
        // lingering row when the order vanished before the sweep). TP/SL fills
        // between ticks are caught by the reconcile at the top of the next
        // tick. Best-effort, never blocks the trading path.
        if (!dryRun && aiResponseId) {
            try {
                const entryPlacedNow =
                    !positionOpen && execRes?.placed === true && (decision.action === 'BUY' || decision.action === 'SELL');
                const fullCloseExecuted =
                    positionOpen &&
                    decision.action === 'CLOSE' &&
                    execRes?.placed === true &&
                    Number(decision.exit_size_pct ?? 100) >= 100;
                // The conversation is OURS to store on both providers (the AI
                // Gateway is stateless) — append this tick's turns (sent user
                // turn + assistant response) to the transcript the tick chained
                // onto, capped so a long-lived position can't grow the row
                // unboundedly.
                const activeProvider = resolveSwingAiProvider();
                const nextTranscript =
                    Array.isArray(aiAppendTurns) && aiAppendTurns.length
                        ? truncateClaudeTranscript([
                              ...(Array.isArray(chainedTranscript) ? chainedTranscript : []),
                              ...aiAppendTurns,
                          ] as Parameters<typeof truncateClaudeTranscript>[0])
                        : null;
                if (fullCloseExecuted) {
                    await endSwingAiThread(platform, symbol);
                } else if (entryPlacedNow) {
                    // Whether an order is RESTING comes from execution, not from
                    // guessing at which price field was used. Both venues report
                    // pendingEntry for a limit AND a stop; a market entry omits
                    // it. Sniffing decision.entry_limit_price (as this did until
                    // 2026-08-31) marked every resting STOP as in_position with
                    // no position on the venue — which hid it from the
                    // dashboard's pending-entry pill and, worse, made the
                    // wake-watcher's close-detection (step 4: in_position thread
                    // + flat venue ⇒ position closed) fire analyze against a
                    // symbol that had simply never entered.
                    const restingAfterExec = (execRes as Record<string, unknown> | null)?.pendingEntry === true;
                    await upsertSwingAiThread({
                        platform,
                        symbol,
                        status: restingAfterExec ? 'pending_entry' : 'in_position',
                        lastResponseId: aiResponseId,
                        provider: activeProvider,
                        transcript: nextTranscript,
                    });
                } else if (positionOpen) {
                    await upsertSwingAiThread({
                        platform,
                        symbol,
                        status: 'in_position',
                        lastResponseId: aiResponseId,
                        provider: activeProvider,
                        transcript: nextTranscript,
                    });
                } else if (aiThreadResponseId) {
                    await endSwingAiThread(platform, symbol);
                }
            } catch (err) {
                console.warn(`AI thread update failed for ${symbol}:`, err);
            }
        }

        // Arm the AI-requested flat cooldown (sanitized above; non-null only on
        // a flat HOLD). The gate at the top of the flat path consumes it on
        // expiry or when a wake band is crossed. Best-effort, never blocks.
        if (!dryRun && holdCooldown.cooldownMinutes) {
            try {
                await upsertSwingAiCooldown({
                    platform,
                    symbol,
                    untilMs: executedAtMs + holdCooldown.cooldownMinutes * 60_000,
                    wakeAbove: holdCooldown.wakeAbove,
                    wakeBelow: holdCooldown.wakeBelow,
                    wakeNote: holdCooldown.wakeNote,
                    wakeConfirmMinutes: holdCooldown.confirmMinutes,
                    // Primary ATR at set time — anchors the extension confirm.
                    wakeAtr: primaryAtrSane,
                });
            } catch (err) {
                console.warn(`AI cooldown arm failed for ${symbol}:`, err);
            }
        }

        // Failed-break watch bookkeeping (best-effort, never blocks): a PLACED
        // flat entry arms the trigger the model declared (or clears a stale row
        // when the new thesis is not a break); a placed full CLOSE or REVERSE
        // ends the watched position, so its trigger goes too (REVERSE's new
        // opposite side is untracked by design — the model can re-declare on
        // its next look). Bracket-side closes are cleaned up by the watcher.
        if (!dryRun && execRes?.placed) {
            try {
                const action = String(decision.action || '').toUpperCase();
                if (!positionOpen && (action === 'BUY' || action === 'SELL')) {
                    if (entryTrigger.triggerPrice) {
                        await upsertSwingBreakTrigger({
                            platform,
                            symbol,
                            side: action === 'BUY' ? 'long' : 'short',
                            triggerPrice: entryTrigger.triggerPrice,
                            timeFrame,
                            entryAtMs: executedAtMs,
                        });
                    } else {
                        await clearSwingBreakTrigger(platform, symbol);
                    }
                } else if (
                    action === 'REVERSE' ||
                    (action === 'CLOSE' && (decision.exit_size_pct == null || Number(decision.exit_size_pct) >= 100))
                ) {
                    await clearSwingBreakTrigger(platform, symbol);
                }
            } catch (err) {
                console.warn(`Break-trigger bookkeeping failed for ${symbol}:`, err);
            }
        }

        // Consume the session-reclaim event after the decision is durably
        // recorded — the per-pool looked-marker (watcher-side) already spent
        // the budget, this only stops the next tick from re-reading a used
        // event before its TTL lapses. Best-effort.
        if (!dryRun && sessionReclaim) {
            await kvDel(sessionSweepEventKey(platform, symbol)).catch((err) =>
                console.warn(`session-reclaim event consume failed for ${symbol}:`, err),
            );
        }

        // A PLACED reclaim-look entry supersedes the band's standing plan —
        // consume the cooldown row (a HOLD leaves it fully armed; the row was
        // never claimed on this path). Best-effort, never blocks.
        if (
            !dryRun &&
            reclaimWake &&
            !positionOpen &&
            execRes?.placed === true &&
            (decision.action === 'BUY' || decision.action === 'SELL')
        ) {
            await clearSwingAiCooldown(platform, symbol).catch((err) =>
                console.warn(`reclaim-entry cooldown consume failed for ${symbol}:`, err),
            );
        }

        if (platform === 'capital' && !dryRun) {
            await persistCapitalClosedPositionSnapshot({
                symbol,
                positionInfo,
                execRes,
                exitPrice: Number.isFinite(lastPrice) ? lastPrice : Number.isFinite(effectivePrice) ? effectivePrice : null,
                closedAtMs: executedAtMs,
            });
        }

        const change24h = Number(tickerData?.change24h ?? tickerData?.changeUtc24h ?? tickerData?.chgPct);
        const spreadBpsSnapshot = safeNum(gatesForExec.metrics?.spreadBpsNow, safeNum(analytics.spreadBps, 0));
        const spreadAbsSnapshot = safeNum(analytics.spreadAbs ?? analytics.spread, 0);
        const bestBid = Number(analytics.bestBid);
        const bestAsk = Number(analytics.bestAsk);
        const snapshot = {
            category: category ?? undefined,
            platform,
            newsSource,
            instrumentId,
            price: Number.isFinite(lastPrice) ? lastPrice : undefined,
            change24h: Number.isFinite(change24h) ? change24h : undefined,
            spread: spreadBpsSnapshot,
            spreadBps: spreadBpsSnapshot,
            spreadAbs: spreadAbsSnapshot,
            bestBid: Number.isFinite(bestBid) ? bestBid : undefined,
            bestAsk: Number.isFinite(bestAsk) ? bestAsk : undefined,
            gates: gatesForExec.gates,
            metrics: gatesForExec.metrics,
            newsSentiment: newsBundle?.sentiment ?? null,
            newsHeadlines: newsBundle?.headlines ?? [],
            // Fresh Perplexity digest as fed to the prompt — keeps "did fresh
            // sentiment help" a SQL query. Omitted (not null) when disabled or
            // failed, so dark deployments leave decision rows byte-identical.
            ...(perplexityContext ? { perplexityContext } : {}),
            forexEventContext: forexEventContext,
            // Post-event reaction measurements as fed to the prompt (null when no
            // recent high-impact release) — makes "did the AI trade the drift and
            // did it pay" a SQL query over decisions × positions.
            eventReaction,
            // BTC regime context as fed to the prompt (null for BTC itself /
            // non-crypto) — keeps "did the AI fight the BTC regime and did it
            // pay" a SQL query over decisions × positions.
            btcContext,
            forexSessionContext,
            // Venue liquidity clock at decision time — lets "did this entry rest
            // into an open/break/thin reopen" stay a SQL query over snapshots
            // instead of a schedule reconstruction.
            venueEvents,
            positionContext,
            momentumSignals,
            // Which actionability branch admitted this call (confirmed_primary_structure /
            // bounce_long / bounce_short when flat). Skips already record theirs; persisting
            // it here makes per-branch outcome tracking a SQL query instead of a
            // reverse-engineering job over prompt STATE.
            actionability,
            // ai-bouncer verdict on ticks it let PROCEED — makes "does the
            // bouncer help" a SQL comparison of bouncer-passed vs
            // bouncer-skipped cohorts. Omitted (not null) when disabled,
            // bypassed, or failed-open, so dark deployments leave decision
            // rows byte-identical.
            ...(aiBouncerVerdict ? { aiBouncer: aiBouncerVerdict } : {}),
            // Wake-band trigger (null unless this call exists because price
            // crossed the previous flat HOLD's cooldown wake band — those calls
            // bypass the flat quality gates). Persisting it makes "what does the
            // AI do when its own wake level fires, and does it pay" a SQL query.
            cooldownWake,
            // Reclaim-wake trigger (null unless this call is a claimed
            // one-shot bounce-moment look at a swept band) — makes "do
            // reclaim looks convert, and do the entries pay" a SQL query.
            reclaimWake,
            // Session-pool sweep-reclaim trigger (phase 2) — same rationale.
            sessionReclaim,
            // In-position wake-band trigger (null unless this call exists
            // because price crossed a band the model set on a previous
            // management look) — same SQL-ability rationale as cooldownWake.
            positionWake: positionWakeFired,
            // Sanitized exchange-side bracket that actually went to execution,
            // plus any clamp/drop notes (e.g. tp_wrong_side_dropped) — makes
            // "what did the model ask for vs what shipped" a SQL query.
            exchangeTpsl: {
                takeProfitPrice: exchangeTpsl.takeProfitPrice,
                stopLossPrice: exchangeTpsl.stopLossPrice,
                standing: { takeProfitPrice: currentTakeProfit, stopLossPrice: currentStopLoss },
                notes: exchangeTpsl.notes,
            },
            // The resting entry that actually shipped: which tool the model
            // reached for, where it rested, and any clamp/drop notes. `kind` is
            // the column that makes "which plays does it pick, and do they
            // work" answerable in SQL alongside `strategy`.
            restingEntry: {
                kind: restingEntry.kind,
                price: restingEntry.price,
                notes: restingEntry.notes,
            },
            strategy: (decision as Record<string, unknown>).strategy ?? null,
        };

        await appendDecisionHistory({
            timestamp: executedAtMs,
            symbol,
            category: category ?? undefined,
            platform,
            instrumentId,
            newsSource,
            timeFrame,
            dryRun,
            prompt: { system, user },
            aiDecision: decision,
            execResult: execRes,
            snapshot,
            biasTimeframes: {
                context: contextTimeFrame,
                macro: macroTimeFrame,
                primary: timeFrame,
                micro: microTimeFrame,
                ...(nanoContext ? { nano: NANO_TIMEFRAME } : {}),
            },
        });
        // Tick-log row for the AI call keeps swing.tick_log a COMPLETE per-tick
        // series (skips + calls) — the decision row above holds the full detail.
        // No KV marker: the decision row already surfaces this tick on the UI.
        await recordTickOutcome({
            kind: 'ai_call',
            // 'confirmed_wake' marks a look the model was handed with the
            // break already proven (held its confirm window, or extended by
            // force) — the cohort that used to be entered mechanically. Kept
            // as its own stage so "does the model convert its own confirmed
            // plans now that it has stop orders?" stays one SQL query.
            stage: confirmedWakeFire
                ? 'confirmed_wake'
                : reclaimWake
                  ? 'reclaim_wake'
                  : sessionReclaim
                    ? 'session_reclaim'
                    : 'decision',
            reason: String(decision.action || 'HOLD'),
            gates: gatesOut.gates,
            // ai-bouncer PROCEED verdicts ride along so tick_log alone supports
            // the bouncer-passed vs bouncer-skipped cohort comparison.
            metrics: aiBouncerVerdict
                ? {
                      ...(gatesOut.metrics ?? {}),
                      aiBouncer: {
                          proceed: true,
                          confidence: aiBouncerVerdict.confidence,
                          reason: aiBouncerVerdict.reason,
                          latencyMs: aiBouncerVerdict.latencyMs,
                      },
                  }
                : gatesOut.metrics,
            kvMarker: false,
        });
        // Refusal investigation: a flat HOLD on a wake evaluation is a DECLINED
        // entry with a measurable counterfactual (the model chose the level
        // and wrote the plan itself). Enqueue it into the same post-mortem
        // pipeline as losses (trigger 'refusal'); the drain runs it after the
        // standard 12h delay so the analyst judges the skip against what price
        // actually did — and corrects the lesson library when the skip was
        // wrong. Best-effort (enqueue catches internally), never fails a tick.
        if (
            (cooldownWake || reclaimWake || sessionReclaim) &&
            !positionOpen &&
            !dryRun &&
            String(decision.action || '').toUpperCase() === 'HOLD'
        ) {
            await maybeEnqueueSwingRefusalInvestigation({
                platform,
                symbol,
                decidedAtMs: Date.now(),
                priceAtEval: effectivePrice,
            });
        }
        // Consume the claimed wake row only NOW — the wake's decision is
        // durably recorded above, so a crash anywhere earlier leaves the row
        // (lease expires, watcher re-fires) instead of losing the wake. When
        // this decision armed a fresh cooldown, the upsert already replaced
        // the row and reset the lease — nothing to clear.
        if (cooldownRowClaimed && !holdCooldown.cooldownMinutes) {
            try {
                await clearSwingAiCooldown(platform, symbol);
            } catch (err) {
                console.warn(`wake cooldown consume failed for ${symbol}:`, err);
            }
        }
        // Replace the thread's in-position wake bands with what THIS decision
        // asked for — nulls included (replace-on-every-look; consuming a fired
        // band falls out of replacement, since a restated just-fired band was
        // already dropped by the side check). Only after the history append
        // above, so a run dying earlier leaves the old bands armed and the
        // watcher re-fires instead of half-consuming the wake. Fresh-entry
        // calls write nulls, clearing stale bands a re-used thread row
        // inherited; a 0-row update (no thread) is harmless.
        if (POSITION_WAKE_ENABLED && !dryRun && aiResponseId) {
            try {
                await setSwingThreadWake({
                    platform,
                    symbol,
                    wakeAbove: positionWakeBands.wakeAbove,
                    wakeBelow: positionWakeBands.wakeBelow,
                    wakeNote: positionWakeBands.wakeNote,
                    setAtMs: executedAtMs,
                });
            } catch (err) {
                console.warn(`position wake persist failed for ${symbol}:`, err);
            }
        }
        // New decision recorded → bust the dashboard summary cache so the next load
        // reflects it. Best-effort; never blocks the trading path.
        await invalidateSwingSummaryCache();
        try {
            const overlayPositionInfo =
                !dryRun && execRes?.placed ? await fetchPositionInfo(symbol).catch(() => positionInfo) : positionInfo;
            // Post-decision bracket for the chart overlay: a fresh entry (incl.
            // REVERSE) carries the bracket just attached; an applied in-position
            // amend carries the new levels; otherwise the pre-decision standing
            // bracket still holds. Handles both venue result shapes (Bitget
            // per-leg {applied}, Capital {updated, stopLevel, profitLevel}).
            const execResView: ExecResultView | null = execRes;
            const tpslExec = execResView?.tpsl;
            const entryPlaced =
                execRes?.placed === true &&
                (decision.action === 'BUY' || decision.action === 'SELL' || decision.action === 'REVERSE');
            const tpAmended =
                tpslExec?.takeProfit?.applied === true ||
                (tpslExec?.updated === true && tpslExec?.profitLevel != null);
            const slAmended =
                tpslExec?.stopLoss?.applied === true || (tpslExec?.updated === true && tpslExec?.stopLevel != null);
            const overlayTakeProfit = entryPlaced
                ? exchangeTpsl.takeProfitPrice
                : tpAmended
                  ? exchangeTpsl.takeProfitPrice
                  : currentTakeProfit;
            const overlayStopLoss = entryPlaced
                ? stopLossPrice
                : slAmended
                  ? exchangeTpsl.stopLossPrice
                  : currentStopLoss;
            await warmPositionOverlayCacheFromAnalyze({
                symbol,
                platform,
                nowMs: Date.now(),
                openPositionInfo: overlayPositionInfo,
                openTakeProfitPrice: overlayTakeProfit,
                openStopLossPrice: overlayStopLoss,
            });
        } catch (err) {
            console.warn(`chart overlay post-decision warm failed for ${symbol}:`, err);
        }
        emitGateDebug('decision_recorded', {
            action: decision.action,
            usedTape,
            historyRecorded: true,
        });

        // 9) Respond
        return res.status(200).json({
            symbol,
            platform,
            newsSource,
            category,
            instrumentId,
            timeFrame,
            dryRun,
            decisionPolicy,
            decision,
            execRes,
            gates: { ...gatesForExec.gates, metrics: gatesForExec.metrics },
            forexEventContext: forexEventContext,
            forexSessionContext,
            usedTape,
            ...(debugGates
                ? {
                      gateDebug: {
                          enforcePrimaryCloseGate,
                          primaryCloseTime,
                          primaryCloseCadence,
                          positionOpen,
                          gateChecksCompleted: true,
                      },
                  }
                : {}),
        });
    } catch (err) {
        console.error('Error in /api/analyze:', err);
        const errMessage =
            typeof err === 'object' && err !== null && 'message' in err ? err.message : null;
        // Durable trace of the crash (best-effort): without this row a run
        // that dies between the gates and the decision record leaves no
        // evidence in tick_log at all. insertSwingTickLog never throws.
        // An AI-call failure (typed AiCallError — quota lapse, bad key, model
        // outage) gets its own stage so it's tellable apart from code crashes.
        if (tickErrorContext) {
            const stage = err instanceof AiCallError ? 'ai_unavailable' : 'handler_error';
            const reason = String(errMessage || err).slice(0, 300);
            await insertSwingTickLog({
                tsMs: Date.now(),
                symbol: tickErrorContext.symbol,
                platform: tickErrorContext.platform,
                kind: 'skip',
                stage,
                reason,
                cadence: tickErrorContext.cadence,
                dryRun: tickErrorContext.dryRun,
            });
            // Stage the failure on the KV last-scan marker too — it's what the
            // dashboard tooltip reads. Before this, an error tick skipped the
            // marker entirely, so an AI outage produced ZERO UI signal (the
            // marker just went stale). recordSwingLastScan never throws.
            await recordSwingLastScan(tickErrorContext.platform, tickErrorContext.symbol, { stage, reason });
        }
        return res.status(500).json({ error: errMessage || String(err) });
    } finally {
        // Countdown latch: the last swing cron of the 15-minute cycle to finish
        // rebuilds the dashboard summary blobs, so the warm always runs AFTER
        // the cycle's final decision landed instead of at a fixed cron offset
        // that races long analyzes. AWAITED on purpose (see recordSwingLastScan
        // above: void'd promises get dropped on serverless); the response is
        // already sent, but the function stays alive until the handler promise
        // settles. Never throws — on failure the summary-warm-fallback cron
        // covers the cycle a few minutes later.
        if (swingWarmLatchCycleId !== null) {
            try {
                if (await recordSwingAnalyzeFinished(swingWarmLatchCycleId)) {
                    console.log(`[swing_warm_latch] last finisher of cycle ${swingWarmLatchCycleId}; warming summaries`);
                    await warmAllSwingSummaries();
                    await markSwingWarmDone(swingWarmLatchCycleId);
                }
            } catch (err) {
                console.warn('swing warm latch failed; summary-warm-fallback cron will cover:', err);
            }
        }
    }
}
