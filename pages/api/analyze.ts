// api/analyze.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { requireAdminAccess } from '../../lib/admin';

import {
    fetchMarketBundle as fetchBitgetMarketBundle,
    computeAnalytics,
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
import { computeNanoContext } from '../../lib/swing/waveGeometry';
import { loadForexEventContext } from '../../lib/swing/forexEvents';
import { buildForexSessionLevelsContext } from '../../lib/swing/sessionLevels';
import { buildVenueSessionEvents } from '../../lib/swing/sessionEvents';

import {
    computeSwingState,
    computeMomentumSignals,
    postprocessDecision,
    resolveDecisionPolicy,
    resolveExtensionThresholds,
    sanitizeEntryLimit,
    sanitizeExchangeTpSl,
    sanitizeHoldCooldown,
    REENTRY_COOLDOWN_MIN,
    resolveReentryCooldown,
    SWING_DECISION_SCHEMA,
    SWING_DECISION_SCHEMA_NO_LEVERAGE,
} from '../../lib/ai';
import type { DecisionPolicy, LastClosedPosition, MomentumSignals } from '../../lib/ai';
import { callSwingDecision, resolveSwingAiProvider } from '../../lib/aiProvider';
import { truncateClaudeTranscript } from '../../lib/claudeAi';
import { getGates } from '../../lib/gates';

import {
    cancelPendingEntryOrders,
    classifyPendingEntrySweep,
    executeDecision,
    fetchPositionTpsl,
    getTargetLeverage,
    getTradeProductType,
} from '../../lib/trading';
import { composePositionContext } from '../../lib/positionContext';
import { updatePositionExtrema } from '../../lib/positionExtrema';
import { appendDecisionHistory, loadDecisionHistory } from '../../lib/history';
import { recordSwingAccountSnapshot } from '../../lib/swing/sync';
import { resolveRiskBasedSizing, RISK_EQUITY_PCT } from '../../lib/swing/riskSizing';
import { wakeWatchRefKey, type WakeWatchRef } from '../../lib/swing/wakeWatch';
import { kvSetJson } from '../../lib/kv';
import { maybeEnqueueSwingPostmortem } from '../../lib/swing/postmortem';
import { loadPromptLessons } from '../../lib/swing/lessons';
import {
    clearSwingAiCooldown,
    endSwingAiThread,
    getSwingAiCooldown,
    getSwingAiThread,
    insertSwingTickLog,
    loadClosedSwingPositions,
    markSwingAiThreadInPosition,
    upsertSwingAiCooldown,
    upsertSwingAiThread,
    upsertSwingPosition,
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

function safeNum(x: any, def = 0): number {
    const n = Number(x);
    return Number.isFinite(n) ? n : def;
}

async function persistCapitalClosedPositionSnapshot(params: {
    symbol: string;
    positionInfo: PositionInfo;
    execRes: any;
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
    if (!persist.has(key)) persist.set(key, {});
    return persist.get(key)!;
}

const ATR_ACTIVE_MIN_PCT = 0.0007; // ~0.07%
const STALE_TRADE_MINUTES = 15;

function readLatestTradeTimestamp(trades: any[]): number | null {
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
    trades: any[];
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
            gates?: Record<string, any> | null;
            metrics?: Record<string, any> | null;
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
            decision: Record<string, any>;
            execResult: Record<string, any>;
            gates?: Record<string, any>;
            metrics?: Record<string, any>;
            usedTape?: boolean;
            snapshot?: Record<string, any>;
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
                } as any,
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
        const isSwingCronAnalyzeRequest = requestPath === '/api/swing/analyze' && automationCron;
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
                const decision = {
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
                const decision = {
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
        // Bounded account-leverage history: one snapshot per HOURLY tick per
        // symbol (quarter ticks skip it so the flat 15m cadence doesn't 4x the
        // table). Best-effort; never blocks the trading path on failure.
        if (!quarterTick) {
            await recordSwingAccountSnapshot({
                platform,
                symbol,
                capturedAtMs: Date.now(),
                positionInfo: positionInfo as any,
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

        // Margin-aware pre-skip (Capital, flat only): if the account can't cover the
        // smallest tradeable size for this symbol, opening is impossible — skip before
        // spending the AI call and record it as an intentional skip rather than letting
        // it surface later as an INSUFFICIENT_AVAILABLE_MARGIN exec rejection. Open
        // positions are exempt: HOLD/CLOSE still need to run to manage them. Fails open.
        if (platform === 'capital' && !positionOpen) {
            const afford = await evaluateCapitalMinSizeAffordability(symbol).catch(() => null);
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
                const decision = {
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
        // when an entry order is placed, survives a pullback-limit fill into
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
        // The thread row claims a pullback limit is resting while we're flat —
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

        // Resting pullback entries (one-tick TTL). Sweep = cancel whatever the
        // PREVIOUS evaluation left resting. Hourly flat ticks always sweep
        // (TTL expiry, even when the gates then skip the AI); quarter ticks
        // sweep later, only when they actually reach a new AI evaluation
        // (supersede) — a gate-skipped quarter tick leaves the order resting.
        // If a cancel fails because the order just FILLED, the tick stops:
        // a position now exists and the next tick manages it.
        // Any OTHER unclean sweep (helper threw, pending-orders fetch failed,
        // cancel failed without a fill) also stops the tick — the previous
        // order may still be live on the venue, and a fresh entry on top of it
        // stacks exposure (fail closed; the DE40 double fill 2026-07-13).
        // What the sweep found, normalized for the prompt: the AI decides fresh
        // each evaluation, but it should KNOW its previous pullback limit rested
        // without filling (re-issue vs chase vs drop is its call to make).
        let sweptPendingEntry: { side: 'BUY' | 'SELL' | null; price: number | null; age_min: number | null } | null =
            null;
        const sweepPendingEntries = async () => {
            try {
                const result =
                    platform === 'capital'
                        ? await cancelCapitalPendingEntryOrders(symbol)
                        : await cancelPendingEntryOrders(symbol, productType!);
                const first: any = result.orders?.[0];
                if (first) {
                    const sideRaw = String(first.side ?? first.direction ?? '').toUpperCase();
                    const price = Number(first.price ?? first.level);
                    const createdAtMs = Number(first.createdAtMs);
                    sweptPendingEntry = {
                        side: sideRaw === 'BUY' || sideRaw === 'SELL' ? (sideRaw as 'BUY' | 'SELL') : null,
                        price: Number.isFinite(price) && price > 0 ? price : null,
                        age_min:
                            Number.isFinite(createdAtMs) && createdAtMs > 0
                                ? Math.max(0, Math.round((Date.now() - createdAtMs) / 60_000))
                                : null,
                    };
                }
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
        if (!positionOpen && !quarterTick) {
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
            // Stale-thread reconcile: the row said a pullback limit was resting,
            // but the sweep found NOTHING on the venue (expired, weekend purge,
            // manual cancel). The sweep's own row-deletion only fires when a
            // cancel succeeded (found > 0) and the post-AI cleanup only on ticks
            // that reach the AI — without this, a vanished order leaves the
            // dashboard's pendingEntry flag latched through every gate-skipped
            // tick (EURUSD sat stale for 25h, 2026-07-15).
            if (!dryRun && sweep && sweep.found === 0 && aiThreadWasPendingEntry) {
                await endSwingAiThread(platform, symbol).catch((err) =>
                    console.warn(`stale pending-entry thread cleanup failed for ${symbol}:`, err),
                );
            }
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
            instrumentId =
                typeof (bundleLight as any)?.epic === 'string' && (bundleLight as any).epic
                    ? (bundleLight as any).epic
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
                        return (b as any)?.candles ?? [];
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
        const inPositionOffCadenceTick = primaryCloseCadence ? offBoundaryTick : quarterTick;
        const inPositionMoveThresholdAtr = primaryCloseCadence
            ? IN_POSITION_EMERGENCY_MOVE_ATR
            : IN_POSITION_QUARTER_MOVE_ATR;
        if (positionOpen && inPositionOffCadenceTick) {
            const tickerLight = Array.isArray(bundleLight?.ticker) ? bundleLight.ticker[0] : bundleLight?.ticker;
            const priceNow = Number(
                tickerLight?.lastPr ?? tickerLight?.last ?? tickerLight?.close ?? tickerLight?.price,
            );
            const atrNow = Number((indicators as any)?.metrics?.[timeFrame]?.atr);
            let refPrice: number | null = null;
            try {
                const recent = await loadDecisionHistory(symbol, 5, platform);
                // loadDecisionHistory is newest-first, so find() already returns
                // the LATEST AI call (a .reverse() here compared against the
                // oldest call in the window — a stale reference price).
                const lastAiCall = recent.find((h) => {
                    const d = h.aiDecision as any;
                    return d && d.decision_source !== 'pre_ai_skip' && !d.promptSkipped;
                });
                const p = Number((lastAiCall?.snapshot as any)?.price);
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
        // (a) this tick just swept a resting pullback entry — the model owes
        // itself a re-issue/switch/drop decision — or (b) a flat wake band is
        // crossed. The band is PEEKED here without consuming the cooldown row:
        // the full cooldown handler below re-reads it, surfaces the crossing to
        // the prompt as market.cooldown_wake and clears the row. Peek failure
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
        // (base executability, event blackout) still apply. Expiry without a
        // crossing consumes the row and proceeds as a normal scan, no bypass.
        // Best-effort: a store hiccup fails open (evaluate rather than trust a
        // stale quiet period).
        let cooldownWake: { crossed: 'above' | 'below'; level: number; setAtMs: number | null } | null = null;
        if (!positionOpen && !dryRun && !aiThreadResponseId) {
            try {
                const cooldown = await getSwingAiCooldown(platform, symbol);
                if (cooldown) {
                    const wokenAbove = cooldown.wakeAbove !== null && effectivePrice >= cooldown.wakeAbove;
                    const wokenBelow = cooldown.wakeBelow !== null && effectivePrice <= cooldown.wakeBelow;
                    const expired = Date.now() >= cooldown.untilMs;
                    if (!expired && !wokenAbove && !wokenBelow) {
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
                    if (wokenAbove || wokenBelow) {
                        cooldownWake = {
                            crossed: wokenAbove ? 'above' : 'below',
                            level: (wokenAbove ? cooldown.wakeAbove : cooldown.wakeBelow) as number,
                            setAtMs: cooldown.setAtMs > 0 ? cooldown.setAtMs : null,
                        };
                        emitGateDebug('flat_cooldown_woken', {
                            gate: 'AI_COOLDOWN',
                            crossed: cooldownWake.crossed,
                            level: cooldownWake.level,
                            price: effectivePrice,
                            expired,
                        });
                    }
                    await clearSwingAiCooldown(platform, symbol);
                }
            } catch (err) {
                console.warn(`AI cooldown check failed for ${symbol}:`, err);
            }
        }

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

        if (!positionOpen && calmMarket && !cooldownWake) {
            const decision = {
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

        // 5) CLOSE conditions (PnL bands + regime)
        let pnlPct = 0;
        let close_conditions:
            | {
                  pnl_gt_pos?: boolean;
                  pnl_lt_neg?: boolean;
                  opposite_regime?: boolean;
                  // time_stop?: boolean; // add if you track bars-in-trade
              }
            | undefined;

        if (positionOpen) {
            pnlPct = parsePnlPct(positionInfo.currentPnl);
            const side = positionInfo.holdSide as 'long' | 'short';

            const regimeUp = indicators.macro.includes('trend=up');
            const regimeDown = indicators.macro.includes('trend=down');
            const opposite_regime = (side === 'long' && regimeDown) || (side === 'short' && regimeUp);

            close_conditions = {
                pnl_gt_pos: pnlPct >= 1.0, // take profit ≥ +1%
                pnl_lt_neg: pnlPct <= -1.0, // stop loss ≤ -1%
                opposite_regime, // macro regime flipped vs side
                // time_stop: false,
            };
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
        // pullback limit rests (aiThreadResponseId set on a flat tick ⇔
        // pending-entry conversation) — a resting opposite-direction entry
        // must keep being re-validated against the moving market.
        // Also does NOT apply during sweep windows (opening_drive / thin_reopen):
        // the cooldown's sweep-reclaim exception (postprocessDecision) can only
        // fire on ticks that actually run, and reclaims resolve in minutes.
        const sweepWindow =
            venueEvents?.liquidity_phase === 'opening_drive' || venueEvents?.liquidity_phase === 'thin_reopen';
        if (!positionOpen && quarterTick && !aiThreadResponseId && !sweepWindow && !cooldownWake) {
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
            .filter((h) => (h.aiDecision as any)?.decision_source !== 'pre_ai_skip' && !(h.aiDecision as any)?.promptSkipped)
            .map((h) => {
                const d = h.aiDecision as any;
                // Preserve the close size so a partial trim (e.g. 30%) is distinguishable
                // from a full exit in the model's recent-actions feedback. Kept as a
                // separate field so the raw `action` string stays clean for the
                // anti-flip guard in postprocessDecision (which matches on `action`).
                const rawPct = d?.exit_size_pct ?? d?.close_size_pct ?? d?.partial_close_pct;
                const pctNum = Number(rawPct);
                const closePct = Number.isFinite(pctNum) ? Math.max(0, Math.min(100, pctNum)) : null;
                const limitNum = Number(d?.entry_limit_price);
                const entryLimitPrice = Number.isFinite(limitNum) && limitNum > 0 ? limitNum : null;
                return { action: d?.action, timestamp: h.timestamp, closePct, entryLimitPrice };
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
                forexSessionContext = buildForexSessionLevelsContext({
                    symbol,
                    candles: Array.isArray((sessionBundle as any)?.candles) ? (sessionBundle as any).candles : [],
                    sourceTimeframe: microTimeFrame,
                });
            } catch (err) {
                console.warn(`Could not build forex session levels for ${symbol}:`, err);
            }
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
            const decision = {
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

        // 6) Build prompt with allowed_actions, gates, and close_conditions
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
        if (!positionOpen && !actionability.actionable && !cooldownWake) {
            const decision = {
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
                microEntryOk: context.micro_entry_ok,
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
        if (!positionOpen && context.signal_strength === 'LOW' && !cooldownWake) {
            const decision = {
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
        if (!positionOpen && (microOverextended || primaryOverextended) && !cooldownWake) {
            const extDetail = [
                microOverextended ? `micro_${microExtAtr.toFixed(2)}atr` : null,
                primaryOverextended ? `primary_${primaryExtAtr.toFixed(2)}atr` : null,
            ]
                .filter(Boolean)
                .join('_');
            const decision = {
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
        if (!positionOpen && quarterTick && !cooldownWake) {
            // recentHistory is newest-first, so find() already returns the most
            // recent flat AI call (the old .reverse() picked the OLDEST in the
            // window, deduping against a stale price reference).
            const lastFlatAiCall = recentHistory.find((h) => {
                const d = h.aiDecision as any;
                if (!d || d.decision_source === 'pre_ai_skip' || d.promptSkipped) return false;
                return !(h.snapshot as any)?.positionContext;
            });
            const lastSnap = (lastFlatAiCall?.snapshot ?? null) as any;
            const lastAction = String((lastFlatAiCall?.aiDecision as any)?.action || '').toUpperCase();
            const ageMin = lastFlatAiCall ? (Date.now() - Number(lastFlatAiCall.timestamp)) / 60_000 : Infinity;
            const lastPrice = Number(lastSnap?.price);
            const dedupeAtr = Number((indicators as any)?.metrics?.[timeFrame]?.atr);
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

        // Supersede sweep for quarter ticks: this tick reaches a fresh AI
        // evaluation, so the previous evaluation's resting pullback order (if
        // any) is stale — cancel it before the new decision executes. Hourly
        // ticks already swept above; the second call is a cheap no-op then.
        if (!positionOpen && quarterTick) {
            const sweep = await sweepPendingEntries();
            if (await pendingEntryFilledMidTick(sweep)) {
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'pending_entry_filled',
                    reason: 'pending_entry_filled_during_supersede_sweep',
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
                        reason: 'pending_entry_filled_during_supersede_sweep',
                    },
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'pending_entry_filled' },
                    usedTape,
                    promptSkipped: true,
                });
            }
            const sweepFailure = classifyPendingEntrySweep(sweep);
            if (sweepFailure) {
                await recordTickOutcome({
                    kind: 'skip',
                    stage: 'pending_entry_sweep_failed',
                    reason: sweepFailure,
                    gates: gatesOut.gates,
                    metrics: gatesOut.metrics,
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
                    usedTape,
                    promptSkipped: true,
                });
            }
        }

        // Past the gates → the AI will be called. Fetch its remaining inputs
        // together: news (its only consumer is the prompt), the nano (15m)
        // candles for wave/entry-timing geometry, and — for non-BTC crypto —
        // the BTC regime context (measured correlation/beta + BTC state). All
        // deferred to here so gated ticks never pay for them; each fails open
        // (prompt just omits the block).
        const [newsBundleRes, nanoRes, btcContext, promptLessons] = await Promise.all([
            fetchNewsWithHeadlines(symbol, { platform, source: newsSource, category }),
            (async () => {
                try {
                    const nanoBundle = await fetchMarketBundle(symbol, '15m', {
                        includeTrades: false,
                        candleLimit: 110,
                    });
                    const nanoCandles = Array.isArray((nanoBundle as any)?.candles)
                        ? ((nanoBundle as any).candles as unknown[])
                        : [];
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
        const { system, user } = swingState.assemble(
            newsBundle?.sentiment ?? null,
            newsBundle?.headlines ?? [],
            nanoContext,
            sweptPendingEntry,
            eventReaction,
            btcContext,
            promptLessons,
        );

        // 7) Query AI via the provider switch (SWING_AI_PROVIDER; post-parse
        // enforces allowed_actions + close_conditions). Capital decides leverage
        // by asset class, so it uses the leverage-free schema. Ticks with a live
        // conversation chain onto it: in-position ticks manage the trade with
        // memory of the entry thesis and every prior management tick, and flat
        // ticks re-evaluating a resting pullback limit remember why they placed
        // it ("market moved — is this entry still valid?"). Fresh flat scans
        // carry no thread and stay stateless.
        // Conversation context is provider-scoped: an OpenAI resp_... head means
        // nothing to Claude and vice-versa. A thread row written by the OTHER
        // provider (mid-position cutover/rollback) degrades this tick to
        // stateless — the prompt's "position adopted mid-life" branch covers it —
        // and this tick's persist below re-anchors the thread on the active
        // provider.
        const chainedPreviousResponseId = aiThreadProvider === 'claude' ? null : aiThreadResponseId;
        const chainedTranscript = aiThreadProvider === 'claude' ? aiThreadTranscript : null;
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
            thread: { previousResponseId: chainedPreviousResponseId, transcript: chainedTranscript },
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
            (decision as any).raise_leverage_to = null;
            (decision as any).move_stop_to_be = false;
        }
        // Nano (15m) bias measured at decision time — persisted on the decision
        // so the dashboard can render a Nano chip next to the other TF biases.
        (decision as any).nano_bias = nanoContext?.bias ?? null;
        // Provider message id of this call (OpenAI resp_..., Claude msg_...) —
        // persisted in ai_decision_json so every decision row maps to its turn
        // in the conversation. The previous id (null on stateless calls) lets
        // the dashboard link chained decisions on the timeline; on the Claude
        // path chaining runs through the stored transcript, so the prior msg id
        // fills the same linkage slot.
        (decision as any).response_id = aiResponseId;
        (decision as any).previous_response_id =
            aiThreadProvider === 'claude' ? aiThreadResponseId : chainedPreviousResponseId;
        // Which provider/model served this call and what it cost (cache activity
        // included) — rides in ai_decision_json next to response_id, so every
        // decision row is self-describing for post-mortems and token audits.
        (decision as any).ai_provider = aiCallProvider;
        (decision as any).ai_model = aiCallModel;
        (decision as any).ai_usage = aiCallUsage;

        // Pullback entry limit first (its price anchors everything downstream):
        // validate the model's limit against live price + ATR — too far clamps;
        // wrong side / inside the noise band / unverifiable DROPS the entry for
        // this tick (no silent market fallback — the model asked for a patience
        // price, and null is its way to request market).
        const tpslAtrRaw = Number((indicators as any)?.metrics?.[timeFrame]?.atr);
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
        const entryLimit = sanitizeEntryLimit({
            action: decision.action,
            positionOpen,
            price: marketAnchor,
            primaryAtr: primaryAtrSane,
            entryLimitPrice: (decision as any).entry_limit_price ?? null,
        });
        (decision as any).entry_limit_price = entryLimit.entryLimitPrice;
        if (entryLimit.dropEntry && (decision.action === 'BUY' || decision.action === 'SELL')) {
            (decision as any).action = 'HOLD';
            (decision as any).reason = `${String((decision as any).reason ?? '')} [entry dropped: ${entryLimit.notes.join(',')}]`.trim();
        }
        // Bracket anchor: for a resting pullback entry the protective stop and
        // TP must be sized from the LIMIT price (where the position would
        // actually open), not from the current price.
        const bracketAnchor = entryLimit.entryLimitPrice ?? marketAnchor;

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
            takeProfitPrice: (decision as any).take_profit_price ?? null,
            stopLossPrice: (decision as any).stop_loss_price ?? null,
            exitSizePct: (decision as any).exit_size_pct ?? null,
            standingStopLossPrice: currentStopLoss,
        });
        (decision as any).take_profit_price = exchangeTpsl.takeProfitPrice;
        (decision as any).stop_loss_price = exchangeTpsl.stopLossPrice;

        // Flat-HOLD cooldown request: clamp minutes and validate wake-band sides
        // against live price; write the SANITIZED values back onto the decision
        // so history/dashboard show what was actually armed. Persisted after
        // execution (below) — only a flat HOLD ever carries non-null values.
        const holdCooldown = sanitizeHoldCooldown({
            action: decision.action,
            positionOpen,
            price: marketAnchor,
            cooldownMinutes: (decision as any).cooldown_minutes,
            wakeAbove: (decision as any).cooldown_wake_above,
            wakeBelow: (decision as any).cooldown_wake_below,
        });
        (decision as any).cooldown_minutes = holdCooldown.cooldownMinutes;
        (decision as any).cooldown_wake_above = holdCooldown.wakeAbove;
        (decision as any).cooldown_wake_below = holdCooldown.wakeBelow;
        if (holdCooldown.notes.length) {
            (decision as any).cooldown_notes = holdCooldown.notes;
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
                const primaryAtr = Number((indicators as any)?.metrics?.[timeFrame]?.atr);
                // Anchored at the pullback limit when one is resting — the stop
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
                    minNotionalUsd = Number.isFinite(afford?.minNotionalUsd as number)
                        ? Number(afford!.minNotionalUsd)
                        : null;
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
                if (minNotionalUsd !== null && riskSizing.notionalUsd < minNotionalUsd) {
                    (decision as any).action = 'HOLD';
                    (decision as any).reason =
                        `${String((decision as any).reason ?? '')} [entry dropped: risk_budget_below_min_size ` +
                        `notional≈${riskSizing.notionalUsd.toFixed(0)} min≈${minNotionalUsd.toFixed(0)}]`.trim();
                } else {
                    execSideSizeUSDT = riskSizing.marginUsd;
                }
                // Persisted with the decision row so post-mortems can audit the
                // realized risk against the budget.
                (decision as any).risk_sizing = {
                    risk_usd: Number(riskSizing.riskUsd.toFixed(2)),
                    notional_usd: Number(riskSizing.notionalUsd.toFixed(2)),
                    margin_usd: Number(riskSizing.marginUsd.toFixed(2)),
                    stop_distance_pct: Number((riskSizing.stopDistancePct * 100).toFixed(3)),
                    equity_usd: equityUsd,
                    source: riskSizing.source,
                };
            }
        }

        const execRes =
            platform === 'capital'
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
                      productType!,
                      dryRun,
                      stopLossPrice,
                      exchangeTpsl.takeProfitPrice,
                  );
        const executedAtMs = Date.now();

        // Thread lifecycle bookkeeping. An entry that actually placed an order
        // STARTS or CONTINUES a conversation (pending_entry while a pullback
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
                    Number((decision as any).exit_size_pct ?? 100) >= 100;
                // Claude path: the conversation is OURS to store — append this
                // tick's turns (sent user turn + assistant response verbatim) to
                // the transcript the tick chained onto, capped so a long-lived
                // position can't grow the row unboundedly. OpenAI path:
                // appendTurns is null and the transcript stays empty (the chain
                // lives server-side behind lastResponseId).
                const activeProvider = resolveSwingAiProvider();
                const nextTranscript =
                    Array.isArray(aiAppendTurns) && aiAppendTurns.length
                        ? truncateClaudeTranscript([
                              ...(Array.isArray(chainedTranscript) ? (chainedTranscript as any[]) : []),
                              ...(aiAppendTurns as any[]),
                          ] as any)
                        : null;
                if (fullCloseExecuted) {
                    await endSwingAiThread(platform, symbol);
                } else if (entryPlacedNow) {
                    await upsertSwingAiThread({
                        platform,
                        symbol,
                        status: (decision as any).entry_limit_price != null ? 'pending_entry' : 'in_position',
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
                });
            } catch (err) {
                console.warn(`AI cooldown arm failed for ${symbol}:`, err);
            }
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
            // Wake-band trigger (null unless this call exists because price
            // crossed the previous flat HOLD's cooldown wake band — those calls
            // bypass the flat quality gates). Persisting it makes "what does the
            // AI do when its own wake level fires, and does it pay" a SQL query.
            cooldownWake,
            // Sanitized exchange-side bracket that actually went to execution,
            // plus any clamp/drop notes (e.g. tp_wrong_side_dropped) — makes
            // "what did the model ask for vs what shipped" a SQL query.
            exchangeTpsl: {
                takeProfitPrice: exchangeTpsl.takeProfitPrice,
                stopLossPrice: exchangeTpsl.stopLossPrice,
                standing: { takeProfitPrice: currentTakeProfit, stopLossPrice: currentStopLoss },
                notes: exchangeTpsl.notes,
            },
            entryLimit: {
                price: entryLimit.entryLimitPrice,
                notes: entryLimit.notes,
            },
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
                ...(nanoContext ? { nano: '15m' } : {}),
            },
        });
        // Tick-log row for the AI call keeps swing.tick_log a COMPLETE per-tick
        // series (skips + calls) — the decision row above holds the full detail.
        // No KV marker: the decision row already surfaces this tick on the UI.
        await recordTickOutcome({
            kind: 'ai_call',
            stage: 'decision',
            reason: String(decision.action || 'HOLD'),
            gates: gatesOut.gates,
            metrics: gatesOut.metrics,
            kvMarker: false,
        });
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
            const tpslExec = (execRes as any)?.tpsl;
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
    } catch (err: any) {
        console.error('Error in /api/analyze:', err);
        return res.status(500).json({ error: err.message || String(err) });
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
