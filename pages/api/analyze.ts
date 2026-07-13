// api/analyze.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { requireAdminAccess } from '../../lib/admin';

import {
    fetchMarketBundle as fetchBitgetMarketBundle,
    computeAnalytics,
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
    getCapitalCategoryLeverage,
    fetchCapitalMarketBundle,
    fetchCapitalMarketTradeability,
    fetchCapitalPositionInfo,
    fetchCapitalRealizedRoi,
    cancelCapitalPendingEntryOrders,
    resolveCapitalEpic,
    resolveCapitalEpicRuntime,
} from '../../lib/capital';
import { resolveAnalysisPlatform, resolveInstrumentId, resolveNewsSource, type AnalysisPlatform } from '../../lib/platform';
import { resolveSwingCategory } from '../../lib/swing/category';
import { loadSwingCronControlState } from '../../lib/swing/cronControl';
import { recordSwingLastScan } from '../../lib/swing/lastScan';
import { computeNanoContext } from '../../lib/swing/waveGeometry';
import { loadForexEventContext } from '../../lib/swing/forexEvents';
import { buildForexSessionLevelsContext } from '../../lib/swing/sessionLevels';

import {
    computeSwingState,
    callAIThread,
    computeMomentumSignals,
    postprocessDecision,
    resolveDecisionPolicy,
    resolveExtensionThresholds,
    sanitizeEntryLimit,
    sanitizeExchangeTpSl,
    REENTRY_COOLDOWN_MIN,
    SWING_DECISION_SCHEMA,
    SWING_DECISION_SCHEMA_NO_LEVERAGE,
} from '../../lib/ai';
import type { DecisionPolicy, LastClosedPosition, MomentumSignals } from '../../lib/ai';
import { getGates } from '../../lib/gates';

import {
    cancelPendingEntryOrders,
    executeDecision,
    fetchPositionTpsl,
    getTargetLeverage,
    getTradeProductType,
} from '../../lib/trading';
import { composePositionContext } from '../../lib/positionContext';
import { updatePositionExtrema } from '../../lib/positionExtrema';
import { appendDecisionHistory, loadDecisionHistory } from '../../lib/history';
import { recordSwingAccountSnapshot } from '../../lib/swing/sync';
import {
    endSwingAiThread,
    getSwingAiThread,
    loadClosedSwingPositions,
    markSwingAiThreadInPosition,
    upsertSwingAiThread,
    upsertSwingPosition,
} from '../../lib/swing/pg';
import { invalidateSwingSummaryCache } from '../../lib/swing/summaryCache';
import { warmChartCandlesFromAnalyze } from '../../lib/swing/chartCache';
import { warmPositionOverlayCacheFromAnalyze } from '../../lib/swing/positionOverlayCache';
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

    try {
        await upsertSwingPosition('capital', {
            id: positionKey,
            symbol: params.symbol.toUpperCase(),
            side: params.positionInfo.holdSide ?? null,
            status: 'closed',
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
            leverageSource: 'captured',
        });
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
// already fences the position 24/7, so the AI is only asked mid-hour when price
// has moved enough since its last look to plausibly change the answer — quiet
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
        // Default OFF: flat ticks now evaluate hourly and are cost-gated by the
        // signal-strength check (below), not by 4H candle close. A caller can still
        // opt back into the old 4H-close cadence by passing enforcePrimaryCloseGate=true.
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
        // Quarter ticks (:15/:30/:45, automation crons only) exist to scan FLAT
        // symbols for new entry windows; manual/API calls are never quarter
        // ticks and always run the full path.
        const quarterTick = automationCron && isQuarterHourTick();
        // Freshness marker for the dashboard: quarter-tick scans don't persist
        // decision rows, so this is the only evidence the 15m cadence ran.
        // Fire-and-forget — never blocks the trading path.
        if (automationCron) void recordSwingLastScan(platform, symbol);
        const persistPreAiSkip = async (params: {
            stage: string;
            decision: Record<string, any>;
            execResult: Record<string, any>;
            gates?: Record<string, any>;
            metrics?: Record<string, any>;
            usedTape?: boolean;
            snapshot?: Record<string, any>;
        }) => {
            // Quarter ticks don't persist skips: gate short-circuits already get
            // recorded on the hourly tick, and 3 more identical rows/hour/symbol
            // would only be noise. The skip stage/reason still lands on the KV
            // last-scan marker so the UI can show WHY the quarter tick stopped.
            // Real AI calls (past all gates) always persist.
            if (quarterTick) {
                void recordSwingLastScan(platform, symbol, {
                    stage: params.stage,
                    reason: typeof params.decision?.reason === 'string' ? params.decision.reason : params.stage,
                });
                return;
            }
            const reason = typeof params.decision?.reason === 'string' ? params.decision.reason : params.stage;
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
        // this gate only applies to the Capital platform.
        if (platform === 'capital') {
            const tradeability = await fetchCapitalMarketTradeability(symbol);
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
        const primaryCloseGateBlocked = !positionOpen && !primaryCloseTime;
        if (primaryCloseGateBlocked && enforcePrimaryCloseGate) {
            emitGateDebug('primary_close_gate_blocked', {
                gate: 'PRIMARY_CLOSE_TIME',
                primaryCloseTime,
                positionOpen,
                enforcePrimaryCloseGate,
                timeFrame,
            });
            const decision = {
                action: 'HOLD',
                bias: 'NEUTRAL',
                signal_strength: 'LOW',
                summary: 'not_primary_close',
                reason: 'flat_skip_until_primary_close',
            };
            const execRes = { placed: false, orderId: null, clientOid: null, reason: 'not_primary_close' };
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
                              blockedBy: 'PRIMARY_CLOSE_TIME',
                              reason: 'flat_skip_until_primary_close',
                              primaryCloseTime,
                              enforcePrimaryCloseGate,
                              positionOpen,
                              timeFrame,
                          },
                      }
                    : {}),
            });
        }
        if (primaryCloseGateBlocked && !enforcePrimaryCloseGate) {
            emitGateDebug('primary_close_gate_bypassed', {
                gate: 'PRIMARY_CLOSE_TIME',
                primaryCloseTime,
                positionOpen,
                enforcePrimaryCloseGate,
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

        // Responses-API thread (per-position conversation chain). A thread starts
        // when an entry order is placed, survives a pullback-limit fill into
        // position management, and ends when the limit expires unfilled or the
        // position closes. Reconciled here against broker reality:
        //   in_position + no open position → closed since last tick (TP/SL fill,
        //     manual close, executed CLOSE) → conversation over;
        //   pending_entry + open position → the limit filled → same conversation
        //     now manages the position.
        // Best-effort: a thread hiccup degrades the tick to stateless, never fails it.
        let aiThreadResponseId: string | null = null;
        if (!dryRun) {
            try {
                const aiThread = await getSwingAiThread(platform, symbol);
                if (aiThread) {
                    if (positionOpen) {
                        if (aiThread.status === 'pending_entry') {
                            await markSwingAiThreadInPosition(platform, symbol);
                        }
                        aiThreadResponseId = aiThread.lastResponseId;
                    } else if (aiThread.status === 'in_position') {
                        await endSwingAiThread(platform, symbol);
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
                // Resting entry cancelled without filling → its conversation ends
                // here (the next entry decision starts a fresh thread). A cancel
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

        // In-position quarter ticks are event-driven: the resting TP/SL bracket
        // fences the position between hourly evaluations, so the AI is only
        // asked mid-hour ("is the setting still fine?") when price has moved at
        // least IN_POSITION_QUARTER_MOVE_ATR primary ATR since its last look.
        // Reference = last real AI call's snapshot price (any tick — the entry
        // decision counts), falling back to entry price. Missing price/ATR fails
        // OPEN (call the AI rather than fly blind); the skip is not persisted.
        if (positionOpen && quarterTick) {
            const tickerLight = Array.isArray(bundleLight?.ticker) ? bundleLight.ticker[0] : bundleLight?.ticker;
            const priceNow = Number(
                tickerLight?.lastPr ?? tickerLight?.last ?? tickerLight?.close ?? tickerLight?.price,
            );
            const atrNow = Number((indicators as any)?.metrics?.[timeFrame]?.atr);
            let refPrice: number | null = null;
            try {
                const recent = await loadDecisionHistory(symbol, 5, platform);
                const lastAiCall = [...recent].reverse().find((h) => {
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
            if (moveAtr != null && moveAtr < IN_POSITION_QUARTER_MOVE_ATR) {
                emitGateDebug('quarter_tick_in_position_quiet', {
                    gate: 'QUARTER_TICK_IN_POSITION_QUIET',
                    moveAtr: Number(moveAtr.toFixed(3)),
                    thresholdAtr: IN_POSITION_QUARTER_MOVE_ATR,
                    refPrice,
                    priceNow,
                });
                const decision = {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'quarter_tick_quiet_position',
                    reason: `in_position_skip_quiet_quarter_tick_move_${moveAtr.toFixed(2)}atr`,
                };
                void recordSwingLastScan(platform, symbol, {
                    stage: 'quiet_position',
                    reason: decision.reason,
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
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'quarter_tick_quiet_position' },
                    usedTape: false,
                    promptSkipped: true,
                });
            }
            emitGateDebug('quarter_tick_in_position_triggered', {
                gate: 'QUARTER_TICK_IN_POSITION_TRIGGERED',
                moveAtr: moveAtr != null ? Number(moveAtr.toFixed(3)) : null,
                thresholdAtr: IN_POSITION_QUARTER_MOVE_ATR,
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

        if (!positionOpen && calmMarket) {
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

        const recentHistory = await loadDecisionHistory(symbol, 5, platform);
        const recentActions = recentHistory
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
                return { action: d?.action, timestamp: h.timestamp, closePct };
            })
            .filter((a) => a.action);
        // Session/day/week levels and the macro-event calendar are both valuable for any
        // session-traded, fiat-macro-sensitive Capital.com instrument (forex, metals,
        // indices). Events resolve to the instrument's macro currency (e.g. USD for gold);
        // crypto is excluded (24/7, no session boundaries, no fiat-macro calendar).
        const SESSION_LEVEL_CATEGORIES = new Set(['forex', 'commodity', 'index']);
        const forexEventContext =
            category && SESSION_LEVEL_CATEGORIES.has(category)
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
            Date.now(),
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
        if (!positionOpen && !actionability.actionable) {
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
                snapshot: { price: effectivePrice, actionability, momentumSignals },
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
        if (!positionOpen && context.signal_strength === 'LOW') {
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
                snapshot: { price: effectivePrice, actionability, momentumSignals },
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
        if (!positionOpen && (microOverextended || primaryOverextended)) {
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
                snapshot: { price: effectivePrice, actionability, momentumSignals },
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
        if (!positionOpen && quarterTick) {
            const lastFlatAiCall = [...recentHistory]
                .reverse()
                .find((h) => {
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
                void recordSwingLastScan(platform, symbol, {
                    stage: 'flat_dedupe',
                    reason: decision.reason,
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
        }

        // Past the gates → the AI will be called. Fetch its two remaining inputs
        // together: news (its only consumer is the prompt) and the nano (15m)
        // candles for wave/entry-timing geometry. Both are deferred to here so
        // gated ticks never pay for them; nano fails open (prompt just omits it).
        const [newsBundleRes, nanoContext] = await Promise.all([
            fetchNewsWithHeadlines(symbol, { platform, source: newsSource, category }),
            (async () => {
                try {
                    const nanoBundle = await fetchMarketBundle(symbol, '15m', {
                        includeTrades: false,
                        candleLimit: 110,
                    });
                    return computeNanoContext((nanoBundle as any)?.candles ?? []);
                } catch (err) {
                    console.warn(`Could not build nano (15m) context for ${symbol}:`, err);
                    return null;
                }
            })(),
        ]);
        newsBundle = newsBundleRes;
        const { system, user } = swingState.assemble(
            newsBundle?.sentiment ?? null,
            newsBundle?.headlines ?? [],
            nanoContext,
            sweptPendingEntry,
        );

        // 7) Query AI (post-parse enforces allowed_actions + close_conditions).
        // Capital decides leverage by asset class, so it uses the leverage-free schema.
        // In-position ticks chain onto the position's stored conversation
        // (previous_response_id) so the model manages the trade with memory of its
        // own entry thesis and every prior management tick; entry scans stay
        // stateless (flat ticks never carry a thread head — sweeps end it first).
        const { json: decisionRaw, responseId: aiResponseId } = await callAIThread(
            system,
            user,
            platform === 'capital' ? SWING_DECISION_SCHEMA_NO_LEVERAGE : SWING_DECISION_SCHEMA,
            { previousResponseId: positionOpen ? aiThreadResponseId : null },
        );
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
        // Responses-API id of this call — persisted in ai_decision_json so every
        // decision row maps to its turn in the stored conversation chain.
        (decision as any).response_id = aiResponseId;

        // Pullback entry limit first (its price anchors everything downstream):
        // validate the model's limit against live price + ATR — too far clamps;
        // wrong side / inside the noise band / unverifiable DROPS the entry for
        // this tick (no silent market fallback — the model asked for a patience
        // price, and null is its way to request market).
        const tpslAtrRaw = Number((indicators as any)?.metrics?.[timeFrame]?.atr);
        const primaryAtrSane = Number.isFinite(tpslAtrRaw) && tpslAtrRaw > 0 ? tpslAtrRaw : null;
        const marketAnchor = Number.isFinite(lastPrice) ? lastPrice : effectivePrice;
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
        // survived sanitation (sanitizeExchangeTpSl: protective side, 0.25–3×ATR
        // from the bracket anchor), otherwise the deliberately WIDE ATR-based
        // catastrophe stop — a circuit breaker bounding the position during the
        // ~1h gap between AI evaluations, not a tactical exit.
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

        const execRes =
            platform === 'capital'
                ? await executeCapitalDecision(
                      symbol,
                      sideSizeUSDT,
                      decision,
                      dryRun,
                      stopLossPrice,
                      true,
                      exchangeTpsl.takeProfitPrice,
                  )
                : await executeDecision(
                      symbol,
                      sideSizeUSDT,
                      decision,
                      productType!,
                      dryRun,
                      stopLossPrice,
                      exchangeTpsl.takeProfitPrice,
                  );
        const executedAtMs = Date.now();

        // Thread lifecycle bookkeeping. An entry that actually placed an order
        // STARTS a conversation (pending_entry while a pullback limit rests;
        // straight to in_position on a market entry — a fresh entry replaces any
        // stale thread row outright). An in-position tick ADVANCES the chain head
        // (HOLD / partial CLOSE / REVERSE — the reversal keeps its conversation).
        // A full CLOSE that executed ends it; TP/SL fills between ticks are caught
        // by the reconcile at the top of the next tick. Best-effort, never blocks
        // the trading path.
        if (!dryRun && aiResponseId) {
            try {
                const entryPlacedNow =
                    !positionOpen && execRes?.placed === true && (decision.action === 'BUY' || decision.action === 'SELL');
                const fullCloseExecuted =
                    positionOpen &&
                    decision.action === 'CLOSE' &&
                    execRes?.placed === true &&
                    Number((decision as any).exit_size_pct ?? 100) >= 100;
                if (fullCloseExecuted) {
                    await endSwingAiThread(platform, symbol);
                } else if (entryPlacedNow) {
                    await upsertSwingAiThread({
                        platform,
                        symbol,
                        status: (decision as any).entry_limit_price != null ? 'pending_entry' : 'in_position',
                        lastResponseId: aiResponseId,
                    });
                } else if (positionOpen) {
                    await upsertSwingAiThread({
                        platform,
                        symbol,
                        status: 'in_position',
                        lastResponseId: aiResponseId,
                    });
                }
            } catch (err) {
                console.warn(`AI thread update failed for ${symbol}:`, err);
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
            forexSessionContext,
            positionContext,
            momentumSignals,
            // Which actionability branch admitted this call (confirmed_primary_structure /
            // bounce_long / bounce_short when flat). Skips already record theirs; persisting
            // it here makes per-branch outcome tracking a SQL query instead of a
            // reverse-engineering job over prompt STATE.
            actionability,
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
                          primaryCloseGateBlocked,
                          positionOpen,
                          gateChecksCompleted: true,
                      },
                  }
                : {}),
        });
    } catch (err: any) {
        console.error('Error in /api/analyze:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
