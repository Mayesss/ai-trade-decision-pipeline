// api/analyze.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { requireAdminAccess } from '../../lib/admin';

import {
    fetchMarketBundle as fetchBitgetMarketBundle,
    computeAnalytics,
    fetchPositionInfo as fetchBitgetPositionInfo,
    fetchRealizedRoi as fetchBitgetRealizedRoi,
} from '../../lib/analytics';
import { calculateMultiTFIndicators as calculateBitgetMultiTFIndicators } from '../../lib/indicators';
import { fetchNewsWithHeadlines } from '../../lib/news';
import {
    calculateCapitalMultiTFIndicators,
    executeCapitalDecision,
    fetchCapitalMarketBundle,
    fetchCapitalPositionInfo,
    fetchCapitalRealizedRoi,
    resolveCapitalEpic,
    resolveCapitalEpicRuntime,
} from '../../lib/capital';
import { resolveAnalysisPlatform, resolveInstrumentId, resolveNewsSource, type AnalysisPlatform } from '../../lib/platform';

import { buildPrompt, callAI, computeMomentumSignals, postprocessDecision, resolveDecisionPolicy } from '../../lib/ai';
import type { DecisionPolicy, MomentumSignals } from '../../lib/ai';
import { getGates } from '../../lib/gates';

import { executeDecision, getTargetLeverage, getTradeProductType } from '../../lib/trading';
import { composePositionContext } from '../../lib/positionContext';
import { updatePositionExtrema } from '../../lib/positionExtrema';
import { appendDecisionHistory, loadDecisionHistory } from '../../lib/history';
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

function shouldSkipMomentumCall(params: { signals: MomentumSignals; price: number; trades: any[] }) {
    const { signals, price, trades } = params;
    const extensionActive = Math.abs(signals.microExtensionInAtr ?? 0) > 0.5;
    const primaryAtr = Number(signals.primaryAtr ?? 0);
    const atrPct = price > 0 && primaryAtr > 0 ? primaryAtr / price : 0;
    const atrActive = atrPct > ATR_ACTIVE_MIN_PCT;
    const latestTradeTs = readLatestTradeTimestamp(trades);
    const minutesSinceLastTrade = latestTradeTs ? (Date.now() - latestTradeTs) / 60000 : Infinity;
    const tapeInactive =
        !Array.isArray(trades) || trades.length === 0 || !Number.isFinite(minutesSinceLastTrade) || minutesSinceLastTrade > STALE_TRADE_MINUTES;
    return tapeInactive || !(extensionActive || atrActive);
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
        const symbolParam = Array.isArray(body.symbol) ? body.symbol[0] : body.symbol;
        const symbol = String(symbolParam || 'ETHUSDT').toUpperCase();
        const platformParam = Array.isArray(body.platform) ? body.platform[0] : body.platform;
        const platform: AnalysisPlatform = resolveAnalysisPlatform(platformParam as string | undefined);
        const newsSourceParam = Array.isArray(body.newsSource) ? body.newsSource[0] : body.newsSource;
        const newsSource = resolveNewsSource(platform, newsSourceParam as string | undefined);
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
        const sideSizeUSDT = Number(body.notional ?? DEFAULT_NOTIONAL_USDT);

        const fetchMarketBundle = platform === 'capital' ? fetchCapitalMarketBundle : fetchBitgetMarketBundle;
        const calculateMultiTFIndicators =
            platform === 'capital' ? calculateCapitalMultiTFIndicators : calculateBitgetMultiTFIndicators;
        const fetchPositionInfo = platform === 'capital' ? fetchCapitalPositionInfo : fetchBitgetPositionInfo;
        const fetchRealizedRoi = platform === 'capital' ? fetchCapitalRealizedRoi : fetchBitgetRealizedRoi;
        let instrumentId =
            platform === 'capital' ? resolveCapitalEpic(symbol).epic : resolveInstrumentId(symbol, platform);

        const positionInfo = await fetchPositionInfo(symbol);
        const positionOpen = positionInfo.status === 'open';
        const primaryCloseTime = isPrimaryCloseTime(timeFrame);
        if (!positionOpen && !primaryCloseTime) {
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision: {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'not_primary_close',
                    reason: 'flat_skip_until_primary_close',
                },
                execRes: { placed: false, orderId: null, clientOid: null, reason: 'not_primary_close' },
                usedTape: false,
                promptSkipped: true,
            });
        }

        // 1) Product & parallel baseline fetches (fast)
        const productType = platform === 'bitget' ? getTradeProductType() : null;

        const [newsBundle, bundleLight, indicators] = await Promise.all([
            fetchNewsWithHeadlines(symbol, { platform, source: newsSource }),
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

        // 3) Gates on light data (orderbook/ATR based)
        const gatesOut = getGates({
            symbol,
            bundle: bundleLight,
            analytics: analyticsLight,
            indicators,
            notionalUSDT: sideSizeUSDT,
            positionOpen,
            disableSymbolExclusions: platform === 'capital',
        });

        // 3b) Short-circuit if no trade allowed and no open position
        if (gatesOut.preDecision && !positionOpen) {
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision: gatesOut.preDecision,
                execRes: { placed: false, orderId: null, clientOid: null, reason: 'gates_short_circuit' },
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                usedTape: false,
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
            });

        if (!positionOpen && calmMarket) {
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision: {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'calm_market',
                    reason: 'conditions_below_momentum_thresholds_or_no_recent_trades',
                },
                execRes: { placed: false, orderId: null, clientOid: null, reason: 'calm_market' },
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                usedTape,
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

        const positionContext = composePositionContext({
            position: positionInfo,
            pnlPct,
            maxDrawdownPct: positionExtrema.maxDrawdownPct,
            maxProfitPct: positionExtrema.maxProfitPct,
            enteredAt: pstate.enteredAt,
        });
        const recentHistory = await loadDecisionHistory(symbol, 5, platform);
        const recentActions = recentHistory
            .map((h) => ({ action: h.aiDecision?.action, timestamp: h.timestamp }))
            .filter((a) => a.action);

        // 6) Build prompt with allowed_actions, gates, and close_conditions
        const roiRes = await fetchRealizedRoi(symbol, 24);

        const { system, user, context } = await buildPrompt(
            symbol, // e.g. "BTCUSDT"
            timeFrame, // e.g. "45m"
            bundle, // from fetchMarketBundle(...)
            analytics, // from computeAnalytics(bundle)
            positionForPrompt, // "none" | JSON string like 'open long @ ...' (your current format)
            newsBundle?.sentiment ?? null, // omit from prompt if unavailable
            newsBundle?.headlines ?? [],
            indicators, // from calculateMultiTFIndicators(symbol)
            gatesOut.gates, // from getGates(...)
            positionContext,
            momentumSignals,
            recentActions,
            roiRes.lastNetPct,
            dryRun,
            Number(gatesOut.metrics?.spreadBpsNow),
            decisionPolicy,
        );

        // 7) Query AI (post-parse enforces allowed_actions + close_conditions)
        const decisionRaw = await callAI(system, user);
        const decision = postprocessDecision({
            decision: decisionRaw,
            context,
            gates: gatesOut.gates,
            positionOpen,
            recentActions,
            positionContext,
            policy: decisionPolicy,
        });

        // 8) Execute (dry run unless explicitly disabled), using leveraged notional for gates
        const execLeverage = getTargetLeverage(decision);
        const execNotionalUSDT = sideSizeUSDT * (execLeverage ?? 1);
        const gatesForExec =
            execNotionalUSDT !== sideSizeUSDT
                ? getGates({
                      symbol,
                      bundle,
                      analytics,
                      indicators,
                      notionalUSDT: execNotionalUSDT,
                      positionOpen,
                      disableSymbolExclusions: platform === 'capital',
                  })
                : gatesOut;

        if (
            !positionOpen &&
            (decision.action === 'BUY' || decision.action === 'SELL') &&
            gatesForExec.preDecision
        ) {
            return res.status(200).json({
                symbol,
                platform,
                newsSource,
                instrumentId,
                timeFrame,
                dryRun,
                decisionPolicy,
                decision,
                execRes: { placed: false, orderId: null, clientOid: null, reason: 'gates_short_circuit' },
                gates: { ...gatesForExec.gates, metrics: gatesForExec.metrics },
                usedTape,
            });
        }

        const execRes =
            platform === 'capital'
                ? await executeCapitalDecision(symbol, sideSizeUSDT, decision, dryRun)
                : await executeDecision(symbol, sideSizeUSDT, decision, productType!, dryRun);

        const change24h = Number(tickerData?.change24h ?? tickerData?.changeUtc24h ?? tickerData?.chgPct);
        const spreadBpsSnapshot = safeNum(gatesForExec.metrics?.spreadBpsNow, safeNum(analytics.spreadBps, 0));
        const spreadAbsSnapshot = safeNum(analytics.spreadAbs ?? analytics.spread, 0);
        const bestBid = Number(analytics.bestBid);
        const bestAsk = Number(analytics.bestAsk);
        const snapshot = {
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
            positionContext,
            momentumSignals,
        };

        await appendDecisionHistory({
            timestamp: Date.now(),
            symbol,
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
            },
        });

        // 9) Respond
        return res.status(200).json({
            symbol,
            platform,
            newsSource,
            instrumentId,
            timeFrame,
            dryRun,
            decisionPolicy,
            decision,
            execRes,
            gates: { ...gatesForExec.gates, metrics: gatesForExec.metrics },
            usedTape,
        });
    } catch (err: any) {
        console.error('Error in /api/analyze:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
