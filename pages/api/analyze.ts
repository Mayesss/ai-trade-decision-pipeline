// api/analyze.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchMarketBundle, computeAnalytics, fetchPositionInfo, fetchRealizedRoi } from '../../lib/analytics';
import { calculateMultiTFIndicators } from '../../lib/indicators';
import { fetchNewsWithHeadlines } from '../../lib/news';

import { buildPrompt, callAI, computeMomentumSignals } from '../../lib/ai';
import type { MomentumSignals } from '../../lib/ai';
import { getGates } from '../../lib/gates';

import { executeDecision, getTargetLeverage, getTradeProductType } from '../../lib/trading';
import { composePositionContext } from '../../lib/positionContext';
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

function shouldSkipMomentumCall(params: { signals: MomentumSignals; price: number }) {
    const { signals, price } = params;
    const extensionActive = Math.abs(signals.microExtensionInAtr ?? 0) > 0.5;
    const primaryAtr = Number(signals.primaryAtr ?? 0);
    const atrPct = price > 0 && primaryAtr > 0 ? primaryAtr / price : 0;
    const atrActive = atrPct > ATR_ACTIVE_MIN_PCT;
    return !(extensionActive || atrActive);
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

        const body = req.query ?? {};
        const symbolParam = Array.isArray(body.symbol) ? body.symbol[0] : body.symbol;
        const symbol = String(symbolParam || 'ETHUSDT').toUpperCase();
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
        const sideSizeUSDT = Number(body.notional ?? DEFAULT_NOTIONAL_USDT);

        // 1) Product & parallel baseline fetches (fast)
        const productType = getTradeProductType();

        const [positionInfo, newsBundle, bundleLight, indicators] = await Promise.all([
            fetchPositionInfo(symbol),
            fetchNewsWithHeadlines(symbol),
            // Light bundle: skip tape (fills)
            fetchMarketBundle(symbol, timeFrame, { includeTrades: false }),
            calculateMultiTFIndicators(symbol, {
                primary: timeFrame,
                micro: microTimeFrame,
                macro: macroTimeFrame,
                context: contextTimeFrame,
            }),
        ]);

        const positionForPrompt =
            positionInfo.status === 'open'
                ? `${positionInfo.holdSide}, entryPrice: ${positionInfo.entryPrice}, currentPnl=${positionInfo.currentPnl}`
                : 'none';

        // Store/refresh entry timestamp for min-hold (best-effort, in-memory)
        const persistKey = `${symbol}:${timeFrame}`;
        const pstate = touchPersist(persistKey);
        if (positionInfo.status === 'open') {
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
            positionOpen: positionInfo.status === 'open',
        });

        // 3b) Short-circuit if no trade allowed and no open position
        if (gatesOut.preDecision && positionInfo.status !== 'open') {
            return res.status(200).json({
                symbol,
                timeFrame,
                dryRun,
                decision: gatesOut.preDecision,
                execRes: { placed: false, orderId: null, clientOid: null, reason: 'gates_short_circuit' },
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                usedTape: false,
            });
        }

        // 4) Decide whether we need tape; if yes, fetch with tight budgets
        const needTape =
            positionInfo.status === 'open' || gatesOut.allowed_actions.some((a) => a === 'BUY' || a === 'SELL');

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

        const positionOpen = positionInfo.status === 'open';
        const calmMarket = !positionOpen && shouldSkipMomentumCall({ signals: momentumSignals, price: effectivePrice });

        if (!positionOpen && calmMarket) {
            return res.status(200).json({
                symbol,
                timeFrame,
                dryRun,
                decision: {
                    action: 'HOLD',
                    bias: 'NEUTRAL',
                    signal_strength: 'LOW',
                    summary: 'calm_market',
                    reason: 'conditions_below_momentum_thresholds',
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

        if (positionInfo.status === 'open') {
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

        const positionContext = composePositionContext({
            position: positionInfo,
            pnlPct,
            enteredAt: pstate.enteredAt,
        });
        const recentHistory = await loadDecisionHistory(symbol, 5);
        const recentActions = recentHistory
            .map((h) => ({ action: h.aiDecision?.action, timestamp: h.timestamp }))
            .filter((a) => a.action);

        // 6) Build prompt with allowed_actions, gates, and close_conditions
        const roiRes = await fetchRealizedRoi(symbol, 24);

        const { system, user } = await buildPrompt(
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
        );

        // 7) Query AI (post-parse enforces allowed_actions + close_conditions)
        const decision = await callAI(system, user);

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
                  })
                : gatesOut;

        if (
            !positionOpen &&
            (decision.action === 'BUY' || decision.action === 'SELL') &&
            gatesForExec.preDecision
        ) {
            return res.status(200).json({
                symbol,
                timeFrame,
                dryRun,
                decision,
                execRes: { placed: false, orderId: null, clientOid: null, reason: 'gates_short_circuit' },
                gates: { ...gatesForExec.gates, metrics: gatesForExec.metrics },
                usedTape,
            });
        }

        const execRes = await executeDecision(symbol, sideSizeUSDT, decision, productType, dryRun);

        const change24h = Number(tickerData?.change24h ?? tickerData?.changeUtc24h ?? tickerData?.chgPct);
        const snapshot = {
            price: Number.isFinite(lastPrice) ? lastPrice : undefined,
            change24h: Number.isFinite(change24h) ? change24h : undefined,
            spread: safeNum(analytics.spread, 0),
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
            timeFrame,
            dryRun,
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
