// api/analyze.ts
export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchMarketBundle, computeAnalytics, fetchPositionInfo } from '../../lib/analytics';
import { calculateMultiTFIndicators } from '../../lib/indicators';
import { fetchNewsSentiment } from '../../lib/news';

import { buildPrompt, callAI, computeMomentumSignals } from '../../lib/ai';
import type { MomentumSignals } from '../../lib/ai';
import { getGates } from '../../lib/gates';

import { executeDecision, getTradeProductType } from '../../lib/trading';
import { composePositionContext } from '../../lib/positionContext';
import { appendDecisionHistory } from '../../lib/history';

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
// Robust CVD flip detection with persistence + confirmation
// NOTE: This is in-memory and resets on cold start (serverless).
// ------------------------------------------------------------------
type PersistState = {
    lastFlipDir?: 'against' | 'for';
    streak: number;
    enteredAt?: number;
    lastSide?: 'long' | 'short';
};
const persist = new Map<string, PersistState>();

function touchPersist(key: string): PersistState {
    if (!persist.has(key)) persist.set(key, { streak: 0 });
    return persist.get(key)!;
}

const ATR_ACTIVE_MIN_PCT = 0.0007; // ~0.07%

function shouldSkipMomentumCall(params: { analytics: any; signals: MomentumSignals; price: number }) {
    const { analytics, signals, price } = params;
    const obActive = Math.abs(safeNum(analytics.obImb, 0)) > 0.2;
    const flowActive = Math.abs(signals.flowBias ?? 0) > 0.3;
    const extensionActive = Math.abs(signals.microExtensionInAtr ?? 0) > 0.5;
    const primaryAtr = Number(signals.primaryAtr ?? 0);
    const atrPct = price > 0 && primaryAtr > 0 ? primaryAtr / price : 0;
    const atrActive = atrPct > ATR_ACTIVE_MIN_PCT;
    return !(obActive || flowActive || extensionActive || atrActive);
}

/**
 * robustCvdFlip
 * - Requires sign flip AGAINST the position
 * - Requires magnitude + confirmation (mid return or OB imbalance)
 * - Requires persistence over >= 2 consecutive ticks
 * - Honors min-hold (ignore for first bar after entry)
 */
function robustCvdFlip(params: {
    side: 'long' | 'short';
    cvdShort: number; // short-window CVD (e.g., the one you compute for prompt)
    cvdMedium?: number; // optional context (not required)
    midRetBps?: number; // mid-price change (bps) over the short window
    obImb?: number; // order-book imbalance [-1,1]
    symbolKey: string; // e.g., `${symbol}:${timeFrame}`
    minHoldMs?: number; // e.g., 1 bar of your timeframe
    nowTs?: number; // Date.now()
    enteredAt?: number; // if tracked
}): boolean {
    const {
        side,
        cvdShort,
        cvdMedium = 0,
        midRetBps = 0,
        obImb = 0,
        symbolKey,
        minHoldMs = 15 * 60_000, // default 15m
        nowTs = Date.now(),
        enteredAt,
    } = params;

    // 0) Min-hold: ignore flips in the first bar after entry
    if (enteredAt && nowTs - enteredAt < minHoldMs) return false;

    // 1) Raw sign flip against side?
    const against = (side === 'long' && cvdShort < 0) || (side === 'short' && cvdShort > 0);
    if (!against) {
        const s = touchPersist(symbolKey);
        s.lastFlipDir = 'for';
        s.streak = 0;
        return false;
    }

    // 2) Magnitude (simple heuristic): at least 5 units or 20% of medium-window CVD
    const magOk = Math.abs(cvdShort) >= Math.max(5, Math.abs(cvdMedium) * 0.2);

    // 3) Confirmation by price or book imbalance
    const confOk = side === 'long' ? midRetBps <= -2 || obImb <= -0.15 : midRetBps >= +2 || obImb >= +0.15;

    if (!(magOk && confOk)) {
        const s = touchPersist(symbolKey);
        s.lastFlipDir = 'against';
        s.streak = 0; // don't count if not confirmed
        return false;
    }

    // 4) Persistence: require 2 consecutive ticks of confirmed "against"
    const s = touchPersist(symbolKey);
    if (s.lastFlipDir === 'against') s.streak += 1;
    else s.streak = 1;
    s.lastFlipDir = 'against';

    return s.streak >= 2;
}

// Compute mid-return in bps vs previous candle close (cheap & stable)
function computeMidRetBps(bundle: any): number {
    const bids = bundle?.orderbook?.bids;
    const asks = bundle?.orderbook?.asks;
    const bestBid = safeNum(bids?.[0]?.[0] ?? bids?.[0]?.price);
    const bestAsk = safeNum(asks?.[0]?.[0] ?? asks?.[0]?.price);
    const midNow = bestBid > 0 && bestAsk > 0 ? (bestBid + bestAsk) / 2 : NaN;

    const candles = Array.isArray(bundle?.candles) ? bundle.candles : [];
    const prevClose = candles.length >= 2 ? safeNum(candles[candles.length - 2]?.[4]) : NaN;

    if (!Number.isFinite(midNow) || !Number.isFinite(prevClose) || prevClose <= 0) return 0;
    return (midNow / prevClose - 1) * 1e4; // bps
}

// ------------------------------------------------------------------
// Handler
// ------------------------------------------------------------------
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    try {
        if (req.method !== 'POST') {
            return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST' });
        }

        const body = req.body ?? {};
        const symbol = (body.symbol as string) || 'ETHUSDT';
        const timeFrame = body.timeFrame || '15m';
        const microTimeFrame = body.microTimeFrame || '1m';
        const macroTimeFrame = body.macroTimeFrame || '1H';
        const dryRun = body.dryRun !== false; // default true
        const sideSizeUSDT = Number(body.notional || 10);

        // 1) Product & parallel baseline fetches (fast)
        const productType = getTradeProductType();

        const [positionInfo, newsSentiment, bundleLight, indicators] = await Promise.all([
            fetchPositionInfo(symbol),
            fetchNewsSentiment(symbol),
            // Light bundle: skip tape (fills)
            fetchMarketBundle(symbol, timeFrame, { includeTrades: false }),
            calculateMultiTFIndicators(symbol, { primary: timeFrame, micro: microTimeFrame, macro: macroTimeFrame }),
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
                pstate.streak = 0;
                pstate.lastFlipDir = undefined;
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
            analytics,
            indicators,
            gates: gatesOut.gates,
            primaryTimeframe: timeFrame,
        });

        const positionOpen = positionInfo.status === 'open';
        const calmMarket = !positionOpen && shouldSkipMomentumCall({ analytics, signals: momentumSignals, price: effectivePrice });

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

        // 5) CLOSE conditions (robust CVD flip + PnL bands + regime)
        let pnlPct = 0;
        let close_conditions:
            | {
                  pnl_gt_pos?: boolean;
                  pnl_lt_neg?: boolean;
                  opposite_regime?: boolean;
                  cvd_flip?: boolean;
                  // time_stop?: boolean; // add if you track bars-in-trade
              }
            | undefined;

        if (positionInfo.status === 'open') {
            pnlPct = parsePnlPct(positionInfo.currentPnl);
            const side = positionInfo.holdSide as 'long' | 'short';

            const regimeUp = indicators.macro.includes('trend=up');
            const regimeDown = indicators.macro.includes('trend=down');
            const opposite_regime = (side === 'long' && regimeDown) || (side === 'short' && regimeUp);

            // Compute mid return (bps) vs previous candle close for confirmation
            const midRetBps = computeMidRetBps(bundle);
            const obImb = safeNum(analytics.obImb, 0);

            // If you later compute a medium-window CVD, plug it here; for now undefined
            const cvdShort = safeNum(analytics.cvd, 0);
            const cvdMedium = undefined;

            const cvd_flip = robustCvdFlip({
                side,
                cvdShort,
                cvdMedium,
                midRetBps,
                obImb,
                symbolKey: persistKey,
                // minHoldMs: 15 * 60_000, // default equals a 15m bar; adjust for your timeframe
                enteredAt: pstate.enteredAt,
            });

            close_conditions = {
                pnl_gt_pos: pnlPct >= 1.0, // take profit ≥ +1%
                pnl_lt_neg: pnlPct <= -1.0, // stop loss ≤ -1%
                opposite_regime, // macro regime flipped vs side
                cvd_flip, // robust & persistent now
                // time_stop: false,
            };
        }

        const positionContext = composePositionContext({
            position: positionInfo,
            pnlPct,
            cvd: safeNum(analytics.cvd, 0),
            obImb: safeNum(analytics.obImb, 0),
            enteredAt: pstate.enteredAt,
        });

        // 6) Build prompt with allowed_actions, gates, and close_conditions

        const { system, user } = buildPrompt(
            symbol, // e.g. "BTCUSDT"
            timeFrame, // e.g. "45m"
            bundle, // from fetchMarketBundle(...)
            analytics, // from computeAnalytics(bundle)
            positionForPrompt, // "none" | JSON string like 'open long @ ...' (your current format)
            newsSentiment ?? null, // omit from prompt if unavailable
            indicators, // from calculateMultiTFIndicators(symbol)
            gatesOut.gates, // from getGates(...)
            positionContext,
            momentumSignals,
        );

        // 7) Query AI (post-parse enforces allowed_actions + close_conditions)
        const decision = await callAI(system, user);

        // 8) Execute (dry run unless explicitly disabled)
        const execRes = await executeDecision(symbol, sideSizeUSDT, decision, productType, dryRun);

        const change24h = Number(tickerData?.change24h ?? tickerData?.changeUtc24h ?? tickerData?.chgPct);
        const snapshot = {
            price: Number.isFinite(lastPrice) ? lastPrice : undefined,
            change24h: Number.isFinite(change24h) ? change24h : undefined,
            obImb: safeNum(analytics.obImb, 0),
            cvd: safeNum(analytics.cvd, 0),
            spread: safeNum(analytics.spread, 0),
            gates: gatesOut.gates,
            metrics: gatesOut.metrics,
            newsSentiment,
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
        });

        // 9) Respond
        return res.status(200).json({
            symbol,
            timeFrame,
            dryRun,
            decision,
            execRes,
            gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
            usedTape,
        });
    } catch (err: any) {
        console.error('Error in /api/analyze:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
