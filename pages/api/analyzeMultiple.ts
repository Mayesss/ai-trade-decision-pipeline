// api/analyzeMultiple.ts
export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchMarketBundle, computeAnalytics, fetchPositionInfo, fetchRealizedRoi } from '../../lib/analytics';
import { calculateMultiTFIndicators } from '../../lib/indicators';
import { fetchNewsWithHeadlines } from '../../lib/news';

import { buildPrompt, callAI, computeMomentumSignals } from '../../lib/ai';
import type { MomentumSignals } from '../../lib/ai';
import { getGates } from '../../lib/gates';

import { executeDecision, getTradeProductType } from '../../lib/trading';
import { composePositionContext } from '../../lib/positionContext';
import { appendDecisionHistory, loadDecisionHistory } from '../../lib/history';

// ------------------------------------------------------------------
// Small utilities (same as analyze.ts)
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

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ------------------------------------------------------------------
// mapLimit: run a function over items with bounded concurrency
// ------------------------------------------------------------------
async function mapLimit<T, R>(items: T[], limit: number, fn: (item: T, index: number) => Promise<R>): Promise<R[]> {
    const results = new Array<R>(items.length);
    let nextIndex = 0;

    async function worker() {
        // eslint-disable-next-line no-constant-condition
        while (true) {
            const currentIndex = nextIndex++;
            if (currentIndex >= items.length) break;

            results[currentIndex] = await fn(items[currentIndex], currentIndex);
        }
    }

    const workers: Promise<void>[] = [];
    const workerCount = Math.min(limit, items.length);
    for (let i = 0; i < workerCount; i++) {
        workers.push(worker());
    }

    await Promise.all(workers);
    return results;
}

// ------------------------------------------------------------------
// Robust CVD flip detection with persistence + confirmation
// (same logic as in analyze.ts)
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

const ATR_ACTIVE_MIN_PCT = 0.0007;

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

function robustCvdFlip(params: {
    side: 'long' | 'short';
    cvdShort: number; // short-window CVD
    cvdMedium?: number; // optional
    midRetBps?: number; // mid-price change (bps) over the short window
    obImb?: number; // order-book imbalance [-1,1]
    symbolKey: string; // `${symbol}:${timeFrame}`
    minHoldMs?: number; // default 15m
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
        minHoldMs = 15 * 60_000,
        nowTs = Date.now(),
        enteredAt,
    } = params;

    // Min-hold: ignore flips in the first bar after entry
    if (enteredAt && nowTs - enteredAt < minHoldMs) return false;

    // Raw sign flip against side?
    const against = (side === 'long' && cvdShort < 0) || (side === 'short' && cvdShort > 0);
    if (!against) {
        const s = touchPersist(symbolKey);
        s.lastFlipDir = 'for';
        s.streak = 0;
        return false;
    }

    // Magnitude: at least 5 units or 20% of medium-window CVD
    const magOk = Math.abs(cvdShort) >= Math.max(5, Math.abs(cvdMedium) * 0.2);

    // Confirmation by price or book imbalance
    const confOk = side === 'long' ? midRetBps <= -2 || obImb <= -0.15 : midRetBps >= +2 || obImb >= +0.15;

    if (!(magOk && confOk)) {
        const s = touchPersist(symbolKey);
        s.lastFlipDir = 'against';
        s.streak = 0; // don't count if not confirmed
        return false;
    }

    // Persistence: require 2 consecutive ticks of confirmed "against"
    const s = touchPersist(symbolKey);
    if (s.lastFlipDir === 'against') s.streak += 1;
    else s.streak = 1;
    s.lastFlipDir = 'against';

    return s.streak >= 2;
}

// Compute mid-return in bps vs previous candle close
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
// Core per-symbol runner with retry & 429 backoff
// ------------------------------------------------------------------
async function runAnalysisForSymbol(params: {
    symbol: string;
    timeFrame: string;
    dryRun: boolean;
    sideSizeUSDT: number;
    productType: ProductType;
    microTimeFrame: string;
    macroTimeFrame: string;
    contextTimeFrame: string;
}) {
    const { symbol, timeFrame, dryRun, sideSizeUSDT, productType, microTimeFrame, macroTimeFrame, contextTimeFrame } =
        params;

    const MAX_RETRIES = 3;
    const BASE_DELAY_MS = 300; // base delay for 429 backoff

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
        try {
            // 1) Parallel baseline fetches (light bundle)
            const [positionInfo, newsBundle, bundleLight, indicators] = await Promise.all([
                fetchPositionInfo(symbol),
                fetchNewsWithHeadlines(symbol),
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

            // Persist state for robustCvdFlip
            const persistKey = `${symbol}:${timeFrame}`;
            const pstate = touchPersist(persistKey);
            if (positionInfo.status === 'open') {
                const entryTimestamp =
                    typeof positionInfo.entryTimestamp === 'number' ? positionInfo.entryTimestamp : undefined;
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

            // 3) Gates on light data
            const gatesOut = getGates({
                symbol,
                bundle: bundleLight,
                analytics: analyticsLight,
                indicators,
                notionalUSDT: sideSizeUSDT,
                positionOpen: positionInfo.status === 'open',
            });

            // Short-circuit if no trade allowed and no open position
            if (gatesOut.preDecision && positionInfo.status !== 'open') {
                return {
                    symbol,
                    decision: gatesOut.preDecision,
                    execRes: {
                        placed: false,
                        reason: 'gates_short_circuit',
                    },
                    gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                    promptSkipped: true,
                    usedTape: false,
                };
            }

            // 4) Decide whether we need tape; if yes, fetch full bundle
            const needTape =
                positionInfo.status === 'open' ||
                gatesOut.allowed_actions.some((a: string) => a === 'BUY' || a === 'SELL');

            let bundle = bundleLight;
            let analytics = analyticsLight;
            let usedTape = false;

            if (needTape) {
                const bundleFull = await fetchMarketBundle(symbol, timeFrame, {
                    includeTrades: true,
                    tradeMinutes: 60,
                    tradeMaxMs: 2500,
                    tradeMaxPages: 6,
                    tradeMaxTrades: 1200,
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
            const calmMarket =
                !positionOpen && shouldSkipMomentumCall({ analytics, signals: momentumSignals, price: effectivePrice });


            // 5) CLOSE conditions (not yet wired into prompt but computed)
            let pnlPct = 0;
            let close_conditions:
                | {
                      pnl_gt_pos?: boolean;
                      pnl_lt_neg?: boolean;
                      opposite_regime?: boolean;
                      cvd_flip?: boolean;
                  }
                | undefined;

            if (positionInfo.status === 'open') {
                pnlPct = parsePnlPct(positionInfo.currentPnl);
                const side = positionInfo.holdSide as 'long' | 'short';

                const regimeUp = indicators.macro.includes('trend=up');
                const regimeDown = indicators.macro.includes('trend=down');
                const opposite_regime = (side === 'long' && regimeDown) || (side === 'short' && regimeUp);

                const midRetBps = computeMidRetBps(bundle);
                const obImb = safeNum(analytics.obImb, 0);

                const cvdShort = safeNum(analytics.cvd, 0);
                const cvdMedium = undefined;

                const cvd_flip = robustCvdFlip({
                    side,
                    cvdShort,
                    cvdMedium,
                    midRetBps,
                    obImb,
                    symbolKey: persistKey,
                    enteredAt: pstate.enteredAt,
                });

                close_conditions = {
                    pnl_gt_pos: pnlPct >= 1.0, // take profit ≥ +1%
                    pnl_lt_neg: pnlPct <= -1.0, // stop loss ≤ -1%
                    opposite_regime,
                    cvd_flip,
                };
            }

            if (!positionOpen && calmMarket) {
                return {
                    symbol,
                    decision: {
                        action: 'HOLD',
                        bias: 'NEUTRAL',
                        signal_strength: 'LOW',
                        summary: 'calm_market',
                        reason: 'conditions_below_momentum_thresholds',
                    },
                    execRes: { placed: false, orderId: null, clientOid: null, reason: 'calm_market' },
                    gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                    close_conditions,
                    promptSkipped: true,
                    usedTape,
                };
            }

            const positionContext = composePositionContext({
                position: positionInfo,
                pnlPct,
                cvd: safeNum(analytics.cvd, 0),
                obImb: safeNum(analytics.obImb, 0),
                enteredAt: pstate.enteredAt,
            });
            const recentHistory = await loadDecisionHistory(symbol, 5);
            const recentActions = recentHistory
                .map((h) => ({ action: h.aiDecision?.action, timestamp: h.timestamp }))
                .filter((a) => a.action);

            // 6) Build prompt
            const roiRes = await fetchRealizedRoi(symbol, 24);
            const { system, user } = buildPrompt(
                symbol,
                timeFrame,
                bundle,
                analytics,
                positionForPrompt,
                newsBundle?.sentiment ?? null,
                newsBundle?.headlines ?? [],
                indicators,
                gatesOut.gates,
                positionContext,
                momentumSignals,
                recentActions,
                roiRes.lastNetPct,
                dryRun,
            );

            // 7) AI decision
            const decision = await callAI(system, user);

            // 8) Execute (dry run unless explicitly disabled)
            const execRes = await executeDecision(symbol, sideSizeUSDT, decision, productType, dryRun);
            // recent actions fetched from history; no in-memory persistence

            const change24h = Number(tickerData?.change24h ?? tickerData?.changeUtc24h ?? tickerData?.chgPct);
            const snapshot = {
                price: Number.isFinite(lastPrice) ? lastPrice : undefined,
                change24h: Number.isFinite(change24h) ? change24h : undefined,
                obImb: safeNum(analytics.obImb, 0),
                cvd: safeNum(analytics.cvd, 0),
                spread: safeNum(analytics.spread, 0),
                gates: gatesOut.gates,
                metrics: gatesOut.metrics,
                momentumSignals,
                newsSentiment: newsBundle?.sentiment ?? null,
                newsHeadlines: newsBundle?.headlines ?? [],
                positionContext,
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

            // 9) Return result for this symbol
            return {
                symbol,
                decision,
                execRes,
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                close_conditions,
                promptSkipped: false,
                prompt: { system, user },
                usedTape,
            };
        } catch (err: any) {
            const msg = err?.message || String(err);

            // Invalid/removed symbol (don’t retry)
            if (msg.includes('40309') || msg.includes('40034')) {
                console.error(`Skipping invalid/removed symbol ${symbol}:`, msg);
                return { symbol, error: msg };
            }

            // Rate limited — backoff and retry
            if (msg.includes('429') && attempt < MAX_RETRIES) {
                const delay = BASE_DELAY_MS * (attempt + 1);
                console.warn(`Rate limited on ${symbol}, retrying in ${delay}ms (attempt ${attempt + 1})`);
                await sleep(delay);
                continue;
            }

            // Any other error or out of retries
            console.error(`Error analyzing ${symbol}:`, msg);
            return { symbol, error: msg };
        }
    }

    // Fallback (shouldn't really hit)
    return { symbol: params.symbol, error: 'Unknown error after retries' };
}

// ------------------------------------------------------------------
// Multi-symbol handler with max 5 concurrent analyses
// ------------------------------------------------------------------
type ProductType = 'usdt-futures' | 'usdc-futures' | 'coin-futures';
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    try {
        if (req.method !== 'POST') {
            return res.status(405).json({ error: 'Method Not Allowed', message: 'Use POST' });
        }

        const body = req.body ?? {};
        const symbols = body.symbols as string[] | undefined;

        if (!Array.isArray(symbols) || symbols.length === 0) {
            return res.status(400).json({
                error: 'Bad Request',
                message: 'Body must include non-empty "symbols" array',
            });
        }

        const timeFrame: string = body.timeFrame || '15m';
        const microTimeFrame: string = body.microTimeFrame || '1m';
        const macroTimeFrame: string = body.macroTimeFrame || '1H';
        const contextTimeFrame: string = body.contextTimeFrame || '1D';
        const dryRun: boolean = body.dryRun !== false; // default true
        const sideSizeUSDT: number = Number(body.notional || 10);
        const productType = getTradeProductType();

        const MAX_CONCURRENCY = 5;
        const PER_TASK_DELAY_MS = 150; // optional extra delay per task, per worker

        const results = await mapLimit(symbols, MAX_CONCURRENCY, async (sym, idx) => {
            const result = await runAnalysisForSymbol({
                symbol: String(sym),
                timeFrame,
                dryRun,
                sideSizeUSDT,
                productType,
                microTimeFrame,
                macroTimeFrame,
                contextTimeFrame,
            });

            if (PER_TASK_DELAY_MS > 0) {
                await sleep(PER_TASK_DELAY_MS);
            }

            return result;
        });

        return res.status(200).json({
            timeFrame,
            dryRun,
            notional: sideSizeUSDT,
            productType,
            results,
        });
    } catch (err: any) {
        console.error('Error in /api/analyzeMultiple:', err);
        return res.status(500).json({ error: err.message || String(err) });
    }
}
