// pages/api/analyzeMultiple.ts
export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchMarketBundle, computeAnalytics, fetchPositionInfo } from '../../lib/analytics';
import { calculateMultiTFIndicators } from '../../lib/indicators';
import { fetchNewsSentiment } from '../../lib/news';

import { buildPrompt, callAI } from '../../lib/ai';
import { getGates } from '../../lib/gates';

import { executeDecision, getTradeProductType } from '../../lib/trading';
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// ------------------------------------------------------------------
// Small utilities (same as in analyze.ts)
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

function robustCvdFlip(params: {
    side: 'long' | 'short';
    cvdShort: number;
    cvdMedium?: number;
    midRetBps?: number;
    obImb?: number;
    symbolKey: string;
    minHoldMs?: number;
    nowTs?: number;
    enteredAt?: number;
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

    if (enteredAt && nowTs - enteredAt < minHoldMs) return false;

    const against = (side === 'long' && cvdShort < 0) || (side === 'short' && cvdShort > 0);
    if (!against) {
        const s = touchPersist(symbolKey);
        s.lastFlipDir = 'for';
        s.streak = 0;
        return false;
    }

    const magOk = Math.abs(cvdShort) >= Math.max(5, Math.abs(cvdMedium) * 0.2);
    const confOk =
        side === 'long'
            ? midRetBps <= -2 || obImb <= -0.15
            : midRetBps >= +2 || obImb >= +0.15;

    if (!(magOk && confOk)) {
        const s = touchPersist(symbolKey);
        s.lastFlipDir = 'against';
        s.streak = 0;
        return false;
    }

    const s = touchPersist(symbolKey);
    if (s.lastFlipDir === 'against') s.streak += 1;
    else s.streak = 1;
    s.lastFlipDir = 'against';

    return s.streak >= 2;
}

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
// Core per-symbol runner
// ------------------------------------------------------------------
type ProductType = 'usdt-futures' | 'usdc-futures' | 'coin-futures';

async function runAnalysisForSymbol(params: {
    symbol: string;
    timeFrame: string;
    dryRun: boolean;
    sideSizeUSDT: number;
    productType: ProductType;
}) {
    const { symbol, timeFrame, dryRun, sideSizeUSDT, productType } = params;

    const MAX_RETRIES = 3;
    const BASE_DELAY_MS = 300; // increase if you still get 429

    for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
        try {
            // ---- your existing logic goes here ----
            // 1) Parallel baseline fetches
            const [positionInfo, news, bundleLight, indicators] = await Promise.all([
                fetchPositionInfo(symbol),
                fetchNewsSentiment(symbol),
                fetchMarketBundle(symbol, timeFrame, { includeTrades: false }),
                calculateMultiTFIndicators(symbol),
            ]);

            const positionForPrompt =
                positionInfo.status === 'open'
                    ? `${positionInfo.holdSide}, entryPrice: ${positionInfo.entryPrice}, currentPnl=${positionInfo.currentPnl}`
                    : 'none';

            const persistKey = `${symbol}:${timeFrame}`;
            const pstate = touchPersist(persistKey);
            if (positionInfo.status === 'open') {
                if (!pstate.enteredAt || pstate.lastSide !== positionInfo.holdSide) {
                    pstate.enteredAt = Date.now();
                    pstate.lastSide = positionInfo.holdSide;
                    pstate.streak = 0;
                    pstate.lastFlipDir = undefined;
                }
            } else {
                persist.delete(persistKey);
            }

            const analyticsLight = computeAnalytics({ ...bundleLight, trades: [] });

            const gatesOut = getGates({
                symbol,
                bundle: bundleLight,
                analytics: analyticsLight,
                indicators,
                notionalUSDT: sideSizeUSDT,
                positionOpen: positionInfo.status === 'open',
            });

            if (gatesOut.preDecision && positionInfo.status !== 'open') {
                return {
                    symbol,
                    timeFrame,
                    dryRun,
                    decision: gatesOut.preDecision,
                    execRes: {
                        placed: false,
                        orderId: null,
                        clientOid: null,
                        reason: 'gates_short_circuit',
                    },
                    gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                    usedTape: false,
                };
            }

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

            let close_conditions:
                | {
                      pnl_gt_pos?: boolean;
                      pnl_lt_neg?: boolean;
                      opposite_regime?: boolean;
                      cvd_flip?: boolean;
                  }
                | undefined;

            if (positionInfo.status === 'open') {
                const pnlPct = parsePnlPct(positionInfo.currentPnl);
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
                    pnl_gt_pos: pnlPct >= 1.0,
                    pnl_lt_neg: pnlPct <= -1.0,
                    opposite_regime,
                    cvd_flip,
                };
            }

            const { system, user } = buildPrompt(
                symbol,
                timeFrame,
                bundle,
                analytics,
                positionForPrompt,
                news,
                indicators,
            );

            const decision = await callAI(system, user);
            const execRes = await executeDecision(symbol, sideSizeUSDT, decision, productType, dryRun);


            return {
                symbol,
                timeFrame,
                dryRun,
                decision,
                execRes,
                gates: { ...gatesOut.gates, metrics: gatesOut.metrics },
                usedTape,
            };
            // ---- end of existing logic ----
        } catch (err: any) {
            const msg = err?.message || String(err);

            // hard-ignore obviously invalid/removed symbols so we don't keep hammering them
            if (msg.includes('40309') || msg.includes('40034')) {
                console.error(`Skipping invalid/removed symbol ${symbol}:`, msg);
                return { symbol, error: msg };
            }

            // if 429 and we still have retries left -> backoff then retry
            if (msg.includes('429') && attempt < MAX_RETRIES) {
                const delay = BASE_DELAY_MS * (attempt + 1);
                console.warn(`Rate limited on ${symbol}, retrying in ${delay}ms (attempt ${attempt + 1})`);
                await sleep(delay);
                continue;
            }

            // any other error (or out of retries)
            console.error(`Error analyzing ${symbol}:`, msg);
            return { symbol, error: msg };
        }
    }

    // should never hit here, but just in case
    return { symbol: params.symbol, error: 'Unknown error after retries' };
}

// ------------------------------------------------------------------
// Multi-symbol handler
// ------------------------------------------------------------------
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
        const dryRun: boolean = body.dryRun !== false; // default true
        const sideSizeUSDT: number = Number(body.notional || 10);
        const productType = getTradeProductType();

        const results = await Promise.all(
            symbols.map((sym) =>
                runAnalysisForSymbol({
                    symbol: String(sym),
                    timeFrame,
                    dryRun,
                    sideSizeUSDT,
                    productType,
                }),
            ),
        );

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
