// api/analyzeMultiple.ts
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

const ATR_ACTIVE_MIN_PCT = 0.0007;

function shouldSkipMomentumCall(params: { signals: MomentumSignals; price: number }) {
    const { signals, price } = params;
    const extensionActive = Math.abs(signals.microExtensionInAtr ?? 0) > 0.5;
    const primaryAtr = Number(signals.primaryAtr ?? 0);
    const atrPct = price > 0 && primaryAtr > 0 ? primaryAtr / price : 0;
    const atrActive = atrPct > ATR_ACTIVE_MIN_PCT;
    return !(extensionActive || atrActive);
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
            const positionInfo = await fetchPositionInfo(symbol);
            const positionOpen = positionInfo.status === 'open';
            const primaryCloseTime = isPrimaryCloseTime(timeFrame);

            if (!positionOpen && !primaryCloseTime) {
                return {
                    symbol,
                    decision: {
                        action: 'HOLD',
                        bias: 'NEUTRAL',
                        signal_strength: 'LOW',
                        summary: 'not_primary_close',
                        reason: 'flat_skip_until_primary_close',
                    },
                    execRes: { placed: false, reason: 'not_primary_close' },
                    promptSkipped: true,
                    usedTape: false,
                };
            }

            // 1) Parallel baseline fetches (light bundle)
            const [newsBundle, bundleLight, indicators] = await Promise.all([
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
                positionOpen
                    ? `${positionInfo.holdSide}, entryPrice: ${positionInfo.entryPrice}, currentPnl=${positionInfo.currentPnl}`
                    : 'none';

            // Persist state for robustCvdFlip
            const persistKey = `${symbol}:${timeFrame}`;
            const pstate = touchPersist(persistKey);
            if (positionOpen) {
                const entryTimestamp =
                    typeof positionInfo.entryTimestamp === 'number' ? positionInfo.entryTimestamp : undefined;
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

            // 3) Gates on light data
            const gatesOut = getGates({
                symbol,
                bundle: bundleLight,
                analytics: analyticsLight,
                indicators,
                notionalUSDT: sideSizeUSDT,
                positionOpen,
            });

            // Short-circuit if no trade allowed and no open position
            if (gatesOut.preDecision && !positionOpen) {
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
            const needTape = positionOpen || gatesOut.allowed_actions.some((a: string) => a === 'BUY' || a === 'SELL');

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
                indicators,
                gates: gatesOut.gates,
                primaryTimeframe: timeFrame,
            });

            const calmMarket = !positionOpen && shouldSkipMomentumCall({ signals: momentumSignals, price: effectivePrice });


            // 5) CLOSE conditions (not yet wired into prompt but computed)
            let pnlPct = 0;
            let close_conditions:
                | {
                      pnl_gt_pos?: boolean;
                      pnl_lt_neg?: boolean;
                      opposite_regime?: boolean;
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
                    opposite_regime,
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
                enteredAt: pstate.enteredAt,
            });
            const recentHistory = await loadDecisionHistory(symbol, 5);
            const recentActions = recentHistory
                .map((h) => ({ action: h.aiDecision?.action, timestamp: h.timestamp }))
                .filter((a) => a.action);

            // 6) Build prompt
            const roiRes = await fetchRealizedRoi(symbol, 24);
            const { system, user } = await buildPrompt(
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
                return {
                    symbol,
                    decision,
                    execRes: { placed: false, reason: 'gates_short_circuit' },
                    gates: { ...gatesForExec.gates, metrics: gatesForExec.metrics },
                    promptSkipped: false,
                    usedTape,
                };
            }

            const execRes = await executeDecision(symbol, sideSizeUSDT, decision, productType, dryRun);
            // recent actions fetched from history; no in-memory persistence

            const change24h = Number(tickerData?.change24h ?? tickerData?.changeUtc24h ?? tickerData?.chgPct);
            const snapshot = {
                price: Number.isFinite(lastPrice) ? lastPrice : undefined,
                change24h: Number.isFinite(change24h) ? change24h : undefined,
                spread: safeNum(analytics.spread, 0),
                gates: gatesForExec.gates,
                metrics: gatesForExec.metrics,
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
                biasTimeframes: {
                    context: contextTimeFrame,
                    macro: macroTimeFrame,
                    primary: timeFrame,
                    micro: microTimeFrame,
                },
            });

            // 9) Return result for this symbol
            return {
                symbol,
                decision,
                execRes,
                gates: { ...gatesForExec.gates, metrics: gatesForExec.metrics },
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
        if (req.method !== 'GET') {
            return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
        }

        const body = req.query ?? {};
        const parseBoolParam = (value: string | string[] | undefined, fallback: boolean) => {
            if (value === undefined) return fallback;
            const v = Array.isArray(value) ? value[0] : value;
            if (v === undefined) return fallback;
            const normalized = String(v).toLowerCase();
            if (['false', '0', 'no', 'off'].includes(normalized)) return false;
            if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
            return fallback;
        };
        const symbols = body.symbols as string[] | undefined;

        if (!Array.isArray(symbols) || symbols.length === 0) {
            return res.status(400).json({
                error: 'Bad Request',
                message: 'Body must include non-empty "symbols" array',
            });
        }

        const timeFrame: string = PRIMARY_TIMEFRAME;
        const microTimeFrame: string = MICRO_TIMEFRAME;
        const macroTimeFrame: string = MACRO_TIMEFRAME;
        const contextTimeFrame: string = CONTEXT_TIMEFRAME;
        const dryRun: boolean = parseBoolParam(body.dryRun as string | string[] | undefined, false);
        const sideSizeUSDT: number = Number(body.notional ?? DEFAULT_NOTIONAL_USDT);
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
