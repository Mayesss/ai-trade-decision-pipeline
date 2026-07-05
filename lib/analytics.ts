// lib/analytics.ts
import { bitgetFetch, resolveProductType } from './bitget';
import type { ProductType } from './bitget';
import { TRADE_WINDOW_MINUTES } from './constants';

// ------------------------------
// Types
// ------------------------------
export interface SymbolMeta {
    symbol: string;
    pricePlace: number;
    volumePlace: number;
    minTradeNum: string;
    sizeMultiplier?: string;
    // Exchange leverage bounds for the contract. Present on the raw Bitget
    // contracts row; surfaced here so the profit-lock margin-recycle maneuver can
    // clamp a leverage raise to the symbol's real ceiling (not the 1–5 entry cap).
    maxLever?: string;
    minLever?: string;
}

export type PositionInfo =
    | { status: 'none' }
    | {
          status: 'open';
          symbol: string;
          holdSide: 'long' | 'short';
          entryPrice: string;
          entryTimestamp?: number;
          posMode?: 'one_way_mode' | 'hedge_mode';
          marginCoin?: string;
          available?: string;
          total?: string;
          currentPnl?: string;
          leverage?: number | null;
          markPrice?: number | null;
      };

export type PositionWindow = {
    id: string;
    symbol: string;
    side: 'long' | 'short' | null;
    entryTimestamp?: number | null;
    exitTimestamp?: number | null;
    entryPrice?: number | null;
    exitPrice?: number | null;
    pnlNet?: number | null; // net after fees
    pnlPct?: number | null; // net pct
    pnlGross?: number | null; // gross before fees if available
    pnlGrossPct?: number | null; // gross pct
    notional?: number | null;
    leverage?: number | null;
};

// Leverage we actually set at execution time, captured from decision history.
// This is ground truth for what leverage was active on a position — unlike
// Bitget's reported leverage, which only ever reflects the *current* account
// setting and is stale for closed/historical positions.
export type CapturedLeverage = { timestamp: number; leverage: number };
const BITGET_POSITION_HISTORY_MAX_INTERVAL_MS = 89 * 24 * 60 * 60 * 1000;

// Pick the captured leverage that was in effect for a position opened at
// `entryTs`: the most recent capture at or before entry; failing that, the
// closest capture after entry. Returns null when nothing usable is provided.
export function pickCapturedLeverage(
    entryTs: number | null | undefined,
    captured?: CapturedLeverage[] | null,
): number | null {
    if (!captured?.length) return null;
    if (!Number.isFinite(entryTs as number)) {
        // No entry time to match against — fall back to the most recent capture.
        const latest = captured.reduce((a, b) => (b.timestamp > a.timestamp ? b : a));
        return latest.leverage;
    }
    const ts = entryTs as number;
    let before: CapturedLeverage | null = null;
    let after: CapturedLeverage | null = null;
    for (const c of captured) {
        if (!(Number.isFinite(c.timestamp) && Number.isFinite(c.leverage) && c.leverage > 0)) continue;
        if (c.timestamp <= ts) {
            if (!before || c.timestamp > before.timestamp) before = c;
        } else if (!after || c.timestamp < after.timestamp) {
            after = c;
        }
    }
    return (before ?? after)?.leverage ?? null;
}

type OBLevel = { price: number; size: number };

// ------------------------------
// Helpers
// ------------------------------
function num(x: any, def = 0): number {
    const n = Number(x);
    return Number.isFinite(n) ? n : def;
}
function normalizeTimestamp(tsLike: any): number | undefined {
    const ts = Number(tsLike);
    if (!Number.isFinite(ts) || ts <= 0) return undefined;
    return ts > 1e12 ? ts : ts * 1000;
}
function ensureAscendingCandles(cs: any[]) {
    if (!Array.isArray(cs)) return [];
    return cs.slice().sort((a: any, b: any) => Number(a[0]) - Number(b[0]));
}
export function roundToDecimals(value: number, decimals: number): number {
    const factor = Math.pow(10, decimals);
    return Math.floor(value * factor) / factor;
}

function extractPositionHistoryItems(res: any): any[] {
    return Array.isArray(res?.list)
        ? res.list
        : Array.isArray(res?.data?.list)
        ? res.data.list
        : Array.isArray(res)
        ? res
        : Array.isArray(res?.data)
        ? res.data
        : Array.isArray(res?.items)
        ? res.items
        : [];
}

async function fetchBitgetPositionHistoryItems(symbol: string, startTime: number, endTime: number): Promise<any[]> {
    const productType = resolveProductType();
    const requestedStart = Math.max(0, Math.floor(startTime));
    const end = Math.max(requestedStart, Math.floor(endTime));
    // Bitget rejects position-history windows that reach too far into the past,
    // even when each request interval is below the documented max. The dashboard
    // complements this live window with the local swing.positions mirror.
    const liveStartFloor = Math.max(0, end - BITGET_POSITION_HISTORY_MAX_INTERVAL_MS + 1);
    const start = Math.max(liveStartFloor, requestedStart);
    const items: any[] = [];

    for (
        let chunkStart = start;
        chunkStart <= end;
        chunkStart = Math.min(end + 1, chunkStart + BITGET_POSITION_HISTORY_MAX_INTERVAL_MS)
    ) {
        const chunkEnd = Math.min(end, chunkStart + BITGET_POSITION_HISTORY_MAX_INTERVAL_MS - 1);
        const res: any = await bitgetFetch('GET', '/api/v2/mix/position/history-position', {
            productType,
            symbol,
            startTime: chunkStart,
            endTime: chunkEnd,
        });
        items.push(...extractPositionHistoryItems(res));
        if (chunkEnd >= end) break;
    }

    return items;
}

// ------------------------------
// Symbol meta (FUTURES only)
// ------------------------------
export async function fetchSymbolMeta(symbol: string, productType: ProductType): Promise<SymbolMeta> {
    const pt = (productType as string).toUpperCase();
    const all = await bitgetFetch('GET', '/api/v2/mix/market/contracts', { productType: pt });
    const meta = (all || []).find((x: any) => x.symbol === symbol);
    if (!meta) throw new Error(`No contract metadata for ${symbol}`);
    return meta;
}

// ------------------------------
// Order size (FUTURES only)
// ------------------------------
export async function computeOrderSize(
    symbol: string,
    notionalUSDT: number,
    productType: ProductType,
): Promise<number> {
    const pt = (productType as string).toUpperCase();
    const all = await bitgetFetch('GET', '/api/v2/mix/market/contracts', { productType: pt });
    const meta = (all || []).find((x: any) => x.symbol === symbol);
    if (!meta) throw new Error(`No contract metadata for ${symbol}`);

    const ticker = await bitgetFetch('GET', '/api/v2/mix/market/ticker', { symbol, productType: pt });
    const t = Array.isArray(ticker) ? ticker[0] : ticker;
    const price = num(t?.lastPr ?? t?.last ?? t?.close ?? t?.price);
    if (!(price > 0)) throw new Error(`Invalid price for ${symbol}`);

    const rawSize = notionalUSDT / price;
    const decimals = Number(meta.volumePlace ?? 3);
    const minTradeNum = parseFloat(meta.minTradeNum ?? '0');
    const step = parseFloat(meta.sizeMultiplier ?? `1e-${decimals}`);
    const quantizeDown = (x: number, s: number) => Math.floor(x / s) * s;

    const rounded = quantizeDown(rawSize, step);
    const finalSize = Math.max(rounded, minTradeNum);
    if (!(finalSize > 0)) throw new Error(`Failed to compute valid size (raw=${rawSize}, rounded=${rounded})`);
    return Number(finalSize.toFixed(decimals));
}

// ------------------------------
// Positions (FUTURES only)
// ------------------------------
type RawPosition = {
    symbol: string;
    holdSide?: string;
    openPriceAvg: string;
    cTime?: string | number;
    createTime?: string | number;
    uTime?: string | number;
    posMode?: 'one_way_mode' | 'hedge_mode';
    marginCoin?: string;
    available?: string;
    total?: string;
    markPrice?: string;
    leverage?: string | number;
    marginLeverage?: string | number;
    lever?: string | number;
    unrealizedPL?: string | number;
};

export async function fetchPositionInfo(symbol: string): Promise<PositionInfo> {
    const productType = resolveProductType();
    const positions: RawPosition[] = await bitgetFetch('GET', '/api/v2/mix/position/all-position', { productType });

    const matches = (positions || []).filter((p) => p.symbol === symbol);
    if (!matches.length) return { status: 'none' };

    const chosen: RawPosition = matches.slice().sort((a: RawPosition, b: RawPosition) => {
        const bSize = Math.abs(num(b.total ?? b.available ?? '0'));
        const aSize = Math.abs(num(a.total ?? a.available ?? '0'));
        return bSize - aSize;
    })[0];

    const levRaw = Number(chosen.leverage ?? chosen.marginLeverage ?? chosen.lever);
    const leverage = Number.isFinite(levRaw) && levRaw > 0 ? levRaw : null;
    const markRaw = Number(chosen.markPrice);
    const markPrice = Number.isFinite(markRaw) && markRaw > 0 ? markRaw : null;

    return {
        status: 'open',
        symbol,
        holdSide: (chosen.holdSide ?? '').toLowerCase() as 'long' | 'short',
        entryPrice: chosen.openPriceAvg,
        entryTimestamp: normalizeTimestamp(chosen.cTime ?? chosen.createTime ?? chosen.uTime),
        posMode: chosen.posMode,
        marginCoin: chosen.marginCoin,
        available: chosen.available,
        total: chosen.total,
        currentPnl: calculatePnLPercent(chosen),
        leverage,
        markPrice,
    };
}
function calculatePnLPercent(data: RawPosition): string {
    const sizeBase = num(data.total);
    const mark = Math.max(1e-9, num(data.markPrice));
    const lev = Math.max(1, num(data.leverage));
    const uPnl = num(data.unrealizedPL);
    const initialMargin = (sizeBase * mark) / lev || 1;
    const pnlPercent = (uPnl / initialMargin) * 100;
    return pnlPercent.toFixed(2) + '%';
}

// ------------------------------
// Recent closed positions (FUTURES only)
// ------------------------------
export async function fetchRecentPositionWindows(
    symbol: string,
    hours = 24,
    capturedLeverages?: CapturedLeverage[] | null,
): Promise<PositionWindow[]> {
    try {
        const now = Date.now();
        const startTime = now - hours * 60 * 60 * 1000;
        const items = await fetchBitgetPositionHistoryItems(symbol, startTime, now);

        const windows: PositionWindow[] = items
            .map((it: any, idx: number): PositionWindow => {
                const entryTimestamp = normalizeTimestamp(
                    it.ctime ?? it.createTime ?? it.openTime ?? it.uTime ?? it.entryTime,
                );
                const exitTimestamp = normalizeTimestamp(
                    it.utime ?? it.closeTime ?? it.updateTime ?? it.endTime ?? it.exitTime,
                );
                const entryPrice = num(it.openAvgPrice ?? it.entryPrice);
                const exitPrice = num(it.closeAvgPrice ?? it.exitPrice);
                const size = num(it.closeTotalPos ?? it.openTotalPos ?? it.size);
                const notional = size * (exitPrice || entryPrice || 0);
                const pnlGross = num(it.pnl, NaN);
                const pnlNet = num(it.netProfit ?? it.pnl, NaN);
                const sideRaw = (it.holdSide ?? it.side ?? it.direction ?? '').toLowerCase();
                const side = sideRaw === 'long' || sideRaw === 'short' ? sideRaw : null;
                const marginVal = num(
                    it.margin ?? it.marginAmount ?? it.marginValue ?? it.fixedMargin ?? it.cMargin,
                    NaN,
                );
                // Leverage authority for a CLOSED position, most to least trustworthy:
                //   1. captured-at-execution leverage (what we actually set when opening)
                //   2. derived notional/margin (computed from the position's own historical
                //      notional + locked margin — both are real historical values)
                //   3. Bitget's reported leverage field (only ever the *current* account
                //      setting, so stale for past positions — last resort)
                const capturedLev = pickCapturedLeverage(entryTimestamp, capturedLeverages);
                const derivedLev =
                    Number.isFinite(notional) && notional! > 0 && Number.isFinite(marginVal) && marginVal > 0
                        ? notional! / marginVal
                        : null;
                const levRaw = Number(it.leverage ?? it.marginLeverage ?? it.lever);
                const reportedLev = Number.isFinite(levRaw) && levRaw > 0 ? levRaw : null;
                const leverage = capturedLev ?? derivedLev ?? reportedLev;
                const marginBasisRaw =
                    Number.isFinite(marginVal) && marginVal > 0
                        ? marginVal
                        : Number.isFinite(notional) && notional > 0 && Number.isFinite(leverage) && leverage! > 0
                        ? notional / leverage!
                        : Number.isFinite(notional) && notional > 0
                        ? notional
                        : null;
                const marginBasis =
                    typeof marginBasisRaw === 'number' && Number.isFinite(marginBasisRaw) && marginBasisRaw > 0
                        ? marginBasisRaw
                        : null;
                const pnlPct = Number.isFinite(pnlNet) && marginBasis ? (pnlNet / marginBasis) * 100 : null;
                const pnlGrossPct = Number.isFinite(pnlGross) && marginBasis ? (pnlGross / marginBasis) * 100 : null;
                const id = String(
                    it.id ??
                        it.positionId ??
                        it.orderId ??
                        it.tradeId ??
                        `${symbol}-${entryTimestamp || 'nots'}-${idx}`,
                );

                return {
                    id,
                    symbol,
                    side,
                    entryTimestamp,
                    exitTimestamp,
                    entryPrice: Number.isFinite(entryPrice) ? entryPrice : null,
                    exitPrice: Number.isFinite(exitPrice) ? exitPrice : null,
                    pnlNet: Number.isFinite(pnlNet) ? pnlNet : null,
                    pnlGross: Number.isFinite(pnlGross) ? pnlGross : null,
                    pnlPct,
                    pnlGrossPct,
                    notional: Number.isFinite(notional) ? notional : null,
                    leverage,
                };
            })
            .filter((p) => {
                const entryMs = p.entryTimestamp ?? 0;
                const exitMs = p.exitTimestamp ?? p.entryTimestamp ?? 0;
                if (!(entryMs > 0 && entryMs <= now)) return false;
                const inWindowByEntry = entryMs >= startTime;
                const inWindowByExit = exitMs >= startTime && exitMs <= now;
                return inWindowByEntry || inWindowByExit;
            });

        return windows.sort(
            (a, b) => Number(a.entryTimestamp ?? a.exitTimestamp ?? 0) - Number(b.entryTimestamp ?? b.exitTimestamp ?? 0),
        );
    } catch (err) {
        console.warn(`Failed to fetch recent position windows for ${symbol}:`, err);
        return [];
    }
}

// ------------------------------
// Realized ROI (Bitget position history) for a window
// ------------------------------
export async function fetchRealizedRoi(
    symbol: string,
    hours = 24,
): Promise<{
    roi: number | null;
    count: number;
    sumPct: number | null;
    last: number | null;
    lastNet: number | null;
    lastNetPct: number | null;
    lastSide: 'long' | 'short' | null;
}> {
    try {
        const now = Date.now();
        const startTime = now - hours * 60 * 60 * 1000;
        const items = await fetchBitgetPositionHistoryItems(symbol, startTime, now);
        let total = 0;
        let count = 0;
        let sumPct = 0;
        let pctCount = 0;
        let last: number | null = null;
        let lastNet: number | null = null;
        let lastNetPct: number | null = null;
        let lastSide: 'long' | 'short' | null = null;
        const sorted = items
            .slice()
            .sort((a: any, b: any) => Number(b.utime || b.ctime || 0) - Number(a.utime || a.ctime || 0));
        for (const it of items) {
            const net = Number(it.netProfit);
            const pnl = Number(it.pnl);
            const value = Number.isFinite(net) ? net : pnl;
            if (Number.isFinite(value)) {
                total += value as number;
                count += 1;
            }
            const size = Number(it.closeTotalPos ?? it.openTotalPos);
            const px = Number(it.closeAvgPrice ?? it.openAvgPrice);
            const notional = Number.isFinite(size) && Number.isFinite(px) ? size * px : null;
            if (Number.isFinite(notional) && notional! > 0 && Number.isFinite(net)) {
                sumPct += (net / notional!) * 100;
                pctCount += 1;
            }
        }
        if (sorted.length) {
            const latest = sorted[0];
            const lastPnl = Number(latest?.pnl);
            const lastNetVal = Number(latest?.netProfit);
            last = Number.isFinite(lastPnl) ? lastPnl : null;
            lastNet = Number.isFinite(lastNetVal) ? lastNetVal : null;
            const sideRaw = (latest?.holdSide || latest?.side || '').toLowerCase();
            lastSide = sideRaw === 'long' || sideRaw === 'short' ? sideRaw : null;
            const size = Number(latest?.openTotalPos ?? latest?.closeTotalPos);
            const px = Number(latest?.openAvgPrice ?? latest?.closeAvgPrice);
            const notional = Number.isFinite(size) && Number.isFinite(px) ? size * px : null;
            if (Number.isFinite(notional) && Number.isFinite(lastNetVal) && notional! > 0) {
                lastNetPct = (lastNetVal / notional!) * 100;
            } else {
                lastNetPct = null;
            }
        }
        return {
            roi: count ? total : null,
            count,
            sumPct: pctCount ? sumPct : null,
            last,
            lastNet,
            lastNetPct,
            lastSide,
        };
    } catch (err) {
        console.warn(`Failed to fetch realized ROI for ${symbol}:`, err);
        return { roi: null, count: 0, sumPct: null, last: null, lastNet: null, lastNetPct: null, lastSide: null };
    }
}

// ------------------------------
// Trades (FUTURES only) with budgets
// ------------------------------
type FillsOpts = {
    minutes: number;
    maxTrades?: number; // cap total trades
    maxPages?: number; // cap pagination loops
    maxMs?: number; // time budget
};
export async function fetchTradesForMinutes(symbol: string, productType: ProductType, minutes: number): Promise<any[]> {
    return fetchTradesBudgeted(symbol, productType, { minutes });
}
export async function fetchTradesBudgeted(symbol: string, productType: ProductType, opts: FillsOpts): Promise<any[]> {
    const { minutes, maxTrades = 1500, maxPages = 8, maxMs = 3000 } = opts;
    const trades: any[] = [];
    const cutoff = Date.now() - minutes * 60_000;
    let lastId: string | undefined;
    let pages = 0;
    const t0 = Date.now();

    while (true) {
        const params: any = { symbol, productType };
        if (lastId) params.after = lastId;

        const batch = await bitgetFetch('GET', '/api/v2/mix/market/fills', params);
        if (!batch?.length) break;
        trades.push(...batch);

        const lastTrade = batch[batch.length - 1];
        const lastTradeTs = Number(lastTrade.ts || batch[0]?.ts);
        if (lastTradeTs < cutoff) break;

        lastId = lastTrade.tradeId || lastTrade.id;
        pages += 1;

        if (trades.length >= maxTrades) break;
        if (pages >= maxPages) break;
        if (Date.now() - t0 >= maxMs) break;
    }
    return trades.filter((t) => Number(t.ts) >= cutoff);
}

// ------------------------------
// Market bundle (FUTURES only) – parallel + optional tape
// ------------------------------
type BundleOpts = {
    includeTrades?: boolean; // default true
    tradeMinutes?: number; // default TRADE_WINDOW_MINUTES
    tradeMaxMs?: number; // default 1500
    tradeMaxPages?: number; // default 8
    tradeMaxTrades?: number; // default 1500
    candleLimit?: number; // default 30
};
export async function fetchMarketBundle(symbol: string, bundleTimeFrame: string, opts: BundleOpts = {}) {
    const {
        includeTrades = true,
        tradeMinutes = Number(TRADE_WINDOW_MINUTES || 30),
        tradeMaxMs = 1500,
        tradeMaxPages = 8,
        tradeMaxTrades = 1500,
        candleLimit = 30,
    } = opts;

    const productType = resolveProductType(); // e.g., 'USDT-FUTURES'

    // Run independent calls in parallel
    const [tickerRaw, candlesRaw, orderbook, fundingRes, fundingHistoryRes, oiRes] = await Promise.all([
        bitgetFetch('GET', '/api/v2/mix/market/ticker', { symbol, productType }),
        bitgetFetch('GET', '/api/v2/mix/market/candles', {
            symbol,
            productType,
            granularity: bundleTimeFrame,
            limit: candleLimit,
        }),
        bitgetFetch('GET', '/api/v2/mix/market/orderbook', {
            symbol,
            productType,
            limit: 100,
        }),
        // settle so a failure here doesn't block
        (async () => {
            try {
                return await bitgetFetch('GET', '/api/v2/mix/market/current-fund-rate', { symbol, productType });
            } catch {
                return null;
            }
        })(),
        (async () => {
            try {
                return await bitgetFetch('GET', '/api/v2/mix/market/history-fund-rate', { symbol, productType });
            } catch {
                return null;
            }
        })(),
        (async () => {
            try {
                return await bitgetFetch('GET', '/api/v2/mix/market/open-interest', { symbol, productType });
            } catch {
                return null;
            }
        })(),
    ]);

    const ticker = Array.isArray(tickerRaw) ? tickerRaw[0] : tickerRaw;
    const candles = ensureAscendingCandles(candlesRaw);
    let trades: any[] = [];

    if (includeTrades) {
        trades = await fetchTradesBudgeted(symbol, productType, {
            minutes: tradeMinutes,
            maxTrades: tradeMaxTrades,
            maxPages: tradeMaxPages,
            maxMs: tradeMaxMs,
        });
    }

    return { ticker, candles, trades, orderbook, funding: fundingRes, fundingHistory: fundingHistoryRes, oi: oiRes, productType };
}

// ------------------------------
// Analytics: volume profile, liquidity
// ------------------------------
export function computeAnalytics(bundle: any) {
    const normTrades = (bundle.trades || [])
        .map((t: any) => ({
            price: num(t.price ?? t.fillPrice ?? t.p ?? t[1] ?? NaN),
            size: num(t.size ?? t.fillQuantity ?? t.q ?? t[2] ?? NaN),
            ts: Number(t.ts ?? t.tradeTime ?? t[0] ?? Date.now()),
        }))
        .filter((t: any) => Number.isFinite(t.price) && Number.isFinite(t.size) && t.size > 0);

    const bestBid = num(bundle.orderbook?.bids?.[0]?.[0] ?? bundle.orderbook?.bids?.[0]?.price);
    const bestAsk = num(bundle.orderbook?.asks?.[0]?.[0] ?? bundle.orderbook?.asks?.[0]?.price);
    const spreadAbs = bestAsk > 0 && bestBid > 0 ? bestAsk - bestBid : 0;
    const lastPrice = normTrades[0]?.price || 0;
    const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
    const last = num(t?.lastPr ?? t?.last ?? t?.close ?? lastPrice) || 0;
    const spreadBps = last > 0 && Number.isFinite(spreadAbs) ? (spreadAbs / last) * 1e4 : 0;

    const pct = Math.max(0.0005, spreadAbs && last ? (spreadAbs / last) * 3 : 0.0005);
    const bins = new Map<number, number>();
    for (const tr of normTrades) {
        const bin = Math.round((tr.price - last) / (last * pct));
        bins.set(bin, (bins.get(bin) || 0) + tr.size);
    }
    const volume_profile = Array.from(bins.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([bin, vol]) => ({
            bin,
            price: last + bin * last * pct,
            volume: Number(vol.toFixed(6)),
        }));

    const bids: OBLevel[] = (bundle.orderbook?.bids || []).map(
        (l: any): OBLevel => ({
            price: num(l[0] ?? l.price),
            size: num(l[1] ?? l.size),
        }),
    );
    const asks: OBLevel[] = (bundle.orderbook?.asks || []).map(
        (l: any): OBLevel => ({
            price: num(l[0] ?? l.price),
            size: num(l[1] ?? l.size),
        }),
    );

    const topWalls = {
        bid: bids
            .slice()
            .sort((a, b) => b.size - a.size)
            .slice(0, 5),
        ask: asks
            .slice()
            .sort((a, b) => b.size - a.size)
            .slice(0, 5),
    };

    return {
        volume_profile,
        topWalls,
        // Legacy alias; absolute spread in price units.
        spread: spreadAbs,
        spreadAbs,
        spreadBps,
        last,
        bestBid,
        bestAsk,
    };
}
