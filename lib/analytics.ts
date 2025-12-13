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
      };

export type PositionWindow = {
    id: string;
    symbol: string;
    side: 'long' | 'short' | null;
    entryTimestamp?: number | null;
    exitTimestamp?: number | null;
    entryPrice?: number | null;
    exitPrice?: number | null;
    pnlNet?: number | null;
    pnlPct?: number | null;
};

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
export async function fetchRecentPositionWindows(symbol: string, hours = 24): Promise<PositionWindow[]> {
    try {
        const productType = resolveProductType();
        const now = Date.now();
        const startTime = now - hours * 60 * 60 * 1000;
        const res: any = await bitgetFetch('GET', '/api/v2/mix/position/history-position', {
            productType,
            symbol,
            startTime,
            endTime: now,
        });

        const items: any[] = Array.isArray(res?.list)
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
                const net = num(it.netProfit ?? it.pnl, NaN);
                const pnlPct = Number.isFinite(net) && notional > 0 ? (net / notional) * 100 : null;
                const sideRaw = (it.holdSide ?? it.side ?? it.direction ?? '').toLowerCase();
                const side = sideRaw === 'long' || sideRaw === 'short' ? sideRaw : null;
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
                    pnlNet: Number.isFinite(net) ? net : null,
                    pnlPct,
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
        const productType = resolveProductType();
        const now = Date.now();
        const startTime = now - hours * 60 * 60 * 1000;
        const res: any = await bitgetFetch('GET', '/api/v2/mix/position/history-position', {
            productType,
            symbol,
            startTime,
            endTime: now,
        });
        const items: any[] = Array.isArray(res?.list)
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
    const [tickerRaw, candlesRaw, orderbook, fundingRes, oiRes] = await Promise.all([
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

    return { ticker, candles, trades, orderbook, funding: fundingRes, oi: oiRes, productType };
}

// ------------------------------
// Analytics: CVD, VP, liquidity
// ------------------------------
export function computeAnalytics(bundle: any) {
    const normTrades = (bundle.trades || [])
        .map((t: any) => ({
            price: num(t.price ?? t.fillPrice ?? t.p ?? t[1] ?? NaN),
            size: num(t.size ?? t.fillQuantity ?? t.q ?? t[2] ?? NaN),
            side: String(t.side ?? t.S ?? t[3] ?? '').toLowerCase(),
            ts: Number(t.ts ?? t.tradeTime ?? t[0] ?? Date.now()),
        }))
        .filter((t: any) => Number.isFinite(t.price) && Number.isFinite(t.size) && t.size > 0);

    const bestBid = num(bundle.orderbook?.bids?.[0]?.[0] ?? bundle.orderbook?.bids?.[0]?.price);
    const bestAsk = num(bundle.orderbook?.asks?.[0]?.[0] ?? bundle.orderbook?.asks?.[0]?.price);
    const spread = bestAsk > 0 && bestBid > 0 ? bestAsk - bestBid : 0;
    const mid = bestAsk > 0 && bestBid > 0 ? (bestAsk + bestBid) / 2 : 0;

    let lastPrice = normTrades[0]?.price || 0;
    const enriched = normTrades.map((tr: any) => {
        let dir = tr.side;
        if (!dir || dir === '') {
            if (mid > 0) dir = tr.price >= mid ? 'buy' : 'sell';
            else dir = tr.price >= lastPrice ? 'buy' : 'sell';
        }
        lastPrice = tr.price;
        return { ...tr, dir };
    });

    const cvd = enriched.reduce((acc: number, t: any) => acc + (t.dir === 'buy' ? t.size : -t.size), 0);
    const buys = enriched.filter((t: any) => t.dir === 'buy').reduce((a: number, t: any) => a + t.size, 0);
    const sells = enriched.filter((t: any) => t.dir === 'sell').reduce((a: number, t: any) => a + t.size, 0);

    const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
    const last = num(t?.lastPr ?? t?.last ?? t?.close ?? lastPrice) || 0;

    const pct = Math.max(0.0005, spread && last ? (spread / last) * 3 : 0.0005);
    const bins = new Map<number, number>();
    for (const tr of enriched) {
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

    const sumTop = (lvls: OBLevel[], n: number): number =>
        (lvls || []).slice(0, n).reduce((acc: number, l: OBLevel) => acc + l.size, 0);

    const topBid = sumTop(bids, 5);
    const topAsk = sumTop(asks, 5);
    const obImb = topBid + topAsk > 0 ? (topBid - topAsk) / (topBid + topAsk) : 0;

    return { cvd, buys, sells, volume_profile, topWalls, obImb, spread, last, bestBid, bestAsk };
}
