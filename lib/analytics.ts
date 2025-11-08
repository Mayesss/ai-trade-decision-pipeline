// lib/analytics.ts

import { bitgetFetch, resolveProductType } from './bitget';
import type { ProductType } from './bitget';

import { TRADE_WINDOW_MINUTES } from './constants';

// ---- Fetch symbol meta ----

export interface SymbolMeta {
    symbol: string;
    pricePlace: number;
    volumePlace: number;
    minTradeNum: string;
    sizeMultiplier?: string;
}

export async function fetchSymbolMeta(symbol: string, productType: ProductType): Promise<SymbolMeta> {
    const pt = (productType as string).toUpperCase();
    const all = await bitgetFetch('GET', '/api/v2/mix/market/contracts', { productType: pt });
    const meta = (all || []).find((x: any) => x.symbol === symbol);
    if (!meta) throw new Error(`No contract metadata for ${symbol}`);
    return meta;
}

// ---- Helper: round decimals ----

export function roundToDecimals(value: number, decimals: number): number {
    const factor = Math.pow(10, decimals);
    return Math.floor(value * factor) / factor;
}

// ---- Order size computation ----

export async function computeOrderSize(
    symbol: string,
    notionalUSDT: number,
    productType: ProductType,
): Promise<number> {
    const pt = (productType as string).toUpperCase();
    const all = await bitgetFetch('GET', '/api/v2/mix/market/contracts', { productType: pt });
    const meta = (all || []).find((x: any) => x.symbol === symbol);
    if (!meta) throw new Error(`No contract metadata for ${symbol}`);

    const ticker = await bitgetFetch('GET', '/api/v2/mix/market/ticker', {
        symbol,
        productType: pt,
    });

    const t = Array.isArray(ticker) ? ticker[0] : ticker;
    const priceStr = t?.lastPr ?? t?.last ?? t?.close ?? t?.price;
    const price = parseFloat(priceStr);
    if (!isFinite(price) || price <= 0) throw new Error(`Invalid price for ${symbol}: ${priceStr}`);

    const rawSize = notionalUSDT / price;
    const decimals = Number(meta.volumePlace ?? 3);
    const minTradeNum = parseFloat(meta.minTradeNum ?? '0');

    const factor = Math.pow(10, decimals);
    const rounded = Math.floor(rawSize * factor) / factor;

    const finalSize = Math.max(rounded, minTradeNum);
    if (!isFinite(finalSize) || finalSize <= 0)
        throw new Error(`Failed to compute valid size (raw=${rawSize}, rounded=${rounded})`);

    return finalSize;
}

// ---- Fetch open positions ----

export type PositionInfo =
    | { status: 'none' }
    | {
          status: 'open';
          symbol: string;
          holdSide: 'long' | 'short';
          entryPrice: string;
          posMode?: 'one_way_mode' | 'hedge_mode';
          marginCoin?: string;
          available?: string;
          total?: string;
          currentPnl?: string;
      };

export async function fetchPositionInfo(symbol: string): Promise<PositionInfo> {
    const productType = resolveProductType();
    const positions = await bitgetFetch('GET', '/api/v2/mix/position/all-position', {
        productType,
    });

    const matches = (positions || []).filter((p: any) => p.symbol === symbol);
    if (!matches.length) return { status: 'none' };

    const chosen = matches
        .slice()
        .sort(
            (a: any, b: any) =>
                Math.abs(parseFloat(b.total || b.available || '0')) -
                Math.abs(parseFloat(a.total || a.available || '0')),
        )[0];

    return {
        status: 'open',
        symbol,
        holdSide: (chosen.holdSide || '').toLowerCase() as 'long' | 'short',
        entryPrice: chosen.openPriceAvg,
        posMode: chosen.posMode,
        marginCoin: chosen.marginCoin,
        available: chosen.available,
        total: chosen.total,
        currentPnl: calculatePnLPercent(chosen),
    };
}

function calculatePnLPercent(data: any): string {
    const toNum = (v: any) => {
        const n = Number(v);
        return Number.isFinite(n) ? n : 0;
    };
    const sizeBase = toNum(data.total); // base coin amount in the position
    const mark = toNum(data.markPrice); // quote per base
    const lev = toNum(data.leverage);
    const uPnl = toNum(data.unrealizedPL); // in margin coin terms
    const initialMargin = (sizeBase * mark) / lev;
    const pnlPercent = uPnl / initialMargin * 100;
    return pnlPercent.toFixed(2) + '%';
}
// ---- Fetch recent trades ----

export async function fetchTradesForMinutes(symbol: string, productType: ProductType, minutes: number) {
    const trades: any[] = [];
    const cutoff = Date.now() - minutes * 60_000;
    let lastId: string | undefined = undefined;
    const isFutures = productType.endsWith('futures');

    while (true) {
        const params: any = { symbol };
        if (isFutures) params.productType = productType;
        if (lastId) params.after = lastId;

        const batch = isFutures
            ? await bitgetFetch('GET', '/api/v2/mix/market/fills', params)
            : await bitgetFetch('GET', '/api/v2/spot/market/fills', params);

        if (!batch.length) break;
        trades.push(...batch);

        const lastTradeTs = Number(batch[batch.length - 1].ts || batch[0]?.ts);
        if (lastTradeTs < cutoff) break;

        lastId = batch[batch.length - 1].tradeId || batch[batch.length - 1].id;
        if (!lastId) break;
        if (trades.length > 5000) break;
    }

    return trades.filter((t) => Number(t.ts) >= cutoff);
}

// ---- Market data bundle fetch ----

export async function fetchMarketBundle(symbol: string, bundleTimeFrame: string) {
    const productType = resolveProductType();
    const isFutures = productType.endsWith('futures');

    let ticker;
    if (isFutures) {
        ticker = await bitgetFetch('GET', '/api/v2/mix/market/ticker', { symbol, productType });
    } else {
        const t = await bitgetFetch('GET', '/api/v2/spot/market/tickers', { symbol });
        ticker = Array.isArray(t) ? t[0] : t;
    }

    const candles = isFutures
        ? await bitgetFetch('GET', '/api/v2/mix/market/candles', {
              symbol,
              productType,
              granularity: bundleTimeFrame,
              limit: 30,
          })
        : await bitgetFetch('GET', '/api/v2/spot/market/candles', {
              symbol,
              granularity: bundleTimeFrame,
              limit: 30,
          });

    const minutes = Number(TRADE_WINDOW_MINUTES || 30);
    const trades = await fetchTradesForMinutes(symbol, productType, minutes);

    const orderbook = await bitgetFetch('GET', '/api/v2/spot/market/orderbook', {
        symbol,
        type: 'step0',
        limit: 100,
    });

    let funding: any = null,
        oi: any = null;
    if (isFutures) {
        try {
            funding = await bitgetFetch('GET', '/api/v2/mix/market/current-fund-rate', { symbol, productType });
        } catch {}
        try {
            oi = await bitgetFetch('GET', '/api/v2/mix/market/open-interest', { symbol, productType });
        } catch {}
    }

    return { ticker, candles, trades, orderbook, funding, oi, productType };
}

// ---- Compute analytics (CVD, volume, liquidity) ----

export function computeAnalytics(bundle: any) {
    const normTrades = (bundle.trades || []).map((t: any) => ({
        price: parseFloat(t.price || t.fillPrice || t.p || t[1]),
        size: parseFloat(t.size || t.fillQuantity || t.q || t[2]),
        side: (t.side || t.S || t[3] || '').toString().toLowerCase(),
        ts: Number(t.ts || t.tradeTime || t[0] || Date.now()),
    }));

    let lastPrice = normTrades[0]?.price || 0;
    const enriched = normTrades.map((tr: any) => {
        let dir = tr.side;
        if (!dir || dir === '') dir = tr.price >= lastPrice ? 'buy' : 'sell';
        lastPrice = tr.price;
        return { ...tr, dir };
    });

    const cvd = enriched.reduce((acc: number, t: any) => acc + (t.dir === 'buy' ? t.size : -t.size), 0);
    const buys = enriched.filter((t: any) => t.dir === 'buy').reduce((a: number, t: any) => a + t.size, 0);
    const sells = enriched.filter((t: any) => t.dir === 'sell').reduce((a: number, t: any) => a + t.size, 0);

    const last = Number(bundle.ticker?.lastPr || bundle.ticker?.last || bundle.ticker?.close || enriched.at(-1)?.price);
    const binPct = 0.005;
    const bins = new Map<number, number>();
    for (const t of enriched) {
        const bin = Math.round((t.price - last) / (last * binPct));
        bins.set(bin, (bins.get(bin) || 0) + t.size);
    }

    const volume_profile = Array.from(bins.entries())
        .sort((a, b) => a[0] - b[0])
        .map(([bin, vol]) => ({
            bin,
            price: last + bin * last * binPct,
            volume: Number(vol.toFixed(6)),
        }));

    const bids = (bundle.orderbook?.bids || []).map((l: any) => ({
        price: parseFloat(l[0] || l.price),
        size: parseFloat(l[1] || l.size),
    }));
    const asks = (bundle.orderbook?.asks || []).map((l: any) => ({
        price: parseFloat(l[0] || l.price),
        size: parseFloat(l[1] || l.size),
    }));

    const topWalls = {
        bid: bids.sort((a: any, b: any) => b.size - a.size).slice(0, 5),
        ask: asks.sort((a: any, b: any) => b.size - a.size).slice(0, 5),
    };

    return { cvd, buys, sells, volume_profile, topWalls };
}
