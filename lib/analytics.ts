// lib/analytics.ts

import { bitgetFetch, resolveProductType } from './bitget';
import type { ProductType } from './bitget';

import { TRADE_WINDOW_MINUTES } from './constants';

// ---- Types ----

export interface SymbolMeta {
  symbol: string;
  pricePlace: number;
  volumePlace: number;
  minTradeNum: string;
  sizeMultiplier?: string; // step size
}

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

// ---- Helpers ----

export function roundToDecimals(value: number, decimals: number): number {
  const factor = Math.pow(10, decimals);
  return Math.floor(value * factor) / factor;
}

function ensureAscendingCandles(cs: any[]) {
  if (!Array.isArray(cs)) return [];
  // Bitget candles are typically [ts, open, high, low, close, volume] and often latest-first
  const asc = cs.slice().sort((a: any, b: any) => Number(a[0]) - Number(b[0]));
  return asc;
}

function toNum(v: any, def = 0): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

// ---- Fetch symbol meta (FUTURES only) ----

export async function fetchSymbolMeta(symbol: string, productType: ProductType): Promise<SymbolMeta> {
  const pt = (productType as string).toUpperCase();
  const all = await bitgetFetch('GET', '/api/v2/mix/market/contracts', { productType: pt });
  const meta = (all || []).find((x: any) => x.symbol === symbol);
  if (!meta) throw new Error(`No contract metadata for ${symbol}`);
  return meta;
}

// ---- Order size computation (uses minTradeNum + sizeMultiplier step) ----

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
  const priceStr = t?.lastPr ?? t?.last ?? t?.close ?? t?.price;
  const price = parseFloat(priceStr);
  if (!isFinite(price) || price <= 0) throw new Error(`Invalid price for ${symbol}: ${priceStr}`);

  const rawSize = notionalUSDT / price;

  const decimals = Number(meta.volumePlace ?? 3);
  const minTradeNum = parseFloat(meta.minTradeNum ?? '0');
  const step = parseFloat(meta.sizeMultiplier ?? `1e-${decimals}`);

  const quantize = (x: number, s: number) => Math.floor(x / s) * s;

  const rounded = quantize(rawSize, step);
  const finalSize = Math.max(rounded, minTradeNum);

  if (!isFinite(finalSize) || finalSize <= 0) {
    throw new Error(`Failed to compute valid size (raw=${rawSize}, rounded=${rounded})`);
  }

  return Number(finalSize.toFixed(decimals));
}

// ---- Fetch open positions (FUTURES only) ----

export async function fetchPositionInfo(symbol: string): Promise<PositionInfo> {
  const productType = resolveProductType(); // should be e.g., 'USDT-FUTURES'
  const positions = await bitgetFetch('GET', '/api/v2/mix/position/all-position', { productType });

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
  const toN = (v: any) => (Number.isFinite(Number(v)) ? Number(v) : 0);
  const sizeBase = toN(data.total);    // base coin amount
  const mark = Math.max(1e-9, toN(data.markPrice)); // guard zero
  const lev = Math.max(1, toN(data.leverage) || 1);
  const uPnl = toN(data.unrealizedPL); // in margin coin
  const initialMargin = (sizeBase * mark) / lev || 1; // avoid divide by 0
  const pnlPercent = (uPnl / initialMargin) * 100;
  return pnlPercent.toFixed(2) + '%';
}

// ---- Fetch recent trades (FUTURES only) ----

export async function fetchTradesForMinutes(symbol: string, productType: ProductType, minutes: number) {
  const trades: any[] = [];
  const cutoff = Date.now() - minutes * 60_000;
  let lastId: string | undefined = undefined;

  while (true) {
    const params: any = { symbol, productType };
    if (lastId) params.after = lastId;

    const batch = await bitgetFetch('GET', '/api/v2/mix/market/fills', params);
    if (!batch?.length) break;
    trades.push(...batch);

    const lastTradeTs = Number(batch[batch.length - 1].ts || batch[0]?.ts);
    if (lastTradeTs < cutoff) break;

    lastId = batch[batch.length - 1].tradeId || batch[batch.length - 1].id;
    if (!lastId) break;
    if (trades.length > 5000) break;
  }

  return trades.filter((t) => Number(t.ts) >= cutoff);
}

// ---- Market data bundle fetch (FUTURES only) ----

export async function fetchMarketBundle(symbol: string, bundleTimeFrame: string) {
  const productType = resolveProductType(); // futures only

  const tickerRaw = await bitgetFetch('GET', '/api/v2/mix/market/ticker', { symbol, productType });
  const ticker = Array.isArray(tickerRaw) ? tickerRaw[0] : tickerRaw;

  const candlesRaw = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
    symbol,
    productType,
    granularity: bundleTimeFrame,
    limit: 30,
  });
  const candles = ensureAscendingCandles(candlesRaw);

  const minutes = Number(TRADE_WINDOW_MINUTES || 30);
  const trades = await fetchTradesForMinutes(symbol, productType, minutes);

  const orderbook = await bitgetFetch('GET', '/api/v2/mix/market/orderbook', {
    symbol,
    productType,
    limit: 100,
  });

  let funding: any = null,
    oi: any = null;
  try {
    funding = await bitgetFetch('GET', '/api/v2/mix/market/current-fund-rate', { symbol, productType });
  } catch {}
  try {
    oi = await bitgetFetch('GET', '/api/v2/mix/market/open-interest', { symbol, productType });
  } catch {}

  return { ticker, candles, trades, orderbook, funding, oi, productType };
}

// ---- Compute analytics (CVD, volume profile, liquidity) ----

export function computeAnalytics(bundle: any) {
  const normTrades = (bundle.trades || []).map((t: any) => ({
    price: parseFloat(t.price || t.fillPrice || t.p || t[1]),
    size: parseFloat(t.size || t.fillQuantity || t.q || t[2]),
    side: (t.side || t.S || t[3] || '').toString().toLowerCase(),
    ts: Number(t.ts || t.tradeTime || t[0] || Date.now()),
  }));

  // derive mid from current book (approx; better than tick rule only)
  const bestBid = toNum(bundle.orderbook?.bids?.[0]?.[0] ?? bundle.orderbook?.bids?.[0]?.price);
  const bestAsk = toNum(bundle.orderbook?.asks?.[0]?.[0] ?? bundle.orderbook?.asks?.[0]?.price);
  const spread = bestAsk > 0 && bestBid > 0 ? bestAsk - bestBid : 0;
  const lastMid = bestAsk > 0 && bestBid > 0 ? (bestAsk + bestBid) / 2 : 0;

  let lastPrice = normTrades[0]?.price || 0;
  const enriched = normTrades.map((tr: any) => {
    let dir = tr.side;
    if (!dir || dir === '') {
      if (lastMid > 0) dir = tr.price >= lastMid ? 'buy' : 'sell';
      else dir = tr.price >= lastPrice ? 'buy' : 'sell'; // fallback tick rule
    }
    lastPrice = tr.price;
    return { ...tr, dir };
  });

  const cvd = enriched.reduce((acc: number, t: any) => acc + (t.dir === 'buy' ? t.size : -t.size), 0);
  const buys = enriched.filter((t: any) => t.dir === 'buy').reduce((a: number, t: any) => a + t.size, 0);
  const sells = enriched.filter((t: any) => t.dir === 'sell').reduce((a: number, t: any) => a + t.size, 0);

  const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
  const last = Number(t?.lastPr ?? t?.last ?? t?.close ?? lastPrice) || 0;

  // bin width tied to spread (or 5 bps floor)
  const pct = Math.max(0.0005, (spread && last ? (spread / last) * 3 : 0.0005));
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

  const bids = (bundle.orderbook?.bids || []).map((l: any) => ({
    price: parseFloat(l[0] || l.price),
    size: parseFloat(l[1] || l.size),
  }));
  const asks = (bundle.orderbook?.asks || []).map((l: any) => ({
    price: parseFloat(l[0] || l.price),
    size: parseFloat(l[1] || l.size),
  }));

  const topWalls = {
    bid: bids.slice().sort((a: any, b: any) => b.size - a.size).slice(0, 5),
    ask: asks.slice().sort((a: any, b: any) => b.size - a.size).slice(0, 5),
  };

  // Simple book imbalance [-1,1]
  const sumTop = (lvls: any[], n: number) => (lvls || []).slice(0, n).reduce((a, l) => a + (Number(l.size) || 0), 0);
  const topBid = sumTop(bids, 5);
  const topAsk = sumTop(asks, 5);
  const obImb = topBid + topAsk > 0 ? (topBid - topAsk) / (topBid + topAsk) : 0;

  return { cvd, buys, sells, volume_profile, topWalls, obImb, spread, last };
}
