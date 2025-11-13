// lib/analytics.ts

import { bitgetFetch, resolveProductType } from './bitget';
import type { ProductType } from './bitget';
import { TRADE_WINDOW_MINUTES } from './constants';

// ------------------------------
// Types
// ------------------------------

export interface SymbolMeta {
  symbol: string;
  pricePlace: number;   // price decimals
  volumePlace: number;  // size decimals
  minTradeNum: string;  // min size
  sizeMultiplier?: string; // step size (if provided)
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
      currentPnl?: string; // "%", e.g., "1.23%"
    };

// ------------------------------
// Helpers
// ------------------------------

function num(x: any, def = 0): number {
  const n = Number(x);
  return Number.isFinite(n) ? n : def;
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
// Order size (step + min) (FUTURES only)
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

  if (!(finalSize > 0)) {
    throw new Error(`Failed to compute valid size (raw=${rawSize}, rounded=${rounded})`);
  }
  return Number(finalSize.toFixed(decimals));
}

// ------------------------------
// Positions (FUTURES only)
// ------------------------------

// ------------------------------
// Positions (FUTURES only)
// ------------------------------

type RawPosition = {
  symbol: string;
  holdSide?: string;        // "long"/"short"
  openPriceAvg: string;
  posMode?: 'one_way_mode' | 'hedge_mode';
  marginCoin?: string;
  available?: string;
  total?: string;           // base size
  markPrice?: string;
  leverage?: string | number;
  unrealizedPL?: string | number;
};



export async function fetchPositionInfo(symbol: string): Promise<PositionInfo> {
  const productType = resolveProductType();
  const positions: RawPosition[] =
    await bitgetFetch('GET', '/api/v2/mix/position/all-position', { productType });

  const matches = (positions || []).filter((p) => p.symbol === symbol);
  if (!matches.length) return { status: 'none' };

  // choose the largest absolute position (by total or available) safely
  const chosen: RawPosition = matches
    .slice()
    .sort((a: RawPosition, b: RawPosition) => {
      const bSize = Math.abs(num(b.total ?? b.available ?? '0'));
      const aSize = Math.abs(num(a.total ?? a.available ?? '0'));
      return bSize - aSize;
    })[0];

  return {
    status: 'open',
    symbol,
    holdSide: (chosen.holdSide ?? '').toLowerCase() as 'long' | 'short',
    entryPrice: chosen.openPriceAvg,
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
// Trades (FUTURES only)
// ------------------------------

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

// ------------------------------
// Market bundle (FUTURES only)
// ------------------------------

export async function fetchMarketBundle(symbol: string, bundleTimeFrame: string) {
  const productType = resolveProductType(); // e.g., 'USDT-FUTURES'

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

// ------------------------------
// Analytics: CVD, VP, liquidity
// ------------------------------

export function computeAnalytics(bundle: any) {
  // normalize trades
  const normTrades = (bundle.trades || []).map((t: any) => ({
    price: num(t.price ?? t.fillPrice ?? t.p ?? (t[1] ?? NaN)),
    size: num(t.size ?? t.fillQuantity ?? t.q ?? (t[2] ?? NaN)),
    side: String(t.side ?? t.S ?? t[3] ?? '').toLowerCase(),
    ts: Number(t.ts ?? t.tradeTime ?? t[0] ?? Date.now()),
  })).filter((t: any) => Number.isFinite(t.price) && Number.isFinite(t.size) && t.size > 0);

  // best bid/ask & mid
  const bestBid = num(bundle.orderbook?.bids?.[0]?.[0] ?? bundle.orderbook?.bids?.[0]?.price);
  const bestAsk = num(bundle.orderbook?.asks?.[0]?.[0] ?? bundle.orderbook?.asks?.[0]?.price);
  const spread = bestAsk > 0 && bestBid > 0 ? bestAsk - bestBid : 0;
  const mid = bestAsk > 0 && bestBid > 0 ? (bestAsk + bestBid) / 2 : 0;

  // mid-based direction (fallback to tick rule)
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

  // CVD / flow
  const cvd = enriched.reduce((acc: number, t: any) => acc + (t.dir === 'buy' ? t.size : -t.size), 0);
  const buys = enriched.filter((t: any) => t.dir === 'buy').reduce((a: number, t: any) => a + t.size, 0);
  const sells = enriched.filter((t: any) => t.dir === 'sell').reduce((a: number, t: any) => a + t.size, 0);

  // last price
  const t = Array.isArray(bundle.ticker) ? bundle.ticker[0] : bundle.ticker;
  const last = num(t?.lastPr ?? t?.last ?? t?.close ?? lastPrice) || 0;

  // volume profile with spread/ATR-aware bin size (use spread*3 or 5 bps min)
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

  // orderbook top walls + imbalance
type OBLevel = { price: number; size: number };

const bids: OBLevel[] = (bundle.orderbook?.bids || []).map((l: any): OBLevel => ({
  price: num(l[0] ?? l.price),
  size: num(l[1] ?? l.size),
}));

const asks: OBLevel[] = (bundle.orderbook?.asks || []).map((l: any): OBLevel => ({
  price: num(l[0] ?? l.price),
  size: num(l[1] ?? l.size),
}));

const topWalls = {
  bid: bids.slice().sort((a: OBLevel, b: OBLevel) => b.size - a.size).slice(0, 5),
  ask: asks.slice().sort((a: OBLevel, b: OBLevel) => b.size - a.size).slice(0, 5),
};

const sumTop = (lvls: OBLevel[], n: number): number =>
  (lvls || []).slice(0, n).reduce((acc: number, l: OBLevel) => acc + l.size, 0);

const topBid = sumTop(bids, 5);
const topAsk = sumTop(asks, 5);
const obImb = topBid + topAsk > 0 ? (topBid - topAsk) / (topBid + topAsk) : 0;

return {
  // flow
  cvd, buys, sells,
  // structure
  volume_profile, topWalls,
  // liquidity & pricing
  obImb, spread, last, bestBid, bestAsk,
};
}
