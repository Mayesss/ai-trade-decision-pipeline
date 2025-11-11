// lib/indicators.ts

import { bitgetFetch, resolveProductType } from './bitget';

// ------------------------------
// Indicator Calculations
// ------------------------------

export function computeVWAP(candles: any[]): number {
  let cumPV = 0,
    cumVol = 0;
  for (const c of candles) {
    const high = parseFloat(c[2]);
    const low = parseFloat(c[3]);
    const close = parseFloat(c[4]);
    const volume = parseFloat(c[5]);
    const typical = (high + low + close) / 3;
    cumPV += typical * volume;
    cumVol += volume;
  }
  return cumVol > 0 ? cumPV / cumVol : 0;
}

// Wilder RSI (more standard)
export function computeRSI_Wilder(closes: number[], period = 14): number {
  if (closes.length <= period) return 50;
  let gains = 0,
    losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i]! - closes[i - 1]!;
    if (diff > 0) gains += diff;
    else losses -= diff;
  }
  let avgGain = gains / period;
  let avgLoss = losses / period;

  for (let i = period + 1; i < closes.length; i++) {
    const diff = closes[i]! - closes[i - 1]!;
    avgGain = (avgGain * (period - 1) + (diff > 0 ? diff : 0)) / period;
    avgLoss = (avgLoss * (period - 1) + (diff < 0 ? -diff : 0)) / period;
  }
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

// EMA with SMA seed
export function computeEMA(closes: number[], period: number): number[] {
  const ema: number[] = [];
  if (closes.length === 0) return ema;
  if (closes.length < period) {
    // seed with first close if too short
    ema[0] = closes[0]!;
  } else {
    const sma = closes.slice(0, period).reduce((a, b) => a + b, 0) / period;
    ema[period - 1] = sma;
  }
  const k = 2 / (period + 1);
  for (let i = (ema[0] !== undefined ? 1 : period); i < closes.length; i++) {
    const prev = ema[i - 1] ?? closes[i - 1];
    ema[i] = closes[i]! * k + prev * (1 - k);
  }
  return ema;
}

// ATR (simple version)
export function computeATR(candles: any[], period = 14): number {
  if (!candles || candles.length < period + 1) return 0;
  const trs: number[] = [];
  for (let i = 1; i < candles.length; i++) {
    const high = Number(candles[i][2]);
    const low = Number(candles[i][3]);
    const prevClose = Number(candles[i - 1][4]);
    const tr = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));
    trs.push(tr);
  }
  if (trs.length < period) return 0;
  return trs.slice(-period).reduce((a, b) => a + b, 0) / period;
}

function ensureAscending(cs: any[]) {
  return cs.slice().sort((a: any, b: any) => Number(a[0]) - Number(b[0]));
}

// ------------------------------
// Multi-Timeframe Indicators (FUTURES only)
// ------------------------------

export async function calculateMultiTFIndicators(symbol: string): Promise<{ micro: string; macro: string }> {
  const productType = resolveProductType(); // futures only

  async function fetchCandles(tf: string) {
    const cs = await bitgetFetch('GET', '/api/v2/mix/market/candles', {
      symbol,
      productType,
      granularity: tf,
      limit: 30,
    });
    return ensureAscending(cs);
  }

  const [microCandles, macroCandles] = await Promise.all([
    fetchCandles('1m'), // micro
    fetchCandles('1H'), // macro
  ]);

  const build = (candles: any[]) => {
    const closes = candles.map((c) => parseFloat(c[4]));
    const vwap = computeVWAP(candles);
    const rsi = computeRSI_Wilder(closes, 14);

    const ema20 = computeEMA(closes, 20);
    const ema50 = computeEMA(closes, 50);
    const e20 = ema20.at(-1)! ?? closes.at(-1)!;
    const e50 = ema50.at(-1)! ?? closes.at(-1)!;
    const trend = e20 > e50 ? 'up' : 'down';

    const atr = computeATR(candles, 14);

    return `VWAP=${vwap.toFixed(2)}, RSI=${rsi.toFixed(1)}, trend=${trend}, ATR=${atr.toFixed(2)}`;
  };

  return {
    micro: build(microCandles),
    macro: build(macroCandles),
  };
}
