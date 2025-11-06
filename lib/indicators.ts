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

export function computeRSI(candles: any[], period = 14): number {
    const closes = candles.map((c) => parseFloat(c[4]));
    if (closes.length <= period) return 50;

    let gains = 0,
        losses = 0;
    for (let i = closes.length - period - 1; i < closes.length - 1; i++) {
        if (closes[i + 1] === undefined || closes[i] === undefined) continue;
        const diff = closes[i + 1]! - closes[i]!;
        if (diff > 0) gains += diff;
        else losses -= diff;
    }
    const avgGain = gains / period;
    const avgLoss = losses / period;
    if (avgLoss === 0) return 100;
    const rs = avgGain / avgLoss;
    return 100 - 100 / (1 + rs);
}

export function computeEMA(closes: number[], period: number): number[] {
  const k = 2 / (period + 1);
  const ema: number[] = [];

  if (closes.length === 0) return ema;

  ema[0] = closes[0]!;

  for (let i = 1; i < closes.length; i++) {
    const prevEma = ema[i - 1];
    const close = closes[i];
    if (prevEma === undefined || close === undefined) continue;
    ema[i] = close * k + prevEma * (1 - k);
  }

  return ema;
}

// ------------------------------
// Multi-Timeframe Indicators
// ------------------------------

export async function calculateMultiTFIndicators(symbol: string): Promise<{ micro: string; macro: string }> {
    const productType = resolveProductType();
    const isFutures = productType.endsWith('futures');

    async function fetchCandles(tf: string) {
        return isFutures
            ? await bitgetFetch('GET', '/api/v2/mix/market/candles', {
                  symbol,
                  productType,
                  granularity: tf,
                  limit: 30,
              })
            : await bitgetFetch('GET', '/api/v2/spot/market/candles', {
                  symbol,
                  granularity: tf,
                  limit: 30,
              });
    }

    const [microCandles, macroCandles] = await Promise.all([
        fetchCandles('1m'), // micro = short-term (scalp)
        fetchCandles('1H'), // macro = higher trend
    ]);

    const build = (candles: any[]) => {
        const vwap = computeVWAP(candles);
        const rsi = computeRSI(candles);
        const closes = candles.map((c) => parseFloat(c[4]));
        const ema20 = computeEMA(closes, 20);
        const ema50 = computeEMA(closes, 50);
        const trend = ema20.at(-1)! > ema50.at(-1)! ? 'up' : 'down';
        return `VWAP=${vwap.toFixed(2)}, RSI=${rsi.toFixed(1)}, trend=${trend}`;
    };

    return {
        micro: build(microCandles),
        macro: build(macroCandles),
    };
}
