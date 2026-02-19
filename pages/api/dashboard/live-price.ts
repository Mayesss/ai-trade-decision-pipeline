import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchMarketBundle as fetchBitgetMarketBundle } from '../../../lib/analytics';
import { requireAdminAccess } from '../../../lib/admin';
import { fetchCapitalLivePrice } from '../../../lib/capital';
import { resolveAnalysisPlatform, type AnalysisPlatform } from '../../../lib/platform';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';

function resolveRequestedPlatform(symbol: string, requested?: string | null): AnalysisPlatform {
  const normalizedRequest = String(requested || '').trim();
  if (normalizedRequest) return resolveAnalysisPlatform(normalizedRequest);
  const fromCron = getCronSymbolConfigs().find((item) => item.symbol === symbol);
  return fromCron?.platform ?? 'bitget';
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;

  const symbol = String(req.query.symbol || '')
    .trim()
    .toUpperCase();
  if (!symbol) {
    return res.status(400).json({ error: 'symbol_required' });
  }

  const platformParam = Array.isArray(req.query.platform) ? req.query.platform[0] : req.query.platform;
  const platform = resolveRequestedPlatform(symbol, platformParam ?? null);

  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  try {
    if (platform === 'capital') {
      const quote = await fetchCapitalLivePrice(symbol);
      return res.status(200).json({
        symbol,
        platform: 'capital',
        instrumentId: quote.epic,
        price: quote.price,
        bid: quote.bid,
        offer: quote.offer,
        ts: quote.ts,
        source: 'capital-market-snapshot',
        mappingSource: quote.mappingSource,
      });
    }

    const bundle = await fetchBitgetMarketBundle(symbol, '1m', { includeTrades: false, candleLimit: 3 });
    const ticker = Array.isArray(bundle?.ticker) ? bundle.ticker[0] : bundle?.ticker;
    const price = Number(ticker?.lastPr ?? ticker?.last ?? ticker?.close ?? ticker?.price);
    if (!(Number.isFinite(price) && price > 0)) {
      throw new Error(`Bitget live quote unavailable for ${symbol}`);
    }
    const ts = Number(ticker?.ts ?? ticker?.timestamp ?? Date.now());
    return res.status(200).json({
      symbol,
      platform: 'bitget',
      instrumentId: symbol,
      price,
      bid: null,
      offer: null,
      ts: Number.isFinite(ts) ? ts : Date.now(),
      source: 'bitget-rest-ticker',
    });
  } catch (err: any) {
    console.error(`Error in /api/dashboard/live-price (${platform}:${symbol}):`, err);
    return res.status(500).json({ error: err?.message || 'live_price_fetch_failed' });
  }
}
