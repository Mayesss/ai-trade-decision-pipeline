import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchBitgetLiveQuote } from '../../../lib/analytics';
import { requireAdminAccess } from '../../../lib/admin';
import { fetchCapitalLivePrice } from '../../../lib/capital';
import { resolveAnalysisPlatform, type AnalysisPlatform } from '../../../lib/platform';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';

// A dashboard tab polls this every 3s. One quote per symbol per poll, so the
// list is bounded — the dashboard only ever asks for the viewed symbol plus the
// symbols holding an open position.
const MAX_SYMBOLS = 12;

type Quote = {
  symbol: string;
  platform: AnalysisPlatform;
  instrumentId: string | null;
  price: number;
  bid: number | null;
  offer: number | null;
  ts: number;
  source: string;
  mappingSource?: string | null;
};

function resolveRequestedPlatform(symbol: string, requested?: string | null): AnalysisPlatform {
  const normalizedRequest = String(requested || '').trim();
  if (normalizedRequest) return resolveAnalysisPlatform(normalizedRequest);
  const fromCron = getCronSymbolConfigs().find((item) => item.symbol === symbol);
  return fromCron?.platform ?? 'bitget';
}

async function quoteFor(symbol: string, platform: AnalysisPlatform): Promise<Quote> {
  if (platform === 'capital') {
    const quote = await fetchCapitalLivePrice(symbol);
    return {
      symbol,
      platform: 'capital',
      instrumentId: quote.epic,
      price: quote.price,
      bid: quote.bid,
      offer: quote.offer,
      ts: quote.ts,
      source: 'capital-market-snapshot',
      mappingSource: quote.mappingSource,
    };
  }
  const { price, ts } = await fetchBitgetLiveQuote(symbol);
  return {
    symbol,
    platform: 'bitget',
    instrumentId: symbol,
    price,
    bid: null,
    offer: null,
    ts,
    source: 'bitget-rest-ticker',
  };
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;

  const first = (value: string | string[] | undefined) => (Array.isArray(value) ? value[0] : value);
  const symbol = String(first(req.query.symbol) || '')
    .trim()
    .toUpperCase();
  // `symbols` (comma-separated) returns a quote array; `symbol` keeps the
  // single-object shape the chart's live candle already reads.
  const requestedList = String(first(req.query.symbols) || '')
    .split(',')
    .map((value) => value.trim().toUpperCase())
    .filter(Boolean);
  const symbols = Array.from(new Set(requestedList.length ? requestedList : symbol ? [symbol] : [])).slice(
    0,
    MAX_SYMBOLS,
  );
  if (!symbols.length) {
    return res.status(400).json({ error: 'symbol_required' });
  }

  const platformParam = first(req.query.platform);
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');

  // Per-symbol settle: one instrument's venue hiccup must not blank the rest of
  // the poll (the dashboard just keeps the previous quote for that pill).
  const settled = await Promise.allSettled(
    symbols.map((item) =>
      quoteFor(item, resolveRequestedPlatform(item, symbols.length === 1 ? platformParam ?? null : null)),
    ),
  );
  const quotes: Quote[] = [];
  settled.forEach((result, idx) => {
    if (result.status === 'fulfilled') {
      quotes.push(result.value);
      return;
    }
    console.warn(`live-price failed for ${symbols[idx]}:`, result.reason);
  });

  if (!requestedList.length) {
    // Single-symbol form: unchanged response, including the 500 on failure.
    const quote = quotes[0];
    if (!quote) {
      const reason = settled[0].status === 'rejected' ? settled[0].reason : null;
      console.error(`Error in /api/dashboard/live-price (${symbols[0]}):`, reason);
      return res
        .status(500)
        .json({ error: reason instanceof Error && reason.message ? reason.message : 'live_price_fetch_failed' });
    }
    return res.status(200).json(quote);
  }

  return res.status(200).json({ quotes });
}
