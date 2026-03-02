import type { NextApiRequest, NextApiResponse } from 'next';

import defaultTickerEpicMap from '../../../../data/capitalTickerMap.json';
import { requireAdminAccess } from '../../../../lib/admin';

type AssetRow = {
  symbol: string;
  epic: string;
  category: 'forex' | 'crypto' | 'index' | 'commodity' | 'equity' | 'other';
};

const FX_CODES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD'];
const CRYPTO_CODES = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOGE', 'AVAX', 'MATIC', 'DOT', 'LTC'];
const INDEX_EPICS = ['US500', 'NAS100', 'US30', 'GER40', 'UK100', 'FRA40', 'JPN225', 'QQQ'];
const COMMODITY_EPICS = ['XAUUSD', 'XAGUSD', 'USOIL', 'UKOIL', 'NGAS'];

function parseEnvTickerMap(): Record<string, string> {
  const raw = process.env.CAPITAL_TICKER_EPIC_MAP;
  if (!raw) return {};
  try {
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return {};
    const out: Record<string, string> = {};
    for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
      if (typeof v !== 'string') continue;
      const symbol = String(k || '').trim().toUpperCase();
      const epic = String(v || '').trim().toUpperCase();
      if (!symbol || !epic) continue;
      out[symbol] = epic;
    }
    return out;
  } catch {
    return {};
  }
}

function inferCategory(symbol: string, epic: string): AssetRow['category'] {
  const upperSymbol = symbol.toUpperCase();
  const upperEpic = epic.toUpperCase();

  if (/^[A-Z]{6}$/.test(upperSymbol)) {
    const lhs = upperSymbol.slice(0, 3);
    const rhs = upperSymbol.slice(3, 6);
    if (FX_CODES.includes(lhs) && FX_CODES.includes(rhs)) return 'forex';
  }

  const cryptoBase = upperSymbol.replace(/USDT$|USDC$|USD$/g, '');
  if (CRYPTO_CODES.includes(cryptoBase)) return 'crypto';
  if (INDEX_EPICS.includes(upperEpic)) return 'index';
  if (COMMODITY_EPICS.includes(upperEpic)) return 'commodity';
  if (upperSymbol.endsWith('USDT')) return 'equity';
  return 'other';
}

function categoryRank(category: AssetRow['category']): number {
  if (category === 'forex') return 1;
  if (category === 'index') return 2;
  if (category === 'commodity') return 3;
  if (category === 'equity') return 4;
  if (category === 'crypto') return 5;
  return 6;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;

  const defaultMap = defaultTickerEpicMap as Record<string, string>;
  const envMap = parseEnvTickerMap();
  const merged = { ...defaultMap, ...envMap };

  const assets: AssetRow[] = Object.entries(merged)
    .map(([symbol, epic]) => {
      const normalizedSymbol = String(symbol || '').trim().toUpperCase();
      const normalizedEpic = String(epic || '').trim().toUpperCase();
      if (!normalizedSymbol || !normalizedEpic) return null;
      return {
        symbol: normalizedSymbol,
        epic: normalizedEpic,
        category: inferCategory(normalizedSymbol, normalizedEpic),
      } satisfies AssetRow;
    })
    .filter((row): row is AssetRow => Boolean(row))
    .sort((a, b) => {
      const byCategory = categoryRank(a.category) - categoryRank(b.category);
      if (byCategory !== 0) return byCategory;
      return a.symbol.localeCompare(b.symbol);
    });

  return res.status(200).json({
    count: assets.length,
    assets,
  });
}

