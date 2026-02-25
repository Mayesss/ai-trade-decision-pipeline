import forexUniverse from '../../data/forexUniverse.json';
import type { AnalysisPlatform } from '../platform';

const MAJOR_FX_CURRENCIES = new Set(['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD']);
const INDEX_HINTS = ['US500', 'SPX', 'NAS100', 'DJ30', 'GER40', 'UK100', 'JP225', 'FR40', 'EU50'];
const COMMODITY_HINTS = ['GOLD', 'SILVER', 'XAU', 'XAG', 'WTI', 'BRENT', 'NGAS', 'NATGAS', 'OIL'];
const EQUITY_HINTS = ['AAPL', 'TSLA', 'MSFT', 'NVDA', 'QQQ', 'SPY'];

const FOREX_UNIVERSE = new Set(
  Array.isArray((forexUniverse as any)?.pairs)
    ? (forexUniverse as any).pairs.map((pair: unknown) => String(pair).trim().toUpperCase())
    : [],
);

function normalizeCategoryValue(value: string | null | undefined): string | null {
  const raw = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '_')
    .replace(/[^a-z0-9_-]/g, '');
  return raw || null;
}

function normalizeAlpha(value: string): string {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z]/g, '');
}

function findMajorForexPair(value: string): string | null {
  const normalized = normalizeAlpha(value);
  if (normalized.length < 6) return null;
  for (let i = 0; i <= normalized.length - 6; i += 1) {
    const candidate = normalized.slice(i, i + 6);
    const base = candidate.slice(0, 3);
    const quote = candidate.slice(3, 6);
    if (MAJOR_FX_CURRENCIES.has(base) && MAJOR_FX_CURRENCIES.has(quote) && base !== quote) {
      return candidate;
    }
  }
  return null;
}

function includesHint(value: string, hints: string[]): boolean {
  const normalized = String(value || '').trim().toUpperCase();
  if (!normalized) return false;
  return hints.some((hint) => normalized.includes(hint));
}

function inferSwingCategory(symbol: string, platform: AnalysisPlatform, instrumentId?: string | null): string | null {
  const symbolNormalized = String(symbol || '').trim().toUpperCase();
  const instrumentNormalized = String(instrumentId || '').trim().toUpperCase();

  if (platform === 'bitget') return 'crypto';

  const directForexCandidate = [symbolNormalized, instrumentNormalized]
    .map((value) => value.replace(/^CAPITAL:/, ''))
    .find((value) => FOREX_UNIVERSE.has(value) || Boolean(findMajorForexPair(value)));
  if (directForexCandidate) return 'forex';

  if (includesHint(symbolNormalized, INDEX_HINTS) || includesHint(instrumentNormalized, INDEX_HINTS)) {
    return 'index';
  }
  if (includesHint(symbolNormalized, COMMODITY_HINTS) || includesHint(instrumentNormalized, COMMODITY_HINTS)) {
    return 'commodity';
  }
  if (includesHint(symbolNormalized, EQUITY_HINTS) || includesHint(instrumentNormalized, EQUITY_HINTS)) {
    return 'equity';
  }
  return null;
}

export function resolveSwingCategory(params: {
  category?: string | null;
  symbol: string;
  platform: AnalysisPlatform;
  instrumentId?: string | null;
}): string | null {
  const explicit = normalizeCategoryValue(params.category);
  if (explicit) return explicit;
  return inferSwingCategory(params.symbol, params.platform, params.instrumentId);
}

