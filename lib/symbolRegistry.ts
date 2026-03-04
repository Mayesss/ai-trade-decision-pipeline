import vercelConfig from '../vercel.json';
import { resolveAnalysisPlatform, resolveNewsSource, type AnalysisPlatform, type NewsSource } from './platform';
import { resolveSwingCategory } from './swing/category';

type VercelCron = {
  path?: string;
  schedule?: string;
};

export type ScalpCronSymbolConfig = {
  symbol: string;
  strategyId: string | null;
  schedule: string | null;
  path: string;
  route: 'execute' | 'execute-hybrid';
};

export type CronSymbolConfig = {
  symbol: string;
  platform: AnalysisPlatform;
  newsSource: NewsSource;
  category: string | null;
  decisionPolicy: string | null;
  schedule: string | null;
  path: string;
};

function parseCronAnalyzePath(path: string, schedule: string | null): CronSymbolConfig | null {
  const rawPath = String(path || '').trim();
  if (!rawPath) return null;

  let parsed: URL;
  try {
    parsed = new URL(rawPath, 'http://localhost');
  } catch {
    return null;
  }

  const pathname = String(parsed.pathname || '').trim();
  const isAnalyzeRoute = pathname === '/api/analyze' || pathname === '/api/swing/analyze';
  if (!isAnalyzeRoute) return null;

  const symbol = String(parsed.searchParams.get('symbol') || '')
    .trim()
    .toUpperCase();
  if (!symbol) return null;

  const platform = resolveAnalysisPlatform(parsed.searchParams.get('platform'));
  const newsSource = resolveNewsSource(platform, parsed.searchParams.get('newsSource'));
  const category = resolveSwingCategory({
    category: parsed.searchParams.get('category'),
    symbol,
    platform,
  });
  const decisionPolicyRaw = String(parsed.searchParams.get('decisionPolicy') || '').trim();

  return {
    symbol,
    platform,
    newsSource,
    category,
    decisionPolicy: decisionPolicyRaw || null,
    schedule,
    path: rawPath,
  };
}

function normalizeScalpStrategyId(value: unknown): string | null {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]/g, '');
  return normalized || null;
}

function parseScalpCronPath(path: string, schedule: string | null): ScalpCronSymbolConfig | null {
  const rawPath = String(path || '').trim();
  if (!rawPath) return null;

  let parsed: URL;
  try {
    parsed = new URL(rawPath, 'http://localhost');
  } catch {
    return null;
  }

  const pathname = String(parsed.pathname || '').trim();
  const isExecuteRoute = pathname === '/api/scalp/cron/execute';
  const isExecuteHybridRoute = pathname === '/api/scalp/cron/execute-hybrid';
  if (!isExecuteRoute && !isExecuteHybridRoute) return null;

  const symbol = String(parsed.searchParams.get('symbol') || '')
    .trim()
    .toUpperCase();
  if (!symbol) return null;

  return {
    symbol,
    strategyId: normalizeScalpStrategyId(parsed.searchParams.get('strategyId')),
    schedule,
    path: rawPath,
    route: isExecuteHybridRoute ? 'execute-hybrid' : 'execute',
  };
}

export function getCronSymbolConfigs(): CronSymbolConfig[] {
  const crons: VercelCron[] = Array.isArray((vercelConfig as any)?.crons) ? (vercelConfig as any).crons : [];
  const out: CronSymbolConfig[] = [];
  const seen = new Set<string>();

  for (const cron of crons) {
    const schedule = typeof cron?.schedule === 'string' ? cron.schedule : null;
    const path = typeof cron?.path === 'string' ? cron.path : '';
    const parsed = parseCronAnalyzePath(path, schedule);
    if (!parsed) continue;
    if (seen.has(parsed.symbol)) continue;
    seen.add(parsed.symbol);
    out.push(parsed);
  }
  return out;
}

export function getCronSymbols(): string[] {
  return getCronSymbolConfigs().map((item) => item.symbol);
}

export function getScalpCronSymbolConfigs(): ScalpCronSymbolConfig[] {
  const crons: VercelCron[] = Array.isArray((vercelConfig as any)?.crons) ? (vercelConfig as any).crons : [];
  const bySymbol = new Map<string, ScalpCronSymbolConfig>();

  for (const cron of crons) {
    const schedule = typeof cron?.schedule === 'string' ? cron.schedule : null;
    const path = typeof cron?.path === 'string' ? cron.path : '';
    const parsed = parseScalpCronPath(path, schedule);
    if (!parsed) continue;
    const existing = bySymbol.get(parsed.symbol);
    if (!existing) {
      bySymbol.set(parsed.symbol, parsed);
      continue;
    }
    if (!existing.strategyId && parsed.strategyId) {
      bySymbol.set(parsed.symbol, parsed);
    }
  }
  return Array.from(bySymbol.values());
}

export function getScalpCronSymbols(): string[] {
  return getScalpCronSymbolConfigs().map((item) => item.symbol);
}
