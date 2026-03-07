import vercelConfig from '../vercel.json';
import { resolveAnalysisPlatform, resolveNewsSource, type AnalysisPlatform, type NewsSource } from './platform';
import { buildScalpDeploymentId, normalizeScalpTuneId } from './scalp/deployments';
import { resolveSwingCategory } from './swing/category';

type VercelCron = {
  path?: string;
  schedule?: string;
};

export type ScalpCronSymbolConfig = {
  symbol: string;
  strategyId: string | null;
  tuneId: string | null;
  deploymentId: string | null;
  schedule: string | null;
  path: string;
  route: 'execute-deployments';
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
  const isExecuteDeploymentsRoute = pathname === '/api/scalp/cron/execute-deployments';
  if (!isExecuteDeploymentsRoute) return null;

  const explicitSymbol = String(parsed.searchParams.get('symbol') || '')
    .trim()
    .toUpperCase();
  const all = String(parsed.searchParams.get('all') || '')
    .trim()
    .toLowerCase();
  const symbol = explicitSymbol || (['1', 'true', 'yes', 'on'].includes(all) ? '*' : '');
  if (!symbol) return null;

  return {
    symbol,
    strategyId: normalizeScalpStrategyId(parsed.searchParams.get('strategyId')),
    tuneId: parsed.searchParams.get('tuneId') ? normalizeScalpTuneId(parsed.searchParams.get('tuneId')) : null,
    deploymentId:
      parsed.searchParams.get('deploymentId') ||
      (explicitSymbol && parsed.searchParams.get('strategyId')
        ? buildScalpDeploymentId({
            symbol: explicitSymbol,
            strategyId: String(parsed.searchParams.get('strategyId') || ''),
            tuneId: parsed.searchParams.get('tuneId'),
          })
        : null),
    schedule,
    path: rawPath,
    route: 'execute-deployments',
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

export function getScalpCronDeploymentConfigs(): ScalpCronSymbolConfig[] {
  const crons: VercelCron[] = Array.isArray((vercelConfig as any)?.crons) ? (vercelConfig as any).crons : [];
  const out: ScalpCronSymbolConfig[] = [];
  const seen = new Set<string>();

  for (const cron of crons) {
    const schedule = typeof cron?.schedule === 'string' ? cron.schedule : null;
    const path = typeof cron?.path === 'string' ? cron.path : '';
    const parsed = parseScalpCronPath(path, schedule);
    if (!parsed) continue;
    const key = parsed.deploymentId || `${parsed.symbol}:${parsed.path}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(parsed);
  }

  return out;
}

export function getScalpCronSymbolConfigs(): ScalpCronSymbolConfig[] {
  const bySymbol = new Map<string, ScalpCronSymbolConfig>();

  for (const parsed of getScalpCronDeploymentConfigs()) {
    const existing = bySymbol.get(parsed.symbol);
    if (!existing) {
      bySymbol.set(parsed.symbol, parsed);
      continue;
    }
    if (!existing.strategyId && parsed.strategyId) {
      bySymbol.set(parsed.symbol, parsed);
      continue;
    }
    if (!existing.deploymentId && parsed.deploymentId) {
      bySymbol.set(parsed.symbol, parsed);
    }
  }
  return Array.from(bySymbol.values());
}

export function getScalpCronSymbols(): string[] {
  return getScalpCronSymbolConfigs().map((item) => item.symbol);
}
