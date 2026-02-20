import vercelConfig from '../vercel.json';
import { resolveAnalysisPlatform, resolveNewsSource, type AnalysisPlatform, type NewsSource } from './platform';

type VercelCron = {
  path?: string;
  schedule?: string;
};

export type CronSymbolConfig = {
  symbol: string;
  platform: AnalysisPlatform;
  newsSource: NewsSource;
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
  const decisionPolicyRaw = String(parsed.searchParams.get('decisionPolicy') || '').trim();

  return {
    symbol,
    platform,
    newsSource,
    decisionPolicy: decisionPolicyRaw || null,
    schedule,
    path: rawPath,
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
