import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { loadDecisionAt, loadDecisionHistory } from '../../../lib/history';
import { getSwingDecisionPrompt } from '../../../lib/swing/pg';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';
import { resolveAnalysisPlatform, type AnalysisPlatform } from '../../../lib/platform';

type DecisionPayload = {
  symbol: string;
  category: string | null;
  platform: AnalysisPlatform | null;
  lastDecisionTs: number | null;
  lastDecision: any | null;
  lastPrompt: { system?: string; user?: string } | null;
  lastMetrics: Record<string, any> | null;
  lastBiasTimeframes: Record<string, string | undefined> | null;
  lastNewsSource: string | null;
};

function resolveRequestedPlatform(symbol: string, requested?: string | null): AnalysisPlatform | null {
  const normalizedRequest = String(requested || '').trim();
  if (normalizedRequest) return resolveAnalysisPlatform(normalizedRequest);
  const fromCron = getCronSymbolConfigs().find((item) => item.symbol === symbol);
  return fromCron?.platform ?? null;
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;

  const symbolRaw = String(req.query.symbol || '')
    .trim()
    .toUpperCase();
  if (!symbolRaw) {
    return res.status(400).json({ error: 'symbol_required' });
  }

  const platformParam = Array.isArray(req.query.platform) ? req.query.platform[0] : req.query.platform;
  const platform = resolveRequestedPlatform(symbolRaw, platformParam ?? null);
  const cronConfig = getCronSymbolConfigs().find((item) => item.symbol === symbolRaw);

  let payload: DecisionPayload = {
    symbol: symbolRaw,
    category: cronConfig?.category ?? null,
    platform,
    lastDecisionTs: null,
    lastDecision: null,
    lastPrompt: null,
    lastMetrics: null,
    lastBiasTimeframes: null,
    lastNewsSource: null,
  };

  // Optional exact-timestamp lookup (timeline tick click) — a single direct KV
  // GET instead of the latest-entry index scan. Falls through to the empty
  // payload when the entry is missing/expired.
  const tsParam = Array.isArray(req.query.ts) ? req.query.ts[0] : req.query.ts;
  const requestedTs = Number(tsParam);

  try {
    const latest =
      Number.isFinite(requestedTs) && requestedTs > 0
        ? await loadDecisionAt(symbolRaw, requestedTs, platform ?? undefined)
        : (platform
            ? await loadDecisionHistory(symbolRaw, 1, platform)
            : await loadDecisionHistory(symbolRaw, 1))[0];
    if (latest) {
      payload = {
        symbol: symbolRaw,
        category:
          typeof latest.category === 'string'
            ? latest.category
            : typeof latest.snapshot?.category === 'string'
            ? latest.snapshot.category
            : cronConfig?.category ?? null,
        platform:
          typeof latest.platform === 'string'
            ? resolveAnalysisPlatform(latest.platform)
            : typeof latest.snapshot?.platform === 'string'
            ? resolveAnalysisPlatform(latest.snapshot.platform)
            : platform,
        lastDecisionTs: Number.isFinite(latest.timestamp) ? Number(latest.timestamp) : null,
        lastDecision: latest.aiDecision ?? null,
        lastPrompt: latest.prompt ?? null,
        lastMetrics: latest.snapshot?.metrics ?? null,
        lastBiasTimeframes: latest.biasTimeframes ?? null,
        lastNewsSource:
          typeof latest.newsSource === 'string'
            ? latest.newsSource
            : typeof latest.snapshot?.newsSource === 'string'
            ? latest.snapshot.newsSource
            : null,
      };
    }
  } catch (err) {
    console.warn(`Could not load latest decision for ${symbolRaw}:`, err);
  }

  // KV history entries no longer embed the prompt (it dominated KV bandwidth);
  // the prompt viewer's copy comes from the Neon dual-write on demand. Rows
  // written before the change still carry it in KV — the fallback only fires
  // when the entry has none. Real AI calls only: skip rows never had a prompt.
  if (
    payload.lastDecision &&
    !payload.lastPrompt &&
    payload.lastDecisionTs &&
    payload.platform &&
    !payload.lastDecision.promptSkipped
  ) {
    try {
      payload.lastPrompt = await getSwingDecisionPrompt(payload.platform, symbolRaw, payload.lastDecisionTs);
    } catch (err) {
      console.warn(`Could not load prompt from Neon for ${symbolRaw}:`, err);
    }
  }

  return res.status(200).json(payload);
}
