import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { loadDecisionAt, loadDecisionHistory, type DecisionHistoryEntry } from '../../../lib/history';
import { readSwingLastScan, scanIsLaterTickThan } from '../../../lib/swing/lastScan';
import { getSwingDecisionPrompt } from '../../../lib/swing/pg';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';
import { resolveAnalysisPlatform, type AnalysisPlatform } from '../../../lib/platform';

type DecisionPayload = {
  symbol: string;
  category: string | null;
  platform: AnalysisPlatform | null;
  lastDecisionTs: number | null;
  lastDecision: DecisionHistoryEntry['aiDecision'] | null;
  lastPrompt: { system?: string; user?: string } | null;
  lastMetrics: unknown;
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
  const wantsLatest = !(Number.isFinite(requestedTs) && requestedTs > 0);

  try {
    const latest = wantsLatest
      ? (platform ? await loadDecisionHistory(symbolRaw, 1, platform) : await loadDecisionHistory(symbolRaw, 1))[0]
      : await loadDecisionAt(symbolRaw, requestedTs, platform ?? undefined);
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
    // Fall back to the scan marker when the last TICK is newer than the last
    // persisted DECISION row. Three gates — primary_close_gate,
    // session_window_gate, flat_cooldown — short-circuit via recordTickOutcome
    // only: they write a tick_log row and stamp this marker, but never a
    // decision row. Without this the panel keeps presenting an hours-old row
    // as "Latest Decision" (a symbol sat on `asset_class_occupied` for 15h
    // after the book went flat). A marker carries a `stage` only when its tick
    // ended in a SKIP — real AI calls pass kvMarker:false and are surfaced by
    // the decision row they wrote.
    //
    // The model's reasoning outranks a bare gate stub while it is still CURRENT:
    // a real decision from the last tick stays up, so an AI call is not buried
    // by the routine skip 15 minutes behind it. It is retired once a whole cycle
    // has passed without a new one — reasoning older than a tick no longer says
    // what the pipeline is doing. A row that was itself a pre-AI skip carries no
    // reasoning to protect, so the freshest gate always wins there.
    if (wantsLatest && platform) {
      const marker = await readSwingLastScan(platform, symbolRaw).catch(() => null);
      const markerTs = Number(marker?.ts);
      const stage = typeof marker?.stage === 'string' ? marker.stage : '';
      const rowTs = payload.lastDecisionTs ?? 0;
      const rowWasPreAiSkip =
        payload.lastDecision?.decision_source === 'pre_ai_skip' ||
        payload.lastDecision?.promptSkipped === true;
      const supersedes = rowWasPreAiSkip || scanIsLaterTickThan(markerTs, rowTs);
      if (stage && Number.isFinite(markerTs) && markerTs > rowTs && supersedes) {
        const reason = typeof marker?.reason === 'string' && marker.reason ? marker.reason : stage;
        payload = {
          ...payload,
          platform,
          // Same shape persistPreAiSkip writes for an hourly gate skip, so the
          // panel renders it exactly as it renders a persisted one.
          lastDecisionTs: markerTs,
          lastDecision: {
            action: 'HOLD',
            bias: 'NEUTRAL',
            signal_strength: 'LOW',
            summary: stage,
            reason,
            decision_source: 'pre_ai_skip',
            promptSkipped: true,
            skipStage: stage,
          } as DecisionPayload['lastDecision'],
          // Belong to the superseded row, not to this tick.
          lastPrompt: null,
          lastMetrics: null,
          lastBiasTimeframes: null,
        };
      }
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
