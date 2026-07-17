import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { loadDecisionHistory } from '../../../lib/history';
import { readSwingScanTicks } from '../../../lib/swing/lastScan';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';
import { resolveAnalysisPlatform, type AnalysisPlatform } from '../../../lib/platform';

// One tick on the swing decision timeline. Two sources merged per tick bucket:
// persisted decision rows (hourly skips + real AI calls) and the rolling scan
// tick log (quarter-tick gate skips, which are deliberately never persisted as
// decision rows — see lib/swing/lastScan.ts).
export type TimelineTick = {
  ts: number;
  source: 'decision' | 'scan';
  // Hourly cadence tick (bigger circle) vs quarter tick. Crons fire on UTC
  // minutes, so minute-of-hour is a reliable discriminator.
  hourly: boolean;
  kind: 'action' | 'ai_call' | 'gate_skip' | 'scan_skip' | 'scan';
  action?: string;
  summary?: string;
  // AI-requested flat cooldown armed by this decision (flat HOLD only) — lets
  // the UI label the tick "HOLD + CD 2h (↑x ↓y)" instead of a bare HOLD.
  cooldownMinutes?: number;
  cooldownWakeAbove?: number;
  cooldownWakeBelow?: number;
  stage?: string;
  reason?: string;
  // Responses-API conversation chain: `responseId` is this decision's turn,
  // `previousResponseId` the turn it chained onto (present only on context
  // calls — in-position ticks and post-fill management of a pullback limit).
  // The UI links chained ticks with a full-contrast connector segment.
  responseId?: string;
  previousResponseId?: string;
  // True when a full decision row exists for this tick and can be fetched via
  // /api/dashboard/decision?ts=.
  hasDetails: boolean;
};

const DEFAULT_HOURS = 24;
const MAX_HOURS = 48;
// Ticks fire on :00/:15/:30/:45 but can slip a few minutes; bucket by 5 minutes
// so a decision row and its own scan-start marker collapse into one tick.
const TICK_BUCKET_MS = 5 * 60 * 1000;
const ACTION_KINDS = new Set(['BUY', 'SELL', 'CLOSE']);

function resolveRequestedPlatform(symbol: string, requested?: string | null): AnalysisPlatform | null {
  const normalizedRequest = String(requested || '').trim();
  if (normalizedRequest) return resolveAnalysisPlatform(normalizedRequest);
  const fromCron = getCronSymbolConfigs().find((item) => item.symbol === symbol);
  return fromCron?.platform ?? null;
}

// Hourly ticks fire at :00 (quarter ticks at :15/:30/:45) and can slip a few
// minutes under cron jitter — minute-of-hour < 7 tags them reliably.
function isHourlyTs(ts: number): boolean {
  return new Date(ts).getUTCMinutes() < 7;
}

// Higher wins when two entries land in the same tick bucket.
function tickPriority(tick: TimelineTick): number {
  if (tick.source === 'decision') return 2;
  return tick.kind === 'scan_skip' ? 1 : 0;
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
  const hoursRaw = Number(Array.isArray(req.query.hours) ? req.query.hours[0] : req.query.hours);
  const hours = Number.isFinite(hoursRaw) && hoursRaw > 0 ? Math.min(hoursRaw, MAX_HOURS) : DEFAULT_HOURS;
  const sinceMs = Date.now() - hours * 60 * 60 * 1000;

  const [decisions, scanTicks] = await Promise.all([
    loadDecisionHistory(symbol, 60, platform ?? undefined).catch(() => []),
    readSwingScanTicks(platform ?? 'bitget', symbol, { sinceMs }),
  ]);

  const byBucket = new Map<number, TimelineTick>();
  const put = (tick: TimelineTick) => {
    const bucket = Math.floor(tick.ts / TICK_BUCKET_MS);
    const existing = byBucket.get(bucket);
    if (!existing || tickPriority(tick) > tickPriority(existing)) {
      byBucket.set(bucket, tick);
    }
  };

  for (const entry of decisions) {
    const ts = Number(entry?.timestamp);
    if (!Number.isFinite(ts) || ts < sinceMs) continue;
    const decision = (entry.aiDecision ?? {}) as Record<string, any>;
    const action = String(decision.action || '').trim().toUpperCase();
    const skipped =
      decision.promptSkipped === true ||
      decision.decision_source === 'pre_ai_skip' ||
      entry.snapshot?.promptSkipped === true;
    put({
      ts,
      source: 'decision',
      hourly: isHourlyTs(ts),
      kind: skipped ? 'gate_skip' : ACTION_KINDS.has(action) ? 'action' : 'ai_call',
      ...(action ? { action } : {}),
      ...(typeof decision.summary === 'string' && decision.summary ? { summary: decision.summary } : {}),
      ...(Number.isFinite(Number(decision.cooldown_minutes)) && Number(decision.cooldown_minutes) > 0
        ? {
            cooldownMinutes: Number(decision.cooldown_minutes),
            ...(Number.isFinite(Number(decision.cooldown_wake_above)) && Number(decision.cooldown_wake_above) > 0
              ? { cooldownWakeAbove: Number(decision.cooldown_wake_above) }
              : {}),
            ...(Number.isFinite(Number(decision.cooldown_wake_below)) && Number(decision.cooldown_wake_below) > 0
              ? { cooldownWakeBelow: Number(decision.cooldown_wake_below) }
              : {}),
          }
        : {}),
      ...(typeof decision.skipStage === 'string' && decision.skipStage ? { stage: decision.skipStage } : {}),
      ...(typeof decision.reason === 'string' && decision.reason ? { reason: decision.reason } : {}),
      ...(typeof decision.response_id === 'string' && decision.response_id
        ? { responseId: decision.response_id }
        : {}),
      ...(typeof decision.previous_response_id === 'string' && decision.previous_response_id
        ? { previousResponseId: decision.previous_response_id }
        : {}),
      hasDetails: true,
    });
  }

  for (const tick of scanTicks) {
    const ts = Number(tick?.ts);
    if (!Number.isFinite(ts) || ts < sinceMs) continue;
    put({
      ts,
      source: 'scan',
      hourly: isHourlyTs(ts),
      kind: tick.stage ? 'scan_skip' : 'scan',
      ...(tick.stage ? { stage: tick.stage } : {}),
      ...(tick.reason ? { reason: tick.reason } : {}),
      hasDetails: false,
    });
  }

  const ticks = Array.from(byBucket.values()).sort((a, b) => b.ts - a.ts);
  return res.status(200).json({ symbol, platform, hours, ticks });
}
