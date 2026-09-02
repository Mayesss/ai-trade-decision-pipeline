export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { kvMGetJson } from '../../../lib/kv';
import { SWING_AI_HEALTH_KEY, parseSwingAiHealth } from '../../../lib/swing/aiHealth';
import { SWING_WARM_LAST_KEY, parseSwingWarmLast } from '../../../lib/swing/warmLatch';

// Tiny poll target for open dashboards: reports when the last summary warm
// (latch or fallback) completed. ONE KV command — clients poll this instead of
// re-fetching the whole summary on a timer, and refresh only when warmedAtMs
// moves forward, i.e. exactly once per completed analyze cycle. The AI
// provider health flag rides along on the same poll (it's the only request
// an open dashboard makes between renders), feeding the outage banner.
//
// Both keys come back in a single MGET: at one poll every 20s an open tab makes
// ~130k requests a month, and Upstash bills per command, so reading them
// separately doubled the cost of the cheapest thing the dashboard does.
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;
  const [warmRaw, healthRaw] = await kvMGetJson<unknown>([SWING_WARM_LAST_KEY, SWING_AI_HEALTH_KEY]);
  const last = parseSwingWarmLast(warmRaw);
  const aiHealth = parseSwingAiHealth(healthRaw);
  return res.status(200).json({
    ok: true,
    warmedAtMs: last?.warmedAtMs ?? null,
    cycleId: last?.cycleId ?? null,
    aiHealth: {
      degraded: aiHealth.degraded,
      provider: aiHealth.provider,
      kind: aiHealth.kind,
      reason: aiHealth.reason,
      sinceMs: aiHealth.sinceMs,
      consecutiveFailures: aiHealth.consecutiveFailures,
    },
  });
}
