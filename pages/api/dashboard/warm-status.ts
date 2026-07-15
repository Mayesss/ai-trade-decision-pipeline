export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { readSwingWarmLast } from '../../../lib/swing/warmLatch';

// Tiny poll target for open dashboards: reports when the last summary warm
// (latch or fallback) completed. One KV read — clients poll this instead of
// re-fetching the whole summary on a timer, and refresh only when warmedAtMs
// moves forward, i.e. exactly once per completed analyze cycle.
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;
  const last = await readSwingWarmLast();
  return res.status(200).json({
    ok: true,
    warmedAtMs: last?.warmedAtMs ?? null,
    cycleId: last?.cycleId ?? null,
  });
}
