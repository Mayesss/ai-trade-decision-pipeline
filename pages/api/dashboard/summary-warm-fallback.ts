export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { isSwingWarmDone, markSwingWarmDone, swingWarmCycleId } from '../../../lib/swing/warmLatch';
import { warmAllSwingSummaries } from './summary';

// Fallback dashboard summary warm. The normal path is the warm latch in
// /api/analyze: the last analyze cron of each 15-minute cycle rebuilds every
// range blob and stamps the cycle's done flag. This cron (a few minutes after
// the cycle fires, see vercel.json) only rebuilds when that flag is missing —
// i.e. an analyze crashed or timed out and the latch never completed — so no
// dashboard visitor pays the cold fan-out even then. Whitelisted for
// unauthenticated Vercel cron in lib/admin.ts; also callable with the admin
// secret for a manual warm (pass ?force=1 to bypass the done-flag skip).
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (!requireAdminAccess(req, res)) return;
  const forceParam = Array.isArray(req.query.force) ? req.query.force[0] : req.query.force;
  const force = forceParam === '1' || forceParam === 'true';
  const cycleId = swingWarmCycleId(Date.now());
  if (!force && (await isSwingWarmDone(cycleId))) {
    return res.status(200).json({ ok: true, skipped: 'latch-already-warmed', cycleId });
  }
  const warmed = await warmAllSwingSummaries();
  // Stamp the done flag + swing:warm:last so open dashboards refresh off this
  // warm too (they poll warm-status), even when the latch never completed.
  await markSwingWarmDone(cycleId).catch(() => undefined);
  return res.status(200).json({ ok: true, warmed, cycleId });
}
