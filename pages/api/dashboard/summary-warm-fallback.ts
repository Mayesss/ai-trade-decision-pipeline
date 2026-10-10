export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { experimentEndedAt } from '../../../lib/swing/cronControl';
import { isSwingWarmDone, markSwingWarmDone, swingWarmCycleId } from '../../../lib/swing/warmLatch';
import { warmAllSwingSummaries } from './summary';

// Fallback dashboard summary warm. The normal path is the warm latch in
// /api/analyze: the last of a venue's analyze crons in a cycle rebuilds every
// range blob and stamps the cycle's done flag. This cron runs five minutes
// after each analyze firing (vercel.json: one entry per venue, ?venue= only
// tells the two apart — the done flag is per cycle, not per venue) and only
// rebuilds when that flag is missing — i.e. an analyze crashed or timed out
// and the latch never completed — so no dashboard visitor pays the cold
// fan-out even then. Happy path = one KV GET, no Postgres. It must only run
// right after an analyze firing: on any other cycle the flag is missing by
// construction and the rebuild (which reads Postgres) would wake the Neon
// compute for nothing — why it moved off its old 3,18,33,48 schedule when the
// analyze crons went daily (docs/neon-compute-cost.md). Whitelisted for
// unauthenticated Vercel cron in lib/admin.ts; also callable with the admin
// secret for a manual warm (pass ?force=1 to bypass the done-flag skip).
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (!requireAdminAccess(req, res)) return;
  // Experiment ended: the scheduled warm would wake Neon twice a day for a
  // trader that is switched off. The dashboard warms on demand when opened.
  if (experimentEndedAt()) {
    return res.status(200).json({ ok: true, skipped: 'experiment-ended', endedAt: experimentEndedAt() });
  }
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
