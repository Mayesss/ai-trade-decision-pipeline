export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { warmAllSwingSummaries } from './summary';

// Cron-warmed dashboard summary. The per-symbol analyze crons bust the summary
// cache (invalidateSwingSummaryCache) when they record a decision at :00; this
// runs a few minutes later to rebuild every range blob, so no dashboard visitor
// ever pays the cold fan-out. Whitelisted for unauthenticated Vercel cron in
// lib/admin.ts; also callable with the admin secret for a manual warm.
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (!requireAdminAccess(req, res)) return;
  const warmed = await warmAllSwingSummaries();
  return res.status(200).json({ ok: true, warmed });
}
