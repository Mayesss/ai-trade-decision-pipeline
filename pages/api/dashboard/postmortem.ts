import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { loadSwingPostmortemById, loadSwingPostmortems } from '../../../lib/swing/pg';

// Post-mortem reads for the dashboard: ?id= returns the full row (report +
// dossier — the timeline tick carries only verdict/lesson); ?symbol= lists
// summaries. Writes/runs stay on /api/swing/postmortem (the worker route).
export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');

  const idRaw = Array.isArray(req.query.id) ? req.query.id[0] : req.query.id;
  const id = Number(idRaw);
  if (Number.isFinite(id) && id > 0) {
    const row = await loadSwingPostmortemById(Math.floor(id));
    if (!row) return res.status(404).json({ error: 'postmortem_not_found', id });
    return res.status(200).json({ postmortem: row });
  }

  const symbol = String(Array.isArray(req.query.symbol) ? req.query.symbol[0] : req.query.symbol || '')
    .trim()
    .toUpperCase();
  if (!symbol) return res.status(400).json({ error: 'id_or_symbol_required' });
  const platformRaw = Array.isArray(req.query.platform) ? req.query.platform[0] : req.query.platform;
  const postmortems = await loadSwingPostmortems({
    symbol,
    platform: platformRaw ? String(platformRaw) : null,
    limit: 30,
  });
  return res.status(200).json({ symbol, postmortems });
}
