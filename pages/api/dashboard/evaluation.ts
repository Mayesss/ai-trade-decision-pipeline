import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { getEvaluation, getEvaluationTimestamp } from '../../../lib/utils';

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

  let evaluation: Record<string, any> = {};
  let evaluationTs: number | null = null;
  try {
    const raw = await getEvaluation(symbol);
    evaluation = raw && typeof raw === 'object' ? raw : {};
  } catch (err) {
    console.warn(`Could not load evaluation for ${symbol}:`, err);
  }

  try {
    evaluationTs = await getEvaluationTimestamp(symbol);
  } catch {
    evaluationTs = null;
  }

  return res.status(200).json({
    symbol,
    evaluation,
    evaluationTs,
  });
}

