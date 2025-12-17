import type { NextApiRequest, NextApiResponse } from 'next';

import { getExecEvaluation, getExecEvaluationTimestamp } from '../../lib/utils';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }

  const symbolRaw = req.query.symbol;
  const symbol = String(Array.isArray(symbolRaw) ? symbolRaw[0] : symbolRaw || '').toUpperCase();
  if (!symbol) {
    return res.status(400).json({ error: 'symbol_required' });
  }

  try {
    const evaluation = await getExecEvaluation(symbol);
    if (!evaluation) {
      return res.status(404).json({ error: 'exec_eval_not_found', symbol });
    }
    const ts = await getExecEvaluationTimestamp(symbol);
    return res.status(200).json({ symbol, ts, evaluation });
  } catch (err: any) {
    return res.status(500).json({ error: 'failed_to_load_exec_eval', message: err?.message || String(err) });
  }
}

