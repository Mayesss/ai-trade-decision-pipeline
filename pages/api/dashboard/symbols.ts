import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { getCronSymbolConfigs } from '../../../lib/symbolRegistry';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
  }
  if (!requireAdminAccess(req, res)) return;

  const data = getCronSymbolConfigs();
  const symbols = data.map((item) => item.symbol);
  return res.status(200).json({ symbols, data });
}

