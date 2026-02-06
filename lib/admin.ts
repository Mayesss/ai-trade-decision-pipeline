import type { NextApiRequest, NextApiResponse } from 'next';

export type AdminAccessResult = { ok: boolean; required: boolean };

export function checkAdminAccessHeader(req: NextApiRequest): AdminAccessResult {
  const expected = process.env.ADMIN_ACCESS_SECRET;
  if (!expected) {
    return { ok: true, required: false };
  }

  const header = req.headers['x-admin-access-secret'];
  const provided = Array.isArray(header) ? header[0] : header || '';
  return { ok: provided === expected, required: true };
}

export function requireAdminAccess(req: NextApiRequest, res: NextApiResponse): boolean {
  const result = checkAdminAccessHeader(req);
  if (result.required && !result.ok) {
    res.status(401).json({ error: 'Unauthorized' });
    return false;
  }
  return true;
}
