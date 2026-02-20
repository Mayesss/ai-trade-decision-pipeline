export const config = { runtime: 'nodejs' };
import type { NextApiRequest, NextApiResponse } from 'next';
import { requireAdminAccess } from '../../../../lib/admin';
import baseHandler from '../../dashboard/symbols';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (!requireAdminAccess(req, res)) return;
  return baseHandler(req, res);
}
