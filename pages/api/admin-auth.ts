import type { NextApiRequest, NextApiResponse } from 'next';

type AdminAuthResponse = {
  ok: boolean;
  required: boolean;
};

export default function handler(req: NextApiRequest, res: NextApiResponse<AdminAuthResponse>) {
  if (req.method !== 'POST') {
    res.status(405).json({ ok: false, required: true });
    return;
  }

  const expected = process.env.ADMIN_ACCESS_SECRET;
  if (!expected) {
    res.status(200).json({ ok: true, required: false });
    return;
  }

  const secret = typeof req.body?.secret === 'string' ? req.body.secret : '';
  const ok = secret === expected;
  res.status(ok ? 200 : 401).json({ ok, required: true });
}
