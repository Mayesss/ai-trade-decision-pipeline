export const config = { runtime: 'nodejs' };
// Cron drain for delayed post-mortems (vercel.json, every 15 min): close-
// triggered rows are deliberately NOT run at close time — they mature at
// exit + SWING_POSTMORTEM_DELAY_MINUTES so the dossier's post-exit tail
// (what the market did after the close) is fully recorded before the analyst
// judges premature-close / misplaced-SL. This route claims only mature queued
// rows and runs them sequentially. It exists separately from
// /api/swing/postmortem because Vercel crons cannot send the admin header and
// the main route also serves reports + manual enqueue, which must stay
// protected; this one is in UNAUTHENTICATED_CRON_ROUTES (lib/admin.ts) and
// exposes nothing but "process what is due".
import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { claimQueuedSwingPostmortems, isSwingPgConfigured } from '../../../lib/swing/pg';
import { resolveSwingPostmortemDelayMs, runSwingPostmortem } from '../../../lib/swing/postmortem';

const DRAIN_LIMIT = 3;

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    if (!isSwingPgConfigured()) {
        return res.status(200).json({ ok: true, processed: 0, note: 'pg_not_configured' });
    }

    const claimed = await claimQueuedSwingPostmortems(DRAIN_LIMIT, {
        exitTsBeforeMs: Date.now() - resolveSwingPostmortemDelayMs(),
    });
    const results = [];
    for (const row of claimed) {
        results.push(await runSwingPostmortem(row));
    }
    return res.status(200).json({ ok: true, processed: results.length, results });
}
