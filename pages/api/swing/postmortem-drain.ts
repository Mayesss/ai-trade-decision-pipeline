export const config = { runtime: 'nodejs' };
// Drain for delayed post-mortems: close-triggered rows are deliberately NOT
// run at close time — they mature at exit + SWING_POSTMORTEM_DELAY_MINUTES so
// the dossier's post-exit tail (what the market did after the close) is fully
// recorded before the analyst judges premature-close / misplaced-SL. This
// route claims only mature queued rows and runs them sequentially.
//
// RETIRED AS A CRON on 2026-10-06. The analyst has been off by default since
// 2026-09-17, so the every-15-minutes cron was a tested no-op; it was removed
// from vercel.json and from UNAUTHENTICATED_CRON_ROUTES (lib/admin.ts), which
// makes this an ordinary admin route. The route and the queue stay, so
// SWING_POSTMORTEM_MODE=loss|all still works: call it by hand with the admin
// secret, or put the cron entry and the allow-list line back
// (docs/neon-compute-cost.md, "Postmortem drain").
import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { loadSwingAiHealth } from '../../../lib/swing/aiHealth';
import { claimQueuedSwingPostmortems, isSwingPgConfigured } from '../../../lib/swing/pg';
import {
    resolveSwingPostmortemDelayMs,
    resolveSwingPostmortemMode,
    runSwingPostmortem,
} from '../../../lib/swing/postmortem';

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

    // The analyst is OFF by default (2026-09-17, lib/swing/postmortem.ts). The
    // mode gates ENQUEUE — but rows queued before it flipped are still mature
    // and still claimable, so an ungated drain would make exactly the AI calls
    // the switch exists to stop (4 refusal rows were sitting queued when it
    // flipped, maturing hours later). Rows are LEFT QUEUED rather than marked
    // skipped: turning the analyst back on resumes them where they stand.
    // /api/swing/postmortem is unaffected — that route is admin-only and is
    // how an operator asks for one analysis on purpose.
    if (resolveSwingPostmortemMode() === 'off') {
        return res.status(200).json({ ok: true, processed: 0, note: 'postmortem_mode_off' });
    }

    // AI provider down for a non-self-healing reason (subscription lapse, bad
    // key)? Don't burn claims and doomed API attempts on every pass — leave
    // the rows queued; they analyze themselves once the flag clears. Transient
    // degradation is NOT gated: the next pass may well succeed.
    const aiHealth = await loadSwingAiHealth();
    if (aiHealth.degraded && (aiHealth.kind === 'billing' || aiHealth.kind === 'config')) {
        return res.status(200).json({
            ok: true,
            processed: 0,
            note: 'ai_unavailable',
            aiHealth: { kind: aiHealth.kind, sinceMs: aiHealth.sinceMs, reason: aiHealth.reason },
        });
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
