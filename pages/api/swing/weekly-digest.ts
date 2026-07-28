export const config = { runtime: 'nodejs' };
// Weekly system digest (Sunday cron + on-demand). Deterministic aggregation
// over durable swing tables — see lib/swing/weeklyDigest.ts. The cron call
// (?store=1) persists the digest row for trend history; manual GETs are
// read-only. ?format=md returns the pasteable markdown alone; the default JSON
// response carries both the structured digest and the markdown rendering.
// In UNAUTHENTICATED_CRON_ROUTES (lib/admin.ts): Vercel crons cannot send the
// admin header; the route exposes aggregate stats only — no prompts, no keys.
import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../lib/admin';
import { isSwingPgConfigured } from '../../../lib/swing/pg';
import {
    buildSwingWeeklyDigest,
    listSwingWeeklyDigests,
    loadSwingWeeklyDigestById,
    renderSwingWeeklyDigestMarkdown,
    storeSwingWeeklyDigest,
} from '../../../lib/swing/weeklyDigest';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    if (!isSwingPgConfigured()) {
        return res.status(200).json({ ok: false, note: 'pg_not_configured' });
    }

    // ?list=1 → stored digest index (id + window) for the UI's history picker.
    if (String(req.query.list || '') === '1') {
        return res.status(200).json({ ok: true, digests: await listSwingWeeklyDigests() });
    }

    // ?id=N → a stored digest verbatim (no recompute — it's the snapshot the
    // Sunday cron took, including data since pruned).
    const storedIdParam = Number(req.query.id);
    if (Number.isFinite(storedIdParam) && storedIdParam > 0) {
        const stored = await loadSwingWeeklyDigestById(storedIdParam);
        if (!stored) return res.status(404).json({ ok: false, note: 'digest_not_found' });
        return res.status(200).json({ ok: true, digest: stored, markdown: renderSwingWeeklyDigestMarkdown(stored) });
    }

    const days = Number(req.query.days);
    const toMs = Number(req.query.to);
    const digest = await buildSwingWeeklyDigest({
        days: Number.isFinite(days) && days > 0 ? days : undefined,
        toMs: Number.isFinite(toMs) && toMs > 0 ? toMs : undefined,
    });
    if (!digest) {
        return res.status(200).json({ ok: false, note: 'digest_unavailable' });
    }

    let storedId: number | null = null;
    if (String(req.query.store || '') === '1') {
        storedId = await storeSwingWeeklyDigest(digest);
    }

    const markdown = renderSwingWeeklyDigestMarkdown(digest);
    if (String(req.query.format || '') === 'md') {
        res.setHeader('Content-Type', 'text/markdown; charset=utf-8');
        return res.status(200).send(markdown);
    }
    return res.status(200).json({ ok: true, storedId, digest, markdown });
}
