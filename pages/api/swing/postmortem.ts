// Post-mortem worker + read API (phase 2). The enqueue side lives in the
// close-persistence paths (lib/swing/postmortem.maybeEnqueueSwingPostmortem),
// which fire a detached request at this route with ?id=&execute=true — same
// worker shape as /api/evaluate. Modes:
//   ?id=N&execute=true[&force=true]  claim + run one post-mortem inline (worker)
//   ?drain=true[&limit=N]            process oldest queued rows (missed triggers)
//   ?enqueue=true&platform=&positionKey=[&run=true]  manual enqueue (bypasses
//                                    the loss filter), optionally run inline
//   ?id=N                            full row (report + dossier)
//   default                          list summaries (?symbol=&platform=&status=&limit=)
import type { NextApiRequest, NextApiResponse } from 'next';
import { requireAdminAccess } from '../../../lib/admin';
import {
    claimQueuedSwingPostmortems,
    claimSwingPostmortemById,
    enqueueSwingPostmortem,
    isSwingPgConfigured,
    loadSwingPositionByKey,
    loadSwingPostmortemById,
    loadSwingPostmortems,
    type SwingPostmortemStatus,
} from '../../../lib/swing/pg';
import { resolveSwingPostmortemDelayMs, runSwingPostmortem } from '../../../lib/swing/postmortem';

function parseBoolParam(value: string | string[] | undefined): boolean {
    const v = Array.isArray(value) ? value[0] : value;
    return ['true', '1', 'yes', 'on'].includes(String(v ?? '').toLowerCase());
}

function firstParam(value: string | string[] | undefined): string {
    return String((Array.isArray(value) ? value[0] : value) ?? '').trim();
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate');
    if (!isSwingPgConfigured()) {
        return res.status(503).json({ error: 'pg_not_configured' });
    }

    const q = req.query ?? {};
    const id = Number(firstParam(q.id as string | string[]));
    const execute = parseBoolParam(q.execute as string | string[]);
    const force = parseBoolParam(q.force as string | string[]);
    const drain = parseBoolParam(q.drain as string | string[]);
    const enqueue = parseBoolParam(q.enqueue as string | string[]);

    if (enqueue) {
        const platform = firstParam(q.platform as string | string[]) || 'bitget';
        const positionKey = firstParam(q.positionKey as string | string[]);
        if (!positionKey) return res.status(400).json({ error: 'positionKey_required' });
        const position = await loadSwingPositionByKey(platform, positionKey);
        if (!position) return res.status(404).json({ error: 'position_not_found', platform, positionKey });
        if (position.status !== 'closed' || !position.exitTimestamp) {
            return res.status(400).json({ error: 'position_not_closed', platform, positionKey });
        }
        const newId = await enqueueSwingPostmortem({
            platform,
            symbol: position.symbol,
            positionKey,
            trigger: 'manual',
            side: position.side,
            entryTsMs: position.entryTimestamp ?? null,
            exitTsMs: position.exitTimestamp ?? null,
            entryPrice: position.entryPrice ?? null,
            exitPrice: position.exitPrice ?? null,
            pnlPct: position.pnlPct ?? position.pnlGrossPct ?? null,
            pnlNet: position.pnlNet ?? null,
        });
        const alreadyExisted = newId == null;
        if (!parseBoolParam(q.run as string | string[])) {
            return res.status(200).json({ enqueued: !alreadyExisted, alreadyExisted, id: newId });
        }
        // run inline: a manual re-request on an existing row means "regenerate"
        const existing = alreadyExisted
            ? (await loadSwingPostmortems({ platform, limit: 200 })).find((p) => p.positionKey === positionKey)
            : null;
        const runId = newId ?? existing?.id;
        if (runId == null) return res.status(404).json({ error: 'postmortem_row_not_found' });
        const claimed = await claimSwingPostmortemById(runId, { force: true });
        if (!claimed) return res.status(409).json({ error: 'claim_failed', id: runId });
        const result = await runSwingPostmortem(claimed);
        return res.status(200).json({ enqueued: !alreadyExisted, ...result });
    }

    if (drain) {
        const limit = Math.max(1, Math.min(5, Number(firstParam(q.limit as string | string[])) || 3));
        // Only mature rows: the post-close delay must have elapsed so the
        // dossier's post-exit tail is fully recorded. ?force=true drains
        // everything regardless (operator intent).
        const claimed = await claimQueuedSwingPostmortems(
            limit,
            force ? {} : { exitTsBeforeMs: Date.now() - resolveSwingPostmortemDelayMs() },
        );
        const results = [];
        for (const row of claimed) {
            results.push(await runSwingPostmortem(row));
        }
        return res.status(200).json({ processed: results.length, results });
    }

    if (Number.isFinite(id) && id > 0) {
        if (execute) {
            const claimed = await claimSwingPostmortemById(id, { force });
            if (!claimed) {
                const row = await loadSwingPostmortemById(id);
                if (!row) return res.status(404).json({ error: 'postmortem_not_found', id });
                // Not claimable = another worker owns it or it already finished.
                return res.status(200).json({ id, status: row.status, claimed: false });
            }
            const result = await runSwingPostmortem(claimed);
            return res.status(200).json(result);
        }
        const row = await loadSwingPostmortemById(id);
        if (!row) return res.status(404).json({ error: 'postmortem_not_found', id });
        return res.status(200).json(row);
    }

    const status = firstParam(q.status as string | string[]);
    const list = await loadSwingPostmortems({
        symbol: firstParam(q.symbol as string | string[]) || null,
        platform: firstParam(q.platform as string | string[]) || null,
        status: (['queued', 'running', 'succeeded', 'failed'] as SwingPostmortemStatus[]).includes(
            status as SwingPostmortemStatus,
        )
            ? (status as SwingPostmortemStatus)
            : null,
        limit: Number(firstParam(q.limit as string | string[])) || 50,
    });
    return res.status(200).json({ postmortems: list });
}
