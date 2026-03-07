export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { aggregateScalpResearchCycle } from '../../../../lib/scalp/researchCycle';

function parseBoolParam(value: string | string[] | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const first = Array.isArray(value) ? value[0] : value;
    if (first === undefined) return fallback;
    const normalized = String(first).trim().toLowerCase();
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    return fallback;
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value.trim() || undefined;
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
    return undefined;
}

function setNoStoreHeaders(res: NextApiResponse): void {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    const cycleId = firstQueryValue(req.query.cycleId);
    const finalizeWhenDone = parseBoolParam(req.query.finalizeWhenDone, true);

    try {
        const aggregate = await aggregateScalpResearchCycle({ cycleId, finalizeWhenDone });
        if (!aggregate) {
            return res.status(200).json({
                ok: true,
                found: false,
                requestedCycleId: cycleId || null,
                message: 'No active or requested research cycle found.',
            });
        }

        return res.status(200).json({
            ok: true,
            found: true,
            requestedCycleId: cycleId || null,
            cycle: {
                cycleId: aggregate.cycle.cycleId,
                status: aggregate.cycle.status,
                symbols: aggregate.cycle.symbols,
                taskCount: aggregate.cycle.taskIds.length,
                createdAtMs: aggregate.cycle.createdAtMs,
                updatedAtMs: aggregate.cycle.updatedAtMs,
            },
            summary: {
                ...aggregate.summary,
                topCandidates: aggregate.summary.candidateAggregates.slice(0, 20),
            },
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'research_cycle_aggregate_failed',
            message: err?.message || String(err),
        });
    }
}
