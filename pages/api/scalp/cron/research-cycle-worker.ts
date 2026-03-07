export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { aggregateScalpResearchCycle, runResearchWorker } from '../../../../lib/scalp/researchCycle';
import { syncResearchCyclePromotionGates } from '../../../../lib/scalp/researchPromotion';

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

function parsePositiveInt(value: string | undefined): number | undefined {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return undefined;
    return Math.floor(n);
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
    const workerId = firstQueryValue(req.query.workerId);
    const maxRuns = parsePositiveInt(firstQueryValue(req.query.maxRuns));
    const aggregateAfter = parseBoolParam(req.query.aggregateAfter, true);
    const finalizeWhenDone = parseBoolParam(req.query.finalizeWhenDone, true);
    const syncPromotionGates = parseBoolParam(req.query.syncPromotionGates, true);
    const requireCompletedCycleForSync = parseBoolParam(req.query.requireCompletedCycleForSync, true);

    try {
        const worker = await runResearchWorker({ cycleId, workerId, maxRuns });
        const aggregate =
            aggregateAfter && worker.cycleId
                ? await aggregateScalpResearchCycle({
                      cycleId: worker.cycleId,
                      finalizeWhenDone,
                  })
                : null;
        const promotionSync =
            syncPromotionGates &&
            worker.cycleId &&
            aggregate &&
            (!requireCompletedCycleForSync || aggregate.summary.status === 'completed')
                ? await syncResearchCyclePromotionGates({
                      cycleId: worker.cycleId,
                      dryRun: false,
                      requireCompletedCycle: requireCompletedCycleForSync,
                      updatedBy: 'cron:research-cycle-worker',
                  })
                : null;

        return res.status(200).json({
            ok: true,
            requestedCycleId: cycleId || null,
            worker,
            aggregate: aggregate
                ? {
                      cycleId: aggregate.cycle.cycleId,
                      status: aggregate.cycle.status,
                      progressPct: aggregate.summary.progressPct,
                      totals: aggregate.summary.totals,
                      topCandidates: aggregate.summary.candidateAggregates.slice(0, 10),
                      generatedAtMs: aggregate.summary.generatedAtMs,
                  }
                : null,
            promotionSync,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'research_cycle_worker_failed',
            message: err?.message || String(err),
        });
    }
}
