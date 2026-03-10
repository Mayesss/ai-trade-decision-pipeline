export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import {
    aggregateScalpResearchCycle,
    listResearchCycleTasks,
    loadActiveResearchCycleId,
    loadLatestCompletedResearchCycleId,
    loadResearchCycle,
    loadResearchCycleSummary,
    retryResearchTask,
} from '../../../../lib/scalp/researchCycle';

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
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    try {
        if (req.method === 'GET') {
            const requestedCycleId = firstQueryValue(req.query.cycleId);
            const includeTasks = parseBoolParam(req.query.includeTasks, false);
            const refreshSummary = parseBoolParam(req.query.refreshSummary, false);
            const allowLatestCompletedFallback = parseBoolParam(req.query.allowLatestCompletedFallback, true);
            const taskLimit = Math.max(1, Math.min(5000, parsePositiveInt(firstQueryValue(req.query.taskLimit)) || 250));

            const activeCycleId = await loadActiveResearchCycleId();
            const latestCompletedCycleId =
                !requestedCycleId && !activeCycleId && allowLatestCompletedFallback
                    ? await loadLatestCompletedResearchCycleId()
                    : null;
            const cycleId = requestedCycleId || activeCycleId || latestCompletedCycleId;
            const cycleSource = requestedCycleId
                ? 'requested'
                : activeCycleId
                  ? 'active'
                  : latestCompletedCycleId
                    ? 'latest_completed_fallback'
                    : 'none';
            if (!cycleId) {
                return res.status(404).json({
                    error: 'research_cycle_not_found',
                    message: 'No active or completed research cycle found and no cycleId was provided.',
                    cycleSource,
                    activeCycleId: activeCycleId || null,
                    latestCompletedCycleId: latestCompletedCycleId || null,
                });
            }

            const aggregated = refreshSummary
                ? await aggregateScalpResearchCycle({ cycleId, finalizeWhenDone: false })
                : null;
            const cycle = aggregated?.cycle || (await loadResearchCycle(cycleId));
            if (!cycle) {
                return res.status(404).json({
                    error: 'research_cycle_not_found',
                    message: `Research cycle '${cycleId}' was not found.`,
                });
            }

            const summary = aggregated?.summary || cycle.latestSummary || (await loadResearchCycleSummary(cycle.cycleId));
            const tasks = includeTasks ? await listResearchCycleTasks(cycle.cycleId, taskLimit) : [];

            return res.status(200).json({
                ok: true,
                cycleId: cycle.cycleId,
                cycleSource,
                cycle,
                summary,
                tasks: includeTasks ? tasks : undefined,
                taskCountReturned: includeTasks ? tasks.length : 0,
                includeTasks,
                taskLimit,
            });
        }

        if (req.method === 'POST') {
            const body = req.body && typeof req.body === 'object' ? (req.body as Record<string, unknown>) : {};
            const action = String(body.action || '').trim();
            if (action !== 'retryTask') {
                return res.status(400).json({
                    error: 'invalid_action',
                    message: 'Supported actions: retryTask',
                });
            }

            const cycleId = String(body.cycleId || '').trim() || undefined;
            const taskId = String(body.taskId || '').trim();
            if (!taskId) {
                return res.status(400).json({
                    error: 'task_id_required',
                    message: 'Provide taskId in request body.',
                });
            }

            const retried = await retryResearchTask({ cycleId, taskId });
            const aggregate = await aggregateScalpResearchCycle({
                cycleId: retried.cycle.cycleId,
                finalizeWhenDone: false,
            });

            return res.status(200).json({
                ok: true,
                action,
                cycleId: retried.cycle.cycleId,
                taskId: retried.task.taskId,
                status: retried.task.status,
                summary: aggregate?.summary || retried.cycle.latestSummary || null,
            });
        }

        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET or POST' });
    } catch (err: any) {
        const code = String(err?.code || '').trim();
        if (
            code === 'task_id_required' ||
            code === 'task_not_failed' ||
            code === 'research_cycle_not_running' ||
            code === 'research_cycle_not_active'
        ) {
            return res.status(409).json({
                error: code,
                message: err?.message || code,
            });
        }
        if (code === 'research_cycle_not_found' || code === 'task_not_found') {
            return res.status(404).json({
                error: code,
                message: err?.message || code,
            });
        }
        if (code === 'task_locked') {
            return res.status(423).json({
                error: code,
                message: err?.message || code,
            });
        }
        return res.status(500).json({
            error: 'research_cycle_read_failed',
            message: err?.message || String(err),
        });
    }
}
