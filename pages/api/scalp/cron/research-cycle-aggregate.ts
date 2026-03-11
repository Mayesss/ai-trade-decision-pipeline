export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { invokeCronEndpoint } from '../../../../lib/scalp/cronChaining';
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

function logAggregate(
    event: string,
    payload: Record<string, unknown>,
    level: 'info' | 'warn' | 'error' = 'info',
    force = false,
    debug = false,
): void {
    if (!force && !debug) return;
    const line = JSON.stringify({
        scope: 'scalp_research_cycle_aggregate_api',
        event,
        ...payload,
    });
    if (level === 'error') console.error(line);
    else if (level === 'warn') console.warn(line);
    else console.info(line);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    const cycleId = firstQueryValue(req.query.cycleId);
    const finalizeWhenDone = parseBoolParam(req.query.finalizeWhenDone, true);
    const debug = parseBoolParam(req.query.debug, false);
    const autoSuccessor = parseBoolParam(req.query.autoSuccessor, true);
    const continueHop = Math.max(0, Math.floor(Number(firstQueryValue(req.query.continueHop)) || 0));
    const autoContinueMaxHops = Math.max(
        0,
        Math.min(10, Math.floor(Number(firstQueryValue(req.query.autoContinueMaxHops)) || 3)),
    );
    const pendingSuccessorPath = firstQueryValue(req.query.pendingSuccessorPath) || '/api/scalp/cron/research-cycle-worker';
    const doneSuccessorPath = firstQueryValue(req.query.doneSuccessorPath) || '/api/scalp/cron/research-cycle-sync-gates';
    const startedAtMs = Date.now();

    try {
        const aggregate = await aggregateScalpResearchCycle({ cycleId, finalizeWhenDone });
        if (!aggregate) {
            const message = 'No active or requested research cycle found.';
            logAggregate(
                'no_cycle',
                {
                    requestedCycleId: cycleId || null,
                    finalizeWhenDone,
                    durationMs: Date.now() - startedAtMs,
                },
                'info',
                false,
                debug,
            );
            return res.status(200).json({
                ok: true,
                found: false,
                requestedCycleId: cycleId || null,
                message,
            });
        }

        const message = `aggregate refreshed cycle ${aggregate.cycle.cycleId} (${aggregate.summary.totals.completed}/${aggregate.summary.totals.tasks} completed)`;
        logAggregate(
            'aggregate_completed',
            {
                requestedCycleId: cycleId || null,
                cycleId: aggregate.cycle.cycleId,
                cycleStatus: aggregate.cycle.status,
                finalizeWhenDone,
                totals: aggregate.summary.totals,
                progressPct: aggregate.summary.progressPct,
                durationMs: Date.now() - startedAtMs,
            },
            'info',
            false,
            debug,
        );
        const shouldCallPendingSuccessor =
            autoSuccessor &&
            continueHop < autoContinueMaxHops &&
            aggregate.summary.status === 'running' &&
            (aggregate.summary.totals.pending > 0 || aggregate.summary.totals.running > 0);
        const shouldCallDoneSuccessor = autoSuccessor && aggregate.summary.status === 'completed';
        const successor = shouldCallPendingSuccessor
            ? await invokeCronEndpoint(req, pendingSuccessorPath, {
                  cycleId: aggregate.cycle.cycleId,
                  autoContinue: 1,
                  continueHop: continueHop + 1,
                  autoContinueMaxHops,
                  autoSuccessor: 1,
                  startedBy: 'cron:research-cycle-aggregate:pending-successor',
              })
            : shouldCallDoneSuccessor
              ? await invokeCronEndpoint(req, doneSuccessorPath, {
                    cycleId: aggregate.cycle.cycleId,
                    dryRun: 0,
                    requireCompletedCycle: 1,
                    updatedBy: 'cron:research-cycle-aggregate:done-successor',
                })
              : null;
        return res.status(200).json({
            ok: true,
            found: true,
            message,
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
            chaining: {
                autoSuccessor,
                continueHop,
                maxHops: autoContinueMaxHops,
                pendingSuccessorPath,
                doneSuccessorPath,
                requestedPendingSuccessor: shouldCallPendingSuccessor,
                requestedDoneSuccessor: shouldCallDoneSuccessor,
                successor,
            },
        });
    } catch (err: any) {
        logAggregate(
            'aggregate_failed',
            {
                requestedCycleId: cycleId || null,
                finalizeWhenDone,
                error: err?.message || String(err),
                stack: err?.stack || null,
                durationMs: Date.now() - startedAtMs,
            },
            'error',
            true,
            debug,
        );
        return res.status(500).json({
            error: 'research_cycle_aggregate_failed',
            message: err?.message || String(err),
        });
    }
}
