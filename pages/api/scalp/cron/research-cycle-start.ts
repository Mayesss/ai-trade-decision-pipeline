export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { invokeCronEndpoint } from '../../../../lib/scalp/cronChaining';
import { startScalpResearchCycle } from '../../../../lib/scalp/researchCycle';

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

function parseSymbolsCsv(value: string | undefined): string[] {
    if (!value) return [];
    return value
        .split(',')
        .map((row) =>
            String(row || '')
                .trim()
                .toUpperCase()
                .replace(/[^A-Z0-9._-]/g, ''),
        )
        .filter((row) => Boolean(row));
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

    const dryRun = parseBoolParam(req.query.dryRun, false);
    const force = parseBoolParam(req.query.force, false);
    const autoSuccessor = parseBoolParam(req.query.autoSuccessor, true);
    const successorPath = firstQueryValue(req.query.successorPath) || '/api/scalp/cron/research-cycle-worker';

    try {
        const out = await startScalpResearchCycle({
            dryRun,
            force,
            symbols: parseSymbolsCsv(firstQueryValue(req.query.symbols)),
            lookbackDays: parsePositiveInt(firstQueryValue(req.query.lookbackDays)),
            chunkDays: parsePositiveInt(firstQueryValue(req.query.chunkDays)),
            minCandlesPerTask: parsePositiveInt(firstQueryValue(req.query.minCandlesPerTask)),
            maxTasks: parsePositiveInt(firstQueryValue(req.query.maxTasks)),
            maxAttempts: parsePositiveInt(firstQueryValue(req.query.maxAttempts)),
            runningStaleAfterMs: parsePositiveInt(firstQueryValue(req.query.runningStaleAfterMs)),
            tunerEnabled: req.query.tunerEnabled === undefined ? undefined : parseBoolParam(req.query.tunerEnabled, true),
            maxTuneVariantsPerStrategy: parsePositiveInt(firstQueryValue(req.query.maxTuneVariantsPerStrategy)),
            plannerEnabled:
                req.query.plannerEnabled === undefined ? undefined : parseBoolParam(req.query.plannerEnabled, true),
            startedBy: firstQueryValue(req.query.startedBy) || 'cron:research-cycle-start',
        });
        const shouldCallSuccessor = autoSuccessor && !dryRun && out.started && Boolean(out.cycle?.cycleId);
        const successor = shouldCallSuccessor
            ? await invokeCronEndpoint(req, successorPath, {
                  cycleId: out.cycle?.cycleId,
                  autoContinue: 1,
                  continueHop: 0,
                  startedBy: 'cron:research-cycle-start:successor',
              })
            : null;

        return res.status(200).json({
            ok: true,
            started: out.started,
            dryRun,
            force,
            cycle: {
                cycleId: out.cycle.cycleId,
                status: out.cycle.status,
                symbols: out.cycle.symbols,
                taskCount: out.cycle.taskIds.length,
                params: out.cycle.params,
                createdAtMs: out.cycle.createdAtMs,
                sourceUniverseGeneratedAt: out.cycle.sourceUniverseGeneratedAt,
            },
            chaining: {
                autoSuccessor: {
                    enabled: autoSuccessor,
                    successorPath,
                    requested: shouldCallSuccessor,
                    successor,
                },
            },
            snapshot: out.cycle,
        });
    } catch (err: any) {
        if (String(err?.code || '') === 'research_cycle_preflight_failed' && err?.preflight) {
            return res.status(409).json({
                error: 'research_cycle_preflight_failed',
                message: 'Research cycle preflight failed. Run /api/scalp/cron/research-preflight for details.',
                preflight: err.preflight,
            });
        }
        return res.status(500).json({
            error: 'research_cycle_start_failed',
            message: err?.message || String(err),
        });
    }
}
