export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { prepareAndStartScalpResearchCycle } from '../../../../lib/scalp/prepareAndStartCycle';

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

    try {
        const out = await prepareAndStartScalpResearchCycle({
            dryRun: parseBoolParam(req.query.dryRun, false),
            force: parseBoolParam(req.query.force, true),
            includeLiveQuotes: parseBoolParam(req.query.includeLiveQuotes, false),
            runDiscovery: parseBoolParam(req.query.runDiscovery, true),
            finalizeBatch: parseBoolParam(req.query.finalizeBatch, false),
            symbols: parseSymbolsCsv(firstQueryValue(req.query.symbols)),
            batchCursor: parsePositiveInt(firstQueryValue(req.query.batchCursor)),
            maxSymbolsPerRun: parsePositiveInt(firstQueryValue(req.query.maxSymbolsPerRun)),
            lookbackDays: parsePositiveInt(firstQueryValue(req.query.lookbackDays)),
            chunkDays: parsePositiveInt(firstQueryValue(req.query.chunkDays)),
            minCandlesPerTask: parsePositiveInt(firstQueryValue(req.query.minCandlesPerTask)),
            maxTasks: parsePositiveInt(firstQueryValue(req.query.maxTasks)),
            maxAttempts: parsePositiveInt(firstQueryValue(req.query.maxAttempts)),
            runningStaleAfterMs: parsePositiveInt(firstQueryValue(req.query.runningStaleAfterMs)),
            tunerEnabled: req.query.tunerEnabled === undefined ? undefined : parseBoolParam(req.query.tunerEnabled, true),
            maxTuneVariantsPerStrategy: parsePositiveInt(firstQueryValue(req.query.maxTuneVariantsPerStrategy)),
            maxRequestsPerSymbol: parsePositiveInt(firstQueryValue(req.query.maxRequestsPerSymbol)),
            seedTimeframe: firstQueryValue(req.query.seedTimeframe),
            startedBy: firstQueryValue(req.query.startedBy) || 'cron:prepare-and-start-cycle',
        });

        return res.status(out.ok ? 200 : 409).json({
            ok: out.ok,
            started: out.started,
            dryRun: out.dryRun,
            nowMs: out.nowMs,
            nowIso: out.nowIso,
            symbols: out.symbols,
            batch: out.batch,
            lookbackDays: out.lookbackDays,
            maxRequestsPerSymbol: out.maxRequestsPerSymbol,
            steps: out.steps,
            cycle: out.cycle
                ? {
                      cycleId: out.cycle.cycleId,
                      status: out.cycle.status,
                      taskCount: out.cycle.taskIds.length,
                      symbols: out.cycle.symbols,
                      params: out.cycle.params,
                      createdAtMs: out.cycle.createdAtMs,
                      sourceUniverseGeneratedAt: out.cycle.sourceUniverseGeneratedAt,
                  }
                : null,
            snapshot: out.cycle,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'prepare_and_start_cycle_failed',
            message: err?.message || String(err),
        });
    }
}
