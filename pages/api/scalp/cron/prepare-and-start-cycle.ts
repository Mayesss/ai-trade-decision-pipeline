export const config = { runtime: 'nodejs', maxDuration: 600 };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { invokeCronEndpoint } from '../../../../lib/scalp/cronChaining';
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
        const dryRun = parseBoolParam(req.query.dryRun, false);
        const force = parseBoolParam(req.query.force, true);
        const includeLiveQuotes = parseBoolParam(req.query.includeLiveQuotes, false);
        const runDiscovery = parseBoolParam(req.query.runDiscovery, true);
        const finalizeBatch = parseBoolParam(req.query.finalizeBatch, false);
        const symbols = parseSymbolsCsv(firstQueryValue(req.query.symbols));
        const batchCursor = parsePositiveInt(firstQueryValue(req.query.batchCursor));
        const maxSymbolsPerRun = parsePositiveInt(firstQueryValue(req.query.maxSymbolsPerRun));
        const lookbackDays = parsePositiveInt(firstQueryValue(req.query.lookbackDays));
        const chunkDays = parsePositiveInt(firstQueryValue(req.query.chunkDays));
        const minCandlesPerTask = parsePositiveInt(firstQueryValue(req.query.minCandlesPerTask));
        const maxTasks = parsePositiveInt(firstQueryValue(req.query.maxTasks));
        const maxAttempts = parsePositiveInt(firstQueryValue(req.query.maxAttempts));
        const runningStaleAfterMs = parsePositiveInt(firstQueryValue(req.query.runningStaleAfterMs));
        const tunerEnabled = req.query.tunerEnabled === undefined ? undefined : parseBoolParam(req.query.tunerEnabled, true);
        const maxTuneVariantsPerStrategy = parsePositiveInt(firstQueryValue(req.query.maxTuneVariantsPerStrategy));
        const maxRequestsPerSymbol = parsePositiveInt(firstQueryValue(req.query.maxRequestsPerSymbol));
        const maxDurationMs = parsePositiveInt(firstQueryValue(req.query.maxDurationMs));
        const seedTimeframe = firstQueryValue(req.query.seedTimeframe);
        const startedBy = firstQueryValue(req.query.startedBy) || 'cron:prepare-and-start-cycle';
        const autoContinue = parseBoolParam(req.query.autoContinue, true);
        const continueHop = Math.max(0, Math.floor(Number(firstQueryValue(req.query.continueHop)) || 0));
        const autoContinueMaxHops = Math.max(
            0,
            Math.min(
                10,
                Math.floor(
                    Number(firstQueryValue(req.query.autoContinueMaxHops)) ||
                        Number(process.env.SCALP_PREPARE_START_AUTO_CONTINUE_MAX_HOPS) ||
                        8,
                ),
            ),
        );
        const autoSuccessor = parseBoolParam(req.query.autoSuccessor, true);
        const successorPath = firstQueryValue(req.query.successorPath) || '/api/scalp/cron/research-cycle-worker';

        const out = await prepareAndStartScalpResearchCycle({
            dryRun,
            force,
            includeLiveQuotes,
            runDiscovery,
            finalizeBatch,
            symbols,
            batchCursor,
            maxSymbolsPerRun,
            lookbackDays,
            chunkDays,
            minCandlesPerTask,
            maxTasks,
            maxAttempts,
            runningStaleAfterMs,
            tunerEnabled,
            maxTuneVariantsPerStrategy,
            maxRequestsPerSymbol,
            maxDurationMs,
            seedTimeframe,
            startedBy,
        });
        const shouldAutoContinue =
            autoContinue && out.batch.hasMore && out.batch.nextCursor !== null && continueHop < autoContinueMaxHops;
        const continuation = shouldAutoContinue
            ? await invokeCronEndpoint(req, '/api/scalp/cron/prepare-and-start-cycle', {
                  dryRun,
                  force,
                  includeLiveQuotes,
                  runDiscovery,
                  finalizeBatch,
                  symbols: symbols.length > 0 ? symbols.join(',') : undefined,
                  batchCursor: out.batch.nextCursor,
                  maxSymbolsPerRun,
                  lookbackDays,
                  chunkDays,
                  minCandlesPerTask,
                  maxTasks,
                  maxAttempts,
                  runningStaleAfterMs,
                  tunerEnabled,
                  maxTuneVariantsPerStrategy,
                  maxRequestsPerSymbol,
                  maxDurationMs,
                  seedTimeframe,
                  startedBy,
                  autoContinue: true,
                  continueHop: continueHop + 1,
                  autoContinueMaxHops,
                  autoSuccessor,
                  successorPath,
              })
            : null;
        const shouldCallSuccessor =
            !shouldAutoContinue && autoSuccessor && out.ok && out.started && !dryRun && Boolean(out.cycle?.cycleId);
        const successor = shouldCallSuccessor
            ? await invokeCronEndpoint(req, successorPath, {
                  cycleId: out.cycle?.cycleId,
                  autoContinue: 1,
                  continueHop: 0,
                  startedBy: 'cron:prepare-and-start-cycle:successor',
              })
            : null;

        const notReadyReasons = (out.steps.preflight?.failures || []).map((row) =>
            String(row?.code || '')
                .trim()
                .toLowerCase(),
        );
        const message = out.ok
            ? out.started
                ? 'Cycle prepared and started.'
                : 'Cycle preparation completed (no start needed yet).'
            : `Cycle preparation completed but preflight is not ready: ${notReadyReasons.join(', ') || 'unknown_reason'}.`;

        const statusCode = out.ok ? 200 : 409;
        return res.status(statusCode).json({
            ok: out.ok,
            started: out.started,
            message,
            status: out.ok ? (out.started ? 'started' : 'prepared') : 'preflight_blocked',
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
            chaining: {
                autoContinue: {
                    enabled: autoContinue,
                    continueHop,
                    maxHops: autoContinueMaxHops,
                    requested: shouldAutoContinue,
                    continuation,
                },
                autoSuccessor: {
                    enabled: autoSuccessor,
                    successorPath,
                    requested: shouldCallSuccessor,
                    successor,
                },
            },
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'prepare_and_start_cycle_failed',
            message: err?.message || String(err),
        });
    }
}
