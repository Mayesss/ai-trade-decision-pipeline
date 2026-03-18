export const config = { runtime: 'nodejs', maxDuration: 600 };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { invokeCronEndpoint } from '../../../../lib/scalp/cronChaining';
import { patchScalpPipelineRuntimeSnapshot } from '../../../../lib/scalp/pipelineRuntime';
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

function buildStageMeta(stageRaw: string): { progressPct: number | null; progressLabel: string | null } {
    const stage = String(stageRaw || '')
        .trim()
        .toLowerCase();
    const map: Record<string, { progressPct: number; progressLabel: string }> = {
        discover: { progressPct: 10, progressLabel: 'discovering symbols' },
        load_candles: { progressPct: 24, progressLabel: 'loading candle history' },
        prepare: { progressPct: 35, progressLabel: 'preparing/backfilling history' },
        worker: { progressPct: 70, progressLabel: 'running cycle worker' },
        aggregate: { progressPct: 88, progressLabel: 'aggregating cycle results' },
        promotion: { progressPct: 96, progressLabel: 'applying promotion gate' },
        done: { progressPct: 100, progressLabel: 'completed' },
    };
    return map[stage] || { progressPct: null, progressLabel: stage || null };
}

async function persistPrepareStartHeroState(params: {
    dryRun: boolean;
    runId: string;
    stage: 'load_candles' | 'prepare' | 'worker' | 'done';
    startedAtMs: number;
    updatedAtMs: number;
    cycleId?: string | null;
    lastError?: string | null;
    isRunning?: boolean;
}): Promise<void> {
    if (params.dryRun) return;
    const meta = buildStageMeta(params.stage);
    const isRunning =
        typeof params.isRunning === 'boolean'
            ? params.isRunning
            : params.stage !== 'done' && !params.lastError;
    await patchScalpPipelineRuntimeSnapshot({
        updatedAtMs: params.updatedAtMs,
        orchestrator: {
            runId: params.runId,
            stage: params.stage,
            cycleId: String(params.cycleId || '').trim() || null,
            startedAtMs: params.startedAtMs,
            updatedAtMs: params.updatedAtMs,
            completedAtMs: params.stage === 'done' ? params.updatedAtMs : null,
            isRunning,
            progressPct: meta.progressPct,
            progressLabel: meta.progressLabel,
            lastError: String(params.lastError || '').trim() || null,
        },
    });
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    try {
        const debug = parseBoolParam(req.query.debug ?? req.query.dubg, false);
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
        const plannerEnabled =
            req.query.plannerEnabled === undefined ? undefined : parseBoolParam(req.query.plannerEnabled, true);
        const maxRequestsPerSymbol = parsePositiveInt(firstQueryValue(req.query.maxRequestsPerSymbol));
        const maxDurationMs = parsePositiveInt(firstQueryValue(req.query.maxDurationMs));
        const seedTimeframe = firstQueryValue(req.query.seedTimeframe);
        const startedBy = firstQueryValue(req.query.startedBy) || 'cron:prepare-and-start-cycle';
        const pipelineRunId =
            firstQueryValue(req.query.pipelineRunId) ||
            `scalp_prepare_start_${Date.now()}_${Math.floor(Math.random() * 1_000_000)}`;
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
        const requestStartedAtMs = Date.now();
        const initialStage = finalizeBatch || firstQueryValue(req.query.skipFill) === 'true' ? 'prepare' : 'load_candles';

        await persistPrepareStartHeroState({
            dryRun,
            runId: pipelineRunId,
            stage: initialStage,
            startedAtMs: requestStartedAtMs,
            updatedAtMs: requestStartedAtMs,
            isRunning: true,
        });

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
            plannerEnabled,
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
                  plannerEnabled,
                  maxRequestsPerSymbol,
                  maxDurationMs,
                  seedTimeframe,
                  startedBy,
                  pipelineRunId,
                  autoContinue: true,
                  continueHop: continueHop + 1,
                  autoContinueMaxHops,
                  autoSuccessor,
                  successorPath,
                  debug,
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
                  debug,
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

        const runningCycleStatus = String(out.cycle?.status || '').trim().toLowerCase();
        const heroStage = out.ok
            ? out.started || runningCycleStatus === 'running'
                ? 'worker'
                : out.batch.hasMore || out.batch.finalized === false
                  ? 'load_candles'
                  : 'prepare'
            : 'prepare';
        const heroError = out.ok
            ? null
            : notReadyReasons.join(',') || 'research_cycle_preflight_failed';
        const heroIsRunning = out.ok
            ? shouldAutoContinue || shouldCallSuccessor
            : false;
        await persistPrepareStartHeroState({
            dryRun,
            runId: pipelineRunId,
            stage: heroStage,
            startedAtMs: requestStartedAtMs,
            updatedAtMs: Date.now(),
            cycleId: out.cycle?.cycleId || null,
            lastError: heroError,
            isRunning: heroIsRunning,
        });

        const statusCode = out.ok ? 200 : 409;
        if (debug || !out.ok) {
            console.info(
                JSON.stringify({
                    scope: 'scalp_prepare_and_start_cycle_api',
                    event: out.ok ? 'prepare_complete' : 'prepare_blocked',
                    debug,
                    dryRun,
                    force,
                    started: out.started,
                    cycleId: out.cycle?.cycleId || null,
                    selectedSymbols: out.symbols.length,
                    processedSymbols: out.batch.processedSymbols.length,
                    batch: out.batch,
                    preflightReady: out.steps.preflight?.ready ?? null,
                    preflightFailures: out.steps.preflight?.failures || [],
                    fillRows: out.steps.fill.length,
                    fillAddedCandles: out.steps.fill.reduce(
                        (sum, row) => sum + Math.max(0, Math.floor(Number(row.addedCount) || 0)),
                        0,
                    ),
                    fillErrors: out.steps.fill.filter((row) => Boolean(row.error)).map((row) => ({
                        symbol: row.symbol,
                        error: row.error,
                    })),
                    chaining: {
                        autoContinue: shouldAutoContinue,
                        continuation,
                        autoSuccessor: shouldCallSuccessor,
                        successor,
                    },
                }),
            );
        }
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
        const dryRun = parseBoolParam(req.query.dryRun, false);
        const pipelineRunId =
            firstQueryValue(req.query.pipelineRunId) ||
            `scalp_prepare_start_${Date.now()}_${Math.floor(Math.random() * 1_000_000)}`;
        const updatedAtMs = Date.now();
        await persistPrepareStartHeroState({
            dryRun,
            runId: pipelineRunId,
            stage: 'prepare',
            startedAtMs: updatedAtMs,
            updatedAtMs,
            lastError: err?.message || String(err),
            isRunning: false,
        });
        if (String(err?.code || '') === 'research_cycle_lookback_below_minimum') {
            const minimumLookbackDays = Number(err?.minimumLookbackDays) || 84;
            const requestedLookbackDays = Number(err?.requestedLookbackDays) || null;
            return res.status(400).json({
                error: 'research_cycle_lookback_below_minimum',
                message: `lookbackDays must be at least ${minimumLookbackDays}.`,
                minimumLookbackDays,
                requestedLookbackDays,
            });
        }
        return res.status(500).json({
            error: 'prepare_and_start_cycle_failed',
            message: err?.message || String(err),
        });
    }
}
