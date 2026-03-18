import { Prisma } from '@prisma/client';

import { patchScalpPipelineRuntimeSnapshot, type ScalpPipelineRuntimeOrchestratorSnapshot } from './pipelineRuntime';
import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import { loadScalpPanicStopState } from './panicStop';
import { prepareAndStartScalpResearchCycle } from './prepareAndStartCycle';
import { runScalpSymbolDiscoveryCycle } from './symbolDiscovery';

type OrchestratorStage = 'discover' | 'load_candles' | 'prepare' | 'done';

interface OrchestratorState {
    version: 1;
    runId: string;
    stage: OrchestratorStage;
    loadCursor: number;
    prepareCursor: number;
    selectedSymbols: string[];
    cycleId: string | null;
    loadPassAddedCandles: number;
    loadPassErrors: number;
    loadPasses: number;
    preparePassAddedCandles: number;
    preparePassErrors: number;
    preparePasses: number;
    lockOwner: string | null;
    lockUntilMs: number;
    updatedAtMs: number;
    startedAtMs: number;
    completedAtMs: number | null;
    lastError: string | null;
}

const ORCHESTRATOR_STATE_KIND = 'execute_cycle';
const ORCHESTRATOR_STATE_DEDUPE_KEY = 'scalp_pipeline_orchestrator_state_v1';
const ORCHESTRATOR_MUTEX_DEDUPE_KEY = 'scalp_pipeline_orchestrator_mutex_v1';

function nowMs(): number {
    return Date.now();
}

function asRecord(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

function asState(value: unknown): OrchestratorState | null {
    const row = asRecord(value);
    if (Number(row.version) !== 1) return null;
    const runId = String(row.runId || '').trim();
    const rawStage = String(row.stage || '')
        .trim()
        .toLowerCase();
    const legacyWorkerStage = rawStage === 'worker' || rawStage === 'aggregate' || rawStage === 'promotion';
    const stage = (['discover', 'load_candles', 'prepare', 'done'].includes(rawStage)
        ? rawStage
        : legacyWorkerStage
          ? 'done'
          : '') as OrchestratorStage | '';
    if (!runId || !stage) return null;
    return {
        version: 1,
        runId,
        stage,
        loadCursor: Math.max(0, Math.floor(Number(row.loadCursor) || 0)),
        prepareCursor: Math.max(0, Math.floor(Number(row.prepareCursor) || 0)),
        selectedSymbols: Array.isArray(row.selectedSymbols)
            ? row.selectedSymbols
                  .map((v) => String(v || '').trim().toUpperCase())
                  .filter((v) => Boolean(v))
            : [],
        cycleId: String(row.cycleId || '').trim() || null,
        loadPassAddedCandles: Math.max(0, Math.floor(Number(row.loadPassAddedCandles) || 0)),
        loadPassErrors: Math.max(0, Math.floor(Number(row.loadPassErrors) || 0)),
        loadPasses: Math.max(0, Math.floor(Number(row.loadPasses) || 0)),
        preparePassAddedCandles: Math.max(0, Math.floor(Number(row.preparePassAddedCandles) || 0)),
        preparePassErrors: Math.max(0, Math.floor(Number(row.preparePassErrors) || 0)),
        preparePasses: Math.max(0, Math.floor(Number(row.preparePasses) || 0)),
        lockOwner: String(row.lockOwner || '').trim() || null,
        lockUntilMs: Math.max(0, Math.floor(Number(row.lockUntilMs) || 0)),
        updatedAtMs: Math.max(0, Math.floor(Number(row.updatedAtMs) || 0)),
        startedAtMs: Math.max(0, Math.floor(Number(row.startedAtMs) || 0)),
        completedAtMs:
            Number.isFinite(Number(row.completedAtMs)) && Number(row.completedAtMs) > 0
                ? Math.floor(Number(row.completedAtMs))
                : null,
        lastError: String(row.lastError || '').trim() || null,
    };
}

function newRunId(tsMs: number): string {
    return `scalp_orch_${tsMs}_${Math.floor(Math.random() * 1_000_000)}`;
}

function newState(tsMs: number): OrchestratorState {
    return {
        version: 1,
        runId: newRunId(tsMs),
        stage: 'discover',
        loadCursor: 0,
        prepareCursor: 0,
        selectedSymbols: [],
        cycleId: null,
        loadPassAddedCandles: 0,
        loadPassErrors: 0,
        loadPasses: 0,
        preparePassAddedCandles: 0,
        preparePassErrors: 0,
        preparePasses: 0,
        lockOwner: null,
        lockUntilMs: 0,
        updatedAtMs: tsMs,
        startedAtMs: tsMs,
        completedAtMs: null,
        lastError: null,
    };
}

function pipelineStageMeta(stageRaw: string | null | undefined): {
    progressPct: number | null;
    progressLabel: string | null;
} {
    const stage = String(stageRaw || '')
        .trim()
        .toLowerCase();
    if (!stage) return { progressPct: null, progressLabel: null };
    const map: Record<string, { pct: number; label: string }> = {
        discover: { pct: 10, label: 'discovering symbols' },
        load_candles: { pct: 24, label: 'loading candle history' },
        prepare: { pct: 35, label: 'preparing/backfilling history' },
        done: { pct: 100, label: 'completed' },
    };
    const hit = map[stage];
    if (!hit) return { progressPct: null, progressLabel: stage.replace(/_/g, ' ') };
    return { progressPct: hit.pct, progressLabel: hit.label };
}

function buildRuntimeOrchestratorSnapshot(
    state: OrchestratorState,
): ScalpPipelineRuntimeOrchestratorSnapshot {
    const stageMeta = pipelineStageMeta(state.stage);
    const startedAtMs = state.startedAtMs > 0 ? state.startedAtMs : null;
    const updatedAtMs = state.updatedAtMs > 0 ? state.updatedAtMs : null;
    const completedAtMs = state.completedAtMs && state.completedAtMs > 0 ? state.completedAtMs : null;
    const lastError = state.lastError || null;
    return {
        runId: state.runId || null,
        stage: state.stage || null,
        cycleId: state.cycleId || null,
        startedAtMs,
        updatedAtMs,
        completedAtMs,
        isRunning:
            Boolean(state.stage) &&
            state.stage !== 'done' &&
            startedAtMs !== null &&
            (completedAtMs === null || completedAtMs < startedAtMs) &&
            !lastError,
        progressPct: stageMeta.progressPct,
        progressLabel: stageMeta.progressLabel,
        lastError,
    };
}

function isRecoverablePrepareFailureCodes(codes: string[]): boolean {
    if (!codes.length) return false;
    return codes.every(
        (code) => code === 'insufficient_candles' || code === 'no_symbols_resolved' || code === 'no_symbols_eligible',
    );
}

async function acquireAdvisoryLock(lockOwner: string, lockTtlMs: number): Promise<boolean> {
    if (!isScalpPgConfigured()) return false;
    const db = scalpPrisma();
    await db.$executeRaw(
        Prisma.sql`
            INSERT INTO scalp_jobs(
                kind,
                dedupe_key,
                payload,
                status,
                attempts,
                max_attempts,
                scheduled_for,
                next_run_at,
                locked_by,
                locked_at,
                last_error
            )
            VALUES(
                ${ORCHESTRATOR_STATE_KIND}::scalp_job_kind,
                ${ORCHESTRATOR_MUTEX_DEDUPE_KEY},
                '{}'::jsonb,
                'succeeded'::scalp_job_status,
                1,
                1,
                NOW(),
                NOW(),
                NULL,
                NULL,
                NULL
            )
            ON CONFLICT(kind, dedupe_key)
            DO NOTHING;
        `,
    );
    const rows = await db.$queryRaw<Array<{ id: bigint }>>(Prisma.sql`
        UPDATE scalp_jobs
        SET
            locked_by = ${lockOwner},
            locked_at = NOW(),
            updated_at = NOW()
        WHERE kind = ${ORCHESTRATOR_STATE_KIND}::scalp_job_kind
          AND dedupe_key = ${ORCHESTRATOR_MUTEX_DEDUPE_KEY}
          AND (
              locked_by IS NULL
              OR locked_at IS NULL
              OR locked_at <= NOW() - (${Math.max(60_000, Math.floor(lockTtlMs))} * INTERVAL '1 millisecond')
              OR locked_by = ${lockOwner}
          )
        RETURNING id;
    `);
    return rows.length > 0;
}

async function releaseAdvisoryLock(lockOwner: string): Promise<void> {
    if (!isScalpPgConfigured()) return;
    const db = scalpPrisma();
    await db.$executeRaw(Prisma.sql`
        UPDATE scalp_jobs
        SET
            locked_by = NULL,
            locked_at = NULL,
            updated_at = NOW()
        WHERE kind = ${ORCHESTRATOR_STATE_KIND}::scalp_job_kind
          AND dedupe_key = ${ORCHESTRATOR_MUTEX_DEDUPE_KEY}
          AND locked_by = ${lockOwner};
    `);
}

async function loadOrchestratorState(): Promise<OrchestratorState | null> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{ payload: unknown }>>(Prisma.sql`
        SELECT payload
        FROM scalp_jobs
        WHERE kind = ${ORCHESTRATOR_STATE_KIND}::scalp_job_kind
          AND dedupe_key = ${ORCHESTRATOR_STATE_DEDUPE_KEY}
        LIMIT 1;
    `);
    return asState(rows[0]?.payload);
}

async function saveOrchestratorState(state: OrchestratorState): Promise<void> {
    const db = scalpPrisma();
    await db.$executeRaw(
        Prisma.sql`
            INSERT INTO scalp_jobs(
                kind,
                dedupe_key,
                payload,
                status,
                attempts,
                max_attempts,
                scheduled_for,
                next_run_at,
                last_error
            )
            VALUES(
                ${ORCHESTRATOR_STATE_KIND}::scalp_job_kind,
                ${ORCHESTRATOR_STATE_DEDUPE_KEY},
                ${JSON.stringify(state)}::jsonb,
                'succeeded'::scalp_job_status,
                1,
                1,
                NOW(),
                NOW(),
                NULL
            )
            ON CONFLICT(kind, dedupe_key)
            DO UPDATE SET
                payload = EXCLUDED.payload,
                status = EXCLUDED.status,
                attempts = EXCLUDED.attempts,
                max_attempts = EXCLUDED.max_attempts,
                scheduled_for = EXCLUDED.scheduled_for,
                next_run_at = EXCLUDED.next_run_at,
                locked_by = NULL,
                locked_at = NULL,
                last_error = NULL,
                updated_at = NOW();
        `,
    );
    await patchScalpPipelineRuntimeSnapshot({
        updatedAtMs: state.updatedAtMs,
        orchestrator: buildRuntimeOrchestratorSnapshot(state),
    });
}

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toBool(value: unknown, fallback: boolean): boolean {
    if (typeof value === 'boolean') return value;
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (!normalized) return fallback;
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
}

function classifyOrchestratorDbError(error: unknown): string {
    const message = String((error as any)?.message || error || '')
        .trim()
        .toLowerCase();
    if (!message) return 'db_error';
    if (message.includes('timed out fetching a new connection from the connection pool')) {
        return 'db_pool_timeout';
    }
    if (message.includes("can't reach database server")) {
        return 'db_unreachable';
    }
    if (message.includes('connection pool')) {
        return 'db_pool_error';
    }
    if (message.includes('prisma')) {
        return 'db_prisma_error';
    }
    return 'db_error';
}

function estimateMaxRequestsPerSymbol(lookbackDays: number): number {
    const minutes = Math.max(1, lookbackDays) * 24 * 60;
    // Bitget history-candles returns up to 200 rows/request; include headroom for sparse windows.
    const requests = Math.ceil(minutes / 180) + 24;
    return Math.max(80, Math.min(1500, requests));
}

export interface RunScalpPipelineOrchestratorParams {
    maxDurationMs?: number;
    continueRun?: boolean;
    debug?: boolean;
}

export interface RunScalpPipelineOrchestratorResult {
    ok: boolean;
    status: 'completed' | 'continued' | 'blocked' | 'busy' | 'error';
    message: string;
    runId: string | null;
    stage: OrchestratorStage | null;
    state: OrchestratorState | null;
    diagnostics: {
        startedAtMs: number;
        finishedAtMs: number;
        durationMs: number;
        maxDurationMs: number;
        continuationRequested: boolean;
        continuation: { invoked: boolean; status: number | null; error: string | null } | null;
        stageEvents: Array<Record<string, unknown>>;
    };
}

export async function runScalpPipelineOrchestrator(
    params: RunScalpPipelineOrchestratorParams = {},
): Promise<RunScalpPipelineOrchestratorResult> {
    const debug = params.debug === true;
    const startedAtMs = nowMs();
    const maxDurationMs = Math.max(60_000, Math.min(10 * 60_000, toPositiveInt(params.maxDurationMs, 10 * 60_000)));
    const lockOwnerToken = `scalp_orch_lock_${startedAtMs}_${Math.floor(Math.random() * 1_000_000)}`;
    const lockTtlMs = maxDurationMs + 60_000;
    const safetyMs = 30_000;
    const deadlineMs = startedAtMs + Math.max(10_000, maxDurationMs - safetyMs);
    const stageEvents: Array<Record<string, unknown>> = [];
    const includeBitgetDiscovery = toBool(process.env.SCALP_ORCHESTRATOR_DISCOVERY_INCLUDE_BITGET, true);
    const includeCapitalDiscovery = toBool(process.env.SCALP_ORCHESTRATOR_DISCOVERY_INCLUDE_CAPITAL, false);

    if (!isScalpPgConfigured()) {
        return {
            ok: false,
            status: 'error',
            message: 'scalp_pg_not_configured',
            runId: null,
            stage: null,
            state: null,
            diagnostics: {
                startedAtMs,
                finishedAtMs: nowMs(),
                durationMs: nowMs() - startedAtMs,
                maxDurationMs,
                continuationRequested: false,
                continuation: null,
                stageEvents,
            },
        };
    }

    let locked = false;
    try {
        locked = await acquireAdvisoryLock(lockOwnerToken, lockTtlMs);
    } catch (err: any) {
        const errorCode = classifyOrchestratorDbError(err);
        const errorMessage = String(err?.message || err || 'orchestrator_lock_acquire_failed');
        stageEvents.push({
            stage: 'init',
            blockedBy: 'db_lock_acquire',
            errorCode,
            errorMessage,
        });
        if (debug) {
            console.warn(
                JSON.stringify({
                    scope: 'scalp_orchestrator',
                    event: 'advisory_lock_acquire_failed',
                    errorCode,
                    errorMessage,
                }),
            );
        }
        return {
            ok: false,
            status: 'error',
            message: `orchestrator_lock_acquire_failed:${errorCode}`,
            runId: null,
            stage: null,
            state: null,
            diagnostics: {
                startedAtMs,
                finishedAtMs: nowMs(),
                durationMs: nowMs() - startedAtMs,
                maxDurationMs,
                continuationRequested: false,
                continuation: null,
                stageEvents,
            },
        };
    }
    if (!locked) {
        return {
            ok: true,
            status: 'busy',
            message: 'orchestrator_lock_busy',
            runId: null,
            stage: null,
            state: null,
            diagnostics: {
                startedAtMs,
                finishedAtMs: nowMs(),
                durationMs: nowMs() - startedAtMs,
                maxDurationMs,
                continuationRequested: false,
                continuation: null,
                stageEvents,
            },
        };
    }

    let continuationRequested = false;
    let continuation: { invoked: boolean; status: number | null; error: string | null } | null = null;
    let state: OrchestratorState | null = null;
    let currentStage: OrchestratorStage | 'init' = 'init';
    try {
        const panicStop = await loadScalpPanicStopState();
        if (panicStop.enabled) {
            return {
                ok: false,
                status: 'blocked',
                message: `panic_stop_enabled${panicStop.reason ? `:${panicStop.reason}` : ''}`,
                runId: null,
                stage: null,
                state: null,
                diagnostics: {
                    startedAtMs,
                    finishedAtMs: nowMs(),
                    durationMs: nowMs() - startedAtMs,
                    maxDurationMs,
                    continuationRequested: false,
                    continuation: null,
                    stageEvents: [
                        {
                            stage: 'init',
                            blockedBy: 'panic_stop',
                            reason: panicStop.reason || null,
                            updatedAtMs: panicStop.updatedAtMs,
                        },
                    ],
                },
            };
        }
        state = (await loadOrchestratorState()) || newState(startedAtMs);
        const shouldStartNew = state.stage === 'done' && !params.continueRun;
        if (shouldStartNew) {
            state = newState(startedAtMs);
        }
        state.lockOwner = state.runId;
        state.lockUntilMs = startedAtMs + maxDurationMs + 60_000;
        state.updatedAtMs = startedAtMs;
        state.lastError = null;
        await saveOrchestratorState(state);

        while (nowMs() < deadlineMs) {
            currentStage = state.stage;
            if (state.stage === 'discover') {
                const t0 = nowMs();
                const snapshot = await runScalpSymbolDiscoveryCycle({
                    dryRun: false,
                    includeLiveQuotes: true,
                    seedTopSymbols: includeCapitalDiscovery ? undefined : 0,
                    sourceOverrides: {
                        includeBitgetMarketsApi: includeBitgetDiscovery,
                        includeCapitalMarketsApi: includeCapitalDiscovery,
                    },
                });
                state.stage = 'load_candles';
                state.loadCursor = 0;
                state.loadPassAddedCandles = 0;
                state.loadPassErrors = 0;
                state.loadPasses = 0;
                state.prepareCursor = 0;
                state.preparePassAddedCandles = 0;
                state.preparePassErrors = 0;
                state.preparePasses = 0;
                state.selectedSymbols = snapshot.selectedSymbols.slice();
                state.updatedAtMs = nowMs();
                stageEvents.push({
                    stage: 'discover',
                    durationMs: nowMs() - t0,
                    selectedSymbols: snapshot.selectedSymbols.length,
                    candidatesEvaluated: snapshot.candidatesEvaluated,
                    includeBitgetDiscovery,
                    includeCapitalDiscovery,
                });
                await saveOrchestratorState(state);
                continue;
            }

            if (state.stage === 'load_candles') {
                const remainingMs = Math.max(10_000, deadlineMs - nowMs());
                const lookbackDays = 90;
                const load = await prepareAndStartScalpResearchCycle({
                    dryRun: false,
                    force: false,
                    runDiscovery: false,
                    finalizeBatch: false,
                    fillOnly: true,
                    symbols: state.selectedSymbols,
                    batchCursor: state.loadCursor,
                    maxDurationMs: Math.max(30_000, Math.min(remainingMs - 5_000, 4 * 60_000)),
                    lookbackDays,
                    chunkDays: 7,
                    minCandlesPerTask: 180,
                    maxSymbolsPerRun: 12,
                    maxRequestsPerSymbol: estimateMaxRequestsPerSymbol(lookbackDays),
                    startedBy: 'cron:scalp-orchestrator',
                });
                const addedCandlesThisBatch = load.steps.fill.reduce(
                    (sum, row) => sum + Math.max(0, Math.floor(Number(row.addedCount) || 0)),
                    0,
                );
                const errorsThisBatch = load.steps.fill.reduce((sum, row) => sum + (row.error ? 1 : 0), 0);
                state.loadPassAddedCandles += addedCandlesThisBatch;
                state.loadPassErrors += errorsThisBatch;
                stageEvents.push({
                    stage: 'load_candles',
                    ok: load.ok,
                    nextCursor: load.batch.nextCursor,
                    hasMore: load.batch.hasMore,
                    addedCandlesThisBatch,
                    errorsThisBatch,
                    passAddedCandles: state.loadPassAddedCandles,
                    passErrors: state.loadPassErrors,
                    passNumber: state.loadPasses + 1,
                    processedSymbols: load.batch.processedSymbols.length,
                });
                state.loadCursor = load.batch.nextCursor || 0;
                if (load.batch.hasMore && load.batch.nextCursor !== null) {
                    state.updatedAtMs = nowMs();
                    await saveOrchestratorState(state);
                    if (nowMs() >= deadlineMs) break;
                    continue;
                }

                state.loadPasses += 1;
                state.stage = 'prepare';
                state.updatedAtMs = nowMs();
                await saveOrchestratorState(state);
                continue;
            }

            if (state.stage === 'prepare') {
                const remainingMs = Math.max(10_000, deadlineMs - nowMs());
                const prep = await prepareAndStartScalpResearchCycle({
                    dryRun: false,
                    force: false,
                    runDiscovery: false,
                    finalizeBatch: true,
                    skipFill: true,
                    symbols: state.selectedSymbols,
                    batchCursor: 0,
                    maxDurationMs: Math.max(30_000, Math.min(remainingMs - 5_000, 4 * 60_000)),
                    lookbackDays: 90,
                    chunkDays: 7,
                    minCandlesPerTask: 180,
                    maxSymbolsPerRun: Math.max(1, state.selectedSymbols.length || 1),
                    maxRequestsPerSymbol: 24,
                    startedBy: 'cron:scalp-orchestrator',
                });
                const addedCandlesThisBatch = prep.steps.fill.reduce(
                    (sum, row) => sum + Math.max(0, Math.floor(Number(row.addedCount) || 0)),
                    0,
                );
                const errorsThisBatch = prep.steps.fill.reduce((sum, row) => sum + (row.error ? 1 : 0), 0);
                state.preparePassAddedCandles += addedCandlesThisBatch;
                state.preparePassErrors += errorsThisBatch;
                stageEvents.push({
                    stage: 'prepare',
                    ok: prep.ok,
                    started: prep.started,
                    nextCursor: prep.batch.nextCursor,
                    hasMore: prep.batch.hasMore,
                    addedCandlesThisBatch,
                    errorsThisBatch,
                    passAddedCandles: state.preparePassAddedCandles,
                    passErrors: state.preparePassErrors,
                    passNumber: state.preparePasses + 1,
                    preflightReady: prep.steps.preflight?.ready ?? null,
                    preflightFailures: prep.steps.preflight?.failures || [],
                });
                if (!prep.ok) {
                    const failureCodes = (prep.steps.preflight?.failures || [])
                        .map((row) => String(row.code || '').trim())
                        .filter((row) => Boolean(row));
                    const failureCodeText = failureCodes.join(',');
                    const recoverable = isRecoverablePrepareFailureCodes(failureCodes);
                    if (recoverable && prep.batch.finalized) {
                        const canRestartBackfillPass =
                            state.loadPasses < 2 && (state.loadPassAddedCandles > 0 || state.loadPassErrors > 0);
                        if (canRestartBackfillPass) {
                            stageEvents.push({
                                stage: 'prepare',
                                action: 'restart_backfill_pass',
                                reason: failureCodeText || 'recoverable_preflight_block',
                                loadPassesCompleted: state.loadPasses,
                                loadPassAddedCandles: state.loadPassAddedCandles,
                                loadPassErrors: state.loadPassErrors,
                                nextLoadPass: state.loadPasses + 1,
                            });
                            state.preparePasses += 1;
                            state.stage = 'load_candles';
                            state.loadCursor = 0;
                            state.prepareCursor = 0;
                            state.loadPassAddedCandles = 0;
                            state.loadPassErrors = 0;
                            state.preparePassAddedCandles = 0;
                            state.preparePassErrors = 0;
                            state.updatedAtMs = nowMs();
                            await saveOrchestratorState(state);
                            if (nowMs() >= deadlineMs) break;
                            continue;
                        }
                    }

                    state.lastError = failureCodeText || 'unknown';
                    state.updatedAtMs = nowMs();
                    await saveOrchestratorState(state);
                    return {
                        ok: false,
                        status: 'blocked',
                        message: `preflight_blocked:${state.lastError}`,
                        runId: state.runId,
                        stage: state.stage,
                        state,
                        diagnostics: {
                            startedAtMs,
                            finishedAtMs: nowMs(),
                            durationMs: nowMs() - startedAtMs,
                            maxDurationMs,
                            continuationRequested: false,
                            continuation: null,
                            stageEvents,
                        },
                    };
                }

                state.prepareCursor = prep.batch.nextCursor || 0;
                if (prep.batch.hasMore && prep.batch.nextCursor !== null) {
                    state.updatedAtMs = nowMs();
                    await saveOrchestratorState(state);
                    if (nowMs() >= deadlineMs) break;
                    continue;
                }

                state.cycleId = prep.cycle?.cycleId || null;
                state.preparePasses += 1;
                state.preparePassAddedCandles = 0;
                state.preparePassErrors = 0;
                state.stage = 'done';
                state.completedAtMs = nowMs();
                state.updatedAtMs = nowMs();
                await saveOrchestratorState(state);
                continue;
            }

            break;
        }

        if (state.stage !== 'done') {
            continuationRequested = true;
            stageEvents.push({
                stage: 'incomplete',
                continuationRequested,
                continuation,
                maxDurationMs,
            });
            state.updatedAtMs = nowMs();
            await saveOrchestratorState(state);
            return {
                ok: true,
                status: 'continued',
                message: `orchestrator_incomplete_at_stage:${state.stage}`,
                runId: state.runId,
                stage: state.stage,
                state,
                diagnostics: {
                    startedAtMs,
                    finishedAtMs: nowMs(),
                    durationMs: nowMs() - startedAtMs,
                    maxDurationMs,
                    continuationRequested,
                    continuation,
                    stageEvents,
                },
            };
        }

        return {
            ok: true,
            status: 'completed',
            message: 'orchestrator_completed',
            runId: state.runId,
            stage: state.stage,
            state,
            diagnostics: {
                startedAtMs,
                finishedAtMs: nowMs(),
                durationMs: nowMs() - startedAtMs,
                maxDurationMs,
                continuationRequested,
                continuation,
                stageEvents,
            },
        };
    } catch (err: any) {
        const rawError = String(err?.message || err || 'orchestrator_failed');
        const stageTaggedError = `orchestrator_failed_at_${currentStage}:${rawError}`.slice(0, 400);
        if (state) {
            state.lastError = stageTaggedError;
            state.updatedAtMs = nowMs();
            try {
                await saveOrchestratorState(state);
            } catch (saveErr: any) {
                if (debug) {
                    console.warn(
                        JSON.stringify({
                            scope: 'scalp_orchestrator',
                            event: 'state_save_failed_after_error',
                            saveErrorCode: classifyOrchestratorDbError(saveErr),
                            saveErrorMessage: String(saveErr?.message || saveErr || 'state_save_failed'),
                        }),
                    );
                }
            }
        }
        return {
            ok: false,
            status: 'error',
            message: stageTaggedError,
            runId: state?.runId || null,
            stage: state?.stage || null,
            state,
            diagnostics: {
                startedAtMs,
                finishedAtMs: nowMs(),
                durationMs: nowMs() - startedAtMs,
                maxDurationMs,
                continuationRequested,
                continuation,
                stageEvents,
            },
        };
    } finally {
        try {
            await releaseAdvisoryLock(lockOwnerToken);
        } catch (releaseErr: any) {
            console.warn(
                JSON.stringify({
                    scope: 'scalp_orchestrator',
                    event: 'advisory_lock_release_failed',
                    errorCode: classifyOrchestratorDbError(releaseErr),
                    errorMessage: String(releaseErr?.message || releaseErr || 'advisory_lock_release_failed'),
                }),
            );
        }
    }
}
