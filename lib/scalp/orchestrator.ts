import { Prisma } from '@prisma/client';

import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import { loadScalpPanicStopState } from './panicStop';
import { prepareAndStartScalpResearchCycle } from './prepareAndStartCycle';
import { aggregateScalpResearchCycle, loadActiveResearchCycleId, loadResearchCycle, runResearchWorker } from './researchCycle';
import { syncResearchCyclePromotionGates } from './researchPromotion';
import { runScalpSymbolDiscoveryCycle } from './symbolDiscovery';

type OrchestratorStage = 'discover' | 'prepare' | 'worker' | 'aggregate' | 'promotion' | 'done';

interface OrchestratorState {
    version: 1;
    runId: string;
    stage: OrchestratorStage;
    prepareCursor: number;
    selectedSymbols: string[];
    cycleId: string | null;
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
const ORCHESTRATOR_ADVISORY_LOCK_KEY = 86753091;

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
    const stage = String(row.stage || '').trim() as OrchestratorStage;
    if (!runId || !['discover', 'prepare', 'worker', 'aggregate', 'promotion', 'done'].includes(stage)) return null;
    return {
        version: 1,
        runId,
        stage,
        prepareCursor: Math.max(0, Math.floor(Number(row.prepareCursor) || 0)),
        selectedSymbols: Array.isArray(row.selectedSymbols)
            ? row.selectedSymbols
                  .map((v) => String(v || '').trim().toUpperCase())
                  .filter((v) => Boolean(v))
            : [],
        cycleId: String(row.cycleId || '').trim() || null,
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
        prepareCursor: 0,
        selectedSymbols: [],
        cycleId: null,
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

function isRecoverablePrepareFailureCodes(codes: string[]): boolean {
    if (!codes.length) return false;
    return codes.every((code) => code === 'insufficient_candles' || code === 'no_symbols_resolved');
}

async function acquireAdvisoryLock(): Promise<boolean> {
    if (!isScalpPgConfigured()) return false;
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{ locked: boolean }>>(Prisma.sql`
        SELECT pg_try_advisory_lock(${ORCHESTRATOR_ADVISORY_LOCK_KEY}) AS locked;
    `);
    return rows[0]?.locked === true;
}

async function releaseAdvisoryLock(): Promise<void> {
    if (!isScalpPgConfigured()) return;
    const db = scalpPrisma();
    await db.$executeRaw(Prisma.sql`
        SELECT pg_advisory_unlock(${ORCHESTRATOR_ADVISORY_LOCK_KEY});
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
}

function resolveBaseUrl(): string | null {
    const explicit = String(process.env.SCALP_ORCHESTRATOR_BASE_URL || '').trim();
    if (explicit) return explicit.replace(/\/+$/, '');
    const appBase = String(process.env.APP_BASE_URL || process.env.URL || '').trim();
    if (appBase) return appBase.replace(/\/+$/, '');
    const vercelUrl = String(process.env.VERCEL_URL || '').trim();
    if (vercelUrl) return `https://${vercelUrl.replace(/\/+$/, '')}`;
    return null;
}

async function invokeContinuation(params: { debug?: boolean } = {}): Promise<{ invoked: boolean; status: number | null; error: string | null }> {
    const baseUrl = resolveBaseUrl();
    if (!baseUrl) {
        return { invoked: false, status: null, error: 'missing_base_url' };
    }
    const url = `${baseUrl}/api/scalp/cron/orchestrate-pipeline?continue=1`;
    const headers: Record<string, string> = {};
    const adminSecret = String(process.env.ADMIN_ACCESS_SECRET || '').trim();
    if (adminSecret) {
        headers['x-admin-access-secret'] = adminSecret;
    }
    const ctrl = new AbortController();
    const timeout = setTimeout(() => ctrl.abort(), 4_000);
    try {
        const res = await fetch(url, {
            method: 'GET',
            headers,
            cache: 'no-store',
            signal: ctrl.signal,
        });
        return { invoked: true, status: res.status, error: res.ok ? null : `http_${res.status}` };
    } catch (err: any) {
        return { invoked: false, status: null, error: String(err?.message || err || 'invoke_failed') };
    } finally {
        clearTimeout(timeout);
        if (params.debug) {
            // no-op placeholder for optional debug hook
        }
    }
}

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function isTerminalCycleStatus(status: string | null | undefined): boolean {
    const normalized = String(status || '')
        .trim()
        .toLowerCase();
    return normalized === 'completed' || normalized === 'failed' || normalized === 'stalled';
}

export interface RunScalpPipelineOrchestratorParams {
    maxDurationMs?: number;
    selfInvokeOnContinue?: boolean;
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
    const startedAtMs = nowMs();
    const maxDurationMs = Math.max(60_000, Math.min(10 * 60_000, toPositiveInt(params.maxDurationMs, 10 * 60_000)));
    const safetyMs = 30_000;
    const deadlineMs = startedAtMs + Math.max(10_000, maxDurationMs - safetyMs);
    const stageEvents: Array<Record<string, unknown>> = [];

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

    const locked = await acquireAdvisoryLock();
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
            if (state.stage === 'discover') {
                const t0 = nowMs();
                const snapshot = await runScalpSymbolDiscoveryCycle({
                    dryRun: false,
                    includeLiveQuotes: true,
                });
                state.stage = 'prepare';
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
                });
                await saveOrchestratorState(state);
                continue;
            }

            if (state.stage === 'prepare') {
                const remainingMs = Math.max(10_000, deadlineMs - nowMs());
                const prep = await prepareAndStartScalpResearchCycle({
                    dryRun: false,
                    force: false,
                    runDiscovery: false,
                    finalizeBatch: false,
                    symbols: state.selectedSymbols,
                    batchCursor: state.prepareCursor,
                    maxDurationMs: Math.max(30_000, Math.min(remainingMs - 5_000, 4 * 60_000)),
                    lookbackDays: 90,
                    chunkDays: 14,
                    minCandlesPerTask: 180,
                    maxSymbolsPerRun: 12,
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
                        state.preparePasses += 1;
                        if (state.preparePassAddedCandles > 0) {
                            stageEvents.push({
                                stage: 'prepare',
                                action: 'restart_backfill_pass',
                                reason: failureCodeText || 'recoverable_preflight_block',
                                passAddedCandles: state.preparePassAddedCandles,
                                passErrors: state.preparePassErrors,
                                nextPass: state.preparePasses + 1,
                            });
                            state.prepareCursor = 0;
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

                state.cycleId = prep.cycle?.cycleId || (await loadActiveResearchCycleId()) || null;
                state.preparePasses += 1;
                state.preparePassAddedCandles = 0;
                state.preparePassErrors = 0;
                state.stage = 'worker';
                state.updatedAtMs = nowMs();
                await saveOrchestratorState(state);
                continue;
            }

            if (state.stage === 'worker') {
                const remainingMs = Math.max(10_000, deadlineMs - nowMs());
                const worker = await runResearchWorker({
                    cycleId: state.cycleId || undefined,
                    maxRuns: 200,
                    concurrency: 16,
                    maxDurationMs: Math.max(30_000, Math.min(remainingMs - 5_000, 2 * 60_000)),
                });
                state.cycleId = worker.cycleId || state.cycleId;
                stageEvents.push({
                    stage: 'worker',
                    cycleId: worker.cycleId,
                    attemptedRuns: worker.attemptedRuns,
                    completedRuns: worker.completedRuns,
                    failedRuns: worker.failedRuns,
                    gate: worker.orchestration.gate,
                    reasonCodes: worker.orchestration.reasonCodes,
                    noClaimScanSummary: worker.noClaimScanSummary,
                });
                if (worker.orchestration.gate === 'blocked') {
                    state.lastError = worker.orchestration.reasonCodes.join(',') || 'worker_preflight_blocked';
                    state.updatedAtMs = nowMs();
                    await saveOrchestratorState(state);
                    return {
                        ok: false,
                        status: 'blocked',
                        message: `worker_blocked:${state.lastError}`,
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
                state.stage = 'aggregate';
                state.updatedAtMs = nowMs();
                await saveOrchestratorState(state);
                continue;
            }

            if (state.stage === 'aggregate') {
                const aggregate = await aggregateScalpResearchCycle({
                    cycleId: state.cycleId || undefined,
                    finalizeWhenDone: true,
                });
                stageEvents.push({
                    stage: 'aggregate',
                    found: Boolean(aggregate),
                    cycleId: aggregate?.cycle?.cycleId || state.cycleId || null,
                    status: aggregate?.summary?.status || null,
                    totals: aggregate?.summary?.totals || null,
                });
                if (!aggregate) {
                    state.stage = 'done';
                    state.completedAtMs = nowMs();
                    state.updatedAtMs = nowMs();
                    await saveOrchestratorState(state);
                    continue;
                }
                state.cycleId = aggregate.cycle.cycleId;
                state.stage = isTerminalCycleStatus(aggregate.summary.status) ? 'promotion' : 'worker';
                state.updatedAtMs = nowMs();
                await saveOrchestratorState(state);
                continue;
            }

            if (state.stage === 'promotion') {
                const out = await syncResearchCyclePromotionGates({
                    cycleId: state.cycleId || undefined,
                    dryRun: false,
                    requireCompletedCycle: true,
                    materializeEnabled: true,
                    updatedBy: 'cron:scalp-orchestrator',
                });
                stageEvents.push({
                    stage: 'promotion',
                    ok: out.ok,
                    reason: out.reason,
                    deploymentsConsidered: out.deploymentsConsidered,
                    deploymentsMatched: out.deploymentsMatched,
                    deploymentsUpdated: out.deploymentsUpdated,
                });
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
            if (params.selfInvokeOnContinue !== false) {
                continuation = await invokeContinuation({ debug: params.debug });
            }
            state.updatedAtMs = nowMs();
            await saveOrchestratorState(state);
            return {
                ok: true,
                status: 'continued',
                message: `orchestrator_continues_at_stage:${state.stage}`,
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
        if (state) {
            state.lastError = String(err?.message || err || 'orchestrator_failed').slice(0, 400);
            state.updatedAtMs = nowMs();
            await saveOrchestratorState(state);
        }
        return {
            ok: false,
            status: 'error',
            message: String(err?.message || err || 'orchestrator_failed'),
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
        await releaseAdvisoryLock();
    }
}
