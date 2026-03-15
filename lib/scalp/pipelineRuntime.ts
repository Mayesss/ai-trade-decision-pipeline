import { Prisma } from '@prisma/client';

import { isScalpPgConfigured, scalpPrisma } from './pg/client';

const PIPELINE_RUNTIME_KIND = 'execute_cycle';
const PIPELINE_RUNTIME_DEDUPE_KEY = 'scalp_pipeline_runtime_v1';

export type ScalpPipelineRuntimeOrchestratorSnapshot = {
    runId: string | null;
    stage: string | null;
    cycleId: string | null;
    startedAtMs: number | null;
    updatedAtMs: number | null;
    completedAtMs: number | null;
    isRunning: boolean;
    progressPct: number | null;
    progressLabel: string | null;
    lastError: string | null;
};

export type ScalpPipelineRuntimePromotionSyncSnapshot = {
    status: 'queued' | 'running' | 'succeeded' | 'failed' | null;
    cycleId: string | null;
    phase: string | null;
    startedAtMs: number | null;
    updatedAtMs: number | null;
    finishedAtMs: number | null;
    totalDeployments: number | null;
    processedDeployments: number | null;
    matchedDeployments: number | null;
    updatedDeployments: number | null;
    currentSymbol: string | null;
    currentStrategyId: string | null;
    currentTuneId: string | null;
    reason: string | null;
    lastError: string | null;
    lastCompletedCycleId: string | null;
    lastCompletedAtMs: number | null;
};

type ScalpPipelineRuntimePromotionCompletionSnapshot = {
    cycleId: string;
    syncedAtMs: number;
    deploymentsConsidered: number;
    deploymentsMatched: number;
    deploymentsUpdated: number;
};

export type ScalpPipelineRuntimeSnapshot = {
    version: 1;
    updatedAtMs: number;
    orchestrator: ScalpPipelineRuntimeOrchestratorSnapshot | null;
    promotionSync: ScalpPipelineRuntimePromotionSyncSnapshot | null;
};

type ScalpPipelineRuntimePatch = {
    updatedAtMs?: number | null;
    orchestrator?: Partial<ScalpPipelineRuntimeOrchestratorSnapshot> | null;
    promotionSync?: Partial<ScalpPipelineRuntimePromotionSyncSnapshot> | null;
    promotionSyncCompletion?: Partial<ScalpPipelineRuntimePromotionCompletionSnapshot> | null;
};

function asRecord(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

function normalizeOptionalText(value: unknown, maxLen: number, transform?: (value: string) => string): string | null {
    const raw = String(value || '').trim();
    if (!raw) return null;
    const normalized = transform ? transform(raw) : raw;
    return normalized ? normalized.slice(0, maxLen) : null;
}

function normalizeTsMs(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return null;
    return Math.floor(n);
}

function normalizeNonNegativeInt(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 0) return null;
    return Math.floor(n);
}

function normalizeProgressPct(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    return Math.max(0, Math.min(100, n));
}

function parseBool(value: unknown): boolean | null {
    if (typeof value === 'boolean') return value;
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (!normalized) return null;
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return null;
}

export function normalizeScalpPipelineRuntimeOrchestrator(
    value: unknown,
): ScalpPipelineRuntimeOrchestratorSnapshot | null {
    const row = asRecord(value);
    const runId = normalizeOptionalText(row.runId, 120);
    const stage = normalizeOptionalText(row.stage, 60, (text) => text.toLowerCase());
    const cycleId = normalizeOptionalText(row.cycleId, 120);
    const startedAtMs = normalizeTsMs(row.startedAtMs);
    const updatedAtMs = normalizeTsMs(row.updatedAtMs);
    const completedAtMs = normalizeTsMs(row.completedAtMs);
    const lastError = normalizeOptionalText(row.lastError, 400);
    const progressPct = normalizeProgressPct(row.progressPct);
    const progressLabel = normalizeOptionalText(row.progressLabel, 240);
    const explicitIsRunning = parseBool(row.isRunning);
    const inferredIsRunning =
        Boolean(stage) &&
        stage !== 'done' &&
        startedAtMs !== null &&
        (completedAtMs === null || completedAtMs < startedAtMs) &&
        !lastError;
    if (
        !runId &&
        !stage &&
        !cycleId &&
        startedAtMs === null &&
        updatedAtMs === null &&
        completedAtMs === null &&
        lastError === null &&
        progressPct === null &&
        progressLabel === null &&
        explicitIsRunning === null
    ) {
        return null;
    }
    return {
        runId,
        stage,
        cycleId,
        startedAtMs,
        updatedAtMs,
        completedAtMs,
        isRunning: explicitIsRunning ?? inferredIsRunning,
        progressPct,
        progressLabel,
        lastError,
    };
}

export function normalizeScalpPipelineRuntimePromotionSync(
    value: unknown,
): ScalpPipelineRuntimePromotionSyncSnapshot | null {
    const row = asRecord(value);
    const statusRaw = normalizeOptionalText(row.status, 40, (text) => text.toLowerCase());
    const status =
        statusRaw === 'queued' || statusRaw === 'running' || statusRaw === 'succeeded' || statusRaw === 'failed'
            ? statusRaw
            : null;
    const cycleId = normalizeOptionalText(row.cycleId, 120);
    const phase = normalizeOptionalText(row.phase, 160);
    const startedAtMs = normalizeTsMs(row.startedAtMs);
    const updatedAtMs = normalizeTsMs(row.updatedAtMs);
    const finishedAtMs = normalizeTsMs(row.finishedAtMs);
    const totalDeployments = normalizeNonNegativeInt(row.totalDeployments);
    const processedDeployments = normalizeNonNegativeInt(row.processedDeployments);
    const matchedDeployments = normalizeNonNegativeInt(row.matchedDeployments);
    const updatedDeployments = normalizeNonNegativeInt(row.updatedDeployments);
    const currentSymbol = normalizeOptionalText(row.currentSymbol, 60, (text) => text.toUpperCase());
    const currentStrategyId = normalizeOptionalText(row.currentStrategyId, 120, (text) => text.toLowerCase());
    const currentTuneId = normalizeOptionalText(row.currentTuneId, 120, (text) => text.toLowerCase());
    const reason = normalizeOptionalText(row.reason, 240);
    const lastError = normalizeOptionalText(row.lastError, 400);
    const lastCompletedCycleId = normalizeOptionalText(row.lastCompletedCycleId, 120);
    const lastCompletedAtMs = normalizeTsMs(row.lastCompletedAtMs);
    if (
        status === null &&
        cycleId === null &&
        phase === null &&
        startedAtMs === null &&
        updatedAtMs === null &&
        finishedAtMs === null &&
        totalDeployments === null &&
        processedDeployments === null &&
        matchedDeployments === null &&
        updatedDeployments === null &&
        currentSymbol === null &&
        currentStrategyId === null &&
        currentTuneId === null &&
        reason === null &&
        lastError === null &&
        lastCompletedCycleId === null &&
        lastCompletedAtMs === null
    ) {
        return null;
    }
    return {
        status,
        cycleId,
        phase,
        startedAtMs,
        updatedAtMs,
        finishedAtMs,
        totalDeployments,
        processedDeployments,
        matchedDeployments,
        updatedDeployments,
        currentSymbol,
        currentStrategyId,
        currentTuneId,
        reason,
        lastError,
        lastCompletedCycleId,
        lastCompletedAtMs,
    };
}

function normalizeScalpPipelineRuntimePromotionCompletion(
    value: unknown,
): ScalpPipelineRuntimePromotionCompletionSnapshot | null {
    const row = asRecord(value);
    const cycleId = normalizeOptionalText(row.cycleId, 120);
    const syncedAtMs = normalizeTsMs(row.syncedAtMs);
    const deploymentsConsidered = normalizeNonNegativeInt(row.deploymentsConsidered);
    const deploymentsMatched = normalizeNonNegativeInt(row.deploymentsMatched);
    const deploymentsUpdated = normalizeNonNegativeInt(row.deploymentsUpdated);
    if (
        cycleId === null ||
        syncedAtMs === null ||
        deploymentsConsidered === null ||
        deploymentsMatched === null ||
        deploymentsUpdated === null
    ) {
        return null;
    }
    return {
        cycleId,
        syncedAtMs,
        deploymentsConsidered,
        deploymentsMatched,
        deploymentsUpdated,
    };
}

export function normalizeScalpPipelineRuntimeSnapshot(
    value: unknown,
): ScalpPipelineRuntimeSnapshot | null {
    const row = asRecord(value);
    const orchestrator = normalizeScalpPipelineRuntimeOrchestrator(row.orchestrator);
    const promotionSyncRaw = normalizeScalpPipelineRuntimePromotionSync(row.promotionSync);
    const promotionCompletion = normalizeScalpPipelineRuntimePromotionCompletion(row.promotionSyncCompletion);
    const promotionSync: ScalpPipelineRuntimePromotionSyncSnapshot | null = promotionSyncRaw
        ? {
              ...promotionSyncRaw,
              totalDeployments:
                  promotionSyncRaw.totalDeployments ?? promotionCompletion?.deploymentsConsidered ?? null,
              processedDeployments:
                  promotionSyncRaw.processedDeployments ?? promotionCompletion?.deploymentsConsidered ?? null,
              matchedDeployments:
                  promotionSyncRaw.matchedDeployments ?? promotionCompletion?.deploymentsMatched ?? null,
              updatedDeployments:
                  promotionSyncRaw.updatedDeployments ?? promotionCompletion?.deploymentsUpdated ?? null,
              lastCompletedCycleId:
                  promotionSyncRaw.lastCompletedCycleId ?? promotionCompletion?.cycleId ?? null,
              lastCompletedAtMs:
                  promotionSyncRaw.lastCompletedAtMs ?? promotionCompletion?.syncedAtMs ?? null,
          }
        : promotionCompletion
          ? {
                status: 'succeeded',
                cycleId: promotionCompletion.cycleId,
                phase: null,
                startedAtMs: null,
                updatedAtMs: promotionCompletion.syncedAtMs,
                finishedAtMs: promotionCompletion.syncedAtMs,
                totalDeployments: promotionCompletion.deploymentsConsidered,
                processedDeployments: promotionCompletion.deploymentsConsidered,
                matchedDeployments: promotionCompletion.deploymentsMatched,
                updatedDeployments: promotionCompletion.deploymentsUpdated,
                currentSymbol: null,
                currentStrategyId: null,
                currentTuneId: null,
                reason: null,
                lastError: null,
                lastCompletedCycleId: promotionCompletion.cycleId,
                lastCompletedAtMs: promotionCompletion.syncedAtMs,
            }
          : null;
    if (!orchestrator && !promotionSync) return null;
    const derivedUpdatedAtMs = Math.max(
        0,
        orchestrator?.updatedAtMs ?? 0,
        promotionSync?.updatedAtMs ?? 0,
        promotionSync?.lastCompletedAtMs ?? 0,
    );
    const updatedAtMs =
        normalizeTsMs(row.updatedAtMs) ??
        (derivedUpdatedAtMs > 0 ? derivedUpdatedAtMs : Date.now());
    return {
        version: 1,
        updatedAtMs,
        orchestrator,
        promotionSync,
    };
}

export function mergeScalpPipelineRuntimeSnapshot(
    current: ScalpPipelineRuntimeSnapshot | null,
    patch: ScalpPipelineRuntimePatch,
): ScalpPipelineRuntimeSnapshot | null {
    const hasOrchestratorPatch = Object.prototype.hasOwnProperty.call(patch, 'orchestrator');
    const hasPromotionPatch = Object.prototype.hasOwnProperty.call(patch, 'promotionSync');
    const nextOrchestrator = hasOrchestratorPatch
        ? patch.orchestrator === null
            ? null
            : normalizeScalpPipelineRuntimeOrchestrator({
                  ...(current?.orchestrator || {}),
                  ...(patch.orchestrator || {}),
              })
        : current?.orchestrator ?? null;
    const nextPromotionSync = hasPromotionPatch
        ? patch.promotionSync === null
            ? null
            : normalizeScalpPipelineRuntimePromotionSync({
                  ...(current?.promotionSync || {}),
                  ...(patch.promotionSync || {}),
              })
        : current?.promotionSync ?? null;
    return normalizeScalpPipelineRuntimeSnapshot({
        version: 1,
        updatedAtMs: normalizeTsMs(patch.updatedAtMs) ?? Date.now(),
        orchestrator: nextOrchestrator,
        promotionSync: nextPromotionSync,
    });
}

export async function loadScalpPipelineRuntimeSnapshot(): Promise<ScalpPipelineRuntimeSnapshot | null> {
    if (!isScalpPgConfigured()) return null;
    try {
        const db = scalpPrisma();
        const rows = await db.$queryRaw<Array<{ payload: unknown }>>(Prisma.sql`
            SELECT payload
            FROM scalp_jobs
            WHERE kind = ${PIPELINE_RUNTIME_KIND}::scalp_job_kind
              AND dedupe_key = ${PIPELINE_RUNTIME_DEDUPE_KEY}
            LIMIT 1;
        `);
        return normalizeScalpPipelineRuntimeSnapshot(rows[0]?.payload);
    } catch {
        return null;
    }
}

export async function patchScalpPipelineRuntimeSnapshot(patch: ScalpPipelineRuntimePatch): Promise<void> {
    if (!isScalpPgConfigured()) return;
    try {
        const payload = {
            version: 1,
            updatedAtMs: normalizeTsMs(patch.updatedAtMs) ?? Date.now(),
            ...(Object.prototype.hasOwnProperty.call(patch, 'orchestrator')
                ? {
                      orchestrator:
                          patch.orchestrator === null
                              ? null
                              : normalizeScalpPipelineRuntimeOrchestrator(patch.orchestrator),
                  }
                : {}),
            ...(Object.prototype.hasOwnProperty.call(patch, 'promotionSync')
                ? {
                      promotionSync:
                          patch.promotionSync === null
                              ? null
                              : normalizeScalpPipelineRuntimePromotionSync(patch.promotionSync),
                  }
                : {}),
            ...(Object.prototype.hasOwnProperty.call(patch, 'promotionSyncCompletion')
                ? {
                      promotionSyncCompletion:
                          patch.promotionSyncCompletion === null
                              ? null
                              : normalizeScalpPipelineRuntimePromotionCompletion(patch.promotionSyncCompletion),
                  }
                : {}),
        };
        if (Object.keys(payload).length <= 2) return;
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
                    ${PIPELINE_RUNTIME_KIND}::scalp_job_kind,
                    ${PIPELINE_RUNTIME_DEDUPE_KEY},
                    ${JSON.stringify(payload)}::jsonb,
                    'succeeded'::scalp_job_status,
                    1,
                    1,
                    NOW(),
                    NOW(),
                    NULL
                )
                ON CONFLICT(kind, dedupe_key)
                DO UPDATE SET
                    payload = COALESCE(scalp_jobs.payload, '{}'::jsonb) || EXCLUDED.payload,
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
    } catch (err) {
        console.warn('Failed to patch scalp pipeline runtime snapshot:', err);
    }
}
