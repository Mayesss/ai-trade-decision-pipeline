import { Prisma } from '@prisma/client';

import { isScalpPgConfigured, scalpPrisma } from './pg/client';

const PANIC_STOP_KIND = 'execute_cycle';
const PANIC_STOP_DEDUPE_KEY = 'scalp_panic_stop_v1';
const ORCHESTRATOR_STATE_DEDUPE_KEY = 'scalp_pipeline_orchestrator_state_v1';

export interface ScalpPanicStopState {
    enabled: boolean;
    reason: string | null;
    updatedAtMs: number | null;
    updatedBy: string | null;
}

function asRecord(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

function normalizeReason(value: unknown): string | null {
    const text = String(value || '').trim();
    if (!text) return null;
    return text.slice(0, 240);
}

function normalizeUpdatedBy(value: unknown): string | null {
    const text = String(value || '').trim();
    if (!text) return null;
    return text.slice(0, 120);
}

function parseBool(value: unknown): boolean {
    if (typeof value === 'boolean') return value;
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    return ['1', 'true', 'yes', 'on'].includes(normalized);
}

export async function loadScalpPanicStopState(): Promise<ScalpPanicStopState> {
    if (!isScalpPgConfigured()) {
        return {
            enabled: false,
            reason: null,
            updatedAtMs: null,
            updatedBy: null,
        };
    }
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{ payload: unknown; updatedAtMs: bigint | number | null }>>(Prisma.sql`
        SELECT
            payload,
            (EXTRACT(EPOCH FROM updated_at) * 1000)::bigint AS "updatedAtMs"
        FROM scalp_jobs
        WHERE kind = ${PANIC_STOP_KIND}::scalp_job_kind
          AND dedupe_key = ${PANIC_STOP_DEDUPE_KEY}
        LIMIT 1;
    `);
    const row = rows[0];
    if (!row) {
        return {
            enabled: false,
            reason: null,
            updatedAtMs: null,
            updatedBy: null,
        };
    }
    const payload = asRecord(row.payload);
    const updatedAtMs = Number(row.updatedAtMs || 0);
    return {
        enabled: parseBool(payload.enabled),
        reason: normalizeReason(payload.reason),
        updatedAtMs: Number.isFinite(updatedAtMs) && updatedAtMs > 0 ? Math.floor(updatedAtMs) : null,
        updatedBy: normalizeUpdatedBy(payload.updatedBy),
    };
}

export async function setScalpPanicStopState(params: {
    enabled: boolean;
    reason?: string | null;
    updatedBy?: string | null;
}): Promise<ScalpPanicStopState> {
    if (!isScalpPgConfigured()) {
        throw new Error('scalp_pg_not_configured');
    }
    const nowMs = Date.now();
    const payload = {
        enabled: Boolean(params.enabled),
        reason: normalizeReason(params.reason),
        updatedAtMs: nowMs,
        updatedBy: normalizeUpdatedBy(params.updatedBy),
    };
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
                ${PANIC_STOP_KIND}::scalp_job_kind,
                ${PANIC_STOP_DEDUPE_KEY},
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
    if (payload.enabled) {
        const orchestratorRows = await db.$queryRaw<Array<{ payload: unknown }>>(Prisma.sql`
            SELECT payload
            FROM scalp_jobs
            WHERE kind = ${PANIC_STOP_KIND}::scalp_job_kind
              AND dedupe_key = ${ORCHESTRATOR_STATE_DEDUPE_KEY}
            LIMIT 1;
        `);
        const current = asRecord(orchestratorRows[0]?.payload);
        if (Object.keys(current).length > 0) {
            const nextPayload = {
                ...current,
                stage: 'done',
                completedAtMs: nowMs,
                updatedAtMs: nowMs,
                lockOwner: null,
                lockUntilMs: 0,
                lastError: 'panic_stop_enabled',
            };
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
                        ${PANIC_STOP_KIND}::scalp_job_kind,
                        ${ORCHESTRATOR_STATE_DEDUPE_KEY},
                        ${JSON.stringify(nextPayload)}::jsonb,
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
    }
    return {
        enabled: payload.enabled,
        reason: payload.reason,
        updatedAtMs: nowMs,
        updatedBy: payload.updatedBy,
    };
}
