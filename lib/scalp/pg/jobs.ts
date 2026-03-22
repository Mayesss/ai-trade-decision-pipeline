import { empty, join, raw, sql } from './sql';

import { scalpPrisma } from './client';

export type ScalpPgJobKind =
    | 'execute_cycle'
    | 'research_task'
    | 'research_aggregate'
    | 'promotion_sync'
    | 'guardrail_check'
    | 'housekeeping';

export interface EnqueueScalpJobInput {
    kind: ScalpPgJobKind;
    dedupeKey: string;
    payload?: Record<string, unknown>;
    scheduledForMs?: number;
    nextRunAtMs?: number;
    maxAttempts?: number;
}

export interface ClaimedScalpJob {
    id: bigint;
    kind: ScalpPgJobKind;
    dedupeKey: string;
    payload: Record<string, unknown>;
    attempts: number;
    maxAttempts: number;
    scheduledForMs: number;
    nextRunAtMs: number;
}

function normalizeTsMs(value: unknown, fallbackMs: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return fallbackMs;
    return Math.floor(n);
}

function asJsonObject(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

export async function enqueueScalpJobsBulk(rows: EnqueueScalpJobInput[]): Promise<number> {
    const nowMs = Date.now();
    const payload = rows
        .map((row) => ({
            kind: row.kind,
            dedupe_key: String(row.dedupeKey || '').trim(),
            payload: asJsonObject(row.payload),
            max_attempts: Math.max(1, Math.floor(Number(row.maxAttempts) || 5)),
            scheduled_for: new Date(normalizeTsMs(row.scheduledForMs, nowMs)).toISOString(),
            next_run_at: new Date(normalizeTsMs(row.nextRunAtMs, nowMs)).toISOString(),
        }))
        .filter((row) => row.dedupe_key.length > 0);

    if (!payload.length) return 0;

    const db = scalpPrisma();
    const payloadJson = JSON.stringify(payload);
    const updated = await db.$executeRaw(
        sql`
        WITH input AS (
            SELECT *
            FROM jsonb_to_recordset(${payloadJson}::jsonb) AS x(
                kind text,
                dedupe_key text,
                payload jsonb,
                max_attempts int,
                scheduled_for timestamptz,
                next_run_at timestamptz
            )
        )
        INSERT INTO scalp_jobs(kind, dedupe_key, payload, status, attempts, max_attempts, scheduled_for, next_run_at)
        SELECT
            x.kind::scalp_job_kind,
            x.dedupe_key,
            COALESCE(x.payload, '{}'::jsonb),
            'pending'::scalp_job_status,
            0,
            GREATEST(1, x.max_attempts),
            x.scheduled_for,
            x.next_run_at
        FROM input x
        ON CONFLICT(kind, dedupe_key)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            max_attempts = EXCLUDED.max_attempts,
            scheduled_for = LEAST(scalp_jobs.scheduled_for, EXCLUDED.scheduled_for),
            next_run_at = LEAST(scalp_jobs.next_run_at, EXCLUDED.next_run_at),
            status = CASE
                WHEN scalp_jobs.status IN ('succeeded', 'failed_permanent', 'cancelled') THEN scalp_jobs.status
                ELSE 'pending'::scalp_job_status
            END,
            locked_by = NULL,
            locked_at = NULL,
            last_error = NULL,
            updated_at = NOW();
        `,
    );
    return Number(updated || 0);
}

export async function claimScalpJobs(params: {
    workerId: string;
    limit: number;
    nowMs?: number;
}): Promise<ClaimedScalpJob[]> {
    const workerId = String(params.workerId || '').trim() || 'worker_unknown';
    const limit = Math.max(1, Math.min(500, Math.floor(Number(params.limit) || 1)));
    const now = new Date(normalizeTsMs(params.nowMs, Date.now()));

    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            id: bigint;
            kind: string;
            dedupeKey: string;
            payload: unknown;
            attempts: number;
            maxAttempts: number;
            scheduledFor: Date;
            nextRunAt: Date;
        }>
    >(sql`
        WITH candidate AS (
            SELECT j.id
            FROM scalp_jobs j
            WHERE j.status IN ('pending', 'retry_wait')
              AND j.next_run_at <= ${now}
            ORDER BY j.scheduled_for ASC, j.id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ${limit}
        )
        UPDATE scalp_jobs j
        SET
            status = 'running',
            attempts = j.attempts + 1,
            locked_by = ${workerId},
            locked_at = NOW(),
            updated_at = NOW()
        FROM candidate c
        WHERE j.id = c.id
        RETURNING
            j.id,
            j.kind::text AS "kind",
            j.dedupe_key AS "dedupeKey",
            j.payload,
            j.attempts,
            j.max_attempts AS "maxAttempts",
            j.scheduled_for AS "scheduledFor",
            j.next_run_at AS "nextRunAt";
    `);

    return rows.map((row) => ({
        id: row.id,
        kind: row.kind as ScalpPgJobKind,
        dedupeKey: row.dedupeKey,
        payload: asJsonObject(row.payload),
        attempts: Number(row.attempts || 0),
        maxAttempts: Number(row.maxAttempts || 0),
        scheduledForMs: row.scheduledFor instanceof Date ? row.scheduledFor.getTime() : 0,
        nextRunAtMs: row.nextRunAt instanceof Date ? row.nextRunAt.getTime() : 0,
    }));
}

export async function completeScalpJob(params: {
    id: bigint;
    workerId: string;
    success: boolean;
    lastError?: string | null;
    nextRunAtMs?: number | null;
}): Promise<number> {
    const id = params.id;
    const workerId = String(params.workerId || '').trim() || 'worker_unknown';
    const nextRunAt =
        params.nextRunAtMs && Number.isFinite(params.nextRunAtMs) && Number(params.nextRunAtMs) > 0
            ? new Date(Number(params.nextRunAtMs))
            : null;

    const db = scalpPrisma();
    if (params.success) {
        const updated = await db.$executeRaw(
            sql`
            UPDATE scalp_jobs
            SET
                status = 'succeeded',
                locked_by = NULL,
                locked_at = NULL,
                last_error = NULL,
                updated_at = NOW()
            WHERE id = ${id}
              AND locked_by = ${workerId};
            `,
        );
        return Number(updated || 0);
    }

    const retryStatus = nextRunAt ? sql`'retry_wait'::scalp_job_status` : sql`'failed_permanent'::scalp_job_status`;
    const updated = await db.$executeRaw(
        sql`
        UPDATE scalp_jobs
        SET
            status = ${retryStatus},
            next_run_at = COALESCE(${nextRunAt}, next_run_at),
            locked_by = NULL,
            locked_at = NULL,
            last_error = ${String(params.lastError || '').slice(0, 600) || null},
            updated_at = NOW()
        WHERE id = ${id}
          AND locked_by = ${workerId};
        `,
    );
    return Number(updated || 0);
}
