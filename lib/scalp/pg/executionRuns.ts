import { Prisma } from '@prisma/client';

import { scalpPrisma } from './client';

export type ScalpExecutionRunStatus = 'running' | 'succeeded' | 'failed' | 'skipped';

export interface ClaimScalpExecutionRunInput {
    deploymentId: string;
    scheduledMinuteMs: number;
    startedAtMs?: number;
}

export interface ClaimedScalpExecutionRunRow {
    deploymentId: string;
    scheduledMinuteMs: number;
}

export interface FinalizeScalpExecutionRunInput {
    deploymentId: string;
    scheduledMinuteMs: number;
    status: Exclude<ScalpExecutionRunStatus, 'running'>;
    reasonCodes?: string[];
    errorCode?: string | null;
    errorMessage?: string | null;
    finishedAtMs?: number;
}

function toDate(valueMs: unknown, fallbackMs = Date.now()): Date {
    const n = Number(valueMs);
    const ms = Number.isFinite(n) && n > 0 ? Math.floor(n) : fallbackMs;
    return new Date(ms);
}

function normalizeReasonCodes(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const out: string[] = [];
    const seen = new Set<string>();
    for (const raw of value) {
        const code = String(raw || '')
            .trim()
            .toUpperCase();
        if (!code || seen.has(code)) continue;
        seen.add(code);
        out.push(code.slice(0, 80));
        if (out.length >= 32) break;
    }
    return out;
}

function normalizeText(value: unknown, max: number): string | null {
    const text = String(value || '').trim();
    if (!text) return null;
    return text.slice(0, max);
}

function dedupeByKey<T>(rows: T[], keyFn: (row: T) => string): T[] {
    const out: T[] = [];
    const seen = new Set<string>();
    for (const row of rows) {
        const key = keyFn(row);
        if (!key || seen.has(key)) continue;
        seen.add(key);
        out.push(row);
    }
    return out;
}

export async function claimScalpExecutionRunSlotsBulk(
    rows: ClaimScalpExecutionRunInput[],
): Promise<ClaimedScalpExecutionRunRow[]> {
    const payload = dedupeByKey(
        rows
            .map((row) => ({
                deployment_id: String(row.deploymentId || '').trim(),
                scheduled_minute: toDate(row.scheduledMinuteMs),
                started_at: toDate(row.startedAtMs),
            }))
            .filter((row) => row.deployment_id.length > 0),
        (row) => `${row.deployment_id}::${row.scheduled_minute.toISOString()}`,
    );
    if (!payload.length) return [];

    const db = scalpPrisma();
    const payloadJson = JSON.stringify(payload);
    const inserted = await db.$queryRaw<Array<{ deploymentId: string; scheduledMinute: Date }>>(Prisma.sql`
        WITH input AS (
            SELECT *
            FROM jsonb_to_recordset(${payloadJson}::jsonb) AS x(
                deployment_id text,
                scheduled_minute timestamptz,
                started_at timestamptz
            )
        )
        INSERT INTO scalp_execution_runs(
            deployment_id,
            scheduled_minute,
            status,
            reason_codes,
            started_at
        )
        SELECT
            x.deployment_id,
            x.scheduled_minute,
            'running',
            '{}'::text[],
            COALESCE(x.started_at, NOW())
        FROM input x
        ON CONFLICT(deployment_id, scheduled_minute) DO NOTHING
        RETURNING
            deployment_id AS "deploymentId",
            scheduled_minute AS "scheduledMinute";
    `);

    return inserted.map((row) => ({
        deploymentId: row.deploymentId,
        scheduledMinuteMs: row.scheduledMinute instanceof Date ? row.scheduledMinute.getTime() : 0,
    }));
}

export async function finalizeScalpExecutionRunsBulk(rows: FinalizeScalpExecutionRunInput[]): Promise<number> {
    const payload = dedupeByKey(
        rows
            .map((row) => ({
                deployment_id: String(row.deploymentId || '').trim(),
                scheduled_minute: toDate(row.scheduledMinuteMs),
                status: row.status,
                reason_codes: normalizeReasonCodes(row.reasonCodes),
                error_code: normalizeText(row.errorCode, 80),
                error_message: normalizeText(row.errorMessage, 1000),
                finished_at: toDate(row.finishedAtMs),
            }))
            .filter((row) => row.deployment_id.length > 0),
        (row) => `${row.deployment_id}::${row.scheduled_minute.toISOString()}`,
    );
    if (!payload.length) return 0;

    const db = scalpPrisma();
    const payloadJson = JSON.stringify(payload);
    const updated = await db.$executeRaw(
        Prisma.sql`
        WITH input AS (
            SELECT *
            FROM jsonb_to_recordset(${payloadJson}::jsonb) AS x(
                deployment_id text,
                scheduled_minute timestamptz,
                status text,
                reason_codes text[],
                error_code text,
                error_message text,
                finished_at timestamptz
            )
        )
        UPDATE scalp_execution_runs r
        SET
            status = x.status,
            reason_codes = COALESCE(x.reason_codes, '{}'::text[]),
            error_code = NULLIF(x.error_code, ''),
            error_message = NULLIF(x.error_message, ''),
            finished_at = COALESCE(x.finished_at, NOW())
        FROM input x
        WHERE r.deployment_id = x.deployment_id
          AND r.scheduled_minute = x.scheduled_minute
          AND r.status = 'running';
        `,
    );

    return Number(updated || 0);
}
