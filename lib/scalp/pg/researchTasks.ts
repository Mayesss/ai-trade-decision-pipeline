import { Prisma } from '@prisma/client';

import { scalpPrisma } from './client';

export interface ClaimedResearchTaskRow {
    taskId: string;
    cycleId: string;
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    attempts: number;
    maxAttempts: number;
    windowFromMs: number;
    windowToMs: number;
    priority: number;
}

function toDate(value: number | Date): Date {
    return value instanceof Date ? value : new Date(Math.floor(Number(value) || Date.now()));
}

export async function claimResearchTasksFromPg(params: {
    cycleId?: string;
    workerId: string;
    limit: number;
    nowMs?: number;
}): Promise<ClaimedResearchTaskRow[]> {
    const cycleId = String(params.cycleId || '').trim();
    const workerId = String(params.workerId || '').trim() || 'worker_unknown';
    const limit = Math.max(1, Math.min(500, Math.floor(Number(params.limit) || 1)));
    const now = toDate(Number(params.nowMs || Date.now()));
    const cycleFilterSql = cycleId ? Prisma.sql`AND t.cycle_id = ${cycleId}` : Prisma.empty;

    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            taskId: string;
            cycleId: string;
            deploymentId: string;
            symbol: string;
            strategyId: string;
            tuneId: string;
            attempts: number;
            maxAttempts: number;
            windowFrom: Date;
            windowTo: Date;
            priority: number;
        }>
    >(Prisma.sql`
        WITH candidate AS (
            SELECT t.task_id
            FROM scalp_research_tasks t
            LEFT JOIN scalp_symbol_cooldowns c
              ON c.symbol = t.symbol
            WHERE t.status = 'pending'
              ${cycleFilterSql}
              AND t.next_eligible_at <= ${now}
              AND t.attempts < t.max_attempts
              AND (c.blocked_until IS NULL OR c.blocked_until <= ${now})
            ORDER BY t.priority ASC, t.created_at ASC, t.task_id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ${limit}
        )
        UPDATE scalp_research_tasks t
        SET
            status = 'running',
            attempts = t.attempts + 1,
            worker_id = ${workerId},
            started_at = ${now},
            finished_at = NULL,
            error_code = NULL,
            error_message = NULL,
            updated_at = NOW()
        FROM candidate c
        WHERE t.task_id = c.task_id
        RETURNING
            t.task_id AS "taskId",
            t.cycle_id AS "cycleId",
            t.deployment_id AS "deploymentId",
            t.symbol,
            t.strategy_id AS "strategyId",
            t.tune_id AS "tuneId",
            t.attempts,
            t.max_attempts AS "maxAttempts",
            t.window_from AS "windowFrom",
            t.window_to AS "windowTo",
            t.priority;
    `);

    return rows.map((row) => ({
        taskId: row.taskId,
        cycleId: row.cycleId,
        deploymentId: row.deploymentId,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        attempts: Number(row.attempts || 0),
        maxAttempts: Number(row.maxAttempts || 0),
        windowFromMs: row.windowFrom instanceof Date ? row.windowFrom.getTime() : 0,
        windowToMs: row.windowTo instanceof Date ? row.windowTo.getTime() : 0,
        priority: Number(row.priority || 0),
    }));
}

export async function completeResearchTaskInPg(params: {
    taskId: string;
    result: Record<string, unknown>;
    nowMs?: number;
}): Promise<number> {
    const taskId = String(params.taskId || '').trim();
    if (!taskId) return 0;

    const now = toDate(Number(params.nowMs || Date.now()));
    const result = params.result && typeof params.result === 'object' && !Array.isArray(params.result) ? params.result : {};

    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        UPDATE scalp_research_tasks
        SET
            status = 'completed',
            result_json = ${JSON.stringify(result)}::jsonb,
            finished_at = ${now},
            error_code = NULL,
            error_message = NULL,
            updated_at = NOW()
        WHERE task_id = ${taskId}
          AND status = 'running';
        `,
    );

    return Number(updated || 0);
}

export async function deferResearchTaskInPg(params: {
    taskId: string;
    nextEligibleAtMs: number;
    errorCode: string;
    errorMessage?: string | null;
}): Promise<number> {
    const taskId = String(params.taskId || '').trim();
    if (!taskId) return 0;

    const nextEligibleAt = toDate(params.nextEligibleAtMs);
    const errorCode = String(params.errorCode || '').trim().slice(0, 80) || 'task_deferred';
    const errorMessage = String(params.errorMessage || '').trim().slice(0, 300) || null;

    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        UPDATE scalp_research_tasks
        SET
            status = 'pending',
            next_eligible_at = ${nextEligibleAt},
            finished_at = NOW(),
            error_code = ${errorCode},
            error_message = ${errorMessage},
            updated_at = NOW()
        WHERE task_id = ${taskId}
          AND status = 'running';
        `,
    );

    return Number(updated || 0);
}

export async function failResearchTaskPermanentInPg(params: {
    taskId: string;
    errorCode: string;
    errorMessage?: string | null;
}): Promise<number> {
    const taskId = String(params.taskId || '').trim();
    if (!taskId) return 0;

    const errorCode = String(params.errorCode || '').trim().slice(0, 80) || 'task_failed_permanent';
    const errorMessage = String(params.errorMessage || '').trim().slice(0, 300) || null;

    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        UPDATE scalp_research_tasks
        SET
            status = 'failed_permanent',
            finished_at = NOW(),
            error_code = ${errorCode},
            error_message = ${errorMessage},
            updated_at = NOW()
        WHERE task_id = ${taskId}
          AND status IN ('running', 'pending');
        `,
    );

    return Number(updated || 0);
}
