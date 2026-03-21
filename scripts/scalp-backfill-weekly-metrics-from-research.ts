import { Prisma } from '@prisma/client';

import { isScalpPgConfigured, scalpPrisma } from '../lib/scalp/pg/client';

type CliOptions = {
    dryRun: boolean;
    verbose: boolean;
};

type SourceCounts = {
    totalTasks: number;
    completedTasks: number;
    completedWithResult: number;
    completedWithResultAndDeployment: number;
    completedWithResultMissingDeployment: number;
    uniqueDeploymentWeeks: number;
};

type BackfillResult = {
    affectedRows: number;
    insertedRows: number;
    updatedRows: number;
};

function parseCliOptions(argv: string[]): CliOptions {
    let dryRun = true;
    let verbose = false;

    for (const arg of argv) {
        const normalized = String(arg || '').trim().toLowerCase();
        if (!normalized) continue;
        if (normalized === '--apply') dryRun = false;
        if (normalized === '--dry-run') dryRun = true;
        if (normalized === '--verbose') verbose = true;
    }

    return { dryRun, verbose };
}

async function loadSourceCounts(): Promise<SourceCounts> {
    const db = scalpPrisma();

    const [rows] = await db.$queryRaw<Array<{
        totalTasks: bigint;
        completedTasks: bigint;
        completedWithResult: bigint;
        completedWithResultAndDeployment: bigint;
        completedWithResultMissingDeployment: bigint;
        uniqueDeploymentWeeks: bigint;
    }>>(Prisma.sql`
        WITH eligible AS (
            SELECT
                t.deployment_id,
                t.window_from
            FROM scalp_research_tasks t
            JOIN scalp_deployments d
              ON d.deployment_id = t.deployment_id
            WHERE t.status = 'completed'
              AND t.result_json IS NOT NULL
            GROUP BY t.deployment_id, t.window_from
        )
        SELECT
            COUNT(*)::bigint AS "totalTasks",
            COUNT(*) FILTER (WHERE t.status = 'completed')::bigint AS "completedTasks",
            COUNT(*) FILTER (
                WHERE t.status = 'completed' AND t.result_json IS NOT NULL
            )::bigint AS "completedWithResult",
            COUNT(*) FILTER (
                WHERE t.status = 'completed'
                  AND t.result_json IS NOT NULL
                  AND EXISTS (
                    SELECT 1
                    FROM scalp_deployments d
                    WHERE d.deployment_id = t.deployment_id
                  )
            )::bigint AS "completedWithResultAndDeployment",
            COUNT(*) FILTER (
                WHERE t.status = 'completed'
                  AND t.result_json IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1
                    FROM scalp_deployments d
                    WHERE d.deployment_id = t.deployment_id
                  )
            )::bigint AS "completedWithResultMissingDeployment",
            COALESCE((SELECT COUNT(*)::bigint FROM eligible), 0::bigint) AS "uniqueDeploymentWeeks"
        FROM scalp_research_tasks t;
    `);

    return {
        totalTasks: Number(rows?.totalTasks || 0),
        completedTasks: Number(rows?.completedTasks || 0),
        completedWithResult: Number(rows?.completedWithResult || 0),
        completedWithResultAndDeployment: Number(
            rows?.completedWithResultAndDeployment || 0,
        ),
        completedWithResultMissingDeployment: Number(
            rows?.completedWithResultMissingDeployment || 0,
        ),
        uniqueDeploymentWeeks: Number(rows?.uniqueDeploymentWeeks || 0),
    };
}

async function tableExists(tableName: string): Promise<boolean> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{ exists: boolean }>>(Prisma.sql`
        SELECT to_regclass(${`public.${tableName}`}) IS NOT NULL AS exists;
    `);
    return Boolean(rows[0]?.exists);
}

async function ensureWeeklyMetricsTable(): Promise<void> {
    const db = scalpPrisma();

    await db.$executeRaw(Prisma.sql`
        CREATE TABLE IF NOT EXISTS scalp_deployment_weekly_metrics (
            id bigserial PRIMARY KEY,
            deployment_id text NOT NULL REFERENCES scalp_deployments(deployment_id) ON DELETE CASCADE,
            entry_session_profile text NOT NULL DEFAULT 'berlin',
            symbol text NOT NULL,
            strategy_id text NOT NULL,
            tune_id text NOT NULL,
            week_start timestamptz NOT NULL,
            week_end timestamptz NOT NULL,
            status text NOT NULL DEFAULT 'pending',
            attempts integer NOT NULL DEFAULT 0,
            next_run_at timestamptz NOT NULL DEFAULT NOW(),
            worker_id text,
            started_at timestamptz,
            finished_at timestamptz,
            error_code text,
            error_message text,
            trades integer,
            win_rate_pct numeric(10, 4),
            net_r numeric(20, 8),
            expectancy_r numeric(20, 8),
            profit_factor numeric(20, 8),
            max_drawdown_r numeric(20, 8),
            avg_hold_minutes numeric(20, 8),
            net_pnl_usd numeric(20, 8),
            gross_profit_r numeric(20, 8),
            gross_loss_r numeric(20, 8),
            metrics_json jsonb,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            CONSTRAINT scalp_deployment_weekly_metrics_unique_week
                UNIQUE (deployment_id, week_start)
        );
    `);

    await db.$executeRaw(Prisma.sql`
        CREATE INDEX IF NOT EXISTS scalp_deployment_weekly_metrics_claim_idx
            ON scalp_deployment_weekly_metrics(status, next_run_at, week_start);
    `);
    await db.$executeRaw(Prisma.sql`
        CREATE INDEX IF NOT EXISTS scalp_deployment_weekly_metrics_session_claim_idx
            ON scalp_deployment_weekly_metrics(entry_session_profile, status, next_run_at, week_start);
    `);

    await db.$executeRaw(Prisma.sql`
        CREATE INDEX IF NOT EXISTS scalp_deployment_weekly_metrics_deployment_week_idx
            ON scalp_deployment_weekly_metrics(deployment_id, week_start DESC);
    `);

    await db.$executeRaw(Prisma.sql`
        CREATE INDEX IF NOT EXISTS scalp_deployment_weekly_metrics_symbol_strategy_week_idx
            ON scalp_deployment_weekly_metrics(symbol, strategy_id, tune_id, week_start DESC);
    `);
}

async function previewBackfill(): Promise<BackfillResult> {
    const db = scalpPrisma();
    const hasWeeklyMetrics = await tableExists('scalp_deployment_weekly_metrics');

    const [sourceRows] = await db.$queryRaw<Array<{ candidateRows: bigint }>>(Prisma.sql`
        WITH src_base AS (
            SELECT
                t.deployment_id,
                d.entry_session_profile,
                t.symbol,
                t.strategy_id,
                t.tune_id,
                t.window_from AS week_start,
                t.window_to AS week_end,
                t.finished_at,
                t.updated_at
            FROM scalp_research_tasks t
            JOIN scalp_deployments d
              ON d.deployment_id = t.deployment_id
            WHERE t.status = 'completed'
              AND t.result_json IS NOT NULL
        ),
        src AS (
            SELECT DISTINCT ON (deployment_id, week_start)
                deployment_id,
                entry_session_profile,
                symbol,
                strategy_id,
                tune_id,
                week_start,
                week_end
            FROM src_base
            ORDER BY deployment_id, week_start, finished_at DESC NULLS LAST, updated_at DESC
        )
        SELECT COUNT(*)::bigint AS "candidateRows"
        FROM src;
    `);

    const candidateRows = Number(sourceRows?.candidateRows || 0);
    if (!hasWeeklyMetrics || candidateRows <= 0) {
        return {
            affectedRows: candidateRows,
            insertedRows: candidateRows,
            updatedRows: 0,
        };
    }

    const [rows] = await db.$queryRaw<Array<{
        candidateRows: bigint;
        wouldInsert: bigint;
        wouldUpdate: bigint;
    }>>(Prisma.sql`
        WITH src_base AS (
            SELECT
                t.deployment_id,
                t.window_from AS week_start,
                t.finished_at,
                t.updated_at
            FROM scalp_research_tasks t
            JOIN scalp_deployments d
              ON d.deployment_id = t.deployment_id
            WHERE t.status = 'completed'
              AND t.result_json IS NOT NULL
        ),
        src AS (
            SELECT DISTINCT ON (deployment_id, week_start)
                deployment_id,
                week_start
            FROM src_base
            ORDER BY deployment_id, week_start, finished_at DESC NULLS LAST, updated_at DESC
        )
        SELECT
            COUNT(*)::bigint AS "candidateRows",
            COUNT(*) FILTER (WHERE m.deployment_id IS NULL)::bigint AS "wouldInsert",
            COUNT(*) FILTER (WHERE m.deployment_id IS NOT NULL)::bigint AS "wouldUpdate"
        FROM src
        LEFT JOIN scalp_deployment_weekly_metrics m
          ON m.deployment_id = src.deployment_id
         AND m.week_start = src.week_start;
    `);

    return {
        affectedRows: Number(rows?.candidateRows || 0),
        insertedRows: Number(rows?.wouldInsert || 0),
        updatedRows: Number(rows?.wouldUpdate || 0),
    };
}

async function runBackfill(): Promise<BackfillResult> {
    const db = scalpPrisma();

    const [rows] = await db.$queryRaw<Array<{
        affectedRows: bigint;
        insertedRows: bigint;
        updatedRows: bigint;
    }>>(Prisma.sql`
        WITH src_base AS (
            SELECT
                t.deployment_id,
                t.symbol,
                t.strategy_id,
                t.tune_id,
                t.window_from AS week_start,
                t.window_to AS week_end,
                t.worker_id,
                t.started_at,
                t.finished_at,
                t.error_code,
                t.error_message,
                t.attempts,
                t.created_at,
                t.updated_at,
                COALESCE(t.result_json -> 'metrics', '{}'::jsonb) AS metrics_json
            FROM scalp_research_tasks t
            JOIN scalp_deployments d
              ON d.deployment_id = t.deployment_id
            WHERE t.status = 'completed'
              AND t.result_json IS NOT NULL
        ),
        src AS (
            SELECT DISTINCT ON (deployment_id, week_start)
                deployment_id,
                entry_session_profile,
                symbol,
                strategy_id,
                tune_id,
                week_start,
                week_end,
                worker_id,
                started_at,
                finished_at,
                error_code,
                error_message,
                attempts,
                created_at,
                updated_at,
                metrics_json,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'trades') = 'number'
                        THEN FLOOR((metrics_json ->> 'trades')::numeric)::int
                    WHEN jsonb_typeof(metrics_json -> 'trades') = 'string'
                        AND (metrics_json ->> 'trades') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN FLOOR((metrics_json ->> 'trades')::numeric)::int
                    ELSE NULL
                END AS trades,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'winRatePct') = 'number'
                        THEN (metrics_json ->> 'winRatePct')::numeric
                    WHEN jsonb_typeof(metrics_json -> 'winRatePct') = 'string'
                        AND (metrics_json ->> 'winRatePct') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN (metrics_json ->> 'winRatePct')::numeric
                    ELSE NULL
                END AS win_rate_pct,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'netR') = 'number'
                        THEN (metrics_json ->> 'netR')::numeric
                    WHEN jsonb_typeof(metrics_json -> 'netR') = 'string'
                        AND (metrics_json ->> 'netR') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN (metrics_json ->> 'netR')::numeric
                    ELSE NULL
                END AS net_r,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'expectancyR') = 'number'
                        THEN (metrics_json ->> 'expectancyR')::numeric
                    WHEN jsonb_typeof(metrics_json -> 'expectancyR') = 'string'
                        AND (metrics_json ->> 'expectancyR') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN (metrics_json ->> 'expectancyR')::numeric
                    ELSE NULL
                END AS expectancy_r,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'profitFactor') = 'number'
                        THEN (metrics_json ->> 'profitFactor')::numeric
                    WHEN jsonb_typeof(metrics_json -> 'profitFactor') = 'string'
                        AND (metrics_json ->> 'profitFactor') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN (metrics_json ->> 'profitFactor')::numeric
                    ELSE NULL
                END AS profit_factor,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'maxDrawdownR') = 'number'
                        THEN (metrics_json ->> 'maxDrawdownR')::numeric
                    WHEN jsonb_typeof(metrics_json -> 'maxDrawdownR') = 'string'
                        AND (metrics_json ->> 'maxDrawdownR') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN (metrics_json ->> 'maxDrawdownR')::numeric
                    ELSE NULL
                END AS max_drawdown_r,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'avgHoldMinutes') = 'number'
                        THEN (metrics_json ->> 'avgHoldMinutes')::numeric
                    WHEN jsonb_typeof(metrics_json -> 'avgHoldMinutes') = 'string'
                        AND (metrics_json ->> 'avgHoldMinutes') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN (metrics_json ->> 'avgHoldMinutes')::numeric
                    ELSE NULL
                END AS avg_hold_minutes,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'netPnlUsd') = 'number'
                        THEN (metrics_json ->> 'netPnlUsd')::numeric
                    WHEN jsonb_typeof(metrics_json -> 'netPnlUsd') = 'string'
                        AND (metrics_json ->> 'netPnlUsd') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN (metrics_json ->> 'netPnlUsd')::numeric
                    ELSE NULL
                END AS net_pnl_usd,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'grossProfitR') = 'number'
                        THEN (metrics_json ->> 'grossProfitR')::numeric
                    WHEN jsonb_typeof(metrics_json -> 'grossProfitR') = 'string'
                        AND (metrics_json ->> 'grossProfitR') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN (metrics_json ->> 'grossProfitR')::numeric
                    ELSE NULL
                END AS gross_profit_r,
                CASE
                    WHEN jsonb_typeof(metrics_json -> 'grossLossR') = 'number'
                        THEN (metrics_json ->> 'grossLossR')::numeric
                    WHEN jsonb_typeof(metrics_json -> 'grossLossR') = 'string'
                        AND (metrics_json ->> 'grossLossR') ~ '^[+-]?[0-9]+(\\.[0-9]+)?$'
                        THEN (metrics_json ->> 'grossLossR')::numeric
                    ELSE NULL
                END AS gross_loss_r
            FROM src_base
            ORDER BY deployment_id, week_start, finished_at DESC NULLS LAST, updated_at DESC
        ),
        upserted AS (
            INSERT INTO scalp_deployment_weekly_metrics(
                deployment_id,
                entry_session_profile,
                symbol,
                strategy_id,
                tune_id,
                week_start,
                week_end,
                status,
                attempts,
                next_run_at,
                worker_id,
                started_at,
                finished_at,
                error_code,
                error_message,
                trades,
                win_rate_pct,
                net_r,
                expectancy_r,
                profit_factor,
                max_drawdown_r,
                avg_hold_minutes,
                net_pnl_usd,
                gross_profit_r,
                gross_loss_r,
                metrics_json,
                created_at,
                updated_at
            )
            SELECT
                src.deployment_id,
                src.entry_session_profile,
                src.symbol,
                src.strategy_id,
                src.tune_id,
                src.week_start,
                src.week_end,
                'succeeded' AS status,
                GREATEST(1, COALESCE(src.attempts, 1)) AS attempts,
                COALESCE(src.finished_at, src.updated_at, NOW()) AS next_run_at,
                src.worker_id,
                src.started_at,
                src.finished_at,
                src.error_code,
                src.error_message,
                src.trades,
                src.win_rate_pct,
                src.net_r,
                src.expectancy_r,
                src.profit_factor,
                src.max_drawdown_r,
                src.avg_hold_minutes,
                src.net_pnl_usd,
                src.gross_profit_r,
                src.gross_loss_r,
                src.metrics_json,
                COALESCE(src.created_at, NOW()) AS created_at,
                NOW() AS updated_at
            FROM src
            ON CONFLICT(deployment_id, week_start)
            DO UPDATE SET
                symbol = EXCLUDED.symbol,
                entry_session_profile = EXCLUDED.entry_session_profile,
                strategy_id = EXCLUDED.strategy_id,
                tune_id = EXCLUDED.tune_id,
                week_end = EXCLUDED.week_end,
                status = 'succeeded',
                attempts = GREATEST(scalp_deployment_weekly_metrics.attempts, EXCLUDED.attempts),
                next_run_at = EXCLUDED.next_run_at,
                worker_id = EXCLUDED.worker_id,
                started_at = EXCLUDED.started_at,
                finished_at = EXCLUDED.finished_at,
                error_code = EXCLUDED.error_code,
                error_message = EXCLUDED.error_message,
                trades = EXCLUDED.trades,
                win_rate_pct = EXCLUDED.win_rate_pct,
                net_r = EXCLUDED.net_r,
                expectancy_r = EXCLUDED.expectancy_r,
                profit_factor = EXCLUDED.profit_factor,
                max_drawdown_r = EXCLUDED.max_drawdown_r,
                avg_hold_minutes = EXCLUDED.avg_hold_minutes,
                net_pnl_usd = EXCLUDED.net_pnl_usd,
                gross_profit_r = EXCLUDED.gross_profit_r,
                gross_loss_r = EXCLUDED.gross_loss_r,
                metrics_json = EXCLUDED.metrics_json,
                updated_at = NOW()
            RETURNING (xmax = 0) AS inserted
        )
        SELECT
            COUNT(*)::bigint AS "affectedRows",
            COUNT(*) FILTER (WHERE inserted)::bigint AS "insertedRows",
            COUNT(*) FILTER (WHERE NOT inserted)::bigint AS "updatedRows"
        FROM upserted;
    `);

    return {
        affectedRows: Number(rows?.affectedRows || 0),
        insertedRows: Number(rows?.insertedRows || 0),
        updatedRows: Number(rows?.updatedRows || 0),
    };
}

async function countWeeklyMetricsRows(): Promise<number> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_deployment_weekly_metrics;
    `);
    return Number(rows[0]?.count || 0);
}

async function main() {
    const opts = parseCliOptions(process.argv.slice(2));
    const startedAt = Date.now();

    if (!isScalpPgConfigured()) {
        console.error(
            JSON.stringify(
                {
                    ok: false,
                    error: 'pg_not_configured',
                    message: 'SCALP_PG_CONNECTION_STRING (or DATABASE_URL) is required.',
                },
                null,
                2,
            ),
        );
        process.exit(1);
    }

    const beforeCounts = await loadSourceCounts();
    const existedBefore = await tableExists('scalp_deployment_weekly_metrics');

    let createdTable = false;
    let backfill: BackfillResult;

    if (opts.dryRun) {
        backfill = await previewBackfill();
    } else {
        if (!existedBefore) {
            await ensureWeeklyMetricsTable();
            createdTable = true;
        }
        backfill = await runBackfill();
    }

    const existsAfter = opts.dryRun ? existedBefore : await tableExists('scalp_deployment_weekly_metrics');
    const weeklyMetricsRows =
        opts.dryRun || !existsAfter ? null : await countWeeklyMetricsRows();

    const output = {
        ok: true,
        dryRun: opts.dryRun,
        table: {
            existedBefore,
            createdTable,
            existsAfter,
        },
        source: beforeCounts,
        backfill,
        weeklyMetricsRows,
        elapsedMs: Date.now() - startedAt,
    };

    if (opts.verbose || true) {
        console.log(JSON.stringify(output, null, 2));
    }

    await scalpPrisma().$disconnect();
}

main().catch(async (err) => {
    const message = String(err?.message || err || 'backfill_failed');
    console.error(JSON.stringify({ ok: false, error: message }, null, 2));
    try {
        await scalpPrisma().$disconnect();
    } catch {
        // ignore
    }
    process.exit(1);
});
