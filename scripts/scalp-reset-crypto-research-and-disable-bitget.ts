import { Prisma } from '@prisma/client';

import { scalpPrisma } from '../lib/scalp/pg/client';

async function main(): Promise<void> {
    const db = scalpPrisma();

    const staleCapitalCryptoRows = await db.$queryRaw<
        Array<{
            deploymentId: string;
            symbol: string;
            enabled: boolean;
            pending: number | bigint;
            running: number | bigint;
            completed: number | bigint;
            withResult: number | bigint;
        }>
    >(Prisma.sql`
        SELECT
            d.deployment_id AS "deploymentId",
            d.symbol,
            d.enabled,
            SUM(CASE WHEN t.status = 'pending' THEN 1 ELSE 0 END)::bigint AS pending,
            SUM(CASE WHEN t.status = 'running' THEN 1 ELSE 0 END)::bigint AS running,
            SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END)::bigint AS completed,
            SUM(CASE WHEN t.result_json IS NOT NULL THEN 1 ELSE 0 END)::bigint AS "withResult"
        FROM scalp_deployments d
        LEFT JOIN scalp_symbol_market_metadata m
          ON m.symbol = d.symbol
        LEFT JOIN scalp_research_tasks t
          ON t.deployment_id = d.deployment_id
        WHERE d.enabled = FALSE
          AND (d.deployment_id NOT ILIKE 'bitget:%')
          AND (
              LOWER(TRIM(COALESCE(m.asset_category, ''))) = 'crypto'
              OR UPPER(TRIM(COALESCE(m.instrument_type, ''))) = 'CRYPTOCURRENCIES'
          )
        GROUP BY d.deployment_id, d.symbol, d.enabled
        ORDER BY d.deployment_id ASC;
    `);

    const staleCapitalCryptoDeploymentIds = staleCapitalCryptoRows
        .map((row) => String(row.deploymentId || '').trim())
        .filter((row) => Boolean(row));

    const resetStaleTasks = staleCapitalCryptoDeploymentIds.length
        ? Number(
              await db.$executeRaw(
                  Prisma.sql`
                      UPDATE scalp_research_tasks t
                      SET
                          status = 'pending',
                          attempts = 0,
                          next_eligible_at = NOW(),
                          worker_id = NULL,
                          started_at = NULL,
                          finished_at = NULL,
                          result_json = NULL,
                          error_code = NULL,
                          error_message = NULL,
                          updated_at = NOW()
                      WHERE t.deployment_id IN (${Prisma.join(staleCapitalCryptoDeploymentIds)});
                  `,
              ),
          )
        : 0;

    const bitgetDeploymentsBefore = await db.$queryRaw<
        Array<{ deploymentId: string; symbol: string; enabled: boolean }>
    >(Prisma.sql`
        SELECT deployment_id AS "deploymentId", symbol, enabled
        FROM scalp_deployments
        WHERE deployment_id ILIKE 'bitget:%'
        ORDER BY deployment_id ASC;
    `);

    const disabledBitgetDeployments = Number(
        await db.$executeRaw(
            Prisma.sql`
                UPDATE scalp_deployments
                SET
                    enabled = FALSE,
                    updated_at = NOW(),
                    updated_by = 'manual:disable_bitget_until_recalc'
                WHERE deployment_id ILIKE 'bitget:%'
                  AND enabled = TRUE;
            `,
        ),
    );

    const bitgetDeploymentsAfter = await db.$queryRaw<
        Array<{ deploymentId: string; symbol: string; enabled: boolean }>
    >(Prisma.sql`
        SELECT deployment_id AS "deploymentId", symbol, enabled
        FROM scalp_deployments
        WHERE deployment_id ILIKE 'bitget:%'
        ORDER BY deployment_id ASC;
    `);

    const staleCapitalAfter = staleCapitalCryptoDeploymentIds.length
        ? await db.$queryRaw<
              Array<{
                  deploymentId: string;
                  pending: number | bigint;
                  running: number | bigint;
                  completed: number | bigint;
                  withResult: number | bigint;
              }>
          >(Prisma.sql`
              SELECT
                  t.deployment_id AS "deploymentId",
                  SUM(CASE WHEN t.status = 'pending' THEN 1 ELSE 0 END)::bigint AS pending,
                  SUM(CASE WHEN t.status = 'running' THEN 1 ELSE 0 END)::bigint AS running,
                  SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END)::bigint AS completed,
                  SUM(CASE WHEN t.result_json IS NOT NULL THEN 1 ELSE 0 END)::bigint AS "withResult"
              FROM scalp_research_tasks t
              WHERE t.deployment_id IN (${Prisma.join(staleCapitalCryptoDeploymentIds)})
              GROUP BY t.deployment_id
              ORDER BY t.deployment_id ASC;
          `)
        : [];

    console.log(
        JSON.stringify(
            {
                staleCapitalCrypto: {
                    deploymentCount: staleCapitalCryptoDeploymentIds.length,
                    deployments: staleCapitalCryptoRows.map((row) => ({
                        deploymentId: row.deploymentId,
                        symbol: row.symbol,
                        pending: Number(row.pending || 0),
                        running: Number(row.running || 0),
                        completed: Number(row.completed || 0),
                        withResult: Number(row.withResult || 0),
                    })),
                    tasksResetToPending: resetStaleTasks,
                    after: staleCapitalAfter.map((row) => ({
                        deploymentId: row.deploymentId,
                        pending: Number(row.pending || 0),
                        running: Number(row.running || 0),
                        completed: Number(row.completed || 0),
                        withResult: Number(row.withResult || 0),
                    })),
                },
                bitgetDeployments: {
                    before: bitgetDeploymentsBefore,
                    disabledNow: disabledBitgetDeployments,
                    after: bitgetDeploymentsAfter,
                },
            },
            null,
            2,
        ),
    );
}

main().catch((err) => {
    const message = String(err?.message || err || 'unknown_error');
    const stack = err instanceof Error ? err.stack : undefined;
    console.error(
        JSON.stringify(
            {
                ok: false,
                error: message,
                stack,
            },
            null,
            2,
        ),
    );
    process.exit(1);
});

