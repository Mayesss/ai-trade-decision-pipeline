import { scalpPrisma } from '../lib/scalp/pg/client';

type CountRow = { count: bigint | number | string };

function toNumber(value: unknown): number {
    if (typeof value === 'bigint') return Number(value);
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
}

async function countTable(table: string): Promise<number> {
    const db = scalpPrisma();
    const rows = await db.$queryRawUnsafe<CountRow[]>(`SELECT COUNT(*)::bigint AS count FROM ${table}`);
    return toNumber(rows?.[0]?.count);
}

async function main() {
    const db = scalpPrisma();
    const ping = await db.$queryRaw<Array<{ ok: number }>>`SELECT 1::int AS ok`;

    const [
        deployments,
        sessions,
        executionRuns,
        journal,
        tradeLedger,
        pipelineJobs,
        pipelineSymbols,
        weeklyMetrics,
        cooldowns,
        jobs,
        shadowJobRuns,
    ] =
        await Promise.all([
            countTable('scalp_deployments'),
            countTable('scalp_sessions'),
            countTable('scalp_execution_runs'),
            countTable('scalp_journal'),
            countTable('scalp_trade_ledger'),
            countTable('scalp_pipeline_jobs'),
            countTable('scalp_pipeline_symbols'),
            countTable('scalp_deployment_weekly_metrics'),
            countTable('scalp_symbol_cooldowns'),
            countTable('scalp_jobs'),
            countTable('scalp_shadow_job_runs'),
        ]);

    console.log(
        JSON.stringify(
            {
                ok: Array.isArray(ping) && ping.length > 0,
                generatedAtIso: new Date().toISOString(),
                counts: {
                    deployments,
                    sessions,
                    executionRuns,
                    journal,
                    tradeLedger,
                    pipelineJobs,
                    pipelineSymbols,
                    deploymentWeeklyMetrics: weeklyMetrics,
                    symbolCooldowns: cooldowns,
                    jobs,
                    shadowJobRuns,
                },
            },
            null,
            2,
        ),
    );
}

main()
    .catch((err) => {
        console.error('scalp-pg-healthcheck failed:', err?.message || String(err));
        process.exitCode = 1;
    })
    .finally(async () => {
        try {
            await scalpPrisma().$disconnect();
        } catch {
            // best effort
        }
    });
