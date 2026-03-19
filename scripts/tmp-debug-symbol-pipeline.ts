import { Prisma } from '@prisma/client';
import { scalpPrisma } from '../lib/scalp/pg/client';

const symbol = (process.argv.find((a) => a.startsWith('--symbol='))?.split('=')[1] || 'BNBUSDT').toUpperCase();

async function main() {
  const db = scalpPrisma();

  const [pipelineRows, deployRows, weeklyRows, weeklyAgg, jobRows] = await Promise.all([
    db.$queryRaw<Array<any>>(Prisma.sql`
      SELECT
        symbol,
        active,
        discover_status,
        discover_attempts,
        discover_next_run_at,
        discover_error,
        load_status,
        load_attempts,
        load_next_run_at,
        load_error,
        weeks_covered,
        latest_week_start,
        prepare_status,
        prepare_attempts,
        prepare_next_run_at,
        prepare_error,
        prepared_deployments,
        last_discovered_at,
        last_loaded_at,
        last_prepared_at,
        updated_at
      FROM scalp_pipeline_symbols
      WHERE symbol = ${symbol}
      LIMIT 1;
    `),
    db.$queryRaw<Array<any>>(Prisma.sql`
      SELECT
        deployment_id,
        strategy_id,
        tune_id,
        in_universe,
        enabled,
        worker_dirty,
        promotion_dirty,
        retired_at,
        last_prepared_at,
        updated_at,
        promotion_gate
      FROM scalp_deployments
      WHERE symbol = ${symbol}
      ORDER BY strategy_id, tune_id;
    `),
    db.$queryRaw<Array<any>>(Prisma.sql`
      SELECT
        deployment_id,
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
        net_r
      FROM scalp_deployment_weekly_metrics
      WHERE symbol = ${symbol}
      ORDER BY week_start DESC, deployment_id
      LIMIT 80;
    `),
    db.$queryRaw<Array<any>>(Prisma.sql`
      SELECT
        COUNT(*)::bigint AS total,
        COUNT(*) FILTER (WHERE status='pending')::bigint AS pending,
        COUNT(*) FILTER (WHERE status='retry_wait')::bigint AS retry_wait,
        COUNT(*) FILTER (WHERE status='running')::bigint AS running,
        COUNT(*) FILTER (WHERE status='succeeded')::bigint AS succeeded,
        COUNT(*) FILTER (WHERE status='failed')::bigint AS failed
      FROM scalp_deployment_weekly_metrics
      WHERE symbol = ${symbol};
    `),
    db.$queryRaw<Array<any>>(Prisma.sql`
      SELECT
        job_kind,
        status,
        lock_token,
        lock_expires_at,
        last_run_at,
        last_success_at,
        last_error,
        progress_label,
        updated_at
      FROM scalp_pipeline_jobs
      ORDER BY job_kind;
    `),
  ]);

  console.log(JSON.stringify({
    symbol,
    pipeline: pipelineRows[0] || null,
    deploymentsCount: deployRows.length,
    deployments: deployRows,
    weeklyAgg: weeklyAgg[0] ? {
      total: Number(weeklyAgg[0].total || 0),
      pending: Number(weeklyAgg[0].pending || 0),
      retry_wait: Number(weeklyAgg[0].retry_wait || 0),
      running: Number(weeklyAgg[0].running || 0),
      succeeded: Number(weeklyAgg[0].succeeded || 0),
      failed: Number(weeklyAgg[0].failed || 0),
    } : null,
    weeklySample: weeklyRows,
    jobs: jobRows,
  }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
}).finally(async () => {
  await scalpPrisma().$disconnect();
});
