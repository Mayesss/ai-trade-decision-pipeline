import { scalpPrisma } from '../lib/scalp/pg/client';

function asNumber(v: unknown): number | null {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

async function main() {
  const db = scalpPrisma();
  const rows = await db.$queryRawUnsafe<Array<{
    taskId: string;
    cycleId: string;
    cycleStatus: string;
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    status: string;
    windowFrom: Date;
    windowTo: Date;
    attempts: number;
    maxAttempts: number;
    errorCode: string | null;
    resultJson: any;
    updatedAt: Date;
  }>>(`
    SELECT
      t.task_id AS "taskId",
      t.cycle_id AS "cycleId",
      c.status::text AS "cycleStatus",
      t.deployment_id AS "deploymentId",
      t.symbol,
      t.strategy_id AS "strategyId",
      t.tune_id AS "tuneId",
      t.status::text AS "status",
      t.window_from AS "windowFrom",
      t.window_to AS "windowTo",
      t.attempts,
      t.max_attempts AS "maxAttempts",
      NULLIF(t.error_code, '') AS "errorCode",
      t.result_json AS "resultJson",
      t.updated_at AS "updatedAt"
    FROM scalp_research_tasks t
    LEFT JOIN scalp_research_cycles c
      ON c.cycle_id = t.cycle_id
    WHERE t.symbol = 'XANUSDT'
      AND t.strategy_id = 'trend_day_reacceleration_m15_m3'
    ORDER BY t.window_from ASC, t.tune_id ASC, t.updated_at DESC;
  `);

  const normalized = rows.map((r) => {
    const payload = (r.resultJson && typeof r.resultJson === 'object' && !Array.isArray(r.resultJson) && r.resultJson.metrics && typeof r.resultJson.metrics === 'object')
      ? r.resultJson.metrics
      : r.resultJson;
    const trades = asNumber(payload?.trades);
    const netR = asNumber(payload?.netR);
    const expectancyR = asNumber(payload?.expectancyR);
    const profitFactor = asNumber(payload?.profitFactor);
    const maxDrawdownR = asNumber(payload?.maxDrawdownR);
    const avgHoldMinutes = asNumber(payload?.avgHoldMinutes);

    return {
      taskId: r.taskId,
      cycleId: r.cycleId,
      cycleStatus: r.cycleStatus,
      deploymentId: r.deploymentId,
      tuneId: r.tuneId,
      status: r.status,
      windowFromIso: r.windowFrom?.toISOString?.() || String(r.windowFrom),
      windowToIso: r.windowTo?.toISOString?.() || String(r.windowTo),
      attempts: r.attempts,
      maxAttempts: r.maxAttempts,
      errorCode: r.errorCode,
      trades,
      netR,
      expectancyR,
      profitFactor,
      maxDrawdownR,
      avgHoldMinutes,
      updatedAtIso: r.updatedAt?.toISOString?.() || String(r.updatedAt),
    };
  });

  const completed = normalized.filter((r) => r.status === 'completed' && r.netR !== null);
  const byTune = new Map<string, typeof completed>();
  for (const row of completed) {
    const bucket = byTune.get(row.tuneId) || [];
    bucket.push(row);
    byTune.set(row.tuneId, bucket);
  }

  const summary = Array.from(byTune.entries()).map(([tuneId, rows]) => {
    const netRows = rows.map((r) => Number(r.netR || 0));
    const maxNetR = netRows.length ? Math.max(...netRows) : null;
    const minNetR = netRows.length ? Math.min(...netRows) : null;
    const sumNetR = netRows.reduce((a, b) => a + b, 0);
    const gt20 = rows.filter((r) => (r.netR || 0) >= 20).length;
    const gt50 = rows.filter((r) => (r.netR || 0) >= 50).length;
    return {
      tuneId,
      completedWindows: rows.length,
      sumNetR,
      maxNetR,
      minNetR,
      windowsGe20R: gt20,
      windowsGe50R: gt50,
    };
  });

  const suspicious = completed
    .filter((r) => (r.netR || 0) >= 20 || (r.expectancyR || 0) >= 2)
    .map((r) => ({
      tuneId: r.tuneId,
      windowFromIso: r.windowFromIso,
      windowToIso: r.windowToIso,
      netR: r.netR,
      trades: r.trades,
      expectancyR: r.expectancyR,
      profitFactor: r.profitFactor,
      maxDrawdownR: r.maxDrawdownR,
      avgHoldMinutes: r.avgHoldMinutes,
      cycleId: r.cycleId,
      cycleStatus: r.cycleStatus,
      taskId: r.taskId,
    }));

  console.log(JSON.stringify({
    totalRows: normalized.length,
    completedRows: completed.length,
    summary,
    suspicious,
    completed,
  }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
