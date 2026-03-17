import { scalpPrisma } from '../lib/scalp/pg/client';

async function main() {
  const db = scalpPrisma();
  const rows = await db.$queryRawUnsafe<Array<{
    tuneId: string;
    windowFrom: Date;
    resultJson: any;
  }>>(`
    SELECT
      t.tune_id AS "tuneId",
      t.window_from AS "windowFrom",
      t.result_json AS "resultJson"
    FROM scalp_research_tasks t
    WHERE t.symbol = 'XANUSDT'
      AND t.strategy_id = 'trend_day_reacceleration_m15_m3'
      AND t.status = 'completed'
    ORDER BY t.window_from ASC, t.tune_id ASC;
  `);

  const out = rows.map((r) => ({
    tuneId: r.tuneId,
    windowFromIso: r.windowFrom?.toISOString?.() || String(r.windowFrom),
    configOverride: r.resultJson?.configOverride ?? null,
  }));

  const byTune = new Map<string, Set<string>>();
  for (const row of out) {
    const key = JSON.stringify(row.configOverride);
    const set = byTune.get(row.tuneId) || new Set<string>();
    set.add(key);
    byTune.set(row.tuneId, set);
  }

  console.log(JSON.stringify({
    uniqueOverridesByTune: Array.from(byTune.entries()).map(([tuneId, set]) => ({
      tuneId,
      count: set.size,
      values: Array.from(set).map((v) => JSON.parse(v)),
    })),
    sample: out.slice(0, 6),
  }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
