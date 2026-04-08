import { scalpPrisma, sql } from "./lib/scalp-v2/pg";

async function main() {
  const db = scalpPrisma();

  await db.$executeRaw(sql`
    UPDATE scalp_v2_runtime_config
    SET config_json = jsonb_set(
      jsonb_set(
        config_json,
        '{budgets,maxCandidatesTotal}',
        '6000'::jsonb
      ),
      '{budgets,maxCandidatesPerSymbol}',
      '50'::jsonb
    ),
    updated_at = NOW()
    WHERE singleton = TRUE;
  `);

  const rc = await db.$queryRaw<any[]>(sql`
    SELECT config_json->'budgets' AS budgets
    FROM scalp_v2_runtime_config
    WHERE singleton = TRUE
    LIMIT 1;
  `);
  console.log("Updated DB budgets:", JSON.stringify(rc[0]?.budgets, null, 2));

  await db.$disconnect();
}
main().catch(e => { console.error("ERROR:", e.message); process.exit(1); });
