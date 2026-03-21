import { Prisma } from '@prisma/client';
import { isScalpPgConfigured, scalpPrisma } from '../lib/scalp/pg/client';

async function main() {
  if (!isScalpPgConfigured()) {
    console.log(JSON.stringify({ ok: false, error: 'scalp_pg_not_configured' }, null, 2));
    return;
  }
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ entrySessionProfile: string; enabled: bigint | number; eligible: bigint | number; eligibleEnabled: bigint | number }>>(Prisma.sql`
    SELECT
      entry_session_profile AS "entrySessionProfile",
      COUNT(*) FILTER (WHERE enabled = TRUE)::bigint AS enabled,
      COUNT(*) FILTER (WHERE COALESCE((promotion_gate #>> '{eligible}')::boolean, FALSE) = TRUE)::bigint AS eligible,
      COUNT(*) FILTER (WHERE enabled = TRUE AND COALESCE((promotion_gate #>> '{eligible}')::boolean, FALSE) = TRUE)::bigint AS "eligibleEnabled"
    FROM scalp_deployments
    GROUP BY entry_session_profile
    ORDER BY entry_session_profile ASC;
  `);
  console.log(JSON.stringify({ ok: true, rows: rows.map((r) => ({ entrySessionProfile: r.entrySessionProfile, enabled: Number(r.enabled), eligible: Number(r.eligible), eligibleEnabled: Number(r.eligibleEnabled) })) }, null, 2));
  await db.$disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
