import { scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";
import { resolveBitgetBrokerCloseLedger } from "../lib/scalp/composer/bitgetCloseHistory";
import { snapshotScalpV2DailyMetrics } from "../lib/scalp/composer/db";
import { deriveCloseTypeFromReasonCodes } from "../lib/scalp/composer/logic";

const APPLY = process.argv.includes("--apply");

const BCH_DEPLOYMENT =
  "bitget:BCHUSDT~model_guided_composer_v2~mdl_basis_m5_m1_xts_80be903a4e__sp_berlin";

const REPAIRS = [
  {
    label: "bch-first",
    ledgerId: "4811b49f-1b92-4e7a-a598-cadda8db2156",
    clientOid: "sclp-5439b557ea-041c518d8dc2",
    openedAtMs: Date.parse("2026-05-23T06:04:00.000Z"),
    exitAtMs: Date.parse("2026-05-23T06:04:57.000Z"),
    mode: "update",
  },
  {
    label: "bch-second",
    ledgerId: "repair-20260523-bitget-bch-second",
    clientOid: "sclp-5439b557ea-901116cb5c63",
    openedAtMs: Date.parse("2026-05-23T06:07:58.000Z"),
    exitAtMs: Date.parse("2026-05-23T06:32:14.000Z"),
    mode: "insert",
  },
] as const;

async function main() {
  const db = scalpPrisma();
  const [deployment] = await db.$queryRaw<
    Array<{
      deploymentId: string;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
    }>
  >(sql`
    SELECT deployment_id AS "deploymentId", venue, symbol, strategy_id AS "strategyId",
           tune_id AS "tuneId", entry_session_profile AS "entrySessionProfile"
    FROM scalp_v2_deployments
    WHERE deployment_id = ${BCH_DEPLOYMENT}
    LIMIT 1
  `);
  if (!deployment) throw new Error(`Missing deployment ${BCH_DEPLOYMENT}`);

  const results = [];
  for (const repair of REPAIRS) {
    const broker = await resolveBitgetBrokerCloseLedger({
      symbol: "BCHUSDT",
      side: "BUY",
      dealReference: repair.clientOid,
      openedAtMs: repair.openedAtMs,
      exitAtMs: repair.exitAtMs,
    });
    const reasonCodes = [
      "LEDGER_REPAIRED_20260523",
      ...broker.reasonCodes,
    ];
    const closeType = deriveCloseTypeFromReasonCodes(reasonCodes);
    results.push({
      ...repair,
      found: broker.found,
      pnlUsd: broker.pnlUsd,
      rMultiple: broker.rMultiple,
      tsExitMs: broker.tsExitMs,
      brokerRef: broker.brokerRef,
      closeType,
    });
    if (!APPLY || !broker.found || !broker.tsExitMs) continue;

    if (repair.mode === "update") {
      await db.$executeRaw(sql`
        UPDATE scalp_v2_ledger
        SET
          ts_exit = TO_TIMESTAMP(${Math.floor(broker.tsExitMs)} / 1000.0),
          close_type = ${closeType},
          r_multiple = ${broker.rMultiple},
          pnl_usd = ${broker.pnlUsd},
          source_of_truth = 'broker',
          reason_codes = ${reasonCodes},
          raw_payload = ${JSON.stringify(broker.rawPayload)}::jsonb
        WHERE id = ${repair.ledgerId}
      `);
    } else {
      await db.$executeRaw(sql`
        INSERT INTO scalp_v2_ledger(
          id, ts_exit, deployment_id, venue, symbol, strategy_id, tune_id,
          entry_session_profile, entry_ref, exit_ref, close_type, r_multiple,
          pnl_usd, source_of_truth, reason_codes, raw_payload, created_at
        ) VALUES (
          ${repair.ledgerId},
          TO_TIMESTAMP(${Math.floor(broker.tsExitMs)} / 1000.0),
          ${deployment.deploymentId},
          ${deployment.venue},
          ${deployment.symbol},
          ${deployment.strategyId},
          ${deployment.tuneId},
          ${deployment.entrySessionProfile},
          ${repair.clientOid},
          ${broker.brokerRef},
          ${closeType},
          ${broker.rMultiple},
          ${broker.pnlUsd},
          'broker',
          ${reasonCodes},
          ${JSON.stringify(broker.rawPayload)}::jsonb,
          NOW()
        )
        ON CONFLICT(id) DO UPDATE SET
          ts_exit = EXCLUDED.ts_exit,
          exit_ref = EXCLUDED.exit_ref,
          close_type = EXCLUDED.close_type,
          r_multiple = EXCLUDED.r_multiple,
          pnl_usd = EXCLUDED.pnl_usd,
          source_of_truth = EXCLUDED.source_of_truth,
          reason_codes = EXCLUDED.reason_codes,
          raw_payload = EXCLUDED.raw_payload
      `);
    }
  }

  if (APPLY) {
    await snapshotScalpV2DailyMetrics({ dayKey: "2026-05-23" });
  }

  console.log(JSON.stringify({ apply: APPLY, deploymentId: BCH_DEPLOYMENT, results }, null, 2));
  await db.$disconnect();
}

main().catch(async (err) => {
  console.error(err instanceof Error ? err.message : err);
  await scalpPrisma().$disconnect().catch(() => undefined);
  process.exit(1);
});
