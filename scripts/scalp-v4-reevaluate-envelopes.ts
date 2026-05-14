#!/usr/bin/env node
// Re-evaluate completed scalp_regime_walkforward_results rows against the
// current envelope thresholds (env-configurable) without re-running any
// replay. Used after threshold tuning to immediately reclassify
// already-evaluated candidates as eligible / no_passing_cells / overbroad.
import {
  reevaluateScalpV4EnvelopeFromCells,
  resolveScalpV4EnvelopeThresholds,
  SCALP_V4_CLASSIFIER_VERSION,
  type ScalpV4RegimeEnvelope,
} from "../lib/scalp-v4";
import { scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";

function parseArgs(argv: string[]): Record<string, string | boolean> {
  const out: Record<string, string | boolean> = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i]!;
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith("--")) out[key] = true;
    else {
      out[key] = next;
      i += 1;
    }
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const dryRun = Boolean(args.dryRun);
  const classifierVersion = String(args.classifierVersion || SCALP_V4_CLASSIFIER_VERSION);
  const thresholds = resolveScalpV4EnvelopeThresholds();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    id: string;
    deploymentId: string;
    candidateId: string | null;
    status: string;
    envelopeJson: unknown;
  }>>(sql`
    SELECT
      id::text AS id,
      deployment_id AS "deploymentId",
      candidate_id::text AS "candidateId",
      status,
      envelope_json AS "envelopeJson"
    FROM scalp_regime_walkforward_results
    WHERE classifier_version = ${classifierVersion}
      AND status <> 'in_progress'
      AND status <> 'regime_overbroad_auto_rejected'
      AND envelope_json IS NOT NULL;
  `);
  const summary = {
    classifierVersion,
    thresholds,
    inspected: rows.length,
    statusBefore: {} as Record<string, number>,
    statusAfter: {} as Record<string, number>,
    flipped: 0,
    sample: [] as Array<{ deploymentId: string; before: string; after: string; allowedCells: string[] }>,
  };
  for (const row of rows) {
    const envelope = row.envelopeJson as ScalpV4RegimeEnvelope | null;
    if (!envelope || !Array.isArray(envelope.cells)) continue;
    summary.statusBefore[row.status] = (summary.statusBefore[row.status] || 0) + 1;
    const next = reevaluateScalpV4EnvelopeFromCells({ envelope, thresholds });
    summary.statusAfter[next.status] = (summary.statusAfter[next.status] || 0) + 1;
    if (next.status !== row.status) {
      summary.flipped += 1;
      if (summary.sample.length < 20) {
        summary.sample.push({
          deploymentId: row.deploymentId,
          before: row.status,
          after: next.status,
          allowedCells: next.allowedCells,
        });
      }
    }
    if (dryRun) continue;
    await db.$executeRaw(sql`
      UPDATE scalp_regime_walkforward_results
      SET
        status = ${next.status},
        envelope_json = ${JSON.stringify(next)}::jsonb,
        auto_reject_after = ${next.overbroadReviewUntilMs ? new Date(next.overbroadReviewUntilMs) : null},
        updated_at = NOW()
      WHERE id = ${BigInt(row.id)}::bigint;
    `);
    await db.$executeRaw(sql`
      UPDATE scalp_v2_deployments
      SET
        promotion_gate = jsonb_set(COALESCE(promotion_gate, '{}'::jsonb), '{regimeEnvelope}', ${JSON.stringify(next)}::jsonb, true),
        updated_at = NOW()
      WHERE deployment_id = ${row.deploymentId};
    `);
  }
  console.log(JSON.stringify({ ok: true, dryRun, ...summary }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
