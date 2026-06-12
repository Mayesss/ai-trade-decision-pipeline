#!/usr/bin/env node
// One-shot backfill: for each completed walk-forward result, synthesize an
// incremental_state_json from envelope.cells so the next sweep doesn't have
// to re-replay 85 windows × ~200 candidates. The synthesis approximates
// per-window expectancy distribution from stored mean + bootstrap p05 +
// positive-window pct. Future windows are aggregated INCREMENTALLY on top.
import {
  initIncrementalStateFromEnvelope,
  SCALP_V4_CLASSIFIER_VERSION,
  type ScalpRegimeEnvelope,
} from "../lib/scalp/regimes";
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
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    id: string;
    deploymentId: string;
    candidateId: string | null;
    status: string;
    windowFrom: Date;
    windowTo: Date;
    envelopeJson: unknown;
    hasIncremental: boolean;
  }>>(sql`
    SELECT
      id::text AS id,
      deployment_id AS "deploymentId",
      candidate_id::text AS "candidateId",
      status,
      window_from AS "windowFrom",
      window_to AS "windowTo",
      envelope_json AS "envelopeJson",
      (incremental_state_json IS NOT NULL) AS "hasIncremental"
    FROM scalp_regime_walkforward_results
    WHERE classifier_version = ${classifierVersion}
      AND status <> 'in_progress'
      AND envelope_json IS NOT NULL;
  `);
  const summary = {
    classifierVersion,
    inspected: rows.length,
    backfilled: 0,
    skippedHasIncremental: 0,
    skippedMissingCells: 0,
    errors: 0,
  };
  for (const row of rows) {
    if (row.hasIncremental) {
      summary.skippedHasIncremental += 1;
      continue;
    }
    const envelope = row.envelopeJson as ScalpRegimeEnvelope | null;
    if (!envelope || !Array.isArray(envelope.cells) || envelope.cells.length === 0) {
      summary.skippedMissingCells += 1;
      continue;
    }
    try {
      const state = initIncrementalStateFromEnvelope({
        envelope,
        windowFromMs: row.windowFrom.getTime(),
        windowToMs: row.windowTo.getTime(),
        synthesizeFromSummary: true,
        seed: row.deploymentId,
      });
      if (!dryRun) {
        await db.$executeRaw(sql`
          UPDATE scalp_regime_walkforward_results
          SET
            incremental_state_json = ${JSON.stringify(state)}::jsonb,
            next_window_start = ${row.windowTo},
            updated_at = NOW()
          WHERE id = ${BigInt(row.id)}::bigint;
        `);
      }
      summary.backfilled += 1;
    } catch (err) {
      summary.errors += 1;
      console.error(`backfill failed for deployment ${row.deploymentId}:`, (err as Error)?.message || err);
    }
  }
  console.log(JSON.stringify({ ok: summary.errors === 0, dryRun, ...summary }, null, 2));
}

main().catch((err) => {
  console.error(err?.stack || err?.message || String(err));
  process.exit(1);
});
