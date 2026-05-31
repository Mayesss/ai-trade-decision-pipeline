#!/usr/bin/env node
// Read-only: re-score the backtest holdout evidence of the currently-allowed
// (enabled / live / not-retired) deployments AFTER charging round-trip trading
// fees, and report how many would still clear a cost-adjusted gate.
//
// Why: the replay harness historically scored trades fee-free (price + 0.15pip
// slippage only). For tight-stop scalping the bitget 0.06% taker fee is ~0.30R
// per round trip — larger than the per-trade edge — so fee-free backtests rank
// cost-losers as winners. This script quantifies the impact without re-running
// any replay: it reads the stored v5_cell_evidence weekly arrays and subtracts
// trades * feeR per venue.
//
// Fee model (in R): a flat per-venue round-trip cost per trade.
//   - bitget: empirically measured from the live ledger (fees_usd / risk_usd),
//     which is a remarkably stable ~0.30R across symbols. Falls back to
//     DEFAULT_BITGET_FEE_R if no live trades exist yet.
//   - capital (forex): cost is an embedded spread, not an explicit fee. Modeled
//     as 0 here (so forex survival is OPTIMISTIC — see the printed caveat).
//
// Usage:
//   node scripts/with-db-env.mjs node --import tsx scripts/scalp-rescore-fees.ts
//   ... --bitgetFeeR 0.30   (override the per-trade bitget fee in R)
//   ... --gateExpectancyR 0 (min cost-adjusted expectancy/trade to survive)

import nextEnv from "@next/env";

import { isScalpPgConfigured, scalpPrisma } from "../lib/scalp/pg/client";
import { sql } from "../lib/scalp/pg/sql";

const { loadEnvConfig } = nextEnv;
loadEnvConfig(process.cwd());

if (process.env.SCALP_PG_USE_HTTP === undefined) {
  process.env.SCALP_PG_USE_HTTP = "1";
}

const DEFAULT_BITGET_FEE_R = 0.3;

function readNumberArg(flag: string, fallback: number): number {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return fallback;
  const v = Number(process.argv[idx + 1]);
  return Number.isFinite(v) ? v : fallback;
}

const gateExpectancyR = readNumberArg("--gateExpectancyR", 0);
const bitgetFeeROverride =
  process.argv.includes("--bitgetFeeR")
    ? readNumberArg("--bitgetFeeR", DEFAULT_BITGET_FEE_R)
    : null;

type EvidenceRow = {
  deploymentId: string;
  venue: string;
  symbol: string;
  grossNetR: number | null;
  trades: number | null;
  weeks: number | null;
};

function fmt(n: number, dp = 2): string {
  return n.toFixed(dp).padStart(9);
}

async function main() {
  if (!isScalpPgConfigured()) throw new Error("scalp_pg_not_configured");
  const db = scalpPrisma();

  // 1) Empirical bitget round-trip fee in R, straight from the live ledger.
  const feeRows = await db.$queryRaw<Array<{ feeR: number | null; n: bigint }>>(sql`
    SELECT
      AVG(
        (ABS((raw_payload->'bitgetBrokerClose'->'position'->>'openFee')::numeric)
       + ABS((raw_payload->'bitgetBrokerClose'->'position'->>'closeFee')::numeric))
        / NULLIF((raw_payload->>'riskUsd')::numeric, 0)
      )::float8 AS "feeR",
      COUNT(*)::bigint AS n
    FROM scalp_v2_ledger
    WHERE venue = 'bitget'
      AND raw_payload->'bitgetBrokerClose'->'position'->>'openFee' IS NOT NULL
      AND (raw_payload->>'riskUsd')::numeric > 0
  `);
  const empiricalBitgetFeeR = Number(feeRows[0]?.feeR ?? NaN);
  const empiricalN = Number(feeRows[0]?.n ?? 0);
  const bitgetFeeR =
    bitgetFeeROverride ??
    (Number.isFinite(empiricalBitgetFeeR) && empiricalN > 0
      ? empiricalBitgetFeeR
      : DEFAULT_BITGET_FEE_R);

  const feeRForVenue = (venue: string): number =>
    venue === "bitget" ? bitgetFeeR : 0;

  // 2) Holdout backtest evidence for each allowed deployment, summed across its
  //    eligible cells (this is the basis the promotion gate used).
  const rows = await db.$queryRaw<Array<EvidenceRow>>(sql`
    WITH live AS (
      SELECT deployment_id, venue, symbol, v5_cell_evidence AS ev
      FROM scalp_v2_deployments
      WHERE enabled AND live_mode = 'live' AND retired_at IS NULL
        AND v5_cell_evidence IS NOT NULL
    ),
    cellrows AS (
      SELECT l.deployment_id, l.venue, l.symbol,
             jsonb_array_elements_text(l.ev->'eligibleCells') AS ck, l.ev
      FROM live l
    )
    SELECT
      c.deployment_id AS "deploymentId",
      c.venue AS venue,
      c.symbol AS symbol,
      SUM((c.ev->'cells'->c.ck->>'netR')::numeric)::float8   AS "grossNetR",
      SUM((c.ev->'cells'->c.ck->>'trades')::numeric)::float8 AS trades,
      MAX(jsonb_array_length(c.ev->'cells'->c.ck->'weeklyNetR'))::float8 AS weeks
    FROM cellrows c
    GROUP BY 1, 2, 3
    ORDER BY 3
  `);

  // 3) Re-score with fees.
  const scored = rows.map((r) => {
    const gross = Number(r.grossNetR ?? 0);
    const trades = Number(r.trades ?? 0);
    const feeR = feeRForVenue(r.venue);
    const feeDrag = feeR * trades;
    const correctedNetR = gross - feeDrag;
    const grossExp = trades > 0 ? gross / trades : 0;
    const correctedExp = grossExp - feeR;
    const survives = trades > 0 && correctedExp >= gateExpectancyR;
    return {
      symbol: r.symbol,
      venue: r.venue,
      weeks: Number(r.weeks ?? 0),
      gross,
      trades,
      feeR,
      feeDrag,
      correctedNetR,
      grossExp,
      correctedExp,
      survives,
    };
  });
  scored.sort((a, b) => a.correctedNetR - b.correctedNetR);

  // 4) Report.
  console.log("");
  console.log(
    `Cost-adjusted re-score of allowed deployments (read-only) — ${new Date().toISOString()}`,
  );
  console.log(
    `  bitget round-trip fee: ${bitgetFeeR.toFixed(4)} R/trade` +
      (bitgetFeeROverride != null
        ? "  (CLI override)"
        : Number.isFinite(empiricalBitgetFeeR) && empiricalN > 0
          ? `  (empirical, n=${empiricalN} live trades)`
          : `  (default fallback — no live trades)`),
  );
  console.log(`  capital (forex) fee: 0 R/trade  (embedded spread — optimistic)`);
  console.log(`  survival gate: cost-adjusted expectancy >= ${gateExpectancyR} R/trade`);
  console.log("");
  console.log(
    "  SYMBOL    VENUE    WEEKS   TRADES   GROSS_R  FEE_DRAG  CORR_NETR  GROSS_EXP  CORR_EXP  GATE",
  );
  console.log("  " + "-".repeat(94));
  for (const s of scored) {
    console.log(
      "  " +
        s.symbol.padEnd(9) +
        s.venue.padEnd(9) +
        String(s.weeks).padStart(5) +
        String(s.trades).padStart(9) +
        fmt(s.gross) +
        fmt(s.feeDrag) +
        fmt(s.correctedNetR) +
        fmt(s.grossExp, 3) +
        fmt(s.correctedExp, 3) +
        "  " +
        (s.survives ? "SURVIVES" : "CUT"),
    );
  }
  console.log("  " + "-".repeat(94));

  const survivors = scored.filter((s) => s.survives);
  const cut = scored.filter((s) => !s.survives);
  const totalGross = scored.reduce((a, s) => a + s.gross, 0);
  const totalCorrected = scored.reduce((a, s) => a + s.correctedNetR, 0);
  const bitgetTotal = scored.filter((s) => s.venue === "bitget");
  const bitgetSurv = bitgetTotal.filter((s) => s.survives).length;

  console.log("");
  console.log(
    `  Survivors: ${survivors.length}/${scored.length}  ` +
      `(bitget ${bitgetSurv}/${bitgetTotal.length}, ` +
      `capital ${survivors.length - bitgetSurv}/${scored.length - bitgetTotal.length})`,
  );
  console.log(
    `  Cut: ${cut.map((s) => s.symbol).join(", ") || "(none)"}`,
  );
  console.log(
    `  Holdout NetR summed: gross ${totalGross.toFixed(2)} R  ->  fee-adjusted ${totalCorrected.toFixed(2)} R`,
  );
  console.log("");
  console.log(
    "  NOTE: forex 'SURVIVES' is optimistic — Capital's spread is an embedded",
  );
  console.log(
    "        cost not modeled here. Add a per-symbol spread-in-R to gate forex fairly.",
  );
  console.log("");

  await db.$disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
