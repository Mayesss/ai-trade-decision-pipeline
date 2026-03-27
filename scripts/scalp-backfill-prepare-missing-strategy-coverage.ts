import { Prisma } from "@prisma/client";

import { scalpPrisma } from "../lib/scalp/pg/client";
import {
  listScalpEntrySessionProfiles,
  parseScalpEntrySessionProfileStrict,
} from "../lib/scalp/sessions";
import { listScalpStrategies } from "../lib/scalp/strategies/registry";
import type { ScalpEntrySessionProfile } from "../lib/scalp/types";

type ScriptArgs = {
  apply: boolean;
  session: ScalpEntrySessionProfile;
  symbols: string[];
  limit: number;
  verbose: boolean;
};

type MissingCoverageRow = {
  symbol: string;
  prepareStatus: string;
  prepareAttempts: number;
  prepareNextRunAt: Date | null;
  missingStrategyCount: number;
  missingStrategyIds: string[] | null;
};

function normalizeSymbol(value: unknown): string {
  return String(value || "")
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, "");
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.floor(n));
}

function parseArgs(argv: string[]): ScriptArgs {
  const apply = argv.includes("--apply");
  const verbose = argv.includes("--verbose");
  const sessionRaw = argv.find((arg) => arg.startsWith("--session="));
  const session = parseScalpEntrySessionProfileStrict(
    sessionRaw ? String(sessionRaw.split("=")[1] || "") : "berlin",
  );
  if (!session) {
    throw new Error(
      `invalid_session: use --session=${listScalpEntrySessionProfiles().join("|")}`,
    );
  }
  const symbolsRaw = argv.find((arg) => arg.startsWith("--symbols="));
  const symbols = symbolsRaw
    ? String(symbolsRaw.split("=")[1] || "")
        .split(",")
        .map((row) => normalizeSymbol(row))
        .filter((row, idx, arr) => row.length > 0 && arr.indexOf(row) === idx)
    : [];
  const limitRaw = argv.find((arg) => arg.startsWith("--limit="));
  const limit = parsePositiveInt(
    limitRaw ? String(limitRaw.split("=")[1] || "") : undefined,
    5000,
  );
  return { apply, session, symbols, limit, verbose };
}

async function loadMissingCoverageRows(params: {
  session: ScalpEntrySessionProfile;
  requiredStrategyIds: string[];
  symbols: string[];
  limit: number;
}): Promise<MissingCoverageRow[]> {
  if (!params.requiredStrategyIds.length) return [];
  const db = scalpPrisma();
  const symbolFilter =
    params.symbols.length > 0
      ? Prisma.sql`AND s.symbol IN (${Prisma.join(params.symbols)})`
      : Prisma.empty;

  return db.$queryRaw<Array<MissingCoverageRow>>(Prisma.sql`
    WITH required AS (
      SELECT UNNEST(ARRAY[${Prisma.join(params.requiredStrategyIds)}]::text[]) AS strategy_id
    )
    SELECT
      s.symbol,
      s.prepare_status AS "prepareStatus",
      s.prepare_attempts AS "prepareAttempts",
      s.prepare_next_run_at AS "prepareNextRunAt",
      (
        SELECT COUNT(*)
        FROM required r
        WHERE NOT EXISTS (
          SELECT 1
          FROM scalp_deployments d
          WHERE d.symbol = s.symbol
            AND d.entry_session_profile = ${params.session}
            AND d.strategy_id = r.strategy_id
        )
      )::int AS "missingStrategyCount",
      (
        SELECT ARRAY(
          SELECT r.strategy_id
          FROM required r
          WHERE NOT EXISTS (
            SELECT 1
            FROM scalp_deployments d
            WHERE d.symbol = s.symbol
              AND d.entry_session_profile = ${params.session}
              AND d.strategy_id = r.strategy_id
          )
          ORDER BY r.strategy_id
        )
      ) AS "missingStrategyIds"
    FROM scalp_discovered_symbols s
    WHERE s.load_status = 'succeeded'
      ${symbolFilter}
      AND (
        SELECT COUNT(*)
        FROM required r
        WHERE NOT EXISTS (
          SELECT 1
          FROM scalp_deployments d
          WHERE d.symbol = s.symbol
            AND d.entry_session_profile = ${params.session}
            AND d.strategy_id = r.strategy_id
        )
      ) > 0
    ORDER BY s.symbol ASC
    LIMIT ${params.limit};
  `);
}

async function applyBackfill(params: {
  symbols: string[];
}): Promise<number> {
  if (!params.symbols.length) return 0;
  const db = scalpPrisma();
  return Number(
    await db.$executeRaw(Prisma.sql`
      UPDATE scalp_discovered_symbols s
      SET
        prepare_status = 'pending',
        prepare_next_run_at = NOW(),
        prepare_error = NULL,
        updated_at = NOW()
      WHERE s.symbol IN (${Prisma.join(params.symbols)})
        AND s.prepare_status NOT IN ('pending', 'running', 'retry_wait');
    `),
  );
}

function summarizeByPrepareStatus(rows: MissingCoverageRow[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const row of rows) {
    const key = String(row.prepareStatus || "").trim().toLowerCase() || "unknown";
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function summarizeMissingStrategyCounts(rows: MissingCoverageRow[]): Array<{ strategyId: string; symbols: number }> {
  const counts = new Map<string, number>();
  for (const row of rows) {
    const missing = Array.isArray(row.missingStrategyIds) ? row.missingStrategyIds : [];
    for (const strategyIdRaw of missing) {
      const strategyId = String(strategyIdRaw || "").trim().toLowerCase();
      if (!strategyId) continue;
      counts.set(strategyId, (counts.get(strategyId) || 0) + 1);
    }
  }
  return Array.from(counts.entries())
    .map(([strategyId, symbols]) => ({ strategyId, symbols }))
    .sort((a, b) => b.symbols - a.symbols || a.strategyId.localeCompare(b.strategyId));
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const requiredStrategyIds = listScalpStrategies().map((row) => row.id);
  const rows = await loadMissingCoverageRows({
    session: args.session,
    requiredStrategyIds,
    symbols: args.symbols,
    limit: args.limit,
  });
  const candidateSymbols = rows.map((row) => row.symbol);
  const touchedSymbols = args.apply
    ? await applyBackfill({ symbols: candidateSymbols })
    : 0;

  const sample = (args.verbose ? rows : rows.slice(0, 25)).map((row) => ({
    symbol: row.symbol,
    prepareStatus: row.prepareStatus,
    prepareAttempts: Number(row.prepareAttempts || 0),
    prepareNextRunAt: row.prepareNextRunAt ? new Date(row.prepareNextRunAt).toISOString() : null,
    missingStrategyCount: Number(row.missingStrategyCount || 0),
    missingStrategyIds: Array.isArray(row.missingStrategyIds)
      ? row.missingStrategyIds
      : [],
  }));

  console.log(
    JSON.stringify(
      {
        mode: args.apply ? "apply" : "dry-run",
        session: args.session,
        symbolsFilter: args.symbols,
        requiredStrategyCount: requiredStrategyIds.length,
        requiredStrategyIds,
        candidateSymbols: candidateSymbols.length,
        touchedSymbols,
        prepareStatusBreakdown: summarizeByPrepareStatus(rows),
        missingByStrategy: summarizeMissingStrategyCounts(rows),
        sample,
        nextStep: args.apply
          ? "Run /api/scalp/v2/cron/prepare for this session."
          : "Re-run with --apply to set prepare_status=pending for eligible symbols.",
      },
      null,
      2,
    ),
  );
}

main()
  .catch((err) => {
    console.error(String(err?.stack || err?.message || err));
    process.exitCode = 1;
  })
  .finally(async () => {
    await scalpPrisma().$disconnect();
  });
