import { Prisma } from "@prisma/client";

import { isScalpPgConfigured, scalpPrisma } from "../lib/scalp/pg/client";

type Counts = {
  capitalDeployments: number;
  deploymentSymbols: number;
  journalByDeployment: number;
  sessionsByDeployment: number;
  executionRunsByDeployment: number;
  tradeLedgerByDeployment: number;
  weeklyMetricsByDeployment: number;
  researchTasksByDeployment: number;
  symbolMetadataCapital: number;
  candleWeeksCapital: number;
  jobsCapital: number;
  pipelineJobRunsCapital: number;
  pipelineJobsProgressCapital: number;
  universeSnapshotsCapital: number;
  pipelineSymbolsCapitalOnly: number;
  cooldownsCapitalOnly: number;
  journalCapitalPayload: number;
};

const tableExistsCache = new Map<string, boolean>();

function parseArgs(argv: string[]) {
  const apply = argv.includes("--apply");
  const verbose = argv.includes("--verbose");
  return {
    apply,
    dryRun: !apply,
    verbose,
  };
}

function uniq(values: string[]): string[] {
  return Array.from(
    new Set(
      values
        .map((row) => String(row || "").trim().toUpperCase())
        .filter(Boolean),
    ),
  ).sort();
}

function asErrorCode(err: unknown): string {
  return String((err as any)?.code || "").trim();
}

function isMissingRelationError(err: unknown): boolean {
  const code = asErrorCode(err);
  if (code === "42P01") return true;
  const msg = String((err as any)?.message || err || "").toLowerCase();
  return (
    msg.includes("does not exist") &&
    (msg.includes("relation") || msg.includes("table"))
  );
}

async function tableExists(tableName: string): Promise<boolean> {
  const normalized = String(tableName || "").trim().toLowerCase();
  if (!normalized) return false;
  if (tableExistsCache.has(normalized)) {
    return Boolean(tableExistsCache.get(normalized));
  }
  const db = scalpPrisma();
  try {
    const rows = await db.$queryRaw<Array<{ exists: boolean }>>(Prisma.sql`
      SELECT to_regclass(${`public.${normalized}`}) IS NOT NULL AS "exists";
    `);
    const exists = Boolean(rows[0]?.exists);
    tableExistsCache.set(normalized, exists);
    return exists;
  } catch (err) {
    if (isMissingRelationError(err)) {
      tableExistsCache.set(normalized, false);
      return false;
    }
    throw err;
  }
}

async function loadTablePresence(
  tableNames: string[],
): Promise<Record<string, boolean>> {
  const out: Record<string, boolean> = {};
  for (const tableName of tableNames) {
    out[tableName] = await tableExists(tableName);
  }
  return out;
}

async function countQueryIfTableExists(
  tableName: string,
  query: () => Promise<Array<{ count: bigint | number }>>,
): Promise<number> {
  if (!(await tableExists(tableName))) return 0;
  const rows = await query();
  return Number(rows[0]?.count || 0);
}

async function loadCapitalDeploymentRows() {
  if (!(await tableExists("scalp_deployments"))) return [];
  const db = scalpPrisma();
  return db.$queryRaw<Array<{ deploymentId: string; symbol: string }>>(Prisma.sql`
    SELECT deployment_id AS "deploymentId", symbol
    FROM scalp_deployments
    WHERE deployment_id LIKE 'capital:%'
       OR deployment_id NOT LIKE '%:%'
    ORDER BY deployment_id ASC;
  `);
}

async function loadSymbolsBySource(source: "capital" | "bitget") {
  const db = scalpPrisma();
  const hasMeta = await tableExists("scalp_symbol_market_metadata");
  const hasCandles = await tableExists("scalp_candle_history_weeks");
  const [metaRows, candleRows] = await Promise.all([
    hasMeta
      ? db.$queryRaw<Array<{ symbol: string }>>(Prisma.sql`
          SELECT symbol
          FROM scalp_symbol_market_metadata
          WHERE source = ${source};
        `)
      : Promise.resolve([]),
    hasCandles
      ? db.$queryRaw<Array<{ symbol: string }>>(Prisma.sql`
          SELECT DISTINCT symbol
          FROM scalp_candle_history_weeks
          WHERE source = ${source};
        `)
      : Promise.resolve([]),
  ]);
  return uniq([
    ...metaRows.map((row) => row.symbol),
    ...candleRows.map((row) => row.symbol),
  ]);
}

async function loadBitgetDeploymentSymbols() {
  if (!(await tableExists("scalp_deployments"))) return [];
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ symbol: string }>>(Prisma.sql`
    SELECT DISTINCT symbol
    FROM scalp_deployments
    WHERE deployment_id LIKE 'bitget:%';
  `);
  return uniq(rows.map((row) => row.symbol));
}

async function countByDeploymentIds(
  tableName: string,
  columnName: string,
  deploymentIds: string[],
): Promise<number> {
  if (!deploymentIds.length) return 0;
  if (!(await tableExists(tableName))) return 0;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
    SELECT COUNT(*)::bigint AS count
    FROM ${Prisma.raw(tableName)}
    WHERE ${Prisma.raw(columnName)} IN (${Prisma.join(deploymentIds)});
  `);
  return Number(rows[0]?.count || 0);
}

async function countBySymbols(
  tableName: string,
  symbols: string[],
): Promise<number> {
  if (!symbols.length) return 0;
  if (!(await tableExists(tableName))) return 0;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
    SELECT COUNT(*)::bigint AS count
    FROM ${Prisma.raw(tableName)}
    WHERE symbol IN (${Prisma.join(symbols)});
  `);
  return Number(rows[0]?.count || 0);
}

async function collectCounts(params: {
  capitalDeploymentIds: string[];
  capitalOnlySymbols: string[];
}): Promise<Counts> {
  const db = scalpPrisma();
  const [
    journalByDeployment,
    sessionsByDeployment,
    executionRunsByDeployment,
    tradeLedgerByDeployment,
    weeklyMetricsByDeployment,
    researchTasksByDeployment,
    pipelineSymbolsCapitalOnly,
    cooldownsCapitalOnly,
    symbolMetadataCapitalRows,
    candleWeeksCapitalRows,
    jobsCapitalRows,
    pipelineJobRunsCapitalRows,
    pipelineJobsProgressCapitalRows,
    universeSnapshotsCapitalRows,
    journalCapitalPayloadRows,
  ] = await Promise.all([
    countByDeploymentIds(
      "scalp_journal",
      "deployment_id",
      params.capitalDeploymentIds,
    ),
    countByDeploymentIds(
      "scalp_sessions",
      "deployment_id",
      params.capitalDeploymentIds,
    ),
    countByDeploymentIds(
      "scalp_execution_runs",
      "deployment_id",
      params.capitalDeploymentIds,
    ),
    countByDeploymentIds(
      "scalp_trade_ledger",
      "deployment_id",
      params.capitalDeploymentIds,
    ),
    countByDeploymentIds(
      "scalp_deployment_weekly_metrics",
      "deployment_id",
      params.capitalDeploymentIds,
    ),
    countByDeploymentIds(
      "scalp_research_tasks",
      "deployment_id",
      params.capitalDeploymentIds,
    ),
    countBySymbols("scalp_pipeline_symbols", params.capitalOnlySymbols),
    countBySymbols("scalp_symbol_cooldowns", params.capitalOnlySymbols),
    countQueryIfTableExists("scalp_symbol_market_metadata", () =>
      db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_symbol_market_metadata
        WHERE source = 'capital';
      `),
    ),
    countQueryIfTableExists("scalp_candle_history_weeks", () =>
      db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_candle_history_weeks
        WHERE source = 'capital';
      `),
    ),
    countQueryIfTableExists("scalp_jobs", () =>
      db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_jobs
        WHERE dedupe_key LIKE 'scalp_execute_deployments_mutex_v2:capital'
           OR dedupe_key LIKE 'scalp_execute_deployments_mutex_v2:capital:%'
           OR payload::text ILIKE '%"venue":"capital"%'
           OR payload::text ILIKE '%capital:%'
           OR payload::text ILIKE '%"source":"capital"%';
      `),
    ),
    countQueryIfTableExists("scalp_pipeline_job_runs", () =>
      db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_pipeline_job_runs
        WHERE details_json::text ILIKE '%capital%';
      `),
    ),
    countQueryIfTableExists("scalp_pipeline_jobs", () =>
      db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_pipeline_jobs
        WHERE progress_json::text ILIKE '%capital%';
      `),
    ),
    countQueryIfTableExists("scalp_symbol_universe_snapshots", () =>
      db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_symbol_universe_snapshots
        WHERE payload_json::text ILIKE '%capital%';
      `),
    ),
    countQueryIfTableExists("scalp_journal", () =>
      db.$queryRaw<Array<{ count: bigint | number }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS count
        FROM scalp_journal
        WHERE payload::text ILIKE '%capital%'
           OR reason_codes::text ILIKE '%capital%';
      `),
    ),
  ]);

  return {
    capitalDeployments: params.capitalDeploymentIds.length,
    deploymentSymbols: params.capitalOnlySymbols.length,
    journalByDeployment,
    sessionsByDeployment,
    executionRunsByDeployment,
    tradeLedgerByDeployment,
    weeklyMetricsByDeployment,
    researchTasksByDeployment,
    symbolMetadataCapital: symbolMetadataCapitalRows,
    candleWeeksCapital: candleWeeksCapitalRows,
    jobsCapital: jobsCapitalRows,
    pipelineJobRunsCapital: pipelineJobRunsCapitalRows,
    pipelineJobsProgressCapital: pipelineJobsProgressCapitalRows,
    universeSnapshotsCapital: universeSnapshotsCapitalRows,
    pipelineSymbolsCapitalOnly,
    cooldownsCapitalOnly,
    journalCapitalPayload: journalCapitalPayloadRows,
  };
}

async function applyPurge(params: {
  capitalDeploymentIds: string[];
  capitalOnlySymbols: string[];
}): Promise<void> {
  const db = scalpPrisma();
  const has = await loadTablePresence([
    "scalp_trade_ledger",
    "scalp_journal",
    "scalp_research_tasks",
    "scalp_deployment_weekly_metrics",
    "scalp_sessions",
    "scalp_execution_runs",
    "scalp_deployments",
    "scalp_symbol_market_metadata",
    "scalp_candle_history_weeks",
    "scalp_jobs",
    "scalp_pipeline_job_runs",
    "scalp_pipeline_jobs",
    "scalp_symbol_universe_snapshots",
    "scalp_pipeline_symbols",
    "scalp_symbol_cooldowns",
  ]);
  await db.$transaction(async (tx) => {
    if (params.capitalDeploymentIds.length) {
      if (has.scalp_trade_ledger) {
        await tx.$executeRaw(Prisma.sql`
          DELETE FROM scalp_trade_ledger
          WHERE deployment_id IN (${Prisma.join(params.capitalDeploymentIds)});
        `);
      }
      if (has.scalp_journal) {
        await tx.$executeRaw(Prisma.sql`
          DELETE FROM scalp_journal
          WHERE deployment_id IN (${Prisma.join(params.capitalDeploymentIds)});
        `);
      }
      if (has.scalp_research_tasks) {
        await tx.$executeRaw(Prisma.sql`
          DELETE FROM scalp_research_tasks
          WHERE deployment_id IN (${Prisma.join(params.capitalDeploymentIds)});
        `);
      }
      if (has.scalp_deployment_weekly_metrics) {
        await tx.$executeRaw(Prisma.sql`
          DELETE FROM scalp_deployment_weekly_metrics
          WHERE deployment_id IN (${Prisma.join(params.capitalDeploymentIds)});
        `);
      }
      if (has.scalp_sessions) {
        await tx.$executeRaw(Prisma.sql`
          DELETE FROM scalp_sessions
          WHERE deployment_id IN (${Prisma.join(params.capitalDeploymentIds)});
        `);
      }
      if (has.scalp_execution_runs) {
        await tx.$executeRaw(Prisma.sql`
          DELETE FROM scalp_execution_runs
          WHERE deployment_id IN (${Prisma.join(params.capitalDeploymentIds)});
        `);
      }
      if (has.scalp_deployments) {
        await tx.$executeRaw(Prisma.sql`
          DELETE FROM scalp_deployments
          WHERE deployment_id IN (${Prisma.join(params.capitalDeploymentIds)});
        `);
      }
    }

    if (has.scalp_symbol_market_metadata) {
      await tx.$executeRaw(Prisma.sql`
        DELETE FROM scalp_symbol_market_metadata
        WHERE source = 'capital';
      `);
    }
    if (has.scalp_candle_history_weeks) {
      await tx.$executeRaw(Prisma.sql`
        DELETE FROM scalp_candle_history_weeks
        WHERE source = 'capital';
      `);
    }
    if (has.scalp_jobs) {
      await tx.$executeRaw(Prisma.sql`
        DELETE FROM scalp_jobs
        WHERE dedupe_key LIKE 'scalp_execute_deployments_mutex_v2:capital'
           OR dedupe_key LIKE 'scalp_execute_deployments_mutex_v2:capital:%'
           OR payload::text ILIKE '%"venue":"capital"%'
           OR payload::text ILIKE '%capital:%'
           OR payload::text ILIKE '%"source":"capital"%';
      `);
    }
    if (has.scalp_pipeline_job_runs) {
      await tx.$executeRaw(Prisma.sql`
        DELETE FROM scalp_pipeline_job_runs
        WHERE details_json::text ILIKE '%capital%';
      `);
    }
    if (has.scalp_pipeline_jobs) {
      await tx.$executeRaw(Prisma.sql`
        UPDATE scalp_pipeline_jobs
        SET
          progress_json = NULL,
          progress_label = NULL,
          updated_at = NOW()
        WHERE progress_json::text ILIKE '%capital%';
      `);
    }
    if (has.scalp_symbol_universe_snapshots) {
      await tx.$executeRaw(Prisma.sql`
        DELETE FROM scalp_symbol_universe_snapshots
        WHERE payload_json::text ILIKE '%capital%';
      `);
    }
    if (has.scalp_journal) {
      await tx.$executeRaw(Prisma.sql`
        DELETE FROM scalp_journal
        WHERE payload::text ILIKE '%capital%'
           OR reason_codes::text ILIKE '%capital%';
      `);
    }

    if (params.capitalOnlySymbols.length) {
      if (has.scalp_pipeline_symbols) {
        await tx.$executeRaw(Prisma.sql`
          DELETE FROM scalp_pipeline_symbols
          WHERE symbol IN (${Prisma.join(params.capitalOnlySymbols)});
        `);
      }
      if (has.scalp_symbol_cooldowns) {
        await tx.$executeRaw(Prisma.sql`
          DELETE FROM scalp_symbol_cooldowns
          WHERE symbol IN (${Prisma.join(params.capitalOnlySymbols)});
        `);
      }
    }
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!isScalpPgConfigured()) {
    throw new Error("scalp_pg_not_configured");
  }

  const capitalDeploymentRows = await loadCapitalDeploymentRows();
  const capitalDeploymentIds = capitalDeploymentRows.map((row) => row.deploymentId);
  const capitalDeploymentSymbols = uniq(capitalDeploymentRows.map((row) => row.symbol));
  const capitalSourceSymbols = await loadSymbolsBySource("capital");
  const bitgetSourceSymbols = await loadSymbolsBySource("bitget");
  const bitgetDeploymentSymbols = await loadBitgetDeploymentSymbols();
  const bitgetSymbols = new Set<string>([
    ...bitgetSourceSymbols,
    ...bitgetDeploymentSymbols,
  ]);
  const capitalOnlySymbols = uniq(
    [...capitalSourceSymbols, ...capitalDeploymentSymbols].filter(
      (symbol) => !bitgetSymbols.has(symbol),
    ),
  );

  const before = await collectCounts({
    capitalDeploymentIds,
    capitalOnlySymbols,
  });

  const output: Record<string, unknown> = {
    ok: true,
    dryRun: args.dryRun,
    apply: args.apply,
    countsBefore: before,
    capitalDeploymentIdSamples: capitalDeploymentIds.slice(0, 20),
    capitalOnlySymbolSamples: capitalOnlySymbols.slice(0, 40),
  };

  if (args.verbose) {
    output.capitalDeploymentIds = capitalDeploymentIds;
    output.capitalOnlySymbols = capitalOnlySymbols;
  }

  if (!args.dryRun) {
    await applyPurge({ capitalDeploymentIds, capitalOnlySymbols });

    const afterCapitalDeploymentRows = await loadCapitalDeploymentRows();
    const afterCapitalDeploymentIds = afterCapitalDeploymentRows.map(
      (row) => row.deploymentId,
    );
    const afterCapitalDeploymentSymbols = uniq(
      afterCapitalDeploymentRows.map((row) => row.symbol),
    );
    const afterCapitalSourceSymbols = await loadSymbolsBySource("capital");
    const afterBitgetSourceSymbols = await loadSymbolsBySource("bitget");
    const afterBitgetDeploymentSymbols = await loadBitgetDeploymentSymbols();
    const afterBitgetSymbols = new Set<string>([
      ...afterBitgetSourceSymbols,
      ...afterBitgetDeploymentSymbols,
    ]);
    const afterCapitalOnlySymbols = uniq(
      [...afterCapitalSourceSymbols, ...afterCapitalDeploymentSymbols].filter(
        (symbol) => !afterBitgetSymbols.has(symbol),
      ),
    );

    const after = await collectCounts({
      capitalDeploymentIds: afterCapitalDeploymentIds,
      capitalOnlySymbols: afterCapitalOnlySymbols,
    });
    output.countsAfter = after;
  }

  console.log(JSON.stringify(output, null, 2));
}

main()
  .catch((err) => {
    console.error(
      JSON.stringify(
        {
          ok: false,
          error: String((err as any)?.message || err || "unknown_error"),
        },
        null,
        2,
      ),
    );
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await scalpPrisma().$disconnect();
    } catch {
      // no-op
    }
  });
