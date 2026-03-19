import { Prisma } from '@prisma/client';
import { scalpPrisma } from '../lib/scalp/pg/client';

function normalizeSymbol(value: string): string {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^A-Z0-9._-]/g, '');
}

function parseArgs(argv: string[]) {
  const apply = argv.includes('--apply');
  const symbolsArg = argv.find((arg) => arg.startsWith('--symbols='));
  const symbols = (symbolsArg ? String(symbolsArg.split('=')[1] || '') : 'XANUSDT,XPLUSDT')
    .split(',')
    .map((s) => normalizeSymbol(s))
    .filter((s, idx, arr) => Boolean(s) && arr.indexOf(s) === idx);
  return { apply, symbols };
}

async function snapshot(symbols: string[]) {
  const db = scalpPrisma();
  const [pipelineRows, deploymentRows] = await Promise.all([
    db.$queryRaw<Array<{
      symbol: string;
      active: boolean;
      discoverStatus: string;
      loadStatus: string;
      loadNextRunAt: Date | null;
      prepareStatus: string;
      prepareNextRunAt: Date | null;
      discoverError: string | null;
      loadError: string | null;
      prepareError: string | null;
    }>>(Prisma.sql`
      SELECT
        symbol,
        active,
        discover_status AS "discoverStatus",
        load_status AS "loadStatus",
        load_next_run_at AS "loadNextRunAt",
        prepare_status AS "prepareStatus",
        prepare_next_run_at AS "prepareNextRunAt",
        discover_error AS "discoverError",
        load_error AS "loadError",
        prepare_error AS "prepareError"
      FROM scalp_pipeline_symbols
      WHERE symbol IN (${Prisma.join(symbols)})
      ORDER BY symbol ASC;
    `),
    db.$queryRaw<Array<{
      symbol: string;
      inUniverse: boolean;
      enabled: boolean;
      retiredAt: Date | null;
      workerDirty: boolean;
      promotionDirty: boolean;
      c: bigint | number;
    }>>(Prisma.sql`
      SELECT
        symbol,
        in_universe AS "inUniverse",
        enabled,
        retired_at AS "retiredAt",
        worker_dirty AS "workerDirty",
        promotion_dirty AS "promotionDirty",
        COUNT(*)::bigint AS c
      FROM scalp_deployments
      WHERE symbol IN (${Prisma.join(symbols)})
      GROUP BY symbol, in_universe, enabled, retired_at, worker_dirty, promotion_dirty
      ORDER BY symbol ASC, in_universe DESC, enabled DESC;
    `),
  ]);

  return {
    pipelineRows,
    deploymentRows: deploymentRows.map((r) => ({ ...r, c: Number(r.c || 0) })),
  };
}

async function main() {
  const { apply, symbols } = parseArgs(process.argv.slice(2));
  if (!symbols.length) {
    console.log(JSON.stringify({ error: 'no_symbols' }, null, 2));
    return;
  }

  const before = await snapshot(symbols);

  if (!apply) {
    console.log(JSON.stringify({ mode: 'dry-run', symbols, before }, null, 2));
    return;
  }

  const db = scalpPrisma();

  for (const symbol of symbols) {
    await db.$executeRaw(Prisma.sql`
      INSERT INTO scalp_pipeline_symbols(
        symbol,
        active,
        discover_status,
        discover_attempts,
        discover_next_run_at,
        discover_error,
        last_discovered_at,
        load_status,
        load_attempts,
        load_next_run_at,
        load_error,
        prepare_status,
        prepare_attempts,
        prepare_next_run_at,
        prepare_error,
        updated_at
      )
      VALUES(
        ${symbol},
        TRUE,
        'succeeded',
        0,
        NULL,
        NULL,
        NOW(),
        'pending',
        0,
        NOW(),
        NULL,
        'pending',
        0,
        NOW(),
        NULL,
        NOW()
      )
      ON CONFLICT(symbol)
      DO UPDATE SET
        active = TRUE,
        discover_status = 'succeeded',
        discover_attempts = 0,
        discover_next_run_at = NULL,
        discover_error = NULL,
        last_discovered_at = NOW(),
        load_status = CASE
          WHEN scalp_pipeline_symbols.load_status IN ('pending', 'running', 'retry_wait')
            THEN scalp_pipeline_symbols.load_status
          ELSE 'pending'
        END,
        load_attempts = CASE
          WHEN scalp_pipeline_symbols.load_status IN ('pending', 'running', 'retry_wait')
            THEN scalp_pipeline_symbols.load_attempts
          ELSE 0
        END,
        load_next_run_at = CASE
          WHEN scalp_pipeline_symbols.load_status IN ('pending', 'running', 'retry_wait')
            THEN scalp_pipeline_symbols.load_next_run_at
          ELSE NOW()
        END,
        load_error = NULL,
        prepare_status = CASE
          WHEN scalp_pipeline_symbols.prepare_status IN ('pending', 'running', 'retry_wait')
            THEN scalp_pipeline_symbols.prepare_status
          ELSE 'pending'
        END,
        prepare_attempts = CASE
          WHEN scalp_pipeline_symbols.prepare_status IN ('pending', 'running', 'retry_wait')
            THEN scalp_pipeline_symbols.prepare_attempts
          ELSE 0
        END,
        prepare_next_run_at = CASE
          WHEN scalp_pipeline_symbols.prepare_status IN ('pending', 'running', 'retry_wait')
            THEN scalp_pipeline_symbols.prepare_next_run_at
          ELSE NOW()
        END,
        prepare_error = NULL,
        updated_at = NOW();
    `);
  }

  const deploymentsUpdated = Number(
    await db.$executeRaw(Prisma.sql`
      UPDATE scalp_deployments
      SET
        in_universe = TRUE,
        retired_at = NULL,
        worker_dirty = TRUE,
        updated_by = 'script:restore-pipeline-symbols',
        updated_at = NOW()
      WHERE symbol IN (${Prisma.join(symbols)});
    `),
  );

  const after = await snapshot(symbols);

  console.log(
    JSON.stringify(
      {
        mode: 'apply',
        symbols,
        deploymentsUpdated,
        before,
        after,
      },
      null,
      2,
    ),
  );
}

main()
  .catch((err) => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await scalpPrisma().$disconnect();
  });
