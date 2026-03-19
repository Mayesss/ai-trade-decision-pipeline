import { Prisma } from '@prisma/client';

import { scalpPrisma } from '../lib/scalp/pg/client';
import { formatScalpVenueDeploymentId, parseScalpVenuePrefixedDeploymentId, type ScalpVenue } from '../lib/scalp/venue';

type Candidate = {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  targetSource: ScalpVenue;
  metadataSource: ScalpVenue | null;
  historySource: ScalpVenue | null;
  historyEpic: string | null;
};

type DeploymentRow = {
  deploymentId: string;
  symbol: string;
  strategyId: string;
  tuneId: string;
  source: string;
  enabled: boolean;
  configOverride: unknown;
  promotionGate: unknown;
  inUniverse: boolean;
  workerDirty: boolean;
  promotionDirty: boolean;
  retiredAt: Date | null;
  lastPreparedAt: Date | null;
  createdAt: Date;
};

function toTargetDeploymentId(currentId: string, targetVenue: ScalpVenue): string {
  const parsed = parseScalpVenuePrefixedDeploymentId(currentId);
  const key = parsed.deploymentKey || String(currentId || '').trim();
  return formatScalpVenueDeploymentId(targetVenue, key);
}

function parseArgs(argv: string[]) {
  const apply = argv.includes('--apply');
  const onlySymbolRaw = argv.find((arg) => arg.startsWith('--symbol='));
  const onlySymbol = onlySymbolRaw ? String(onlySymbolRaw.split('=')[1] || '').trim().toUpperCase() : '';
  return { apply, onlySymbol };
}

async function loadCandidates(params: { onlySymbol?: string }): Promise<Candidate[]> {
  const db = scalpPrisma();
  const symbolFilter = params.onlySymbol
    ? Prisma.sql`AND d.symbol = ${params.onlySymbol}`
    : Prisma.empty;

  const rows = await db.$queryRaw<Array<Candidate>>(Prisma.sql`
    WITH history_ranked AS (
      SELECT
        symbol,
        source,
        epic,
        COUNT(*)::bigint AS c,
        ROW_NUMBER() OVER (
          PARTITION BY symbol
          ORDER BY COUNT(*) DESC, CASE WHEN source = 'bitget' THEN 0 ELSE 1 END
        ) AS rn
      FROM scalp_candle_history_weeks
      GROUP BY symbol, source, epic
    ),
    history_dominant AS (
      SELECT
        symbol,
        source AS history_source,
        epic AS history_epic
      FROM history_ranked
      WHERE rn = 1
    )
    SELECT
      d.deployment_id AS "deploymentId",
      d.symbol,
      d.strategy_id AS "strategyId",
      d.tune_id AS "tuneId",
      CASE
        WHEN m.source = 'bitget' THEN 'bitget'
        WHEN m.source = 'capital' THEN 'capital'
        ELSE NULL
      END AS "metadataSource",
      CASE
        WHEN h.history_source = 'bitget' THEN 'bitget'
        WHEN h.history_source = 'capital' THEN 'capital'
        ELSE NULL
      END AS "historySource",
      h.history_epic AS "historyEpic",
      CASE
        WHEN h.history_source = 'bitget' THEN 'bitget'
        WHEN h.history_source = 'capital' THEN 'capital'
        WHEN m.source = 'bitget' THEN 'bitget'
        ELSE 'capital'
      END AS "targetSource"
    FROM scalp_deployments d
    LEFT JOIN scalp_symbol_market_metadata m
      ON m.symbol = d.symbol
    LEFT JOIN history_dominant h
      ON h.symbol = d.symbol
    WHERE COALESCE(h.history_source, m.source, 'capital') IN ('capital', 'bitget')
      ${symbolFilter}
      AND (
        (COALESCE(h.history_source, m.source, 'capital') = 'capital' AND d.deployment_id LIKE 'bitget:%')
        OR
        (COALESCE(h.history_source, m.source, 'capital') = 'bitget' AND d.deployment_id NOT LIKE 'bitget:%')
      )
    ORDER BY d.symbol ASC, d.deployment_id ASC;
  `);

  return rows;
}

async function loadDeploymentRow(
  tx: Prisma.TransactionClient,
  deploymentId: string,
): Promise<DeploymentRow | null> {
  const rows = await tx.$queryRaw<Array<DeploymentRow>>(Prisma.sql`
    SELECT
      deployment_id AS "deploymentId",
      symbol,
      strategy_id AS "strategyId",
      tune_id AS "tuneId",
      source,
      enabled,
      config_override AS "configOverride",
      promotion_gate AS "promotionGate",
      in_universe AS "inUniverse",
      worker_dirty AS "workerDirty",
      promotion_dirty AS "promotionDirty",
      retired_at AS "retiredAt",
      last_prepared_at AS "lastPreparedAt",
      created_at AS "createdAt"
    FROM scalp_deployments
    WHERE deployment_id = ${deploymentId}
    LIMIT 1;
  `);
  return rows[0] || null;
}

async function migrateOne(params: {
  tx: Prisma.TransactionClient;
  oldId: string;
  newId: string;
  actor: string;
}): Promise<{
  moved: boolean;
  oldId: string;
  newId: string;
  childUpdates: {
    weeklyMetrics: number;
    sessions: number;
    executionRuns: number;
    journal: number;
    tradeLedger: number;
  };
}> {
  const { tx, oldId, newId, actor } = params;
  if (oldId === newId) {
    return {
      moved: false,
      oldId,
      newId,
      childUpdates: {
        weeklyMetrics: 0,
        sessions: 0,
        executionRuns: 0,
        journal: 0,
        tradeLedger: 0,
      },
    };
  }

  const oldRow = await loadDeploymentRow(tx, oldId);
  if (!oldRow) {
    return {
      moved: false,
      oldId,
      newId,
      childUpdates: {
        weeklyMetrics: 0,
        sessions: 0,
        executionRuns: 0,
        journal: 0,
        tradeLedger: 0,
      },
    };
  }

  const newRow = await loadDeploymentRow(tx, newId);

  if (!newRow) {
    const tempTune = `${oldRow.tuneId}__mig_${Date.now().toString(36).slice(-6)}`.slice(0, 80);
    await tx.$executeRaw(Prisma.sql`
      UPDATE scalp_deployments
      SET
        tune_id = ${tempTune},
        updated_by = ${actor},
        updated_at = NOW()
      WHERE deployment_id = ${oldId};
    `);

    await tx.$executeRaw(Prisma.sql`
      INSERT INTO scalp_deployments(
        deployment_id,
        symbol,
        strategy_id,
        tune_id,
        source,
        enabled,
        config_override,
        promotion_gate,
        in_universe,
        worker_dirty,
        promotion_dirty,
        retired_at,
        last_prepared_at,
        updated_by,
        created_at,
        updated_at
      )
      VALUES(
        ${newId},
        ${oldRow.symbol},
        ${oldRow.strategyId},
        ${oldRow.tuneId},
        ${oldRow.source},
        ${oldRow.enabled},
        ${JSON.stringify(oldRow.configOverride || {})}::jsonb,
        ${oldRow.promotionGate ? JSON.stringify(oldRow.promotionGate) : null}::jsonb,
        ${oldRow.inUniverse},
        ${oldRow.workerDirty},
        ${oldRow.promotionDirty},
        ${oldRow.retiredAt},
        ${oldRow.lastPreparedAt},
        ${actor},
        ${oldRow.createdAt},
        NOW()
      );
    `);
  }

  const weeklyMetrics = Number(
    await tx.$executeRaw(Prisma.sql`
      UPDATE scalp_deployment_weekly_metrics
      SET deployment_id = ${newId}, updated_at = NOW()
      WHERE deployment_id = ${oldId};
    `),
  );

  const sessions = Number(
    await tx.$executeRaw(Prisma.sql`
      UPDATE scalp_sessions
      SET deployment_id = ${newId}, updated_at = NOW()
      WHERE deployment_id = ${oldId};
    `),
  );

  const executionRuns = Number(
    await tx.$executeRaw(Prisma.sql`
      UPDATE scalp_execution_runs
      SET deployment_id = ${newId}
      WHERE deployment_id = ${oldId};
    `),
  );

  const journal = Number(
    await tx.$executeRaw(Prisma.sql`
      UPDATE scalp_journal
      SET deployment_id = ${newId}
      WHERE deployment_id = ${oldId};
    `),
  );

  const tradeLedger = Number(
    await tx.$executeRaw(Prisma.sql`
      UPDATE scalp_trade_ledger
      SET deployment_id = ${newId}
      WHERE deployment_id = ${oldId};
    `),
  );

  await tx.$executeRaw(Prisma.sql`
    DELETE FROM scalp_deployments
    WHERE deployment_id = ${oldId};
  `);

  return {
    moved: true,
    oldId,
    newId,
    childUpdates: {
      weeklyMetrics,
      sessions,
      executionRuns,
      journal,
      tradeLedger,
    },
  };
}

async function syncSymbolMetadataVenue(params: {
  tx: Prisma.TransactionClient;
  symbol: string;
  targetSource: ScalpVenue;
  historyEpic: string | null;
}): Promise<number> {
  const epic =
    params.targetSource === "capital"
      ? params.historyEpic
      : params.symbol;
  return Number(
    await params.tx.$executeRaw(Prisma.sql`
      UPDATE scalp_symbol_market_metadata
      SET
        source = ${params.targetSource},
        epic = COALESCE(${epic}, epic),
        updated_at = NOW()
      WHERE symbol = ${params.symbol};
    `),
  );
}

async function main() {
  const { apply, onlySymbol } = parseArgs(process.argv.slice(2));
  const candidates = await loadCandidates({ onlySymbol });

  const plan = candidates.map((row) => ({
    ...row,
    targetDeploymentId: toTargetDeploymentId(row.deploymentId, row.targetSource),
  }));

  if (!apply) {
    console.log(
      JSON.stringify(
        {
          mode: 'dry-run',
          onlySymbol: onlySymbol || null,
          candidateCount: plan.length,
          plan,
        },
        null,
        2,
      ),
    );
    return;
  }

  const db = scalpPrisma();
  const actor = 'script:realign-deployment-venues';
  const moved: Array<{
    oldId: string;
    newId: string;
    symbol: string;
    childUpdates: {
      weeklyMetrics: number;
      sessions: number;
      executionRuns: number;
      journal: number;
      tradeLedger: number;
    };
  }> = [];

  for (const row of plan) {
    const result = await db.$transaction(async (tx) =>
      migrateOne({
        tx,
        oldId: row.deploymentId,
        newId: row.targetDeploymentId,
        actor,
      }),
    );

    if (result.moved) {
      await db.$transaction(async (tx) => {
        await syncSymbolMetadataVenue({
          tx,
          symbol: row.symbol,
          targetSource: row.targetSource,
          historyEpic: row.historyEpic,
        });
      });
      moved.push({
        oldId: result.oldId,
        newId: result.newId,
        symbol: row.symbol,
        childUpdates: result.childUpdates,
      });
    }
  }

  const postRemaining = await loadCandidates({ onlySymbol });

  console.log(
    JSON.stringify(
      {
        mode: 'apply',
        onlySymbol: onlySymbol || null,
        requested: plan.length,
        moved: moved.length,
        postRemaining: postRemaining.length,
        movedRows: moved,
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
