import { Prisma } from '@prisma/client';

import {
    buildScalpDeploymentId,
    normalizeScalpTuneId,
    parseScalpDeploymentId,
} from '../lib/scalp/deployments';
import { isScalpPgConfigured, scalpPrisma } from '../lib/scalp/pg/client';

type CliOptions = {
    dryRun: boolean;
    verbose: boolean;
};

type DeploymentRow = {
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
};

type RenameMapping = {
    oldDeploymentId: string;
    newDeploymentId: string;
    oldTuneId: string;
    newTuneId: string;
    symbol: string;
    strategyId: string;
};

type TableColumnRow = {
    tableName: string;
    columnName: string;
};

type TableSummary = {
    tableName: string;
    hasDeploymentId: boolean;
    hasTuneId: boolean;
};

type TableImpact = {
    tableName: string;
    deploymentIdRows: number;
    tuneIdRows: number;
};

type ApplyImpact = {
    tableName: string;
    deploymentIdUpdated: number;
    tuneIdUpdated: number;
};

const MAP_TEMP_TABLE = 'tmp_session_tune_suffix_rename_map';

function parseCliOptions(argv: string[]): CliOptions {
    let dryRun = true;
    let verbose = false;
    for (const arg of argv) {
        const normalized = String(arg || '').trim().toLowerCase();
        if (!normalized) continue;
        if (normalized === '--apply') dryRun = false;
        if (normalized === '--dry-run') dryRun = true;
        if (normalized === '--verbose') verbose = true;
    }
    return { dryRun, verbose };
}

function isLegacySessionTuneId(value: unknown): boolean {
    const tuneId = String(value || '')
        .trim()
        .toLowerCase();
    if (!tuneId.startsWith('auto_sp')) return false;
    if (tuneId.startsWith('auto_sp_')) return false;
    return tuneId.length > 'auto_sp'.length;
}

function toSessionSuffixTuneId(oldTuneId: string): string {
    const suffix = oldTuneId.slice('auto_sp'.length);
    return normalizeScalpTuneId(`auto_sp_${suffix}`, oldTuneId);
}

function sanitizeTableName(name: string): string {
    if (!/^[a-z_][a-z0-9_]*$/i.test(name)) {
        throw new Error(`invalid_table_name:${name}`);
    }
    return name;
}

function quoteIdent(name: string): string {
    const sanitized = sanitizeTableName(name);
    return `"${sanitized.replace(/"/g, '""')}"`;
}

async function loadLegacySessionDeploymentRows(): Promise<DeploymentRow[]> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<DeploymentRow>>(Prisma.sql`
        SELECT
            deployment_id AS "deploymentId",
            symbol,
            strategy_id AS "strategyId",
            tune_id AS "tuneId"
        FROM scalp_deployments
        WHERE tune_id LIKE 'auto_sp%'
          AND tune_id NOT LIKE 'auto_sp\_%' ESCAPE '\'
        ORDER BY symbol ASC, strategy_id ASC, tune_id ASC;
    `);
    return rows;
}

function buildMappings(rows: DeploymentRow[]): RenameMapping[] {
    const out: RenameMapping[] = [];
    for (const row of rows) {
        const oldDeploymentId = String(row.deploymentId || '').trim();
        const oldTuneId = String(row.tuneId || '')
            .trim()
            .toLowerCase();
        const symbol = String(row.symbol || '')
            .trim()
            .toUpperCase();
        const strategyId = String(row.strategyId || '')
            .trim()
            .toLowerCase();
        if (!oldDeploymentId || !symbol || !strategyId || !isLegacySessionTuneId(oldTuneId)) continue;
        const parsed = parseScalpDeploymentId(oldDeploymentId);
        if (!parsed) continue;
        const newTuneId = toSessionSuffixTuneId(oldTuneId);
        const newDeploymentId = buildScalpDeploymentId({
            venue: parsed.venue,
            symbol,
            strategyId,
            tuneId: newTuneId,
        });
        if (!newDeploymentId || newDeploymentId === oldDeploymentId) continue;
        out.push({
            oldDeploymentId,
            newDeploymentId,
            oldTuneId,
            newTuneId,
            symbol,
            strategyId,
        });
    }
    return out;
}

async function loadScalpTableSummaries(): Promise<TableSummary[]> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<TableColumnRow>>(Prisma.sql`
        SELECT
            table_name AS "tableName",
            column_name AS "columnName"
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name LIKE 'scalp_%'
          AND column_name IN ('deployment_id', 'tune_id')
        ORDER BY table_name ASC, column_name ASC;
    `);
    const byTable = new Map<string, Set<string>>();
    for (const row of rows) {
        const tableName = String(row.tableName || '').trim();
        const columnName = String(row.columnName || '').trim();
        if (!tableName || !columnName) continue;
        const set = byTable.get(tableName) || new Set<string>();
        set.add(columnName);
        byTable.set(tableName, set);
    }
    return Array.from(byTable.entries())
        .map(([tableName, set]) => ({
            tableName,
            hasDeploymentId: set.has('deployment_id'),
            hasTuneId: set.has('tune_id'),
        }))
        .sort((a, b) => a.tableName.localeCompare(b.tableName));
}

async function loadTableImpacts(
    tables: TableSummary[],
    mappings: RenameMapping[],
): Promise<TableImpact[]> {
    const db = scalpPrisma();
    const oldDeploymentIds = Array.from(new Set(mappings.map((row) => row.oldDeploymentId)));
    const oldTuneIds = Array.from(new Set(mappings.map((row) => row.oldTuneId)));
    const out: TableImpact[] = [];

    for (const table of tables) {
        const tableIdent = quoteIdent(table.tableName);
        let deploymentIdRows = 0;
        let tuneIdRows = 0;

        if (table.hasDeploymentId && oldDeploymentIds.length > 0) {
            const [row] = await db.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
                SELECT COUNT(*)::bigint AS count
                FROM ${Prisma.raw(tableIdent)}
                WHERE deployment_id IN (${Prisma.join(oldDeploymentIds)});
            `);
            deploymentIdRows = Number(row?.count || 0);
        }

        if (table.hasTuneId && oldTuneIds.length > 0) {
            const [row] = await db.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
                SELECT COUNT(*)::bigint AS count
                FROM ${Prisma.raw(tableIdent)}
                WHERE tune_id IN (${Prisma.join(oldTuneIds)});
            `);
            tuneIdRows = Number(row?.count || 0);
        }

        if (deploymentIdRows > 0 || tuneIdRows > 0) {
            out.push({
                tableName: table.tableName,
                deploymentIdRows,
                tuneIdRows,
            });
        }
    }

    return out.sort((a, b) => a.tableName.localeCompare(b.tableName));
}

async function loadConflicts(mappings: RenameMapping[]): Promise<{
    deploymentIdConflicts: string[];
    tripletConflicts: Array<{ symbol: string; strategyId: string; tuneId: string; deploymentId: string }>;
}> {
    const db = scalpPrisma();
    const newDeploymentIds = Array.from(new Set(mappings.map((row) => row.newDeploymentId)));
    const oldDeploymentIds = new Set(mappings.map((row) => row.oldDeploymentId));
    const newTriplets = mappings.map((row) => [row.symbol, row.strategyId, row.newTuneId] as const);

    const deploymentIdConflicts: string[] = [];
    if (newDeploymentIds.length > 0) {
        const rows = await db.$queryRaw<Array<{ deploymentId: string }>>(Prisma.sql`
            SELECT deployment_id AS "deploymentId"
            FROM scalp_deployments
            WHERE deployment_id IN (${Prisma.join(newDeploymentIds)});
        `);
        for (const row of rows) {
            const id = String(row.deploymentId || '').trim();
            if (!id) continue;
            if (!oldDeploymentIds.has(id)) deploymentIdConflicts.push(id);
        }
    }

    const tripletConflicts: Array<{
        symbol: string;
        strategyId: string;
        tuneId: string;
        deploymentId: string;
    }> = [];
    if (newTriplets.length > 0) {
        const whereRows = Prisma.join(
            newTriplets.map(
                ([symbol, strategyId, tuneId]) =>
                    Prisma.sql`(symbol = ${symbol} AND strategy_id = ${strategyId} AND tune_id = ${tuneId})`,
            ),
            ' OR ',
        );
        const rows = await db.$queryRaw<
            Array<{ symbol: string; strategyId: string; tuneId: string; deploymentId: string }>
        >(Prisma.sql`
            SELECT
                symbol,
                strategy_id AS "strategyId",
                tune_id AS "tuneId",
                deployment_id AS "deploymentId"
            FROM scalp_deployments
            WHERE ${whereRows};
        `);
        for (const row of rows) {
            const id = String(row.deploymentId || '').trim();
            if (!id || oldDeploymentIds.has(id)) continue;
            tripletConflicts.push(row);
        }
    }

    return {
        deploymentIdConflicts: Array.from(new Set(deploymentIdConflicts)).sort(),
        tripletConflicts,
    };
}

async function applyRename(
    mappings: RenameMapping[],
    tables: TableSummary[],
): Promise<{
    insertedDeployments: number;
    deletedDeployments: number;
    tableUpdates: ApplyImpact[];
}> {
    const db = scalpPrisma();
    const targetTables = tables.filter((row) => row.tableName !== 'scalp_deployments');

    return db.$transaction(async (tx) => {
        await tx.$executeRawUnsafe(`
            CREATE TEMP TABLE ${MAP_TEMP_TABLE}(
                old_deployment_id text PRIMARY KEY,
                new_deployment_id text UNIQUE NOT NULL,
                old_tune_id text NOT NULL,
                new_tune_id text NOT NULL
            ) ON COMMIT DROP;
        `);

        await tx.$executeRaw(
            Prisma.sql`
                INSERT INTO ${Prisma.raw(MAP_TEMP_TABLE)}(
                    old_deployment_id,
                    new_deployment_id,
                    old_tune_id,
                    new_tune_id
                )
                VALUES ${Prisma.join(
                    mappings.map(
                        (row) =>
                            Prisma.sql`(${row.oldDeploymentId}, ${row.newDeploymentId}, ${row.oldTuneId}, ${row.newTuneId})`,
                    ),
                )};
            `,
        );

        const [insertRow] = await tx.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
            WITH inserted AS (
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
                SELECT
                    m.new_deployment_id,
                    d.symbol,
                    d.strategy_id,
                    m.new_tune_id,
                    d.source,
                    d.enabled,
                    d.config_override,
                    d.promotion_gate,
                    d.in_universe,
                    d.worker_dirty,
                    d.promotion_dirty,
                    d.retired_at,
                    d.last_prepared_at,
                    COALESCE(d.updated_by, 'session_tune_suffix_rename'),
                    d.created_at,
                    NOW()
                FROM scalp_deployments d
                JOIN ${Prisma.raw(MAP_TEMP_TABLE)} m
                  ON m.old_deployment_id = d.deployment_id
                RETURNING 1
            )
            SELECT COUNT(*)::bigint AS count FROM inserted;
        `);

        const tableUpdates: ApplyImpact[] = [];
        for (const table of targetTables) {
            const tableIdent = quoteIdent(table.tableName);
            let deploymentIdUpdated = 0;
            let tuneIdUpdated = 0;

            if (table.hasDeploymentId) {
                const [row] = await tx.$queryRawUnsafe<Array<{ count: bigint }>>(`
                    WITH updated AS (
                        UPDATE ${tableIdent} t
                        SET deployment_id = m.new_deployment_id
                        FROM ${MAP_TEMP_TABLE} m
                        WHERE t.deployment_id = m.old_deployment_id
                        RETURNING 1
                    )
                    SELECT COUNT(*)::bigint AS count FROM updated;
                `);
                deploymentIdUpdated = Number(row?.count || 0);
            }

            if (table.hasTuneId) {
                const [row] = await tx.$queryRawUnsafe<Array<{ count: bigint }>>(`
                    WITH updated AS (
                        UPDATE ${tableIdent} t
                        SET tune_id = m.new_tune_id
                        FROM ${MAP_TEMP_TABLE} m
                        WHERE t.tune_id = m.old_tune_id
                        RETURNING 1
                    )
                    SELECT COUNT(*)::bigint AS count FROM updated;
                `);
                tuneIdUpdated = Number(row?.count || 0);
            }

            if (deploymentIdUpdated > 0 || tuneIdUpdated > 0) {
                tableUpdates.push({
                    tableName: table.tableName,
                    deploymentIdUpdated,
                    tuneIdUpdated,
                });
            }
        }

        const [deleteRow] = await tx.$queryRaw<Array<{ count: bigint }>>(Prisma.sql`
            WITH deleted AS (
                DELETE FROM scalp_deployments d
                USING ${Prisma.raw(MAP_TEMP_TABLE)} m
                WHERE d.deployment_id = m.old_deployment_id
                RETURNING 1
            )
            SELECT COUNT(*)::bigint AS count FROM deleted;
        `);

        return {
            insertedDeployments: Number(insertRow?.count || 0),
            deletedDeployments: Number(deleteRow?.count || 0),
            tableUpdates: tableUpdates.sort((a, b) => a.tableName.localeCompare(b.tableName)),
        };
    });
}

async function main(): Promise<void> {
    const options = parseCliOptions(process.argv.slice(2));
    if (!isScalpPgConfigured()) {
        throw new Error('scalp_pg_not_configured');
    }

    const rows = await loadLegacySessionDeploymentRows();
    const mappings = buildMappings(rows);

    const oldIds = new Set<string>();
    const newIds = new Set<string>();
    for (const row of mappings) {
        if (oldIds.has(row.oldDeploymentId)) {
            throw new Error(`duplicate_old_deployment_id:${row.oldDeploymentId}`);
        }
        if (newIds.has(row.newDeploymentId)) {
            throw new Error(`duplicate_new_deployment_id:${row.newDeploymentId}`);
        }
        oldIds.add(row.oldDeploymentId);
        newIds.add(row.newDeploymentId);
    }

    const tables = await loadScalpTableSummaries();
    const impacts = await loadTableImpacts(tables, mappings);
    const conflicts = await loadConflicts(mappings);

    const baseSummary = {
        dryRun: options.dryRun,
        candidates: mappings.length,
        impacts,
        conflictCounts: {
            deploymentIdConflicts: conflicts.deploymentIdConflicts.length,
            tripletConflicts: conflicts.tripletConflicts.length,
        },
    };

    if (options.verbose) {
        console.log(
            JSON.stringify(
                {
                    ...baseSummary,
                    mappings,
                    deploymentIdConflicts: conflicts.deploymentIdConflicts,
                    tripletConflicts: conflicts.tripletConflicts,
                },
                null,
                2,
            ),
        );
    } else {
        console.log(JSON.stringify(baseSummary, null, 2));
    }

    if (mappings.length === 0) {
        console.log(
            JSON.stringify(
                {
                    ok: true,
                    skipped: true,
                    reason: 'no_legacy_session_tune_rows_found',
                },
                null,
                2,
            ),
        );
        return;
    }

    if (
        conflicts.deploymentIdConflicts.length > 0 ||
        conflicts.tripletConflicts.length > 0
    ) {
        throw new Error('session_tune_suffix_rename_conflicts_detected');
    }

    if (options.dryRun) {
        console.log(
            JSON.stringify(
                {
                    ok: true,
                    applied: false,
                    nextStep: 'Re-run with --apply to execute transaction.',
                },
                null,
                2,
            ),
        );
        return;
    }

    const applied = await applyRename(mappings, tables);
    console.log(
        JSON.stringify(
            {
                ok: true,
                applied: true,
                ...applied,
            },
            null,
            2,
        ),
    );
}

main().catch((err) => {
    console.error(
        JSON.stringify(
            {
                ok: false,
                error: String((err as Error)?.message || err || 'unknown_error'),
            },
            null,
            2,
        ),
    );
    process.exitCode = 1;
});
