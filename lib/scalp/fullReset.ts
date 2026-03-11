import { mkdir, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { scalpDeploymentRegistryPath } from './deploymentRegistry';
import { isScalpPgConfigured, scalpPrisma } from './pg/client';

const DEFAULT_UNIVERSE_FILE_PATH = path.resolve(process.cwd(), 'data/scalp-symbol-universe.json');
const DEFAULT_REPORT_FILE_PATH = path.resolve(process.cwd(), 'data/scalp-research-report.json');
const DEFAULT_CANDLE_HISTORY_DIR = path.resolve(process.cwd(), 'data/candles-history');
const EMPTY_DEPLOYMENTS_SNAPSHOT = {
    version: 1,
    updatedAt: null,
    deployments: [],
};

const BASE_RESET_TABLES = [
    'scalp_shadow_job_runs',
    'scalp_jobs',
    'scalp_symbol_cooldowns',
    'scalp_research_attempts',
    'scalp_research_tasks',
    'scalp_research_cycles',
    'scalp_execution_runs',
    'scalp_sessions',
    'scalp_journal',
    'scalp_trade_ledger',
    'scalp_deployments',
] as const;

const RUNTIME_RESET_TABLES = ['scalp_strategy_overrides', 'scalp_runtime_settings'] as const;

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function resolvePath(envKey: string, fallbackAbs: string): string {
    const configured = String(process.env[envKey] || '').trim();
    if (!configured) return fallbackAbs;
    return path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
}

async function pathExists(targetPath: string): Promise<boolean> {
    try {
        await stat(targetPath);
        return true;
    } catch {
        return false;
    }
}

export interface ScalpFullResetPrefixRow {
    prefix: string;
    matchedKeys: number;
    deletedKeys: number;
    error: string | null;
}

export interface ScalpFullResetExactKeyRow {
    key: string;
    existed: boolean | null;
    deleted: boolean;
    error: string | null;
}

export interface ScalpFullResetFileRow {
    path: string;
    action: 'write-empty-registry' | 'delete-file' | 'delete-directory';
    existed: boolean;
    changed: boolean;
    error: string | null;
}

export interface RunScalpFullResetParams {
    dryRun?: boolean;
    includeCandleHistory?: boolean;
    includeRuntimeSettings?: boolean;
    maxScanKeys?: number;
}

export interface RunScalpFullResetResult {
    ok: boolean;
    dryRun: boolean;
    generatedAtMs: number;
    generatedAtIso: string;
    config: {
        includeCandleHistory: boolean;
        includeRuntimeSettings: boolean;
        maxScanKeys: number;
        kvEnabled: boolean;
        pgEnabled: boolean;
    };
    summary: {
        exactKeysTargeted: number;
        exactKeysDeleted: number;
        prefixesScanned: number;
        prefixMatchedKeys: number;
        prefixDeletedKeys: number;
        filesTargeted: number;
        filesChanged: number;
        errors: number;
    };
    details: {
        exactKeys: ScalpFullResetExactKeyRow[];
        prefixes: ScalpFullResetPrefixRow[];
        files: ScalpFullResetFileRow[];
    };
}

function tableList(includeRuntimeSettings: boolean): string[] {
    return includeRuntimeSettings
        ? [...BASE_RESET_TABLES, ...RUNTIME_RESET_TABLES]
        : [...BASE_RESET_TABLES];
}

function assertResetTableName(tableName: string): void {
    const allowed = new Set([...BASE_RESET_TABLES, ...RUNTIME_RESET_TABLES]);
    if (!allowed.has(tableName as any)) {
        throw new Error(`Invalid reset table: ${tableName}`);
    }
}

async function countTableRows(tableName: string): Promise<number> {
    assertResetTableName(tableName);
    const db = scalpPrisma();
    const rows = await db.$queryRawUnsafe<Array<{ count: bigint | number }>>(
        `SELECT COUNT(*)::bigint AS count FROM ${tableName};`,
    );
    return Number(rows[0]?.count || 0);
}

async function truncateTables(tables: string[]): Promise<void> {
    for (const tableName of tables) {
        assertResetTableName(tableName);
    }
    const db = scalpPrisma();
    await db.$executeRawUnsafe(`TRUNCATE TABLE ${tables.join(', ')} RESTART IDENTITY CASCADE;`);
}

export async function runScalpFullReset(
    params: RunScalpFullResetParams = {},
): Promise<RunScalpFullResetResult> {
    const nowMs = Date.now();
    const dryRun = Boolean(params.dryRun);
    const includeCandleHistory = Boolean(params.includeCandleHistory);
    const includeRuntimeSettings = params.includeRuntimeSettings !== false;
    const maxScanKeys = toPositiveInt(params.maxScanKeys, 20_000);
    const pgEnabled = isScalpPgConfigured();

    const exactKeyRows: ScalpFullResetExactKeyRow[] = [];
    const prefixRows: ScalpFullResetPrefixRow[] = [];
    const fileRows: ScalpFullResetFileRow[] = [];
    let errors = 0;

    const tables = tableList(includeRuntimeSettings);
    if (pgEnabled) {
        for (const tableName of tables) {
            try {
                const count = await countTableRows(tableName);
                prefixRows.push({
                    prefix: `pg:${tableName}`,
                    matchedKeys: count,
                    deletedKeys: dryRun ? 0 : count,
                    error: null,
                });
            } catch (err: any) {
                errors += 1;
                prefixRows.push({
                    prefix: `pg:${tableName}`,
                    matchedKeys: 0,
                    deletedKeys: 0,
                    error: String(err?.message || err || 'count_failed'),
                });
            }
        }

        if (!dryRun) {
            try {
                await truncateTables(tables);
            } catch (err: any) {
                errors += 1;
                prefixRows.push({
                    prefix: 'pg:truncate',
                    matchedKeys: 0,
                    deletedKeys: 0,
                    error: String(err?.message || err || 'truncate_failed'),
                });
            }
        }
    } else {
        for (const tableName of tables) {
            prefixRows.push({
                prefix: `pg:${tableName}`,
                matchedKeys: 0,
                deletedKeys: 0,
                error: 'pg_not_configured',
            });
        }
    }

    const deploymentRegistryFile = scalpDeploymentRegistryPath();
    const universeFile = resolvePath('SCALP_SYMBOL_UNIVERSE_PATH', DEFAULT_UNIVERSE_FILE_PATH);
    const reportFile = resolvePath('SCALP_RESEARCH_REPORT_PATH', DEFAULT_REPORT_FILE_PATH);
    const candleHistoryDir = resolvePath('CANDLE_HISTORY_DIR', DEFAULT_CANDLE_HISTORY_DIR);

    {
        const existed = await pathExists(deploymentRegistryFile);
        let changed = false;
        let error: string | null = null;
        if (!dryRun) {
            try {
                await mkdir(path.dirname(deploymentRegistryFile), { recursive: true });
                await writeFile(deploymentRegistryFile, `${JSON.stringify(EMPTY_DEPLOYMENTS_SNAPSHOT, null, 2)}\n`, 'utf8');
                changed = true;
            } catch (err: any) {
                error = err?.message || String(err);
                errors += 1;
            }
        }
        fileRows.push({
            path: deploymentRegistryFile,
            action: 'write-empty-registry',
            existed,
            changed,
            error,
        });
    }

    for (const targetFile of [universeFile, reportFile]) {
        const existed = await pathExists(targetFile);
        let changed = false;
        let error: string | null = null;
        if (!dryRun && existed) {
            try {
                await rm(targetFile, { force: true });
                changed = true;
            } catch (err: any) {
                error = err?.message || String(err);
                errors += 1;
            }
        }
        fileRows.push({
            path: targetFile,
            action: 'delete-file',
            existed,
            changed,
            error,
        });
    }

    if (includeCandleHistory) {
        const existed = await pathExists(candleHistoryDir);
        let changed = false;
        let error: string | null = null;
        if (!dryRun && existed) {
            try {
                await rm(candleHistoryDir, { recursive: true, force: true });
                changed = true;
            } catch (err: any) {
                error = err?.message || String(err);
                errors += 1;
            }
        }
        fileRows.push({
            path: candleHistoryDir,
            action: 'delete-directory',
            existed,
            changed,
            error,
        });
    }

    const exactKeysDeleted = exactKeyRows.filter((row) => row.deleted).length;
    const prefixMatchedKeys = prefixRows.reduce((acc, row) => acc + row.matchedKeys, 0);
    const prefixDeletedKeys = prefixRows.reduce((acc, row) => acc + row.deletedKeys, 0);
    const filesChanged = fileRows.filter((row) => row.changed).length;

    return {
        ok: errors === 0,
        dryRun,
        generatedAtMs: nowMs,
        generatedAtIso: new Date(nowMs).toISOString(),
        config: {
            includeCandleHistory,
            includeRuntimeSettings,
            maxScanKeys,
            kvEnabled: false,
            pgEnabled,
        },
        summary: {
            exactKeysTargeted: exactKeyRows.length,
            exactKeysDeleted,
            prefixesScanned: prefixRows.length,
            prefixMatchedKeys,
            prefixDeletedKeys,
            filesTargeted: fileRows.length,
            filesChanged,
            errors,
        },
        details: {
            exactKeys: exactKeyRows,
            prefixes: prefixRows,
            files: fileRows,
        },
    };
}
