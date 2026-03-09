import { mkdir, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { scalpDeploymentRegistryKvKey, scalpDeploymentRegistryPath } from './deploymentRegistry';

const upstash_payasyougo_KV_REST_API_URL = (process.env.upstash_payasyougo_KV_REST_API_URL || '').replace(/\/$/, '');
const upstash_payasyougo_KV_REST_API_TOKEN = process.env.upstash_payasyougo_KV_REST_API_TOKEN || '';

const STATE_V2_KEY_PREFIX = 'scalp:state:v2';
const STATE_V1_KEY_PREFIX = 'scalp:state:v1';
const RUN_LOCK_KEY_PREFIX = 'scalp:runlock:v2';
const RESEARCH_CYCLE_KEY_PREFIX = 'scalp:research:cycle:v1';
const RESEARCH_TASK_KEY_PREFIX = 'scalp:research:task:v1';
const RESEARCH_AGG_KEY_PREFIX = 'scalp:research:aggregate:v1';
const RESEARCH_CLAIM_CURSOR_KEY_PREFIX = 'scalp:research:claim-cursor:v1';
const RESEARCH_LOCK_KEY_PREFIX = 'scalp:research:lock:v1';
const CANDLE_HISTORY_KEY_PREFIX = 'scalp:candles-history:v1';

const RUNTIME_SETTINGS_KEY = 'scalp:runtime:settings:v1';
const RESEARCH_ACTIVE_CYCLE_KEY = 'scalp:research:active-cycle:v1';
const JOURNAL_LIST_KEY = 'scalp:journal:list:v1';
const TRADE_LEDGER_LIST_KEY = 'scalp:trade-ledger:list:v1';
const UNIVERSE_KV_KEY = 'scalp:symbol-universe:v1';
const REPORT_KV_KEY = 'scalp:research:portfolio-report:v1';
const PROMOTION_SYNC_STATE_KEY = 'scalp:research:promotion-sync:last:v1';

const DEFAULT_UNIVERSE_FILE_PATH = path.resolve(process.cwd(), 'data/scalp-symbol-universe.json');
const DEFAULT_REPORT_FILE_PATH = path.resolve(process.cwd(), 'data/scalp-research-report.json');
const DEFAULT_CANDLE_HISTORY_DIR = path.resolve(process.cwd(), 'data/candles-history');
const EMPTY_DEPLOYMENTS_SNAPSHOT = {
    version: 1,
    updatedAt: null,
    deployments: [],
};

function hasKvConfig(): boolean {
    return Boolean(upstash_payasyougo_KV_REST_API_URL && upstash_payasyougo_KV_REST_API_TOKEN);
}

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

async function kvRawCommand(command: string, ...args: Array<string | number>): Promise<unknown> {
    if (!hasKvConfig()) return null;
    const encodedArgs = args
        .map((arg) => encodeURIComponent(typeof arg === 'string' ? arg : String(arg)))
        .join('/');
    const url = `${upstash_payasyougo_KV_REST_API_URL}/${command}${encodedArgs ? `/${encodedArgs}` : ''}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${upstash_payasyougo_KV_REST_API_TOKEN}`,
        },
    });
    const text = await res.text();
    const data = (() => {
        try {
            return text ? JSON.parse(text) : null;
        } catch {
            return null;
        }
    })();
    if (!res.ok) {
        const message =
            (data && typeof data === 'object' && (data.error || data.message)) ||
            text ||
            `KV command failed (${command}) HTTP ${res.status}`;
        throw new Error(String(message));
    }
    if (data && typeof data === 'object' && data.error) {
        throw new Error(String(data.error || data.message || `KV command failed: ${command}`));
    }
    return data && typeof data === 'object' ? data.result : null;
}

async function scanKeysByPrefix(prefix: string, maxKeys: number): Promise<string[]> {
    if (!hasKvConfig()) return [];
    let cursor = '0';
    const out = new Set<string>();
    const hardCap = Math.max(1, Math.min(200_000, Math.floor(maxKeys)));
    for (let i = 0; i < 400; i += 1) {
        const raw = await kvRawCommand('SCAN', cursor, 'MATCH', `${prefix}*`, 'COUNT', 500);
        if (!Array.isArray(raw) || raw.length < 2) break;
        cursor = String(raw[0] || '0');
        const keysRaw = Array.isArray(raw[1]) ? raw[1] : [];
        for (const row of keysRaw) {
            const key = String(row || '').trim();
            if (!key) continue;
            out.add(key);
            if (out.size >= hardCap) return Array.from(out);
        }
        if (cursor === '0') break;
    }
    return Array.from(out);
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

export async function runScalpFullReset(
    params: RunScalpFullResetParams = {},
): Promise<RunScalpFullResetResult> {
    const nowMs = Date.now();
    const dryRun = Boolean(params.dryRun);
    const includeCandleHistory = Boolean(params.includeCandleHistory);
    const includeRuntimeSettings = params.includeRuntimeSettings !== false;
    const maxScanKeys = toPositiveInt(params.maxScanKeys, 20_000);
    const kvEnabled = hasKvConfig();

    const exactKeys: string[] = [
        scalpDeploymentRegistryKvKey(),
        RESEARCH_ACTIVE_CYCLE_KEY,
        JOURNAL_LIST_KEY,
        TRADE_LEDGER_LIST_KEY,
        UNIVERSE_KV_KEY,
        REPORT_KV_KEY,
        PROMOTION_SYNC_STATE_KEY,
    ];
    if (includeRuntimeSettings) {
        exactKeys.push(RUNTIME_SETTINGS_KEY);
    }

    const prefixes: string[] = [
        `${STATE_V2_KEY_PREFIX}:`,
        `${STATE_V1_KEY_PREFIX}:`,
        `${RUN_LOCK_KEY_PREFIX}:`,
        `${RESEARCH_CYCLE_KEY_PREFIX}:`,
        `${RESEARCH_TASK_KEY_PREFIX}:`,
        `${RESEARCH_AGG_KEY_PREFIX}:`,
        `${RESEARCH_CLAIM_CURSOR_KEY_PREFIX}:`,
        `${RESEARCH_LOCK_KEY_PREFIX}:`,
    ];
    if (includeCandleHistory) {
        prefixes.push(`${CANDLE_HISTORY_KEY_PREFIX}:`);
    }

    const exactKeyRows: ScalpFullResetExactKeyRow[] = [];
    const prefixRows: ScalpFullResetPrefixRow[] = [];
    const fileRows: ScalpFullResetFileRow[] = [];
    let errors = 0;

    if (kvEnabled) {
        for (const key of exactKeys) {
            let existed: boolean | null = null;
            let deleted = false;
            let error: string | null = null;
            try {
                if (dryRun) {
                    const existsRaw = await kvRawCommand('EXISTS', key);
                    existed = Number(existsRaw || 0) > 0;
                } else {
                    const delRaw = await kvRawCommand('DEL', key);
                    deleted = Number(delRaw || 0) > 0;
                }
            } catch (err: any) {
                error = err?.message || String(err);
                errors += 1;
            }
            exactKeyRows.push({
                key,
                existed,
                deleted,
                error,
            });
        }

        for (const prefix of prefixes) {
            let matchedKeys = 0;
            let deletedKeys = 0;
            let error: string | null = null;
            try {
                const keys = await scanKeysByPrefix(prefix, maxScanKeys);
                matchedKeys = keys.length;
                if (!dryRun) {
                    for (const key of keys) {
                        const delRaw = await kvRawCommand('DEL', key);
                        if (Number(delRaw || 0) > 0) deletedKeys += 1;
                    }
                }
            } catch (err: any) {
                error = err?.message || String(err);
                errors += 1;
            }
            prefixRows.push({
                prefix,
                matchedKeys,
                deletedKeys,
                error,
            });
        }
    } else {
        for (const key of exactKeys) {
            exactKeyRows.push({
                key,
                existed: null,
                deleted: false,
                error: 'kv_not_configured',
            });
        }
        for (const prefix of prefixes) {
            prefixRows.push({
                prefix,
                matchedKeys: 0,
                deletedKeys: 0,
                error: 'kv_not_configured',
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
            kvEnabled,
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
