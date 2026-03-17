import { Prisma } from '@prisma/client';

import { bitgetFetch, resolveProductType } from '../lib/bitget';
import { buildScalpDeploymentId, parseScalpDeploymentId } from '../lib/scalp/deployments';
import { scalpPrisma } from '../lib/scalp/pg/client';
import type { ScalpCandle } from '../lib/scalp/types';

type RawDeploymentRow = {
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    source: 'manual' | 'backtest' | 'matrix';
    enabled: boolean;
    configOverride: Record<string, unknown> | null;
    promotionGate: Record<string, unknown> | null;
    updatedBy: string | null;
    assetCategory: string | null;
    instrumentType: string | null;
};

type CandleBoundsRow = {
    symbol: string;
    weekRows: number;
    candles: number;
    fromTsMs: number | null;
    toTsMs: number | null;
};

type CandidateMigration = {
    oldDeploymentId: string;
    newDeploymentId: string;
    oldSymbol: string;
    newSymbol: string;
    strategyId: string;
    tuneId: string;
    source: 'manual' | 'backtest' | 'matrix';
    enabled: boolean;
    configOverride: Record<string, unknown> | null;
    promotionGate: Record<string, unknown> | null;
    updatedBy: string | null;
    reason: string;
};

type WeeklyBucket = {
    weekStartMs: number;
    candles: ScalpCandle[];
};

type BackfillSummary = {
    symbol: string;
    fromMs: number;
    toMs: number;
    requests: number;
    fetchedCandles: number;
    weekRows: number;
};

type MigrationRowSummary = {
    oldDeploymentId: string;
    newDeploymentId: string;
    oldSymbol: string;
    newSymbol: string;
    childRowsMoved: {
        sessions: number;
        executionRuns: number;
        tradeLedger: number;
        researchTasks: number;
        journal: number;
    };
    targetRowsReset: {
        sessions: number;
        executionRuns: number;
        tradeLedger: number;
        researchTasks: number;
    };
};

type ScriptOptions = {
    apply: boolean;
    includeDisabled: boolean;
    backfillSymbols: string[];
    requeueDeploymentIds: string[];
    timeframe: string;
    defaultLookbackDays: number;
    limitPerRequest: number;
    requestSpanMinutes: number;
    sleepMs: number;
    maxRequestsPerSymbol: number;
    actor: string;
};

const ONE_MINUTE_MS = 60_000;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const BITGET_HISTORY_CANDLES_MAX_LIMIT = 200;

function parseArgs(argv: string[]): ScriptOptions {
    const opts: ScriptOptions = {
        apply: false,
        includeDisabled: false,
        backfillSymbols: [],
        requeueDeploymentIds: [],
        timeframe: '1m',
        defaultLookbackDays: 120,
        limitPerRequest: BITGET_HISTORY_CANDLES_MAX_LIMIT,
        requestSpanMinutes: 220,
        sleepMs: 25,
        maxRequestsPerSymbol: 2500,
        actor: 'phase3_capital_crypto_recycle_to_bitget',
    };

    for (const arg of argv) {
        if (arg === '--apply') {
            opts.apply = true;
            continue;
        }
        if (arg === '--includeDisabled') {
            opts.includeDisabled = true;
            continue;
        }
        if (!arg.startsWith('--')) continue;
        const [rawKey, rawValue = ''] = arg.split('=');
        const key = rawKey.trim();
        const value = rawValue.trim();
        if (!key) continue;

        if (key === '--timeframe' && value) {
            opts.timeframe = value.toLowerCase();
        } else if (key === '--includeDisabled' && value) {
            const normalized = value.toLowerCase();
            opts.includeDisabled = normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on';
        } else if (key === '--backfillSymbols' && value) {
            opts.backfillSymbols = Array.from(
                new Set(
                    value
                        .split(',')
                        .map((row) => normalizeSymbol(row))
                        .filter((row) => Boolean(row)),
                ),
            );
        } else if (key === '--requeueDeploymentIds' && value) {
            opts.requeueDeploymentIds = Array.from(
                new Set(
                    value
                        .split(',')
                        .map((row) => String(row || '').trim())
                        .filter((row) => Boolean(row)),
                ),
            );
        } else if (key === '--defaultLookbackDays' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n > 0) opts.defaultLookbackDays = Math.max(1, Math.floor(n));
        } else if (key === '--limitPerRequest' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n > 0) {
                opts.limitPerRequest = Math.max(20, Math.min(BITGET_HISTORY_CANDLES_MAX_LIMIT, Math.floor(n)));
            }
        } else if (key === '--requestSpanMinutes' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n > 0) opts.requestSpanMinutes = Math.max(120, Math.floor(n));
        } else if (key === '--sleepMs' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n >= 0) opts.sleepMs = Math.max(0, Math.floor(n));
        } else if (key === '--maxRequestsPerSymbol' && value) {
            const n = Number(value);
            if (Number.isFinite(n) && n > 0) opts.maxRequestsPerSymbol = Math.max(100, Math.floor(n));
        } else if (key === '--actor' && value) {
            opts.actor = value.slice(0, 120);
        }
    }

    return opts;
}

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function isCapitalVenueDeploymentId(deploymentId: string): boolean {
    const normalized = String(deploymentId || '').trim().toLowerCase();
    if (!normalized) return true;
    return !normalized.startsWith('bitget:');
}

function isCryptoDeploymentRow(row: RawDeploymentRow): boolean {
    const assetCategory = String(row.assetCategory || '')
        .trim()
        .toLowerCase();
    const instrumentType = String(row.instrumentType || '')
        .trim()
        .toUpperCase();
    if (assetCategory === 'crypto') return true;
    if (instrumentType === 'CRYPTOCURRENCIES') return true;
    return false;
}

function parseBitgetCandle(row: unknown): ScalpCandle | null {
    if (!Array.isArray(row)) return null;
    const ts = Number(row[0]);
    const open = Number(row[1]);
    const high = Number(row[2]);
    const low = Number(row[3]);
    const close = Number(row[4]);
    const volume = Number(row[5] ?? 0);
    if (![ts, open, high, low, close].every((n) => Number.isFinite(n) && n > 0)) return null;
    return [Math.floor(ts), open, high, low, close, Number.isFinite(volume) ? volume : 0];
}

function dedupeSortCandles(candles: ScalpCandle[]): ScalpCandle[] {
    const byTs = new Map<number, ScalpCandle>();
    for (const row of candles) {
        const parsed = parseBitgetCandle(row);
        if (!parsed) continue;
        byTs.set(parsed[0], parsed);
    }
    return Array.from(byTs.values()).sort((lhs, rhs) => lhs[0] - rhs[0]);
}

function weekStartMondayUtcMs(tsMs: number): number {
    const date = new Date(tsMs);
    const dayStart = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
    const day = new Date(dayStart).getUTCDay();
    const sinceMonday = (day + 6) % 7;
    return dayStart - sinceMonday * ONE_DAY_MS;
}

function toWeeklyBuckets(candles: ScalpCandle[]): WeeklyBucket[] {
    const byWeek = new Map<number, ScalpCandle[]>();
    for (const candle of candles) {
        const ts = Number(candle?.[0] || 0);
        if (!Number.isFinite(ts) || ts <= 0) continue;
        const key = weekStartMondayUtcMs(ts);
        const bucket = byWeek.get(key) || [];
        bucket.push(candle);
        byWeek.set(key, bucket);
    }

    return Array.from(byWeek.entries())
        .sort((lhs, rhs) => lhs[0] - rhs[0])
        .map(([weekStartMs, rows]) => ({
            weekStartMs,
            candles: dedupeSortCandles(rows),
        }));
}

function delay(ms: number): Promise<void> {
    if (!(ms > 0)) return Promise.resolve();
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function resolveBitgetSymbol(symbol: string, bitgetContracts: Set<string>): { symbol: string | null; reason: string } {
    const normalized = normalizeSymbol(symbol);
    if (!normalized) return { symbol: null, reason: 'invalid_symbol' };
    if (bitgetContracts.has(normalized)) return { symbol: normalized, reason: 'exact_contract' };

    const candidates: string[] = [];
    if (normalized.endsWith('USD') && !normalized.endsWith('USDT')) {
        const base = normalized.slice(0, -3);
        if (base) candidates.push(`${base}USDT`);
    }
    if (!normalized.endsWith('USDT')) candidates.push(`${normalized}USDT`);

    for (const candidate of candidates) {
        if (bitgetContracts.has(candidate)) {
            return { symbol: candidate, reason: `mapped_${normalized}_to_${candidate}` };
        }
    }

    return { symbol: null, reason: 'bitget_contract_not_found' };
}

async function fetchBitgetContractSet(): Promise<Set<string>> {
    const productType = String(resolveProductType() || 'usdt-futures')
        .trim()
        .toUpperCase();
    const contracts = await bitgetFetch('GET', '/api/v2/mix/market/contracts', { productType });
    const set = new Set<string>();
    if (Array.isArray(contracts)) {
        for (const row of contracts) {
            const symbol = normalizeSymbol((row as Record<string, unknown>)?.symbol);
            if (!symbol) continue;
            set.add(symbol);
        }
    }
    return set;
}

async function fetchBitget1mCandles(params: {
    symbol: string;
    fromMs: number;
    toMs: number;
    limitPerRequest: number;
    requestSpanMinutes: number;
    sleepMs: number;
    maxRequestsPerSymbol: number;
}): Promise<{ candles: ScalpCandle[]; requests: number }> {
    const symbol = normalizeSymbol(params.symbol);
    const fromMs = Math.floor(params.fromMs);
    const toMs = Math.floor(params.toMs);
    if (!symbol) throw new Error('invalid_symbol_for_bitget_backfill');
    if (!(Number.isFinite(fromMs) && Number.isFinite(toMs) && fromMs > 0 && toMs > fromMs)) {
        throw new Error(`invalid_backfill_window_for_${symbol}`);
    }

    const productType = String(resolveProductType() || 'usdt-futures')
        .trim()
        .toUpperCase();

    const candlesByTs = new Map<number, ScalpCandle>();
    let cursorEnd = toMs;
    let requests = 0;
    const spanMs = Math.max(120, Math.floor(params.requestSpanMinutes)) * ONE_MINUTE_MS;
    const requestLimit = Math.max(20, Math.min(BITGET_HISTORY_CANDLES_MAX_LIMIT, Math.floor(params.limitPerRequest)));
    const maxRequests = Math.max(100, Math.floor(params.maxRequestsPerSymbol));

    while (cursorEnd >= fromMs) {
        if (requests >= maxRequests) {
            throw new Error(`backfill_max_requests_reached_for_${symbol}`);
        }
        const startTime = Math.max(fromMs, cursorEnd - spanMs + ONE_MINUTE_MS);
        const rows = await bitgetFetch('GET', '/api/v2/mix/market/history-candles', {
            symbol,
            productType,
            granularity: '1m',
            limit: requestLimit,
            startTime,
            endTime: cursorEnd,
        });
        requests += 1;

        const parsedRows = Array.isArray(rows)
            ? rows
                  .map((row) => parseBitgetCandle(row))
                  .filter((row): row is ScalpCandle => Boolean(row))
                  .filter((row) => row[0] >= fromMs && row[0] <= toMs)
            : [];

        if (!parsedRows.length) {
            if (startTime <= fromMs) break;
            cursorEnd = startTime - ONE_MINUTE_MS;
            if (params.sleepMs > 0) await delay(params.sleepMs);
            continue;
        }

        let oldestTs = Number.POSITIVE_INFINITY;
        for (const candle of parsedRows) {
            candlesByTs.set(candle[0], candle);
            if (candle[0] < oldestTs) oldestTs = candle[0];
        }

        if (!Number.isFinite(oldestTs)) break;
        if (oldestTs >= cursorEnd) {
            cursorEnd -= spanMs;
        } else {
            cursorEnd = oldestTs - 1;
        }
        if (params.sleepMs > 0) await delay(params.sleepMs);
    }

    return {
        candles: Array.from(candlesByTs.values()).sort((lhs, rhs) => lhs[0] - rhs[0]),
        requests,
    };
}

async function loadCapitalCryptoDeployments(): Promise<RawDeploymentRow[]> {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            deploymentId: string;
            symbol: string;
            strategyId: string;
            tuneId: string;
            source: string;
            enabled: boolean;
            configOverride: unknown;
            promotionGate: unknown;
            updatedBy: string | null;
            assetCategory: string | null;
            instrumentType: string | null;
        }>
    >(Prisma.sql`
        SELECT
            d.deployment_id AS "deploymentId",
            d.symbol,
            d.strategy_id AS "strategyId",
            d.tune_id AS "tuneId",
            d.source,
            d.enabled,
            d.config_override AS "configOverride",
            d.promotion_gate AS "promotionGate",
            d.updated_by AS "updatedBy",
            NULLIF(TRIM(COALESCE(m.asset_category, '')), '') AS "assetCategory",
            NULLIF(TRIM(COALESCE(m.instrument_type, '')), '') AS "instrumentType"
        FROM scalp_deployments d
        LEFT JOIN scalp_symbol_market_metadata m
          ON m.symbol = d.symbol
        ORDER BY d.symbol ASC, d.strategy_id ASC, d.tune_id ASC;
    `);

    return rows.map((row) => ({
        deploymentId: String(row.deploymentId || '').trim(),
        symbol: normalizeSymbol(row.symbol),
        strategyId: String(row.strategyId || '').trim().toLowerCase(),
        tuneId: String(row.tuneId || '').trim().toLowerCase(),
        source: (String(row.source || '').trim().toLowerCase() as RawDeploymentRow['source']) || 'manual',
        enabled: Boolean(row.enabled),
        configOverride:
            row.configOverride && typeof row.configOverride === 'object' && !Array.isArray(row.configOverride)
                ? (row.configOverride as Record<string, unknown>)
                : null,
        promotionGate:
            row.promotionGate && typeof row.promotionGate === 'object' && !Array.isArray(row.promotionGate)
                ? (row.promotionGate as Record<string, unknown>)
                : null,
        updatedBy: row.updatedBy ? String(row.updatedBy) : null,
        assetCategory: row.assetCategory ? String(row.assetCategory) : null,
        instrumentType: row.instrumentType ? String(row.instrumentType) : null,
    }));
}

async function loadCandleBoundsBySymbol(timeframe: string, symbols: string[]): Promise<Map<string, CandleBoundsRow>> {
    const out = new Map<string, CandleBoundsRow>();
    if (!symbols.length) return out;
    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            symbol: string;
            weekRows: number | bigint;
            candles: number | bigint;
            fromTsMs: number | bigint | null;
            toTsMs: number | bigint | null;
        }>
    >(Prisma.sql`
        SELECT
            symbol,
            COUNT(*)::bigint AS "weekRows",
            COALESCE(SUM(jsonb_array_length(candles_json)), 0)::bigint AS candles,
            MIN(
                CASE
                    WHEN jsonb_array_length(candles_json) > 0
                         AND (candles_json -> 0 ->> 0) ~ '^[0-9]+$'
                    THEN (candles_json -> 0 ->> 0)::bigint
                    ELSE NULL
                END
            ) AS "fromTsMs",
            MAX(
                CASE
                    WHEN jsonb_array_length(candles_json) > 0
                         AND (candles_json -> (jsonb_array_length(candles_json) - 1) ->> 0) ~ '^[0-9]+$'
                    THEN (candles_json -> (jsonb_array_length(candles_json) - 1) ->> 0)::bigint
                    ELSE NULL
                END
            ) AS "toTsMs"
        FROM scalp_candle_history_weeks
        WHERE timeframe = ${timeframe}
          AND symbol IN (${Prisma.join(symbols)})
        GROUP BY symbol;
    `);

    for (const row of rows) {
        const symbol = normalizeSymbol(row.symbol);
        if (!symbol) continue;
        out.set(symbol, {
            symbol,
            weekRows: Number(row.weekRows || 0),
            candles: Number(row.candles || 0),
            fromTsMs: row.fromTsMs === null ? null : Number(row.fromTsMs),
            toTsMs: row.toTsMs === null ? null : Number(row.toTsMs),
        });
    }
    return out;
}

async function resetTargetDeploymentIfExists(params: {
    tx: Prisma.TransactionClient;
    targetDeploymentId: string;
}): Promise<MigrationRowSummary['targetRowsReset']> {
    const out = {
        sessions: 0,
        executionRuns: 0,
        tradeLedger: 0,
        researchTasks: 0,
    };
    const targetDeploymentId = String(params.targetDeploymentId || '').trim();
    if (!targetDeploymentId) return out;

    const targetExistsRows = await params.tx.$queryRaw<Array<{ exists: boolean }>>(Prisma.sql`
        SELECT EXISTS(
            SELECT 1
            FROM scalp_deployments
            WHERE deployment_id = ${targetDeploymentId}
        ) AS exists;
    `);
    const targetExists = Boolean(targetExistsRows[0]?.exists);
    if (!targetExists) return out;

    out.sessions = Number(
        await params.tx.$executeRaw(Prisma.sql`
            DELETE FROM scalp_sessions
            WHERE deployment_id = ${targetDeploymentId};
        `),
    );
    out.executionRuns = Number(
        await params.tx.$executeRaw(Prisma.sql`
            DELETE FROM scalp_execution_runs
            WHERE deployment_id = ${targetDeploymentId};
        `),
    );
    out.tradeLedger = Number(
        await params.tx.$executeRaw(Prisma.sql`
            DELETE FROM scalp_trade_ledger
            WHERE deployment_id = ${targetDeploymentId};
        `),
    );
    out.researchTasks = Number(
        await params.tx.$executeRaw(Prisma.sql`
            DELETE FROM scalp_research_tasks
            WHERE deployment_id = ${targetDeploymentId};
        `),
    );

    await params.tx.$executeRaw(Prisma.sql`
        DELETE FROM scalp_deployments
        WHERE deployment_id = ${targetDeploymentId};
    `);

    return out;
}

async function remapDeploymentRow(params: {
    tx: Prisma.TransactionClient;
    row: CandidateMigration;
    actor: string;
}): Promise<MigrationRowSummary> {
    const { tx, row } = params;

    const targetRowsReset = await resetTargetDeploymentIfExists({
        tx,
        targetDeploymentId: row.newDeploymentId,
    });

    if (
        row.oldDeploymentId !== row.newDeploymentId &&
        row.oldSymbol === row.newSymbol
    ) {
        const tempSymbol = `${row.oldSymbol}_TMP_${Date.now().toString(36).toUpperCase().slice(-6)}`;
        await tx.$executeRaw(
            Prisma.sql`
                UPDATE scalp_deployments
                SET
                    symbol = ${tempSymbol},
                    updated_by = ${params.actor},
                    updated_at = NOW()
                WHERE deployment_id = ${row.oldDeploymentId};
            `,
        );
    }

    await tx.$executeRaw(
        Prisma.sql`
            INSERT INTO scalp_deployments(
                deployment_id,
                symbol,
                strategy_id,
                tune_id,
                source,
                enabled,
                config_override,
                promotion_gate,
                updated_by
            )
            VALUES(
                ${row.newDeploymentId},
                ${row.newSymbol},
                ${row.strategyId},
                ${row.tuneId},
                ${row.source},
                ${row.enabled},
                ${JSON.stringify(row.configOverride || {})}::jsonb,
                ${row.promotionGate ? JSON.stringify(row.promotionGate) : null}::jsonb,
                ${params.actor}
            )
            ON CONFLICT(deployment_id) DO UPDATE
            SET
                symbol = EXCLUDED.symbol,
                strategy_id = EXCLUDED.strategy_id,
                tune_id = EXCLUDED.tune_id,
                source = EXCLUDED.source,
                enabled = EXCLUDED.enabled,
                config_override = EXCLUDED.config_override,
                promotion_gate = EXCLUDED.promotion_gate,
                updated_by = EXCLUDED.updated_by,
                updated_at = NOW();
        `,
    );

    const moved = {
        sessions: Number(
            await tx.$executeRaw(
                Prisma.sql`
                    UPDATE scalp_sessions
                    SET deployment_id = ${row.newDeploymentId}
                    WHERE deployment_id = ${row.oldDeploymentId};
                `,
            ),
        ),
        executionRuns: Number(
            await tx.$executeRaw(
                Prisma.sql`
                    UPDATE scalp_execution_runs
                    SET deployment_id = ${row.newDeploymentId}
                    WHERE deployment_id = ${row.oldDeploymentId};
                `,
            ),
        ),
        tradeLedger: Number(
            await tx.$executeRaw(
                Prisma.sql`
                    UPDATE scalp_trade_ledger
                    SET deployment_id = ${row.newDeploymentId}
                    WHERE deployment_id = ${row.oldDeploymentId};
                `,
            ),
        ),
        researchTasks: Number(
            await tx.$executeRaw(
                Prisma.sql`
                    UPDATE scalp_research_tasks
                    SET deployment_id = ${row.newDeploymentId}
                    WHERE deployment_id = ${row.oldDeploymentId};
                `,
            ),
        ),
        journal: Number(
            await tx.$executeRaw(
                Prisma.sql`
                    UPDATE scalp_journal
                    SET deployment_id = ${row.newDeploymentId}
                    WHERE deployment_id = ${row.oldDeploymentId};
                `,
            ),
        ),
    };

    await tx.$executeRaw(
        Prisma.sql`
            UPDATE scalp_trade_ledger
            SET symbol = ${row.newSymbol}
            WHERE deployment_id = ${row.newDeploymentId}
              AND symbol <> ${row.newSymbol};
        `,
    );
    await tx.$executeRaw(
        Prisma.sql`
            UPDATE scalp_research_tasks
            SET symbol = ${row.newSymbol}
            WHERE deployment_id = ${row.newDeploymentId}
              AND symbol <> ${row.newSymbol};
        `,
    );
    await tx.$executeRaw(
        Prisma.sql`
            UPDATE scalp_journal
            SET symbol = ${row.newSymbol}
            WHERE deployment_id = ${row.newDeploymentId}
              AND (symbol IS NULL OR symbol <> ${row.newSymbol});
        `,
    );

    await tx.$executeRaw(
        Prisma.sql`
            DELETE FROM scalp_deployments
            WHERE deployment_id = ${row.oldDeploymentId};
        `,
    );

    return {
        oldDeploymentId: row.oldDeploymentId,
        newDeploymentId: row.newDeploymentId,
        oldSymbol: row.oldSymbol,
        newSymbol: row.newSymbol,
        childRowsMoved: moved,
        targetRowsReset,
    };
}

async function replaceWeekly1mCandles(params: {
    symbol: string;
    epic: string;
    source: string;
    candles: ScalpCandle[];
    timeframe: string;
}): Promise<number> {
    const db = scalpPrisma();
    const symbol = normalizeSymbol(params.symbol);
    if (!symbol) return 0;
    const weekly = toWeeklyBuckets(params.candles);
    const timeframe = String(params.timeframe || '1m').trim().toLowerCase();

    await db.$executeRaw(
        Prisma.sql`
            DELETE FROM scalp_candle_history_weeks
            WHERE symbol = ${symbol}
              AND timeframe = ${timeframe};
        `,
    );

    for (const bucket of weekly) {
        await db.$executeRaw(
            Prisma.sql`
                INSERT INTO scalp_candle_history_weeks(
                    symbol,
                    timeframe,
                    week_start,
                    epic,
                    source,
                    candles_json,
                    updated_at
                )
                VALUES(
                    ${symbol},
                    ${timeframe},
                    to_timestamp(${bucket.weekStartMs} / 1000.0),
                    ${params.epic},
                    ${params.source},
                    ${JSON.stringify(bucket.candles)}::jsonb,
                    NOW()
                )
                ON CONFLICT(symbol, timeframe, week_start)
                DO UPDATE SET
                    epic = EXCLUDED.epic,
                    source = EXCLUDED.source,
                    candles_json = EXCLUDED.candles_json,
                    updated_at = NOW();
            `,
        );
    }

    return weekly.length;
}

async function requeueTasksForDeployments(deploymentIds: string[]): Promise<number> {
    if (!deploymentIds.length) return 0;
    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
            UPDATE scalp_research_tasks t
            SET
                status = 'pending',
                attempts = 0,
                next_eligible_at = NOW(),
                worker_id = NULL,
                started_at = NULL,
                finished_at = NULL,
                result_json = NULL,
                error_code = NULL,
                error_message = NULL,
                updated_at = NOW()
            WHERE t.deployment_id IN (${Prisma.join(deploymentIds)});
        `,
    );
    return Number(updated || 0);
}

async function main(): Promise<void> {
    const opts = parseArgs(process.argv.slice(2));
    const nowMs = Date.now();
    const db = scalpPrisma();

    const allDeployments = await loadCapitalCryptoDeployments();
    const contractSet = await fetchBitgetContractSet();

    const migrationCandidates: CandidateMigration[] = [];
    const skippedRows: Array<{ deploymentId: string; symbol: string; reason: string }> = [];

    for (const row of allDeployments) {
        if (!isCapitalVenueDeploymentId(row.deploymentId)) continue;
        if (!isCryptoDeploymentRow(row)) continue;
        if (!opts.includeDisabled && !row.enabled) continue;
        if (!row.symbol || !row.strategyId || !row.tuneId) continue;
        const parsed = parseScalpDeploymentId(row.deploymentId);
        if (!parsed) {
            skippedRows.push({
                deploymentId: row.deploymentId,
                symbol: row.symbol,
                reason: 'invalid_deployment_id',
            });
            continue;
        }
        const mapping = resolveBitgetSymbol(row.symbol, contractSet);
        if (!mapping.symbol) {
            skippedRows.push({
                deploymentId: row.deploymentId,
                symbol: row.symbol,
                reason: mapping.reason,
            });
            continue;
        }
        const newSymbol = mapping.symbol;
        const newDeploymentId = buildScalpDeploymentId({
            venue: 'bitget',
            symbol: newSymbol,
            strategyId: row.strategyId,
            tuneId: row.tuneId,
        });
        migrationCandidates.push({
            oldDeploymentId: row.deploymentId,
            newDeploymentId,
            oldSymbol: row.symbol,
            newSymbol,
            strategyId: row.strategyId,
            tuneId: row.tuneId,
            source: row.source,
            enabled: row.enabled,
            configOverride: row.configOverride,
            promotionGate: row.promotionGate,
            updatedBy: row.updatedBy,
            reason: mapping.reason,
        });
    }

    const explicitBackfillSymbols = Array.from(
        new Set(
            opts.backfillSymbols
                .map((row) => normalizeSymbol(row))
                .filter((row) => Boolean(row)),
        ),
    ).sort();
    const uniqueOldSymbols = Array.from(new Set(migrationCandidates.map((row) => row.oldSymbol))).sort();
    const uniqueNewSymbols = Array.from(new Set(migrationCandidates.map((row) => row.newSymbol))).sort();
    const boundsSymbols = Array.from(new Set([...uniqueOldSymbols, ...explicitBackfillSymbols])).sort();
    const symbolBounds = await loadCandleBoundsBySymbol(opts.timeframe, boundsSymbols);

    const plan = migrationCandidates.map((row) => {
        const bounds = symbolBounds.get(row.oldSymbol) || null;
        const fromMs = bounds?.fromTsMs ?? nowMs - opts.defaultLookbackDays * ONE_DAY_MS;
        const toMs = Math.max(bounds?.toTsMs ?? 0, nowMs);
        return {
            ...row,
            history: {
                oldWeekRows: bounds?.weekRows ?? 0,
                oldCandles: bounds?.candles ?? 0,
                fromMs,
                toMs,
            },
        };
    });

    const report: Record<string, unknown> = {
        apply: opts.apply,
        options: opts,
        nowIso: new Date(nowMs).toISOString(),
        counts: {
            totalDeploymentsScanned: allDeployments.length,
            migrationCandidates: plan.length,
            skipped: skippedRows.length,
        },
        explicitBackfillSymbols,
        explicitRequeueDeploymentIds: opts.requeueDeploymentIds,
        skipped: skippedRows,
        plan,
    };

    if (!opts.apply) {
        console.log(JSON.stringify(report, null, 2));
        return;
    }

    const migrationSummaries: MigrationRowSummary[] = [];
    for (const row of migrationCandidates) {
        const summary = await db.$transaction(async (tx) =>
            remapDeploymentRow({
                tx,
                row,
                actor: opts.actor,
            }),
        );
        migrationSummaries.push(summary);
    }

    const symbolsToDelete = Array.from(
        new Set([...migrationCandidates.flatMap((row) => [row.oldSymbol, row.newSymbol]), ...explicitBackfillSymbols]),
    ).sort();
    if (symbolsToDelete.length) {
        await db.$executeRaw(
            Prisma.sql`
                DELETE FROM scalp_candle_history_weeks
                WHERE timeframe = ${opts.timeframe}
                  AND symbol IN (${Prisma.join(symbolsToDelete)});
            `,
        );
    }

    const backfillSummaries: BackfillSummary[] = [];
    const symbolBackfillWindows = new Map<string, { fromMs: number; toMs: number }>();
    for (const row of plan) {
        const current = symbolBackfillWindows.get(row.newSymbol);
        if (!current) {
            symbolBackfillWindows.set(row.newSymbol, {
                fromMs: row.history.fromMs,
                toMs: row.history.toMs,
            });
            continue;
        }
        current.fromMs = Math.min(current.fromMs, row.history.fromMs);
        current.toMs = Math.max(current.toMs, row.history.toMs);
    }
    for (const symbol of explicitBackfillSymbols) {
        const bounds = symbolBounds.get(symbol) || null;
        const fromMs = bounds?.fromTsMs ?? nowMs - opts.defaultLookbackDays * ONE_DAY_MS;
        const toMs = Math.max(bounds?.toTsMs ?? 0, nowMs);
        const current = symbolBackfillWindows.get(symbol);
        if (!current) {
            symbolBackfillWindows.set(symbol, { fromMs, toMs });
            continue;
        }
        current.fromMs = Math.min(current.fromMs, fromMs);
        current.toMs = Math.max(current.toMs, toMs);
    }

    for (const [symbol, window] of symbolBackfillWindows.entries()) {
        const fetchRes = await fetchBitget1mCandles({
            symbol,
            fromMs: window.fromMs,
            toMs: window.toMs,
            limitPerRequest: opts.limitPerRequest,
            requestSpanMinutes: opts.requestSpanMinutes,
            sleepMs: opts.sleepMs,
            maxRequestsPerSymbol: opts.maxRequestsPerSymbol,
        });
        const weekRows = await replaceWeekly1mCandles({
            symbol,
            epic: symbol,
            source: 'bitget',
            candles: fetchRes.candles,
            timeframe: opts.timeframe,
        });
        backfillSummaries.push({
            symbol,
            fromMs: window.fromMs,
            toMs: window.toMs,
            requests: fetchRes.requests,
            fetchedCandles: fetchRes.candles.length,
            weekRows,
        });
    }

    const deploymentIdsForQueue = Array.from(
        new Set([...migrationCandidates.map((row) => row.newDeploymentId), ...opts.requeueDeploymentIds]),
    );
    const requeuedTasks = await requeueTasksForDeployments(deploymentIdsForQueue);

    const postBounds = await loadCandleBoundsBySymbol(opts.timeframe, uniqueNewSymbols);
    const postBoundsRows = Array.from(postBounds.values()).sort((lhs, rhs) => lhs.symbol.localeCompare(rhs.symbol));

    console.log(
        JSON.stringify(
            {
                ...report,
                applied: true,
                migrationSummaries,
                backfillSummaries,
                queue: {
                    deploymentIds: deploymentIdsForQueue,
                    requeuedTasks,
                },
                postCandleBounds: postBoundsRows,
            },
            null,
            2,
        ),
    );
}

main().catch((err) => {
    const message = String(err?.message || err || 'unknown_error');
    const stack = err instanceof Error ? err.stack : undefined;
    console.error(
        JSON.stringify(
            {
                ok: false,
                error: message,
                stack,
            },
            null,
            2,
        ),
    );
    process.exit(1);
});
