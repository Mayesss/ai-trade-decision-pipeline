import crypto from 'node:crypto';

import { Prisma } from '@prisma/client';

import { normalizeScalpTuneId } from '../deployments';
import { getDefaultScalpStrategy, getScalpStrategyById } from '../strategies/registry';
import type { ScalpJournalEntry, ScalpSessionState, ScalpTradeLedgerEntry } from '../types';
import { scalpPrisma } from './client';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function toDate(valueMs: unknown, fallbackMs = Date.now()): Date {
    const n = Number(valueMs);
    if (!Number.isFinite(n) || n <= 0) return new Date(Math.floor(fallbackMs));
    return new Date(Math.floor(n));
}

function normalizeReasonCodes(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    const out: string[] = [];
    for (const raw of value) {
        const code = String(raw || '')
            .trim()
            .toUpperCase();
        if (!code) continue;
        out.push(code.slice(0, 80));
        if (out.length >= 64) break;
    }
    return out;
}

function normalizeOptionalText(value: unknown, maxLen: number): string | null {
    const normalized = String(value || '').trim();
    if (!normalized) return null;
    return normalized.slice(0, maxLen);
}

function stableUuidFromSeed(seed: string): string {
    const hex = crypto.createHash('sha1').update(seed).digest('hex').slice(0, 32).split('');
    hex[12] = '5';
    hex[16] = ((parseInt(hex[16] || '0', 16) & 0x3) | 0x8).toString(16);
    return `${hex.slice(0, 8).join('')}-${hex.slice(8, 12).join('')}-${hex.slice(12, 16).join('')}-${hex.slice(16, 20).join('')}-${hex.slice(20, 32).join('')}`;
}

function normalizeUuid(value: unknown, fallbackSeed: string): string {
    const candidate = String(value || '').trim().toLowerCase();
    if (UUID_RE.test(candidate)) return candidate;
    return stableUuidFromSeed(fallbackSeed);
}

function normalizeDayKey(value: unknown): string {
    const raw = String(value || '').trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
    const asDate = new Date(raw);
    if (Number.isNaN(asDate.getTime())) {
        return new Date().toISOString().slice(0, 10);
    }
    return asDate.toISOString().slice(0, 10);
}

function asJsonObject(value: unknown): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
    return value as Record<string, unknown>;
}

export async function upsertRuntimeDefaultToPg(params: {
    defaultStrategyId: string;
    envEnabled: boolean;
    updatedBy?: string | null;
}): Promise<number> {
    const defaultStrategyId = getScalpStrategyById(params.defaultStrategyId)?.id || getDefaultScalpStrategy().id;
    const updatedBy = normalizeOptionalText(params.updatedBy, 120);
    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        INSERT INTO scalp_runtime_settings(singleton, default_strategy_id, env_enabled, updated_by, updated_at)
        VALUES(TRUE, ${defaultStrategyId}, ${Boolean(params.envEnabled)}, ${updatedBy}, NOW())
        ON CONFLICT(singleton)
        DO UPDATE SET
            default_strategy_id = EXCLUDED.default_strategy_id,
            env_enabled = EXCLUDED.env_enabled,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW();
        `,
    );
    return Number(updated || 0);
}

export async function upsertStrategyOverrideToPg(params: {
    strategyId: string;
    kvEnabled: boolean | null;
    updatedAtMs?: number | null;
    updatedBy?: string | null;
}): Promise<number> {
    const strategyId = String(params.strategyId || '')
        .trim()
        .toLowerCase();
    if (!strategyId) return 0;
    const kvEnabled = typeof params.kvEnabled === 'boolean' ? params.kvEnabled : null;
    const updatedAt = toDate(params.updatedAtMs, Date.now());
    const updatedBy = normalizeOptionalText(params.updatedBy, 120);
    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        INSERT INTO scalp_strategy_overrides(strategy_id, kv_enabled, updated_by, updated_at)
        VALUES(${strategyId}, ${kvEnabled}, ${updatedBy}, ${updatedAt})
        ON CONFLICT(strategy_id)
        DO UPDATE SET
            kv_enabled = EXCLUDED.kv_enabled,
            updated_by = EXCLUDED.updated_by,
            updated_at = EXCLUDED.updated_at;
        `,
    );
    return Number(updated || 0);
}

export async function upsertStrategyOverridesBulkToPg(
    rows: Array<{
        strategyId: string;
        kvEnabled: boolean | null;
        updatedAtMs?: number | null;
        updatedBy?: string | null;
    }>,
): Promise<number> {
    const payload = rows
        .map((row) => {
            const strategyId = String(row.strategyId || '')
                .trim()
                .toLowerCase();
            if (!strategyId) return null;
            return {
                strategy_id: strategyId,
                kv_enabled: typeof row.kvEnabled === 'boolean' ? row.kvEnabled : null,
                updated_by: normalizeOptionalText(row.updatedBy, 120),
                updated_at: toDate(row.updatedAtMs, Date.now()).toISOString(),
            };
        })
        .filter((row): row is NonNullable<typeof row> => Boolean(row));

    if (!payload.length) return 0;
    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        WITH input AS (
            SELECT *
            FROM jsonb_to_recordset(${JSON.stringify(payload)}::jsonb) AS x(
                strategy_id text,
                kv_enabled boolean,
                updated_by text,
                updated_at timestamptz
            )
        )
        INSERT INTO scalp_strategy_overrides(strategy_id, kv_enabled, updated_by, updated_at)
        SELECT
            x.strategy_id,
            x.kv_enabled,
            x.updated_by,
            x.updated_at
        FROM input x
        ON CONFLICT(strategy_id)
        DO UPDATE SET
            kv_enabled = EXCLUDED.kv_enabled,
            updated_by = EXCLUDED.updated_by,
            updated_at = EXCLUDED.updated_at;
        `,
    );

    return Number(updated || 0);
}

export async function upsertSessionStateToPg(state: ScalpSessionState): Promise<number> {
    const deploymentId = String(state.deploymentId || '').trim();
    if (!deploymentId) return 0;
    const symbol = String(state.symbol || '')
        .trim()
        .toUpperCase();
    const strategyId = String(state.strategyId || '')
        .trim()
        .toLowerCase();
    const tuneId = normalizeScalpTuneId(state.tuneId, 'default');
    const dayKey = normalizeDayKey(state.dayKey);
    const updatedAt = toDate(state.updatedAtMs, Date.now());
    const reasonCodes = normalizeReasonCodes(state.run?.lastReasonCodes);
    const stateJson = asJsonObject(state);

    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        WITH dep AS (
            INSERT INTO scalp_deployments(
                deployment_id, symbol, strategy_id, tune_id, source, enabled, config_override, updated_by
            )
            VALUES(
                ${deploymentId},
                ${symbol},
                ${strategyId},
                ${tuneId},
                'manual',
                TRUE,
                '{}'::jsonb,
                'phase_g_pg_primary'
            )
            ON CONFLICT(deployment_id) DO NOTHING
        )
        INSERT INTO scalp_sessions(
            deployment_id,
            day_key,
            state_json,
            last_reason_codes,
            updated_at
        )
        VALUES(
            ${deploymentId},
            ${dayKey}::date,
            ${JSON.stringify(stateJson)}::jsonb,
            ${reasonCodes},
            ${updatedAt}
        )
        ON CONFLICT(deployment_id, day_key)
        DO UPDATE SET
            state_json = EXCLUDED.state_json,
            last_reason_codes = EXCLUDED.last_reason_codes,
            updated_at = EXCLUDED.updated_at;
        `,
    );

    return Number(updated || 0);
}

export async function insertJournalEntryToPg(entry: ScalpJournalEntry): Promise<number> {
    const entryId = normalizeUuid(
        entry.id,
        `journal:${entry.id}:${entry.timestampMs}:${entry.type}:${entry.symbol || ''}`,
    );
    const ts = toDate(entry.timestampMs, Date.now());
    const deploymentId = normalizeOptionalText((entry.payload as Record<string, unknown> | undefined)?.deploymentId, 180);
    const symbol = normalizeOptionalText(entry.symbol, 40)?.toUpperCase() || null;
    const dayKey = entry.dayKey ? normalizeDayKey(entry.dayKey) : null;
    const level = entry.level === 'warn' || entry.level === 'error' ? entry.level : 'info';
    const type = normalizeOptionalText(entry.type, 80) || 'execution';
    const reasonCodes = normalizeReasonCodes(entry.reasonCodes);
    const payload = asJsonObject(entry.payload);

    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        INSERT INTO scalp_journal(
            id, ts, deployment_id, symbol, day_key, level, type, reason_codes, payload
        )
        VALUES(
            ${entryId}::uuid,
            ${ts},
            ${deploymentId},
            ${symbol},
            ${dayKey ? Prisma.sql`${dayKey}::date` : Prisma.sql`NULL::date`},
            ${level},
            ${type},
            ${reasonCodes},
            ${JSON.stringify(payload)}::jsonb
        )
        ON CONFLICT(id) DO NOTHING;
        `,
    );

    return Number(updated || 0);
}

export async function insertTradeLedgerEntryToPg(entry: ScalpTradeLedgerEntry): Promise<number> {
    const entryId = normalizeUuid(
        entry.id,
        `ledger:${entry.id}:${entry.exitAtMs}:${entry.deploymentId}:${entry.symbol}:${entry.strategyId}:${entry.tuneId}`,
    );
    const deploymentId = String(entry.deploymentId || '').trim();
    if (!deploymentId) return 0;
    const symbol = String(entry.symbol || '')
        .trim()
        .toUpperCase();
    const strategyId = String(entry.strategyId || '')
        .trim()
        .toLowerCase();
    const tuneId = normalizeScalpTuneId(entry.tuneId, 'default');
    const exitAt = toDate(entry.exitAtMs, Date.now());
    const side = entry.side === 'BUY' || entry.side === 'SELL' ? entry.side : null;
    const reasonCodes = normalizeReasonCodes(entry.reasonCodes);
    const rMultiple = Number.isFinite(Number(entry.rMultiple)) ? Number(entry.rMultiple) : 0;

    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        WITH dep AS (
            INSERT INTO scalp_deployments(
                deployment_id, symbol, strategy_id, tune_id, source, enabled, config_override, updated_by
            )
            VALUES(
                ${deploymentId},
                ${symbol},
                ${strategyId},
                ${tuneId},
                'manual',
                TRUE,
                '{}'::jsonb,
                'phase_g_pg_primary'
            )
            ON CONFLICT(deployment_id) DO NOTHING
        )
        INSERT INTO scalp_trade_ledger(
            id,
            exit_at,
            deployment_id,
            symbol,
            strategy_id,
            tune_id,
            side,
            dry_run,
            r_multiple,
            reason_codes
        )
        VALUES(
            ${entryId}::uuid,
            ${exitAt},
            ${deploymentId},
            ${symbol},
            ${strategyId},
            ${tuneId},
            ${side},
            ${Boolean(entry.dryRun)},
            ${rMultiple},
            ${reasonCodes}
        )
        ON CONFLICT(id)
        DO UPDATE SET
            exit_at = EXCLUDED.exit_at,
            side = EXCLUDED.side,
            dry_run = EXCLUDED.dry_run,
            r_multiple = EXCLUDED.r_multiple,
            reason_codes = EXCLUDED.reason_codes;
        `,
    );

    return Number(updated || 0);
}
