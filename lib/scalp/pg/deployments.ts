import { Prisma } from '@prisma/client';

import { resolveScalpDeploymentVenueFromId } from '../deployments';
import {
    formatScalpVenueDeploymentId,
    normalizeScalpVenue,
    parseScalpVenuePrefixedDeploymentId,
    type ScalpVenue,
} from '../venue';
import { scalpPrisma } from './client';

export interface PgExecutableDeploymentRow {
    deploymentId: string;
    venue: ScalpVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    source: 'manual' | 'backtest' | 'matrix';
    enabled: boolean;
    configOverride: Record<string, unknown> | null;
    promotionEligible: boolean;
    promotionReason: string | null;
    updatedAtMs: number;
    updatedBy: string | null;
}

export interface PgDeploymentRegistryRow {
    deploymentId: string;
    venue: ScalpVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    source: 'manual' | 'backtest' | 'matrix';
    enabled: boolean;
    inUniverse: boolean;
    configOverride: Record<string, unknown> | null;
    promotionGate: Record<string, unknown> | null;
    createdAtMs: number;
    updatedAtMs: number;
    updatedBy: string | null;
}

export interface PgUpsertDeploymentInput {
    deploymentId: string;
    venue?: ScalpVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    source: 'manual' | 'backtest' | 'matrix';
    enabled: boolean;
    configOverride?: Record<string, unknown> | null;
    promotionGate?: Record<string, unknown> | null;
    updatedBy?: string | null;
}

function asJsonObject(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
    return value as Record<string, unknown>;
}

export async function listExecutableDeploymentsFromPg(params: {
    requirePromotionEligible?: boolean;
    venue?: ScalpVenue;
    symbol?: string;
    symbols?: string[];
    strategyId?: string;
    deploymentIds?: string[];
    limit?: number;
} = {}): Promise<PgExecutableDeploymentRow[]> {
    const requirePromotionEligible = Boolean(params.requirePromotionEligible);
    const venue = params.venue ? normalizeScalpVenue(params.venue) : null;
    const symbol = String(params.symbol || '')
        .trim()
        .toUpperCase();
    const symbols = Array.isArray(params.symbols)
        ? params.symbols
              .map((row) =>
                  String(row || '')
                      .trim()
                      .toUpperCase(),
              )
              .filter((row, idx, rows) => row.length > 0 && rows.indexOf(row) === idx)
        : [];
    const strategyId = String(params.strategyId || '')
        .trim()
        .toLowerCase();
    const deploymentIds = Array.isArray(params.deploymentIds)
        ? params.deploymentIds
              .map((row) => String(row || '').trim())
              .filter((row, idx, rows) => row.length > 0 && rows.indexOf(row) === idx)
        : [];
    const limit = Math.max(1, Math.min(2000, Math.floor(Number(params.limit) || 250)));
    const symbolFilterSql = symbols.length > 0 ? Prisma.sql`AND d.symbol IN (${Prisma.join(symbols)})` : Prisma.empty;
    const deploymentFilterSql =
        deploymentIds.length > 0 ? Prisma.sql`AND d.deployment_id IN (${Prisma.join(deploymentIds)})` : Prisma.empty;
    const venueFilterSql =
        venue === 'bitget'
            ? Prisma.sql`AND d.deployment_id LIKE 'bitget:%'`
            : venue === 'capital'
            ? Prisma.sql`AND (d.deployment_id NOT LIKE '%:%' OR d.deployment_id LIKE 'capital:%')`
            : Prisma.empty;

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
            promotionEligible: boolean;
            promotionReason: string | null;
            updatedAt: Date;
            updatedBy: string | null;
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
            COALESCE((d.promotion_gate->>'eligible')::boolean, false) AS "promotionEligible",
            NULLIF(d.promotion_gate->>'reason', '') AS "promotionReason",
            d.updated_at AS "updatedAt",
            d.updated_by AS "updatedBy"
        FROM scalp_deployments d
        WHERE d.enabled = TRUE
          AND (${symbol} = '' OR d.symbol = ${symbol})
          ${symbolFilterSql}
          AND (${strategyId} = '' OR d.strategy_id = ${strategyId})
          ${deploymentFilterSql}
          ${venueFilterSql}
          AND (${requirePromotionEligible} = FALSE OR COALESCE((d.promotion_gate->>'eligible')::boolean, false) = TRUE)
        ORDER BY d.symbol ASC, d.strategy_id ASC, d.tune_id ASC
        LIMIT ${limit};
    `);

    return rows.map((row) => ({
        deploymentId: row.deploymentId,
        venue: resolveScalpDeploymentVenueFromId(row.deploymentId),
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        source: row.source as 'manual' | 'backtest' | 'matrix',
        enabled: Boolean(row.enabled),
        configOverride: asJsonObject(row.configOverride),
        promotionEligible: Boolean(row.promotionEligible),
        promotionReason: row.promotionReason,
        updatedAtMs: row.updatedAt instanceof Date ? row.updatedAt.getTime() : 0,
        updatedBy: row.updatedBy,
    }));
}

export async function listDeploymentsFromPg(params: {
    venue?: ScalpVenue;
    symbol?: string;
    strategyId?: string;
    tuneId?: string;
    deploymentId?: string;
    limit?: number;
} = {}): Promise<PgDeploymentRegistryRow[]> {
    const venue = params.venue ? normalizeScalpVenue(params.venue) : null;
    const symbol = String(params.symbol || '')
        .trim()
        .toUpperCase();
    const strategyId = String(params.strategyId || '')
        .trim()
        .toLowerCase();
    const tuneId = String(params.tuneId || '')
        .trim()
        .toLowerCase();
    const deploymentId = String(params.deploymentId || '').trim();
    const limit = Math.max(1, Math.min(5000, Math.floor(Number(params.limit) || 2000)));
    const venueFilterSql =
        venue === 'bitget'
            ? Prisma.sql`AND deployment_id LIKE 'bitget:%'`
            : venue === 'capital'
            ? Prisma.sql`AND (deployment_id NOT LIKE '%:%' OR deployment_id LIKE 'capital:%')`
            : Prisma.empty;

    const db = scalpPrisma();
    const rows = await db.$queryRaw<
        Array<{
            deploymentId: string;
            symbol: string;
            strategyId: string;
            tuneId: string;
            source: string;
            enabled: boolean;
            inUniverse: boolean;
            configOverride: unknown;
            promotionGate: unknown;
            createdAt: Date;
            updatedAt: Date;
            updatedBy: string | null;
        }>
    >(Prisma.sql`
        SELECT
            deployment_id AS "deploymentId",
            symbol,
            strategy_id AS "strategyId",
            tune_id AS "tuneId",
            source,
            enabled,
            in_universe AS "inUniverse",
            config_override AS "configOverride",
            promotion_gate AS "promotionGate",
            created_at AS "createdAt",
            updated_at AS "updatedAt",
            updated_by AS "updatedBy"
        FROM scalp_deployments
        WHERE (${symbol} = '' OR symbol = ${symbol})
          AND (${strategyId} = '' OR strategy_id = ${strategyId})
          AND (${tuneId} = '' OR tune_id = ${tuneId})
          AND (${deploymentId} = '' OR deployment_id = ${deploymentId})
          ${venueFilterSql}
        ORDER BY symbol ASC, strategy_id ASC, tune_id ASC
        LIMIT ${limit};
    `);

    return rows.map((row) => ({
        deploymentId: row.deploymentId,
        venue: resolveScalpDeploymentVenueFromId(row.deploymentId),
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        source: row.source as 'manual' | 'backtest' | 'matrix',
        enabled: Boolean(row.enabled),
        inUniverse: Boolean(row.inUniverse),
        configOverride: asJsonObject(row.configOverride),
        promotionGate: asJsonObject(row.promotionGate),
        createdAtMs: row.createdAt instanceof Date ? row.createdAt.getTime() : 0,
        updatedAtMs: row.updatedAt instanceof Date ? row.updatedAt.getTime() : 0,
        updatedBy: row.updatedBy,
    }));
}

export async function upsertDeploymentsBulkToPg(rows: PgUpsertDeploymentInput[]): Promise<number> {
    const payload = rows
        .map((row) => {
            const parsedDeploymentId = parseScalpVenuePrefixedDeploymentId(row.deploymentId);
            const venue = normalizeScalpVenue(
                row.venue ?? parsedDeploymentId.venue,
                parsedDeploymentId.venue,
            );
            const deploymentKey =
                parsedDeploymentId.deploymentKey || String(row.deploymentId || '').trim();
            return {
                deployment_id: formatScalpVenueDeploymentId(venue, deploymentKey),
            symbol: String(row.symbol || '').trim().toUpperCase(),
            strategy_id: String(row.strategyId || '').trim().toLowerCase(),
            tune_id: String(row.tuneId || '').trim().toLowerCase(),
            source: row.source,
            enabled: Boolean(row.enabled),
            config_override: row.configOverride && typeof row.configOverride === 'object' ? row.configOverride : {},
            promotion_gate: row.promotionGate && typeof row.promotionGate === 'object' ? row.promotionGate : null,
            updated_by: String(row.updatedBy || '').trim() || null,
            };
        })
        .filter((row) => row.deployment_id.length > 0 && row.symbol.length > 0 && row.strategy_id.length > 0 && row.tune_id.length > 0);

    if (!payload.length) return 0;

    const db = scalpPrisma();
    const payloadJson = JSON.stringify(payload);
    const updated = await db.$executeRaw(
        Prisma.sql`
        WITH input AS (
            SELECT *
            FROM jsonb_to_recordset(${payloadJson}::jsonb) AS x(
                deployment_id text,
                symbol text,
                strategy_id text,
                tune_id text,
                source text,
                enabled boolean,
                config_override jsonb,
                promotion_gate jsonb,
                updated_by text
            )
        )
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
        SELECT
            x.deployment_id,
            x.symbol,
            x.strategy_id,
            x.tune_id,
            x.source,
            x.enabled,
            COALESCE(x.config_override, '{}'::jsonb),
            x.promotion_gate,
            x.updated_by
        FROM input x
        ON CONFLICT(deployment_id)
        DO UPDATE SET
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

    return Number(updated || 0);
}

export async function deleteDeploymentsByIdFromPg(deploymentIds: string[]): Promise<number> {
    const ids = deploymentIds
        .map((row) => String(row || '').trim())
        .filter((row) => row.length > 0);
    if (!ids.length) return 0;

    const db = scalpPrisma();
    const updated = await db.$executeRaw(
        Prisma.sql`
        DELETE FROM scalp_deployments
        WHERE deployment_id IN (${Prisma.join(ids)});
        `,
    );

    return Number(updated || 0);
}
