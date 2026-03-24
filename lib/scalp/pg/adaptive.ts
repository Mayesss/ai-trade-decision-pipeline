import { normalizeScalpEntrySessionProfile } from '../sessions';
import type { ScalpEntrySessionProfile } from '../types';
import { scalpPrisma } from './client';
import { empty, join, sql } from './sql';
import type {
  ScalpAdaptiveDecisionRecord,
  ScalpAdaptiveSelectorSnapshotRecord,
  ScalpAdaptiveSnapshotCatalog,
  ScalpAdaptiveSnapshotMetrics,
  ScalpAdaptiveSnapshotStatus,
} from '../adaptive/types';

type SnapshotDbRow = {
  snapshotId: string;
  symbol: string;
  entrySessionProfile: string;
  strategyId: string;
  status: string;
  trainedAt: Date;
  windowFromTs: number;
  windowToTs: number;
  catalogJson: unknown;
  metricsJson: unknown;
  lockStartedAtMs: number | null;
  lockUntilMs: number | null;
  baselineMaxDrawdownR: number | null;
  updatedBy: string | null;
  createdAt: Date;
  updatedAt: Date;
};

function asObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
  return value as Record<string, unknown>;
}

function normalizeStatus(value: unknown): ScalpAdaptiveSnapshotStatus {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'active' || normalized === 'archived' || normalized === 'shadow') return normalized;
  return 'shadow';
}

function toSnapshotRecord(row: SnapshotDbRow): ScalpAdaptiveSelectorSnapshotRecord {
  const catalogObj = asObject(row.catalogJson) || {};
  const metricsObj = asObject(row.metricsJson) || {};
  return {
    snapshotId: row.snapshotId,
    symbol: row.symbol,
    entrySessionProfile: normalizeScalpEntrySessionProfile(row.entrySessionProfile, 'berlin'),
    strategyId: row.strategyId,
    status: normalizeStatus(row.status),
    trainedAtMs: row.trainedAt instanceof Date ? row.trainedAt.getTime() : 0,
    windowFromTs: Math.floor(Number(row.windowFromTs) || 0),
    windowToTs: Math.floor(Number(row.windowToTs) || 0),
    catalog: catalogObj as unknown as ScalpAdaptiveSnapshotCatalog,
    metrics: metricsObj as unknown as ScalpAdaptiveSnapshotMetrics,
    lockStartedAtMs: Number.isFinite(Number(row.lockStartedAtMs)) ? Number(row.lockStartedAtMs) : null,
    lockUntilMs: Number.isFinite(Number(row.lockUntilMs)) ? Number(row.lockUntilMs) : null,
    baselineMaxDrawdownR: Number.isFinite(Number(row.baselineMaxDrawdownR))
      ? Number(row.baselineMaxDrawdownR)
      : null,
    updatedBy: row.updatedBy,
    createdAtMs: row.createdAt instanceof Date ? row.createdAt.getTime() : 0,
    updatedAtMs: row.updatedAt instanceof Date ? row.updatedAt.getTime() : 0,
  };
}

export async function listScalpAdaptiveSelectorSnapshots(params: {
  symbol?: string;
  entrySessionProfile?: ScalpEntrySessionProfile;
  strategyId?: string;
  status?: ScalpAdaptiveSnapshotStatus | 'all';
  limit?: number;
} = {}): Promise<ScalpAdaptiveSelectorSnapshotRecord[]> {
  const symbol = String(params.symbol || '').trim().toUpperCase();
  const entrySessionProfile = String(params.entrySessionProfile || '').trim().toLowerCase();
  const strategyId = String(params.strategyId || '').trim().toLowerCase();
  const status = String(params.status || '').trim().toLowerCase();
  const limit = Math.max(1, Math.min(2000, Math.floor(Number(params.limit) || 200)));

  const statusFilter =
    status && status !== 'all'
      ? sql`AND status = ${status}`
      : empty;

  const db = scalpPrisma();
  const rows = await db.$queryRaw<SnapshotDbRow[]>(sql`
    SELECT
      snapshot_id AS "snapshotId",
      symbol,
      entry_session_profile AS "entrySessionProfile",
      strategy_id AS "strategyId",
      status,
      trained_at AS "trainedAt",
      window_from_ts AS "windowFromTs",
      window_to_ts AS "windowToTs",
      catalog_json AS "catalogJson",
      metrics_json AS "metricsJson",
      lock_started_at_ms AS "lockStartedAtMs",
      lock_until_ms AS "lockUntilMs",
      baseline_max_drawdown_r AS "baselineMaxDrawdownR",
      updated_by AS "updatedBy",
      created_at AS "createdAt",
      updated_at AS "updatedAt"
    FROM scalp_adaptive_selector_snapshots
    WHERE (${symbol} = '' OR symbol = ${symbol})
      AND (${entrySessionProfile} = '' OR entry_session_profile = ${entrySessionProfile})
      AND (${strategyId} = '' OR strategy_id = ${strategyId})
      ${statusFilter}
    ORDER BY trained_at DESC, snapshot_id DESC
    LIMIT ${limit};
  `);

  return rows.map(toSnapshotRecord);
}

export async function getScalpAdaptiveActiveSnapshot(params: {
  symbol: string;
  entrySessionProfile: ScalpEntrySessionProfile;
  strategyId: string;
}): Promise<ScalpAdaptiveSelectorSnapshotRecord | null> {
  const rows = await listScalpAdaptiveSelectorSnapshots({
    symbol: params.symbol,
    entrySessionProfile: params.entrySessionProfile,
    strategyId: params.strategyId,
    status: 'active',
    limit: 1,
  });
  return rows[0] || null;
}

export async function upsertScalpAdaptiveSelectorSnapshotsBulk(
  rows: Array<{
    snapshotId: string;
    symbol: string;
    entrySessionProfile: ScalpEntrySessionProfile;
    strategyId: string;
    status: ScalpAdaptiveSnapshotStatus;
    trainedAtMs: number;
    windowFromTs: number;
    windowToTs: number;
    catalog: ScalpAdaptiveSnapshotCatalog;
    metrics: ScalpAdaptiveSnapshotMetrics;
    lockStartedAtMs?: number | null;
    lockUntilMs?: number | null;
    baselineMaxDrawdownR?: number | null;
    updatedBy?: string | null;
  }>,
): Promise<number> {
  const payload = rows
    .map((row) => ({
      snapshot_id: String(row.snapshotId || '').trim(),
      symbol: String(row.symbol || '').trim().toUpperCase(),
      entry_session_profile: normalizeScalpEntrySessionProfile(row.entrySessionProfile, 'berlin'),
      strategy_id: String(row.strategyId || '').trim().toLowerCase(),
      status: normalizeStatus(row.status),
      trained_at: new Date(Math.floor(Number(row.trainedAtMs) || Date.now())).toISOString(),
      window_from_ts: Math.floor(Number(row.windowFromTs) || 0),
      window_to_ts: Math.floor(Number(row.windowToTs) || 0),
      catalog_json: row.catalog && typeof row.catalog === 'object' ? row.catalog : {},
      metrics_json: row.metrics && typeof row.metrics === 'object' ? row.metrics : {},
      lock_started_at_ms: Number.isFinite(Number(row.lockStartedAtMs))
        ? Math.floor(Number(row.lockStartedAtMs))
        : null,
      lock_until_ms: Number.isFinite(Number(row.lockUntilMs))
        ? Math.floor(Number(row.lockUntilMs))
        : null,
      baseline_max_drawdown_r: Number.isFinite(Number(row.baselineMaxDrawdownR))
        ? Number(row.baselineMaxDrawdownR)
        : null,
      updated_by: String(row.updatedBy || '').trim() || null,
    }))
    .filter((row) => row.snapshot_id && row.symbol && row.strategy_id && row.window_to_ts > row.window_from_ts);

  if (!payload.length) return 0;
  const payloadJson = JSON.stringify(payload);
  const db = scalpPrisma();
  return Number(
    await db.$executeRaw(sql`
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset(${payloadJson}::jsonb) AS x(
          snapshot_id text,
          symbol text,
          entry_session_profile text,
          strategy_id text,
          status text,
          trained_at timestamptz,
          window_from_ts bigint,
          window_to_ts bigint,
          catalog_json jsonb,
          metrics_json jsonb,
          lock_started_at_ms bigint,
          lock_until_ms bigint,
          baseline_max_drawdown_r numeric,
          updated_by text
        )
      )
      INSERT INTO scalp_adaptive_selector_snapshots(
        snapshot_id,
        symbol,
        entry_session_profile,
        strategy_id,
        status,
        trained_at,
        window_from_ts,
        window_to_ts,
        catalog_json,
        metrics_json,
        lock_started_at_ms,
        lock_until_ms,
        baseline_max_drawdown_r,
        updated_by
      )
      SELECT
        x.snapshot_id,
        x.symbol,
        x.entry_session_profile,
        x.strategy_id,
        x.status,
        x.trained_at,
        x.window_from_ts,
        x.window_to_ts,
        COALESCE(x.catalog_json, '{}'::jsonb),
        COALESCE(x.metrics_json, '{}'::jsonb),
        x.lock_started_at_ms,
        x.lock_until_ms,
        x.baseline_max_drawdown_r,
        x.updated_by
      FROM input x
      ON CONFLICT(snapshot_id)
      DO UPDATE SET
        symbol = EXCLUDED.symbol,
        entry_session_profile = EXCLUDED.entry_session_profile,
        strategy_id = EXCLUDED.strategy_id,
        status = EXCLUDED.status,
        trained_at = EXCLUDED.trained_at,
        window_from_ts = EXCLUDED.window_from_ts,
        window_to_ts = EXCLUDED.window_to_ts,
        catalog_json = EXCLUDED.catalog_json,
        metrics_json = EXCLUDED.metrics_json,
        lock_started_at_ms = EXCLUDED.lock_started_at_ms,
        lock_until_ms = EXCLUDED.lock_until_ms,
        baseline_max_drawdown_r = EXCLUDED.baseline_max_drawdown_r,
        updated_by = EXCLUDED.updated_by,
        updated_at = NOW();
    `),
  );
}

export async function activateScalpAdaptiveSnapshot(params: {
  snapshotId: string;
  symbol: string;
  entrySessionProfile: ScalpEntrySessionProfile;
  strategyId: string;
  lockStartedAtMs?: number | null;
  lockUntilMs?: number | null;
  baselineMaxDrawdownR?: number | null;
  updatedBy?: string | null;
}): Promise<void> {
  const snapshotId = String(params.snapshotId || '').trim();
  if (!snapshotId) return;
  const symbol = String(params.symbol || '').trim().toUpperCase();
  const strategyId = String(params.strategyId || '').trim().toLowerCase();
  const entrySessionProfile = normalizeScalpEntrySessionProfile(params.entrySessionProfile, 'berlin');
  const updatedBy = String(params.updatedBy || '').trim() || 'adaptive:promotion';
  const lockStartedAtMs = Number.isFinite(Number(params.lockStartedAtMs))
    ? Math.floor(Number(params.lockStartedAtMs))
    : null;
  const lockUntilMs = Number.isFinite(Number(params.lockUntilMs))
    ? Math.floor(Number(params.lockUntilMs))
    : null;
  const baselineMaxDrawdownR = Number.isFinite(Number(params.baselineMaxDrawdownR))
    ? Number(params.baselineMaxDrawdownR)
    : null;

  const db = scalpPrisma();
  await db.$transaction(async (tx) => {
    await tx.$executeRaw(
      sql`
        UPDATE scalp_adaptive_selector_snapshots
        SET
          status = 'archived',
          updated_by = ${updatedBy},
          updated_at = NOW()
        WHERE symbol = ${symbol}
          AND entry_session_profile = ${entrySessionProfile}
          AND strategy_id = ${strategyId}
          AND status = 'active'
          AND snapshot_id <> ${snapshotId};
      `,
    );
    await tx.$executeRaw(
      sql`
        UPDATE scalp_adaptive_selector_snapshots
        SET
          status = 'active',
          lock_started_at_ms = COALESCE(${lockStartedAtMs}, lock_started_at_ms),
          lock_until_ms = COALESCE(${lockUntilMs}, lock_until_ms),
          baseline_max_drawdown_r = COALESCE(${baselineMaxDrawdownR}, baseline_max_drawdown_r),
          updated_by = ${updatedBy},
          updated_at = NOW()
        WHERE snapshot_id = ${snapshotId};
      `,
    );
  });
}

export async function archiveScalpAdaptiveSnapshots(params: {
  symbol: string;
  entrySessionProfile: ScalpEntrySessionProfile;
  strategyId: string;
  exceptSnapshotIds?: string[];
  updatedBy?: string | null;
}): Promise<number> {
  const symbol = String(params.symbol || '').trim().toUpperCase();
  const strategyId = String(params.strategyId || '').trim().toLowerCase();
  const entrySessionProfile = normalizeScalpEntrySessionProfile(params.entrySessionProfile, 'berlin');
  const updatedBy = String(params.updatedBy || '').trim() || 'adaptive:archive';
  const exceptSnapshotIds = Array.isArray(params.exceptSnapshotIds)
    ? params.exceptSnapshotIds
        .map((row) => String(row || '').trim())
        .filter((row, idx, all) => row.length > 0 && all.indexOf(row) === idx)
    : [];

  const excludeSql =
    exceptSnapshotIds.length > 0
      ? sql`AND snapshot_id NOT IN (${join(exceptSnapshotIds)})`
      : empty;
  const db = scalpPrisma();
  return Number(
    await db.$executeRaw(sql`
      UPDATE scalp_adaptive_selector_snapshots
      SET
        status = 'archived',
        updated_by = ${updatedBy},
        updated_at = NOW()
      WHERE symbol = ${symbol}
        AND entry_session_profile = ${entrySessionProfile}
        AND strategy_id = ${strategyId}
        AND status <> 'archived'
        ${excludeSql};
    `),
  );
}

type DecisionDbRow = {
  id: number;
  ts: Date;
  deploymentId: string;
  symbol: string;
  strategyId: string;
  entrySessionProfile: string;
  snapshotId: string | null;
  selectedArmId: string | null;
  selectedArmType: string;
  confidence: number | null;
  skipReason: string | null;
  reasonCodes: string[] | null;
  featuresHash: string | null;
  detailsJson: unknown;
  createdAt: Date;
};

function toDecisionRecord(row: DecisionDbRow): ScalpAdaptiveDecisionRecord {
  return {
    id: Math.floor(Number(row.id) || 0),
    tsMs: row.ts instanceof Date ? row.ts.getTime() : 0,
    deploymentId: row.deploymentId,
    symbol: row.symbol,
    strategyId: row.strategyId,
    entrySessionProfile: normalizeScalpEntrySessionProfile(row.entrySessionProfile, 'berlin'),
    snapshotId: row.snapshotId,
    selectedArmId: row.selectedArmId,
    selectedArmType:
      row.selectedArmType === 'pattern' || row.selectedArmType === 'incumbent'
        ? row.selectedArmType
        : 'none',
    confidence: Number.isFinite(Number(row.confidence)) ? Number(row.confidence) : null,
    skipReason: row.skipReason,
    reasonCodes: Array.isArray(row.reasonCodes) ? row.reasonCodes : [],
    featuresHash: row.featuresHash,
    details: asObject(row.detailsJson),
    createdAtMs: row.createdAt instanceof Date ? row.createdAt.getTime() : 0,
  };
}

export async function appendScalpAdaptiveSelectorDecisions(
  rows: Array<{
    tsMs?: number;
    deploymentId: string;
    symbol: string;
    strategyId: string;
    entrySessionProfile: ScalpEntrySessionProfile;
    snapshotId?: string | null;
    selectedArmId?: string | null;
    selectedArmType: 'pattern' | 'incumbent' | 'none';
    confidence?: number | null;
    skipReason?: string | null;
    reasonCodes?: string[];
    featuresHash?: string | null;
    details?: Record<string, unknown> | null;
  }>,
): Promise<number> {
  const payload = rows
    .map((row) => ({
      ts: new Date(
        Number.isFinite(Number(row.tsMs)) ? Math.floor(Number(row.tsMs)) : Date.now(),
      ).toISOString(),
      deployment_id: String(row.deploymentId || '').trim(),
      symbol: String(row.symbol || '').trim().toUpperCase(),
      strategy_id: String(row.strategyId || '').trim().toLowerCase(),
      entry_session_profile: normalizeScalpEntrySessionProfile(row.entrySessionProfile, 'berlin'),
      snapshot_id: String(row.snapshotId || '').trim() || null,
      selected_arm_id: String(row.selectedArmId || '').trim() || null,
      selected_arm_type:
        row.selectedArmType === 'pattern' || row.selectedArmType === 'incumbent'
          ? row.selectedArmType
          : 'none',
      confidence: Number.isFinite(Number(row.confidence)) ? Number(row.confidence) : null,
      skip_reason: String(row.skipReason || '').trim() || null,
      reason_codes: Array.isArray(row.reasonCodes)
        ? Array.from(
            new Set(
              row.reasonCodes
                .map((code) => String(code || '').trim().toUpperCase())
                .filter((code) => code.length > 0),
            ),
          )
        : [],
      features_hash: String(row.featuresHash || '').trim() || null,
      details_json: row.details && typeof row.details === 'object' ? row.details : {},
    }))
    .filter((row) => row.deployment_id && row.symbol && row.strategy_id);

  if (!payload.length) return 0;
  const payloadJson = JSON.stringify(payload);
  const db = scalpPrisma();
  return Number(
    await db.$executeRaw(sql`
      WITH input AS (
        SELECT *
        FROM jsonb_to_recordset(${payloadJson}::jsonb) AS x(
          ts timestamptz,
          deployment_id text,
          symbol text,
          strategy_id text,
          entry_session_profile text,
          snapshot_id text,
          selected_arm_id text,
          selected_arm_type text,
          confidence numeric,
          skip_reason text,
          reason_codes text[],
          features_hash text,
          details_json jsonb
        )
      )
      INSERT INTO scalp_adaptive_selector_decisions(
        ts,
        deployment_id,
        symbol,
        strategy_id,
        entry_session_profile,
        snapshot_id,
        selected_arm_id,
        selected_arm_type,
        confidence,
        skip_reason,
        reason_codes,
        features_hash,
        details_json
      )
      SELECT
        x.ts,
        x.deployment_id,
        x.symbol,
        x.strategy_id,
        x.entry_session_profile,
        NULLIF(x.snapshot_id, ''),
        NULLIF(x.selected_arm_id, ''),
        x.selected_arm_type,
        x.confidence,
        x.skip_reason,
        COALESCE(x.reason_codes, '{}'),
        x.features_hash,
        COALESCE(x.details_json, '{}'::jsonb)
      FROM input x;
    `),
  );
}

export async function listScalpAdaptiveSelectorDecisions(params: {
  symbol?: string;
  entrySessionProfile?: ScalpEntrySessionProfile;
  strategyId?: string;
  hours?: number;
  limit?: number;
} = {}): Promise<ScalpAdaptiveDecisionRecord[]> {
  const symbol = String(params.symbol || '').trim().toUpperCase();
  const entrySessionProfile = String(params.entrySessionProfile || '').trim().toLowerCase();
  const strategyId = String(params.strategyId || '').trim().toLowerCase();
  const limit = Math.max(1, Math.min(5000, Math.floor(Number(params.limit) || 250)));
  const hours = Math.max(0, Math.floor(Number(params.hours) || 0));
  const since = hours > 0 ? new Date(Date.now() - hours * 60 * 60_000) : null;
  const sinceFilter = since ? sql`AND ts >= ${since}` : empty;

  const db = scalpPrisma();
  const rows = await db.$queryRaw<DecisionDbRow[]>(sql`
    SELECT
      id,
      ts,
      deployment_id AS "deploymentId",
      symbol,
      strategy_id AS "strategyId",
      entry_session_profile AS "entrySessionProfile",
      snapshot_id AS "snapshotId",
      selected_arm_id AS "selectedArmId",
      selected_arm_type AS "selectedArmType",
      confidence,
      skip_reason AS "skipReason",
      reason_codes AS "reasonCodes",
      features_hash AS "featuresHash",
      details_json AS "detailsJson",
      created_at AS "createdAt"
    FROM scalp_adaptive_selector_decisions
    WHERE (${symbol} = '' OR symbol = ${symbol})
      AND (${entrySessionProfile} = '' OR entry_session_profile = ${entrySessionProfile})
      AND (${strategyId} = '' OR strategy_id = ${strategyId})
      ${sinceFilter}
    ORDER BY ts DESC, id DESC
    LIMIT ${limit};
  `);
  return rows.map(toDecisionRecord);
}
