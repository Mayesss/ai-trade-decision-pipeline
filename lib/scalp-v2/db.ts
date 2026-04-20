import crypto from "crypto";

import { isScalpPgConfigured, join, scalpPrisma, sql } from "./pg";

import { applyScalpV2FixedSeedScope, getScalpV2RuntimeConfig } from "./config";
import {
  deriveCloseTypeFromReasonCodes,
  normalizeReasonCodes,
  toDeploymentId,
  toLedgerCloseTypeFromEvent,
} from "./logic";
import type { ScalpJournalEntry, ScalpSessionState } from "../scalp/types";
import type {
  ScalpV2Candidate,
  ScalpV2CandidateStatus,
  ScalpV2CloseType,
  ScalpV2Deployment,
  ScalpV2EventType,
  ScalpV2ExecutionEvent,
  ScalpV2JobKind,
  ScalpV2JobResult,
  ScalpV2JobStatus,
  ScalpV2LiveMode,
  ScalpV2ResearchCursor,
  ScalpV2ResearchHighlight,
  ScalpV2RuntimeConfig,
  ScalpV2RiskProfile,
  ScalpV2Session,
  ScalpV2SourceOfTruth,
  ScalpV2Venue,
  ScalpV2WorkerStageId,
  ScalpV2WorkerStageWeeklyMetrics,
} from "./types";

function toMs(value: unknown): number {
  if (value instanceof Date) return value.getTime();
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return Date.now();
  return Math.floor(n);
}

function toOptionalMs(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (value instanceof Date) return value.getTime();
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  return Math.floor(n);
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function toPositiveInt(value: unknown, fallback: number, max = 10_000): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(1, Math.min(max, n));
}

const SCALP_TABLE_NAME_PATTERN = /^scalp_[a-z0-9_]+$/;

async function scalpTableExists(tableName: string): Promise<boolean> {
  if (!isScalpPgConfigured()) return false;
  const normalized = String(tableName || "")
    .trim()
    .toLowerCase();
  if (!SCALP_TABLE_NAME_PATTERN.test(normalized)) return false;
  try {
    const db = scalpPrisma();
    const [row] = await db.$queryRaw<Array<{ exists: boolean }>>(sql`
      SELECT EXISTS(
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ANY(current_schemas(FALSE))
          AND table_name = ${normalized}
      ) AS "exists";
    `);
    return Boolean(row?.exists);
  } catch {
    return false;
  }
}

function resolveScalpV2JobLockStaleMinutes(): number {
  return Math.max(
    2,
    Math.min(
      120,
      toPositiveInt(process.env.SCALP_V2_JOB_LOCK_STALE_MINUTES, 10, 120),
    ),
  );
}

function normalizeVenue(value: unknown): ScalpV2Venue {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return normalized === "capital" ? "capital" : "bitget";
}

function normalizeSession(value: unknown): ScalpV2Session {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "tokyo") return "tokyo";
  if (normalized === "newyork") return "newyork";
  if (normalized === "pacific") return "pacific";
  if (normalized === "sydney") return "sydney";
  return "berlin";
}

function normalizeOptionalSession(value: unknown): ScalpV2Session | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "tokyo") return "tokyo";
  if (normalized === "berlin") return "berlin";
  if (normalized === "newyork") return "newyork";
  if (normalized === "pacific") return "pacific";
  if (normalized === "sydney") return "sydney";
  return null;
}

function normalizeOptionalVenue(value: unknown): ScalpV2Venue | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "bitget") return "bitget";
  if (normalized === "capital") return "capital";
  return null;
}

function normalizeLiveMode(value: unknown): ScalpV2LiveMode {
  return String(value || "").trim().toLowerCase() === "live" ? "live" : "shadow";
}

function normalizeResearchPhase(value: unknown): ScalpV2ResearchCursor["phase"] {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "score") return "score";
  if (normalized === "validate") return "validate";
  if (normalized === "promote") return "promote";
  return "scan";
}

function normalizeCandidateStatus(value: unknown): ScalpV2CandidateStatus {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "evaluated") return "evaluated";
  if (normalized === "promoted") return "promoted";
  if (normalized === "rejected") return "rejected";
  return "discovered";
}

function normalizeEventType(value: unknown): ScalpV2EventType {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "order_submitted") return "order_submitted";
  if (normalized === "order_rejected") return "order_rejected";
  if (normalized === "position_snapshot") return "position_snapshot";
  if (normalized === "fill") return "fill";
  if (normalized === "stop_loss") return "stop_loss";
  if (normalized === "liquidation") return "liquidation";
  if (normalized === "manual_close") return "manual_close";
  if (normalized === "reconcile_close") return "reconcile_close";
  return "position_snapshot";
}

function normalizeSourceOfTruth(value: unknown): ScalpV2SourceOfTruth {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "broker") return "broker";
  if (normalized === "reconciler") return "reconciler";
  if (normalized === "legacy_v1_import") return "legacy_v1_import";
  return "system";
}

function normalizeJournalType(value: unknown): ScalpJournalEntry["type"] {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "state") return "state";
  if (normalized === "risk") return "risk";
  if (normalized === "error") return "error";
  return "execution";
}

function normalizeRiskProfile(value: unknown): ScalpV2RiskProfile {
  const row = asRecord(value);
  const riskPerTradePct = Number(row.riskPerTradePct);
  const maxOpenPositionsPerSymbol = Number(row.maxOpenPositionsPerSymbol);
  const autoPauseDailyR = Number(row.autoPauseDailyR);
  const autoPause30dR = Number(row.autoPause30dR);
  return {
    riskPerTradePct: Number.isFinite(riskPerTradePct) ? riskPerTradePct : 0.35,
    maxOpenPositionsPerSymbol: Number.isFinite(maxOpenPositionsPerSymbol)
      ? Math.max(1, Math.floor(maxOpenPositionsPerSymbol))
      : 1,
    autoPauseDailyR: Number.isFinite(autoPauseDailyR) ? autoPauseDailyR : -3,
    autoPause30dR: Number.isFinite(autoPause30dR) ? autoPause30dR : -8,
  };
}

function parseRuntimeConfigRow(raw: unknown): ScalpV2RuntimeConfig | null {
  if (!raw || typeof raw !== "object") return null;
  const row = raw as Record<string, unknown>;
  const defaults = getScalpV2RuntimeConfig();
  const config = asRecord(row.configJson);

  const merged: ScalpV2RuntimeConfig = {
    ...defaults,
    ...config,
    budgets: {
      ...defaults.budgets,
      ...asRecord(config.budgets),
    },
    riskProfile: {
      ...defaults.riskProfile,
      ...asRecord(config.riskProfile),
    },
    seedSymbolsByVenue: defaults.seedSymbolsByVenue,
    seedLiveSymbolsByVenue: defaults.seedLiveSymbolsByVenue,
  };

  const runtime: ScalpV2RuntimeConfig = {
    ...merged,
    enabled: Boolean(merged.enabled),
    liveEnabled: Boolean(merged.liveEnabled),
    dryRunDefault: Boolean(merged.dryRunDefault),
  };
  return applyScalpV2FixedSeedScope(runtime);
}

export async function loadScalpV2RuntimeConfig(): Promise<ScalpV2RuntimeConfig> {
  const defaults = getScalpV2RuntimeConfig();
  if (!isScalpPgConfigured()) return defaults;
  try {
    const db = scalpPrisma();
    const rows = await db.$queryRaw<Array<{ configJson: unknown }>>(sql`
      SELECT config_json AS "configJson"
      FROM scalp_v2_runtime_config
      WHERE singleton = TRUE
      LIMIT 1;
    `);
    const parsed = parseRuntimeConfigRow(rows[0]);
    return parsed || defaults;
  } catch {
    return defaults;
  }
}

export async function upsertScalpV2RuntimeConfig(
  config: ScalpV2RuntimeConfig,
): Promise<ScalpV2RuntimeConfig> {
  if (!isScalpPgConfigured()) return config;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    INSERT INTO scalp_v2_runtime_config(singleton, config_json, updated_at)
    VALUES (TRUE, ${JSON.stringify(config)}::jsonb, NOW())
    ON CONFLICT(singleton)
    DO UPDATE SET
      config_json = EXCLUDED.config_json,
      updated_at = NOW();
  `);
  return loadScalpV2RuntimeConfig();
}

export async function claimScalpV2Job(params: {
  jobKind: ScalpV2JobKind;
  lockOwner: string;
}): Promise<boolean> {
  if (!isScalpPgConfigured()) return true;
  const db = scalpPrisma();
  const dedupeKey = `${params.jobKind}:singleton`;
  const staleLockMinutes = resolveScalpV2JobLockStaleMinutes();
  await db.$executeRaw(sql`
    INSERT INTO scalp_v2_jobs(
      job_kind,
      dedupe_key,
      payload,
      status,
      attempts,
      next_run_at,
      created_at,
      updated_at
    ) VALUES (
      ${params.jobKind},
      ${dedupeKey},
      '{}'::jsonb,
      'pending',
      0,
      NOW(),
      NOW(),
      NOW()
    )
    ON CONFLICT(job_kind, dedupe_key)
    DO NOTHING;
  `);
  const rows = await db.$queryRaw<Array<{ id: bigint }>>(sql`
    UPDATE scalp_v2_jobs
    SET
      payload = '{}'::jsonb,
      status = 'running',
      locked_by = ${params.lockOwner},
      locked_at = NOW(),
      attempts = attempts + 1,
      updated_at = NOW()
    WHERE job_kind = ${params.jobKind}
      AND dedupe_key = ${dedupeKey}
      AND (
        status <> 'running'
        OR locked_at < NOW() - (${staleLockMinutes} * INTERVAL '1 minute')
        OR locked_by = ${params.lockOwner}
      )
    RETURNING id;
  `);
  return rows.length > 0;
}

export async function heartbeatScalpV2Job(params: {
  jobKind: ScalpV2JobKind;
  lockOwner: string;
  details?: Record<string, unknown>;
}): Promise<boolean> {
  if (!isScalpPgConfigured()) return true;
  const db = scalpPrisma();
  const dedupeKey = `${params.jobKind}:singleton`;
  const rows = await db.$queryRaw<Array<{ id: bigint }>>(sql`
    UPDATE scalp_v2_jobs
    SET
      payload = COALESCE(payload, '{}'::jsonb) || ${JSON.stringify(params.details || {})}::jsonb,
      locked_at = NOW(),
      updated_at = NOW()
    WHERE job_kind = ${params.jobKind}
      AND dedupe_key = ${dedupeKey}
      AND status = 'running'
      AND locked_by = ${params.lockOwner}
    RETURNING id;
  `);
  return rows.length > 0;
}

export async function finalizeScalpV2Job(params: {
  jobKind: ScalpV2JobKind;
  lockOwner: string;
  ok: boolean;
  details?: Record<string, unknown>;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  const dedupeKey = `${params.jobKind}:singleton`;
  await db.$executeRaw(sql`
    UPDATE scalp_v2_jobs
    SET
      status = ${params.ok ? "succeeded" : "failed"},
      payload = COALESCE(payload, '{}'::jsonb) || ${JSON.stringify(params.details || {})}::jsonb,
      locked_by = NULL,
      locked_at = NULL,
      next_run_at = NOW(),
      updated_at = NOW()
    WHERE job_kind = ${params.jobKind}
      AND dedupe_key = ${dedupeKey}
      AND (locked_by = ${params.lockOwner} OR locked_by IS NULL);
  `);
}

const UPSERT_CANDIDATE_BATCH_SIZE = 500;

export async function upsertScalpV2Candidates(params: {
  rows: Array<{
    venue: ScalpV2Venue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: ScalpV2Session;
    score: number;
    status: ScalpV2CandidateStatus;
    reasonCodes?: string[];
    metadata?: Record<string, unknown>;
  }>;
}): Promise<number> {
  if (!isScalpPgConfigured() || params.rows.length === 0) return 0;
  const db = scalpPrisma();

  for (let offset = 0; offset < params.rows.length; offset += UPSERT_CANDIDATE_BATCH_SIZE) {
    const batch = params.rows.slice(offset, offset + UPSERT_CANDIDATE_BATCH_SIZE);
    const values = batch.map((row) =>
      sql`(
        ${row.venue},
        ${row.symbol},
        ${row.strategyId},
        ${row.tuneId},
        ${row.entrySessionProfile},
        ${Number.isFinite(row.score) ? row.score : 0},
        ${row.status},
        ${normalizeReasonCodes(row.reasonCodes || [])},
        ${JSON.stringify(row.metadata || {})}::jsonb,
        NOW(),
        NOW()
      )`,
    );

    await db.$executeRaw(sql`
      INSERT INTO scalp_v2_candidates(
        venue,
        symbol,
        strategy_id,
        tune_id,
        entry_session_profile,
        score,
        status,
        reason_codes,
        metadata_json,
        created_at,
        updated_at
      ) VALUES ${join(values, ",")}
      ON CONFLICT(venue, symbol, strategy_id, tune_id, entry_session_profile)
      DO UPDATE SET
        score = EXCLUDED.score,
        status = CASE
          WHEN EXCLUDED.status = 'discovered'
            AND scalp_v2_candidates.status IN ('evaluated', 'promoted', 'rejected')
            THEN scalp_v2_candidates.status
          ELSE EXCLUDED.status
        END,
        reason_codes = CASE
          WHEN EXCLUDED.status = 'discovered'
            AND scalp_v2_candidates.status IN ('evaluated', 'promoted', 'rejected')
            THEN scalp_v2_candidates.reason_codes
          ELSE EXCLUDED.reason_codes
        END,
        metadata_json = COALESCE(scalp_v2_candidates.metadata_json, '{}'::jsonb) || EXCLUDED.metadata_json,
        updated_at = NOW();
    `);
  }

  return params.rows.length;
}

export async function listScalpV2Candidates(params: {
  status?: ScalpV2CandidateStatus;
  venue?: ScalpV2Venue;
  session?: ScalpV2Session;
  symbols?: string[];
  limit?: number;
} = {}): Promise<ScalpV2Candidate[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(10_000, Math.floor(params.limit || 500)));
  const where: string[] = [];
  const values: unknown[] = [];
  if (params.status) {
    values.push(params.status);
    where.push(`c.status = $${values.length}`);
  }
  if (params.venue) {
    values.push(params.venue);
    where.push(`c.venue = $${values.length}`);
  }
  if (params.session) {
    values.push(params.session);
    where.push(`c.entry_session_profile = $${values.length}`);
  }
  if (params.symbols && params.symbols.length > 0) {
    values.push(params.symbols);
    where.push(`c.symbol = ANY($${values.length})`);
  }
  values.push(limit);
  const discoveredOnly = normalizeCandidateStatus(params.status) === "discovered";
  const orderBySql = discoveredOnly
    ? `ORDER BY c.score DESC, c.updated_at DESC`
    : `ORDER BY COALESCE((c.metadata_json->'worker'->'stageC'->>'netR')::double precision, -999) DESC, c.score DESC`;
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = await db.$queryRawUnsafe<
    Array<{
      id: number;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
      score: number;
      status: string;
      reasonCodes: string[];
      metadataJson: unknown;
      deploymentId: string | null;
      deploymentEnabled: boolean | null;
      createdAt: Date;
      updatedAt: Date;
    }>
  >(
    `
      SELECT
        c.id,
        c.venue,
        c.symbol,
        c.strategy_id AS "strategyId",
        c.tune_id AS "tuneId",
        c.entry_session_profile AS "entrySessionProfile",
        c.score::double precision AS score,
        c.status,
        c.reason_codes AS "reasonCodes",
        c.metadata_json AS "metadataJson",
        d.deployment_id AS "deploymentId",
        d.enabled AS "deploymentEnabled",
        c.created_at AS "createdAt",
        c.updated_at AS "updatedAt"
      FROM scalp_v2_candidates c
      LEFT JOIN scalp_v2_deployments d
        ON d.venue = c.venue
       AND d.symbol = c.symbol
       AND d.strategy_id = c.strategy_id
       AND d.tune_id = c.tune_id
       AND d.entry_session_profile = c.entry_session_profile
      ${whereSql}
      ${orderBySql}
      LIMIT $${values.length};
    `,
    ...values,
  );

  return rows.map((row) => ({
    id: Number(row.id),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    strategyId: String(row.strategyId || "").trim().toLowerCase(),
    tuneId: String(row.tuneId || "").trim().toLowerCase(),
    entrySessionProfile: normalizeSession(row.entrySessionProfile),
    score: Number.isFinite(Number(row.score)) ? Number(row.score) : 0,
    status: normalizeCandidateStatus(row.status),
    reasonCodes: normalizeReasonCodes(row.reasonCodes || []),
    metadata: asRecord(row.metadataJson),
    deploymentId: String(row.deploymentId || "").trim() || null,
    deploymentEnabled:
      typeof row.deploymentEnabled === "boolean"
        ? row.deploymentEnabled
        : null,
    createdAtMs: toMs(row.createdAt),
    updatedAtMs: toMs(row.updatedAt),
  }));
}

export async function paginateScalpV2Candidates(params: {
  session?: ScalpV2Session;
  venue?: ScalpV2Venue;
  status?: ScalpV2CandidateStatus;
  deploymentEnabled?: boolean | null;
  offset?: number;
  limit?: number;
}): Promise<{ rows: ScalpV2Candidate[]; total: number }> {
  if (!isScalpPgConfigured()) return { rows: [], total: 0 };
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(500, Math.floor(params.limit || 100)));
  const offset = Math.max(0, Math.floor(params.offset || 0));
  const where: string[] = [];
  const values: unknown[] = [];
  if (params.session) {
    values.push(params.session);
    where.push(`c.entry_session_profile = $${values.length}`);
  }
  if (params.venue) {
    values.push(params.venue);
    where.push(`c.venue = $${values.length}`);
  }
  if (params.status) {
    values.push(normalizeCandidateStatus(params.status));
    where.push(`c.status = $${values.length}`);
  }
  if (params.deploymentEnabled === true) {
    where.push(`d.enabled = TRUE`);
  } else if (params.deploymentEnabled === false) {
    where.push(`COALESCE(d.enabled, FALSE) = FALSE`);
  }
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const fromSql = `
    FROM scalp_v2_candidates c
    LEFT JOIN scalp_v2_deployments d
      ON d.venue = c.venue
     AND d.symbol = c.symbol
     AND d.strategy_id = c.strategy_id
     AND d.tune_id = c.tune_id
     AND d.entry_session_profile = c.entry_session_profile
  `;

  const [countRow] = await db.$queryRawUnsafe<Array<{ cnt: bigint }>>(
    `SELECT COUNT(*)::bigint AS cnt ${fromSql} ${whereSql}`,
    ...values,
  );
  const total = Number(countRow?.cnt || 0);

  values.push(limit, offset);
  const limitIdx = values.length - 1;
  const offsetIdx = values.length;

  const rows = await db.$queryRawUnsafe<
    Array<{
      id: number;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
      score: number;
      status: string;
      reasonCodes: string[];
      metadataJson: unknown;
      deploymentId: string | null;
      deploymentEnabled: boolean | null;
      createdAt: Date;
      updatedAt: Date;
    }>
  >(
    `
      SELECT
        c.id,
        c.venue,
        c.symbol,
        c.strategy_id AS "strategyId",
        c.tune_id AS "tuneId",
        c.entry_session_profile AS "entrySessionProfile",
        c.score::double precision AS score,
        c.status,
        c.reason_codes AS "reasonCodes",
        c.metadata_json AS "metadataJson",
        d.deployment_id AS "deploymentId",
        d.enabled AS "deploymentEnabled",
        c.created_at AS "createdAt",
        c.updated_at AS "updatedAt"
      ${fromSql}
      ${whereSql}
      ORDER BY COALESCE((c.metadata_json->'worker'->'stageC'->>'netR')::double precision, -999) DESC, c.score DESC
      LIMIT $${limitIdx} OFFSET $${offsetIdx};
    `,
    ...values,
  );

  return {
    total,
    rows: rows.map((row) => ({
      id: Number(row.id),
      venue: normalizeVenue(row.venue),
      symbol: String(row.symbol || "").trim().toUpperCase(),
      strategyId: String(row.strategyId || "").trim().toLowerCase(),
      tuneId: String(row.tuneId || "").trim().toLowerCase(),
      entrySessionProfile: normalizeSession(row.entrySessionProfile),
      score: Number.isFinite(Number(row.score)) ? Number(row.score) : 0,
      status: normalizeCandidateStatus(row.status),
      reasonCodes: normalizeReasonCodes(row.reasonCodes || []),
      metadata: asRecord(row.metadataJson),
      deploymentId: String(row.deploymentId || "").trim() || null,
      deploymentEnabled:
        typeof row.deploymentEnabled === "boolean"
          ? row.deploymentEnabled
          : null,
      createdAtMs: toMs(row.createdAt),
      updatedAtMs: toMs(row.updatedAt),
    })),
  };
}

export async function paginateScalpV2CandidatesForBackfill(params: {
  statuses: ScalpV2CandidateStatus[];
  symbols?: string[];
  session?: ScalpV2Session | null;
  venue?: ScalpV2Venue | null;
  offset?: number;
  limit?: number;
}): Promise<{ rows: ScalpV2Candidate[]; total: number }> {
  if (!isScalpPgConfigured()) return { rows: [], total: 0 };
  const db = scalpPrisma();
  const statuses = Array.from(
    new Set(
      (params.statuses || [])
        .map((row) => normalizeCandidateStatus(row))
        .filter(Boolean),
    ),
  );
  if (!statuses.length) return { rows: [], total: 0 };
  const symbols = Array.from(
    new Set(
      (params.symbols || [])
        .map((row) => String(row || "").trim().toUpperCase())
        .filter(Boolean),
    ),
  );
  const limit = Math.max(1, Math.min(2_000, Math.floor(params.limit || 100)));
  const offset = Math.max(0, Math.floor(params.offset || 0));
  const where: string[] = [];
  const values: unknown[] = [];
  values.push(statuses);
  where.push(`status = ANY($${values.length}::text[])`);
  if (params.venue) {
    values.push(params.venue);
    where.push(`venue = $${values.length}`);
  }
  if (params.session) {
    values.push(params.session);
    where.push(`entry_session_profile = $${values.length}`);
  }
  if (symbols.length) {
    values.push(symbols);
    where.push(`symbol = ANY($${values.length}::text[])`);
  }
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const [countRow] = await db.$queryRawUnsafe<Array<{ cnt: bigint }>>(
    `SELECT COUNT(*)::bigint AS cnt FROM scalp_v2_candidates ${whereSql}`,
    ...values,
  );
  const total = Number(countRow?.cnt || 0);

  values.push(limit, offset);
  const limitIdx = values.length - 1;
  const offsetIdx = values.length;
  const rows = await db.$queryRawUnsafe<
    Array<{
      id: number;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
      score: number;
      status: string;
      reasonCodes: string[];
      metadataJson: unknown;
      createdAt: Date;
      updatedAt: Date;
    }>
  >(
    `
      SELECT
        id,
        venue,
        symbol,
        strategy_id AS "strategyId",
        tune_id AS "tuneId",
        entry_session_profile AS "entrySessionProfile",
        score::double precision AS score,
        status,
        reason_codes AS "reasonCodes",
        metadata_json AS "metadataJson",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM scalp_v2_candidates
      ${whereSql}
      ORDER BY id ASC
      LIMIT $${limitIdx} OFFSET $${offsetIdx};
    `,
    ...values,
  );

  return {
    total,
    rows: rows.map((row) => ({
      id: Number(row.id),
      venue: normalizeVenue(row.venue),
      symbol: String(row.symbol || "").trim().toUpperCase(),
      strategyId: String(row.strategyId || "").trim().toLowerCase(),
      tuneId: String(row.tuneId || "").trim().toLowerCase(),
      entrySessionProfile: normalizeSession(row.entrySessionProfile),
      score: Number.isFinite(Number(row.score)) ? Number(row.score) : 0,
      status: normalizeCandidateStatus(row.status),
      reasonCodes: normalizeReasonCodes(row.reasonCodes || []),
      metadata: asRecord(row.metadataJson),
      createdAtMs: toMs(row.createdAt),
      updatedAtMs: toMs(row.updatedAt),
    })),
  };
}

/**
 * Returns a set of "venue:symbol:tuneId:session" keys for candidates
 * that were already backtested for the CURRENT windowToTs this week.
 * These are exact cache hits — no need to re-run at all.
 */
/** Check if warm-up was completed for the given window. */
export async function loadScalpV2WarmUpState(params: {
  windowToTs: number;
}): Promise<{ scopeHash: string; candidateCount: number } | null> {
  if (!isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ scopeHash: string; candidateCount: number }>
  >(sql`
    SELECT scope_hash AS "scopeHash", candidate_count AS "candidateCount"
    FROM scalp_v2_research_warm_up
    WHERE window_to_ts = ${String(params.windowToTs)}::bigint
    LIMIT 1
  `);
  return rows[0] || null;
}

/** Persist warm-up completion state. */
export async function upsertScalpV2WarmUpState(params: {
  windowToTs: number;
  scopeHash: string;
  candidateCount: number;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    INSERT INTO scalp_v2_research_warm_up(window_to_ts, scope_hash, candidate_count, created_at)
    VALUES (${String(params.windowToTs)}::bigint, ${params.scopeHash}, ${params.candidateCount}, NOW())
    ON CONFLICT(window_to_ts)
    DO UPDATE SET scope_hash = EXCLUDED.scope_hash, candidate_count = EXCLUDED.candidate_count
  `);
}

/** Distinct symbols that still have "discovered" candidates (not yet backtested). */
export async function listScalpV2DiscoveredSymbols(): Promise<string[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ symbol: string }>>(sql`
    SELECT DISTINCT symbol FROM scalp_v2_candidates
    WHERE status = 'discovered'
    ORDER BY symbol
  `);
  return rows.map((r) => r.symbol);
}

export async function countScalpV2CandidatesByStatus(params: {
  status: ScalpV2CandidateStatus;
  symbols?: string[];
}): Promise<number> {
  if (!isScalpPgConfigured()) return 0;
  const db = scalpPrisma();
  const status = normalizeCandidateStatus(params.status);
  const symbols = Array.from(
    new Set(
      (params.symbols || [])
        .map((row) => String(row || "").trim().toUpperCase())
        .filter(Boolean),
    ),
  );
  if (symbols.length > 0) {
    const [row] = await db.$queryRaw<Array<{ cnt: bigint | number }>>(sql`
      SELECT COUNT(*)::bigint AS cnt
      FROM scalp_v2_candidates
      WHERE status = ${status}
        AND symbol IN (${join(symbols)});
    `);
    return Math.max(0, Math.floor(Number(row?.cnt || 0)));
  }
  const [row] = await db.$queryRaw<Array<{ cnt: bigint | number }>>(sql`
    SELECT COUNT(*)::bigint AS cnt
    FROM scalp_v2_candidates
    WHERE status = ${status};
  `);
  return Math.max(0, Math.floor(Number(row?.cnt || 0)));
}

export async function loadScalpV2EvaluatedCandidateKeys(params: {
  windowToTs: number;
}): Promise<Set<string>> {
  if (!isScalpPgConfigured()) return new Set();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{ venue: string; symbol: string; tuneId: string; session: string }>
  >(sql`
    SELECT
      venue,
      symbol,
      tune_id AS "tuneId",
      entry_session_profile AS "session"
    FROM scalp_v2_candidates
    WHERE status IN ('evaluated', 'promoted', 'rejected')
      AND (metadata_json->'worker'->>'windowToTs')::bigint = ${params.windowToTs}
  `);
  const keys = new Set<string>();
  for (const row of rows) {
    keys.add(
      `${row.venue}:${row.symbol}:${row.tuneId}:${row.session}`.toLowerCase(),
    );
  }
  return keys;
}

/** Previous week's stage results for smart-skip decisions. */
export interface PreviousStageResult {
  windowToTs: number;
  stageAPassed: boolean;
  stageANetR: number | null;
  stageATrades: number | null;
  stageCPassed: boolean;
  stageCNetR: number | null;
  /** Per-week netR from stage A — used by the weeklyNetR pre-filter. */
  stageAWeeklyNetR: Record<string, number>;
}

export interface ScalpV2ScopeWindowStageStats {
  windowToTs: number;
  venue: ScalpV2Venue;
  symbol: string;
  session: ScalpV2Session;
  total: number;
  stageAPass: number;
  stageCPass: number;
}

/**
 * Loads previous week's backtest results for candidates evaluated with a
 * DIFFERENT windowToTs (prior week). Used for smart-skip decisions and
 * the weeklyNetR pre-filter. Only fetches the fields actually needed.
 */
export async function loadScalpV2PreviousWeekResults(params: {
  currentWindowToTs: number;
  symbols?: string[];
  tuneIds?: string[];
}): Promise<Map<string, PreviousStageResult>> {
  if (!isScalpPgConfigured()) return new Map();
  const db = scalpPrisma();
  const symbols = Array.from(
    new Set(
      (params.symbols || [])
        .map((row) => String(row || "").trim().toUpperCase())
        .filter(Boolean),
    ),
  );
  const symbolFilter =
    symbols.length > 0 ? sql`AND symbol IN (${join(symbols)})` : sql``;
  const tuneIds = Array.from(
    new Set(
      (params.tuneIds || [])
        .map((row) => String(row || "").trim().toLowerCase())
        .filter(Boolean),
    ),
  );
  const tuneFilter =
    tuneIds.length > 0 ? sql`AND tune_id IN (${join(tuneIds)})` : sql``;
  const rows = await db.$queryRaw<
    Array<{
      venue: string;
      symbol: string;
      tuneId: string;
      session: string;
      windowToTs: string;
      stageAPassed: string | null;
      stageANetR: string | null;
      stageATrades: string | null;
      stageCPassed: string | null;
      stageCNetR: string | null;
      stageAWeeklyNetR: unknown;
    }>
  >(sql`
    WITH ranked AS (
      SELECT
        venue,
        symbol,
        tune_id AS "tuneId",
        entry_session_profile AS "session",
        (metadata_json->'worker'->>'windowToTs') AS "windowToTs",
        (metadata_json->'worker'->'stageA'->>'passed') AS "stageAPassed",
        (metadata_json->'worker'->'stageA'->>'netR') AS "stageANetR",
        (metadata_json->'worker'->'stageA'->>'trades') AS "stageATrades",
        (metadata_json->'worker'->'stageC'->>'passed') AS "stageCPassed",
        (metadata_json->'worker'->'stageC'->>'netR') AS "stageCNetR",
        (metadata_json->'worker'->'stageA'->'weeklyNetR') AS "stageAWeeklyNetR",
        ROW_NUMBER() OVER (
          PARTITION BY venue, symbol, tune_id, entry_session_profile
          ORDER BY (metadata_json->'worker'->>'windowToTs')::bigint DESC, updated_at DESC
        ) AS rn
      FROM scalp_v2_candidates
      WHERE status IN ('evaluated', 'promoted', 'rejected')
        AND (metadata_json->'worker'->>'windowToTs')::bigint != ${params.currentWindowToTs}
        AND metadata_json->'worker'->'stageA' IS NOT NULL
        ${symbolFilter}
        ${tuneFilter}
    )
    SELECT
      venue,
      symbol,
      "tuneId",
      "session",
      "windowToTs",
      "stageAPassed",
      "stageANetR",
      "stageATrades",
      "stageCPassed",
      "stageCNetR",
      "stageAWeeklyNetR"
    FROM ranked
    WHERE rn = 1
  `);
  const results = new Map<string, PreviousStageResult>();
  for (const row of rows) {
    const key = `${row.venue}:${row.symbol}:${row.tuneId}:${row.session}`.toLowerCase();
    // Extract weeklyNetR from the JSON object
    const weeklyNetR: Record<string, number> = {};
    const raw = asRecord(row.stageAWeeklyNetR);
    for (const [k, v] of Object.entries(raw)) {
      const n = Number(v);
      if (Number.isFinite(n)) weeklyNetR[String(k)] = n;
    }
    results.set(key, {
      windowToTs: Number(row.windowToTs) || 0,
      stageAPassed: row.stageAPassed === "true",
      stageANetR: row.stageANetR !== null ? Number(row.stageANetR) : null,
      stageATrades: row.stageATrades !== null ? Number(row.stageATrades) : null,
      stageCPassed: row.stageCPassed === "true",
      stageCNetR: row.stageCNetR !== null ? Number(row.stageCNetR) : null,
      stageAWeeklyNetR: weeklyNetR,
    });
  }
  return results;
}

// ---------------------------------------------------------------------------
// Worker stage weekly cache — stores full per-week metrics so subsequent
// weeks only need to replay the newest week slice.
// ---------------------------------------------------------------------------

const WEEKLY_CACHE_TABLE = "scalp_v2_worker_stage_weekly_cache";
let weeklyCacheTableExists: boolean | null = null;

async function ensureWeeklyCacheTable(): Promise<boolean> {
  if (weeklyCacheTableExists !== null) return weeklyCacheTableExists;
  const exists = await scalpTableExists(WEEKLY_CACHE_TABLE);
  weeklyCacheTableExists = exists;
  return exists;
}

function normalizeStageId(value: unknown): ScalpV2WorkerStageId | null {
  const s = String(value || "").trim().toLowerCase();
  if (s === "a" || s === "b" || s === "c") return s;
  return null;
}

function normalizeWeeklyCacheMetrics(
  value: unknown,
): ScalpV2WorkerStageWeeklyMetrics {
  const r = asRecord(value);
  const fin = (v: unknown, fb = 0) => { const n = Number(v); return Number.isFinite(n) ? n : fb; };
  const nat = (v: unknown) => Math.max(0, Math.floor(fin(v)));
  return {
    trades: nat(r.trades),
    wins: nat(r.wins),
    netR: fin(r.netR),
    grossProfitR: Math.max(0, fin(r.grossProfitR)),
    grossLossR: Math.min(0, fin(r.grossLossR)),
    maxDrawdownR: Math.max(0, fin(r.maxDrawdownR)),
    maxPrefixR: fin(r.maxPrefixR),
    minPrefixR: fin(r.minPrefixR),
    largestTradeR: Math.max(0, fin(r.largestTradeR)),
    exitStop: nat(r.exitStop),
    exitTp: nat(r.exitTp),
    exitTimeStop: nat(r.exitTimeStop),
    exitForceClose: nat(r.exitForceClose),
  };
}

/**
 * Load cached per-week metrics for a set of candidate+stage keys.
 * Returns Map<cacheKey, Map<weekStartTs, metrics>>.
 */
export async function loadScalpV2WeeklyCache(params: {
  keys: Array<{
    venue: ScalpV2Venue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    session: ScalpV2Session;
    stageId: ScalpV2WorkerStageId;
  }>;
  fromWeekStartTs: number;
  toWeekStartTs: number;
}): Promise<Map<string, Map<number, ScalpV2WorkerStageWeeklyMetrics>>> {
  const out = new Map<string, Map<number, ScalpV2WorkerStageWeeklyMetrics>>();
  if (!isScalpPgConfigured() || !params.keys.length) return out;
  if (!(await ensureWeeklyCacheTable())) return out;
  const fromTs = Math.floor(Number(params.fromWeekStartTs) || 0);
  const toTs = Math.floor(Number(params.toWeekStartTs) || 0);
  if (toTs <= fromTs) return out;

  const db = scalpPrisma();

  // Fast-path: skip batched key queries if the cache table is empty
  const [{ cnt }] = await db.$queryRaw<[{ cnt: bigint }]>(sql`
    SELECT count(*) AS cnt FROM scalp_v2_worker_stage_weekly_cache LIMIT 1
  `);
  if (Number(cnt) === 0) return out;

  const BATCH = 1500;
  for (let offset = 0; offset < params.keys.length; offset += BATCH) {
    const batch = params.keys.slice(offset, offset + BATCH);
    if (!batch.length) continue;
    const keyRows = batch.map((k) =>
      sql`(
        ${normalizeVenue(k.venue)},
        ${String(k.symbol || "").trim().toUpperCase()},
        ${String(k.strategyId || "").trim().toLowerCase()},
        ${String(k.tuneId || "").trim().toLowerCase()},
        ${normalizeSession(k.session)},
        ${normalizeStageId(k.stageId) || "a"}
      )`,
    );
    const rows = await db.$queryRaw<
      Array<{
        venue: string;
        symbol: string;
        strategyId: string;
        tuneId: string;
        session: string;
        stageId: string;
        weekStartTs: string | number;
        metricsJson: unknown;
      }>
    >(sql`
      WITH keys(venue, symbol, strategy_id, tune_id, entry_session_profile, stage_id) AS (
        VALUES ${join(keyRows, ",")}
      )
      SELECT
        c.venue,
        c.symbol,
        c.strategy_id AS "strategyId",
        c.tune_id AS "tuneId",
        c.entry_session_profile AS "session",
        c.stage_id AS "stageId",
        c.week_start_ts AS "weekStartTs",
        c.metrics_json AS "metricsJson"
      FROM scalp_v2_worker_stage_weekly_cache c
      INNER JOIN keys k
        ON c.venue = k.venue
       AND c.symbol = k.symbol
       AND c.strategy_id = k.strategy_id
       AND c.tune_id = k.tune_id
       AND c.entry_session_profile = k.entry_session_profile
       AND c.stage_id = k.stage_id
      WHERE c.week_start_ts >= ${fromTs}
        AND c.week_start_ts < ${toTs}
      ORDER BY c.week_start_ts ASC;
    `);
    for (const row of rows) {
      const sid = normalizeStageId(row.stageId);
      if (!sid) continue;
      const key = `${normalizeVenue(row.venue)}:${String(row.symbol || "").trim().toUpperCase()}:${String(row.strategyId || "").trim().toLowerCase()}:${String(row.tuneId || "").trim().toLowerCase()}:${normalizeSession(row.session)}:${sid}`;
      const weekStart = Math.floor(Number(row.weekStartTs) || 0);
      if (weekStart <= 0) continue;
      let perWeek = out.get(key);
      if (!perWeek) { perWeek = new Map(); out.set(key, perWeek); }
      perWeek.set(weekStart, normalizeWeeklyCacheMetrics(row.metricsJson));
    }
  }
  return out;
}

/**
 * Persist per-week metrics to the cache table (upsert).
 */
export async function upsertScalpV2WeeklyCache(params: {
  rows: Array<{
    venue: ScalpV2Venue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    session: ScalpV2Session;
    stageId: ScalpV2WorkerStageId;
    weekStartTs: number;
    weekToTs: number;
    metrics: ScalpV2WorkerStageWeeklyMetrics;
  }>;
}): Promise<number> {
  if (!isScalpPgConfigured() || !params.rows.length) return 0;
  if (!(await ensureWeeklyCacheTable())) return 0;
  const db = scalpPrisma();
  let written = 0;
  const BATCH = 600;
  for (let offset = 0; offset < params.rows.length; offset += BATCH) {
    const batch = params.rows.slice(offset, offset + BATCH);
    if (!batch.length) continue;
    const values = batch.map((r) =>
      sql`(
        ${normalizeVenue(r.venue)},
        ${String(r.symbol || "").trim().toUpperCase()},
        ${String(r.strategyId || "").trim().toLowerCase()},
        ${String(r.tuneId || "").trim().toLowerCase()},
        ${normalizeSession(r.session)},
        ${normalizeStageId(r.stageId) || "a"},
        ${Math.floor(Number(r.weekStartTs) || 0)},
        ${Math.floor(Number(r.weekToTs) || 0)},
        ${JSON.stringify(normalizeWeeklyCacheMetrics(r.metrics))}::jsonb,
        NOW()
      )`,
    );
    await db.$executeRaw(sql`
      INSERT INTO scalp_v2_worker_stage_weekly_cache(
        venue, symbol, strategy_id, tune_id, entry_session_profile,
        stage_id, week_start_ts, week_to_ts, metrics_json, updated_at
      ) VALUES ${join(values, ",")}
      ON CONFLICT(venue, symbol, strategy_id, tune_id, entry_session_profile, stage_id, week_start_ts)
      DO UPDATE SET
        week_to_ts = EXCLUDED.week_to_ts,
        metrics_json = EXCLUDED.metrics_json,
        updated_at = NOW();
    `);
    written += batch.length;
  }
  return written;
}

/**
 * Delete cache rows with week_start_ts older than the given timestamp.
 */
export async function pruneScalpV2WeeklyCache(params: {
  olderThanTs: number;
}): Promise<number> {
  if (!isScalpPgConfigured()) return 0;
  if (!(await ensureWeeklyCacheTable())) return 0;
  const db = scalpPrisma();
  const ts = Math.floor(Number(params.olderThanTs) || 0);
  if (ts <= 0) return 0;
  const result = await db.$executeRaw(sql`
    DELETE FROM scalp_v2_worker_stage_weekly_cache
    WHERE week_start_ts < ${ts}
  `);
  return typeof result === "number" ? result : 0;
}

export async function loadScalpV2ScopeWindowStageStats(params: {
  latestWindowCount?: number;
} = {}): Promise<ScalpV2ScopeWindowStageStats[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const latestWindowCount = Math.max(
    2,
    Math.min(8, toPositiveInt(params.latestWindowCount, 2, 8)),
  );
  const rows = await db.$queryRaw<
    Array<{
      windowToTs: string | number;
      venue: string;
      symbol: string;
      session: string;
      total: string | number;
      stageAPass: string | number;
      stageCPass: string | number;
    }>
  >(sql`
    WITH windows AS (
      SELECT DISTINCT
        (metadata_json->'worker'->>'windowToTs')::bigint AS window_to_ts
      FROM scalp_v2_candidates
      WHERE metadata_json ? 'worker'
        AND metadata_json->'worker'->>'windowToTs' IS NOT NULL
      ORDER BY window_to_ts DESC
      LIMIT ${latestWindowCount}
    )
    SELECT
      (metadata_json->'worker'->>'windowToTs')::bigint AS "windowToTs",
      venue,
      symbol,
      entry_session_profile AS "session",
      COUNT(*)::bigint AS total,
      COUNT(*) FILTER (
        WHERE COALESCE((metadata_json->'worker'->'stageA'->>'passed')::boolean, false)
      )::bigint AS "stageAPass",
      COUNT(*) FILTER (
        WHERE COALESCE((metadata_json->'worker'->'stageC'->>'passed')::boolean, false)
      )::bigint AS "stageCPass"
    FROM scalp_v2_candidates
    WHERE (metadata_json->'worker'->>'windowToTs')::bigint IN (
      SELECT window_to_ts FROM windows
    )
    GROUP BY 1, 2, 3, 4
    ORDER BY "windowToTs" DESC, venue, symbol, "session";
  `);

  return rows.map((row) => ({
    windowToTs: Number(row.windowToTs) || 0,
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    session: normalizeSession(row.session),
    total: Number(row.total) || 0,
    stageAPass: Number(row.stageAPass) || 0,
    stageCPass: Number(row.stageCPass) || 0,
  }));
}

export async function updateScalpV2CandidateStatuses(params: {
  ids: number[];
  status: ScalpV2CandidateStatus;
  metadataPatch?: Record<string, unknown>;
}): Promise<number> {
  if (!isScalpPgConfigured() || params.ids.length === 0) return 0;
  const db = scalpPrisma();
  const ids = Array.from(new Set(params.ids.map((id) => Math.floor(id)).filter((id) => id > 0)));
  if (!ids.length) return 0;

  if (params.metadataPatch && Object.keys(params.metadataPatch).length > 0) {
    await db.$executeRaw(sql`
      UPDATE scalp_v2_candidates
      SET
        status = ${params.status},
        metadata_json = COALESCE(metadata_json, '{}'::jsonb) || ${JSON.stringify(params.metadataPatch)}::jsonb,
        updated_at = NOW()
      WHERE id = ANY(${ids}::int[]);
    `);
  } else {
    await db.$executeRaw(sql`
      UPDATE scalp_v2_candidates
      SET
        status = ${params.status},
        updated_at = NOW()
      WHERE id = ANY(${ids}::int[]);
    `);
  }
  return ids.length;
}

const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * Requeue deployment-linked candidates for weekly rollover backtesting.
 *
 * This moves stale deployment candidates back to "discovered" so research
 * can re-evaluate them for the current completed week window.
 */
export async function requeueScalpV2DeploymentCandidatesForWindow(params: {
  windowToTs: number;
  previousWindowOnly?: boolean;
  includeDisabledDeployments?: boolean;
  reasonCode?: string;
}): Promise<number> {
  if (!isScalpPgConfigured()) return 0;
  const db = scalpPrisma();
  const windowToTs = Math.floor(Number(params.windowToTs) || 0);
  if (windowToTs <= 0) return 0;
  const previousWindowOnly = params.previousWindowOnly !== false;
  const includeDisabledDeployments = params.includeDisabledDeployments !== false;
  const previousWindowToTs = windowToTs - ONE_WEEK_MS;
  const reasonCode = String(
    params.reasonCode || "SCALP_V2_REQUEUE_DEPLOYMENT_WINDOW_ROLLOVER",
  )
    .trim()
    .toUpperCase();
  const nowTs = Date.now();

  const enabledFilter = includeDisabledDeployments
    ? sql``
    : sql`AND d.enabled = TRUE`;
  const previousWindowFilter = previousWindowOnly
    ? sql`
        AND (
          (c.metadata_json->'worker'->>'windowToTs') IS NULL
          OR (c.metadata_json->'worker'->>'windowToTs')::bigint = ${previousWindowToTs}
        )
      `
    : sql``;

  const rows = await db.$queryRaw<Array<{ id: bigint | number }>>(sql`
    WITH target AS (
      SELECT c.id
      FROM scalp_v2_candidates c
      INNER JOIN scalp_v2_deployments d
        ON d.candidate_id = c.id
      WHERE c.status <> 'discovered'
        AND (
          (c.metadata_json->'worker'->>'windowToTs') IS NULL
          OR (c.metadata_json->'worker'->>'windowToTs')::bigint <> ${windowToTs}
        )
        ${previousWindowFilter}
        ${enabledFilter}
    )
    UPDATE scalp_v2_candidates c
    SET
      status = 'discovered',
      reason_codes = (
        SELECT ARRAY(
          SELECT DISTINCT x
          FROM unnest(
            COALESCE(c.reason_codes, '{}'::text[]) || ARRAY[${reasonCode}]::text[]
          ) AS x
        )
      ),
      metadata_json = COALESCE(c.metadata_json, '{}'::jsonb) || jsonb_build_object(
        'requeue',
        jsonb_build_object(
          'triggeredAtMs',
          ${nowTs}::bigint,
          'trigger',
          'deployment_window_rollover',
          'windowToTs',
          ${windowToTs}::bigint,
          'previousWindowOnly',
          (${previousWindowOnly})::boolean,
          'includeDisabledDeployments',
          (${includeDisabledDeployments})::boolean
        )
      ),
      updated_at = NOW()
    FROM target t
    WHERE c.id = t.id
    RETURNING c.id;
  `);

  return rows.length;
}

const UPSERT_DEPLOYMENT_BATCH_SIZE = 400;

export async function upsertScalpV2Deployments(params: {
  rows: Array<{
    candidateId: number | null;
    venue: ScalpV2Venue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: ScalpV2Session;
    enabled: boolean;
    liveMode: ScalpV2LiveMode;
    promotionGate?: Record<string, unknown>;
    riskProfile: ScalpV2RiskProfile;
  }>;
}): Promise<number> {
  if (!isScalpPgConfigured() || params.rows.length === 0) return 0;
  const db = scalpPrisma();

  for (let offset = 0; offset < params.rows.length; offset += UPSERT_DEPLOYMENT_BATCH_SIZE) {
    const batch = params.rows.slice(offset, offset + UPSERT_DEPLOYMENT_BATCH_SIZE);
    const values = batch.map((row) => {
      const deploymentId = toDeploymentId({
        venue: row.venue,
        symbol: row.symbol,
        strategyId: row.strategyId,
        tuneId: row.tuneId,
        session: row.entrySessionProfile,
      });
      return sql`(
        ${deploymentId},
        ${row.candidateId},
        ${row.venue},
        ${row.symbol},
        ${row.strategyId},
        ${row.tuneId},
        ${row.entrySessionProfile},
        ${row.enabled},
        ${row.liveMode},
        ${JSON.stringify(row.promotionGate || {})}::jsonb,
        ${JSON.stringify(row.riskProfile || {})}::jsonb,
        NOW(),
        NOW(),
        CASE WHEN ${row.enabled} THEN NOW() ELSE NULL END
      )`;
    });

    await db.$executeRaw(sql`
      INSERT INTO scalp_v2_deployments(
        deployment_id,
        candidate_id,
        venue,
        symbol,
        strategy_id,
        tune_id,
        entry_session_profile,
        enabled,
        live_mode,
        promotion_gate,
        risk_profile,
        created_at,
        updated_at,
        last_promoted_at
      ) VALUES ${join(values, ",")}
      ON CONFLICT(deployment_id)
      DO UPDATE SET
        candidate_id = EXCLUDED.candidate_id,
        enabled = EXCLUDED.enabled,
        live_mode = EXCLUDED.live_mode,
        promotion_gate = EXCLUDED.promotion_gate,
        risk_profile = EXCLUDED.risk_profile,
        updated_at = NOW(),
        last_promoted_at = CASE WHEN EXCLUDED.enabled THEN NOW() ELSE scalp_v2_deployments.last_promoted_at END;
    `);
  }

  return params.rows.length;
}

export async function listScalpV2Deployments(params: {
  enabledOnly?: boolean;
  liveOnly?: boolean;
  venue?: ScalpV2Venue;
  session?: ScalpV2Session;
  limit?: number;
} = {}): Promise<ScalpV2Deployment[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(10_000, Math.floor(params.limit || 500)));
  const where: string[] = [];
  const values: unknown[] = [];

  if (params.enabledOnly) {
    values.push(true);
    where.push(`enabled = $${values.length}`);
  }
  if (params.liveOnly) {
    values.push("live");
    where.push(`live_mode = $${values.length}`);
  }
  if (params.venue) {
    values.push(params.venue);
    where.push(`venue = $${values.length}`);
  }
  if (params.session) {
    values.push(params.session);
    where.push(`entry_session_profile = $${values.length}`);
  }

  values.push(limit);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const rows = await db.$queryRawUnsafe<
    Array<{
      deploymentId: string;
      candidateId: number | null;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
      enabled: boolean;
      liveMode: string;
      promotionGate: unknown;
      riskProfile: unknown;
      createdAt: Date;
      updatedAt: Date;
    }>
  >(
    `
      SELECT
        deployment_id AS "deploymentId",
        candidate_id AS "candidateId",
        venue,
        symbol,
        strategy_id AS "strategyId",
        tune_id AS "tuneId",
        entry_session_profile AS "entrySessionProfile",
        enabled,
        live_mode AS "liveMode",
        promotion_gate AS "promotionGate",
        risk_profile AS "riskProfile",
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM scalp_v2_deployments
      ${whereSql}
      ORDER BY enabled DESC, updated_at DESC
      LIMIT $${values.length};
    `,
    ...values,
  );

  return rows.map((row) => ({
    deploymentId: String(row.deploymentId || "").trim(),
    candidateId:
      row.candidateId === null || row.candidateId === undefined
        ? null
        : Number(row.candidateId),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    strategyId: String(row.strategyId || "").trim().toLowerCase(),
    tuneId: String(row.tuneId || "").trim().toLowerCase(),
    entrySessionProfile: normalizeSession(row.entrySessionProfile),
    enabled: Boolean(row.enabled),
    liveMode: normalizeLiveMode(row.liveMode),
    promotionGate: asRecord(row.promotionGate),
    riskProfile: normalizeRiskProfile(row.riskProfile),
    createdAtMs: toMs(row.createdAt),
    updatedAtMs: toMs(row.updatedAt),
  }));
}

export async function loadScalpV2DeploymentById(
  deploymentIdRaw: string,
): Promise<ScalpV2Deployment | null> {
  const deploymentId = String(deploymentIdRaw || "").trim();
  if (!deploymentId || !isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  const [row] = await db.$queryRaw<
    Array<{
      deploymentId: string;
      candidateId: number | null;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
      enabled: boolean;
      liveMode: string;
      promotionGate: unknown;
      riskProfile: unknown;
      createdAt: Date;
      updatedAt: Date;
    }>
  >(sql`
    SELECT
      deployment_id AS "deploymentId",
      candidate_id AS "candidateId",
      venue,
      symbol,
      strategy_id AS "strategyId",
      tune_id AS "tuneId",
      entry_session_profile AS "entrySessionProfile",
      enabled,
      live_mode AS "liveMode",
      promotion_gate AS "promotionGate",
      risk_profile AS "riskProfile",
      created_at AS "createdAt",
      updated_at AS "updatedAt"
    FROM scalp_v2_deployments
    WHERE deployment_id = ${deploymentId}
    LIMIT 1;
  `);
  if (!row) return null;
  return {
    deploymentId: String(row.deploymentId || "").trim(),
    candidateId:
      row.candidateId === null || row.candidateId === undefined
        ? null
        : Number(row.candidateId),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    strategyId: String(row.strategyId || "").trim().toLowerCase(),
    tuneId: String(row.tuneId || "").trim().toLowerCase(),
    entrySessionProfile: normalizeSession(row.entrySessionProfile),
    enabled: Boolean(row.enabled),
    liveMode: normalizeLiveMode(row.liveMode),
    promotionGate: asRecord(row.promotionGate),
    riskProfile: normalizeRiskProfile(row.riskProfile),
    createdAtMs: toMs(row.createdAt),
    updatedAtMs: toMs(row.updatedAt),
  };
}

export async function setScalpV2DeploymentEnabled(params: {
  deploymentId: string;
  enabled: boolean;
  liveMode?: ScalpV2LiveMode;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    UPDATE scalp_v2_deployments
    SET
      enabled = ${params.enabled},
      live_mode = ${params.liveMode || "shadow"},
      updated_at = NOW(),
      last_promoted_at = CASE WHEN ${params.enabled} THEN NOW() ELSE last_promoted_at END
    WHERE deployment_id = ${params.deploymentId};
  `);
}

export async function appendScalpV2ExecutionEvent(
  event: ScalpV2ExecutionEvent,
): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    INSERT INTO scalp_v2_execution_events(
      id,
      ts,
      deployment_id,
      venue,
      symbol,
      strategy_id,
      tune_id,
      entry_session_profile,
      event_type,
      broker_ref,
      reason_codes,
      source_of_truth,
      raw_payload,
      created_at
    ) VALUES (
      ${event.id},
      TO_TIMESTAMP(${Math.floor(event.tsMs)} / 1000.0),
      ${event.deploymentId},
      ${event.venue},
      ${event.symbol},
      ${event.strategyId},
      ${event.tuneId},
      ${event.entrySessionProfile},
      ${event.eventType},
      ${event.brokerRef},
      ${normalizeReasonCodes(event.reasonCodes)},
      ${event.sourceOfTruth},
      ${JSON.stringify(event.rawPayload || {})}::jsonb,
      NOW()
    )
    ON CONFLICT(id)
    DO NOTHING;
  `);

  const closeType = toLedgerCloseTypeFromEvent(event.eventType, event.reasonCodes);
  if (!closeType) return;

  await appendScalpV2LedgerRow({
    id: crypto.randomUUID(),
    tsExitMs: event.tsMs,
    deploymentId: event.deploymentId,
    venue: event.venue,
    symbol: event.symbol,
    strategyId: event.strategyId,
    tuneId: event.tuneId,
    entrySessionProfile: event.entrySessionProfile,
    entryRef: null,
    exitRef: event.brokerRef,
    closeType,
    rMultiple: Number(event.rawPayload.rMultiple || 0),
    pnlUsd:
      Number.isFinite(Number(event.rawPayload.pnlUsd))
        ? Number(event.rawPayload.pnlUsd)
        : null,
    sourceOfTruth: event.sourceOfTruth,
    reasonCodes: event.reasonCodes,
    rawPayload: event.rawPayload,
  });
}

export async function appendScalpV2LedgerRow(row: {
  id: string;
  tsExitMs: number;
  deploymentId: string;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpV2Session;
  entryRef: string | null;
  exitRef: string | null;
  closeType: ScalpV2CloseType;
  rMultiple: number;
  pnlUsd: number | null;
  sourceOfTruth: ScalpV2SourceOfTruth;
  reasonCodes: string[];
  rawPayload: Record<string, unknown>;
}): Promise<boolean> {
  if (!isScalpPgConfigured()) return false;
  const db = scalpPrisma();
  const inserted = await db.$queryRaw<Array<{ id: string }>>(sql`
    INSERT INTO scalp_v2_ledger(
      id,
      ts_exit,
      deployment_id,
      venue,
      symbol,
      strategy_id,
      tune_id,
      entry_session_profile,
      entry_ref,
      exit_ref,
      close_type,
      r_multiple,
      pnl_usd,
      source_of_truth,
      reason_codes,
      raw_payload,
      created_at
    ) VALUES (
      ${row.id},
      TO_TIMESTAMP(${Math.floor(row.tsExitMs)} / 1000.0),
      ${row.deploymentId},
      ${row.venue},
      ${row.symbol},
      ${row.strategyId},
      ${row.tuneId},
      ${row.entrySessionProfile},
      ${row.entryRef},
      ${row.exitRef},
      ${row.closeType},
      ${Number.isFinite(row.rMultiple) ? row.rMultiple : 0},
      ${row.pnlUsd},
      ${row.sourceOfTruth},
      ${normalizeReasonCodes(row.reasonCodes)},
      ${JSON.stringify(row.rawPayload || {})}::jsonb,
      NOW()
    )
    ON CONFLICT(id)
    DO NOTHING
    RETURNING id;
  `);
  return inserted.length > 0;
}

export async function listScalpV2LedgerRows(params: {
  deploymentIds: string[];
  fromTsMs: number;
  toTsMs: number;
  limit?: number;
}): Promise<
  Array<{
    deploymentId: string;
    tsExitMs: number;
    entrySessionProfile: ScalpV2Session;
    rMultiple: number;
  }>
> {
  if (!isScalpPgConfigured()) return [];
  const deploymentIds = Array.from(
    new Set(
      (params.deploymentIds || [])
        .map((row) => String(row || "").trim())
        .filter(Boolean),
    ),
  );
  if (!deploymentIds.length) return [];

  const fromTsMs = Math.max(0, Math.floor(Number(params.fromTsMs) || 0));
  const toTsMs = Math.max(fromTsMs + 1, Math.floor(Number(params.toTsMs) || 0));
  const limit = Math.max(1, Math.min(1_000_000, Math.floor(params.limit || 250_000)));

  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      deploymentId: string;
      tsExitMs: bigint;
      entrySessionProfile: string;
      rMultiple: number;
    }>
  >(sql`
    SELECT
      deployment_id AS "deploymentId",
      (EXTRACT(EPOCH FROM ts_exit) * 1000.0)::bigint AS "tsExitMs",
      entry_session_profile AS "entrySessionProfile",
      r_multiple::double precision AS "rMultiple"
    FROM scalp_v2_ledger
    WHERE deployment_id = ANY(${deploymentIds}::text[])
      AND ts_exit >= TO_TIMESTAMP(${fromTsMs} / 1000.0)
      AND ts_exit < TO_TIMESTAMP(${toTsMs} / 1000.0)
    ORDER BY ts_exit ASC
    LIMIT ${limit};
  `);

  return rows.map((row) => ({
    deploymentId: String(row.deploymentId || "").trim(),
    tsExitMs: Number(row.tsExitMs || 0),
    entrySessionProfile: normalizeSession(row.entrySessionProfile),
    rMultiple: Number.isFinite(Number(row.rMultiple)) ? Number(row.rMultiple) : 0,
  }));
}

export async function upsertScalpV2PositionSnapshot(params: {
  deploymentId: string;
  venue: ScalpV2Venue;
  symbol: string;
  side: "long" | "short" | null;
  entryPrice: number | null;
  leverage: number | null;
  size: number | null;
  dealId?: string | null;
  dealReference?: string | null;
  brokerSnapshotAtMs?: number | null;
  status: "open" | "flat";
  rawPayload?: Record<string, unknown>;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    INSERT INTO scalp_v2_positions(
      deployment_id,
      venue,
      symbol,
      side,
      entry_price,
      leverage,
      size,
      deal_id,
      deal_reference,
      broker_snapshot_at,
      status,
      raw_payload,
      created_at,
      updated_at
    ) VALUES (
      ${params.deploymentId},
      ${params.venue},
      ${params.symbol},
      ${params.side},
      ${params.entryPrice},
      ${params.leverage},
      ${params.size},
      ${params.dealId || null},
      ${params.dealReference || null},
      ${params.brokerSnapshotAtMs ? sql`TO_TIMESTAMP(${Math.floor(params.brokerSnapshotAtMs)} / 1000.0)` : sql`NULL`},
      ${params.status},
      ${JSON.stringify(params.rawPayload || {})}::jsonb,
      NOW(),
      NOW()
    )
    ON CONFLICT(deployment_id)
    DO UPDATE SET
      side = EXCLUDED.side,
      entry_price = EXCLUDED.entry_price,
      leverage = EXCLUDED.leverage,
      size = EXCLUDED.size,
      deal_id = EXCLUDED.deal_id,
      deal_reference = EXCLUDED.deal_reference,
      broker_snapshot_at = EXCLUDED.broker_snapshot_at,
      status = EXCLUDED.status,
      raw_payload = EXCLUDED.raw_payload,
      updated_at = NOW();
  `);
}

export async function listScalpV2OpenPositions(): Promise<
  Array<{
    deploymentId: string;
    venue: ScalpV2Venue;
    symbol: string;
    side: "long" | "short" | null;
    dealId: string | null;
    dealReference: string | null;
    updatedAtMs: number;
  }>
> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      deploymentId: string;
      venue: string;
      symbol: string;
      side: string | null;
      dealId: string | null;
      dealReference: string | null;
      updatedAt: Date;
    }>
  >(sql`
    SELECT
      deployment_id AS "deploymentId",
      venue,
      symbol,
      side,
      deal_id AS "dealId",
      deal_reference AS "dealReference",
      updated_at AS "updatedAt"
    FROM scalp_v2_positions
    WHERE status = 'open'
    ORDER BY updated_at DESC;
  `);
  return rows.map((row) => ({
    deploymentId: String(row.deploymentId || "").trim(),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    side: row.side === "long" || row.side === "short" ? row.side : null,
    dealId: row.dealId || null,
    dealReference: row.dealReference || null,
    updatedAtMs: toMs(row.updatedAt),
  }));
}

export async function listScalpV2ExecutionEvents(params: {
  limit?: number;
  deploymentId?: string;
  venue?: ScalpV2Venue;
  session?: ScalpV2Session;
} = {}): Promise<ScalpV2ExecutionEvent[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(5_000, Math.floor(params.limit || 300)));
  const where: string[] = [];
  const values: unknown[] = [];
  const deploymentId = String(params.deploymentId || "").trim();
  if (deploymentId) {
    values.push(deploymentId);
    where.push(`deployment_id = $${values.length}`);
  }
  if (params.venue) {
    values.push(params.venue);
    where.push(`venue = $${values.length}`);
  }
  if (params.session) {
    values.push(params.session);
    where.push(`entry_session_profile = $${values.length}`);
  }
  values.push(limit);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const rows = await db.$queryRawUnsafe<
    Array<{
      id: string;
      tsMs: number;
      deploymentId: string;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
      eventType: string;
      brokerRef: string | null;
      reasonCodes: string[];
      sourceOfTruth: string;
      rawPayload: unknown;
    }>
  >(
    `
      SELECT
        id,
        (EXTRACT(EPOCH FROM ts) * 1000.0)::bigint AS "tsMs",
        deployment_id AS "deploymentId",
        venue,
        symbol,
        strategy_id AS "strategyId",
        tune_id AS "tuneId",
        entry_session_profile AS "entrySessionProfile",
        event_type AS "eventType",
        broker_ref AS "brokerRef",
        reason_codes AS "reasonCodes",
        source_of_truth AS "sourceOfTruth",
        raw_payload AS "rawPayload"
      FROM scalp_v2_execution_events
      ${whereSql}
      ORDER BY ts DESC
      LIMIT $${values.length};
    `,
    ...values,
  );

  return rows.map((row) => ({
    id: String(row.id || ""),
    tsMs: Number(row.tsMs || Date.now()),
    deploymentId: String(row.deploymentId || "").trim(),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    strategyId: String(row.strategyId || "").trim().toLowerCase(),
    tuneId: String(row.tuneId || "").trim().toLowerCase(),
    entrySessionProfile: normalizeSession(row.entrySessionProfile),
    eventType: normalizeEventType(row.eventType),
    brokerRef: row.brokerRef || null,
    reasonCodes: normalizeReasonCodes(row.reasonCodes || []),
    sourceOfTruth: normalizeSourceOfTruth(row.sourceOfTruth),
    rawPayload: asRecord(row.rawPayload),
  }));
}

export async function listScalpV2SessionSnapshots(params: {
  deploymentIds?: string[];
  venue?: ScalpV2Venue;
  session?: ScalpV2Session;
  limit?: number;
} = {}): Promise<
  Array<{
    deploymentId: string;
    venue: ScalpV2Venue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: ScalpV2Session;
    dayKey: string;
    state: ScalpSessionState | null;
    lastReasonCodes: string[];
    updatedAtMs: number;
  }>
> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(10_000, Math.floor(params.limit || 500)));
  const where: string[] = [];
  const values: unknown[] = [];

  const deploymentIds = Array.from(
    new Set(
      (params.deploymentIds || [])
        .map((row) => String(row || "").trim())
        .filter(Boolean),
    ),
  );
  if (deploymentIds.length) {
    values.push(deploymentIds);
    where.push(`s.deployment_id = ANY($${values.length}::text[])`);
  }
  if (params.venue) {
    values.push(params.venue);
    where.push(`d.venue = $${values.length}`);
  }
  if (params.session) {
    values.push(params.session);
    where.push(`d.entry_session_profile = $${values.length}`);
  }

  values.push(limit);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = await db.$queryRawUnsafe<
    Array<{
      deploymentId: string;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
      dayKey: string;
      stateJson: unknown;
      lastReasonCodes: string[] | null;
      updatedAt: Date;
    }>
  >(
    `
      SELECT DISTINCT ON (s.deployment_id)
        s.deployment_id AS "deploymentId",
        d.venue AS "venue",
        d.symbol AS "symbol",
        d.strategy_id AS "strategyId",
        d.tune_id AS "tuneId",
        d.entry_session_profile AS "entrySessionProfile",
        TO_CHAR(s.day_key, 'YYYY-MM-DD') AS "dayKey",
        s.state_json AS "stateJson",
        s.last_reason_codes AS "lastReasonCodes",
        s.updated_at AS "updatedAt"
      FROM scalp_v2_sessions s
      INNER JOIN scalp_v2_deployments d
        ON d.deployment_id = s.deployment_id
      ${whereSql}
      ORDER BY s.deployment_id, s.updated_at DESC
      LIMIT $${values.length};
    `,
    ...values,
  );

  return rows.map((row) => {
    const stateRaw = asRecord(row.stateJson);
    const state = stateRaw && Object.keys(stateRaw).length
      ? ({ ...stateRaw, version: 2 } as unknown as ScalpSessionState)
      : null;
    return {
      deploymentId: String(row.deploymentId || "").trim(),
      venue: normalizeVenue(row.venue),
      symbol: String(row.symbol || "").trim().toUpperCase(),
      strategyId: String(row.strategyId || "").trim().toLowerCase(),
      tuneId: String(row.tuneId || "").trim().toLowerCase(),
      entrySessionProfile: normalizeSession(row.entrySessionProfile),
      dayKey: String(row.dayKey || "").trim(),
      state,
      lastReasonCodes: normalizeReasonCodes(row.lastReasonCodes || []),
      updatedAtMs: toMs(row.updatedAt),
    };
  });
}

export async function listScalpV2JournalRows(params: {
  limit?: number;
  venue?: ScalpV2Venue;
  session?: ScalpV2Session;
} = {}): Promise<
  Array<{
    id: string;
    tsMs: number;
    deploymentId: string | null;
    venue: ScalpV2Venue | null;
    symbol: string | null;
    strategyId: string | null;
    tuneId: string | null;
    entrySessionProfile: ScalpV2Session | null;
    dayKey: string | null;
    level: "info" | "warn" | "error";
    type: "execution" | "state" | "risk" | "error";
    reasonCodes: string[];
    payload: Record<string, unknown>;
  }>
> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(5_000, Math.floor(params.limit || 300)));
  const where: string[] = [];
  const values: unknown[] = [];

  if (params.venue) {
    values.push(params.venue);
    where.push(`venue = $${values.length}`);
  }
  if (params.session) {
    values.push(params.session);
    where.push(`entry_session_profile = $${values.length}`);
  }

  values.push(limit);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = await db.$queryRawUnsafe<
    Array<{
      id: string;
      tsMs: bigint;
      deploymentId: string | null;
      venue: string | null;
      symbol: string | null;
      strategyId: string | null;
      tuneId: string | null;
      entrySessionProfile: string | null;
      dayKey: string | null;
      level: string;
      type: string;
      reasonCodes: string[] | null;
      payload: unknown;
    }>
  >(
    `
      SELECT
        id,
        (EXTRACT(EPOCH FROM ts) * 1000.0)::bigint AS "tsMs",
        deployment_id AS "deploymentId",
        venue,
        symbol,
        strategy_id AS "strategyId",
        tune_id AS "tuneId",
        entry_session_profile AS "entrySessionProfile",
        TO_CHAR(day_key, 'YYYY-MM-DD') AS "dayKey",
        level,
        type,
        reason_codes AS "reasonCodes",
        payload
      FROM scalp_v2_journal
      ${whereSql}
      ORDER BY ts DESC
      LIMIT $${values.length};
    `,
    ...values,
  );

  return rows.map((row) => ({
    id: String(row.id || "").trim(),
    tsMs: Number(row.tsMs || Date.now()),
    deploymentId: String(row.deploymentId || "").trim() || null,
    venue: normalizeOptionalVenue(row.venue),
    symbol: row.symbol ? String(row.symbol).trim().toUpperCase() : null,
    strategyId: row.strategyId
      ? String(row.strategyId).trim().toLowerCase()
      : null,
    tuneId: row.tuneId ? String(row.tuneId).trim().toLowerCase() : null,
    entrySessionProfile: normalizeOptionalSession(row.entrySessionProfile),
    dayKey: String(row.dayKey || "").trim() || null,
    level: row.level === "warn" || row.level === "error" ? row.level : "info",
    type:
      row.type === "state" ||
      row.type === "risk" ||
      row.type === "error"
        ? row.type
        : "execution",
    reasonCodes: normalizeReasonCodes(row.reasonCodes || []),
    payload: asRecord(row.payload),
  }));
}

export async function loadScalpV2SessionState(params: {
  deploymentId: string;
  dayKey: string;
}): Promise<ScalpSessionState | null> {
  const deploymentId = String(params.deploymentId || "").trim();
  const dayKey = String(params.dayKey || "").trim();
  if (!deploymentId || !dayKey || !isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  const [row] = await db.$queryRaw<
    Array<{
      stateJson: unknown;
    }>
  >(sql`
    SELECT state_json AS "stateJson"
    FROM scalp_v2_sessions
    WHERE deployment_id = ${deploymentId}
      AND day_key = ${dayKey}::date
    LIMIT 1;
  `);
  if (!row || !row.stateJson || typeof row.stateJson !== "object") return null;
  const state = row.stateJson as ScalpSessionState;
  return {
    ...state,
    version: 2,
  };
}

export async function upsertScalpV2SessionState(
  state: ScalpSessionState,
): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const deploymentId = String(state.deploymentId || "").trim();
  const dayKey = String(state.dayKey || "").trim();
  if (!deploymentId || !dayKey) return;
  const db = scalpPrisma();
  const lastReasonCodes = normalizeReasonCodes(state.run?.lastReasonCodes || []);
  await db.$executeRaw(sql`
    INSERT INTO scalp_v2_sessions(
      deployment_id,
      day_key,
      state_json,
      last_reason_codes,
      updated_at
    ) VALUES (
      ${deploymentId},
      ${dayKey}::date,
      ${JSON.stringify({ ...state, version: 2 })}::jsonb,
      ${lastReasonCodes},
      NOW()
    )
    ON CONFLICT(deployment_id, day_key)
    DO UPDATE SET
      state_json = EXCLUDED.state_json,
      last_reason_codes = EXCLUDED.last_reason_codes,
      updated_at = NOW();
  `);
}

export async function appendScalpV2JournalEntry(params: {
  entry: ScalpJournalEntry;
  deploymentId?: string | null;
  venue?: ScalpV2Venue | null;
  strategyId?: string | null;
  tuneId?: string | null;
  entrySessionProfile?: ScalpV2Session | null;
}): Promise<boolean> {
  if (!isScalpPgConfigured()) return false;
  const entry = params.entry;
  const id = String(entry?.id || "").trim();
  if (!id) return false;
  const levelRaw = String(entry?.level || "info")
    .trim()
    .toLowerCase();
  const level = levelRaw === "warn" || levelRaw === "error" ? levelRaw : "info";
  const tsMs = Math.floor(
    Number.isFinite(Number(entry?.timestampMs))
      ? Number(entry.timestampMs)
      : Date.now(),
  );
  const symbol = entry?.symbol ? String(entry.symbol).trim().toUpperCase() : null;
  const dayKey = entry?.dayKey ? String(entry.dayKey).trim() : null;
  const deploymentId = String(params.deploymentId || "").trim() || null;
  const payload = asRecord(entry?.payload);
  const [row] = await scalpPrisma().$queryRaw<
    Array<{ id: string }>
  >(sql`
    INSERT INTO scalp_v2_journal(
      id,
      ts,
      deployment_id,
      venue,
      symbol,
      strategy_id,
      tune_id,
      entry_session_profile,
      day_key,
      level,
      type,
      reason_codes,
      payload,
      created_at
    ) VALUES (
      ${id},
      TO_TIMESTAMP(${tsMs} / 1000.0),
      ${deploymentId},
      ${params.venue || null},
      ${symbol},
      ${params.strategyId || null},
      ${params.tuneId || null},
      ${params.entrySessionProfile || null},
      ${dayKey ? sql`${dayKey}::date` : sql`NULL`},
      ${level},
      ${normalizeJournalType(entry?.type)},
      ${normalizeReasonCodes(entry?.reasonCodes || [])},
      ${JSON.stringify(payload)}::jsonb,
      NOW()
    )
    ON CONFLICT(id)
    DO NOTHING
    RETURNING id;
  `);
  return Boolean(row?.id);
}

export async function loadScalpV2Summary(): Promise<Record<string, unknown>> {
  if (!isScalpPgConfigured()) {
    return {
      pgConfigured: false,
      generatedAtMs: Date.now(),
      candidates: 0,
      deployments: 0,
      enabledDeployments: 0,
      candidateStatusCounts: {
        discovered: 0,
        evaluated: 0,
        promoted: 0,
        rejected: 0,
      },
      events24h: 0,
      ledgerRows30d: 0,
      netR30d: 0,
    };
  }
  const db = scalpPrisma();
  const [row] = await db.$queryRaw<
    Array<{
      candidates: bigint;
      deployments: bigint;
      enabledDeployments: bigint;
      discoveredCandidates: bigint;
      evaluatedCandidates: bigint;
      promotedCandidates: bigint;
      rejectedCandidates: bigint;
      events24h: bigint;
      ledgerRows30d: bigint;
      netR30d: number | null;
      coverageJson: unknown;
    }>
  >(sql`
    SELECT
      (SELECT COUNT(*)::bigint FROM scalp_v2_candidates) AS candidates,
      (SELECT COUNT(*)::bigint FROM scalp_v2_deployments) AS deployments,
      (SELECT COUNT(*)::bigint FROM scalp_v2_deployments WHERE enabled = TRUE) AS "enabledDeployments",
      (SELECT COUNT(*)::bigint FROM scalp_v2_candidates WHERE status = 'discovered') AS "discoveredCandidates",
      (SELECT COUNT(*)::bigint FROM scalp_v2_candidates WHERE status = 'evaluated') AS "evaluatedCandidates",
      (SELECT COUNT(*)::bigint FROM scalp_v2_candidates WHERE status = 'promoted') AS "promotedCandidates",
      (SELECT COUNT(*)::bigint FROM scalp_v2_candidates WHERE status = 'rejected') AS "rejectedCandidates",
      0::bigint AS "events24h",
      0::bigint AS "ledgerRows30d",
      0::double precision AS "netR30d",
      (SELECT COALESCE(jsonb_agg(jsonb_build_object('symbol', g.symbol, 'candidates', g.c, 'deployments', g.d)), '[]'::jsonb)
       FROM (
         SELECT c.symbol, c.cnt AS c, COALESCE(d.cnt, 0) AS d
         FROM (
           SELECT symbol, COUNT(*)::bigint AS cnt
           FROM scalp_v2_candidates
           WHERE (metadata_json->'worker'->'stageC'->>'passed') IS NULL
             AND status NOT IN ('rejected')
           GROUP BY symbol
         ) c
         LEFT JOIN (SELECT symbol, COUNT(*)::bigint AS cnt FROM scalp_v2_deployments GROUP BY symbol) d ON c.symbol = d.symbol
         WHERE c.cnt > COALESCE(d.cnt, 0)
         ORDER BY c.symbol
       ) g
      ) AS "coverageJson";
  `);

  const symbolCoverage = (Array.isArray(row?.coverageJson) ? row.coverageJson : []).map((r: any) => ({
    symbol: String(r?.symbol || ""),
    candidates: Number(r?.candidates || 0),
    deployments: Number(r?.deployments || 0),
  }));

  return {
    pgConfigured: true,
    generatedAtMs: Date.now(),
    candidates: Number(row?.candidates || 0),
    deployments: Number(row?.deployments || 0),
    enabledDeployments: Number(row?.enabledDeployments || 0),
    candidateStatusCounts: {
      discovered: Number(row?.discoveredCandidates || 0),
      evaluated: Number(row?.evaluatedCandidates || 0),
      promoted: Number(row?.promotedCandidates || 0),
      rejected: Number(row?.rejectedCandidates || 0),
    },
    events24h: Number(row?.events24h || 0),
    ledgerRows30d: Number(row?.ledgerRows30d || 0),
    netR30d: Number.isFinite(Number(row?.netR30d)) ? Number(row?.netR30d) : 0,
    symbolCoverage,
  };
}

export async function listScalpV2Jobs(params: {
  limit?: number;
} = {}): Promise<
  Array<{
    jobKind: ScalpV2JobKind;
    status: ScalpV2JobStatus;
    attempts: number;
    nextRunAtMs: number;
    lockedBy: string | null;
    lockedAtMs: number | null;
    updatedAtMs: number;
    payload: Record<string, unknown>;
  }>
> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(100, Math.floor(params.limit || 20)));
  const rows = await db.$queryRaw<
    Array<{
      jobKind: string;
      status: string;
      attempts: number;
      nextRunAt: Date;
      lockedBy: string | null;
      lockedAt: Date | null;
      updatedAt: Date;
      payload: unknown;
    }>
  >(sql`
    SELECT
      job_kind AS "jobKind",
      status,
      attempts,
      next_run_at AS "nextRunAt",
      locked_by AS "lockedBy",
      locked_at AS "lockedAt",
      updated_at AS "updatedAt",
      payload
    FROM scalp_v2_jobs
    ORDER BY updated_at DESC
    LIMIT ${limit};
  `);

  return rows.map((row) => {
    const statusRaw = String(row.status || "")
      .trim()
      .toLowerCase();
    const status: ScalpV2JobStatus =
      statusRaw === "running"
        ? "running"
        : statusRaw === "succeeded"
          ? "succeeded"
          : statusRaw === "failed"
            ? "failed"
            : "pending";
    return {
      jobKind:
        String(row.jobKind || "").trim().toLowerCase() === "evaluate"
          ? "evaluate"
        : String(row.jobKind || "").trim().toLowerCase() === "worker"
            ? "worker"
        : String(row.jobKind || "").trim().toLowerCase() === "research"
            ? "research"
        : String(row.jobKind || "").trim().toLowerCase() === "promote"
            ? "promote"
        : String(row.jobKind || "").trim().toLowerCase() === "execute"
              ? "execute"
              : String(row.jobKind || "").trim().toLowerCase() === "reconcile"
                ? "reconcile"
                : "discover",
      status,
      attempts: Math.max(0, Math.floor(Number(row.attempts || 0))),
      nextRunAtMs: toMs(row.nextRunAt),
      lockedBy: row.lockedBy || null,
      lockedAtMs: toOptionalMs(row.lockedAt),
      updatedAtMs: toMs(row.updatedAt),
      payload: asRecord(row.payload),
    };
  });
}

export async function loadScalpV2ResearchCursor(params: {
  cursorKey: string;
}): Promise<ScalpV2ResearchCursor | null> {
  const cursorKey = String(params.cursorKey || "").trim();
  if (!cursorKey || !isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  const [row] = await db.$queryRaw<
    Array<{
      cursorKey: string;
      venue: string;
      symbol: string;
      entrySessionProfile: string;
      phase: string;
      lastCandidateOffset: number;
      lastWeekStartMs: number | null;
      progressJson: unknown;
      updatedAt: Date;
    }>
  >(sql`
    SELECT
      cursor_key AS "cursorKey",
      venue,
      symbol,
      entry_session_profile AS "entrySessionProfile",
      phase,
      last_candidate_offset AS "lastCandidateOffset",
      CASE
        WHEN last_week_start IS NULL THEN NULL
        ELSE (EXTRACT(EPOCH FROM last_week_start) * 1000.0)::bigint
      END AS "lastWeekStartMs",
      progress_json AS "progressJson",
      updated_at AS "updatedAt"
    FROM scalp_v2_research_cursor
    WHERE cursor_key = ${cursorKey}
    LIMIT 1;
  `);
  if (!row) return null;
  return {
    cursorKey: String(row.cursorKey || "").trim(),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    entrySessionProfile: normalizeSession(row.entrySessionProfile),
    phase: normalizeResearchPhase(row.phase),
    lastCandidateOffset: Math.max(
      0,
      Math.floor(Number(row.lastCandidateOffset || 0)),
    ),
    lastWeekStartMs: toOptionalMs(row.lastWeekStartMs),
    progress: asRecord(row.progressJson),
    updatedAtMs: toMs(row.updatedAt),
  };
}

export async function upsertScalpV2ResearchCursor(params: {
  cursorKey: string;
  venue: ScalpV2Venue;
  symbol: string;
  entrySessionProfile: ScalpV2Session;
  phase?: ScalpV2ResearchCursor["phase"];
  lastCandidateOffset?: number;
  lastWeekStartMs?: number | null;
  progress?: Record<string, unknown>;
}): Promise<ScalpV2ResearchCursor | null> {
  const cursorKey = String(params.cursorKey || "").trim();
  if (!cursorKey || !isScalpPgConfigured()) return null;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    INSERT INTO scalp_v2_research_cursor(
      cursor_key,
      venue,
      symbol,
      entry_session_profile,
      phase,
      last_candidate_offset,
      last_week_start,
      progress_json,
      updated_at
    ) VALUES (
      ${cursorKey},
      ${params.venue},
      ${String(params.symbol || "").trim().toUpperCase()},
      ${params.entrySessionProfile},
      ${normalizeResearchPhase(params.phase)},
      ${Math.max(0, Math.floor(Number(params.lastCandidateOffset || 0)))},
      ${
        Number.isFinite(Number(params.lastWeekStartMs))
          ? sql`TO_TIMESTAMP(${Math.floor(Number(params.lastWeekStartMs))} / 1000.0)`
          : sql`NULL`
      },
      ${JSON.stringify(params.progress || {})}::jsonb,
      NOW()
    )
    ON CONFLICT(cursor_key)
    DO UPDATE SET
      venue = EXCLUDED.venue,
      symbol = EXCLUDED.symbol,
      entry_session_profile = EXCLUDED.entry_session_profile,
      phase = EXCLUDED.phase,
      last_candidate_offset = EXCLUDED.last_candidate_offset,
      last_week_start = EXCLUDED.last_week_start,
      progress_json = EXCLUDED.progress_json,
      updated_at = NOW();
  `);
  return loadScalpV2ResearchCursor({ cursorKey });
}

export async function listScalpV2ResearchCursors(params: {
  venue?: ScalpV2Venue;
  symbol?: string;
  entrySessionProfile?: ScalpV2Session;
  limit?: number;
} = {}): Promise<ScalpV2ResearchCursor[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(5_000, Math.floor(params.limit || 200)));
  const where: string[] = [];
  const values: unknown[] = [];

  if (params.venue) {
    values.push(params.venue);
    where.push(`venue = $${values.length}`);
  }
  const symbol = String(params.symbol || "")
    .trim()
    .toUpperCase();
  if (symbol) {
    values.push(symbol);
    where.push(`symbol = $${values.length}`);
  }
  if (params.entrySessionProfile) {
    values.push(params.entrySessionProfile);
    where.push(`entry_session_profile = $${values.length}`);
  }

  values.push(limit);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = await db.$queryRawUnsafe<
    Array<{
      cursorKey: string;
      venue: string;
      symbol: string;
      entrySessionProfile: string;
      phase: string;
      lastCandidateOffset: number;
      lastWeekStartMs: number | null;
      progressJson: unknown;
      updatedAt: Date;
    }>
  >(
    `
      SELECT
        cursor_key AS "cursorKey",
        venue,
        symbol,
        entry_session_profile AS "entrySessionProfile",
        phase,
        last_candidate_offset AS "lastCandidateOffset",
        CASE
          WHEN last_week_start IS NULL THEN NULL
          ELSE (EXTRACT(EPOCH FROM last_week_start) * 1000.0)::bigint
        END AS "lastWeekStartMs",
        progress_json AS "progressJson",
        updated_at AS "updatedAt"
      FROM scalp_v2_research_cursor
      ${whereSql}
      ORDER BY updated_at DESC
      LIMIT $${values.length};
    `,
    ...values,
  );

  return rows.map((row) => ({
    cursorKey: String(row.cursorKey || "").trim(),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    entrySessionProfile: normalizeSession(row.entrySessionProfile),
    phase: normalizeResearchPhase(row.phase),
    lastCandidateOffset: Math.max(
      0,
      Math.floor(Number(row.lastCandidateOffset || 0)),
    ),
    lastWeekStartMs: toOptionalMs(row.lastWeekStartMs),
    progress: asRecord(row.progressJson),
    updatedAtMs: toMs(row.updatedAt),
  }));
}

export async function upsertScalpV2ResearchHighlights(params: {
  rows: Array<{
    candidateId: string;
    venue: ScalpV2Venue;
    symbol: string;
    entrySessionProfile: ScalpV2Session;
    score: number;
    trades12w?: number;
    winningWeeks12w?: number;
    consecutiveWinningWeeks?: number;
    robustness?: Record<string, unknown>;
    dsl?: Record<string, unknown>;
    notes?: string | null;
    remarkable?: boolean;
  }>;
}): Promise<number> {
  if (!isScalpPgConfigured() || params.rows.length === 0) return 0;
  const db = scalpPrisma();
  const values = params.rows
    .map((row) => ({
      candidateId: String(row.candidateId || "").trim(),
      symbol: String(row.symbol || "").trim().toUpperCase(),
      score: Number.isFinite(Number(row.score)) ? Number(row.score) : 0,
      trades12w: Math.max(0, Math.floor(Number(row.trades12w || 0))),
      winningWeeks12w: Math.max(0, Math.floor(Number(row.winningWeeks12w || 0))),
      consecutiveWinningWeeks: Math.max(
        0,
        Math.floor(Number(row.consecutiveWinningWeeks || 0)),
      ),
      robustness: JSON.stringify(row.robustness || {}),
      dsl: JSON.stringify(row.dsl || {}),
      notes: row.notes === undefined ? null : row.notes,
      remarkable: row.remarkable !== false,
      venue: row.venue,
      entrySessionProfile: row.entrySessionProfile,
    }))
    .filter((row) => row.candidateId && row.symbol);
  if (!values.length) return 0;

  await db.$executeRaw(sql`
    INSERT INTO scalp_v2_research_highlights(
      candidate_id,
      venue,
      symbol,
      entry_session_profile,
      score,
      trades_12w,
      winning_weeks_12w,
      consecutive_winning_weeks,
      robustness_json,
      dsl_json,
      notes,
      remarkable,
      created_at,
      updated_at
    ) VALUES ${join(
      values.map(
        (row) => sql`(
          ${row.candidateId},
          ${row.venue},
          ${row.symbol},
          ${row.entrySessionProfile},
          ${row.score},
          ${row.trades12w},
          ${row.winningWeeks12w},
          ${row.consecutiveWinningWeeks},
          ${row.robustness}::jsonb,
          ${row.dsl}::jsonb,
          ${row.notes},
          ${row.remarkable},
          NOW(),
          NOW()
        )`,
      ),
      ",",
    )}
    ON CONFLICT(candidate_id, venue, symbol, entry_session_profile)
    DO UPDATE SET
      score = EXCLUDED.score,
      trades_12w = EXCLUDED.trades_12w,
      winning_weeks_12w = EXCLUDED.winning_weeks_12w,
      consecutive_winning_weeks = EXCLUDED.consecutive_winning_weeks,
      robustness_json = EXCLUDED.robustness_json,
      dsl_json = EXCLUDED.dsl_json,
      notes = EXCLUDED.notes,
      remarkable = EXCLUDED.remarkable,
      updated_at = NOW();
  `);

  return values.length;
}

export async function listScalpV2ResearchHighlights(params: {
  venue?: ScalpV2Venue;
  symbol?: string;
  entrySessionProfile?: ScalpV2Session;
  remarkableOnly?: boolean;
  limit?: number;
} = {}): Promise<ScalpV2ResearchHighlight[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(5_000, Math.floor(params.limit || 200)));
  const where: string[] = [];
  const values: unknown[] = [];

  if (params.venue) {
    values.push(params.venue);
    where.push(`venue = $${values.length}`);
  }
  const symbol = String(params.symbol || "")
    .trim()
    .toUpperCase();
  if (symbol) {
    values.push(symbol);
    where.push(`symbol = $${values.length}`);
  }
  if (params.entrySessionProfile) {
    values.push(params.entrySessionProfile);
    where.push(`entry_session_profile = $${values.length}`);
  }
  if (params.remarkableOnly !== false) {
    values.push(true);
    where.push(`remarkable = $${values.length}`);
  }

  values.push(limit);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  const rows = await db.$queryRawUnsafe<
    Array<{
      id: bigint;
      candidateId: string;
      venue: string;
      symbol: string;
      entrySessionProfile: string;
      score: number;
      trades12w: number;
      winningWeeks12w: number;
      consecutiveWinningWeeks: number;
      robustnessJson: unknown;
      dslJson: unknown;
      notes: string | null;
      remarkable: boolean;
      createdAt: Date;
      updatedAt: Date;
    }>
  >(
    `
      SELECT
        id,
        candidate_id AS "candidateId",
        venue,
        symbol,
        entry_session_profile AS "entrySessionProfile",
        score::double precision AS score,
        trades_12w AS "trades12w",
        winning_weeks_12w AS "winningWeeks12w",
        consecutive_winning_weeks AS "consecutiveWinningWeeks",
        robustness_json AS "robustnessJson",
        dsl_json AS "dslJson",
        notes,
        remarkable,
        created_at AS "createdAt",
        updated_at AS "updatedAt"
      FROM scalp_v2_research_highlights
      ${whereSql}
      ORDER BY score DESC, updated_at DESC
      LIMIT $${values.length};
    `,
    ...values,
  );

  return rows.map((row) => ({
    id: Number(row.id || 0),
    candidateId: String(row.candidateId || "").trim(),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    entrySessionProfile: normalizeSession(row.entrySessionProfile),
    score: Number.isFinite(Number(row.score)) ? Number(row.score) : 0,
    trades12w: Math.max(0, Math.floor(Number(row.trades12w || 0))),
    winningWeeks12w: Math.max(0, Math.floor(Number(row.winningWeeks12w || 0))),
    consecutiveWinningWeeks: Math.max(
      0,
      Math.floor(Number(row.consecutiveWinningWeeks || 0)),
    ),
    robustness: asRecord(row.robustnessJson),
    dsl: asRecord(row.dslJson),
    notes: row.notes || null,
    remarkable: Boolean(row.remarkable),
    createdAtMs: toMs(row.createdAt),
    updatedAtMs: toMs(row.updatedAt),
  }));
}

export async function importV1LedgerIntoScalpV2(params: {
  limit?: number;
} = {}): Promise<{
  processed: number;
  inserted: number;
  updated: number;
  skipped: number;
}> {
  if (!isScalpPgConfigured()) {
    return { processed: 0, inserted: 0, updated: 0, skipped: 0 };
  }
  const db = scalpPrisma();
  if (!(await scalpTableExists("scalp_trade_ledger"))) {
    return { processed: 0, inserted: 0, updated: 0, skipped: 0 };
  }
  const limit = Math.max(1, Math.min(200_000, Math.floor(params.limit || 50_000)));

  const rows = await db.$queryRaw<
    Array<{
      id: string;
      exitAtMs: bigint;
      deploymentId: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      rMultiple: number;
      reasonCodes: string[];
    }>
  >(sql`
    SELECT
      id::text AS id,
      (EXTRACT(EPOCH FROM exit_at) * 1000.0)::bigint AS "exitAtMs",
      deployment_id AS "deploymentId",
      symbol,
      strategy_id AS "strategyId",
      tune_id AS "tuneId",
      r_multiple::double precision AS "rMultiple",
      reason_codes AS "reasonCodes"
    FROM scalp_trade_ledger
    ORDER BY exit_at DESC
    LIMIT ${limit};
  `);

  const deploymentIds = Array.from(
    new Set(
      rows
        .map((row) => String(row.deploymentId || "").trim())
        .filter(Boolean),
    ),
  );
  const deploymentById = new Map<
    string,
    { venue: ScalpV2Venue; entrySessionProfile: ScalpV2Session }
  >();
  if (deploymentIds.length) {
    const deploymentRows = await db.$queryRaw<
      Array<{ deploymentId: string; venue: string; entrySessionProfile: string }>
    >(sql`
      SELECT
        deployment_id AS "deploymentId",
        venue,
        entry_session_profile AS "entrySessionProfile"
      FROM scalp_v2_deployments
      WHERE deployment_id = ANY(${deploymentIds}::text[]);
    `);
    for (const row of deploymentRows) {
      const deploymentId = String(row.deploymentId || "").trim();
      if (!deploymentId) continue;
      deploymentById.set(deploymentId, {
        venue: normalizeVenue(row.venue),
        entrySessionProfile: normalizeSession(row.entrySessionProfile),
      });
    }
  }

  const inferSessionFromDeploymentId = (deploymentIdRaw: string): ScalpV2Session => {
    const deploymentId = String(deploymentIdRaw || "").trim().toLowerCase();
    const match = deploymentId.match(/__sp_([a-z]+)/);
    if (!match?.[1]) return "berlin";
    return normalizeSession(match[1]);
  };

  let inserted = 0;
  let skipped = 0;

  for (const row of rows) {
    const deploymentId = String(row.deploymentId || "").trim();
    const deployment = deploymentById.get(deploymentId);
    const venue = deployment?.venue
      ? deployment.venue
      : String(row.deploymentId || "").toLowerCase().startsWith("capital:")
        ? "capital"
        : "bitget";
    const entrySessionProfile =
      deployment?.entrySessionProfile || inferSessionFromDeploymentId(deploymentId);
    const reasonCodes = normalizeReasonCodes(row.reasonCodes || []);
    const closeType = deriveCloseTypeFromReasonCodes(reasonCodes);

    try {
      const wasInserted = await appendScalpV2LedgerRow({
        id: row.id,
        tsExitMs: Number(row.exitAtMs || Date.now()),
        deploymentId,
        venue,
        symbol: String(row.symbol || "").trim().toUpperCase(),
        strategyId: String(row.strategyId || "").trim().toLowerCase(),
        tuneId: String(row.tuneId || "").trim().toLowerCase(),
        entrySessionProfile,
        entryRef: null,
        exitRef: null,
        closeType,
        rMultiple: Number.isFinite(Number(row.rMultiple)) ? Number(row.rMultiple) : 0,
        pnlUsd: null,
        sourceOfTruth: "legacy_v1_import",
        reasonCodes,
        rawPayload: { importedFrom: "scalp_trade_ledger" },
      });
      if (wasInserted) inserted += 1;
      else skipped += 1;
    } catch {
      skipped += 1;
    }
  }

  return {
    processed: rows.length,
    inserted,
    updated: 0,
    skipped,
  };
}

type ScalpV2DeploymentIdentity = {
  deploymentId: string;
  venue: ScalpV2Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpV2Session;
};

async function loadScalpV2DeploymentIdentityMap(): Promise<
  Map<string, ScalpV2DeploymentIdentity>
> {
  const map = new Map<string, ScalpV2DeploymentIdentity>();
  if (!isScalpPgConfigured()) return map;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      deploymentId: string;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      entrySessionProfile: string;
    }>
  >(sql`
    SELECT
      deployment_id AS "deploymentId",
      venue,
      symbol,
      strategy_id AS "strategyId",
      tune_id AS "tuneId",
      entry_session_profile AS "entrySessionProfile"
    FROM scalp_v2_deployments;
  `);
  for (const row of rows) {
    const deploymentId = String(row.deploymentId || "").trim();
    if (!deploymentId) continue;
    map.set(deploymentId, {
      deploymentId,
      venue: normalizeVenue(row.venue),
      symbol: String(row.symbol || "").trim().toUpperCase(),
      strategyId: String(row.strategyId || "").trim().toLowerCase(),
      tuneId: String(row.tuneId || "").trim().toLowerCase(),
      entrySessionProfile: normalizeSession(row.entrySessionProfile),
    });
  }
  return map;
}

function inferSessionFromTuneOrDeploymentId(value: unknown): ScalpV2Session {
  const raw = String(value || "")
    .trim()
    .toLowerCase();
  if (!raw) return "berlin";
  const match = raw.match(/__sp_([a-z]+)/);
  if (!match?.[1]) return "berlin";
  return normalizeSession(match[1]);
}

function resolveMappedDeploymentId(params: {
  candidates: string[];
  deploymentMap: Map<string, ScalpV2DeploymentIdentity>;
  venueHint?: unknown;
  symbolHint?: unknown;
  strategyIdHint?: unknown;
  tuneIdHint?: unknown;
  sessionHint?: unknown;
}): string | null {
  for (const candidate of params.candidates) {
    const deploymentId = String(candidate || "").trim();
    if (!deploymentId) continue;
    if (params.deploymentMap.has(deploymentId)) return deploymentId;
  }

  const symbol = String(params.symbolHint || "")
    .trim()
    .toUpperCase();
  const strategyId = String(params.strategyIdHint || "")
    .trim()
    .toLowerCase();
  const tuneId = String(params.tuneIdHint || "")
    .trim()
    .toLowerCase();
  if (!symbol || !strategyId || !tuneId) return null;
  const venueHint = String(params.venueHint || "")
    .trim()
    .toLowerCase();
  const venue: ScalpV2Venue = venueHint === "capital" ? "capital" : "bitget";
  const session = normalizeSession(
    params.sessionHint || inferSessionFromTuneOrDeploymentId(tuneId),
  );
  const inferred = toDeploymentId({
    venue,
    symbol,
    strategyId,
    tuneId,
    session,
  });
  return params.deploymentMap.has(inferred) ? inferred : null;
}

export async function importV1SessionsIntoScalpV2(params: {
  limit?: number;
} = {}): Promise<{
  processed: number;
  inserted: number;
  updated: number;
  skipped: number;
}> {
  if (!isScalpPgConfigured()) {
    return { processed: 0, inserted: 0, updated: 0, skipped: 0 };
  }
  const db = scalpPrisma();
  if (!(await scalpTableExists("scalp_sessions"))) {
    return { processed: 0, inserted: 0, updated: 0, skipped: 0 };
  }
  const limit = Math.max(1, Math.min(200_000, Math.floor(params.limit || 50_000)));
  const deploymentMap = await loadScalpV2DeploymentIdentityMap();

  const rows = await db.$queryRaw<
    Array<{
      deploymentId: string;
      dayKey: string;
      stateJson: unknown;
      lastReasonCodes: string[];
      updatedAtMs: bigint;
    }>
  >(sql`
    SELECT
      deployment_id AS "deploymentId",
      TO_CHAR(day_key, 'YYYY-MM-DD') AS "dayKey",
      state_json AS "stateJson",
      last_reason_codes AS "lastReasonCodes",
      (EXTRACT(EPOCH FROM updated_at) * 1000.0)::bigint AS "updatedAtMs"
    FROM scalp_sessions
    ORDER BY updated_at DESC
    LIMIT ${limit};
  `);

  let inserted = 0;
  let updated = 0;
  let skipped = 0;

  for (const row of rows) {
    const stateJson = asRecord(row.stateJson);
    const mappedDeploymentId = resolveMappedDeploymentId({
      candidates: [
        row.deploymentId,
        String(stateJson.deploymentId || ""),
      ],
      deploymentMap,
      venueHint: stateJson.venue,
      symbolHint: stateJson.symbol,
      strategyIdHint: stateJson.strategyId,
      tuneIdHint: stateJson.tuneId,
      sessionHint:
        stateJson.entrySessionProfile || inferSessionFromTuneOrDeploymentId(stateJson.tuneId),
    });
    if (!mappedDeploymentId) {
      skipped += 1;
      continue;
    }
    const persistedState = {
      ...stateJson,
      version: 2,
      deploymentId: mappedDeploymentId,
    };
    const dayKey = String(row.dayKey || "").trim();
    if (!dayKey) {
      skipped += 1;
      continue;
    }
    const updatedAtMs = Number.isFinite(Number(row.updatedAtMs))
      ? Number(row.updatedAtMs)
      : Date.now();
    const [result] = await db.$queryRaw<Array<{ inserted: boolean }>>(sql`
      INSERT INTO scalp_v2_sessions(
        deployment_id,
        day_key,
        state_json,
        last_reason_codes,
        updated_at
      ) VALUES (
        ${mappedDeploymentId},
        ${dayKey}::date,
        ${JSON.stringify(persistedState)}::jsonb,
        ${normalizeReasonCodes(row.lastReasonCodes || [])},
        TO_TIMESTAMP(${Math.floor(updatedAtMs)} / 1000.0)
      )
      ON CONFLICT(deployment_id, day_key)
      DO UPDATE SET
        state_json = EXCLUDED.state_json,
        last_reason_codes = EXCLUDED.last_reason_codes,
        updated_at = EXCLUDED.updated_at
      WHERE
        scalp_v2_sessions.state_json IS DISTINCT FROM EXCLUDED.state_json
        OR scalp_v2_sessions.last_reason_codes IS DISTINCT FROM EXCLUDED.last_reason_codes
        OR scalp_v2_sessions.updated_at IS DISTINCT FROM EXCLUDED.updated_at
      RETURNING (xmax = 0) AS inserted;
    `);
    if (!result) {
      skipped += 1;
    } else if (result.inserted) {
      inserted += 1;
    } else {
      updated += 1;
    }
  }

  return {
    processed: rows.length,
    inserted,
    updated,
    skipped,
  };
}

export async function importV1JournalIntoScalpV2(params: {
  limit?: number;
} = {}): Promise<{
  processed: number;
  inserted: number;
  updated: number;
  skipped: number;
}> {
  if (!isScalpPgConfigured()) {
    return { processed: 0, inserted: 0, updated: 0, skipped: 0 };
  }
  const db = scalpPrisma();
  if (!(await scalpTableExists("scalp_journal"))) {
    return { processed: 0, inserted: 0, updated: 0, skipped: 0 };
  }
  const limit = Math.max(1, Math.min(500_000, Math.floor(params.limit || 100_000)));
  const deploymentMap = await loadScalpV2DeploymentIdentityMap();

  const rows = await db.$queryRaw<
    Array<{
      id: string;
      tsMs: bigint;
      deploymentId: string | null;
      symbol: string | null;
      dayKey: string | null;
      level: string;
      type: string;
      reasonCodes: string[];
      payload: unknown;
    }>
  >(sql`
    SELECT
      id::text AS id,
      (EXTRACT(EPOCH FROM ts) * 1000.0)::bigint AS "tsMs",
      deployment_id AS "deploymentId",
      symbol,
      TO_CHAR(day_key, 'YYYY-MM-DD') AS "dayKey",
      level,
      type,
      reason_codes AS "reasonCodes",
      payload
    FROM scalp_journal
    ORDER BY ts DESC
    LIMIT ${limit};
  `);

  let inserted = 0;
  let skipped = 0;

  for (const row of rows) {
    const payload = asRecord(row.payload);
    const mappedDeploymentId = resolveMappedDeploymentId({
      candidates: [
        String(row.deploymentId || ""),
        String(payload.deploymentId || ""),
      ],
      deploymentMap,
      venueHint: payload.venue,
      symbolHint: payload.symbol || row.symbol,
      strategyIdHint: payload.strategyId,
      tuneIdHint: payload.tuneId,
      sessionHint:
        payload.entrySessionProfile || inferSessionFromTuneOrDeploymentId(payload.tuneId),
    });
    const deployment = mappedDeploymentId
      ? deploymentMap.get(mappedDeploymentId) || null
      : null;
    const tsMs = Number.isFinite(Number(row.tsMs)) ? Number(row.tsMs) : Date.now();
    const symbol = String(row.symbol || payload.symbol || deployment?.symbol || "")
      .trim()
      .toUpperCase();
    const [insertedRow] = await db.$queryRaw<Array<{ id: string }>>(sql`
      INSERT INTO scalp_v2_journal(
        id,
        ts,
        deployment_id,
        venue,
        symbol,
        strategy_id,
        tune_id,
        entry_session_profile,
        day_key,
        level,
        type,
        reason_codes,
        payload,
        created_at
      ) VALUES (
        ${String(row.id || "").trim()},
        TO_TIMESTAMP(${Math.floor(tsMs)} / 1000.0),
        ${mappedDeploymentId || null},
        ${deployment?.venue || null},
        ${symbol || null},
        ${deployment?.strategyId || null},
        ${deployment?.tuneId || null},
        ${deployment?.entrySessionProfile || null},
        ${row.dayKey ? sql`${row.dayKey}::date` : sql`NULL`},
        ${
          String(row.level || "").trim().toLowerCase() === "warn" ||
          String(row.level || "").trim().toLowerCase() === "error"
            ? String(row.level || "").trim().toLowerCase()
            : "info"
        },
        ${normalizeJournalType(row.type)},
        ${normalizeReasonCodes(row.reasonCodes || [])},
        ${JSON.stringify(payload)}::jsonb,
        NOW()
      )
      ON CONFLICT(id)
      DO NOTHING
      RETURNING id;
    `);
    if (insertedRow?.id) inserted += 1;
    else skipped += 1;
  }

  return {
    processed: rows.length,
    inserted,
    updated: 0,
    skipped,
  };
}

export function buildScalpV2JobResult(params: {
  jobKind: ScalpV2JobKind;
  processed: number;
  succeeded: number;
  failed: number;
  pendingAfter?: number;
  busy?: boolean;
  details?: Record<string, unknown>;
}): ScalpV2JobResult {
  return {
    ok: params.failed <= 0,
    busy: Boolean(params.busy),
    jobKind: params.jobKind,
    processed: Math.max(0, Math.floor(params.processed || 0)),
    succeeded: Math.max(0, Math.floor(params.succeeded || 0)),
    failed: Math.max(0, Math.floor(params.failed || 0)),
    pendingAfter: Math.max(0, Math.floor(params.pendingAfter || 0)),
    details: params.details || {},
  };
}

export async function trimScalpV2CandidatesByBudget(params: {
  maxCandidatesTotal: number;
  maxCandidatesPerSymbol: number;
}): Promise<{
  deleted: number;
}> {
  if (!isScalpPgConfigured()) return { deleted: 0 };
  const db = scalpPrisma();
  const totalCap = Math.max(1, Math.floor(params.maxCandidatesTotal));
  const perSymbolCap = Math.max(1, Math.floor(params.maxCandidatesPerSymbol));

  // Delete overflow per symbol first.
  const deletedPerSymbol = await db.$executeRaw(sql`
    WITH ranked AS (
      SELECT
        id,
        ROW_NUMBER() OVER (
          PARTITION BY venue, symbol
          ORDER BY score DESC, updated_at DESC
        ) AS rn
      FROM scalp_v2_candidates
    )
    DELETE FROM scalp_v2_candidates c
    USING ranked r
    WHERE c.id = r.id
      AND r.rn > ${perSymbolCap};
  `);

  const deletedGlobal = await db.$executeRaw(sql`
    WITH ranked AS (
      SELECT
        id,
        ROW_NUMBER() OVER (ORDER BY score DESC, updated_at DESC) AS rn
      FROM scalp_v2_candidates
    )
    DELETE FROM scalp_v2_candidates c
    USING ranked r
    WHERE c.id = r.id
      AND r.rn > ${totalCap};
  `);

  return { deleted: deletedPerSymbol + deletedGlobal };
}

export async function enforceScalpV2EnabledCap(params: {
  maxEnabledDeployments: number;
}): Promise<{ demoted: number }> {
  if (!isScalpPgConfigured()) return { demoted: 0 };
  const db = scalpPrisma();
  const cap = Math.max(1, Math.floor(params.maxEnabledDeployments));
  const demoted = await db.$executeRaw(sql`
    WITH ranked AS (
      SELECT
        deployment_id,
        ROW_NUMBER() OVER (
          ORDER BY
            CASE WHEN live_mode = 'live' THEN 0 ELSE 1 END,
            updated_at DESC
        ) AS rn
      FROM scalp_v2_deployments
      WHERE enabled = TRUE
    )
    UPDATE scalp_v2_deployments d
    SET
      enabled = FALSE,
      live_mode = 'shadow',
      updated_at = NOW()
    FROM ranked r
    WHERE d.deployment_id = r.deployment_id
      AND r.rn > ${cap};
  `);

  return { demoted };
}

export async function snapshotScalpV2DailyMetrics(params: {
  dayKey?: string;
} = {}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  const dayKey = String(params.dayKey || "").trim();

  if (dayKey) {
    await db.$executeRaw(sql`
      INSERT INTO scalp_v2_metrics_daily(
        day_key,
        deployment_id,
        venue,
        symbol,
        strategy_id,
        tune_id,
        entry_session_profile,
        trades,
        wins,
        losses,
        net_r,
        net_pnl_usd,
        updated_at
      )
      SELECT
        ${dayKey}::date,
        l.deployment_id,
        l.venue,
        l.symbol,
        l.strategy_id,
        l.tune_id,
        l.entry_session_profile,
        COUNT(*)::int AS trades,
        COUNT(*) FILTER (WHERE l.r_multiple > 0)::int AS wins,
        COUNT(*) FILTER (WHERE l.r_multiple <= 0)::int AS losses,
        COALESCE(SUM(l.r_multiple), 0)::double precision AS net_r,
        COALESCE(SUM(l.pnl_usd), 0)::double precision AS net_pnl_usd,
        NOW()
      FROM scalp_v2_ledger l
      WHERE l.ts_exit >= ${dayKey}::date
        AND l.ts_exit < ${dayKey}::date + INTERVAL '1 day'
      GROUP BY
        l.deployment_id,
        l.venue,
        l.symbol,
        l.strategy_id,
        l.tune_id,
        l.entry_session_profile
      ON CONFLICT(day_key, deployment_id)
      DO UPDATE SET
        trades = EXCLUDED.trades,
        wins = EXCLUDED.wins,
        losses = EXCLUDED.losses,
        net_r = EXCLUDED.net_r,
        net_pnl_usd = EXCLUDED.net_pnl_usd,
        updated_at = NOW();
    `);
    return;
  }

  await db.$executeRaw(sql`
    INSERT INTO scalp_v2_metrics_daily(
      day_key,
      deployment_id,
      venue,
      symbol,
      strategy_id,
      tune_id,
      entry_session_profile,
      trades,
      wins,
      losses,
      net_r,
      net_pnl_usd,
      updated_at
    )
    SELECT
      DATE_TRUNC('day', l.ts_exit)::date AS day_key,
      l.deployment_id,
      l.venue,
      l.symbol,
      l.strategy_id,
      l.tune_id,
      l.entry_session_profile,
      COUNT(*)::int AS trades,
      COUNT(*) FILTER (WHERE l.r_multiple > 0)::int AS wins,
      COUNT(*) FILTER (WHERE l.r_multiple <= 0)::int AS losses,
      COALESCE(SUM(l.r_multiple), 0)::double precision AS net_r,
      COALESCE(SUM(l.pnl_usd), 0)::double precision AS net_pnl_usd,
      NOW()
    FROM scalp_v2_ledger l
    WHERE l.ts_exit >= NOW() - INTERVAL '35 days'
    GROUP BY
      DATE_TRUNC('day', l.ts_exit)::date,
      l.deployment_id,
      l.venue,
      l.symbol,
      l.strategy_id,
      l.tune_id,
      l.entry_session_profile
    ON CONFLICT(day_key, deployment_id)
    DO UPDATE SET
      trades = EXCLUDED.trades,
      wins = EXCLUDED.wins,
      losses = EXCLUDED.losses,
      net_r = EXCLUDED.net_r,
      net_pnl_usd = EXCLUDED.net_pnl_usd,
      updated_at = NOW();
  `);
}

export async function listScalpV2RecentLedger(params: {
  limit?: number;
} = {}): Promise<
  Array<{
    id: string;
    tsExitMs: number;
    deploymentId: string;
    venue: ScalpV2Venue;
    symbol: string;
    closeType: ScalpV2CloseType;
    rMultiple: number;
    reasonCodes: string[];
    sourceOfTruth: ScalpV2SourceOfTruth;
  }>
> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(10_000, Math.floor(params.limit || 500)));
  const rows = await db.$queryRaw<
    Array<{
      id: string;
      tsExitMs: bigint;
      deploymentId: string;
      venue: string;
      symbol: string;
      closeType: string;
      rMultiple: number;
      reasonCodes: string[];
      sourceOfTruth: string;
    }>
  >(sql`
    SELECT
      id,
      (EXTRACT(EPOCH FROM ts_exit) * 1000.0)::bigint AS "tsExitMs",
      deployment_id AS "deploymentId",
      venue,
      symbol,
      close_type AS "closeType",
      r_multiple::double precision AS "rMultiple",
      reason_codes AS "reasonCodes",
      source_of_truth AS "sourceOfTruth"
    FROM scalp_v2_ledger
    ORDER BY ts_exit DESC
    LIMIT ${limit};
  `);

  return rows.map((row) => ({
    id: String(row.id || ""),
    tsExitMs: Number(row.tsExitMs || Date.now()),
    deploymentId: String(row.deploymentId || "").trim(),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    closeType: (String(row.closeType || "manual_close").trim().toLowerCase() as ScalpV2CloseType),
    rMultiple: Number.isFinite(Number(row.rMultiple)) ? Number(row.rMultiple) : 0,
    reasonCodes: normalizeReasonCodes(row.reasonCodes || []),
    sourceOfTruth: normalizeSourceOfTruth(row.sourceOfTruth),
  }));
}

export async function aggregateScalpV2CutoverParityWindow(params: {
  sinceDays: number;
  journalLimit?: number;
  mismatchLimit?: number;
}): Promise<{
  windowDays: number;
  ledger: {
    v1Trades: number;
    v1NetR: number;
    v2Trades: number;
    v2NetR: number;
    tradeDelta: number;
    netRDelta: number;
    deploymentMismatches: Array<{
      deploymentId: string;
      v1Trades: number;
      v1NetR: number;
      v2Trades: number;
      v2NetR: number;
      tradeDelta: number;
      netRDelta: number;
    }>;
  };
  sessions: {
    v1Rows: number;
    v2Rows: number;
    rowDelta: number;
    v1LatestUpdatedAtMs: number | null;
    v2LatestUpdatedAtMs: number | null;
    v1EnabledRows: number;
    v2EnabledRows: number;
  };
  journal: {
    sampleLimit: number;
    v1Rows: number;
    v2Rows: number;
    rowDelta: number;
    v1OldestTsMs: number | null;
    v1NewestTsMs: number | null;
    v2OldestTsMs: number | null;
    v2NewestTsMs: number | null;
  };
}> {
  const sinceDays = Math.max(1, Math.min(3650, Math.floor(params.sinceDays || 30)));
  const journalLimit = Math.max(
    100,
    Math.min(50_000, Math.floor(params.journalLimit || 2_000)),
  );
  const mismatchLimit = Math.max(
    10,
    Math.min(2_000, Math.floor(params.mismatchLimit || 200)),
  );
  if (!isScalpPgConfigured()) {
    return {
      windowDays: sinceDays,
      ledger: {
        v1Trades: 0,
        v1NetR: 0,
        v2Trades: 0,
        v2NetR: 0,
        tradeDelta: 0,
        netRDelta: 0,
        deploymentMismatches: [],
      },
      sessions: {
        v1Rows: 0,
        v2Rows: 0,
        rowDelta: 0,
        v1LatestUpdatedAtMs: null,
        v2LatestUpdatedAtMs: null,
        v1EnabledRows: 0,
        v2EnabledRows: 0,
      },
      journal: {
        sampleLimit: journalLimit,
        v1Rows: 0,
        v2Rows: 0,
        rowDelta: 0,
        v1OldestTsMs: null,
        v1NewestTsMs: null,
        v2OldestTsMs: null,
        v2NewestTsMs: null,
      },
    };
  }
  const db = scalpPrisma();
  const [hasV1Ledger, hasV1Sessions, hasV1Journal] = await Promise.all([
    scalpTableExists("scalp_trade_ledger"),
    scalpTableExists("scalp_sessions"),
    scalpTableExists("scalp_journal"),
  ]);
  const [ledgerTotals] = hasV1Ledger
    ? await db.$queryRaw<
        Array<{
          v1Trades: bigint;
          v1NetR: number | null;
          v2Trades: bigint;
          v2NetR: number | null;
        }>
      >(sql`
        SELECT
          (SELECT COUNT(*)::bigint FROM scalp_trade_ledger WHERE exit_at >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v1Trades",
          (SELECT SUM(r_multiple)::double precision FROM scalp_trade_ledger WHERE exit_at >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v1NetR",
          (SELECT COUNT(*)::bigint FROM scalp_v2_ledger WHERE ts_exit >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v2Trades",
          (SELECT SUM(r_multiple)::double precision FROM scalp_v2_ledger WHERE ts_exit >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v2NetR";
      `)
    : await db.$queryRaw<
        Array<{
          v1Trades: bigint;
          v1NetR: number | null;
          v2Trades: bigint;
          v2NetR: number | null;
        }>
      >(sql`
        SELECT
          0::bigint AS "v1Trades",
          0::double precision AS "v1NetR",
          (SELECT COUNT(*)::bigint FROM scalp_v2_ledger WHERE ts_exit >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v2Trades",
          (SELECT SUM(r_multiple)::double precision FROM scalp_v2_ledger WHERE ts_exit >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v2NetR";
      `);

  const deploymentMismatches = hasV1Ledger
    ? await db.$queryRaw<
        Array<{
          deploymentId: string;
          v1Trades: bigint;
          v1NetR: number | null;
          v2Trades: bigint;
          v2NetR: number | null;
        }>
      >(sql`
        WITH v1 AS (
          SELECT
            deployment_id AS deployment_id,
            COUNT(*)::bigint AS trades,
            COALESCE(SUM(r_multiple), 0)::double precision AS net_r
          FROM scalp_trade_ledger
          WHERE exit_at >= NOW() - (${sinceDays} * INTERVAL '1 day')
          GROUP BY deployment_id
        ),
        v2 AS (
          SELECT
            deployment_id AS deployment_id,
            COUNT(*)::bigint AS trades,
            COALESCE(SUM(r_multiple), 0)::double precision AS net_r
          FROM scalp_v2_ledger
          WHERE ts_exit >= NOW() - (${sinceDays} * INTERVAL '1 day')
          GROUP BY deployment_id
        )
        SELECT
          COALESCE(v1.deployment_id, v2.deployment_id) AS "deploymentId",
          COALESCE(v1.trades, 0)::bigint AS "v1Trades",
          COALESCE(v1.net_r, 0)::double precision AS "v1NetR",
          COALESCE(v2.trades, 0)::bigint AS "v2Trades",
          COALESCE(v2.net_r, 0)::double precision AS "v2NetR"
        FROM v1
        FULL OUTER JOIN v2
          ON v1.deployment_id = v2.deployment_id
        WHERE
          COALESCE(v1.trades, 0) <> COALESCE(v2.trades, 0)
          OR ABS(COALESCE(v1.net_r, 0) - COALESCE(v2.net_r, 0)) > 1e-9
        ORDER BY
          ABS(COALESCE(v1.net_r, 0) - COALESCE(v2.net_r, 0)) DESC,
          ABS(COALESCE(v1.trades, 0) - COALESCE(v2.trades, 0)) DESC
        LIMIT ${mismatchLimit};
      `)
    : [];

  const [sessionsTotals] = hasV1Sessions
    ? await db.$queryRaw<
        Array<{
          v1Rows: bigint;
          v2Rows: bigint;
          v1LatestUpdatedAtMs: bigint | null;
          v2LatestUpdatedAtMs: bigint | null;
          v1EnabledRows: bigint;
          v2EnabledRows: bigint;
        }>
      >(sql`
        SELECT
          (SELECT COUNT(*)::bigint FROM scalp_sessions WHERE updated_at >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v1Rows",
          (SELECT COUNT(*)::bigint FROM scalp_v2_sessions WHERE updated_at >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v2Rows",
          (SELECT (EXTRACT(EPOCH FROM MAX(updated_at)) * 1000.0)::bigint FROM scalp_sessions WHERE updated_at >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v1LatestUpdatedAtMs",
          (SELECT (EXTRACT(EPOCH FROM MAX(updated_at)) * 1000.0)::bigint FROM scalp_v2_sessions WHERE updated_at >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v2LatestUpdatedAtMs",
          (
            SELECT COUNT(*)::bigint
            FROM scalp_sessions s
            INNER JOIN scalp_v2_deployments d ON d.deployment_id = s.deployment_id
            WHERE d.enabled = TRUE
              AND s.updated_at >= NOW() - (${sinceDays} * INTERVAL '1 day')
          ) AS "v1EnabledRows",
          (
            SELECT COUNT(*)::bigint
            FROM scalp_v2_sessions s
            INNER JOIN scalp_v2_deployments d ON d.deployment_id = s.deployment_id
            WHERE d.enabled = TRUE
              AND s.updated_at >= NOW() - (${sinceDays} * INTERVAL '1 day')
          ) AS "v2EnabledRows";
      `)
    : await db.$queryRaw<
        Array<{
          v1Rows: bigint;
          v2Rows: bigint;
          v1LatestUpdatedAtMs: bigint | null;
          v2LatestUpdatedAtMs: bigint | null;
          v1EnabledRows: bigint;
          v2EnabledRows: bigint;
        }>
      >(sql`
        SELECT
          0::bigint AS "v1Rows",
          (SELECT COUNT(*)::bigint FROM scalp_v2_sessions WHERE updated_at >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v2Rows",
          NULL::bigint AS "v1LatestUpdatedAtMs",
          (SELECT (EXTRACT(EPOCH FROM MAX(updated_at)) * 1000.0)::bigint FROM scalp_v2_sessions WHERE updated_at >= NOW() - (${sinceDays} * INTERVAL '1 day')) AS "v2LatestUpdatedAtMs",
          0::bigint AS "v1EnabledRows",
          (
            SELECT COUNT(*)::bigint
            FROM scalp_v2_sessions s
            INNER JOIN scalp_v2_deployments d ON d.deployment_id = s.deployment_id
            WHERE d.enabled = TRUE
              AND s.updated_at >= NOW() - (${sinceDays} * INTERVAL '1 day')
          ) AS "v2EnabledRows";
      `);

  const [v1JournalEnvelope] = hasV1Journal
    ? await db.$queryRaw<
        Array<{ rows: bigint; oldestTsMs: bigint | null; newestTsMs: bigint | null }>
      >(sql`
        WITH sample AS (
          SELECT ts
          FROM scalp_journal
          ORDER BY ts DESC
          LIMIT ${journalLimit}
        )
        SELECT
          COUNT(*)::bigint AS rows,
          (EXTRACT(EPOCH FROM MIN(ts)) * 1000.0)::bigint AS "oldestTsMs",
          (EXTRACT(EPOCH FROM MAX(ts)) * 1000.0)::bigint AS "newestTsMs"
        FROM sample;
      `)
    : [{ rows: BigInt(0), oldestTsMs: null, newestTsMs: null }];
  const [v2JournalEnvelope] = await db.$queryRaw<
    Array<{ rows: bigint; oldestTsMs: bigint | null; newestTsMs: bigint | null }>
  >(sql`
    WITH sample AS (
      SELECT ts
      FROM scalp_v2_journal
      ORDER BY ts DESC
      LIMIT ${journalLimit}
    )
    SELECT
      COUNT(*)::bigint AS rows,
      (EXTRACT(EPOCH FROM MIN(ts)) * 1000.0)::bigint AS "oldestTsMs",
      (EXTRACT(EPOCH FROM MAX(ts)) * 1000.0)::bigint AS "newestTsMs"
    FROM sample;
  `);

  const v1Trades = Number(ledgerTotals?.v1Trades || 0);
  const v1NetR = Number.isFinite(Number(ledgerTotals?.v1NetR))
    ? Number(ledgerTotals?.v1NetR)
    : 0;
  const v2Trades = Number(ledgerTotals?.v2Trades || 0);
  const v2NetR = Number.isFinite(Number(ledgerTotals?.v2NetR))
    ? Number(ledgerTotals?.v2NetR)
    : 0;

  return {
    windowDays: sinceDays,
    ledger: {
      v1Trades,
      v1NetR,
      v2Trades,
      v2NetR,
      tradeDelta: v2Trades - v1Trades,
      netRDelta: v2NetR - v1NetR,
      deploymentMismatches: deploymentMismatches.map((row) => {
        const perV1Trades = Number(row.v1Trades || 0);
        const perV2Trades = Number(row.v2Trades || 0);
        const perV1NetR = Number.isFinite(Number(row.v1NetR))
          ? Number(row.v1NetR)
          : 0;
        const perV2NetR = Number.isFinite(Number(row.v2NetR))
          ? Number(row.v2NetR)
          : 0;
        return {
          deploymentId: String(row.deploymentId || "").trim(),
          v1Trades: perV1Trades,
          v1NetR: perV1NetR,
          v2Trades: perV2Trades,
          v2NetR: perV2NetR,
          tradeDelta: perV2Trades - perV1Trades,
          netRDelta: perV2NetR - perV1NetR,
        };
      }),
    },
    sessions: {
      v1Rows: Number(sessionsTotals?.v1Rows || 0),
      v2Rows: Number(sessionsTotals?.v2Rows || 0),
      rowDelta: Number(sessionsTotals?.v2Rows || 0) - Number(sessionsTotals?.v1Rows || 0),
      v1LatestUpdatedAtMs: sessionsTotals?.v1LatestUpdatedAtMs
        ? Number(sessionsTotals.v1LatestUpdatedAtMs)
        : null,
      v2LatestUpdatedAtMs: sessionsTotals?.v2LatestUpdatedAtMs
        ? Number(sessionsTotals.v2LatestUpdatedAtMs)
        : null,
      v1EnabledRows: Number(sessionsTotals?.v1EnabledRows || 0),
      v2EnabledRows: Number(sessionsTotals?.v2EnabledRows || 0),
    },
    journal: {
      sampleLimit: journalLimit,
      v1Rows: Number(v1JournalEnvelope?.rows || 0),
      v2Rows: Number(v2JournalEnvelope?.rows || 0),
      rowDelta: Number(v2JournalEnvelope?.rows || 0) - Number(v1JournalEnvelope?.rows || 0),
      v1OldestTsMs: v1JournalEnvelope?.oldestTsMs
        ? Number(v1JournalEnvelope.oldestTsMs)
        : null,
      v1NewestTsMs: v1JournalEnvelope?.newestTsMs
        ? Number(v1JournalEnvelope.newestTsMs)
        : null,
      v2OldestTsMs: v2JournalEnvelope?.oldestTsMs
        ? Number(v2JournalEnvelope.oldestTsMs)
        : null,
      v2NewestTsMs: v2JournalEnvelope?.newestTsMs
        ? Number(v2JournalEnvelope.newestTsMs)
        : null,
    },
  };
}

export async function aggregateScalpV2ParityWindow(params: {
  sinceDays: number;
}): Promise<{
  v1Trades: number;
  v1NetR: number;
  v2Trades: number;
  v2NetR: number;
}> {
  const parity = await aggregateScalpV2CutoverParityWindow({
    sinceDays: params.sinceDays,
    journalLimit: 100,
    mismatchLimit: 20,
  });
  return {
    v1Trades: parity.ledger.v1Trades,
    v1NetR: parity.ledger.v1NetR,
    v2Trades: parity.ledger.v2Trades,
    v2NetR: parity.ledger.v2NetR,
  };
}

export { toDeploymentId };
