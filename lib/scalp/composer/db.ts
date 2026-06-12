import crypto from "crypto";

import { isScalpPgConfigured, join, scalpPrisma, sql } from "./pg";

import { applyScalpComposerFixedSeedScope, getScalpComposerRuntimeConfig } from "./config";
import {
  deriveCloseTypeFromReasonCodes,
  normalizeReasonCodes,
  toDeploymentId,
  toLedgerCloseTypeFromEvent,
} from "./logic";
import type { ScalpJournalEntry, ScalpSessionState } from "../types";
import type {
  ScalpComposerPatternEdge,
  ScalpComposerPatternTradeVector,
} from "./patternEvidence";
import type {
  ScalpComposerCandidate,
  ScalpComposerCandidateStatus,
  ScalpComposerCloseType,
  ScalpComposerDeployment,
  ScalpComposerEventType,
  ScalpComposerExecutionEvent,
  ScalpComposerJobKind,
  ScalpComposerJobResult,
  ScalpComposerJobStatus,
  ScalpComposerLiveMode,
  ScalpComposerResearchCursor,
  ScalpComposerResearchHighlight,
  ScalpComposerRuntimeConfig,
  ScalpComposerRiskProfile,
  ScalpComposerSession,
  ScalpComposerSourceOfTruth,
  ScalpComposerVenue,
  ScalpComposerWorkerStageId,
  ScalpComposerWorkerStageWeeklyMetrics,
} from "./types";

const RETIRED_LEGACY_COMPOSER_STRATEGY_ID = "model_guided_composer_v2";
const RETIRED_DAY_COMPOSER_STRATEGY_ID = "day_model_guided_composer_v1";
const RETIRED_COMPOSER_STRATEGY_IDS = Object.freeze([
  RETIRED_LEGACY_COMPOSER_STRATEGY_ID,
  RETIRED_DAY_COMPOSER_STRATEGY_ID,
]);
const COMPOSER_RETIRED_METADATA = Object.freeze({
  retiredBy: "composer_family_retirement",
  retiredReason: "composer_replaced_by_session_structure_composer_v1",
});

function appendVisibleCandidateWhere(
  where: string[],
  values: unknown[],
  alias = "c",
): void {
  values.push(RETIRED_COMPOSER_STRATEGY_IDS);
  where.push(`${alias}.strategy_id <> ALL($${values.length}::text[])`);
  where.push(`
    NOT EXISTS (
      SELECT 1
      FROM scalp_v2_deployments retired_d
      WHERE retired_d.venue = ${alias}.venue
        AND retired_d.symbol = ${alias}.symbol
        AND retired_d.strategy_id = ${alias}.strategy_id
        AND retired_d.tune_id = ${alias}.tune_id
        AND retired_d.entry_session_profile = ${alias}.entry_session_profile
        AND retired_d.retired_at IS NOT NULL
    )
  `);
}

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

const DAY_MS = 24 * 60 * 60_000;

function utcDayKeyFromMs(tsMs: number): string {
  const n = Math.floor(Number(tsMs) || 0);
  const dayStart = Math.floor(Math.max(0, n) / DAY_MS) * DAY_MS;
  return new Date(dayStart).toISOString().slice(0, 10);
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
const CANDIDATE_TABLE = "scalp_v2_candidates";
let candidateResearchLeaseReady: boolean | null = null;

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

async function ensureCandidateResearchLeaseColumns(): Promise<boolean> {
  if (candidateResearchLeaseReady !== null) return candidateResearchLeaseReady;
  if (!isScalpPgConfigured()) {
    candidateResearchLeaseReady = false;
    return false;
  }
  if (!(await scalpTableExists(CANDIDATE_TABLE))) {
    candidateResearchLeaseReady = false;
    return false;
  }
  try {
    const db = scalpPrisma();
    await db.$executeRaw(sql`
      ALTER TABLE scalp_v2_candidates
        ADD COLUMN IF NOT EXISTS research_locked_by TEXT,
        ADD COLUMN IF NOT EXISTS research_claimed_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS research_lease_until TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS research_attempts INTEGER NOT NULL DEFAULT 0
    `);
    await db.$executeRaw(sql`
      CREATE INDEX IF NOT EXISTS scalp_v2_candidates_research_lease_idx
      ON scalp_v2_candidates(status, research_lease_until, score DESC, updated_at DESC)
      WHERE status = 'discovered'
    `);
    await db.$executeRaw(sql`
      CREATE INDEX IF NOT EXISTS scalp_v2_candidates_research_lock_owner_idx
      ON scalp_v2_candidates(research_locked_by, research_lease_until)
      WHERE research_locked_by IS NOT NULL
    `);
    candidateResearchLeaseReady = true;
    return true;
  } catch {
    candidateResearchLeaseReady = false;
    return false;
  }
}

function resolveScalpComposerJobLockStaleMinutes(): number {
  return Math.max(
    2,
    Math.min(
      120,
      toPositiveInt(process.env.SCALP_COMPOSER_JOB_LOCK_STALE_MINUTES, 10, 120),
    ),
  );
}

function normalizeJobDedupeScope(value: unknown): string {
  const normalized = String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9:_-]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return normalized || "singleton";
}

function normalizeVenue(value: unknown): ScalpComposerVenue {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  return normalized === "capital" ? "capital" : "bitget";
}

function normalizeSession(value: unknown): ScalpComposerSession {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "tokyo") return "tokyo";
  if (normalized === "newyork") return "newyork";
  if (normalized === "pacific") return "pacific";
  if (normalized === "sydney") return "sydney";
  return "berlin";
}

function normalizeOptionalSession(value: unknown): ScalpComposerSession | null {
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

function normalizeOptionalVenue(value: unknown): ScalpComposerVenue | null {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "bitget") return "bitget";
  if (normalized === "capital") return "capital";
  return null;
}

function normalizeLiveMode(value: unknown): ScalpComposerLiveMode {
  return String(value || "").trim().toLowerCase() === "live" ? "live" : "shadow";
}

function normalizeResearchPhase(value: unknown): ScalpComposerResearchCursor["phase"] {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "score") return "score";
  if (normalized === "validate") return "validate";
  if (normalized === "promote") return "promote";
  return "scan";
}

function normalizeCandidateStatus(value: unknown): ScalpComposerCandidateStatus {
  const normalized = String(value || "")
    .trim()
    .toLowerCase();
  if (normalized === "evaluated") return "evaluated";
  if (normalized === "promoted") return "promoted";
  if (normalized === "rejected") return "rejected";
  return "discovered";
}

function normalizeEventType(value: unknown): ScalpComposerEventType {
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

function normalizeSourceOfTruth(value: unknown): ScalpComposerSourceOfTruth {
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

function normalizeRiskProfile(value: unknown): ScalpComposerRiskProfile {
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

function parseRuntimeConfigRow(raw: unknown): ScalpComposerRuntimeConfig | null {
  if (!raw || typeof raw !== "object") return null;
  const row = raw as Record<string, unknown>;
  const defaults = getScalpComposerRuntimeConfig();
  const config = asRecord(row.configJson);

  const merged: ScalpComposerRuntimeConfig = {
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
    seedSymbolsByVenue: {
      ...defaults.seedSymbolsByVenue,
      ...asRecord(config.seedSymbolsByVenue),
    } as ScalpComposerRuntimeConfig["seedSymbolsByVenue"],
    seedLiveSymbolsByVenue: {
      ...defaults.seedLiveSymbolsByVenue,
      ...asRecord(config.seedLiveSymbolsByVenue),
    } as ScalpComposerRuntimeConfig["seedLiveSymbolsByVenue"],
  };

  const runtime: ScalpComposerRuntimeConfig = {
    ...merged,
    enabled: Boolean(merged.enabled),
    liveEnabled: Boolean(merged.liveEnabled),
    dryRunDefault: Boolean(merged.dryRunDefault),
  };
  return applyScalpComposerFixedSeedScope(runtime);
}

export async function loadScalpComposerRuntimeConfig(): Promise<ScalpComposerRuntimeConfig> {
  const defaults = getScalpComposerRuntimeConfig();
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

export async function upsertScalpComposerRuntimeConfig(
  config: ScalpComposerRuntimeConfig,
): Promise<ScalpComposerRuntimeConfig> {
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
  return loadScalpComposerRuntimeConfig();
}

export async function claimScalpComposerJob(params: {
  jobKind: ScalpComposerJobKind;
  lockOwner: string;
  dedupeScope?: string;
  allowSucceededRetry?: boolean;
}): Promise<boolean> {
  if (!isScalpPgConfigured()) return true;
  const db = scalpPrisma();
  const dedupeKey = `${params.jobKind}:${normalizeJobDedupeScope(params.dedupeScope)}`;
  const staleLockMinutes = resolveScalpComposerJobLockStaleMinutes();
  const allowSucceededRetry = params.allowSucceededRetry !== false;
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
      AND (${allowSucceededRetry} OR status <> 'succeeded')
      AND (
        status <> 'running'
        OR locked_at < NOW() - (${staleLockMinutes} * INTERVAL '1 minute')
        OR locked_by = ${params.lockOwner}
      )
    RETURNING id;
  `);
  return rows.length > 0;
}

export async function heartbeatScalpComposerJob(params: {
  jobKind: ScalpComposerJobKind;
  lockOwner: string;
  dedupeScope?: string;
  details?: Record<string, unknown>;
}): Promise<boolean> {
  if (!isScalpPgConfigured()) return true;
  const db = scalpPrisma();
  const dedupeKey = `${params.jobKind}:${normalizeJobDedupeScope(params.dedupeScope)}`;
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

export async function finalizeScalpComposerJob(params: {
  jobKind: ScalpComposerJobKind;
  lockOwner: string;
  ok: boolean;
  dedupeScope?: string;
  details?: Record<string, unknown>;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  const dedupeKey = `${params.jobKind}:${normalizeJobDedupeScope(params.dedupeScope)}`;
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

export async function upsertScalpComposerCandidates(params: {
  rows: Array<{
    venue: ScalpComposerVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: ScalpComposerSession;
    score: number;
    status: ScalpComposerCandidateStatus;
    reasonCodes?: string[];
    metadata?: Record<string, unknown>;
  }>;
}): Promise<number> {
  if (!isScalpPgConfigured() || params.rows.length === 0) return 0;
  await ensureCandidateResearchLeaseColumns().catch(() => false);
  const db = scalpPrisma();

  for (let offset = 0; offset < params.rows.length; offset += UPSERT_CANDIDATE_BATCH_SIZE) {
    const batch = params.rows.slice(offset, offset + UPSERT_CANDIDATE_BATCH_SIZE).map((row) => {
      const isRetiredComposer = RETIRED_COMPOSER_STRATEGY_IDS.includes(
        String(row.strategyId || "").trim().toLowerCase(),
      );
      if (!isRetiredComposer || row.status !== "discovered") return row;
      return {
        ...row,
        status: "rejected" as ScalpComposerCandidateStatus,
        reasonCodes: normalizeReasonCodes([
          ...(row.reasonCodes || []),
          "COMPOSER_RETIRED_DISCOVERY_BLOCKED",
        ]),
        metadata: {
          ...(row.metadata || {}),
          ...COMPOSER_RETIRED_METADATA,
          retiredAtMs: Date.now(),
        },
      };
    });
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
            AND (
              (EXCLUDED.metadata_json->>'researchWindowToTs') IS NULL
              OR (
                scalp_v2_candidates.metadata_json->'worker'->>'windowToTs'
              ) = (EXCLUDED.metadata_json->>'researchWindowToTs')
            )
            THEN scalp_v2_candidates.status
          ELSE EXCLUDED.status
        END,
        reason_codes = CASE
          WHEN EXCLUDED.status = 'discovered'
            AND scalp_v2_candidates.status IN ('evaluated', 'promoted', 'rejected')
            AND (
              (EXCLUDED.metadata_json->>'researchWindowToTs') IS NULL
              OR (
                scalp_v2_candidates.metadata_json->'worker'->>'windowToTs'
              ) = (EXCLUDED.metadata_json->>'researchWindowToTs')
            )
            THEN scalp_v2_candidates.reason_codes
          ELSE EXCLUDED.reason_codes
        END,
        research_locked_by = CASE
          WHEN EXCLUDED.status <> 'discovered' THEN NULL
          ELSE scalp_v2_candidates.research_locked_by
        END,
        research_claimed_at = CASE
          WHEN EXCLUDED.status <> 'discovered' THEN NULL
          ELSE scalp_v2_candidates.research_claimed_at
        END,
        research_lease_until = CASE
          WHEN EXCLUDED.status <> 'discovered' THEN NULL
          ELSE scalp_v2_candidates.research_lease_until
        END,
        metadata_json = (
          CASE
            WHEN EXCLUDED.status = 'discovered'
              AND scalp_v2_candidates.status IN ('evaluated', 'promoted', 'rejected')
              AND (EXCLUDED.metadata_json->>'researchWindowToTs') IS NOT NULL
              AND COALESCE(
                scalp_v2_candidates.metadata_json->'worker'->>'windowToTs',
                ''
              ) <> (EXCLUDED.metadata_json->>'researchWindowToTs')
              THEN CASE
                WHEN scalp_v2_candidates.metadata_json->'worker' IS NOT NULL
                  THEN jsonb_set(
                    COALESCE(scalp_v2_candidates.metadata_json, '{}'::jsonb) - 'worker',
                    '{previousWorker}',
                    scalp_v2_candidates.metadata_json->'worker',
                    true
                  )
                ELSE COALESCE(scalp_v2_candidates.metadata_json, '{}'::jsonb) - 'worker'
              END
            ELSE COALESCE(scalp_v2_candidates.metadata_json, '{}'::jsonb)
          END
        ) || EXCLUDED.metadata_json,
        updated_at = NOW();
    `);
  }

  return params.rows.length;
}

export async function listScalpComposerCandidates(params: {
  status?: ScalpComposerCandidateStatus;
  venue?: ScalpComposerVenue;
  session?: ScalpComposerSession;
  symbols?: string[];
  limit?: number;
} = {}): Promise<ScalpComposerCandidate[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(10_000, Math.floor(params.limit || 500)));
  const where: string[] = [];
  const values: unknown[] = [];
  const discoveredOnly = normalizeCandidateStatus(params.status) === "discovered";
  appendVisibleCandidateWhere(where, values, "c");
  if (discoveredOnly) {
    await ensureCandidateResearchLeaseColumns().catch(() => false);
  }
  if (params.status) {
    values.push(params.status);
    where.push(`c.status = $${values.length}`);
  }
  if (discoveredOnly) {
    where.push(`(c.research_lease_until IS NULL OR c.research_lease_until < NOW())`);
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
  const orderBySql = discoveredOnly
    ? `ORDER BY
        COALESCE((c.metadata_json->'previousWorker'->'stageC'->>'passed')::boolean, false) DESC,
        COALESCE((c.metadata_json->'previousWorker'->'stageA'->>'passed')::boolean, false) DESC,
        COALESCE(
          (c.metadata_json->'previousWorker'->'stageC'->>'netR')::double precision,
          (c.metadata_json->'previousWorker'->'stageA'->>'netR')::double precision,
          c.score::double precision,
          -999
        ) DESC,
        c.score DESC,
        c.updated_at DESC`
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
      researchAttempts: number;
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
        COALESCE(c.research_attempts, 0)::int AS "researchAttempts",
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
    researchAttempts: Math.max(0, Math.floor(Number(row.researchAttempts) || 0)),
    deploymentId: String(row.deploymentId || "").trim() || null,
    deploymentEnabled:
      typeof row.deploymentEnabled === "boolean"
        ? row.deploymentEnabled
        : null,
    createdAtMs: toMs(row.createdAt),
    updatedAtMs: toMs(row.updatedAt),
  }));
}

export async function claimScalpComposerResearchCandidateLeases(params: {
  candidateIds: number[];
  lockOwner: string;
  limit: number;
  leaseMs: number;
}): Promise<Set<number>> {
  if (!isScalpPgConfigured()) return new Set();
  if (!(await ensureCandidateResearchLeaseColumns())) return new Set();
  const ids = Array.from(
    new Set(
      (params.candidateIds || [])
        .map((id) => Math.floor(Number(id) || 0))
        .filter((id) => id > 0),
    ),
  );
  if (!ids.length) return new Set();
  const lockOwner = String(params.lockOwner || "").trim();
  if (!lockOwner) return new Set();
  const limit = Math.max(1, Math.min(ids.length, Math.floor(Number(params.limit) || ids.length)));
  const leaseMs = Math.max(60_000, Math.min(24 * 60 * 60 * 1000, Math.floor(Number(params.leaseMs) || 0)));
  const requestedRows = ids.map((id, idx) => sql`(${id}::bigint, ${idx}::integer)`);
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ id: bigint | number }>>(sql`
    WITH requested(id, ord) AS (
      VALUES ${join(requestedRows, ",")}
    ),
    claimable AS (
      SELECT c.id, r.ord
      FROM scalp_v2_candidates c
      INNER JOIN requested r ON r.id = c.id
      WHERE c.status = 'discovered'
        AND c.strategy_id <> ALL(${RETIRED_COMPOSER_STRATEGY_IDS}::text[])
        AND (
          c.research_lease_until IS NULL
          OR c.research_lease_until < NOW()
          OR c.research_locked_by = ${lockOwner}
        )
      ORDER BY r.ord ASC
      LIMIT ${limit}
      FOR UPDATE OF c SKIP LOCKED
    )
    UPDATE scalp_v2_candidates c
    SET
      research_locked_by = ${lockOwner},
      research_claimed_at = NOW(),
      research_lease_until = NOW() + (${leaseMs} * INTERVAL '1 millisecond'),
      research_attempts = COALESCE(c.research_attempts, 0) + 1,
      updated_at = NOW()
    FROM claimable
    WHERE c.id = claimable.id
    RETURNING c.id;
  `);
  return new Set(rows.map((row) => Math.floor(Number(row.id) || 0)).filter((id) => id > 0));
}

export async function paginateScalpComposerCandidates(params: {
  session?: ScalpComposerSession;
  venue?: ScalpComposerVenue;
  status?: ScalpComposerCandidateStatus;
  deploymentEnabled?: boolean | null;
  offset?: number;
  limit?: number;
}): Promise<{ rows: ScalpComposerCandidate[]; total: number }> {
  if (!isScalpPgConfigured()) return { rows: [], total: 0 };
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(500, Math.floor(params.limit || 100)));
  const offset = Math.max(0, Math.floor(params.offset || 0));
  const candidateWhere: string[] = [];
  const values: unknown[] = [];
  appendVisibleCandidateWhere(candidateWhere, values, "c");
  if (params.session) {
    values.push(params.session);
    candidateWhere.push(`c.entry_session_profile = $${values.length}`);
  }
  if (params.venue) {
    values.push(params.venue);
    candidateWhere.push(`c.venue = $${values.length}`);
  }
  if (params.status) {
    values.push(normalizeCandidateStatus(params.status));
    candidateWhere.push(`c.status = $${values.length}`);
  }
  const deploymentWhere: string[] = [];
  if (params.deploymentEnabled === true) {
    deploymentWhere.push(`d.enabled = TRUE`);
  } else if (params.deploymentEnabled === false) {
    deploymentWhere.push(`COALESCE(d.enabled, FALSE) = FALSE`);
  }
  const candidateWhereSql = candidateWhere.length
    ? `WHERE ${candidateWhere.join(" AND ")}`
    : "";
  const deploymentJoinSql = `
    FROM scalp_v2_candidates c
    LEFT JOIN scalp_v2_deployments d
      ON d.venue = c.venue
     AND d.symbol = c.symbol
     AND d.strategy_id = c.strategy_id
     AND d.tune_id = c.tune_id
     AND d.entry_session_profile = c.entry_session_profile
  `;
  const needsDeploymentFilter = deploymentWhere.length > 0;
  const joinedWhereSql = [...candidateWhere, ...deploymentWhere].length
    ? `WHERE ${[...candidateWhere, ...deploymentWhere].join(" AND ")}`
    : "";

  const [countRow] = await db.$queryRawUnsafe<Array<{ cnt: bigint }>>(
    needsDeploymentFilter
      ? `SELECT COUNT(*)::bigint AS cnt ${deploymentJoinSql} ${joinedWhereSql}`
      : `SELECT COUNT(*)::bigint AS cnt FROM scalp_v2_candidates c ${candidateWhereSql}`,
    ...values,
  );
  const total = Number(countRow?.cnt || 0);

  const rowValues = [...values, limit, offset];
  const limitIdx = rowValues.length - 1;
  const offsetIdx = rowValues.length;
  const totalNetRSortSql = (alias: string) => `
    COALESCE(
      (${alias}.metadata_json->'worker'->'stageC'->>'netR')::double precision,
      (${alias}.metadata_json->'worker'->'stageB'->>'netR')::double precision,
      (${alias}.metadata_json->'worker'->'stageA'->>'netR')::double precision,
      -999
    )
  `;

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
    needsDeploymentFilter
      ? `
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
        ${deploymentJoinSql}
        ${joinedWhereSql}
        ORDER BY ${totalNetRSortSql("c")} DESC, c.score DESC, c.updated_at DESC, c.id DESC
        LIMIT $${limitIdx} OFFSET $${offsetIdx};
      `
      : `
        WITH ordered_candidates AS (
          SELECT
            c.id,
            c.venue,
            c.symbol,
            c.strategy_id,
            c.tune_id,
            c.entry_session_profile,
            c.score,
            c.status,
            c.reason_codes,
            c.metadata_json,
            c.created_at,
            c.updated_at
          FROM scalp_v2_candidates c
          ${candidateWhereSql}
          ORDER BY ${totalNetRSortSql("c")} DESC, c.score DESC, c.updated_at DESC, c.id DESC
          LIMIT $${limitIdx} OFFSET $${offsetIdx}
        )
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
        FROM ordered_candidates c
        LEFT JOIN LATERAL (
          SELECT
            d.deployment_id,
            d.enabled
          FROM scalp_v2_deployments d
          WHERE d.venue = c.venue
            AND d.symbol = c.symbol
            AND d.strategy_id = c.strategy_id
            AND d.tune_id = c.tune_id
            AND d.entry_session_profile = c.entry_session_profile
          ORDER BY d.updated_at DESC
          LIMIT 1
        ) d ON TRUE
        ORDER BY ${totalNetRSortSql("c")} DESC, c.score DESC, c.updated_at DESC, c.id DESC;
      `,
    ...rowValues,
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

export async function paginateScalpComposerCandidatesForBackfill(params: {
  statuses: ScalpComposerCandidateStatus[];
  symbols?: string[];
  session?: ScalpComposerSession | null;
  venue?: ScalpComposerVenue | null;
  offset?: number;
  limit?: number;
}): Promise<{ rows: ScalpComposerCandidate[]; total: number }> {
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
 * Returns a set of "venue:symbol:strategyId:tuneId:session" keys for candidates
 * that were already backtested for the CURRENT windowToTs this week.
 * These are exact cache hits — no need to re-run at all.
 */
/** Check if warm-up was completed for the given window. */
export async function loadScalpComposerWarmUpState(params: {
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
export async function upsertScalpComposerWarmUpState(params: {
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
export async function listScalpComposerDiscoveredSymbols(): Promise<string[]> {
  if (!isScalpPgConfigured()) return [];
  await ensureCandidateResearchLeaseColumns().catch(() => false);
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ symbol: string }>>(sql`
    SELECT symbol
    FROM scalp_v2_candidates
    WHERE status = 'discovered'
      AND strategy_id <> ALL(${RETIRED_COMPOSER_STRATEGY_IDS}::text[])
      AND (research_lease_until IS NULL OR research_lease_until < NOW())
      AND NOT EXISTS (
        SELECT 1
        FROM scalp_v2_deployments retired_d
        WHERE retired_d.venue = scalp_v2_candidates.venue
          AND retired_d.symbol = scalp_v2_candidates.symbol
          AND retired_d.strategy_id = scalp_v2_candidates.strategy_id
          AND retired_d.tune_id = scalp_v2_candidates.tune_id
          AND retired_d.entry_session_profile = scalp_v2_candidates.entry_session_profile
          AND retired_d.retired_at IS NOT NULL
      )
    GROUP BY symbol
    ORDER BY
      BOOL_OR(
        COALESCE(
          (metadata_json->'previousWorker'->'stageC'->>'passed')::boolean,
          false
        )
      ) DESC,
      MAX(
        COALESCE(
          (metadata_json->'previousWorker'->'stageC'->>'netR')::double precision,
          (metadata_json->'previousWorker'->'stageA'->>'netR')::double precision,
          score::double precision,
          -999
        )
      ) DESC,
      symbol
  `);
  return rows.map((r) => r.symbol);
}

export async function countScalpComposerCandidatesByStatus(params: {
  status: ScalpComposerCandidateStatus;
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
        AND strategy_id <> ALL(${RETIRED_COMPOSER_STRATEGY_IDS}::text[])
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_deployments retired_d
          WHERE retired_d.venue = scalp_v2_candidates.venue
            AND retired_d.symbol = scalp_v2_candidates.symbol
            AND retired_d.strategy_id = scalp_v2_candidates.strategy_id
            AND retired_d.tune_id = scalp_v2_candidates.tune_id
            AND retired_d.entry_session_profile = scalp_v2_candidates.entry_session_profile
            AND retired_d.retired_at IS NOT NULL
        )
        AND symbol IN (${join(symbols)});
    `);
    return Math.max(0, Math.floor(Number(row?.cnt || 0)));
  }
  const [row] = await db.$queryRaw<Array<{ cnt: bigint | number }>>(sql`
    SELECT COUNT(*)::bigint AS cnt
    FROM scalp_v2_candidates
    WHERE status = ${status}
      AND strategy_id <> ALL(${RETIRED_COMPOSER_STRATEGY_IDS}::text[])
      AND NOT EXISTS (
        SELECT 1
        FROM scalp_v2_deployments retired_d
        WHERE retired_d.venue = scalp_v2_candidates.venue
          AND retired_d.symbol = scalp_v2_candidates.symbol
          AND retired_d.strategy_id = scalp_v2_candidates.strategy_id
          AND retired_d.tune_id = scalp_v2_candidates.tune_id
          AND retired_d.entry_session_profile = scalp_v2_candidates.entry_session_profile
          AND retired_d.retired_at IS NOT NULL
      );
  `);
  return Math.max(0, Math.floor(Number(row?.cnt || 0)));
}

export async function loadScalpComposerEvaluatedCandidateKeys(params: {
  windowToTs: number;
}): Promise<Set<string>> {
  if (!isScalpPgConfigured()) return new Set();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      session: string;
    }>
  >(sql`
    SELECT
      venue,
      symbol,
      strategy_id AS "strategyId",
      tune_id AS "tuneId",
      entry_session_profile AS "session"
    FROM scalp_v2_candidates
    WHERE status IN ('evaluated', 'promoted', 'rejected')
      AND (metadata_json->'worker'->>'windowToTs')::bigint = ${params.windowToTs}
  `);
  const keys = new Set<string>();
  for (const row of rows) {
    keys.add(
      `${row.venue}:${row.symbol}:${row.strategyId}:${row.tuneId}:${row.session}`.toLowerCase(),
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

export interface ScalpComposerScopeWindowStageStats {
  windowToTs: number;
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
  total: number;
  stageAPass: number;
  stageCPass: number;
}

/**
 * Loads previous week's backtest results for candidates evaluated with a
 * DIFFERENT windowToTs (prior week). Used for smart-skip decisions and
 * the weeklyNetR pre-filter. Only fetches the fields actually needed.
 */
export async function loadScalpComposerPreviousWeekResults(params: {
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
      strategyId: string;
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
    WITH candidate_workers AS (
      SELECT
        venue,
        symbol,
        strategy_id AS "strategyId",
        tune_id AS "tuneId",
        entry_session_profile AS "session",
        CASE
          WHEN status = 'discovered' AND metadata_json ? 'previousWorker'
            THEN metadata_json->'previousWorker'
          ELSE metadata_json->'worker'
        END AS worker_json,
        updated_at
      FROM scalp_v2_candidates
      WHERE status IN ('evaluated', 'promoted', 'rejected', 'discovered')
        ${symbolFilter}
        ${tuneFilter}
    ),
    ranked AS (
      SELECT
        venue,
        symbol,
        "strategyId",
        "tuneId",
        "session",
        (worker_json->>'windowToTs') AS "windowToTs",
        (worker_json->'stageA'->>'passed') AS "stageAPassed",
        (worker_json->'stageA'->>'netR') AS "stageANetR",
        (worker_json->'stageA'->>'trades') AS "stageATrades",
        (worker_json->'stageC'->>'passed') AS "stageCPassed",
        (worker_json->'stageC'->>'netR') AS "stageCNetR",
        (worker_json->'stageA'->'weeklyNetR') AS "stageAWeeklyNetR",
        ROW_NUMBER() OVER (
          PARTITION BY venue, symbol, "strategyId", "tuneId", "session"
          ORDER BY (worker_json->>'windowToTs')::bigint DESC, updated_at DESC
        ) AS rn
      FROM candidate_workers
      WHERE worker_json->>'windowToTs' IS NOT NULL
        AND (worker_json->>'windowToTs')::bigint != ${params.currentWindowToTs}
        AND worker_json->'stageA' IS NOT NULL
    )
    SELECT
      venue,
      symbol,
      "strategyId",
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
    const key =
      `${row.venue}:${row.symbol}:${row.strategyId}:${row.tuneId}:${row.session}`.toLowerCase();
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
const PATTERN_TRADE_VECTOR_TABLE = "scalp_v2_pattern_trade_vectors";
const PATTERN_EDGE_TABLE = "scalp_v2_pattern_edges";
let patternTradeVectorTableExists: boolean | null = null;
let patternEdgeTableExists: boolean | null = null;

async function ensureWeeklyCacheTable(): Promise<boolean> {
  if (weeklyCacheTableExists !== null) return weeklyCacheTableExists;
  const exists = await scalpTableExists(WEEKLY_CACHE_TABLE);
  weeklyCacheTableExists = exists;
  return exists;
}

async function ensurePatternTradeVectorTable(): Promise<boolean> {
  if (patternTradeVectorTableExists !== null) return patternTradeVectorTableExists;
  const exists = await scalpTableExists(PATTERN_TRADE_VECTOR_TABLE);
  patternTradeVectorTableExists = exists;
  return exists;
}

async function ensurePatternEdgeTable(): Promise<boolean> {
  if (patternEdgeTableExists !== null) return patternEdgeTableExists;
  const exists = await scalpTableExists(PATTERN_EDGE_TABLE);
  patternEdgeTableExists = exists;
  return exists;
}

function normalizeStageId(value: unknown): ScalpComposerWorkerStageId | null {
  const s = String(value || "").trim().toLowerCase();
  if (s === "a" || s === "b" || s === "c") return s;
  return null;
}

function normalizeWeeklyCacheMetrics(
  value: unknown,
): ScalpComposerWorkerStageWeeklyMetrics {
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
export async function loadScalpComposerWeeklyCache(params: {
  keys: Array<{
    venue: ScalpComposerVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    session: ScalpComposerSession;
    stageId: ScalpComposerWorkerStageId;
  }>;
  fromWeekStartTs: number;
  toWeekStartTs: number;
}): Promise<Map<string, Map<number, ScalpComposerWorkerStageWeeklyMetrics>>> {
  const out = new Map<string, Map<number, ScalpComposerWorkerStageWeeklyMetrics>>();
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
 * Load cached per-week metrics for candidate keys, independent of worker stage.
 *
 * The replay metrics are candidate-specific, not stage-specific.  New writes use
 * the existing table with stage_id='a' as the canonical row, while this reader
 * can still fall back to older duplicated stage rows.
 */
export async function loadScalpComposerCandidateWeeklyCache(params: {
  keys: Array<{
    venue: ScalpComposerVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    session: ScalpComposerSession;
  }>;
  fromWeekStartTs: number;
  toWeekStartTs: number;
}): Promise<Map<string, Map<number, ScalpComposerWorkerStageWeeklyMetrics>>> {
  const out = new Map<string, Map<number, ScalpComposerWorkerStageWeeklyMetrics>>();
  if (!isScalpPgConfigured() || !params.keys.length) return out;
  if (!(await ensureWeeklyCacheTable())) return out;
  const fromTs = Math.floor(Number(params.fromWeekStartTs) || 0);
  const toTs = Math.floor(Number(params.toWeekStartTs) || 0);
  if (toTs <= fromTs) return out;

  const db = scalpPrisma();
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
        ${normalizeSession(k.session)}
      )`,
    );
    const rows = await db.$queryRaw<
      Array<{
        venue: string;
        symbol: string;
        strategyId: string;
        tuneId: string;
        session: string;
        weekStartTs: string | number;
        metricsJson: unknown;
      }>
    >(sql`
      WITH keys(venue, symbol, strategy_id, tune_id, entry_session_profile) AS (
        VALUES ${join(keyRows, ",")}
      ),
      ranked AS (
        SELECT
          c.venue,
          c.symbol,
          c.strategy_id AS "strategyId",
          c.tune_id AS "tuneId",
          c.entry_session_profile AS "session",
          c.week_start_ts AS "weekStartTs",
          c.metrics_json AS "metricsJson",
          ROW_NUMBER() OVER (
            PARTITION BY
              c.venue,
              c.symbol,
              c.strategy_id,
              c.tune_id,
              c.entry_session_profile,
              c.week_start_ts
            ORDER BY
              CASE WHEN c.stage_id = 'a' THEN 0 ELSE 1 END,
              c.updated_at DESC
          ) AS rn
        FROM scalp_v2_worker_stage_weekly_cache c
        INNER JOIN keys k
          ON c.venue = k.venue
         AND c.symbol = k.symbol
         AND c.strategy_id = k.strategy_id
         AND c.tune_id = k.tune_id
         AND c.entry_session_profile = k.entry_session_profile
        WHERE c.week_start_ts >= ${fromTs}
          AND c.week_start_ts < ${toTs}
      )
      SELECT
        venue,
        symbol,
        "strategyId",
        "tuneId",
        "session",
        "weekStartTs",
        "metricsJson"
      FROM ranked
      WHERE rn = 1
      ORDER BY "weekStartTs" ASC;
    `);
    for (const row of rows) {
      const key = `${normalizeVenue(row.venue)}:${String(row.symbol || "").trim().toUpperCase()}:${String(row.strategyId || "").trim().toLowerCase()}:${String(row.tuneId || "").trim().toLowerCase()}:${normalizeSession(row.session)}`.toLowerCase();
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
export async function upsertScalpComposerWeeklyCache(params: {
  rows: Array<{
    venue: ScalpComposerVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    session: ScalpComposerSession;
    stageId: ScalpComposerWorkerStageId;
    weekStartTs: number;
    weekToTs: number;
    metrics: ScalpComposerWorkerStageWeeklyMetrics;
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

export async function upsertScalpComposerCandidateWeeklyCache(params: {
  rows: Array<{
    venue: ScalpComposerVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    session: ScalpComposerSession;
    weekStartTs: number;
    weekToTs: number;
    metrics: ScalpComposerWorkerStageWeeklyMetrics;
  }>;
}): Promise<number> {
  return upsertScalpComposerWeeklyCache({
    rows: params.rows.map((row) => ({
      ...row,
      stageId: "a",
    })),
  });
}

export async function listScalpComposerPatternEvidenceBackfillCandidates(params: {
  windowToTs?: number | "latest" | null;
  limit?: number;
} = {}): Promise<ScalpComposerCandidate[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(50_000, Math.floor(Number(params.limit) || 1_000)));
  let windowToTs: number | null = null;
  if (params.windowToTs === "latest" || params.windowToTs === undefined || params.windowToTs === null) {
    const [row] = await db.$queryRaw<Array<{ windowToTs: string | number | null }>>(sql`
      SELECT MAX((metadata_json->'worker'->>'windowToTs')::bigint) AS "windowToTs"
      FROM scalp_v2_candidates
      WHERE COALESCE((metadata_json->'worker'->'stageC'->>'passed')::boolean, false)
        AND metadata_json->'worker'->>'windowToTs' IS NOT NULL;
    `);
    const n = Number(row?.windowToTs);
    windowToTs = Number.isFinite(n) && n > 0 ? Math.floor(n) : null;
  } else {
    const n = Number(params.windowToTs);
    windowToTs = Number.isFinite(n) && n > 0 ? Math.floor(n) : null;
  }
  if (!windowToTs) return [];

  const rows = await db.$queryRaw<
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
      researchAttempts: number;
      deploymentId: string | null;
      deploymentEnabled: boolean | null;
      createdAt: Date;
      updatedAt: Date;
    }>
  >(sql`
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
      COALESCE(c.research_attempts, 0)::int AS "researchAttempts",
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
    WHERE COALESCE((c.metadata_json->'worker'->'stageC'->>'passed')::boolean, false)
      AND (c.metadata_json->'worker'->>'windowToTs')::bigint = ${windowToTs}
      AND c.metadata_json->'sessionComposerPlan' IS NOT NULL
    ORDER BY
      COALESCE((c.metadata_json->'v3Ranking'->'stageC'->'stats'->>'lowerBoundR')::double precision, -999) DESC,
      COALESCE((c.metadata_json->'worker'->'stageC'->>'netR')::double precision, -999) DESC,
      COALESCE((c.metadata_json->'worker'->'stageC'->>'trades')::int, 0) DESC
    LIMIT ${limit};
  `);

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
    researchAttempts: Math.max(0, Math.floor(Number(row.researchAttempts) || 0)),
    deploymentId: String(row.deploymentId || "").trim() || null,
    deploymentEnabled:
      typeof row.deploymentEnabled === "boolean" ? row.deploymentEnabled : null,
    createdAtMs: toMs(row.createdAt),
    updatedAtMs: toMs(row.updatedAt),
  }));
}

export async function replaceScalpComposerPatternTradeVectors(params: {
  identity: {
    venue: ScalpComposerVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    session: ScalpComposerSession;
    windowToTs: number;
    stageId: "c";
  };
  rows: ScalpComposerPatternTradeVector[];
}): Promise<number> {
  if (!isScalpPgConfigured()) return 0;
  if (!(await ensurePatternTradeVectorTable())) return 0;
  const db = scalpPrisma();
  const identity = {
    venue: normalizeVenue(params.identity.venue),
    symbol: String(params.identity.symbol || "").trim().toUpperCase(),
    strategyId: String(params.identity.strategyId || "").trim().toLowerCase(),
    tuneId: String(params.identity.tuneId || "").trim().toLowerCase(),
    session: normalizeSession(params.identity.session),
    windowToTs: Math.floor(Number(params.identity.windowToTs) || 0),
    stageId: "c" as const,
  };
  if (!identity.symbol || !identity.strategyId || !identity.tuneId || identity.windowToTs <= 0) {
    return 0;
  }
  await db.$executeRaw(sql`
    DELETE FROM scalp_v2_pattern_trade_vectors
    WHERE venue = ${identity.venue}
      AND symbol = ${identity.symbol}
      AND strategy_id = ${identity.strategyId}
      AND tune_id = ${identity.tuneId}
      AND entry_session_profile = ${identity.session}
      AND window_to_ts = ${identity.windowToTs}
      AND stage_id = ${identity.stageId};
  `);
  const rows = params.rows || [];
  if (!rows.length) return 0;

  let written = 0;
  const BATCH = 800;
  for (let offset = 0; offset < rows.length; offset += BATCH) {
    const batch = rows.slice(offset, offset + BATCH);
    const values = batch.map((row) => sql`(
      ${row.candidateId === null || row.candidateId === undefined ? null : Math.floor(Number(row.candidateId) || 0)},
      ${normalizeVenue(row.venue)},
      ${String(row.symbol || "").trim().toUpperCase()},
      ${String(row.strategyId || "").trim().toLowerCase()},
      ${String(row.tuneId || "").trim().toLowerCase()},
      ${normalizeSession(row.session)},
      ${Math.floor(Number(row.windowToTs) || 0)},
      ${row.stageId},
      ${Math.max(0, Math.floor(Number(row.replayTradeIndex) || 0))},
      ${String(row.behaviorFingerprint || "").trim()},
      ${String(row.patternKey || "").trim()},
      ${Math.floor(Number(row.entryTs) || 0)},
      ${Math.floor(Number(row.exitTs) || 0)},
      ${Math.floor(Number(row.bucketStartTs) || 0)},
      ${row.side === "SELL" ? "SELL" : "BUY"},
      ${String(row.exitReason || "UNKNOWN")},
      ${Number.isFinite(Number(row.rMultiple)) ? Number(row.rMultiple) : 0},
      ${Number.isFinite(Number(row.feeR)) ? Number(row.feeR) : null},
      ${Number.isFinite(Number(row.grossRMultiple)) ? Number(row.grossRMultiple) : null},
      NOW(),
      NOW()
    )`);
    await db.$executeRaw(sql`
      INSERT INTO scalp_v2_pattern_trade_vectors(
        candidate_id,
        venue,
        symbol,
        strategy_id,
        tune_id,
        entry_session_profile,
        window_to_ts,
        stage_id,
        replay_trade_index,
        behavior_fingerprint,
        pattern_key,
        entry_ts,
        exit_ts,
        bucket_start_ts,
        side,
        exit_reason,
        r_multiple,
        fee_r,
        gross_r_multiple,
        created_at,
        updated_at
      ) VALUES ${join(values, ",")}
      ON CONFLICT(
        venue,
        symbol,
        strategy_id,
        tune_id,
        entry_session_profile,
        window_to_ts,
        stage_id,
        replay_trade_index
      )
      DO UPDATE SET
        candidate_id = EXCLUDED.candidate_id,
        behavior_fingerprint = EXCLUDED.behavior_fingerprint,
        pattern_key = EXCLUDED.pattern_key,
        entry_ts = EXCLUDED.entry_ts,
        exit_ts = EXCLUDED.exit_ts,
        bucket_start_ts = EXCLUDED.bucket_start_ts,
        side = EXCLUDED.side,
        exit_reason = EXCLUDED.exit_reason,
        r_multiple = EXCLUDED.r_multiple,
        fee_r = EXCLUDED.fee_r,
        gross_r_multiple = EXCLUDED.gross_r_multiple,
        updated_at = NOW();
    `);
    written += batch.length;
  }
  return written;
}

export async function loadScalpComposerPatternTradeVectors(params: {
  windowToTs: number;
  bucketMinutes: number;
  populationScope: string;
}): Promise<ScalpComposerPatternTradeVector[]> {
  if (!isScalpPgConfigured()) return [];
  if (!(await ensurePatternTradeVectorTable())) return [];
  const windowToTs = Math.floor(Number(params.windowToTs) || 0);
  if (windowToTs <= 0) return [];
  const db = scalpPrisma();
  const rows = await db.$queryRaw<
    Array<{
      candidateId: number | bigint | null;
      venue: string;
      symbol: string;
      strategyId: string;
      tuneId: string;
      session: string;
      windowToTs: number | bigint;
      stageId: string;
      replayTradeIndex: number;
      behaviorFingerprint: string;
      patternKey: string;
      entryTs: number | bigint;
      exitTs: number | bigint;
      bucketStartTs: number | bigint;
      side: string;
      exitReason: string;
      rMultiple: number;
      feeR: number | null;
      grossRMultiple: number | null;
    }>
  >(sql`
    SELECT
      candidate_id AS "candidateId",
      venue,
      symbol,
      strategy_id AS "strategyId",
      tune_id AS "tuneId",
      entry_session_profile AS "session",
      window_to_ts AS "windowToTs",
      stage_id AS "stageId",
      replay_trade_index AS "replayTradeIndex",
      behavior_fingerprint AS "behaviorFingerprint",
      pattern_key AS "patternKey",
      entry_ts AS "entryTs",
      exit_ts AS "exitTs",
      bucket_start_ts AS "bucketStartTs",
      side,
      exit_reason AS "exitReason",
      r_multiple AS "rMultiple",
      fee_r AS "feeR",
      gross_r_multiple AS "grossRMultiple"
    FROM scalp_v2_pattern_trade_vectors
    WHERE window_to_ts = ${windowToTs}
      AND stage_id = 'c'
    ORDER BY pattern_key ASC, bucket_start_ts ASC, symbol ASC, replay_trade_index ASC;
  `);
  return rows.map((row) => ({
    candidateId: row.candidateId === null ? null : Number(row.candidateId),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    strategyId: String(row.strategyId || "").trim().toLowerCase(),
    tuneId: String(row.tuneId || "").trim().toLowerCase(),
    session: normalizeSession(row.session),
    windowToTs: Math.floor(Number(row.windowToTs) || 0),
    stageId: "c",
    replayTradeIndex: Math.max(0, Math.floor(Number(row.replayTradeIndex) || 0)),
    behaviorFingerprint: String(row.behaviorFingerprint || "").trim(),
    patternKey: String(row.patternKey || "").trim(),
    entryTs: Math.floor(Number(row.entryTs) || 0),
    exitTs: Math.floor(Number(row.exitTs) || 0),
    bucketStartTs: Math.floor(Number(row.bucketStartTs) || 0),
    side: row.side === "SELL" ? "SELL" : "BUY",
    exitReason: String(row.exitReason || "UNKNOWN"),
    rMultiple: Number.isFinite(Number(row.rMultiple)) ? Number(row.rMultiple) : 0,
    feeR: Number.isFinite(Number(row.feeR)) ? Number(row.feeR) : null,
    grossRMultiple: Number.isFinite(Number(row.grossRMultiple))
      ? Number(row.grossRMultiple)
      : null,
  }));
}

export async function upsertScalpComposerPatternEdges(params: {
  edges: ScalpComposerPatternEdge[];
}): Promise<number> {
  if (!isScalpPgConfigured() || !params.edges.length) return 0;
  if (!(await ensurePatternEdgeTable())) return 0;
  const db = scalpPrisma();
  let written = 0;
  const BATCH = 300;
  for (let offset = 0; offset < params.edges.length; offset += BATCH) {
    const batch = params.edges.slice(offset, offset + BATCH);
    const values = batch.map((edge) => sql`(
      ${edge.patternKey},
      ${normalizeVenue(edge.venue)},
      ${normalizeSession(edge.session)},
      ${edge.behaviorFingerprint},
      ${Math.floor(Number(edge.windowToTs) || 0)},
      ${Math.max(1, Math.floor(Number(edge.bucketMinutes) || 60))},
      ${edge.populationScope},
      ${Math.max(0, Math.floor(Number(edge.candidateCount) || 0))},
      ${Math.max(0, Math.floor(Number(edge.representativeCandidateCount) || 0))},
      ${Math.max(0, Math.floor(Number(edge.symbolCount) || 0))},
      ${Math.max(0, Math.floor(Number(edge.positiveSymbolCount) || 0))},
      ${Number.isFinite(Number(edge.positiveSymbolPct)) ? Number(edge.positiveSymbolPct) : 0},
      ${edge.topSymbol || null},
      ${Number.isFinite(Number(edge.topSymbolNetR)) ? Number(edge.topSymbolNetR) : 0},
      ${Number.isFinite(Number(edge.topSymbolConcentrationPct)) ? Number(edge.topSymbolConcentrationPct) : 0},
      ${Math.max(0, Math.floor(Number(edge.rawTrades) || 0))},
      ${Number.isFinite(Number(edge.rawNetR)) ? Number(edge.rawNetR) : 0},
      ${Number.isFinite(Number(edge.rawMeanR)) ? Number(edge.rawMeanR) : 0},
      ${Number.isFinite(Number(edge.rawStdR)) ? Number(edge.rawStdR) : 0},
      ${Number.isFinite(Number(edge.rawLowerBoundR)) ? Number(edge.rawLowerBoundR) : 0},
      ${Math.max(0, Math.floor(Number(edge.bucketCount) || 0))},
      ${Number.isFinite(Number(edge.bucketNetR)) ? Number(edge.bucketNetR) : 0},
      ${Number.isFinite(Number(edge.bucketMeanR)) ? Number(edge.bucketMeanR) : 0},
      ${Number.isFinite(Number(edge.bucketStdR)) ? Number(edge.bucketStdR) : 0},
      ${Number.isFinite(Number(edge.bucketLowerBoundR)) ? Number(edge.bucketLowerBoundR) : 0},
      ${Number.isFinite(Number(edge.leaveOneSymbolOutBucketLowerBoundR)) ? Number(edge.leaveOneSymbolOutBucketLowerBoundR) : null},
      ${JSON.stringify(edge.scoreJson || {})}::jsonb,
      NOW(),
      NOW()
    )`);
    await db.$executeRaw(sql`
      INSERT INTO scalp_v2_pattern_edges(
        pattern_key,
        venue,
        entry_session_profile,
        behavior_fingerprint,
        window_to_ts,
        bucket_minutes,
        population_scope,
        candidate_count,
        representative_candidate_count,
        symbol_count,
        positive_symbol_count,
        positive_symbol_pct,
        top_symbol,
        top_symbol_net_r,
        top_symbol_concentration_pct,
        raw_trades,
        raw_net_r,
        raw_mean_r,
        raw_std_r,
        raw_lower_bound_r,
        bucket_count,
        bucket_net_r,
        bucket_mean_r,
        bucket_std_r,
        bucket_lower_bound_r,
        leave_one_symbol_out_bucket_lower_bound_r,
        score_json,
        created_at,
        updated_at
      ) VALUES ${join(values, ",")}
      ON CONFLICT(pattern_key, window_to_ts, bucket_minutes, population_scope)
      DO UPDATE SET
        venue = EXCLUDED.venue,
        entry_session_profile = EXCLUDED.entry_session_profile,
        behavior_fingerprint = EXCLUDED.behavior_fingerprint,
        candidate_count = EXCLUDED.candidate_count,
        representative_candidate_count = EXCLUDED.representative_candidate_count,
        symbol_count = EXCLUDED.symbol_count,
        positive_symbol_count = EXCLUDED.positive_symbol_count,
        positive_symbol_pct = EXCLUDED.positive_symbol_pct,
        top_symbol = EXCLUDED.top_symbol,
        top_symbol_net_r = EXCLUDED.top_symbol_net_r,
        top_symbol_concentration_pct = EXCLUDED.top_symbol_concentration_pct,
        raw_trades = EXCLUDED.raw_trades,
        raw_net_r = EXCLUDED.raw_net_r,
        raw_mean_r = EXCLUDED.raw_mean_r,
        raw_std_r = EXCLUDED.raw_std_r,
        raw_lower_bound_r = EXCLUDED.raw_lower_bound_r,
        bucket_count = EXCLUDED.bucket_count,
        bucket_net_r = EXCLUDED.bucket_net_r,
        bucket_mean_r = EXCLUDED.bucket_mean_r,
        bucket_std_r = EXCLUDED.bucket_std_r,
        bucket_lower_bound_r = EXCLUDED.bucket_lower_bound_r,
        leave_one_symbol_out_bucket_lower_bound_r = EXCLUDED.leave_one_symbol_out_bucket_lower_bound_r,
        score_json = EXCLUDED.score_json,
        updated_at = NOW();
    `);
    written += batch.length;
  }
  return written;
}

/**
 * Delete cache rows with week_start_ts older than the given timestamp.
 */
export async function pruneScalpComposerWeeklyCache(params: {
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

export async function loadScalpComposerScopeWindowStageStats(params: {
  latestWindowCount?: number;
} = {}): Promise<ScalpComposerScopeWindowStageStats[]> {
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

export async function updateScalpComposerCandidateStatuses(params: {
  ids: number[];
  status: ScalpComposerCandidateStatus;
  metadataPatch?: Record<string, unknown>;
}): Promise<number> {
  if (!isScalpPgConfigured() || params.ids.length === 0) return 0;
  await ensureCandidateResearchLeaseColumns().catch(() => false);
  const db = scalpPrisma();
  const ids = Array.from(new Set(params.ids.map((id) => Math.floor(id)).filter((id) => id > 0)));
  if (!ids.length) return 0;
  const discoveryAllowedFilter =
    params.status === "discovered"
      ? sql`AND strategy_id <> ALL(${RETIRED_COMPOSER_STRATEGY_IDS}::text[])`
      : sql``;

  if (params.metadataPatch && Object.keys(params.metadataPatch).length > 0) {
    await db.$executeRaw(sql`
      UPDATE scalp_v2_candidates
      SET
        status = ${params.status},
        metadata_json = COALESCE(metadata_json, '{}'::jsonb) || ${JSON.stringify(params.metadataPatch)}::jsonb,
        research_locked_by = CASE WHEN ${params.status} <> 'discovered' THEN NULL ELSE research_locked_by END,
        research_claimed_at = CASE WHEN ${params.status} <> 'discovered' THEN NULL ELSE research_claimed_at END,
        research_lease_until = CASE WHEN ${params.status} <> 'discovered' THEN NULL ELSE research_lease_until END,
        updated_at = NOW()
      WHERE id = ANY(${ids}::int[])
        ${discoveryAllowedFilter};
    `);
  } else {
    await db.$executeRaw(sql`
      UPDATE scalp_v2_candidates
      SET
        status = ${params.status},
        research_locked_by = CASE WHEN ${params.status} <> 'discovered' THEN NULL ELSE research_locked_by END,
        research_claimed_at = CASE WHEN ${params.status} <> 'discovered' THEN NULL ELSE research_claimed_at END,
        research_lease_until = CASE WHEN ${params.status} <> 'discovered' THEN NULL ELSE research_lease_until END,
        updated_at = NOW()
      WHERE id = ANY(${ids}::int[])
        ${discoveryAllowedFilter};
    `);
  }
  if (params.status !== "discovered") return ids.length;
  const updatedRows = await db.$queryRaw<Array<{ count: bigint }>>(sql`
    SELECT COUNT(*)::bigint AS count
    FROM scalp_v2_candidates
    WHERE id = ANY(${ids}::int[])
      AND status = 'discovered'
      AND strategy_id <> ALL(${RETIRED_COMPOSER_STRATEGY_IDS}::text[]);
  `);
  return Math.max(0, Number(updatedRows[0]?.count || 0));
}

export async function backfillScalpComposerDeploymentHoldout(params: {
  rows: Array<{
    deploymentId: string;
    candidateId: number | null;
    holdout: Record<string, unknown>;
  }>;
}): Promise<number> {
  if (!isScalpPgConfigured() || params.rows.length === 0) return 0;
  const db = scalpPrisma();
  let updated = 0;
  for (const row of params.rows) {
    const holdoutJson = JSON.stringify(row.holdout);
    await db.$executeRaw(sql`
      UPDATE scalp_v2_deployments
      SET promotion_gate = jsonb_set(
            jsonb_set(
              CASE WHEN promotion_gate ? 'worker' THEN promotion_gate
                   ELSE jsonb_set(promotion_gate, '{worker}', '{}'::jsonb, true)
              END,
              '{worker,holdout}', ${holdoutJson}::jsonb, true
            ),
            '{v3ValidationStatus}', '"validated"'::jsonb, true
          ),
          updated_at = NOW()
      WHERE deployment_id = ${row.deploymentId};
    `);
    if (row.candidateId && row.candidateId > 0) {
      await db.$executeRaw(sql`
        UPDATE scalp_v2_candidates
        SET metadata_json = jsonb_set(
              CASE WHEN metadata_json ? 'worker' THEN metadata_json
                   ELSE jsonb_set(metadata_json, '{worker}', '{}'::jsonb, true)
              END,
              '{worker,holdout}', ${holdoutJson}::jsonb, true
            ),
            updated_at = NOW()
        WHERE id = ${row.candidateId};
      `);
    }
    updated += 1;
  }
  return updated;
}

const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * Requeue deployment-linked candidates for weekly rollover backtesting.
 *
 * This moves stale deployment candidates back to "discovered" so research
 * can re-evaluate them for the current completed week window.
 */
export async function requeueScalpComposerDeploymentCandidatesForWindow(params: {
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
    params.reasonCode || "SCALP_COMPOSER_REQUEUE_DEPLOYMENT_WINDOW_ROLLOVER",
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
        AND c.strategy_id <> ALL(${RETIRED_COMPOSER_STRATEGY_IDS}::text[])
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

// Bulk upsert. The ON CONFLICT clause is gated on retired_at IS NULL: if a
// row was previously retired by v5 trim-tail / cull-bottom, conflicting
// upserts (e.g., v2 pipeline regenerating the same combo as a new
// candidate) are silently no-op'd at the DB layer. This is the DB-side
// enforcement of the v5 "permanent ban" — combined with the retired_at
// exclusions in the v5 refill pool queries, it means a (venue, symbol,
// strategy_id, tune_id, entry_session_profile) tuple that has been retired
// can NEVER be resurrected through any code path that flows through this
// function. Manual reset requires clearing retired_at directly in SQL.
export async function upsertScalpComposerDeployments(params: {
  rows: Array<{
    candidateId: number | null;
    venue: ScalpComposerVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: ScalpComposerSession;
    enabled: boolean;
    liveMode: ScalpComposerLiveMode;
    promotionGate?: Record<string, unknown>;
    riskProfile: ScalpComposerRiskProfile;
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
        last_promoted_at = CASE WHEN EXCLUDED.enabled THEN NOW() ELSE scalp_v2_deployments.last_promoted_at END
      WHERE scalp_v2_deployments.retired_at IS NULL;
    `);
  }

  return params.rows.length;
}

export async function orphanRetiredLegacyComposerDeployments(params: {
  nowMs?: number;
} = {}): Promise<{
  updated: number;
  orphanedFlat: number;
  openManagementOnly: number;
}> {
  if (!isScalpPgConfigured()) {
    return { updated: 0, orphanedFlat: 0, openManagementOnly: 0 };
  }
  const db = scalpPrisma();
  const nowMs = Math.max(0, Math.floor(Number(params.nowMs) || Date.now()));
  const rows = await db.$queryRaw<
    Array<{
      updated: bigint | number;
      orphanedFlat: bigint | number;
      openManagementOnly: bigint | number;
    }>
  >(sql`
    WITH legacy AS (
      SELECT
        d.deployment_id,
        EXISTS (
          SELECT 1
          FROM scalp_v2_positions p
          WHERE p.deployment_id = d.deployment_id
            AND p.status = 'open'
        ) AS has_open_position
      FROM scalp_v2_deployments d
      WHERE d.strategy_id = ANY(${RETIRED_COMPOSER_STRATEGY_IDS}::text[])
    ),
    updated AS (
      UPDATE scalp_v2_deployments d
      SET
        candidate_id = CASE WHEN legacy.has_open_position THEN d.candidate_id ELSE NULL END,
        enabled = CASE WHEN legacy.has_open_position THEN d.enabled ELSE FALSE END,
        live_mode = CASE WHEN legacy.has_open_position THEN d.live_mode ELSE 'shadow' END,
        v5_evaluated_at = NULL,
        v5_lease_until = NULL,
        v5_cell_evidence = NULL,
        v5_enabled = FALSE,
        v5_replay_checkpoint = NULL,
        retired_at = CASE
          WHEN legacy.has_open_position THEN d.retired_at
          ELSE COALESCE(d.retired_at, NOW())
        END,
        promotion_gate = COALESCE(d.promotion_gate, '{}'::jsonb) || jsonb_build_object(
          'eligible', false,
          'shadowEligible', false,
          'reason', CASE
            WHEN legacy.has_open_position THEN 'composer_retired_management_only'
            ELSE 'composer_retired_orphaned'
          END,
          'source', 'composer_family_retirement',
          'retiredAtMs', ${nowMs}::bigint,
          'entryBlockReasonCodes', jsonb_build_array('COMPOSER_RETIRED_NEW_ENTRIES_BLOCKED'),
          'brokerSeat', jsonb_build_object('status', 'management_only'),
          'lifecycle', jsonb_build_object('state', 'retired')
        ),
        updated_at = NOW()
      FROM legacy
      WHERE d.deployment_id = legacy.deployment_id
        AND (
          d.candidate_id IS NOT NULL
          OR d.enabled IS TRUE
          OR d.live_mode <> 'shadow'
          OR d.v5_evaluated_at IS NOT NULL
          OR d.v5_lease_until IS NOT NULL
          OR d.v5_cell_evidence IS NOT NULL
          OR d.v5_enabled IS TRUE
          OR d.v5_replay_checkpoint IS NOT NULL
          OR d.retired_at IS NULL
          OR COALESCE(d.promotion_gate->>'reason', '') NOT IN (
            'legacy_composer_retired_management_only',
            'legacy_composer_retired_orphaned',
            'composer_retired_management_only',
            'composer_retired_orphaned'
          )
        )
      RETURNING legacy.has_open_position
    )
    SELECT
      COUNT(*)::bigint AS updated,
      COUNT(*) FILTER (WHERE NOT has_open_position)::bigint AS "orphanedFlat",
      COUNT(*) FILTER (WHERE has_open_position)::bigint AS "openManagementOnly"
    FROM updated;
  `);
  const row = rows[0];
  return {
    updated: Math.max(0, Math.floor(Number(row?.updated || 0))),
    orphanedFlat: Math.max(0, Math.floor(Number(row?.orphanedFlat || 0))),
    openManagementOnly: Math.max(0, Math.floor(Number(row?.openManagementOnly || 0))),
  };
}

export async function listScalpComposerDeployments(params: {
  enabledOnly?: boolean;
  liveOnly?: boolean;
  includeRetired?: boolean;
  venue?: ScalpComposerVenue;
  session?: ScalpComposerSession;
  compactPromotionGate?: boolean;
  limit?: number;
} = {}): Promise<ScalpComposerDeployment[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(10_000, Math.floor(params.limit || 500)));
  const where: string[] = [];
  const values: unknown[] = [];

  if (!params.includeRetired) {
    where.push(`retired_at IS NULL`);
  }
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
  const promotionGateSelect = params.compactPromotionGate
    ? `
        jsonb_strip_nulls(jsonb_build_object(
          'eligible', promotion_gate->'eligible',
          'shadowEligible', promotion_gate->'shadowEligible',
          'shortlistIncluded', promotion_gate->'shortlistIncluded',
          'droppedByBudget', promotion_gate->'droppedByBudget',
          'strictSessionEvidence', promotion_gate->'strictSessionEvidence',
          'score', promotion_gate->'score',
          'reason', promotion_gate->'reason',
          'lifecycle', promotion_gate->'lifecycle',
          'forwardValidation', promotion_gate->'forwardValidation',
          'holdout', promotion_gate->'holdout',
          'drift', promotion_gate->'drift',
          'v3ValidationStatus', promotion_gate->'v3ValidationStatus',
          'regimeEnvelope', promotion_gate->'regimeEnvelope',
          'worker', CASE
            WHEN promotion_gate->'worker'->'holdout' IS NOT NULL
              THEN jsonb_build_object('holdout', promotion_gate->'worker'->'holdout')
            ELSE NULL
          END,
          'v3TemporalFilter', COALESCE(promotion_gate->'v3TemporalFilter', promotion_gate->'metadata'->'v3TemporalFilter'),
          'brokerSeat', COALESCE(promotion_gate->'brokerSeat', promotion_gate->'metadata'->'brokerSeat'),
          'entryBlockReasonCodes', COALESCE(promotion_gate->'entryBlockReasonCodes', promotion_gate->'metadata'->'entryBlockReasonCodes'),
          'v3Ranking', COALESCE(promotion_gate->'v3Ranking', promotion_gate->'metadata'->'v3Ranking')
        )) AS "promotionGate",
        risk_profile AS "riskProfile"
      `
    : `
        promotion_gate AS "promotionGate",
        risk_profile AS "riskProfile"
      `;

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
        ${promotionGateSelect},
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

export async function loadScalpComposerDeploymentById(
  deploymentIdRaw: string,
): Promise<ScalpComposerDeployment | null> {
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

export async function setScalpComposerDeploymentEnabled(params: {
  deploymentId: string;
  enabled: boolean;
  liveMode?: ScalpComposerLiveMode;
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

export async function appendScalpComposerExecutionEvent(
  event: ScalpComposerExecutionEvent,
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

  const rawPnlUsd = event.rawPayload.pnlUsd;
  await appendScalpComposerLedgerRow({
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
      rawPnlUsd !== null &&
      rawPnlUsd !== undefined &&
      rawPnlUsd !== "" &&
      Number.isFinite(Number(rawPnlUsd))
        ? Number(rawPnlUsd)
        : null,
    sourceOfTruth: event.sourceOfTruth,
    reasonCodes: event.reasonCodes,
    rawPayload: event.rawPayload,
  });
}

export async function appendScalpComposerLedgerRow(row: {
  id: string;
  tsExitMs: number;
  deploymentId: string;
  venue: ScalpComposerVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpComposerSession;
  entryRef: string | null;
  exitRef: string | null;
  closeType: ScalpComposerCloseType;
  rMultiple: number;
  pnlUsd: number | null;
  sourceOfTruth: ScalpComposerSourceOfTruth;
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
  const wasInserted = inserted.length > 0;
  if (wasInserted) {
    await snapshotScalpComposerDailyMetrics({ dayKey: utcDayKeyFromMs(row.tsExitMs) }).catch(
      () => undefined,
    );
  }
  return wasInserted;
}

export async function listScalpComposerLedgerRows(params: {
  deploymentIds: string[];
  fromTsMs: number;
  toTsMs: number;
  limit?: number;
}): Promise<
  Array<{
    deploymentId: string;
    tsExitMs: number;
    entrySessionProfile: ScalpComposerSession;
    venue: ScalpComposerVenue;
    symbol: string;
    closeType: ScalpComposerCloseType;
    rMultiple: number;
    pnlUsd: number | null;
    sourceOfTruth: ScalpComposerSourceOfTruth;
    reasonCodes: string[];
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
      venue: string;
      symbol: string;
      closeType: string;
      rMultiple: number;
      pnlUsd: number | null;
      sourceOfTruth: string;
      reasonCodes: string[];
    }>
  >(sql`
    SELECT
      deployment_id AS "deploymentId",
      (EXTRACT(EPOCH FROM ts_exit) * 1000.0)::bigint AS "tsExitMs",
      entry_session_profile AS "entrySessionProfile",
      venue,
      symbol,
      close_type AS "closeType",
      r_multiple::double precision AS "rMultiple",
      pnl_usd::double precision AS "pnlUsd",
      source_of_truth AS "sourceOfTruth",
      reason_codes AS "reasonCodes"
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
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    closeType: (String(row.closeType || "manual_close").trim().toLowerCase() as ScalpComposerCloseType),
    rMultiple: Number.isFinite(Number(row.rMultiple)) ? Number(row.rMultiple) : 0,
    pnlUsd:
      row.pnlUsd === null || row.pnlUsd === undefined
        ? null
        : Number.isFinite(Number(row.pnlUsd))
          ? Number(row.pnlUsd)
          : null,
    sourceOfTruth: normalizeSourceOfTruth(row.sourceOfTruth),
    reasonCodes: normalizeReasonCodes(row.reasonCodes || []),
  }));
}

export async function upsertScalpComposerPositionSnapshot(params: {
  deploymentId: string;
  venue: ScalpComposerVenue;
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

export async function listScalpComposerOpenPositions(): Promise<
  Array<{
    deploymentId: string;
    venue: ScalpComposerVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: ScalpComposerSession;
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
      strategyId: string | null;
      tuneId: string | null;
      entrySessionProfile: string | null;
      side: string | null;
      dealId: string | null;
      dealReference: string | null;
      updatedAt: Date;
    }>
  >(sql`
    SELECT
      p.deployment_id AS "deploymentId",
      p.venue,
      p.symbol,
      d.strategy_id AS "strategyId",
      d.tune_id AS "tuneId",
      d.entry_session_profile AS "entrySessionProfile",
      p.side,
      p.deal_id AS "dealId",
      p.deal_reference AS "dealReference",
      p.updated_at AS "updatedAt"
    FROM scalp_v2_positions p
    LEFT JOIN scalp_v2_deployments d
      ON d.deployment_id = p.deployment_id
    WHERE p.status = 'open'
    ORDER BY p.updated_at DESC;
  `);
  return rows.map((row) => ({
    deploymentId: String(row.deploymentId || "").trim(),
    venue: normalizeVenue(row.venue),
    symbol: String(row.symbol || "").trim().toUpperCase(),
    strategyId: String(row.strategyId || "unknown").trim() || "unknown",
    tuneId: String(row.tuneId || "unknown").trim() || "unknown",
    entrySessionProfile: normalizeSession(row.entrySessionProfile),
    side: row.side === "long" || row.side === "short" ? row.side : null,
    dealId: row.dealId || null,
    dealReference: row.dealReference || null,
    updatedAtMs: toMs(row.updatedAt),
  }));
}

export async function listScalpComposerExecutionEvents(params: {
  limit?: number;
  deploymentId?: string;
  venue?: ScalpComposerVenue;
  session?: ScalpComposerSession;
} = {}): Promise<ScalpComposerExecutionEvent[]> {
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

export async function listScalpComposerSessionSnapshots(params: {
  deploymentIds?: string[];
  venue?: ScalpComposerVenue;
  session?: ScalpComposerSession;
  limit?: number;
} = {}): Promise<
  Array<{
    deploymentId: string;
    venue: ScalpComposerVenue;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: ScalpComposerSession;
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

export async function listScalpComposerJournalRows(params: {
  limit?: number;
  venue?: ScalpComposerVenue;
  session?: ScalpComposerSession;
} = {}): Promise<
  Array<{
    id: string;
    tsMs: number;
    deploymentId: string | null;
    venue: ScalpComposerVenue | null;
    symbol: string | null;
    strategyId: string | null;
    tuneId: string | null;
    entrySessionProfile: ScalpComposerSession | null;
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

export async function loadScalpComposerSessionState(params: {
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

export async function upsertScalpComposerSessionState(
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

export async function appendScalpComposerJournalEntry(params: {
  entry: ScalpJournalEntry;
  deploymentId?: string | null;
  venue?: ScalpComposerVenue | null;
  strategyId?: string | null;
  tuneId?: string | null;
  entrySessionProfile?: ScalpComposerSession | null;
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

export async function loadScalpComposerSummary(): Promise<Record<string, unknown>> {
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
      v3HoldoutCompletedCandidates: bigint;
      v3TemporalCandidates: bigint;
      v3TemporalFloorPassedCandidates: bigint;
      v3SingleAxisTemporalResultCandidates: bigint;
      v3EnabledValidatedDeployments: bigint;
      v3PendingEnabledValidationDeployments: bigint;
      v3DriftingDeployments: bigint;
      v3LowSampleDriftDeployments: bigint;
      coverageJson: unknown;
    }>
  >(sql`
    WITH candidate_stats AS (
      SELECT
        COUNT(*)::bigint AS candidates,
        COUNT(*) FILTER (
          WHERE status = 'discovered'
        )::bigint AS "discoveredCandidates",
        COUNT(*) FILTER (WHERE status = 'evaluated')::bigint AS "evaluatedCandidates",
        COUNT(*) FILTER (WHERE status = 'promoted')::bigint AS "promotedCandidates",
        COUNT(*) FILTER (WHERE status = 'rejected')::bigint AS "rejectedCandidates"
      FROM scalp_v2_candidates c
      WHERE c.strategy_id <> ALL(${RETIRED_COMPOSER_STRATEGY_IDS}::text[])
        AND NOT EXISTS (
          SELECT 1
          FROM scalp_v2_deployments retired_d
          WHERE retired_d.venue = c.venue
            AND retired_d.symbol = c.symbol
            AND retired_d.strategy_id = c.strategy_id
            AND retired_d.tune_id = c.tune_id
            AND retired_d.entry_session_profile = c.entry_session_profile
            AND retired_d.retired_at IS NOT NULL
        )
    ),
    v3_candidate_stats AS (
      SELECT
        (SELECT COUNT(*)::bigint
         FROM scalp_v2_candidates
         WHERE metadata_json->'worker'->'holdout' IS NOT NULL
            OR metadata_json->'v3Holdout' IS NOT NULL
        ) AS "v3HoldoutCompletedCandidates",
        (SELECT COUNT(*)::bigint
         FROM scalp_v2_candidates
         WHERE metadata_json->'v3TemporalFilter'->>'variantKind' IS NOT NULL
        ) AS "v3TemporalCandidates",
        (SELECT COUNT(*)::bigint
         FROM scalp_v2_candidates
         WHERE metadata_json->'v3TemporalFilter'->>'variantKind' IS NOT NULL
           AND COALESCE((metadata_json->'v3Ranking'->'stageA'->>'variantTradeFloorPassed')::boolean, false)
        ) AS "v3TemporalFloorPassedCandidates",
        (SELECT COUNT(*)::bigint
         FROM scalp_v2_candidates
         WHERE metadata_json->'v3TemporalFilter'->>'variantKind' IS NOT NULL
           AND metadata_json->'v3TemporalFilter'->>'variantKind' <> 'slot_weekday'
           AND metadata_json->'v3Ranking'->'stageA' IS NOT NULL
        ) AS "v3SingleAxisTemporalResultCandidates"
    ),
    deployment_stats AS (
      SELECT
        COUNT(*)::bigint AS deployments,
        COUNT(*) FILTER (WHERE enabled = TRUE)::bigint AS "enabledDeployments",
        COUNT(*) FILTER (
          WHERE enabled = TRUE
            AND promotion_gate->>'v3ValidationStatus' = 'validated'
        )::bigint AS "v3EnabledValidatedDeployments",
        COUNT(*) FILTER (
          WHERE enabled = TRUE
            AND COALESCE(promotion_gate->>'v3ValidationStatus', 'pending') <> 'validated'
        )::bigint AS "v3PendingEnabledValidationDeployments",
        COUNT(*) FILTER (
          WHERE promotion_gate->'drift'->>'status' = 'drifting'
        )::bigint AS "v3DriftingDeployments",
        COUNT(*) FILTER (
          WHERE promotion_gate->'drift'->>'status' = 'low_sample'
        )::bigint AS "v3LowSampleDriftDeployments"
      FROM scalp_v2_deployments
    ),
    candidate_symbol_counts AS (
      SELECT symbol, COUNT(*)::bigint AS cnt
      FROM scalp_v2_candidates
      WHERE (metadata_json->'worker'->'stageC'->>'passed') IS NULL
        AND status NOT IN ('rejected')
      GROUP BY symbol
    ),
    deployment_symbol_counts AS (
      SELECT symbol, COUNT(*)::bigint AS cnt
      FROM scalp_v2_deployments
      GROUP BY symbol
    ),
    coverage AS (
      SELECT c.symbol, c.cnt AS c, COALESCE(d.cnt, 0) AS d
      FROM candidate_symbol_counts c
      LEFT JOIN deployment_symbol_counts d ON c.symbol = d.symbol
      WHERE c.cnt > COALESCE(d.cnt, 0)
      ORDER BY c.symbol
    )
    SELECT
      cs.candidates,
      ds.deployments,
      ds."enabledDeployments",
      cs."discoveredCandidates",
      cs."evaluatedCandidates",
      cs."promotedCandidates",
      cs."rejectedCandidates",
      0::bigint AS "events24h",
      0::bigint AS "ledgerRows30d",
      0::double precision AS "netR30d",
      v3s."v3HoldoutCompletedCandidates",
      v3s."v3TemporalCandidates",
      v3s."v3TemporalFloorPassedCandidates",
      v3s."v3SingleAxisTemporalResultCandidates",
      ds."v3EnabledValidatedDeployments",
      ds."v3PendingEnabledValidationDeployments",
      ds."v3DriftingDeployments",
      ds."v3LowSampleDriftDeployments",
      (SELECT COALESCE(jsonb_agg(jsonb_build_object('symbol', g.symbol, 'candidates', g.c, 'deployments', g.d)), '[]'::jsonb)
       FROM coverage g
      ) AS "coverageJson"
    FROM candidate_stats cs
    CROSS JOIN v3_candidate_stats v3s
    CROSS JOIN deployment_stats ds
  `);

  const symbolCoverage = (Array.isArray(row?.coverageJson) ? row.coverageJson : []).map((r: any) => ({
    symbol: String(r?.symbol || ""),
    candidates: Number(r?.candidates || 0),
    deployments: Number(r?.deployments || 0),
  }));
  const v3TemporalCandidates = Number(row?.v3TemporalCandidates || 0);
  const v3TemporalFloorPassedCandidates = Number(row?.v3TemporalFloorPassedCandidates || 0);
  const v3SingleAxisTemporalResultCandidates = Number(
    row?.v3SingleAxisTemporalResultCandidates || 0,
  );
  const v3HardGateMinCandidates = toPositiveInt(
    process.env.SCALP_EVIDENCE_HARD_GATE_MIN_CANDIDATES,
    50,
    10_000,
  );

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
    v3: {
      enabled:
        String(process.env.SCALP_COMPOSER_RESEARCH_VERSION || "v3").trim().toLowerCase() ===
        "v3",
      holdoutCompletedCandidates: Number(row?.v3HoldoutCompletedCandidates || 0),
      hardGateMinCandidates: v3HardGateMinCandidates,
      holdoutHardGateReady:
        Number(row?.v3HoldoutCompletedCandidates || 0) >= v3HardGateMinCandidates,
      weekTwoComboReadinessCount: v3SingleAxisTemporalResultCandidates,
      weekTwoComboThreshold: v3HardGateMinCandidates,
      weekTwoCombosReady: v3SingleAxisTemporalResultCandidates >= v3HardGateMinCandidates,
      temporalVariantCandidates: v3TemporalCandidates,
      temporalVariantFloorPassedCandidates: v3TemporalFloorPassedCandidates,
      temporalVariantSurvivalRatePct:
        v3TemporalCandidates > 0
          ? (v3TemporalFloorPassedCandidates / v3TemporalCandidates) * 100
          : null,
      enabledValidatedDeployments: Number(row?.v3EnabledValidatedDeployments || 0),
      pendingEnabledValidationDeployments: Number(
        row?.v3PendingEnabledValidationDeployments || 0,
      ),
      drift: {
        driftingDeployments: Number(row?.v3DriftingDeployments || 0),
        lowSampleDeployments: Number(row?.v3LowSampleDriftDeployments || 0),
        minTrades: toPositiveInt(process.env.SCALP_EVIDENCE_DRIFT_MIN_TRADES, 20, 10_000),
        minWeeks: toPositiveInt(process.env.SCALP_EVIDENCE_DRIFT_MIN_WEEKS, 2, 520),
        autoPause:
          ["1", "true", "yes", "on"].includes(
            String(process.env.SCALP_EVIDENCE_DRIFT_AUTO_PAUSE || "false")
              .trim()
              .toLowerCase(),
          ),
      },
    },
  };
}

export async function listScalpComposerJobs(params: {
  limit?: number;
} = {}): Promise<
  Array<{
    jobKind: ScalpComposerJobKind;
    status: ScalpComposerJobStatus;
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
    const status: ScalpComposerJobStatus =
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
        : String(row.jobKind || "").trim().toLowerCase() === "robustness"
            ? "robustness"
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

export async function loadScalpComposerResearchCursor(params: {
  cursorKey: string;
}): Promise<ScalpComposerResearchCursor | null> {
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

export async function upsertScalpComposerResearchCursor(params: {
  cursorKey: string;
  venue: ScalpComposerVenue;
  symbol: string;
  entrySessionProfile: ScalpComposerSession;
  phase?: ScalpComposerResearchCursor["phase"];
  lastCandidateOffset?: number;
  lastWeekStartMs?: number | null;
  progress?: Record<string, unknown>;
}): Promise<ScalpComposerResearchCursor | null> {
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
  return loadScalpComposerResearchCursor({ cursorKey });
}

export async function listScalpComposerResearchCursors(params: {
  venue?: ScalpComposerVenue;
  symbol?: string;
  entrySessionProfile?: ScalpComposerSession;
  limit?: number;
} = {}): Promise<ScalpComposerResearchCursor[]> {
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

export async function upsertScalpComposerResearchHighlights(params: {
  rows: Array<{
    candidateId: string;
    venue: ScalpComposerVenue;
    symbol: string;
    entrySessionProfile: ScalpComposerSession;
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

export async function listScalpComposerResearchHighlights(params: {
  venue?: ScalpComposerVenue;
  symbol?: string;
  entrySessionProfile?: ScalpComposerSession;
  remarkableOnly?: boolean;
  limit?: number;
} = {}): Promise<ScalpComposerResearchHighlight[]> {
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

export async function importV1LedgerIntoScalpComposer(params: {
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
    { venue: ScalpComposerVenue; entrySessionProfile: ScalpComposerSession }
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

  const inferSessionFromDeploymentId = (deploymentIdRaw: string): ScalpComposerSession => {
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
      const wasInserted = await appendScalpComposerLedgerRow({
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

type ScalpComposerDeploymentIdentity = {
  deploymentId: string;
  venue: ScalpComposerVenue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  entrySessionProfile: ScalpComposerSession;
};

async function loadScalpComposerDeploymentIdentityMap(): Promise<
  Map<string, ScalpComposerDeploymentIdentity>
> {
  const map = new Map<string, ScalpComposerDeploymentIdentity>();
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

function inferSessionFromTuneOrDeploymentId(value: unknown): ScalpComposerSession {
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
  deploymentMap: Map<string, ScalpComposerDeploymentIdentity>;
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
  const venue: ScalpComposerVenue = venueHint === "capital" ? "capital" : "bitget";
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

export async function importV1SessionsIntoScalpComposer(params: {
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
  const deploymentMap = await loadScalpComposerDeploymentIdentityMap();

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

export async function importV1JournalIntoScalpComposer(params: {
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
  const deploymentMap = await loadScalpComposerDeploymentIdentityMap();

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

export function buildScalpComposerJobResult(params: {
  jobKind: ScalpComposerJobKind;
  processed: number;
  succeeded: number;
  failed: number;
  pendingAfter?: number;
  busy?: boolean;
  details?: Record<string, unknown>;
}): ScalpComposerJobResult {
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

export async function trimScalpComposerCandidatesByBudget(params: {
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

export async function enforceScalpComposerEnabledCap(params: {
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
        AND COALESCE(promotion_gate->'brokerSeat'->>'status', '') <> 'management_only'
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

export async function snapshotScalpComposerDailyMetrics(params: {
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
        MIN(l.venue) AS venue,
        MIN(l.symbol) AS symbol,
        MIN(l.strategy_id) AS strategy_id,
        MIN(l.tune_id) AS tune_id,
        MIN(l.entry_session_profile) AS entry_session_profile,
        COUNT(*)::int AS trades,
        COUNT(*) FILTER (WHERE l.r_multiple > 0)::int AS wins,
        COUNT(*) FILTER (WHERE l.r_multiple <= 0)::int AS losses,
        COALESCE(SUM(l.r_multiple), 0)::double precision AS net_r,
        COALESCE(SUM(l.pnl_usd), 0)::double precision AS net_pnl_usd,
        NOW()
      FROM scalp_v2_ledger l
      WHERE l.ts_exit >= ${dayKey}::date
        AND l.ts_exit < ${dayKey}::date + INTERVAL '1 day'
      GROUP BY l.deployment_id
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
      MIN(l.venue) AS venue,
      MIN(l.symbol) AS symbol,
      MIN(l.strategy_id) AS strategy_id,
      MIN(l.tune_id) AS tune_id,
      MIN(l.entry_session_profile) AS entry_session_profile,
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
      l.deployment_id
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

export async function listScalpComposerRecentLedger(params: {
  limit?: number;
} = {}): Promise<
  Array<{
    id: string;
    tsExitMs: number;
    deploymentId: string;
    venue: ScalpComposerVenue;
    symbol: string;
    closeType: ScalpComposerCloseType;
    rMultiple: number;
    reasonCodes: string[];
    sourceOfTruth: ScalpComposerSourceOfTruth;
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
    closeType: (String(row.closeType || "manual_close").trim().toLowerCase() as ScalpComposerCloseType),
    rMultiple: Number.isFinite(Number(row.rMultiple)) ? Number(row.rMultiple) : 0,
    reasonCodes: normalizeReasonCodes(row.reasonCodes || []),
    sourceOfTruth: normalizeSourceOfTruth(row.sourceOfTruth),
  }));
}

export async function aggregateScalpComposerCutoverParityWindow(params: {
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

export async function aggregateScalpComposerParityWindow(params: {
  sinceDays: number;
}): Promise<{
  v1Trades: number;
  v1NetR: number;
  v2Trades: number;
  v2NetR: number;
}> {
  const parity = await aggregateScalpComposerCutoverParityWindow({
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
