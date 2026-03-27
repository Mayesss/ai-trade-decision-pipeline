import crypto from "crypto";

import { isScalpPgConfigured, join, scalpPrisma, sql } from "./pg";

import { applyScalpV2FixedSeedScope, getScalpV2RuntimeConfig } from "./config";
import {
  deriveCloseTypeFromReasonCodes,
  normalizeReasonCodes,
  toDeploymentId,
  toLedgerCloseTypeFromEvent,
} from "./logic";
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
  if (normalized === "sydney") return "sydney";
  return "berlin";
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
  if (normalized === "shadow") return "shadow";
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
    seedSymbolsByVenue: {
      ...defaults.seedSymbolsByVenue,
      ...asRecord(config.seedSymbolsByVenue),
    } as ScalpV2RuntimeConfig["seedSymbolsByVenue"],
    seedLiveSymbolsByVenue: {
      ...defaults.seedLiveSymbolsByVenue,
      ...asRecord(config.seedLiveSymbolsByVenue),
    } as ScalpV2RuntimeConfig["seedLiveSymbolsByVenue"],
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

  const values = params.rows.map((row) =>
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
      status = EXCLUDED.status,
      reason_codes = EXCLUDED.reason_codes,
      metadata_json = COALESCE(scalp_v2_candidates.metadata_json, '{}'::jsonb) || EXCLUDED.metadata_json,
      updated_at = NOW();
  `);

  return params.rows.length;
}

export async function listScalpV2Candidates(params: {
  status?: ScalpV2CandidateStatus;
  limit?: number;
} = {}): Promise<ScalpV2Candidate[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(10_000, Math.floor(params.limit || 500)));

  const rows = params.status
    ? await db.$queryRaw<
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
      >(sql`
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
        WHERE status = ${params.status}
        ORDER BY score DESC, updated_at DESC
        LIMIT ${limit};
      `)
    : await db.$queryRaw<
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
      >(sql`
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
        ORDER BY score DESC, updated_at DESC
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
    createdAtMs: toMs(row.createdAt),
    updatedAtMs: toMs(row.updatedAt),
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
      WHERE id IN (${join(ids, ",")});
    `);
  } else {
    await db.$executeRaw(sql`
      UPDATE scalp_v2_candidates
      SET
        status = ${params.status},
        updated_at = NOW()
      WHERE id IN (${join(ids, ",")});
    `);
  }
  return ids.length;
}

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

  const values = params.rows.map((row) => {
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
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
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
    DO NOTHING;
  `);
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
    WHERE deployment_id IN (${join(deploymentIds)})
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
} = {}): Promise<ScalpV2ExecutionEvent[]> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const limit = Math.max(1, Math.min(5_000, Math.floor(params.limit || 300)));

  const rows = params.deploymentId
    ? await db.$queryRaw<
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
      >(sql`
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
        WHERE deployment_id = ${params.deploymentId}
        ORDER BY ts DESC
        LIMIT ${limit};
      `)
    : await db.$queryRaw<
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
      >(sql`
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
        ORDER BY ts DESC
        LIMIT ${limit};
      `);

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

export async function loadScalpV2Summary(): Promise<Record<string, unknown>> {
  if (!isScalpPgConfigured()) {
    return {
      pgConfigured: false,
      generatedAtMs: Date.now(),
      candidates: 0,
      deployments: 0,
      enabledDeployments: 0,
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
      events24h: bigint;
      ledgerRows30d: bigint;
      netR30d: number | null;
    }>
  >(sql`
    SELECT
      (SELECT COUNT(*)::bigint FROM scalp_v2_candidates) AS candidates,
      (SELECT COUNT(*)::bigint FROM scalp_v2_deployments) AS deployments,
      (SELECT COUNT(*)::bigint FROM scalp_v2_deployments WHERE enabled = TRUE) AS "enabledDeployments",
      (SELECT COUNT(*)::bigint FROM scalp_v2_execution_events WHERE ts >= NOW() - INTERVAL '24 hours') AS "events24h",
      (SELECT COUNT(*)::bigint FROM scalp_v2_ledger WHERE ts_exit >= NOW() - INTERVAL '30 days') AS "ledgerRows30d",
      (SELECT SUM(r_multiple)::double precision FROM scalp_v2_ledger WHERE ts_exit >= NOW() - INTERVAL '30 days') AS "netR30d";
  `);

  return {
    pgConfigured: true,
    generatedAtMs: Date.now(),
    candidates: Number(row?.candidates || 0),
    deployments: Number(row?.deployments || 0),
    enabledDeployments: Number(row?.enabledDeployments || 0),
    events24h: Number(row?.events24h || 0),
    ledgerRows30d: Number(row?.ledgerRows30d || 0),
    netR30d: Number.isFinite(Number(row?.netR30d)) ? Number(row?.netR30d) : 0,
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
  imported: number;
  skipped: number;
}> {
  if (!isScalpPgConfigured()) return { imported: 0, skipped: 0 };
  const db = scalpPrisma();
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

  let imported = 0;
  let skipped = 0;

  for (const row of rows) {
    const deployment = await db.$queryRaw<
      Array<{ venue: string; entrySessionProfile: string }>
    >(sql`
      SELECT
        venue,
        entry_session_profile AS "entrySessionProfile"
      FROM scalp_v2_deployments
      WHERE deployment_id = ${row.deploymentId}
      LIMIT 1;
    `);

    const venue = deployment[0]?.venue
      ? normalizeVenue(deployment[0].venue)
      : String(row.deploymentId || "").toLowerCase().startsWith("capital:")
        ? "capital"
        : "bitget";
    const entrySessionProfile = normalizeSession(deployment[0]?.entrySessionProfile || "berlin");
    const reasonCodes = normalizeReasonCodes(row.reasonCodes || []);
    const closeType = deriveCloseTypeFromReasonCodes(reasonCodes);

    try {
      await appendScalpV2LedgerRow({
        id: row.id,
        tsExitMs: Number(row.exitAtMs || Date.now()),
        deploymentId: String(row.deploymentId || "").trim(),
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
      imported += 1;
    } catch {
      skipped += 1;
    }
  }

  return { imported, skipped };
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

export async function aggregateScalpV2ParityWindow(params: {
  sinceDays: number;
}): Promise<{
  v1Trades: number;
  v1NetR: number;
  v2Trades: number;
  v2NetR: number;
}> {
  if (!isScalpPgConfigured()) {
    return { v1Trades: 0, v1NetR: 0, v2Trades: 0, v2NetR: 0 };
  }
  const db = scalpPrisma();
  const sinceDays = Math.max(1, Math.min(3650, Math.floor(params.sinceDays || 30)));
  const [row] = await db.$queryRaw<
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
  `);

  return {
    v1Trades: Number(row?.v1Trades || 0),
    v1NetR: Number.isFinite(Number(row?.v1NetR)) ? Number(row?.v1NetR) : 0,
    v2Trades: Number(row?.v2Trades || 0),
    v2NetR: Number.isFinite(Number(row?.v2NetR)) ? Number(row?.v2NetR) : 0,
  };
}

export { toDeploymentId };
