import { isScalpPgConfigured, scalpPrisma } from "../scalp/pg/client";
import { join, sql } from "../scalp/pg/sql";
import type {
  ScalpV4CellId,
  ScalpV4ResearchCandidate,
  ScalpV4RegimeEnvelope,
  ScalpV4RegimeSnapshot,
  ScalpV4Venue,
} from "./types";
import { startOfUtcWeekMondayMs } from "./week";

function normalizeVenue(value: unknown): ScalpV4Venue {
  return String(value || "").trim().toLowerCase() === "capital" ? "capital" : "bitget";
}

function normalizeSymbol(value: unknown): string {
  return String(value || "").trim().toUpperCase().replace(/[^A-Z0-9._-]/g, "");
}

function asCellId(value: unknown): ScalpV4CellId {
  const normalized = String(value || "").trim();
  return normalized.startsWith("vol=") ? (normalized as ScalpV4CellId) : "unknown";
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

export function isScalpV4Enabled(): boolean {
  const raw = String(process.env.SCALP_V4_ENABLED ?? "true").trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(raw);
}

export function isScalpV4HardGateEnabled(): boolean {
  // Default ON: v4 entry-blocks are real, not shadow. Set
  // SCALP_V4_HARD_GATE_ENABLED=false to revert to shadow-only logging.
  const raw = String(process.env.SCALP_V4_HARD_GATE_ENABLED ?? "true").trim().toLowerCase();
  return !["0", "false", "no", "off"].includes(raw);
}

export async function loadScalpV4DeploymentSymbols(): Promise<Array<{ venue: ScalpV4Venue; symbol: string }>> {
  if (!isScalpPgConfigured()) return [];
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ venue: string; symbol: string }>>(sql`
    SELECT DISTINCT venue, symbol
    FROM scalp_v2_deployments
    WHERE candidate_id IS NOT NULL
    ORDER BY venue, symbol;
  `);
  return rows
    .map((row) => ({ venue: normalizeVenue(row.venue), symbol: normalizeSymbol(row.symbol) }))
    .filter((row) => Boolean(row.symbol));
}

export async function listScalpV4ResearchCandidates(params: {
  limit?: number;
} = {}): Promise<ScalpV4ResearchCandidate[]> {
  if (!isScalpPgConfigured()) return [];
  const limit = Math.max(1, Math.min(2_000, Math.floor(Number(params.limit || 100))));
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    id: bigint;
    venue: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: string;
    metadataJson: unknown;
  }>>(sql`
    SELECT
      id,
      venue,
      symbol,
      strategy_id AS "strategyId",
      tune_id AS "tuneId",
      entry_session_profile AS "entrySessionProfile",
      metadata_json AS "metadataJson"
    FROM scalp_v2_candidates
    WHERE COALESCE((metadata_json->'worker'->>'finalPass')::boolean, FALSE)
       OR COALESCE((metadata_json->'worker'->'stageC'->>'passed')::boolean, FALSE)
    ORDER BY
      COALESCE(
        (metadata_json->'worker'->'stageC'->>'netR')::double precision,
        (metadata_json->'worker'->'stageB'->>'netR')::double precision,
        (metadata_json->'worker'->'stageA'->>'netR')::double precision,
        -999
      ) DESC,
      score DESC,
      updated_at DESC,
      id DESC
    LIMIT ${limit};
  `);
  return rows
    .map((row) => ({
      id: Number(row.id),
      venue: normalizeVenue(row.venue),
      symbol: normalizeSymbol(row.symbol),
      strategyId: String(row.strategyId || "").trim().toLowerCase(),
      tuneId: String(row.tuneId || "").trim().toLowerCase() || "default",
      entrySessionProfile: String(row.entrySessionProfile || "").trim().toLowerCase(),
      metadata: asRecord(row.metadataJson),
    }))
    .filter((row) => row.id > 0 && Boolean(row.symbol) && Boolean(row.strategyId));
}

export async function loadScalpV4SymbolsWithSnapshotForWeek(params: {
  classifierVersion: string;
  weekStartMs: number;
}): Promise<Set<string>> {
  if (!isScalpPgConfigured()) return new Set();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ venue: string; symbol: string }>>(sql`
    SELECT DISTINCT venue, symbol
    FROM scalp_regime_snapshots
    WHERE classifier_version = ${params.classifierVersion}
      AND granularity = 'week'
      AND week_start = ${new Date(params.weekStartMs)};
  `);
  return new Set(rows.map((row) => `${normalizeVenue(row.venue)}:${normalizeSymbol(row.symbol)}`));
}

export function resolveScalpV4SnapshotTtlMs(venue: unknown): number {
  const fallback = normalizeVenue(venue) === "capital" ? 15 * 60_000 : 5 * 60_000;
  const n = Number(process.env.SCALP_V4_SNAPSHOT_TTL_MS);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.max(60_000, Math.min(24 * 60 * 60_000, Math.floor(n)));
}

export function resolveScalpV4FailClosedStaleMs(): number {
  const n = Number(process.env.SCALP_V4_FAIL_CLOSED_STALE_MS);
  if (!Number.isFinite(n) || n <= 0) return 2 * 60 * 60_000;
  return Math.max(5 * 60_000, Math.min(7 * 24 * 60 * 60_000, Math.floor(n)));
}

export async function upsertScalpV4RegimeSnapshots(rows: ScalpV4RegimeSnapshot[]): Promise<number> {
  if (!isScalpPgConfigured()) return 0;
  const normalized = (rows || []).filter((row) => row.symbol && row.venue && row.weekStartMs > 0);
  if (!normalized.length) return 0;
  const db = scalpPrisma();
  const values = normalized.map((row) => sql`(
    ${row.venue},
    ${row.symbol},
    'week',
    ${new Date(row.weekStartMs)},
    ${row.classifierVersion},
    ${row.rawCellId},
    ${row.cellId},
    ${row.pendingCellId || null},
    ${row.pendingWeeks},
    ${row.volAxis},
    ${row.trendAxis},
    ${row.riskAxis},
    ${JSON.stringify(row.confidence || {})}::jsonb,
    ${JSON.stringify(row.sourceCoverage || {})}::jsonb,
    ${JSON.stringify(row.details || {})}::jsonb
  )`);
  await db.$executeRaw(sql`
    INSERT INTO scalp_regime_snapshots(
      venue,
      symbol,
      granularity,
      week_start,
      classifier_version,
      raw_cell_id,
      cell_id,
      pending_cell_id,
      pending_weeks,
      vol_axis,
      trend_axis,
      risk_axis,
      confidence_json,
      source_coverage_json,
      details_json
    ) VALUES ${join(values, ",")}
    ON CONFLICT(venue, symbol, granularity, week_start, classifier_version)
    DO UPDATE SET
      raw_cell_id = EXCLUDED.raw_cell_id,
      cell_id = EXCLUDED.cell_id,
      pending_cell_id = EXCLUDED.pending_cell_id,
      pending_weeks = EXCLUDED.pending_weeks,
      vol_axis = EXCLUDED.vol_axis,
      trend_axis = EXCLUDED.trend_axis,
      risk_axis = EXCLUDED.risk_axis,
      confidence_json = EXCLUDED.confidence_json,
      source_coverage_json = EXCLUDED.source_coverage_json,
      details_json = EXCLUDED.details_json,
      updated_at = NOW();
  `);
  const transitions = normalized
    .filter((row) => row.transition?.toCellId)
    .map((row) => sql`(
      ${row.venue},
      ${row.symbol},
      ${new Date(row.weekStartMs)},
      ${row.classifierVersion},
      ${row.transition?.fromCellId || null},
      ${row.transition?.toCellId || null},
      ${JSON.stringify({ rawCellId: row.rawCellId, pendingWeeks: row.pendingWeeks })}::jsonb
    )`);
  if (transitions.length > 0) {
    await db.$executeRaw(sql`
      INSERT INTO scalp_regime_transitions(
        venue,
        symbol,
        transition_week_start,
        classifier_version,
        from_cell_id,
        to_cell_id,
        details_json
      ) VALUES ${join(transitions, ",")}
      ON CONFLICT(venue, symbol, transition_week_start, classifier_version)
      DO UPDATE SET
        from_cell_id = EXCLUDED.from_cell_id,
        to_cell_id = EXCLUDED.to_cell_id,
        details_json = EXCLUDED.details_json,
        created_at = scalp_regime_transitions.created_at;
    `);
  }
  return normalized.length;
}

export async function loadScalpV4CurrentRegimeSnapshot(params: {
  venue: unknown;
  symbol: unknown;
  nowMs?: number;
  classifierVersion?: string;
}): Promise<{ cellId: ScalpV4CellId | null; stale: boolean; snapshot: Record<string, unknown> | null }> {
  if (!isScalpPgConfigured()) return { cellId: null, stale: true, snapshot: null };
  const venue = normalizeVenue(params.venue);
  const symbol = normalizeSymbol(params.symbol);
  if (!symbol) return { cellId: null, stale: true, snapshot: null };
  const weekStartMs = startOfUtcWeekMondayMs(params.nowMs || Date.now());
  const classifierVersion = String(params.classifierVersion || "").trim();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    cellId: string | null;
    rawCellId: string | null;
    classifierVersion: string;
    updatedAt: Date;
    confidenceJson: unknown;
    sourceCoverageJson: unknown;
  }>>(sql`
    SELECT
      cell_id AS "cellId",
      raw_cell_id AS "rawCellId",
      classifier_version AS "classifierVersion",
      updated_at AS "updatedAt",
      confidence_json AS "confidenceJson",
      source_coverage_json AS "sourceCoverageJson"
    FROM scalp_regime_snapshots
    WHERE venue = ${venue}
      AND symbol = ${symbol}
      AND granularity = 'week'
      AND week_start = ${new Date(weekStartMs)}
      AND (${classifierVersion} = '' OR classifier_version = ${classifierVersion})
    ORDER BY updated_at DESC
    LIMIT 1;
  `);
  const row = rows[0];
  if (!row) return { cellId: null, stale: true, snapshot: null };
  const ageMs = Math.max(0, Date.now() - row.updatedAt.getTime());
  return {
    cellId: asCellId(row.cellId),
    stale: ageMs > resolveScalpV4FailClosedStaleMs(),
    snapshot: {
      ...row,
      updatedAtMs: row.updatedAt.getTime(),
      ageMs,
      ttlMs: resolveScalpV4SnapshotTtlMs(venue),
      failClosedStaleMs: resolveScalpV4FailClosedStaleMs(),
    },
  };
}

export async function loadScalpV4RegimeSnapshots(params: {
  venue: unknown;
  symbol: unknown;
  classifierVersion?: string;
  fromMs?: number;
  toMs?: number;
}): Promise<Array<{
  weekStartMs: number;
  classifierVersion: string;
  venue: ScalpV4Venue;
  symbol: string;
  rawCellId: ScalpV4CellId;
  cellId: ScalpV4CellId;
  pendingCellId: ScalpV4CellId | null;
  pendingWeeks: number;
  volAxis: string;
  trendAxis: string;
  riskAxis: string;
  confidence: Record<string, unknown>;
  sourceCoverage: Record<string, unknown>;
  details: Record<string, unknown>;
  transition: null;
}>> {
  if (!isScalpPgConfigured()) return [];
  const venue = normalizeVenue(params.venue);
  const symbol = normalizeSymbol(params.symbol);
  if (!symbol) return [];
  const classifierVersion = String(params.classifierVersion || "").trim();
  const fromMs = Math.max(0, Math.floor(Number(params.fromMs) || 0));
  const toMs = Math.max(fromMs, Math.floor(Number(params.toMs) || Date.now()));
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    weekStart: Date;
    classifierVersion: string;
    rawCellId: string;
    cellId: string;
    pendingCellId: string | null;
    pendingWeeks: number;
    volAxis: string;
    trendAxis: string;
    riskAxis: string;
    confidenceJson: unknown;
    sourceCoverageJson: unknown;
    detailsJson: unknown;
  }>>(sql`
    SELECT
      week_start AS "weekStart",
      classifier_version AS "classifierVersion",
      raw_cell_id AS "rawCellId",
      cell_id AS "cellId",
      pending_cell_id AS "pendingCellId",
      pending_weeks AS "pendingWeeks",
      vol_axis AS "volAxis",
      trend_axis AS "trendAxis",
      risk_axis AS "riskAxis",
      confidence_json AS "confidenceJson",
      source_coverage_json AS "sourceCoverageJson",
      details_json AS "detailsJson"
    FROM scalp_regime_snapshots
    WHERE venue = ${venue}
      AND symbol = ${symbol}
      AND granularity = 'week'
      AND week_start >= ${new Date(fromMs)}
      AND week_start < ${new Date(toMs)}
      AND (${classifierVersion} = '' OR classifier_version = ${classifierVersion})
    ORDER BY week_start ASC;
  `);
  return rows.map((row) => ({
    weekStartMs: row.weekStart.getTime(),
    classifierVersion: row.classifierVersion,
    venue,
    symbol,
    rawCellId: asCellId(row.rawCellId),
    cellId: asCellId(row.cellId),
    pendingCellId: row.pendingCellId ? asCellId(row.pendingCellId) : null,
    pendingWeeks: Math.max(0, Math.floor(Number(row.pendingWeeks) || 0)),
    volAxis: String(row.volAxis || "unknown"),
    trendAxis: String(row.trendAxis || "unknown"),
    riskAxis: String(row.riskAxis || "unknown"),
    confidence: row.confidenceJson && typeof row.confidenceJson === "object" ? (row.confidenceJson as Record<string, unknown>) : {},
    sourceCoverage: row.sourceCoverageJson && typeof row.sourceCoverageJson === "object" ? (row.sourceCoverageJson as Record<string, unknown>) : {},
    details: row.detailsJson && typeof row.detailsJson === "object" ? (row.detailsJson as Record<string, unknown>) : {},
    transition: null,
  }));
}

export async function upsertScalpV4WalkforwardResult(params: {
  candidateId?: number | null;
  deploymentId: string;
  venue: ScalpV4Venue;
  symbol: string;
  strategyId: string;
  tuneId: string;
  classifierVersion: string;
  windowFromMs: number;
  windowToMs: number;
  effectiveTrials: number;
  status: string;
  envelope: ScalpV4RegimeEnvelope;
  windowResults?: unknown;
  details?: Record<string, unknown>;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    INSERT INTO scalp_regime_walkforward_results(
      candidate_id,
      deployment_id,
      venue,
      symbol,
      strategy_id,
      tune_id,
      classifier_version,
      window_from,
      window_to,
      effective_trials,
      status,
      auto_reject_after,
      envelope_json,
      window_results_json,
      details_json,
      evaluated_at
    ) VALUES (
      ${params.candidateId || null},
      ${params.deploymentId},
      ${params.venue},
      ${params.symbol},
      ${params.strategyId},
      ${params.tuneId},
      ${params.classifierVersion},
      ${new Date(params.windowFromMs)},
      ${new Date(params.windowToMs)},
      ${params.effectiveTrials},
      ${params.status},
      ${params.envelope.overbroadReviewUntilMs ? new Date(params.envelope.overbroadReviewUntilMs) : null},
      ${JSON.stringify(params.envelope)}::jsonb,
      ${JSON.stringify(params.windowResults || [])}::jsonb,
      ${JSON.stringify(params.details || {})}::jsonb,
      NOW()
    )
    ON CONFLICT(deployment_id, classifier_version, window_from, window_to)
    DO UPDATE SET
      candidate_id = EXCLUDED.candidate_id,
      venue = EXCLUDED.venue,
      symbol = EXCLUDED.symbol,
      strategy_id = EXCLUDED.strategy_id,
      tune_id = EXCLUDED.tune_id,
      effective_trials = EXCLUDED.effective_trials,
      status = EXCLUDED.status,
      auto_reject_after = EXCLUDED.auto_reject_after,
      envelope_json = EXCLUDED.envelope_json,
      window_results_json = EXCLUDED.window_results_json,
      details_json = EXCLUDED.details_json,
      evaluated_at = NOW(),
      updated_at = NOW();
  `);
}

export async function loadScalpV4CompletedWalkforwardDeploymentIds(params: {
  classifierVersion: string;
  windowFromMs: number;
  windowToMs: number;
}): Promise<Set<string>> {
  if (!isScalpPgConfigured()) return new Set();
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ deploymentId: string }>>(sql`
    SELECT deployment_id AS "deploymentId"
    FROM scalp_regime_walkforward_results
    WHERE classifier_version = ${params.classifierVersion}
      AND window_from = ${new Date(params.windowFromMs)}
      AND window_to = ${new Date(params.windowToMs)};
  `);
  return new Set(rows.map((row) => String(row.deploymentId || "").trim()).filter(Boolean));
}

export async function applyScalpV4OverbroadAutoRejects(nowMs = Date.now()): Promise<number> {
  if (!isScalpPgConfigured()) return 0;
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{ id: string }>>(sql`
    UPDATE scalp_regime_walkforward_results
    SET
      status = 'regime_overbroad_auto_rejected',
      envelope_json = jsonb_set(
        COALESCE(envelope_json, '{}'::jsonb),
        '{status}',
        to_jsonb('regime_overbroad_auto_rejected'::text),
        true
      ),
      updated_at = NOW()
    WHERE status = 'regime_overbroad_pending_review'
      AND manual_approved = FALSE
      AND auto_reject_after IS NOT NULL
      AND auto_reject_after <= ${new Date(nowMs)}
    RETURNING id::text;
  `);
  return rows.length;
}
