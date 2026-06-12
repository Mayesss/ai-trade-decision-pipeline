import os from "node:os";

import { loadScalpCandleHistoryInRange } from "../candleHistory";
import { pipSizeForScalpSymbol } from "../marketData";
import {
  defaultScalpReplayConfig,
  runScalpReplay,
} from "../replay/harness";
import type {
  ScalpReplayCandle,
  ScalpReplayTrade,
} from "../replay/types";
import type { ScalpCandle } from "../types";
import { loadScalpSymbolMarketMetadataBulk } from "../symbolMarketMetadataStore";
import {
  SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
  parseSessionStructureComposerTuneId,
} from "./sessionStructureComposer";
import { buildScalpComposerExecuteConfigOverride } from "./executeConfigOverride";
import { toDeploymentId } from "./logic";
import { isScalpPgConfigured, scalpPrisma, sql } from "./pg";
import { inferScalpComposerAssetCategory, minSpreadPipsForCategory } from "./symbolInfo";
import type { ScalpComposerCandidate, ScalpComposerRiskProfile, ScalpComposerSession } from "./types";
import { getScalpComposerRuntimeConfig } from "./config";
import { startOfScalpComposerWeekMondayUtc } from "./weekWindows";

const ONE_DAY_MS = 24 * 60 * 60_000;
const ONE_WEEK_MS = 7 * ONE_DAY_MS;

export const DAY_ROBUSTNESS_VERSION = "session_finalist_robustness_v1";

export interface DayRobustnessPolicy {
  enabled: boolean;
  weeks: number;
  extendedWeeks: number;
  windowWeeks: number;
  maxCandidates: number;
  leaseMs: number;
  maxAgeMs: number;
  minTotalTrades: number;
  minTotalNetR: number;
  minProfitFactor: number;
  minPositiveWindowPct: number;
  minWorstWindowNetR: number;
  maxDrawdownR: number;
  maxStageCDrawdownMultiple: number;
}

export interface DayRobustnessWindowMetric {
  fromTs: number;
  toTs: number;
  trades: number;
  netR: number;
}

export interface DayRobustnessEvidence {
  version: string;
  evaluatedAtMs: number;
  windowToTs: number;
  fromTs: number;
  toTs: number;
  weeks: number;
  windowWeeks: number;
  trades: number;
  netR: number;
  expectancyR: number;
  winRatePct: number;
  profitFactor: number | null;
  maxDrawdownR: number;
  positiveWindows: number;
  totalWindows: number;
  positiveWindowPct: number;
  worstWindowNetR: number;
  stageCMaxDrawdownR: number | null;
  passed: boolean;
  reasonCodes: string[];
  windows: DayRobustnessWindowMetric[];
}

export interface DayRobustnessPromotionCheck {
  required: boolean;
  passed: boolean;
  reason: "day_robustness_not_required" | "day_robustness_passed" | "DAY_ROBUSTNESS_MISSING" | "DAY_ROBUSTNESS_FAILED";
  evidence: DayRobustnessEvidence | null;
}

function envBool(name: string, fallback: boolean): boolean {
  const raw = String(process.env[name] ?? "").trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function envNumber(name: string, fallback: number): number {
  const raw = String(process.env[name] ?? "").trim();
  if (!raw) return fallback;
  const n = Number(raw);
  return Number.isFinite(n) ? n : fallback;
}

function intBound(value: unknown, fallback: number, min: number, max: number): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function numBound(value: unknown, fallback: number, min: number, max: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, n));
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

export function resolveDayRobustnessPolicy(): DayRobustnessPolicy {
  const weeks = intBound(envNumber("SCALP_DAY_ROBUSTNESS_WEEKS", 26), 26, 13, 104);
  return {
    enabled: envBool("SCALP_DAY_ROBUSTNESS_ENABLED", true),
    weeks,
    extendedWeeks: intBound(envNumber("SCALP_DAY_ROBUSTNESS_EXTENDED_WEEKS", 52), 52, weeks, 156),
    windowWeeks: intBound(envNumber("SCALP_DAY_ROBUSTNESS_WINDOW_WEEKS", 13), 13, 4, 26),
    maxCandidates: intBound(envNumber("SCALP_DAY_ROBUSTNESS_MAX_CANDIDATES", 100), 100, 1, 5_000),
    leaseMs: intBound(envNumber("SCALP_DAY_ROBUSTNESS_LEASE_MINUTES", 120), 120, 5, 24 * 60) * 60_000,
    maxAgeMs: intBound(envNumber("SCALP_DAY_ROBUSTNESS_MAX_AGE_DAYS", 14), 14, 1, 90) * ONE_DAY_MS,
    minTotalTrades: intBound(envNumber("SCALP_DAY_ROBUSTNESS_MIN_TOTAL_TRADES", 60), 60, 0, 20_000),
    minTotalNetR: numBound(envNumber("SCALP_DAY_ROBUSTNESS_MIN_TOTAL_NET_R", 4), 4, -100, 1_000),
    minProfitFactor: numBound(envNumber("SCALP_DAY_ROBUSTNESS_MIN_PROFIT_FACTOR", 1.05), 1.05, 0, 10),
    minPositiveWindowPct: numBound(envNumber("SCALP_DAY_ROBUSTNESS_MIN_POSITIVE_WINDOW_PCT", 50), 50, 0, 100),
    minWorstWindowNetR: numBound(envNumber("SCALP_DAY_ROBUSTNESS_MIN_WORST_WINDOW_NET_R", -2), -2, -100, 100),
    maxDrawdownR: numBound(envNumber("SCALP_DAY_ROBUSTNESS_MAX_DD_R", 18), 18, 0.1, 500),
    maxStageCDrawdownMultiple: numBound(envNumber("SCALP_DAY_ROBUSTNESS_MAX_STAGEC_DD_MULT", 1.5), 1.5, 0.1, 20),
  };
}

export function isDayRobustnessRequired(strategyId: unknown, policy = resolveDayRobustnessPolicy()): boolean {
  return policy.enabled && String(strategyId || "").trim().toLowerCase() === SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID;
}

export function readDayRobustnessEvidence(metadata: unknown): DayRobustnessEvidence | null {
  const worker = asRecord(asRecord(metadata).worker);
  const robustness = asRecord(worker.robustness);
  if (String(robustness.version || "") !== DAY_ROBUSTNESS_VERSION) return null;
  return robustness as unknown as DayRobustnessEvidence;
}

export function evaluateDayRobustnessForPromotion(params: {
  strategyId: unknown;
  metadata: unknown;
  policy?: DayRobustnessPolicy;
  nowMs?: number;
  windowToTs?: number | null;
}): DayRobustnessPromotionCheck {
  const policy = params.policy || resolveDayRobustnessPolicy();
  if (!isDayRobustnessRequired(params.strategyId, policy)) {
    return { required: false, passed: true, reason: "day_robustness_not_required", evidence: null };
  }
  const evidence = readDayRobustnessEvidence(params.metadata);
  const nowMs = Math.floor(Number(params.nowMs || Date.now()));
  const expectedWindowToTs = Math.floor(Number(params.windowToTs || 0));
  if (!evidence) {
    return { required: true, passed: false, reason: "DAY_ROBUSTNESS_MISSING", evidence: null };
  }
  const evaluatedAtMs = Math.floor(Number(evidence.evaluatedAtMs || 0));
  const evidenceWindowToTs = Math.floor(Number(evidence.windowToTs || 0));
  const stale =
    evaluatedAtMs <= 0 ||
    nowMs - evaluatedAtMs > policy.maxAgeMs ||
    Number(evidence.weeks || 0) < policy.weeks ||
    (expectedWindowToTs > 0 && evidenceWindowToTs > 0 && evidenceWindowToTs !== expectedWindowToTs);
  if (stale || !evidence.passed) {
    return { required: true, passed: false, reason: "DAY_ROBUSTNESS_FAILED", evidence };
  }
  return { required: true, passed: true, reason: "day_robustness_passed", evidence };
}

function profitFactor(trades: ScalpReplayTrade[]): number | null {
  let wins = 0;
  let losses = 0;
  for (const trade of trades) {
    const r = Number(trade.rMultiple || 0);
    if (r > 0) wins += r;
    else if (r < 0) losses += Math.abs(r);
  }
  if (losses <= 0) return wins > 0 ? null : 0;
  return wins / losses;
}

function maxDrawdownRFromTrades(trades: ScalpReplayTrade[]): number {
  let equity = 0;
  let peak = 0;
  let dd = 0;
  for (const trade of trades.slice().sort((a, b) => Number(a.exitTs) - Number(b.exitTs))) {
    equity += Number(trade.rMultiple || 0);
    peak = Math.max(peak, equity);
    dd = Math.max(dd, peak - equity);
  }
  return dd;
}

function buildWindowMetrics(params: {
  trades: ScalpReplayTrade[];
  fromTs: number;
  toTs: number;
  windowWeeks: number;
}): DayRobustnessWindowMetric[] {
  const spanMs = Math.max(1, params.windowWeeks) * ONE_WEEK_MS;
  const out: DayRobustnessWindowMetric[] = [];
  for (let from = params.fromTs; from < params.toTs; from += spanMs) {
    const to = Math.min(params.toTs, from + spanMs);
    const windowTrades = params.trades.filter((trade) => Number(trade.exitTs) >= from && Number(trade.exitTs) < to);
    out.push({
      fromTs: from,
      toTs: to,
      trades: windowTrades.length,
      netR: windowTrades.reduce((acc, trade) => acc + Number(trade.rMultiple || 0), 0),
    });
  }
  return out;
}

export function buildDayRobustnessEvidence(params: {
  trades: ScalpReplayTrade[];
  summary: { trades: number; netR: number; expectancyR: number; winRatePct: number; profitFactor: number | null; maxDrawdownR: number };
  fromTs: number;
  toTs: number;
  windowToTs: number;
  weeks: number;
  policy: DayRobustnessPolicy;
  stageCMaxDrawdownR?: number | null;
  nowMs?: number;
}): DayRobustnessEvidence {
  const trades = params.trades || [];
  const windows = buildWindowMetrics({
    trades,
    fromTs: params.fromTs,
    toTs: params.toTs,
    windowWeeks: params.policy.windowWeeks,
  });
  const positiveWindows = windows.filter((row) => row.netR > 0).length;
  const totalWindows = windows.length;
  const positiveWindowPct = totalWindows > 0 ? (positiveWindows / totalWindows) * 100 : 0;
  const worstWindowNetR = totalWindows > 0 ? Math.min(...windows.map((row) => row.netR)) : 0;
  const pf = params.summary.profitFactor ?? profitFactor(trades);
  const maxDrawdownR = Number.isFinite(Number(params.summary.maxDrawdownR))
    ? Number(params.summary.maxDrawdownR)
    : maxDrawdownRFromTrades(trades);
  const stageCMaxDrawdownR =
    params.stageCMaxDrawdownR !== null && params.stageCMaxDrawdownR !== undefined && Number.isFinite(Number(params.stageCMaxDrawdownR))
      ? Number(params.stageCMaxDrawdownR)
      : null;
  const reasonCodes: string[] = [];
  if (params.summary.trades < params.policy.minTotalTrades) reasonCodes.push("DAY_ROBUSTNESS_TOTAL_TRADES_BELOW_THRESHOLD");
  if (params.summary.netR < params.policy.minTotalNetR) reasonCodes.push("DAY_ROBUSTNESS_TOTAL_NET_R_BELOW_THRESHOLD");
  if (pf !== null && pf < params.policy.minProfitFactor) reasonCodes.push("DAY_ROBUSTNESS_PROFIT_FACTOR_BELOW_THRESHOLD");
  if (positiveWindowPct < params.policy.minPositiveWindowPct) reasonCodes.push("DAY_ROBUSTNESS_POSITIVE_WINDOWS_BELOW_THRESHOLD");
  if (worstWindowNetR < params.policy.minWorstWindowNetR) reasonCodes.push("DAY_ROBUSTNESS_WORST_WINDOW_BELOW_THRESHOLD");
  if (maxDrawdownR > params.policy.maxDrawdownR) reasonCodes.push("DAY_ROBUSTNESS_MAX_DD_ABOVE_THRESHOLD");
  if (
    stageCMaxDrawdownR !== null &&
    stageCMaxDrawdownR > 0 &&
    maxDrawdownR > stageCMaxDrawdownR * params.policy.maxStageCDrawdownMultiple
  ) {
    reasonCodes.push("DAY_ROBUSTNESS_DD_DEGRADED_VS_STAGE_C");
  }

  return {
    version: DAY_ROBUSTNESS_VERSION,
    evaluatedAtMs: Math.floor(Number(params.nowMs || Date.now())),
    windowToTs: params.windowToTs,
    fromTs: params.fromTs,
    toTs: params.toTs,
    weeks: params.weeks,
    windowWeeks: params.policy.windowWeeks,
    trades: Math.max(0, Math.floor(Number(params.summary.trades || 0))),
    netR: Number(params.summary.netR || 0),
    expectancyR: Number(params.summary.expectancyR || 0),
    winRatePct: Number(params.summary.winRatePct || 0),
    profitFactor: pf,
    maxDrawdownR,
    positiveWindows,
    totalWindows,
    positiveWindowPct,
    worstWindowNetR,
    stageCMaxDrawdownR,
    passed: reasonCodes.length === 0,
    reasonCodes,
    windows,
  };
}

function toReplayCandlesFromHistory(candles: ScalpCandle[], spreadPips: number): ScalpReplayCandle[] {
  const out: ScalpReplayCandle[] = [];
  for (const candle of candles || []) {
    const row = Array.isArray(candle)
      ? { ts: candle[0], open: candle[1], high: candle[2], low: candle[3], close: candle[4], volume: candle[5] }
      : candle;
    const ts = Math.floor(Number(row.ts || 0));
    const open = Number(row.open || 0);
    const high = Number(row.high || 0);
    const low = Number(row.low || 0);
    const close = Number(row.close || 0);
    const volume = Number(row.volume || 0);
    if (![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) continue;
    out.push({ ts, open, high, low, close, volume: Number.isFinite(volume) ? volume : 0, spreadPips });
  }
  return out.sort((a, b) => a.ts - b.ts);
}

function filterSundayReplayCandles(candles: ScalpReplayCandle[]): ScalpReplayCandle[] {
  const byWeek = new Map<number, { nonSunday: ScalpReplayCandle[]; sunday: ScalpReplayCandle[] }>();
  for (const row of candles) {
    const weekStart = startOfScalpComposerWeekMondayUtc(row.ts);
    const bucket = byWeek.get(weekStart) || { nonSunday: [], sunday: [] };
    if (new Date(row.ts).getUTCDay() === 0) bucket.sunday.push(row);
    else bucket.nonSunday.push(row);
    byWeek.set(weekStart, bucket);
  }
  const out: ScalpReplayCandle[] = [];
  for (const weekStart of Array.from(byWeek.keys()).sort((a, b) => a - b)) {
    const bucket = byWeek.get(weekStart)!;
    out.push(...(bucket.nonSunday.length > 0 ? bucket.nonSunday : bucket.sunday));
  }
  return out.sort((a, b) => a.ts - b.ts);
}

function normalizeReadOrder(): Array<"pg" | "kv" | "broker"> {
  const raw = String(process.env.SCALP_DAY_ROBUSTNESS_READ_ORDER || "pg").trim().toLowerCase();
  const values = raw.split(",").map((row) => row.trim()).filter(Boolean);
  const out = values.filter((row): row is "pg" | "kv" | "broker" => row === "pg" || row === "kv" || row === "broker");
  return out.length > 0 ? out : ["pg"];
}

async function ensureRobustnessLeaseColumns(): Promise<boolean> {
  if (!isScalpPgConfigured()) return false;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    ALTER TABLE scalp_v2_candidates
      ADD COLUMN IF NOT EXISTS research_locked_by TEXT,
      ADD COLUMN IF NOT EXISTS research_claimed_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS research_lease_until TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS research_attempts INTEGER NOT NULL DEFAULT 0
  `);
  return true;
}

export async function claimDayRobustnessCandidates(params: {
  limit?: number;
  lockOwner?: string;
  leaseMs?: number;
  nowMs?: number;
  windowToTs: number;
  policy?: DayRobustnessPolicy;
  includeFailed?: boolean;
}): Promise<ScalpComposerCandidate[]> {
  if (!isScalpPgConfigured()) return [];
  await ensureRobustnessLeaseColumns();
  const policy = params.policy || resolveDayRobustnessPolicy();
  const limit = Math.max(1, Math.min(5_000, Math.floor(Number(params.limit || policy.maxCandidates))));
  const lockOwner = String(params.lockOwner || `day-robustness:${os.hostname()}:${process.pid}`).trim();
  const leaseMs = Math.max(60_000, Math.min(24 * 60 * 60_000, Math.floor(Number(params.leaseMs || policy.leaseMs))));
  const windowToTs = Math.floor(Number(params.windowToTs || 0));
  const includeFailed = Boolean(params.includeFailed);
  const clusterDedupe = envBool("SCALP_DAY_ROBUSTNESS_CLUSTER_DEDUPE_ENABLED", true);
  const claimPoolLimit = Math.max(limit, Math.min(25_000, limit * (clusterDedupe ? 20 : 1)));
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    id: number | bigint;
    venue: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    entrySessionProfile: string;
    score: string | number | null;
    status: string;
    reasonCodes: string[] | null;
    metadataJson: unknown;
    createdAt: Date;
    updatedAt: Date;
  }>>(sql`
    WITH eligible AS (
      SELECT
        c.id,
        c.venue,
        c.symbol,
        c.entry_session_profile,
        c.metadata_json,
        c.score,
        COALESCE((c.metadata_json->'worker'->'stageC'->>'netR')::double precision, -999) AS stage_c_net_r,
        COALESCE((c.metadata_json->'worker'->'stageC'->>'trades')::int, 0) AS stage_c_trades
      FROM scalp_v2_candidates c
      WHERE c.status = 'evaluated'
        AND c.strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
        AND COALESCE((c.metadata_json->'worker'->'stageC'->>'passed')::boolean, false)
        AND COALESCE((c.metadata_json->'worker'->>'windowToTs')::bigint, 0) = ${windowToTs}
        AND (
          c.metadata_json->'worker'->'robustness' IS NULL
          OR COALESCE((c.metadata_json->'worker'->'robustness'->>'windowToTs')::bigint, 0) <> ${windowToTs}
          OR COALESCE((c.metadata_json->'worker'->'robustness'->>'weeks')::int, 0) < ${policy.weeks}
          OR (
            ${includeFailed} = TRUE
            AND COALESCE((c.metadata_json->'worker'->'robustness'->>'passed')::boolean, false) = FALSE
          )
        )
        AND (
          c.research_lease_until IS NULL
          OR c.research_lease_until < NOW()
          OR c.research_locked_by = ${lockOwner}
        )
      ORDER BY
        COALESCE((c.metadata_json->'worker'->'stageC'->>'netR')::double precision, -999) DESC,
        COALESCE((c.metadata_json->'worker'->'stageC'->>'trades')::int, 0) DESC,
        c.score DESC
      LIMIT ${claimPoolLimit}
      FOR UPDATE SKIP LOCKED
    ),
    ranked AS (
      SELECT
        id,
        row_number() OVER (
          PARTITION BY
            venue,
            symbol,
            entry_session_profile,
            COALESCE(metadata_json->'sessionComposerPlan'->>'contextId', ''),
            CASE
              WHEN COALESCE(metadata_json->'sessionComposerPlan'->>'levelId', '') LIKE 'opening_range_%'
                THEN 'opening_range'
              ELSE COALESCE(metadata_json->'sessionComposerPlan'->>'levelId', '')
            END,
            CASE
              WHEN COALESCE(metadata_json->'sessionComposerPlan'->>'triggerId', '') LIKE 'breakout_retest_hold%'
                THEN 'breakout_retest'
              ELSE COALESCE(metadata_json->'sessionComposerPlan'->>'triggerId', '')
            END
          ORDER BY
            stage_c_net_r DESC,
            stage_c_trades DESC,
            score DESC
        ) AS cluster_rank,
        stage_c_net_r,
        stage_c_trades,
        score
      FROM eligible
    ),
    claimable AS (
      SELECT id
      FROM ranked
      WHERE (${clusterDedupe} = FALSE OR cluster_rank = 1)
      ORDER BY
        stage_c_net_r DESC,
        stage_c_trades DESC,
        score DESC
      LIMIT ${limit}
    ),
    updated AS (
      UPDATE scalp_v2_candidates c
      SET
        research_locked_by = ${lockOwner},
        research_claimed_at = NOW(),
        research_lease_until = NOW() + (${leaseMs} * INTERVAL '1 millisecond'),
        research_attempts = COALESCE(c.research_attempts, 0) + 1,
        updated_at = NOW()
      FROM claimable
      WHERE c.id = claimable.id
      RETURNING c.*
    )
    SELECT
      id,
      venue,
      symbol,
      strategy_id AS "strategyId",
      tune_id AS "tuneId",
      entry_session_profile AS "entrySessionProfile",
      score::text AS score,
      status,
      reason_codes AS "reasonCodes",
      metadata_json AS "metadataJson",
      created_at AS "createdAt",
      updated_at AS "updatedAt"
    FROM updated;
  `);
  return rows.map((row) => ({
    id: Math.floor(Number(row.id) || 0),
    venue: String(row.venue || "").trim().toLowerCase() === "capital" ? "capital" : "bitget",
    symbol: String(row.symbol || "").trim().toUpperCase(),
    strategyId: String(row.strategyId || "").trim().toLowerCase(),
    tuneId: String(row.tuneId || "").trim().toLowerCase(),
    entrySessionProfile: String(row.entrySessionProfile || "").trim().toLowerCase() as ScalpComposerSession,
    score: Number(row.score || 0),
    status: "evaluated",
    reasonCodes: Array.isArray(row.reasonCodes) ? row.reasonCodes.map(String) : [],
    metadata: asRecord(row.metadataJson),
    deploymentId: null,
    deploymentEnabled: null,
    createdAtMs: row.createdAt instanceof Date ? row.createdAt.getTime() : Date.now(),
    updatedAtMs: row.updatedAt instanceof Date ? row.updatedAt.getTime() : Date.now(),
  }));
}

export async function saveDayRobustnessEvidence(params: {
  candidateId: number;
  evidence: DayRobustnessEvidence;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    UPDATE scalp_v2_candidates
    SET
      metadata_json = jsonb_set(
        CASE WHEN metadata_json ? 'worker' THEN metadata_json ELSE jsonb_set(COALESCE(metadata_json, '{}'::jsonb), '{worker}', '{}'::jsonb, true) END,
        '{worker,robustness}',
        ${JSON.stringify(params.evidence)}::jsonb,
        true
      ),
      research_locked_by = NULL,
      research_claimed_at = NULL,
      research_lease_until = NULL,
      updated_at = NOW()
    WHERE id = ${Math.floor(Number(params.candidateId) || 0)};
  `);
}

export async function releaseDayRobustnessCandidateLease(params: {
  candidateId: number;
}): Promise<void> {
  if (!isScalpPgConfigured()) return;
  const db = scalpPrisma();
  await db.$executeRaw(sql`
    UPDATE scalp_v2_candidates
    SET
      research_locked_by = NULL,
      research_claimed_at = NULL,
      research_lease_until = NULL,
      updated_at = NOW()
    WHERE id = ${Math.floor(Number(params.candidateId) || 0)};
  `);
}

export async function runDayRobustnessBatch(params: {
  windowToTs: number;
  limit?: number;
  dryRun?: boolean;
  extended?: boolean;
  includeFailed?: boolean;
  lockOwner?: string;
  onProgress?: (event: Record<string, unknown>) => void;
}): Promise<{
  selected: number;
  processed: number;
  passed: number;
  failed: number;
  errors: number;
  rows: Array<{ candidateId: number; symbol: string; tuneId: string; passed: boolean; reasonCodes: string[]; trades: number; netR: number }>;
}> {
  const policyBase = resolveDayRobustnessPolicy();
  const policy = params.extended ? { ...policyBase, weeks: policyBase.extendedWeeks } : policyBase;
  const candidates = await claimDayRobustnessCandidates({
    limit: params.limit || policy.maxCandidates,
    lockOwner: params.lockOwner,
    leaseMs: policy.leaseMs,
    windowToTs: params.windowToTs,
    policy,
    includeFailed: params.includeFailed,
  });
  const symbols = Array.from(new Set(candidates.map((row) => row.symbol)));
  const metadata = await loadScalpSymbolMarketMetadataBulk(symbols).catch(() => new Map());
  const runtime = getScalpComposerRuntimeConfig();
  const candleCache = new Map<string, ScalpReplayCandle[]>();
  let processed = 0;
  let passed = 0;
  let failed = 0;
  let errors = 0;
  const rows: Array<{ candidateId: number; symbol: string; tuneId: string; passed: boolean; reasonCodes: string[]; trades: number; netR: number }> = [];
  const toTs = params.windowToTs;
  const fromTs = toTs - policy.weeks * ONE_WEEK_MS;
  const readOrder = normalizeReadOrder();

  for (const candidate of candidates) {
    try {
      params.onProgress?.({ phase: "candidate_start", candidateId: candidate.id, symbol: candidate.symbol, tuneId: candidate.tuneId, processed, total: candidates.length });
      let candles = candleCache.get(candidate.symbol) || null;
      const meta = metadata.get(candidate.symbol) || null;
      const pipSize = pipSizeForScalpSymbol(candidate.symbol, meta || undefined);
      if (!candles) {
        const category = inferScalpComposerAssetCategory(candidate.symbol);
        const baseReplayConfig = defaultScalpReplayConfig(candidate.symbol);
        const tickSpreadPips = meta?.tickSize ? meta.tickSize / pipSize : 0;
        const spreadPips = Math.max(baseReplayConfig.defaultSpreadPips, minSpreadPipsForCategory(category), tickSpreadPips);
        const history = await loadScalpCandleHistoryInRange(candidate.symbol, "1m", fromTs, toTs, {
          venue: candidate.venue,
          readOrder,
          requireCoverageRatio: 0.65,
          auditSource: "day_robustness",
        });
        candles = filterSundayReplayCandles(toReplayCandlesFromHistory((history.record?.candles || []) as ScalpCandle[], spreadPips));
        candleCache.set(candidate.symbol, candles);
      }
      const deploymentId = toDeploymentId({
        venue: candidate.venue,
        symbol: candidate.symbol,
        strategyId: candidate.strategyId,
        tuneId: candidate.tuneId,
        session: candidate.entrySessionProfile,
      });
      const base = defaultScalpReplayConfig(candidate.symbol);
      const sessionManagement = parseSessionStructureComposerTuneId(candidate.tuneId).managementId;
      const configOverride = buildScalpComposerExecuteConfigOverride({
        entrySessionProfile: candidate.entrySessionProfile,
        riskProfile: runtime.riskProfile as ScalpComposerRiskProfile,
        stateMachineOverrides: {},
      });
      const replayConfig = {
        ...base,
        symbol: candidate.symbol,
        strategyId: candidate.strategyId,
        tuneId: candidate.tuneId,
        deploymentId,
        tuneLabel: candidate.tuneId,
        configOverride: {
          ...configOverride,
          risk: {
            ...(configOverride.risk || {}),
            maxTradesPerSymbolPerDay: 2,
            timeStopBars:
              sessionManagement === "fixed_1_5r_time_2h"
                ? 8
                : sessionManagement === "trail_after_0_8r_time_3h"
                  ? 12
                  : 16,
            ...(sessionManagement === "trail_after_0_8r_time_3h" && {
              trailStartR: 0.8,
              trailAtrMult: 1.6,
            }),
          },
        },
        strategy: {
          ...base.strategy,
          entrySessionProfile: candidate.entrySessionProfile,
        },
      };
      const replay = await runScalpReplay({
        candles,
        pipSize,
        config: replayConfig,
        captureTimeline: false,
        symbolMeta: meta || null,
      });
      const stageC = asRecord(asRecord(candidate.metadata).worker).stageC;
      const evidence = buildDayRobustnessEvidence({
        trades: replay.trades,
        summary: replay.summary,
        fromTs,
        toTs,
        windowToTs: toTs,
        weeks: policy.weeks,
        policy,
        stageCMaxDrawdownR: Number(asRecord(stageC).maxDrawdownR),
      });
      if (!params.dryRun) {
        await saveDayRobustnessEvidence({ candidateId: candidate.id, evidence });
      } else {
        await releaseDayRobustnessCandidateLease({ candidateId: candidate.id });
      }
      processed += 1;
      if (evidence.passed) passed += 1;
      else failed += 1;
      rows.push({
        candidateId: candidate.id,
        symbol: candidate.symbol,
        tuneId: candidate.tuneId,
        passed: evidence.passed,
        reasonCodes: evidence.reasonCodes,
        trades: evidence.trades,
        netR: evidence.netR,
      });
      params.onProgress?.({ phase: "candidate_done", candidateId: candidate.id, symbol: candidate.symbol, passed: evidence.passed, trades: evidence.trades, netR: evidence.netR, processed, total: candidates.length });
    } catch (err: any) {
      errors += 1;
      processed += 1;
      const evidence: DayRobustnessEvidence = {
        version: DAY_ROBUSTNESS_VERSION,
        evaluatedAtMs: Date.now(),
        windowToTs: toTs,
        fromTs,
        toTs,
        weeks: policy.weeks,
        windowWeeks: policy.windowWeeks,
        trades: 0,
        netR: 0,
        expectancyR: 0,
        winRatePct: 0,
        profitFactor: null,
        maxDrawdownR: 0,
        positiveWindows: 0,
        totalWindows: 0,
        positiveWindowPct: 0,
        worstWindowNetR: 0,
        stageCMaxDrawdownR: null,
        passed: false,
        reasonCodes: ["DAY_ROBUSTNESS_REPLAY_ERROR"],
        windows: [],
      };
      if (!params.dryRun) await saveDayRobustnessEvidence({ candidateId: candidate.id, evidence });
      else await releaseDayRobustnessCandidateLease({ candidateId: candidate.id });
      rows.push({ candidateId: candidate.id, symbol: candidate.symbol, tuneId: candidate.tuneId, passed: false, reasonCodes: evidence.reasonCodes, trades: 0, netR: 0 });
      params.onProgress?.({ phase: "candidate_error", candidateId: candidate.id, symbol: candidate.symbol, error: err?.message || String(err), processed, total: candidates.length });
    }
  }
  return { selected: candidates.length, processed, passed, failed, errors, rows };
}
