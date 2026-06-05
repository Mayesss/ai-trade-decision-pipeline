import { isScalpPgConfigured, scalpPrisma, sql } from "./pg";
import {
  SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
  sessionStructureAdaptiveKeys,
  type SessionStructureAdaptivePriorEntry,
  type SessionStructureAdaptivePriorSet,
  type SessionStructureComposerPlan,
} from "./sessionStructureComposer";
import type { ScalpV2Session, ScalpV2Venue } from "./types";

export const SESSION_STRUCTURE_ADAPTIVE_SEARCH_VERSION =
  "session_structure_adaptive_search_v1";

type CandidateResultRow = {
  venue: ScalpV2Venue;
  symbol: string;
  session: ScalpV2Session;
  metadata: Record<string, unknown>;
};

type MutablePriorStats = {
  samples: number;
  qualitySum: number;
  stageAPass: number;
  stageBPass: number;
  stageCPass: number;
};

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

function stableJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableJson(record[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

function clamp(value: number, min: number, max: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.max(min, Math.min(max, n));
}

function normalizeVenue(value: unknown): ScalpV2Venue {
  return String(value || "").trim().toLowerCase() === "capital" ? "capital" : "bitget";
}

function normalizeSession(value: unknown): ScalpV2Session {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "tokyo") return "tokyo";
  if (raw === "newyork") return "newyork";
  if (raw === "pacific") return "pacific";
  if (raw === "sydney") return "sydney";
  return "berlin";
}

function scopeKey(row: Pick<CandidateResultRow, "venue" | "symbol" | "session">): string {
  return [row.venue, String(row.symbol || "").trim().toUpperCase(), row.session].join(":");
}

function activeWorker(metadata: Record<string, unknown>): Record<string, unknown> {
  const worker = asRecord(metadata.worker);
  if (Object.keys(worker).length > 0) return worker;
  return asRecord(metadata.previousWorker);
}

function stageQuality(worker: Record<string, unknown>): {
  quality: number;
  stageAPass: boolean;
  stageBPass: boolean;
  stageCPass: boolean;
} | null {
  const stageA = asRecord(worker.stageA);
  if (Object.keys(stageA).length === 0) return null;
  const stageB = asRecord(worker.stageB);
  const stageC = asRecord(worker.stageC);
  const stageAPass = stageA.passed === true;
  const stageBPass = stageB.passed === true;
  const stageCPass = stageC.passed === true;
  const deepest = Object.keys(stageC).length > 0
    ? stageC
    : Object.keys(stageB).length > 0
      ? stageB
      : stageA;
  const netR = Number(deepest.netR);
  const trades = Math.max(0, Math.floor(Number(deepest.trades) || 0));
  let quality = stageAPass ? 0.05 : -0.32;
  if (Object.keys(stageB).length > 0) quality = stageBPass ? 0.32 : 0.02;
  if (Object.keys(stageC).length > 0) quality = stageCPass ? 0.72 : 0.16;
  quality += clamp(Number.isFinite(netR) ? netR / 10 : 0, -0.28, 0.28);
  if (trades > 0 && trades < 12) quality -= 0.08;
  if (trades >= 40) quality += 0.04;
  return {
    quality: clamp(quality, -1, 1),
    stageAPass,
    stageBPass,
    stageCPass,
  };
}

function addStats(
  target: Map<string, MutablePriorStats>,
  key: string,
  result: NonNullable<ReturnType<typeof stageQuality>>,
): void {
  const existing = target.get(key) || {
    samples: 0,
    qualitySum: 0,
    stageAPass: 0,
    stageBPass: 0,
    stageCPass: 0,
  };
  existing.samples += 1;
  existing.qualitySum += result.quality;
  if (result.stageAPass) existing.stageAPass += 1;
  if (result.stageBPass) existing.stageBPass += 1;
  if (result.stageCPass) existing.stageCPass += 1;
  target.set(key, existing);
}

function finalizeStats(
  input: Map<string, MutablePriorStats>,
  minSamples: number,
): Record<string, SessionStructureAdaptivePriorEntry> {
  const out: Record<string, SessionStructureAdaptivePriorEntry> = {};
  for (const [key, value] of input.entries()) {
    if (value.samples <= 0) continue;
    const shrink = value.samples / (value.samples + Math.max(1, minSamples));
    out[key] = {
      score: clamp((value.qualitySum / value.samples) * shrink, -1, 1),
      samples: value.samples,
      stageAPass: value.stageAPass,
      stageBPass: value.stageBPass,
      stageCPass: value.stageCPass,
    };
  }
  return out;
}

export function buildSessionStructureAdaptivePriorsFromRows(params: {
  rows: CandidateResultRow[];
  windowToTs: number;
  nowMs?: number;
  minSamples?: number;
}): SessionStructureAdaptivePriorSet {
  const minSamples = Math.max(1, Math.min(100, Math.floor(Number(params.minSamples || 8))));
  const global = new Map<string, MutablePriorStats>();
  const scoped = new Map<string, Map<string, MutablePriorStats>>();
  let rows = 0;
  let stageAPass = 0;
  let stageBPass = 0;
  let stageCPass = 0;

  for (const row of params.rows) {
    const metadata = asRecord(row.metadata);
    const plan = asRecord(metadata.sessionComposerPlan) as Partial<SessionStructureComposerPlan>;
    if (!plan.contextId || !plan.levelId || !plan.triggerId || !plan.confirmationId || !plan.managementId) {
      continue;
    }
    const result = stageQuality(activeWorker(metadata));
    if (!result) continue;
    rows += 1;
    if (result.stageAPass) stageAPass += 1;
    if (result.stageBPass) stageBPass += 1;
    if (result.stageCPass) stageCPass += 1;
    const keys = sessionStructureAdaptiveKeys({
      contextId: plan.contextId,
      levelId: plan.levelId,
      triggerId: plan.triggerId,
      confirmationId: plan.confirmationId,
      managementId: plan.managementId,
    } as Omit<SessionStructureComposerPlan, "digest">);
    const scopedKey = scopeKey(row);
    const scopedStats = scoped.get(scopedKey) || new Map<string, MutablePriorStats>();
    for (const key of keys) {
      addStats(global, key, result);
      addStats(scopedStats, key, result);
    }
    scoped.set(scopedKey, scopedStats);
  }

  const scopedOut: Record<string, Record<string, SessionStructureAdaptivePriorEntry>> = {};
  for (const [key, value] of scoped.entries()) {
    scopedOut[key] = finalizeStats(value, minSamples);
  }
  const globalOut = finalizeStats(global, minSamples);
  const scoredKeys =
    Object.keys(globalOut).length +
    Object.values(scopedOut).reduce((acc, row) => acc + Object.keys(row).length, 0);

  return {
    version: SESSION_STRUCTURE_ADAPTIVE_SEARCH_VERSION,
    generatedAtMs: Math.floor(Number(params.nowMs || Date.now())),
    windowToTs: Math.floor(Number(params.windowToTs || 0)),
    minSamples,
    global: globalOut,
    scoped: scopedOut,
    diagnostics: {
      rows,
      scoredKeys,
      stageAPass,
      stageBPass,
      stageCPass,
    },
  };
}

export async function loadSessionStructureAdaptivePriors(params: {
  windowToTs: number;
  nowMs?: number;
  limit?: number;
  minSamples?: number;
}): Promise<SessionStructureAdaptivePriorSet | null> {
  if (!isScalpPgConfigured()) return null;
  const limit = Math.max(1, Math.min(100_000, Math.floor(Number(params.limit || 50_000))));
  const windowToTs = Math.floor(Number(params.windowToTs || 0));
  const db = scalpPrisma();
  const rows = await db.$queryRaw<Array<{
    venue: string;
    symbol: string;
    entrySessionProfile: string;
    metadataJson: unknown;
  }>>(sql`
    SELECT
      venue,
      symbol,
      entry_session_profile AS "entrySessionProfile",
      metadata_json AS "metadataJson"
    FROM scalp_v2_candidates
    WHERE strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
      AND metadata_json->'sessionComposerPlan' IS NOT NULL
      AND (
        metadata_json->'worker'->'stageA' IS NOT NULL
        OR metadata_json->'previousWorker'->'stageA' IS NOT NULL
      )
      AND COALESCE(
        (metadata_json->'worker'->>'windowToTs')::bigint,
        (metadata_json->'previousWorker'->>'windowToTs')::bigint,
        0
      ) <= ${windowToTs}
    ORDER BY updated_at DESC
    LIMIT ${limit};
  `);
  const priors = buildSessionStructureAdaptivePriorsFromRows({
    rows: rows.map((row) => ({
      venue: normalizeVenue(row.venue),
      symbol: String(row.symbol || "").trim().toUpperCase(),
      session: normalizeSession(row.entrySessionProfile),
      metadata: asRecord(row.metadataJson),
    })),
    windowToTs,
    nowMs: params.nowMs,
    minSamples: params.minSamples,
  });
  return priors.diagnostics.rows > 0 ? priors : null;
}

export function fingerprintSessionStructureAdaptivePriors(
  priors: SessionStructureAdaptivePriorSet | null | undefined,
): string {
  if (!priors) return "adaptive:none";
  return stableJson({
    version: priors.version,
    windowToTs: priors.windowToTs,
    minSamples: priors.minSamples,
    global: priors.global,
    scoped: priors.scoped,
  });
}
