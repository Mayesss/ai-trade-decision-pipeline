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

export function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, unknown>;
}

export function stableJson(value: unknown): string {
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

export function normalizeVenue(value: unknown): ScalpV2Venue {
  return String(value || "").trim().toLowerCase() === "capital" ? "capital" : "bitget";
}

export function normalizeSession(value: unknown): ScalpV2Session {
  const raw = String(value || "").trim().toLowerCase();
  if (raw === "tokyo") return "tokyo";
  if (raw === "newyork") return "newyork";
  if (raw === "pacific") return "pacific";
  if (raw === "sydney") return "sydney";
  return "berlin";
}

export function scopeKey(row: Pick<CandidateResultRow, "venue" | "symbol" | "session">): string {
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
    stageABaseRate: rows > 0 ? stageAPass / rows : 0,
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
  // Project only the SCALAR fields the builder reads, via a subquery that
  // coalesces worker→previousWorker at the object level (matching activeWorker).
  // Selecting full metadata_json — or even the whole `worker` object — for tens
  // of thousands of rows exceeds the Neon HTTP 64MB response cap, so the load
  // silently returns null and disables adaptive search. Scalars keep it tiny.
  const rows = await db.$queryRaw<Array<{
    venue: string;
    symbol: string;
    entrySessionProfile: string;
    plan: unknown;
    saPassed: string | null;
    saNetR: string | null;
    saTrades: string | null;
    sbPassed: string | null;
    sbNetR: string | null;
    sbTrades: string | null;
    scPassed: string | null;
    scNetR: string | null;
    scTrades: string | null;
  }>>(sql`
    SELECT
      t.venue,
      t.symbol,
      t."entrySessionProfile",
      t.plan AS "plan",
      t.w->'stageA'->>'passed' AS "saPassed",
      t.w->'stageA'->>'netR' AS "saNetR",
      t.w->'stageA'->>'trades' AS "saTrades",
      t.w->'stageB'->>'passed' AS "sbPassed",
      t.w->'stageB'->>'netR' AS "sbNetR",
      t.w->'stageB'->>'trades' AS "sbTrades",
      t.w->'stageC'->>'passed' AS "scPassed",
      t.w->'stageC'->>'netR' AS "scNetR",
      t.w->'stageC'->>'trades' AS "scTrades"
    FROM (
      SELECT
        venue,
        symbol,
        entry_session_profile AS "entrySessionProfile",
        metadata_json->'sessionComposerPlan' AS plan,
        COALESCE(metadata_json->'worker', metadata_json->'previousWorker') AS w
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
      LIMIT ${limit}
    ) t;
  `);
  const mkStage = (
    passed: string | null,
    netR: string | null,
    trades: string | null,
  ): Record<string, unknown> =>
    passed == null
      ? {}
      : {
          passed: passed === "true",
          netR: netR == null ? undefined : Number(netR),
          trades: trades == null ? undefined : Number(trades),
        };
  const priors = buildSessionStructureAdaptivePriorsFromRows({
    rows: rows.map((row) => ({
      venue: normalizeVenue(row.venue),
      symbol: String(row.symbol || "").trim().toUpperCase(),
      session: normalizeSession(row.entrySessionProfile),
      // Reconstruct the metadata shape the builder reads from the scalars.
      metadata: {
        sessionComposerPlan: asRecord(row.plan),
        worker: {
          stageA: mkStage(row.saPassed, row.saNetR, row.saTrades),
          stageB: mkStage(row.sbPassed, row.sbNetR, row.sbTrades),
          stageC: mkStage(row.scPassed, row.scNetR, row.scTrades),
        },
      },
    })),
    windowToTs,
    nowMs: params.nowMs,
    minSamples: params.minSamples,
  });
  return priors.diagnostics.rows > 0 ? priors : null;
}

export interface SessionStructureStageAPrediction {
  probability: number;
  /** Evidence (max contributing key sample count) behind the estimate. */
  samples: number;
  source: "behavior" | "blend" | "base";
}

function logit(p: number): number {
  const c = Math.max(1e-4, Math.min(1 - 1e-4, p));
  return Math.log(c / (1 - c));
}

function sigmoid(x: number): number {
  return 1 / (1 + Math.exp(-x));
}

/**
 * Surrogate P(stage-A pass) for a candidate, derived from the adaptive-prior
 * aggregation (no ML model). The exact-fingerprint ("behavior") pass rate is the
 * best predictor and captures block interactions, so when it has enough samples
 * we trust it DIRECTLY (shrunk toward base) — validated against live data, where
 * the marginal-only blend mis-ranked interacting combos (e.g. vwap_pullback,
 * 55% paired with the right context but ~10% with atr_expansion). For unseen
 * combos we fall back to a Naive-Bayes log-odds blend of the marginal block
 * dimensions; that estimate is used for PRIORITISATION ONLY (samples reported as
 * 0 so it is never skip-eligible) since its calibration is unverified. Skips are
 * thus only ever backed by direct exact-combo evidence.
 */
export function predictSessionStructureStageAPass(params: {
  plan: Omit<SessionStructureComposerPlan, "digest">;
  venue: ScalpV2Venue;
  symbol: string;
  session: ScalpV2Session;
  priors: SessionStructureAdaptivePriorSet | null | undefined;
}): SessionStructureStageAPrediction {
  const priors = params.priors;
  const base = Number(priors?.stageABaseRate);
  if (!priors || !Number.isFinite(base) || base <= 0) {
    return { probability: Number.isFinite(base) ? base : 0, samples: 0, source: "base" };
  }
  const minSamples = Math.max(1, Math.floor(priors.minSamples || 8));
  const keys = sessionStructureAdaptiveKeys(params.plan);
  const scoped =
    priors.scoped[
      scopeKey({ venue: params.venue, symbol: params.symbol, session: params.session })
    ] || {};

  // Empirical pass rate for one key: scoped shrunk toward global, global shrunk
  // toward base. Returns the rate and the evidence (sample count) behind it.
  const keyEstimate = (key: string | undefined): { rate: number; samples: number } => {
    if (!key) return { rate: base, samples: 0 };
    const g = priors.global[key];
    const s = scoped[key];
    let gRate = base;
    let gSamples = 0;
    if (g && g.samples > 0) {
      const raw = g.stageAPass / g.samples;
      const shrink = g.samples / (g.samples + minSamples);
      gRate = raw * shrink + base * (1 - shrink);
      gSamples = g.samples;
    }
    if (s && s.samples > 0) {
      const raw = s.stageAPass / s.samples;
      const shrink = s.samples / (s.samples + minSamples);
      return { rate: raw * shrink + gRate * (1 - shrink), samples: s.samples + gSamples };
    }
    return { rate: gRate, samples: gSamples };
  };

  // Exact-fingerprint estimate first: when well-sampled it captures the block
  // interactions the marginals miss, so trust it directly (no marginal dilution).
  const behaviorKey = keys.find((k) => k.startsWith("behavior:"));
  const beh = keyEstimate(behaviorKey);
  if (beh.samples >= minSamples) {
    return {
      probability: Math.max(0, Math.min(1, beh.rate)),
      samples: beh.samples,
      source: "behavior",
    };
  }

  // Fallback: Naive-Bayes log-odds blend of the marginal block dimensions,
  // relative to the base rate. Prioritisation only — samples 0 (not skippable).
  const dims = ["context", "level", "trigger", "confirmation", "management"];
  const baseLogit = logit(base);
  let lo = baseLogit;
  let anyEvidence = false;
  for (const dim of dims) {
    const key = keys.find((k) => k.startsWith(`${dim}:`));
    const est = keyEstimate(key);
    if (est.samples <= 0) continue;
    const shrink = est.samples / (est.samples + minSamples);
    // Clamp each dimension's log-odds shift so one feature can't dominate.
    const shift = Math.max(-4, Math.min(4, (logit(est.rate) - baseLogit) * shrink));
    lo += shift;
    anyEvidence = true;
  }
  if (!anyEvidence) {
    return { probability: base, samples: 0, source: "base" };
  }
  return {
    probability: Math.max(0, Math.min(1, sigmoid(lo))),
    samples: 0,
    source: "blend",
  };
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
