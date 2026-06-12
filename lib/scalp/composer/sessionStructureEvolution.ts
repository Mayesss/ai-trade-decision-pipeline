import crypto from "crypto";

import { isScalpPgConfigured, scalpPrisma, sql } from "./pg";
import {
  asRecord,
  normalizeSession,
  normalizeVenue,
  scopeKey as adaptiveScopeKey,
  stableJson,
} from "./sessionStructureAdaptiveSearch";
import {
  SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID,
  SESSION_STRUCTURE_CONFIRMATION_BLOCKS,
  SESSION_STRUCTURE_CONTEXT_BLOCKS,
  SESSION_STRUCTURE_LEVEL_BLOCKS,
  SESSION_STRUCTURE_MANAGEMENT_BLOCKS,
  SESSION_STRUCTURE_TRIGGER_BLOCKS,
  buildSessionStructureComposerTuneId,
  sessionStructureBehaviorFingerprint,
  validateSessionStructureCompatibility,
  type SessionStructureComposerPlan,
} from "./sessionStructureComposer";
import type { ScalpComposerSession, ScalpComposerVenue } from "./types";

export const SESSION_STRUCTURE_EVOLUTION_VERSION =
  "session_structure_evolution_v1";

/** The heritable part of a candidate: the 5 genes, no scope-derived digest. */
export type SessionStructureGenome = Omit<
  SessionStructureComposerPlan,
  "digest"
>;

export type SessionStructureEvolutionOp = "mutation" | "crossover";

export interface SessionStructureSurvivor {
  scopeKey: string;
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
  genome: SessionStructureGenome;
  fingerprint: string;
  tuneId: string;
  fitness: number;
  fitnessSource: "lowerBoundR" | "netR";
  stageCTrades: number;
  holdoutPassed: boolean | null;
}

export interface SessionStructureSurvivorPool {
  version: string;
  generatedAtMs: number;
  windowToTs: number;
  global: SessionStructureSurvivor[];
  scoped: Record<string, SessionStructureSurvivor[]>;
  /** Per-scope fingerprints already evaluated — best-effort dedup (sorted). */
  evaluatedByScope: Record<string, string[]>;
  diagnostics: {
    rows: number;
    survivors: number;
    scopedScopes: number;
    usedNetRFallback: number;
    weakOnly: boolean;
  };
}

export interface SessionStructureEvolutionConfig {
  enabled: boolean;
  topKScoped: number;
  topKGlobal: number;
  maxRows: number;
  minTrades: number;
  maxOffspringPerSurvivor: number;
  maxOffspringPerCycle: number;
  maxCrossoverPartners: number;
  minFitness: number;
  globalCrossover: boolean;
}

export interface SessionStructureOffspring {
  genome: SessionStructureGenome;
  op: SessionStructureEvolutionOp;
  parentFingerprints: string[];
  parentTuneIds: string[];
  bestParentFitness: number;
  /** Rank-scaled boost weight in [0,1]; the builder multiplies by the env boost. */
  rankWeight: number;
}

type SurvivorRow = {
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
  tuneId: string;
  metadata: Record<string, unknown>;
};

const BLOCK_DIMENSIONS = [
  { key: "contextId", values: SESSION_STRUCTURE_CONTEXT_BLOCKS },
  { key: "levelId", values: SESSION_STRUCTURE_LEVEL_BLOCKS },
  { key: "triggerId", values: SESSION_STRUCTURE_TRIGGER_BLOCKS },
  { key: "confirmationId", values: SESSION_STRUCTURE_CONFIRMATION_BLOCKS },
  { key: "managementId", values: SESSION_STRUCTURE_MANAGEMENT_BLOCKS },
] as const;

function envBool(name: string, fallback: boolean): boolean {
  const raw = String(process.env[name] || "").trim().toLowerCase();
  if (!raw) return fallback;
  return raw === "1" || raw === "true" || raw === "yes" || raw === "on";
}

function envInt(name: string, fallback: number, min: number, max: number): number {
  const rawStr = String(process.env[name] || "").trim();
  const raw = rawStr ? Number(rawStr) : fallback;
  const value = Number.isFinite(raw) ? Math.floor(raw) : fallback;
  return Math.max(min, Math.min(max, value));
}

function envFiniteOr(name: string, fallback: number): number {
  const raw = String(process.env[name] || "").trim();
  if (!raw) return fallback;
  const value = Number(raw);
  return Number.isFinite(value) ? value : fallback;
}

export function resolveSessionStructureEvolutionConfig(): SessionStructureEvolutionConfig {
  return {
    enabled: envBool("SCALP_COMPOSER_SESSION_EVOLUTION_ENABLED", true),
    topKScoped: envInt("SCALP_COMPOSER_SESSION_EVOLUTION_TOP_K_SCOPED", 8, 1, 50),
    topKGlobal: envInt("SCALP_COMPOSER_SESSION_EVOLUTION_TOP_K_GLOBAL", 24, 1, 200),
    maxRows: envInt("SCALP_COMPOSER_SESSION_EVOLUTION_MAX_ROWS", 5_000, 1, 100_000),
    minTrades: envInt("SCALP_COMPOSER_SESSION_EVOLUTION_MIN_TRADES", 8, 0, 10_000),
    maxOffspringPerSurvivor: envInt(
      "SCALP_COMPOSER_SESSION_EVOLUTION_MAX_OFFSPRING_PER_SURVIVOR",
      12,
      1,
      200,
    ),
    maxOffspringPerCycle: envInt(
      "SCALP_COMPOSER_SESSION_EVOLUTION_MAX_OFFSPRING_PER_CYCLE",
      200,
      1,
      5_000,
    ),
    maxCrossoverPartners: envInt(
      "SCALP_COMPOSER_SESSION_EVOLUTION_MAX_CROSSOVER_PARTNERS",
      4,
      0,
      50,
    ),
    // Breed ONLY from significant survivors (lowerBoundR >= 0). Live data
    // (2026-06-08) showed breeding from negative-edge "least-bad" survivors is
    // net-negative: scope-controlled, evolution offspring passed stage A at
    // 13.6% vs the baseline grid's 56.4% (mutating a working combo's block
    // usually breaks it). With all current survivors negative this yields zero
    // breeders → evolution sleeps; it auto-reactivates around a genuinely
    // significant winner. Set to -Infinity to breed from least-bad again.
    minFitness: envFiniteOr("SCALP_COMPOSER_SESSION_EVOLUTION_MIN_FITNESS", 0),
    globalCrossover: envBool("SCALP_COMPOSER_SESSION_EVOLUTION_GLOBAL_CROSSOVER", false),
  };
}

function genomeFromPlan(
  plan: Partial<SessionStructureComposerPlan>,
): SessionStructureGenome | null {
  if (
    !plan.contextId ||
    !plan.levelId ||
    !plan.triggerId ||
    !plan.confirmationId ||
    !plan.managementId
  ) {
    return null;
  }
  return {
    contextId: plan.contextId,
    levelId: plan.levelId,
    triggerId: plan.triggerId,
    confirmationId: plan.confirmationId,
    managementId: plan.managementId,
  };
}

function stageCStats(metadata: Record<string, unknown>): {
  lowerBoundR: number;
  netR: number;
  trades: number;
  holdoutPassed: boolean | null;
} {
  const worker = asRecord(metadata.worker);
  const activeWorker =
    Object.keys(worker).length > 0 ? worker : asRecord(metadata.previousWorker);
  const stageC = asRecord(activeWorker.stageC);
  const ranking = asRecord(metadata.v3Ranking);
  const stageCRankingStats = asRecord(asRecord(ranking.stageC).stats);
  const lowerBoundR = Number(stageCRankingStats.lowerBoundR);
  const netR = Number(
    Number.isFinite(Number(stageCRankingStats.netR))
      ? stageCRankingStats.netR
      : stageC.netR,
  );
  const trades = Math.max(0, Math.floor(Number(stageC.trades) || 0));
  const holdout = asRecord(activeWorker.holdout);
  const stageCHoldout = asRecord(stageC.v3Holdout);
  const holdoutPassed =
    typeof holdout.passed === "boolean"
      ? holdout.passed
      : typeof stageCHoldout.passed === "boolean"
        ? stageCHoldout.passed
        : null;
  return { lowerBoundR, netR, trades, holdoutPassed };
}

/**
 * Pure transform: rank stage-C-reached rows into per-scope and global survivor
 * lists. Fitness = lowerBoundR (the #4 significance statistic), netR fallback
 * only when lowerBoundR is missing. Weak survivors (all negative) are KEPT and
 * ranked least-bad — gating on positive fitness would empty the pool today.
 */
export function buildSurvivorPoolFromRows(params: {
  rows: SurvivorRow[];
  windowToTs: number;
  nowMs?: number;
  topKScoped?: number;
  topKGlobal?: number;
  minFitness?: number;
  minTrades?: number;
}): SessionStructureSurvivorPool {
  const topKScoped = Math.max(1, Math.floor(Number(params.topKScoped ?? 8)));
  const topKGlobal = Math.max(1, Math.floor(Number(params.topKGlobal ?? 24)));
  const minTrades = Math.max(0, Math.floor(Number(params.minTrades ?? 0)));
  const minFitness = Number.isFinite(Number(params.minFitness))
    ? Number(params.minFitness)
    : Number.NEGATIVE_INFINITY;

  // Dedup by (scope, fingerprint), keep the max-fitness instance.
  const byScopeFingerprint = new Map<string, SessionStructureSurvivor>();
  const evaluatedByScope = new Map<string, Set<string>>();
  let rows = 0;
  let usedNetRFallback = 0;

  for (const row of params.rows) {
    const metadata = asRecord(row.metadata);
    const genome = genomeFromPlan(
      asRecord(metadata.sessionComposerPlan) as Partial<SessionStructureComposerPlan>,
    );
    if (!genome) continue;
    rows += 1;
    const fingerprint = sessionStructureBehaviorFingerprint(genome);
    const scope = adaptiveScopeKey(row);
    const evalSet = evaluatedByScope.get(scope) || new Set<string>();
    evalSet.add(fingerprint);
    evaluatedByScope.set(scope, evalSet);

    const stats = stageCStats(metadata);
    // Drop degenerate rows (e.g. 0-trade stage-C) — they carry no edge estimate.
    if (stats.trades < minTrades) continue;
    let fitness = stats.lowerBoundR;
    let fitnessSource: SessionStructureSurvivor["fitnessSource"] = "lowerBoundR";
    if (!Number.isFinite(fitness)) {
      fitness = stats.netR;
      fitnessSource = "netR";
    }
    if (!Number.isFinite(fitness)) continue;
    if (fitness < minFitness) continue;

    const survivor: SessionStructureSurvivor = {
      scopeKey: scope,
      venue: row.venue,
      symbol: row.symbol,
      session: row.session,
      genome,
      fingerprint,
      tuneId: row.tuneId,
      fitness,
      fitnessSource,
      stageCTrades: stats.trades,
      holdoutPassed: stats.holdoutPassed,
    };
    const dedupKey = `${scope}#${fingerprint}`;
    const prev = byScopeFingerprint.get(dedupKey);
    if (!prev || rank(survivor, prev) < 0) {
      byScopeFingerprint.set(dedupKey, survivor);
    }
  }

  // Two-tier: lowerBoundR-bearing survivors ALWAYS outrank netR-fallback ones
  // (lowerBoundR primary, netR only when missing). Then by fitness within tier,
  // then trades, then fingerprint for determinism. lowerBoundR (significance)
  // and netR (raw return) are not on the same scale — never compare across tiers.
  function rank(a: SessionStructureSurvivor, b: SessionStructureSurvivor): number {
    const tierA = a.fitnessSource === "lowerBoundR" ? 0 : 1;
    const tierB = b.fitnessSource === "lowerBoundR" ? 0 : 1;
    return (
      tierA - tierB ||
      b.fitness - a.fitness ||
      b.stageCTrades - a.stageCTrades ||
      a.fingerprint.localeCompare(b.fingerprint)
    );
  }

  const all = Array.from(byScopeFingerprint.values());
  usedNetRFallback = all.filter((s) => s.fitnessSource === "netR").length;

  const scoped: Record<string, SessionStructureSurvivor[]> = {};
  const byScope = new Map<string, SessionStructureSurvivor[]>();
  for (const survivor of all) {
    const bucket = byScope.get(survivor.scopeKey) || [];
    bucket.push(survivor);
    byScope.set(survivor.scopeKey, bucket);
  }
  for (const [scope, bucket] of byScope.entries()) {
    scoped[scope] = bucket.sort(rank).slice(0, topKScoped);
  }
  const global = all.slice().sort(rank).slice(0, topKGlobal);

  const evaluatedOut: Record<string, string[]> = {};
  for (const [scope, set] of evaluatedByScope.entries()) {
    evaluatedOut[scope] = Array.from(set).sort();
  }

  return {
    version: SESSION_STRUCTURE_EVOLUTION_VERSION,
    generatedAtMs: Math.floor(Number(params.nowMs || Date.now())),
    windowToTs: Math.floor(Number(params.windowToTs || 0)),
    global,
    scoped,
    evaluatedByScope: evaluatedOut,
    diagnostics: {
      rows,
      survivors: all.length,
      scopedScopes: Object.keys(scoped).length,
      usedNetRFallback,
      weakOnly: all.length > 0 && all.every((s) => s.fitness < 0),
    },
  };
}

export async function loadSessionStructureSurvivors(params: {
  windowToTs: number;
  nowMs?: number;
  limit?: number;
  topKScoped?: number;
  topKGlobal?: number;
  minFitness?: number;
  minTrades?: number;
}): Promise<SessionStructureSurvivorPool | null> {
  if (!isScalpPgConfigured()) return null;
  const limit = Math.max(1, Math.min(100_000, Math.floor(Number(params.limit || 5_000))));
  const windowToTs = Math.floor(Number(params.windowToTs || 0));
  const db = scalpPrisma();
  // Scalar projection (subquery coalesces worker→previousWorker at object level,
  // matching the reader). Selecting whole metadata_json / worker objects for many
  // rows exceeds the Neon HTTP 64MB cap and the load silently returns null.
  const rows = await db.$queryRaw<Array<{
    venue: string;
    symbol: string;
    entrySessionProfile: string;
    tuneId: string;
    plan: unknown;
    scPassed: string | null;
    scNetR: string | null;
    scTrades: string | null;
    lowerBoundR: string | null;
    statsNetR: string | null;
    holdoutPassed: string | null;
  }>>(sql`
    SELECT
      t.venue,
      t.symbol,
      t."entrySessionProfile",
      t."tuneId",
      t.plan AS "plan",
      t.w->'stageC'->>'passed' AS "scPassed",
      t.w->'stageC'->>'netR' AS "scNetR",
      t.w->'stageC'->>'trades' AS "scTrades",
      t.vr->'stageC'->'stats'->>'lowerBoundR' AS "lowerBoundR",
      t.vr->'stageC'->'stats'->>'netR' AS "statsNetR",
      COALESCE(
        t.w->'holdout'->>'passed',
        t.w->'stageC'->'v3Holdout'->>'passed'
      ) AS "holdoutPassed"
    FROM (
      SELECT
        venue,
        symbol,
        entry_session_profile AS "entrySessionProfile",
        tune_id AS "tuneId",
        metadata_json->'sessionComposerPlan' AS plan,
        COALESCE(metadata_json->'worker', metadata_json->'previousWorker') AS w,
        metadata_json->'v3Ranking' AS vr
      FROM scalp_v2_candidates
      WHERE strategy_id = ${SESSION_STRUCTURE_COMPOSER_V1_STRATEGY_ID}
        AND metadata_json->'sessionComposerPlan' IS NOT NULL
        AND (
          metadata_json->'worker'->'stageC' IS NOT NULL
          OR metadata_json->'previousWorker'->'stageC' IS NOT NULL
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
  const pool = buildSurvivorPoolFromRows({
    rows: rows.map((row) => ({
      venue: normalizeVenue(row.venue),
      symbol: String(row.symbol || "").trim().toUpperCase(),
      session: normalizeSession(row.entrySessionProfile),
      tuneId: String(row.tuneId || ""),
      // Reconstruct the minimal shape stageCStats reads from the scalars.
      metadata: {
        sessionComposerPlan: asRecord(row.plan),
        worker: {
          stageC: {
            passed: row.scPassed === "true",
            netR: row.scNetR == null ? undefined : Number(row.scNetR),
            trades: row.scTrades == null ? undefined : Number(row.scTrades),
          },
          holdout:
            row.holdoutPassed == null
              ? undefined
              : { passed: row.holdoutPassed === "true" },
        },
        v3Ranking: {
          stageC: {
            stats: {
              lowerBoundR:
                row.lowerBoundR == null ? undefined : Number(row.lowerBoundR),
              netR: row.statsNetR == null ? undefined : Number(row.statsNetR),
            },
          },
        },
      },
    })),
    windowToTs,
    nowMs: params.nowMs,
    topKScoped: params.topKScoped,
    topKGlobal: params.topKGlobal,
    minFitness: params.minFitness,
    minTrades: params.minTrades,
  });
  return pool.diagnostics.survivors > 0 ? pool : null;
}

export function fingerprintSessionStructureSurvivorPool(
  pool: SessionStructureSurvivorPool | null | undefined,
): string {
  if (!pool) return "survivors:none";
  const compact = (s: SessionStructureSurvivor) => ({
    f: s.fingerprint,
    fit: Number(s.fitness.toFixed(4)),
  });
  return stableJson({
    version: pool.version,
    windowToTs: pool.windowToTs,
    global: pool.global.map(compact),
    scoped: Object.fromEntries(
      Object.entries(pool.scoped).map(([scope, list]) => [
        scope,
        list.map(compact),
      ]),
    ),
  });
}

// ---------------------------------------------------------------------------
// Genetic operators (pure)
// ---------------------------------------------------------------------------

/** Hamming-1 neighbourhood: each gene swapped to every other valid block ID. */
export function enumerateSingleGeneMutations(
  genome: SessionStructureGenome,
): SessionStructureGenome[] {
  const out: SessionStructureGenome[] = [];
  for (const dim of BLOCK_DIMENSIONS) {
    const current = genome[dim.key];
    for (const value of dim.values) {
      if (value === current) continue;
      out.push({ ...genome, [dim.key]: value } as SessionStructureGenome);
    }
  }
  return out;
}

/** 2-parent uniform crossover: all 2^5 gene-source combos, minus the parents. */
export function enumerateCrossovers(
  a: SessionStructureGenome,
  b: SessionStructureGenome,
): SessionStructureGenome[] {
  const keys = BLOCK_DIMENSIONS.map((dim) => dim.key);
  const seen = new Set<string>([
    sessionStructureBehaviorFingerprint(a),
    sessionStructureBehaviorFingerprint(b),
  ]);
  const out: SessionStructureGenome[] = [];
  const total = 1 << keys.length; // 32
  for (let mask = 0; mask < total; mask += 1) {
    const child = {} as SessionStructureGenome;
    for (let i = 0; i < keys.length; i += 1) {
      const key = keys[i]!;
      const source = mask & (1 << i) ? b : a;
      (child as Record<string, unknown>)[key] = source[key];
    }
    const fp = sessionStructureBehaviorFingerprint(child);
    if (seen.has(fp)) continue;
    seen.add(fp);
    out.push(child);
  }
  return out;
}

function compatible(genome: SessionStructureGenome): boolean {
  return validateSessionStructureCompatibility(genome).compatible;
}

/**
 * Generate offspring for one scope from its survivors (scoped pool, optionally
 * topped up with global survivors / global crossover for cold scopes). Returns
 * a fingerprint -> offspring map. Offspring are GENOMES only; the canonical
 * digest/tuneId is computed by the grid builder, never minted here.
 */
export function generateOffspring(params: {
  scopedSurvivors: SessionStructureSurvivor[];
  globalSurvivors?: SessionStructureSurvivor[];
  evaluatedFingerprints?: Set<string>;
  config: Pick<
    SessionStructureEvolutionConfig,
    | "maxOffspringPerSurvivor"
    | "maxOffspringPerCycle"
    | "maxCrossoverPartners"
    | "globalCrossover"
  >;
}): Map<string, SessionStructureOffspring> {
  const evaluated = params.evaluatedFingerprints || new Set<string>();
  const scoped = params.scopedSurvivors || [];
  const global = params.globalSurvivors || [];
  // Cold scope: no scoped survivors → optionally seed from global pool.
  const breeders = scoped.length > 0 ? scoped : params.config.globalCrossover ? global : [];
  if (breeders.length === 0) return new Map();

  const parentFingerprints = new Set(breeders.map((s) => s.fingerprint));
  const crossoverPool =
    scoped.length > 0
      ? scoped
      : params.config.globalCrossover
        ? global
        : [];

  const out = new Map<string, SessionStructureOffspring>();
  const topK = Math.max(1, breeders.length);

  for (let i = 0; i < breeders.length; i += 1) {
    const survivor = breeders[i]!;
    const rankWeight = 1 - i / topK;
    const local: Array<{ genome: SessionStructureGenome; op: SessionStructureEvolutionOp; parents: SessionStructureSurvivor[] }> = [];

    for (const child of enumerateSingleGeneMutations(survivor.genome)) {
      local.push({ genome: child, op: "mutation", parents: [survivor] });
    }
    const partners = crossoverPool
      .filter((p) => p.fingerprint !== survivor.fingerprint)
      .slice(0, params.config.maxCrossoverPartners);
    for (const partner of partners) {
      for (const child of enumerateCrossovers(survivor.genome, partner.genome)) {
        local.push({ genome: child, op: "crossover", parents: [survivor, partner] });
      }
    }

    // Compatible, not a parent, not already evaluated. Mutations preferred,
    // then deterministic lexical order by fingerprint.
    const candidates = local
      .map((entry) => ({
        ...entry,
        fingerprint: sessionStructureBehaviorFingerprint(entry.genome),
      }))
      .filter((entry) => compatible(entry.genome))
      .filter((entry) => !parentFingerprints.has(entry.fingerprint))
      .filter((entry) => !evaluated.has(entry.fingerprint))
      .sort(
        (a, b) =>
          (a.op === "mutation" ? 0 : 1) - (b.op === "mutation" ? 0 : 1) ||
          a.fingerprint.localeCompare(b.fingerprint),
      )
      .slice(0, params.config.maxOffspringPerSurvivor);

    for (const entry of candidates) {
      const existing = out.get(entry.fingerprint);
      const bestParentFitness = Math.max(...entry.parents.map((p) => p.fitness));
      if (existing && existing.bestParentFitness >= bestParentFitness) continue;
      out.set(entry.fingerprint, {
        genome: entry.genome,
        op: entry.op,
        parentFingerprints: entry.parents.map((p) => p.fingerprint),
        parentTuneIds: entry.parents.map((p) => p.tuneId),
        bestParentFitness,
        rankWeight: Math.max(0, Math.min(1, rankWeight)),
      });
    }
  }

  // Hard per-cycle cap: keep the highest-parent-fitness offspring deterministically.
  if (out.size <= params.config.maxOffspringPerCycle) return out;
  const trimmed = Array.from(out.entries())
    .sort(
      (a, b) =>
        b[1].bestParentFitness - a[1].bestParentFitness ||
        a[0].localeCompare(b[0]),
    )
    .slice(0, params.config.maxOffspringPerCycle);
  return new Map(trimmed);
}

/** Builder-facing helper: the canonical tuneId for an offspring in a scope. */
export function offspringTuneId(params: {
  genome: SessionStructureGenome;
  venue: ScalpComposerVenue;
  symbol: string;
  session: ScalpComposerSession;
}): string {
  const fingerprint = sessionStructureBehaviorFingerprint(params.genome);
  const digest = crypto
    .createHash("sha1")
    .update(`${params.venue}:${params.symbol}:${params.session}:${fingerprint}`)
    .digest("hex")
    .slice(0, 10);
  return buildSessionStructureComposerTuneId({ ...params.genome, digest });
}
