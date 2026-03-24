import { resolveScalpAdaptiveRuntimeConfig } from './config';
import { buildAdaptivePatternStatsMap, mineAdaptivePatternStats } from './mining';
import { computeEdge, computeHybridPriorScore } from './priors';
import type {
  ScalpAdaptiveFeatureContext,
  ScalpAdaptivePatternArm,
  ScalpAdaptivePatternStats,
  ScalpAdaptiveSelectionResult,
  ScalpAdaptiveSnapshotCatalog,
  ScalpAdaptiveSnapshotMetrics,
  ScalpAdaptiveTrainingRow,
} from './types';

function patternKey(tokens: string[]): string {
  return tokens.join('>');
}

function toPatternArmId(index: number): string {
  return `pattern_${String(index + 1).padStart(3, '0')}`;
}

function matchesNgram(tokens: string[], ngram: string[]): boolean {
  if (ngram.length > tokens.length) return false;
  for (let start = 0; start <= tokens.length - ngram.length; start += 1) {
    let matched = true;
    for (let i = 0; i < ngram.length; i += 1) {
      if (tokens[start + i] !== ngram[i]) {
        matched = false;
        break;
      }
    }
    if (matched) return true;
  }
  return false;
}

function statsForPattern(
  rowMap: Map<string, ScalpAdaptivePatternStats>,
  ngram: string[],
): ScalpAdaptivePatternStats {
  return (
    rowMap.get(patternKey(ngram)) || {
      ngram,
      support: 0,
      wins: 0,
      winRate: 0.5,
      meanProxyR: 0,
      edge: 0,
    }
  );
}

export function buildAdaptiveSnapshotCatalog(params: {
  localRows: ScalpAdaptiveTrainingRow[];
  sessionRows: ScalpAdaptiveTrainingRow[];
  globalRows: ScalpAdaptiveTrainingRow[];
  incumbentStrategyId: string;
  incumbentStrategyLabel?: string;
  generatedAtMs?: number;
}): {
  catalog: ScalpAdaptiveSnapshotCatalog;
  metrics: ScalpAdaptiveSnapshotMetrics;
} {
  const cfg = resolveScalpAdaptiveRuntimeConfig();
  const localPatterns = mineAdaptivePatternStats({
    rows: params.localRows,
    minSupport: cfg.minSupport,
    edgeScoreThreshold: cfg.edgeScoreThreshold,
    minLen: 2,
    maxLen: 3,
    maxPatterns: cfg.maxPatternArms,
  });
  const localStatsMap = buildAdaptivePatternStatsMap({
    rows: params.localRows,
    minLen: 2,
    maxLen: 3,
  });
  const sessionStatsMap = buildAdaptivePatternStatsMap({
    rows: params.sessionRows,
    minLen: 2,
    maxLen: 3,
  });
  const globalStatsMap = buildAdaptivePatternStatsMap({
    rows: params.globalRows,
    minLen: 2,
    maxLen: 3,
  });

  const patternArms: ScalpAdaptivePatternArm[] = [];
  for (let i = 0; i < localPatterns.length; i += 1) {
    const local = localPatterns[i] as ScalpAdaptivePatternStats;
    const session = statsForPattern(sessionStatsMap, local.ngram);
    const global = statsForPattern(globalStatsMap, local.ngram);
    const edgeLocal = computeEdge(local.winRate, local.meanProxyR);
    const edgeSession = computeEdge(session.winRate, session.meanProxyR);
    const edgeGlobal = computeEdge(global.winRate, global.meanProxyR);
    const hybrid = computeHybridPriorScore({
      nLocal: local.support,
      edgeLocal,
      nSession: session.support,
      edgeSession,
      edgeGlobal,
    });
    if (hybrid.score < cfg.edgeScoreThreshold) continue;
    patternArms.push({
      armId: toPatternArmId(i),
      ngram: local.ngram,
      support: local.support,
      winRate: local.winRate,
      meanProxyR: local.meanProxyR,
      edgeLocal,
      edgeSession,
      edgeGlobal,
      score: hybrid.score,
      confidence: hybrid.confidence,
    });
  }

  const generatedAtMs = Number.isFinite(params.generatedAtMs as number)
    ? Number(params.generatedAtMs)
    : Date.now();
  const localSamples = params.localRows.length;
  const positivePctLocal = localSamples
    ? (params.localRows.filter((row) => row.positive).length / localSamples) * 100
    : 0;
  const avgProxyRLocal = localSamples
    ? params.localRows.reduce((acc, row) => acc + row.proxyR, 0) / localSamples
    : 0;

  return {
    catalog: {
      version: 1,
      minConfidence: cfg.minConfidence,
      minSupport: cfg.minSupport,
      edgeScoreThreshold: cfg.edgeScoreThreshold,
      generatedAtMs,
      patternArms,
      incumbentArm: {
        armId: 'incumbent_arm',
        strategyId: params.incumbentStrategyId,
        strategyLabel: params.incumbentStrategyLabel || params.incumbentStrategyId,
      },
    },
    metrics: {
      localSamples,
      sessionSamples: params.sessionRows.length,
      globalSamples: params.globalRows.length,
      positivePctLocal,
      avgProxyRLocal,
      trainedAtMs: generatedAtMs,
      note: patternArms.length ? null : 'no_patterns_passing_threshold',
    },
  };
}

export function selectAdaptivePatternArm(
  context: ScalpAdaptiveFeatureContext,
  catalog: ScalpAdaptiveSnapshotCatalog | null | undefined,
): ScalpAdaptiveSelectionResult {
  const empty: ScalpAdaptiveSelectionResult = {
    selectedArmType: 'none',
    selectedArmId: null,
    confidence: 0,
    skipReason: 'catalog_unavailable',
    snapshotId: null,
    reasonCodes: ['ADAPTIVE_CATALOG_UNAVAILABLE'],
    featureHash: context.featureHash,
  };
  if (!catalog) return empty;
  if (!Array.isArray(catalog.patternArms) || !catalog.patternArms.length) {
    return {
      ...empty,
      skipReason: 'pattern_catalog_empty',
      reasonCodes: ['ADAPTIVE_PATTERN_CATALOG_EMPTY'],
    };
  }

  let best: ScalpAdaptivePatternArm | null = null;
  for (const arm of catalog.patternArms) {
    if (!matchesNgram(context.tokens, arm.ngram)) continue;
    if (!best || arm.confidence > best.confidence || (arm.confidence === best.confidence && arm.score > best.score)) {
      best = arm;
    }
  }
  if (!best) {
    return {
      selectedArmType: 'none',
      selectedArmId: null,
      confidence: 0,
      skipReason: 'no_pattern_match',
      snapshotId: null,
      reasonCodes: ['ADAPTIVE_NO_PATTERN_MATCH'],
      featureHash: context.featureHash,
    };
  }

  const minConfidence = Number.isFinite(catalog.minConfidence) ? catalog.minConfidence : 0.6;
  if (best.confidence < minConfidence) {
    return {
      selectedArmType: 'none',
      selectedArmId: null,
      confidence: best.confidence,
      skipReason: 'below_min_confidence',
      snapshotId: null,
      reasonCodes: ['ADAPTIVE_PATTERN_BELOW_CONFIDENCE'],
      featureHash: context.featureHash,
    };
  }

  return {
    selectedArmType: 'pattern',
    selectedArmId: best.armId,
    confidence: best.confidence,
    skipReason: null,
    snapshotId: null,
    reasonCodes: ['ADAPTIVE_PATTERN_SELECTED', `ADAPTIVE_ARM_${best.armId.toUpperCase()}`],
    featureHash: context.featureHash,
  };
}
