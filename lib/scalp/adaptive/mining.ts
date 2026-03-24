import { computeEdge } from './priors';
import type { ScalpAdaptivePatternStats, ScalpAdaptiveTrainingRow } from './types';

type PatternAccumulator = {
  ngram: string[];
  support: number;
  wins: number;
  proxySum: number;
};

function patternKey(ngram: string[]): string {
  return ngram.join('>');
}

function buildRowNgrams(tokens: string[], minLen: number, maxLen: number): string[][] {
  const out: string[][] = [];
  for (let start = 0; start < tokens.length; start += 1) {
    for (let len = minLen; len <= maxLen; len += 1) {
      const end = start + len;
      if (end > tokens.length) continue;
      out.push(tokens.slice(start, end));
    }
  }
  return out;
}

export function buildAdaptivePatternStatsMap(params: {
  rows: ScalpAdaptiveTrainingRow[];
  minLen?: number;
  maxLen?: number;
}): Map<string, ScalpAdaptivePatternStats> {
  const minLen = Math.max(1, Math.floor(Number(params.minLen) || 2));
  const maxLen = Math.max(minLen, Math.floor(Number(params.maxLen) || 3));
  const acc = new Map<string, PatternAccumulator>();

  for (const row of params.rows) {
    const ngrams = buildRowNgrams(row.tokens, minLen, maxLen);
    const seen = new Set<string>();
    for (const ngram of ngrams) {
      const key = patternKey(ngram);
      if (seen.has(key)) continue;
      seen.add(key);
      const current = acc.get(key) || {
        ngram,
        support: 0,
        wins: 0,
        proxySum: 0,
      };
      current.support += 1;
      current.wins += row.positive ? 1 : 0;
      current.proxySum += row.proxyR;
      acc.set(key, current);
    }
  }

  const out = new Map<string, ScalpAdaptivePatternStats>();
  for (const [key, row] of acc.entries()) {
    const support = Math.max(0, row.support);
    const winRate = support > 0 ? row.wins / support : 0;
    const meanProxyR = support > 0 ? row.proxySum / support : 0;
    out.set(key, {
      ngram: row.ngram,
      support,
      wins: row.wins,
      winRate,
      meanProxyR,
      edge: computeEdge(winRate, meanProxyR),
    });
  }
  return out;
}

export function mineAdaptivePatternStats(params: {
  rows: ScalpAdaptiveTrainingRow[];
  minSupport: number;
  edgeScoreThreshold: number;
  minLen?: number;
  maxLen?: number;
  maxPatterns?: number;
}): ScalpAdaptivePatternStats[] {
  const minSupport = Math.max(1, Math.floor(Number(params.minSupport) || 1));
  const edgeThreshold = Number.isFinite(params.edgeScoreThreshold)
    ? Number(params.edgeScoreThreshold)
    : 0;
  const maxPatterns = Math.max(1, Math.floor(Number(params.maxPatterns) || 24));
  const statsMap = buildAdaptivePatternStatsMap({
    rows: params.rows,
    minLen: params.minLen,
    maxLen: params.maxLen,
  });

  return Array.from(statsMap.values())
    .filter((row) => row.support >= minSupport && row.edge >= edgeThreshold)
    .sort((a, b) => {
      if (b.edge !== a.edge) return b.edge - a.edge;
      if (b.support !== a.support) return b.support - a.support;
      if (b.meanProxyR !== a.meanProxyR) return b.meanProxyR - a.meanProxyR;
      return patternKey(a.ngram).localeCompare(patternKey(b.ngram));
    })
    .slice(0, maxPatterns);
}
