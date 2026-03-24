export function clip(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

export function sigmoid(value: number): number {
  return 1 / (1 + Math.exp(-value));
}

export function computeEdge(winRate: number, meanProxyR: number): number {
  const wr = Number.isFinite(winRate) ? winRate : 0.5;
  const proxy = Number.isFinite(meanProxyR) ? meanProxyR : 0;
  return 0.7 * (wr - 0.5) + 0.3 * clip(proxy / 2, -1, 1);
}

export interface HybridPriorInput {
  nLocal: number;
  edgeLocal: number;
  nSession: number;
  edgeSession: number;
  edgeGlobal: number;
}

export interface HybridPriorResult {
  wLocal: number;
  wSession: number;
  wGlobal: number;
  score: number;
  confidence: number;
}

export function computeHybridPriorScore(input: HybridPriorInput): HybridPriorResult {
  const nLocal = Math.max(0, Math.floor(Number(input.nLocal) || 0));
  const nSession = Math.max(0, Math.floor(Number(input.nSession) || 0));
  const edgeLocal = Number.isFinite(input.edgeLocal) ? input.edgeLocal : 0;
  const edgeSession = Number.isFinite(input.edgeSession) ? input.edgeSession : 0;
  const edgeGlobal = Number.isFinite(input.edgeGlobal) ? input.edgeGlobal : 0;

  const wLocal = nLocal / (nLocal + 40);
  const wSessionUnscaled = nSession / (nSession + 200);
  const wSession = (1 - wLocal) * wSessionUnscaled;
  const wGlobal = Math.max(0, 1 - wLocal - wSession);

  const score = wLocal * edgeLocal + wSession * edgeSession + wGlobal * edgeGlobal;
  return {
    wLocal,
    wSession,
    wGlobal,
    score,
    confidence: sigmoid(4 * score),
  };
}
