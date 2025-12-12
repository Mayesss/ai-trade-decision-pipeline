// Simple key-value store for evaluations, in-memory for now, can be replaced with persistent storage later
// Usage: setEvaluation(symbol, evaluation), getEvaluation(symbol)

const evaluationStore: Record<string, any> = {};

export function setEvaluation(symbol: string, evaluation: any) {
  evaluationStore[symbol] = evaluation;
}

export function getEvaluation(symbol: string) {
  return evaluationStore[symbol] || null;
}
