// Simple in-memory evaluation store.
// Not durable: data resets on server restart or cold starts.

export type EvaluationRecord = Record<string, any>;

function getStore(): EvaluationRecord {
  const g = globalThis as any;
  if (!g.__evaluationStore) {
    g.__evaluationStore = {};
  }
  return g.__evaluationStore as EvaluationRecord;
}

// Save or overwrite the latest evaluation for a symbol.
export function setEvaluation(symbol: string, evaluation: any) {
  if (!symbol) return;
  const store = getStore();
  store[symbol] = evaluation;
}

// Fetch the latest evaluation for a symbol.
export function getEvaluation(symbol: string) {
  if (!symbol) return null;
  const store = getStore();
  return store[symbol] ?? null;
}

// Snapshot of all evaluations (one per symbol).
export function getAllEvaluations() {
  const store = getStore();
  return { ...store };
}

// Remove a symbol's evaluation.
export function deleteEvaluation(symbol: string) {
  if (!symbol) return;
  const store = getStore();
  delete store[symbol];
}
