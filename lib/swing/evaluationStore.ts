// lib/swing/evaluationStore.ts
//
// KV read/write for a symbol's last evaluation.
//
// NOTE: both functions are currently UNREFERENCED anywhere in the repo — they
// were left behind by an earlier evaluation flow. Kept (rather than deleted) so
// the module split stays a pure move; delete them if nothing adopts them.

import { setEvaluation, getEvaluation } from '../utils';

// Persist the last evaluation for a symbol
export async function persistEvaluation(symbol: string, evaluation: unknown) {
    await setEvaluation(symbol, evaluation);
}

// Retrieve the last evaluation for a symbol
export async function getLastEvaluation(symbol: string) {
    return getEvaluation(symbol);
}

// ------------------------------
// Prompt Builder (with guardrails, regime, momentum & extension gates)
// ------------------------------
