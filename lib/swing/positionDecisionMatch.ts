import type { DecisionHistoryEntry } from '../history';

// Matching a closed position to the AI decisions around it, for the chart's
// position-overlay tooltips. Shared by the chart endpoint (cache miss) and the
// overlay warm cache (cache hit) — the two had their own copies and drifted,
// which is how a BUY that placed the NEXT resting entry ended up labelled as a
// long's "exit AI decision".

// How close an AI CLOSE/REVERSE has to sit to a position's exit to be credited
// with it. Beyond this the position was closed by its exchange-side bracket.
export const AI_CLOSE_MATCH_MS = 20 * 60 * 1000;
// Bracket-close inference is only claimed inside the KV history window; older
// exits are unknowable (their decision rows have expired, so "no decision
// found" means nothing).
export const CLOSE_REASON_HISTORY_MS = 7 * 24 * 60 * 60 * 1000;

export type OverlayDecisionBrief = {
  timestamp: number | null;
  action?: string;
  summary?: string;
  reason?: string;
  closePct: number | null;
};

const finiteNumber = (value: unknown): number | null => {
  if (value === null || value === undefined || value === '') return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const actionOf = (entry: DecisionHistoryEntry | null | undefined): string =>
  String(entry?.aiDecision?.action || '').toUpperCase();

// An exit-shaped decision: the only two actions that end a position.
const isExitAction = (entry: DecisionHistoryEntry | null | undefined): boolean => {
  const action = actionOf(entry);
  return action === 'CLOSE' || action === 'REVERSE';
};

// Carried so a trim renders as "Close 30%" rather than a bare "Close" in the
// overlay tooltip's exit/entry decision label.
export function getPartialClosePct(entry: DecisionHistoryEntry | null | undefined): number | null {
  const pct =
    finiteNumber(entry?.execResult?.partialClosePct) ??
    finiteNumber(entry?.aiDecision?.exit_size_pct) ??
    finiteNumber(entry?.aiDecision?.close_size_pct) ??
    finiteNumber(entry?.aiDecision?.partial_close_pct);
  return pct !== null && pct > 0 && pct < 100 ? pct : null;
}

const brief = (entry: DecisionHistoryEntry): OverlayDecisionBrief => ({
  timestamp: Number(entry.timestamp) || null,
  action: entry.aiDecision?.action,
  summary: entry.aiDecision?.summary,
  reason: entry.aiDecision?.reason,
  closePct: getPartialClosePct(entry),
});

function nearest(
  history: DecisionHistoryEntry[] | null | undefined,
  tsMs: number | null | undefined,
  accept: (entry: DecisionHistoryEntry) => boolean,
  maxDiffMs = Number.POSITIVE_INFINITY,
): { entry: DecisionHistoryEntry; diffMs: number } | null {
  if (!tsMs || !history?.length) return null;
  let best: DecisionHistoryEntry | null = null;
  let bestDiff = Number.POSITIVE_INFINITY;
  for (const h of history) {
    if (!h.timestamp) continue;
    if (!accept(h)) continue;
    const diff = Math.abs(Number(h.timestamp) - tsMs);
    if (diff < bestDiff) {
      bestDiff = diff;
      best = h;
    }
  }
  if (!best || bestDiff > maxDiffMs) return null;
  return { entry: best, diffMs: bestDiff };
}

// The decision that opened a position: nearest in time, any action. An entry is
// unambiguous — the position exists because something placed it — so this stays
// a plain proximity match.
export function findEntryDecision(
  history: DecisionHistoryEntry[] | null | undefined,
  tsMs?: number | null,
): OverlayDecisionBrief | null {
  const hit = nearest(history, tsMs, () => true);
  return hit ? brief(hit.entry) : null;
}

// The decision that closed a position — CLOSE or REVERSE only, and only inside
// the same tolerance the bracket inference below uses. A plain proximity match
// happily credited the exit to whatever ran next (a fresh BUY placing the next
// resting entry, minutes after a TP hit), which is not an exit at all. No match
// means the bracket closed it, and `inferCloseReason` says which leg.
export function findExitDecision(
  history: DecisionHistoryEntry[] | null | undefined,
  tsMs?: number | null,
): OverlayDecisionBrief | null {
  const hit = nearest(history, tsMs, isExitAction, AI_CLOSE_MATCH_MS);
  return hit ? brief(hit.entry) : null;
}

// An exchange-side TP/SL exit has no AI decision of its own, so a closed
// position with no CLOSE/REVERSE near its exit was closed by the resting
// bracket — TP vs SL by realized pnl sign.
export function inferCloseReason(params: {
  history: DecisionHistoryEntry[] | null | undefined;
  exitTsMs: number | null | undefined;
  pnlValue: number | null | undefined;
  nowMs: number;
}): 'tp' | 'sl' | null {
  const { exitTsMs, pnlValue } = params;
  if (!exitTsMs || typeof pnlValue !== 'number') return null;
  if (exitTsMs < params.nowMs - CLOSE_REASON_HISTORY_MS) return null;
  if (nearest(params.history, exitTsMs, isExitAction, AI_CLOSE_MATCH_MS)) return null;
  return pnlValue >= 0 ? 'tp' : 'sl';
}
