import { RESTING_ENTRY_MAX_AGE_MINUTES } from './decisionConfig';
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

// --------------------------------------------------------------------------
// Which bracket leg closed the position
// --------------------------------------------------------------------------

// One tick's worth of bracket evidence, flattened. Callers hold different row
// shapes (KV history entries, Neon decision rows, the slim bracket-trail
// query), so they all narrow to this before the reconstruction below reads it.
export type BracketTrailRow = {
  tsMs: number;
  action?: string | null;
  // The tick actually placed an order (an entry ships its bracket with it).
  placed?: boolean | null;
  // Sanitized bracket ON the decision — what analyze wrote back after
  // validation, i.e. the levels an accepted entry was placed with.
  takeProfitPrice?: number | null;
  stopLossPrice?: number | null;
  // execResult.tpsl: an in-position amendment, with the venue's verdict.
  tpsl?: unknown;
  // execResult.management.beStop: the auto/requested breakeven stop.
  beStop?: unknown;
};

export type BracketAtExit = { takeProfit: number | null; stopLoss: number | null };

// Cause of a close, with the evidence that decided it. 'unknown' means the
// exit predates what we can read back, not that nothing closed it.
export type CloseCause = {
  cause: 'take_profit' | 'stop_loss' | 'ai_close' | 'unknown';
  // How it was decided: which bracket leg the exit price landed on, the pnl
  // sign (no bracket levels recoverable), or a matched CLOSE/REVERSE.
  basis: 'bracket_level' | 'pnl_sign' | 'ai_decision' | 'unknown';
  takeProfit: number | null;
  stopLoss: number | null;
};

// The rows the AI-close check reads. Deliberately looser than
// DecisionHistoryEntry: the post-mortem holds Neon rows (decidedAtMs, an
// untyped aiDecision) and must reach the same verdict as the chart, which
// holds KV history entries.
export type ExitCandidateRow = {
  timestamp?: unknown;
  decidedAtMs?: unknown;
  aiDecision?: { action?: unknown } | null;
};

// Did an AI CLOSE/REVERSE run close enough to the exit to own it?
function hasAiExitNear(rows: ExitCandidateRow[] | null | undefined, exitTsMs: number): boolean {
  for (const row of rows || []) {
    const ts = finiteNumber(row?.decidedAtMs ?? row?.timestamp);
    if (ts === null) continue;
    const action = String(row?.aiDecision?.action || '').toUpperCase();
    if (action !== 'CLOSE' && action !== 'REVERSE') continue;
    if (Math.abs(ts - exitTsMs) <= AI_CLOSE_MATCH_MS) return true;
  }
  return false;
}

// How far BEFORE a position's entry its own entry decision can sit: a resting
// entry is placed, then waits for the fill, and the fill timestamp is the
// position's entry. The age backstop cancels anything older, so this is the
// exact bound — and a 5-minute slack silently dropped the bracket of every
// limit- or stop-entered position, leaving only the amendments behind it.
export const BRACKET_ENTRY_LOOKBACK_MS = RESTING_ENTRY_MAX_AGE_MINUTES * 60 * 1000;

// Where a chart-side bracket-trail read has to start. A close reason is only
// claimed inside CLOSE_REASON_HISTORY_MS, so nothing older can change a verdict
// — which keeps the read at ~9 days even on the 6M chart range, where the raw
// window start would otherwise pull half a year of decisions on every analyze
// tick (this runs per symbol, per tick, so the bound is the point).
export function bracketTrailFromMs(windowStartMs: number, nowMs: number): number {
  return Math.max(0, Math.max(windowStartMs, nowMs - CLOSE_REASON_HISTORY_MS) - BRACKET_ENTRY_LOOKBACK_MS);
}
// With only one leg recoverable, the exit has to land ON it to be credited to
// it — otherwise the close is something we cannot read and the pnl sign
// decides. A stop fill can slip well past its trigger, so this is generous.
const LONE_LEG_TOLERANCE = 0.01;

const positive = (value: unknown): number | null => {
  const n = finiteNumber(value);
  return n !== null && n > 0 ? n : null;
};

const record = (value: unknown): Record<string, unknown> | null =>
  value && typeof value === 'object' ? (value as Record<string, unknown>) : null;

// Narrow any decision row (KV history or Neon) to the bracket evidence.
export function toBracketTrailRow(row: {
  timestamp?: unknown;
  decidedAtMs?: unknown;
  aiDecision?: Record<string, unknown> | null;
  execResult?: Record<string, unknown> | null;
}): BracketTrailRow {
  const exec = row.execResult ?? {};
  const ai = row.aiDecision ?? {};
  return {
    tsMs: Number(row.decidedAtMs ?? row.timestamp),
    action: typeof ai.action === 'string' ? ai.action : null,
    placed: exec.placed === true,
    takeProfitPrice: positive(ai.take_profit_price),
    stopLossPrice: positive(ai.stop_loss_price),
    tpsl: exec.tpsl,
    beStop: record(exec.management)?.beStop,
  };
}

// The exchange-side bracket actually resting when the position closed, replayed
// from its own ticks. Only levels the VENUE ACCEPTED count: a rejected amendment
// (Bitget 45122 "stop loss price please > mark price" and friends) leaves the
// previous level standing while the decision row still shows what was asked for.
//
// Within one tick the order is the one analyze executes and the prompt
// documents — breakeven stop first, then the model's own stop when it is
// tighter — so a tick carrying both ends on the tpsl leg.
export function resolveBracketAtExit(params: {
  trail: BracketTrailRow[] | null | undefined;
  entryTsMs?: number | null;
  exitTsMs: number;
}): BracketAtExit {
  const fromMs = params.entryTsMs ? params.entryTsMs - BRACKET_ENTRY_LOOKBACK_MS : Number.NEGATIVE_INFINITY;
  // Hard stop at the exit — no forward slack. Nothing after the close took part
  // in it (an amendment on a position that is already gone fails at the venue),
  // while the NEXT position's entry follows within minutes and would otherwise
  // overwrite the bracket that just fired: XRPUSDT 2026-09-05, a short stopped
  // out at its 1.4105 stop, re-labelled a take-profit by the BUY 2 minutes later.
  const toMs = params.exitTsMs;
  const rows = (params.trail || [])
    .filter((row) => Number.isFinite(row.tsMs) && row.tsMs >= fromMs && row.tsMs <= toMs)
    .sort((a, b) => a.tsMs - b.tsMs);
  let takeProfit: number | null = null;
  let stopLoss: number | null = null;
  for (const row of rows) {
    const action = String(row.action || '').toUpperCase();
    // An accepted entry REPLACES both legs: whatever the previous position left
    // behind is gone, and an entry always ships a bracket (a null TP gets the
    // ATR fallback), so this is an assignment, not a merge.
    if (row.placed === true && (action === 'BUY' || action === 'SELL' || action === 'REVERSE')) {
      takeProfit = row.takeProfitPrice ?? null;
      stopLoss = row.stopLossPrice ?? null;
    }
    const beStop = record(row.beStop);
    if (beStop?.ok === true) stopLoss = positive(beStop.triggerPrice) ?? stopLoss;
    const tpsl = record(row.tpsl);
    const slLeg = record(tpsl?.stopLoss);
    if (slLeg?.applied === true) stopLoss = positive(slLeg.requested) ?? stopLoss;
    const tpLeg = record(tpsl?.takeProfit);
    if (tpLeg?.applied === true) takeProfit = positive(tpLeg.requested) ?? takeProfit;
  }
  return { takeProfit, stopLoss };
}

// Which leg the exit price landed on. A bracket close IS one of the two legs,
// so when both are known the nearer one wins outright — no tolerance needed,
// and slippage past the trigger cannot flip the verdict. Null = undecidable;
// the caller falls back to the pnl sign.
export function classifyBracketExit(params: {
  bracket: BracketAtExit;
  exitPrice: number | null | undefined;
}): 'tp' | 'sl' | null {
  const exitPrice = positive(params.exitPrice);
  if (!exitPrice) return null;
  const { takeProfit, stopLoss } = params.bracket;
  const distTp = takeProfit ? Math.abs(exitPrice - takeProfit) / exitPrice : null;
  const distSl = stopLoss ? Math.abs(exitPrice - stopLoss) / exitPrice : null;
  if (distTp !== null && distSl !== null) {
    if (distSl < distTp) return 'sl';
    if (distTp < distSl) return 'tp';
    return null;
  }
  if (distSl !== null) return distSl <= LONE_LEG_TOLERANCE ? 'sl' : null;
  if (distTp !== null) return distTp <= LONE_LEG_TOLERANCE ? 'tp' : null;
  return null;
}

// What closed a position. An exchange-side TP/SL exit has no AI decision of its
// own, so a closed position with no CLOSE/REVERSE near its exit was closed by
// the resting bracket — and WHICH leg is decided by where the exit price landed
// against the bracket that was actually resting, replayed from the tick trail.
//
// The pnl sign is only the last-resort fallback (no trail, no exit price). It is
// not the answer: a stop trailed into profit closes a WINNER, and reading the
// sign alone labelled every one of those a take-profit — the trade then looks
// like it reached its target when it was actually stopped out, both on the chart
// and in the post-mortem the win evaluation runs off this.
export function classifyCloseCause(params: {
  history: ExitCandidateRow[] | null | undefined;
  bracketTrail?: BracketTrailRow[] | null;
  entryTsMs?: number | null;
  exitTsMs: number | null | undefined;
  exitPrice?: number | null;
  pnlValue: number | null | undefined;
  nowMs: number;
}): CloseCause {
  const unknown: CloseCause = { cause: 'unknown', basis: 'unknown', takeProfit: null, stopLoss: null };
  const { exitTsMs } = params;
  if (!exitTsMs) return unknown;
  if (exitTsMs < params.nowMs - CLOSE_REASON_HISTORY_MS) return unknown;
  const bracket = resolveBracketAtExit({
    trail: params.bracketTrail,
    entryTsMs: params.entryTsMs,
    exitTsMs,
  });
  if (hasAiExitNear(params.history, exitTsMs)) {
    return { cause: 'ai_close', basis: 'ai_decision', ...bracket };
  }
  const leg = classifyBracketExit({ bracket, exitPrice: params.exitPrice });
  if (leg) {
    return { cause: leg === 'sl' ? 'stop_loss' : 'take_profit', basis: 'bracket_level', ...bracket };
  }
  if (typeof params.pnlValue !== 'number') return { ...unknown, ...bracket };
  return {
    cause: params.pnlValue >= 0 ? 'take_profit' : 'stop_loss',
    basis: 'pnl_sign',
    ...bracket,
  };
}

// Chart-overlay view of the above: 'tp'/'sl' for a bracket hit, null for an
// AI close or an exit too old to reason about.
export function inferCloseReason(params: {
  history: ExitCandidateRow[] | null | undefined;
  bracketTrail?: BracketTrailRow[] | null;
  entryTsMs?: number | null;
  exitTsMs: number | null | undefined;
  exitPrice?: number | null;
  pnlValue: number | null | undefined;
  nowMs: number;
}): 'tp' | 'sl' | null {
  const { cause } = classifyCloseCause(params);
  if (cause === 'take_profit') return 'tp';
  if (cause === 'stop_loss') return 'sl';
  return null;
}
