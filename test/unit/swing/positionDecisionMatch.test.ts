// lib/swing/positionDecisionMatch.ts — which AI decision gets credited with a
// position's entry and exit. The rule that matters: only a CLOSE/REVERSE near
// the exit counts as the exit. A plain proximity match credited a long's exit
// to the BUY that placed the NEXT resting entry two minutes later (BNBUSDT,
// 2026-09-03: TP hit at 10:05, fresh BUY limit at 10:07).

import assert from 'node:assert/strict';
import { test } from 'vitest';

import {
    AI_CLOSE_MATCH_MS,
    findEntryDecision,
    findExitDecision,
    inferCloseReason,
} from '../../../lib/swing/positionDecisionMatch';

import type { DecisionHistoryEntry } from '../../../lib/history';

const MIN = 60_000;
const EXIT = Date.UTC(2026, 8, 3, 10, 5, 0);
const ENTRY = Date.UTC(2026, 8, 3, 3, 39, 0);
const NOW = Date.UTC(2026, 8, 3, 10, 30, 0);

const row = (tsMs: number, action: string, extra: Record<string, unknown> = {}) =>
    ({ timestamp: tsMs, dryRun: false, aiDecision: { action, ...extra } }) as unknown as DecisionHistoryEntry;

// The real shape of that BNBUSDT day: an entry, HOLDs, then a fresh BUY after
// the bracket took the position off.
const BNB_HISTORY = [
    row(ENTRY, 'BUY'),
    row(Date.UTC(2026, 8, 3, 6, 1, 0), 'HOLD'),
    row(Date.UTC(2026, 8, 3, 10, 2, 0), 'HOLD'),
    row(Date.UTC(2026, 8, 3, 10, 7, 0), 'BUY', { entry_limit_price: 694.6 }),
];

test('a BUY placing the next resting entry is not the exit decision', () => {
    assert.equal(findExitDecision(BNB_HISTORY, EXIT), null);
    // ...and the bracket inference then owns the exit, so the tooltip has
    // something true to show instead.
    assert.equal(inferCloseReason({ history: BNB_HISTORY, exitTsMs: EXIT, pnlValue: 6.24, nowMs: NOW }), 'tp');
});

test('the entry decision is still the nearest row of any action', () => {
    const entry = findEntryDecision(BNB_HISTORY, ENTRY);
    assert.equal(entry?.action, 'BUY');
    assert.equal(entry?.timestamp, ENTRY);
});

test('an AI CLOSE near the exit is credited, and suppresses the bracket guess', () => {
    const history = [...BNB_HISTORY, row(EXIT - 3 * MIN, 'CLOSE')];
    const exit = findExitDecision(history, EXIT);
    assert.equal(exit?.action, 'CLOSE');
    assert.equal(exit?.timestamp, EXIT - 3 * MIN);
    assert.equal(inferCloseReason({ history, exitTsMs: EXIT, pnlValue: 6.24, nowMs: NOW }), null);
});

test('a REVERSE counts as an exit; a CLOSE beyond the match window does not', () => {
    const reversed = [row(EXIT + 2 * MIN, 'REVERSE')];
    assert.equal(findExitDecision(reversed, EXIT)?.action, 'REVERSE');

    const tooFar = [row(EXIT - (AI_CLOSE_MATCH_MS + MIN), 'CLOSE')];
    assert.equal(findExitDecision(tooFar, EXIT), null);
    assert.equal(inferCloseReason({ history: tooFar, exitTsMs: EXIT, pnlValue: -2.1, nowMs: NOW }), 'sl');
});

test('a partial close carries its size into the label', () => {
    const history = [row(EXIT - MIN, 'CLOSE', { exit_size_pct: 30 })];
    assert.equal(findExitDecision(history, EXIT)?.closePct, 30);
    // A full close has no percentage to show.
    assert.equal(findExitDecision([row(EXIT - MIN, 'CLOSE', { exit_size_pct: 100 })], EXIT)?.closePct, null);
});

test('the bracket guess is only claimed inside the history window', () => {
    const old = NOW - 8 * 24 * 3600 * 1000;
    assert.equal(inferCloseReason({ history: [], exitTsMs: old, pnlValue: 4, nowMs: NOW }), null);
    // ...and never without a realized number to read the sign off.
    assert.equal(inferCloseReason({ history: [], exitTsMs: EXIT, pnlValue: null, nowMs: NOW }), null);
});
