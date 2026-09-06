// lib/swing/positionDecisionMatch.ts — which AI decision gets credited with a
// position's entry and exit. The rule that matters: only a CLOSE/REVERSE near
// the exit counts as the exit. A plain proximity match credited a long's exit
// to the BUY that placed the NEXT resting entry two minutes later (BNBUSDT,
// 2026-09-03: TP hit at 10:05, fresh BUY limit at 10:07).

import assert from 'node:assert/strict';
import { test } from 'vitest';

import {
    AI_CLOSE_MATCH_MS,
    BRACKET_ENTRY_LOOKBACK_MS,
    CLOSE_REASON_HISTORY_MS,
    bracketTrailFromMs,
    classifyCloseCause,
    findEntryDecision,
    findExitDecision,
    inferCloseReason,
    resolveBracketAtExit,
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

// --------------------------------------------------------------------------
// Which bracket leg closed the position
//
// The case that forced this: XRPUSDT, 2026-09-06. A long entered at 1.404 with
// TP 1.436 / SL 1.378; the AI trailed the stop 1.3864 → 1.3914 → 1.4095 as
// price rose, and the stop fired at 1.4087 for +1.71%. Reading the pnl sign
// called that a take-profit — on the chart AND in the win evaluation, which
// then judged the exit as a target that was reached.
// --------------------------------------------------------------------------

const XRP_ENTRY = Date.UTC(2026, 8, 5, 9, 34, 13);
const XRP_EXIT = Date.UTC(2026, 8, 6, 7, 13, 20);
const XRP_NOW = Date.UTC(2026, 8, 6, 8, 0, 0);
// The BUY rested as a limit and filled 19 minutes later, so it sits BEFORE the
// position's own entry timestamp.
const XRP_BUY_TS = Date.UTC(2026, 8, 5, 9, 15, 5);

const entryTick = (tsMs: number, tp: number | null, sl: number | null) => ({
    tsMs,
    action: 'BUY',
    placed: true,
    takeProfitPrice: tp,
    stopLossPrice: sl,
    tpsl: null,
    beStop: null,
});
const stopAmend = (tsMs: number, requested: number, applied = true) => ({
    tsMs,
    action: 'HOLD',
    placed: false,
    takeProfitPrice: null,
    stopLossPrice: requested,
    tpsl: { stopLoss: { mode: 'modify', applied, requested } },
    beStop: null,
});

const XRP_TRAIL = [
    entryTick(XRP_BUY_TS, 1.436, 1.378),
    stopAmend(Date.UTC(2026, 8, 5, 16, 1, 9), 1.3864),
    stopAmend(Date.UTC(2026, 8, 5, 20, 1, 4), 1.3914),
    {
        ...stopAmend(Date.UTC(2026, 8, 6, 3, 40, 43), 1.4095),
        // Same tick raised leverage behind a breakeven stop; analyze applies the
        // BE floor first and the model's tighter stop after it.
        beStop: { ok: true, mode: 'modify', triggerPrice: 1.4051 },
    },
];

const xrpCause = (over: Record<string, unknown> = {}) =>
    classifyCloseCause({
        history: [],
        bracketTrail: XRP_TRAIL,
        entryTsMs: XRP_ENTRY,
        exitTsMs: XRP_EXIT,
        exitPrice: 1.4087,
        pnlValue: 1.708,
        nowMs: XRP_NOW,
        ...over,
    });

test('a stop trailed into profit closes the position at the STOP, not the target', () => {
    const cause = xrpCause();
    assert.equal(cause.cause, 'stop_loss');
    assert.equal(cause.basis, 'bracket_level');
    assert.equal(cause.stopLoss, 1.4095, 'the last accepted amendment, not the BE floor under it');
    assert.equal(cause.takeProfit, 1.436, 'entry bracket survives from before the fill');
    assert.equal(
        inferCloseReason({
            history: [],
            bracketTrail: XRP_TRAIL,
            entryTsMs: XRP_ENTRY,
            exitTsMs: XRP_EXIT,
            exitPrice: 1.4087,
            pnlValue: 1.708,
            nowMs: XRP_NOW,
        }),
        'sl',
        'and the chart overlay reads the same verdict',
    );
});

test('the same trade filling its target still reads take-profit', () => {
    const cause = xrpCause({ exitPrice: 1.4361, pnlValue: 15.9 });
    assert.equal(cause.cause, 'take_profit');
    assert.equal(cause.basis, 'bracket_level');
});

test('an amendment the venue rejected leaves the previous level standing', () => {
    const trail = [
        entryTick(XRP_BUY_TS, 1.436, 1.378),
        stopAmend(Date.UTC(2026, 8, 5, 16, 1, 9), 1.3864),
        // Bitget 45122: the tighten never reached the book.
        stopAmend(Date.UTC(2026, 8, 5, 20, 1, 4), 1.4102, false),
    ];
    const bracket = resolveBracketAtExit({ trail, entryTsMs: XRP_ENTRY, exitTsMs: XRP_EXIT });
    assert.equal(bracket.stopLoss, 1.3864);
});

test('an entry replaces the previous position bracket rather than merging with it', () => {
    const trail = [
        entryTick(Date.UTC(2026, 8, 4, 9, 0, 0), 1.51, 1.47),
        stopAmend(Date.UTC(2026, 8, 4, 12, 0, 0), 1.49),
        entryTick(XRP_BUY_TS, 1.436, 1.378),
    ];
    assert.deepEqual(resolveBracketAtExit({ trail, entryTsMs: XRP_ENTRY, exitTsMs: XRP_EXIT }), {
        takeProfit: 1.436,
        stopLoss: 1.378,
    });
});

test('an entry older than the resting backstop is not this position bracket', () => {
    const trail = [entryTick(XRP_ENTRY - BRACKET_ENTRY_LOOKBACK_MS - MIN, 1.436, 1.378)];
    assert.deepEqual(resolveBracketAtExit({ trail, entryTsMs: XRP_ENTRY, exitTsMs: XRP_EXIT }), {
        takeProfit: null,
        stopLoss: null,
    });
});

test('with no trail to replay, the pnl sign is the fallback and says so', () => {
    const cause = xrpCause({ bracketTrail: [] });
    assert.equal(cause.cause, 'take_profit');
    assert.equal(cause.basis, 'pnl_sign');
});

test('an exit price nowhere near the only known leg decides nothing', () => {
    const cause = classifyCloseCause({
        history: [],
        bracketTrail: [stopAmend(XRP_ENTRY + MIN, 1.3914)],
        entryTsMs: XRP_ENTRY,
        exitTsMs: XRP_EXIT,
        exitPrice: 1.4087,
        pnlValue: 1.708,
        nowMs: XRP_NOW,
    });
    assert.equal(cause.basis, 'pnl_sign', '1.4087 is 1.2% off the stop — not a fill of it');
});

test('an AI close still wins over the bracket, on Neon-shaped rows too', () => {
    const cause = classifyCloseCause({
        // The post-mortem passes swing.decisions rows: decidedAtMs, not timestamp.
        history: [{ decidedAtMs: XRP_EXIT - 2 * MIN, aiDecision: { action: 'CLOSE' } }],
        bracketTrail: XRP_TRAIL,
        entryTsMs: XRP_ENTRY,
        exitTsMs: XRP_EXIT,
        exitPrice: 1.4087,
        pnlValue: 1.708,
        nowMs: XRP_NOW,
    });
    assert.equal(cause.cause, 'ai_close');
    assert.equal(cause.stopLoss, 1.4095, 'the resting bracket is still reported');
});

test('the NEXT position entry, minutes after the exit, cannot claim this bracket', () => {
    const trail = [
        // The short: stop at 1.4105, which is where it got taken out.
        { ...entryTick(Date.UTC(2026, 8, 4, 16, 1, 1), 1.381, 1.4105), action: 'SELL' },
        // The long that replaced it, placed 2 minutes after the stop filled.
        entryTick(Date.UTC(2026, 8, 5, 9, 15, 5), 1.436, 1.378),
    ];
    const cause = classifyCloseCause({
        history: [],
        bracketTrail: trail,
        entryTsMs: Date.UTC(2026, 8, 4, 16, 1, 1),
        exitTsMs: Date.UTC(2026, 8, 5, 9, 13, 18),
        exitPrice: 1.4105,
        pnlValue: -6.96,
        nowMs: XRP_NOW,
    });
    assert.equal(cause.cause, 'stop_loss');
    assert.equal(cause.stopLoss, 1.4105);
});

test('the trail read is bounded by how far back a close reason is claimable', () => {
    const now = XRP_NOW;
    const sixMonthsBack = now - 183 * 24 * 3600 * 1000;
    assert.equal(
        bracketTrailFromMs(sixMonthsBack, now),
        now - CLOSE_REASON_HISTORY_MS - BRACKET_ENTRY_LOOKBACK_MS,
        'the 6M chart range must not pull half a year of decisions per tick',
    );
    // A window inside the claimable span reads only its own span.
    const yesterday = now - 24 * 3600 * 1000;
    assert.equal(bracketTrailFromMs(yesterday, now), yesterday - BRACKET_ENTRY_LOOKBACK_MS);
});
