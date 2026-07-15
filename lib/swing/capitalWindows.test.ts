import assert from 'node:assert/strict';
import test from 'node:test';

import type { PositionWindow } from '../analytics';
import {
    assembleCapitalPositionWindows,
    attachTrimChunkPnl,
    enrichCapitalWindowFromHistory,
    foldCapitalTrimChunks,
    mergeCapitalPositionWindows,
    withDerivedPnlPct,
} from './capitalWindows';

const T0 = Date.parse('2026-07-13T18:30:00.000Z'); // entry
const T1 = Date.parse('2026-07-13T21:09:00.000Z'); // close

// AI-close pair: the captured-at-close row (prices + pct, no cash) and the
// transaction row (cash only), exits ~seconds apart.
const capturedRow: PositionWindow = {
    id: 'capital:TLT:entry:exit:deal',
    symbol: 'TLT',
    side: 'short',
    entryTimestamp: T0,
    exitTimestamp: T1,
    entryPrice: 83.965,
    exitPrice: 84.1,
    pnlNet: null,
    pnlGross: null,
    pnlPct: -3.2,
    pnlGrossPct: -3.2,
    notional: null,
    leverage: 5,
};
const txRow: PositionWindow = {
    id: 'capital-tx:133203731116164:1783976987978',
    symbol: 'TLT',
    side: null,
    entryTimestamp: null,
    exitTimestamp: T1 + 3_000,
    entryPrice: null,
    exitPrice: null,
    pnlNet: -1.04,
    pnlGross: -1.04,
    pnlPct: null,
    pnlGrossPct: null,
    notional: null,
    leverage: null,
};

test('mergeCapitalPositionWindows: captured + tx rows for the same close collapse into one window', () => {
    const merged = mergeCapitalPositionWindows([capturedRow, txRow]);
    assert.equal(merged.length, 1);
    const [w] = merged;
    assert.equal(w.side, 'short');
    assert.equal(w.pnlPct, -3.2); // from captured row
    assert.equal(w.pnlNet, -1.04); // from tx row
    assert.equal(w.entryPrice, 83.965);
});

test('mergeCapitalPositionWindows: different symbols or >5min apart stay separate', () => {
    const otherSymbol = { ...txRow, id: 'capital-tx:x', symbol: 'COPPER' };
    const laterClose = { ...txRow, id: 'capital-tx:y', exitTimestamp: T1 + 6 * 60 * 1000 };
    assert.equal(mergeCapitalPositionWindows([capturedRow, otherSymbol]).length, 2);
    assert.equal(mergeCapitalPositionWindows([capturedRow, laterClose]).length, 2);
});

test('withDerivedPnlPct: derives percent from cash net over margin basis; keeps a real stored percent', () => {
    const bracketClose: PositionWindow = {
        ...txRow,
        notional: 50, // €50 exposure at 5x → €10 margin
        leverage: 5,
    };
    const derived = withDerivedPnlPct(bracketClose);
    assert.ok(Math.abs((derived.pnlPct as number) - -10.4) < 1e-9); // -1.04 / 10 * 100

    const untouched = withDerivedPnlPct({ ...bracketClose, pnlPct: -7.5 });
    assert.equal(untouched.pnlPct, -7.5);
});

const sellEntryHistory = [
    {
        timestamp: T0,
        aiDecision: { action: 'SELL', leverage: null },
        execResult: { placed: true, notionalUsd: 50, leverage: 5 },
        snapshot: { positionContext: { entry_price: 83.965 }, price: 83.97 },
    },
    // Later HOLD must not be picked as the entry.
    { timestamp: T0 + 60 * 60 * 1000, aiDecision: { action: 'HOLD' }, execResult: { placed: false }, snapshot: {} },
];

test('enrichCapitalWindowFromHistory: fills side/entry/notional from the last placed BUY/SELL before exit', () => {
    const enriched = enrichCapitalWindowFromHistory(txRow, sellEntryHistory);
    assert.equal(enriched.side, 'short');
    assert.equal(enriched.entryTimestamp, T0);
    assert.equal(enriched.entryPrice, 83.965);
    assert.equal(enriched.notional, 50);
    assert.equal(enriched.leverage, 5);
});

test('enrichCapitalWindowFromHistory: derives notional from exec size × entry price when no notional field exists', () => {
    // Real Capital exec results carry size (units) + leverage, no notional.
    const history = [
        {
            timestamp: T0,
            aiDecision: { action: 'SELL' },
            execResult: { placed: true, size: 5.9, leverage: 5 },
            snapshot: { positionContext: { entry_price: 83.965 }, price: 83.97 },
        },
    ];
    const enriched = withDerivedPnlPct(enrichCapitalWindowFromHistory(txRow, history));
    assert.ok(Math.abs((enriched.notional as number) - 5.9 * 83.965) < 1e-9);
    assert.equal(enriched.leverage, 5);
    // pnlNet -1.04 over margin (5.9 × 83.965 / 5 ≈ 99.08) ≈ -1.05%
    assert.ok(Math.abs((enriched.pnlPct as number) - -1.04 / ((5.9 * 83.965) / 5) * 100) < 1e-9);
});

test('assembleCapitalPositionWindows: end-to-end — dedupes the AI-close pair and repairs a bracket-only close', () => {
    // Bracket close 3h earlier: only a tx row exists (no AI CLOSE fired).
    const bracketTx: PositionWindow = {
        ...txRow,
        id: 'capital-tx:bracket',
        exitTimestamp: T1 - 3 * 60 * 60 * 1000,
        pnlNet: -0.63,
        pnlGross: -0.63,
    };
    const bracketEntryHistory = [
        {
            timestamp: T0 - 4 * 60 * 60 * 1000,
            aiDecision: { action: 'SELL' },
            execResult: { placed: true, notionalUsd: 50, leverage: 5 },
            snapshot: { positionContext: { entry_price: 84.4 }, price: 84.41 },
        },
        ...sellEntryHistory,
    ];
    const out = assembleCapitalPositionWindows([capturedRow, txRow, bracketTx], bracketEntryHistory);
    assert.equal(out.length, 2);
    const bracket = out.find((w) => String(w.id).includes('bracket'));
    assert.ok(bracket, 'bracket-only close survives as its own window');
    assert.equal(bracket!.side, 'short');
    assert.ok(Number.isFinite(bracket!.pnlPct as number), 'bracket close gets a derived percent');
    const pair = out.find((w) => !String(w.id).includes('bracket'));
    assert.equal(pair!.pnlPct, -3.2);
    assert.equal(pair!.pnlNet, -1.04);
});

// --- foldCapitalTrimChunks ------------------------------------------------

const TRIM_T = T0 + 60 * 60 * 1000; // trim one hour after entry

// A trimmed position: the trim's chunk (entry → trim) and the remainder's
// final close (entry → T1), both enriched with the same entry + full notional.
const trimChunk: PositionWindow = {
    id: 'capital-tx:trim',
    symbol: 'TLT',
    side: 'short',
    entryTimestamp: T0,
    exitTimestamp: TRIM_T,
    entryPrice: 83.965,
    exitPrice: null,
    pnlNet: 2.3,
    pnlGross: 2.3,
    pnlPct: 2.3,
    pnlGrossPct: 2.3,
    notional: 50,
    leverage: 5,
};
const finalChunk: PositionWindow = {
    id: 'capital:TLT:final',
    symbol: 'TLT',
    side: 'short',
    entryTimestamp: T0 + 30_000, // venue-execution skew vs the decision ts
    exitTimestamp: T1,
    entryPrice: 83.965,
    exitPrice: 84.1,
    pnlNet: 1.0,
    pnlGross: 1.0,
    pnlPct: 1.0,
    pnlGrossPct: 1.0,
    notional: 50,
    leverage: 5,
};

test('foldCapitalTrimChunks: trim + final close fold into ONE window with summed cash/percent', () => {
    const { windows, openChunks } = foldCapitalTrimChunks([trimChunk, finalChunk]);
    assert.equal(windows.length, 1);
    assert.equal(openChunks.length, 0);
    const [w] = windows;
    assert.equal(w.entryTimestamp, T0); // earliest entry frames the box
    assert.equal(w.exitTimestamp, T1); // final close frames the exit
    assert.ok(Math.abs((w.pnlNet as number) - 3.3) < 1e-9);
    assert.ok(Math.abs((w.pnlPct as number) - 3.3) < 1e-9); // chunk pcts share the full-notional basis
    assert.equal(w.notional, 50); // max, not sum — same exposure, not double
    assert.equal(w.chunks?.length, 2);
    assert.equal(w.chunks?.[0].exitTimestamp, TRIM_T);
});

test('foldCapitalTrimChunks: chunks of the STILL-OPEN position are peeled off, not drawn as closed boxes', () => {
    const { windows, openChunks } = foldCapitalTrimChunks([trimChunk], { openEntryTimestampMs: T0 + 45_000 });
    assert.equal(windows.length, 0);
    assert.equal(openChunks.length, 1);
    assert.equal(openChunks[0].pnlNet, 2.3);
});

test('foldCapitalTrimChunks: sequential positions and entry-less rows stay separate', () => {
    // A REAL later position starts after the previous one closed (one position
    // per symbol); back-to-back close/re-entry may overlap by venue skew only.
    const laterPosition = {
        ...finalChunk,
        id: 'capital:TLT:later',
        entryTimestamp: T1 + 30_000, // re-entered seconds after the close booked
        exitTimestamp: T1 + 4 * 60 * 60 * 1000,
    };
    const orphanTx = { ...txRow, id: 'capital-tx:orphan', exitTimestamp: T1 + 8 * 60 * 60 * 1000, entryTimestamp: null }; // no entry — unattributable
    const { windows } = foldCapitalTrimChunks([trimChunk, finalChunk, laterPosition, orphanTx]);
    assert.equal(windows.length, 3); // folded position + later position + orphan
    const folded = windows.find((w) => w.chunks?.length);
    assert.equal(folded?.chunks?.length, 2);
    assert.equal(windows.filter((w) => !(w.chunks && w.chunks.length)).length, 2);
});

test('foldCapitalTrimChunks: pullback-limit skew — decision-ts chunk and fill-ts chunk still fold (span overlap)', () => {
    // Limit placed at T0 (decision ts on the enriched trim row), filled 2h
    // later (venue ts on the captured close row). Entries are 2h apart but the
    // spans coexist — same position.
    const fillTs = T0 + 2 * 60 * 60 * 1000;
    const trimAfterFill = { ...trimChunk, exitTimestamp: fillTs + 60 * 60 * 1000 };
    const finalFromFill = { ...finalChunk, entryTimestamp: fillTs, exitTimestamp: fillTs + 6 * 60 * 60 * 1000 };
    const { windows } = foldCapitalTrimChunks([trimAfterFill, finalFromFill]);
    assert.equal(windows.length, 1);
    assert.equal(windows[0].chunks?.length, 2);
    assert.ok(Math.abs((windows[0].pnlNet as number) - 3.3) < 1e-9);
});

test('foldCapitalTrimChunks: open-position peel-off works across the limit-fill skew too', () => {
    // Open position filled 2h after the decision; the trim (enriched with the
    // decision ts) realized while it was open → peel, not a closed box.
    const fillTs = T0 + 2 * 60 * 60 * 1000;
    const trimAfterFill = { ...trimChunk, exitTimestamp: fillTs + 60 * 60 * 1000 };
    const { windows, openChunks } = foldCapitalTrimChunks([trimAfterFill], { openEntryTimestampMs: fillTs });
    assert.equal(windows.length, 0);
    assert.equal(openChunks.length, 1);
    // …while a PREVIOUS position's close (booked before the fill) stays a closed window.
    const previousClose = { ...finalChunk, id: 'capital:TLT:prev', exitTimestamp: fillTs - 60_000 };
    const res2 = foldCapitalTrimChunks([previousClose], { openEntryTimestampMs: fillTs });
    assert.equal(res2.windows.length, 1);
    assert.equal(res2.openChunks.length, 0);
});

test('attachTrimChunkPnl: partial-close briefs pick up the matching chunk cash by timestamp', () => {
    const partials = [
        { timestamp: TRIM_T - 2 * 60 * 1000, closePct: 30 }, // AI decision ~2min before the venue fill
        { timestamp: T0 + 5 * 60 * 60 * 1000, closePct: 20 }, // no chunk anywhere near
    ];
    const out = attachTrimChunkPnl(partials, [{ exitTimestamp: TRIM_T, pnlNet: 2.3, pnlPct: 2.3 }]);
    assert.equal((out[0] as any).pnlNet, 2.3);
    assert.equal((out[1] as any).pnlNet, undefined);
});
