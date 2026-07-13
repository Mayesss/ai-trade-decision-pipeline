import assert from 'node:assert/strict';
import test from 'node:test';

import type { PositionWindow } from '../analytics';
import {
    assembleCapitalPositionWindows,
    enrichCapitalWindowFromHistory,
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
