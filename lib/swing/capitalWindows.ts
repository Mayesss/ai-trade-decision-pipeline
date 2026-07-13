// Capital.com closed-position window assembly, shared by the dashboard
// summary, the chart overlay endpoint and the analyze-tick reconcile.
//
// A Capital position produces up to TWO persisted rows in swing.positions:
// - a `capital:SYMBOL:...` row written at close time by the AI execution path
//   (side, entry/exit price, pnl_pct — but no venue cash figure), and
// - a `capital-tx:...` row reconstructed from Capital's transaction history
//   (cash pnl_net only — no exit price or percent).
// AI-initiated closes get both; venue-side bracket (TP/SL) closes get ONLY the
// transaction row, since no AI decision fired at close time. Every consumer
// must therefore (a) merge the pair into one window (mergeCapitalPositionWindows)
// and (b) derive the missing percent from net + notional (withDerivedPnlPct),
// or it renders duplicates and percent-less windows.
import type { PositionWindow } from '../analytics';
import { fetchCapitalTradeTransactions, type CapitalTradeTransactionRow } from '../capital';
import { loadDecisionHistory } from '../history';
import { upsertSwingPosition } from './pg';

// Number(null) === 0, so a bare Number() coercion would fabricate zeros out of
// explicit nulls (e.g. a tx window's entryPrice: null becoming a 0 price that
// poisons the size × price notional). Same guard as lib/swing/pg.ts `finite`.
function finiteNumber(value: unknown): number | null {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function positiveNumber(value: unknown): number | null {
    const n = finiteNumber(value);
    return n !== null && n > 0 ? n : null;
}

export function normalizeCapitalSymbolKey(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, '');
}

// Map one venue transaction row to a cash-only closed window. Returns null for
// anything that is not a processed, position-closing TRADE row.
export function capitalTransactionToWindow(row: CapitalTradeTransactionRow): PositionWindow | null {
    const ts = finiteNumber(row.dateUtcMs);
    if (ts === null || ts <= 0) return null;
    const status = String(row.status || '').trim().toUpperCase();
    if (status && status !== 'PROCESSED') return null;
    const type = String(row.transactionType || '').trim().toUpperCase();
    if (type && type !== 'TRADE') return null;
    const note = String(row.note || '').trim().toLowerCase();
    if (note && !note.includes('closed')) return null;
    const symbol = String(row.instrumentName || '').trim().toUpperCase();
    if (!symbol) return null;
    const pnlNet = finiteNumber(row.pnlNet);
    if (pnlNet === null) return null;
    const reference = String(row.reference || '').trim() || `${symbol}-${Math.floor(ts)}`;
    return {
        id: `capital-tx:${reference}:${Math.floor(ts)}`,
        symbol,
        side: null,
        entryTimestamp: null,
        exitTimestamp: ts,
        entryPrice: null,
        exitPrice: null,
        pnlNet,
        pnlGross: pnlNet,
        pnlPct: null,
        pnlGrossPct: null,
        notional: null,
        leverage: null,
    };
}

// Fill a cash-only transaction window with entry data (and/or a missing
// notional, needed for the derived percent) from the most recent placed
// BUY/SELL decision before its exit. Window-provided fields always win —
// history only fills the gaps. No-op when nothing is missing.
export function enrichCapitalWindowFromHistory(window: PositionWindow, history: any[]): PositionWindow {
    const needsEntry = !window.entryTimestamp;
    const needsNotional = positiveNumber(window.notional) === null;
    if (!needsEntry && !needsNotional) return window;
    const exitTs = Number(window.exitTimestamp);
    if (!Number.isFinite(exitTs) || exitTs <= 0) return window;
    const priorEntries = (history || [])
        .filter((entry) => {
            const ts = Number(entry?.timestamp);
            if (!Number.isFinite(ts) || ts <= 0 || ts > exitTs) return false;
            const action = String(entry?.aiDecision?.action || '').toUpperCase();
            const placed = entry?.execResult?.placed === true;
            return placed && (action === 'BUY' || action === 'SELL');
        })
        .sort((a, b) => Number(b.timestamp) - Number(a.timestamp));
    const entry = priorEntries[0];
    if (!entry) return window;
    const action = String(entry?.aiDecision?.action || '').toUpperCase();
    const entryPrice =
        finiteNumber(window.entryPrice) ??
        finiteNumber(entry?.snapshot?.positionContext?.entry_price) ??
        finiteNumber(entry?.snapshot?.price) ??
        null;
    // Capital exec results record deal size in units, not notional — fall back
    // to size × entry price. That's quote-currency notional against an
    // account-currency (EUR) pnl_net, the same approximation the direct
    // notionalUsd fields already carry; good enough for a display percent.
    const sizeUnits = positiveNumber(entry?.execResult?.size);
    const notional =
        positiveNumber(window.notional) ??
        positiveNumber(entry?.execResult?.notionalUsd) ??
        positiveNumber(entry?.execResult?.notionalUSDT) ??
        positiveNumber(entry?.execResult?.orderNotionalUsd) ??
        positiveNumber(entry?.snapshot?.gates?.notionalUSDT) ??
        positiveNumber(entry?.snapshot?.gates?.notionalUsd) ??
        (sizeUnits !== null && entryPrice !== null && entryPrice > 0 ? sizeUnits * entryPrice : null);
    return {
        ...window,
        entryTimestamp: window.entryTimestamp ?? Number(entry.timestamp),
        entryPrice,
        side: window.side ?? (action === 'BUY' ? 'long' : action === 'SELL' ? 'short' : null),
        leverage: window.leverage ?? finiteNumber(entry?.execResult?.leverage) ?? finiteNumber(entry?.aiDecision?.leverage),
        notional,
    };
}

// Merge the captured-at-close row and the transaction row for the same close
// into one window: same symbol, exits within 5 minutes. Field-level COALESCE —
// the earlier (already-merged) row wins, the later one fills its gaps.
export function mergeCapitalPositionWindows(windows: PositionWindow[]): PositionWindow[] {
    const sorted = windows
        .slice()
        .sort((a, b) => Number(a.exitTimestamp ?? a.entryTimestamp ?? 0) - Number(b.exitTimestamp ?? b.entryTimestamp ?? 0));
    const merged: PositionWindow[] = [];
    for (const window of sorted) {
        const ts = Number(window.exitTimestamp ?? window.entryTimestamp ?? 0);
        const match = merged.find((row) => {
            const rowTs = Number(row.exitTimestamp ?? row.entryTimestamp ?? 0);
            return (
                normalizeCapitalSymbolKey(row.symbol) === normalizeCapitalSymbolKey(window.symbol) &&
                Number.isFinite(rowTs) &&
                Number.isFinite(ts) &&
                Math.abs(rowTs - ts) <= 5 * 60 * 1000
            );
        });
        if (!match) {
            merged.push({ ...window });
            continue;
        }
        match.id = `${match.id}|${window.id}`;
        match.entryTimestamp = match.entryTimestamp ?? window.entryTimestamp ?? null;
        match.exitTimestamp = match.exitTimestamp ?? window.exitTimestamp ?? null;
        match.entryPrice = match.entryPrice ?? window.entryPrice ?? null;
        match.exitPrice = match.exitPrice ?? window.exitPrice ?? null;
        match.side = match.side ?? window.side ?? null;
        match.pnlNet = match.pnlNet ?? window.pnlNet ?? null;
        match.pnlGross = match.pnlGross ?? window.pnlGross ?? null;
        match.pnlPct = match.pnlPct ?? window.pnlPct ?? null;
        match.pnlGrossPct = match.pnlGrossPct ?? window.pnlGrossPct ?? null;
        match.notional = match.notional ?? window.notional ?? null;
        match.leverage = match.leverage ?? window.leverage ?? null;
    }
    return merged;
}

function derivePnlPctFromNetExposure(window: PositionWindow): number | null {
    const pnlNet = finiteNumber(window.pnlNet);
    const notional = positiveNumber(window.notional);
    if (pnlNet === null || notional === null) return null;
    const leverage = positiveNumber(window.leverage);
    const basis = leverage !== null ? notional / leverage : notional;
    return basis > 0 ? (pnlNet / basis) * 100 : null;
}

// Backfill pnl_pct/pnl_gross_pct from cash net + margin basis when the stored
// percent is missing or a 0.00 placeholder next to a non-zero cash figure.
export function withDerivedPnlPct(window: PositionWindow): PositionWindow {
    const derivedPct = derivePnlPctFromNetExposure(window);
    if (derivedPct === null) return window;
    const existingPct = finiteNumber(window.pnlPct);
    const existingGrossPct = finiteNumber(window.pnlGrossPct);
    const existingPctLooksPlaceholder =
        existingPct !== null &&
        Math.abs(existingPct) < 0.005 &&
        finiteNumber(window.pnlNet) !== null &&
        Math.abs(finiteNumber(window.pnlNet) as number) > 0.005;
    const existingGrossPctLooksPlaceholder =
        existingGrossPct !== null &&
        Math.abs(existingGrossPct) < 0.005 &&
        finiteNumber(window.pnlGross ?? window.pnlNet) !== null &&
        Math.abs(finiteNumber(window.pnlGross ?? window.pnlNet) as number) > 0.005;
    return {
        ...window,
        pnlPct: existingPct !== null && !existingPctLooksPlaceholder ? existingPct : derivedPct,
        pnlGrossPct: existingGrossPct !== null && !existingGrossPctLooksPlaceholder ? existingGrossPct : derivedPct,
    };
}

// Read-path assembly for one symbol's persisted windows: merge captured+tx row
// pairs, enrich transaction-only rows from decision history, derive missing
// percents. Pure — pass the already-loaded rows and history.
export function assembleCapitalPositionWindows(
    persisted: PositionWindow[],
    history: any[] = [],
): PositionWindow[] {
    const enriched = persisted.map((window) => enrichCapitalWindowFromHistory(window, history));
    return mergeCapitalPositionWindows(enriched).map(withDerivedPnlPct);
}

// Venue-bracket close reconcile: when an analyze tick detects a Capital
// position vanished without an AI CLOSE (TP/SL hit on the venue), pull the
// recent transaction history, enrich the matching close from decision history
// and persist it — so the close lands in Neon within one tick instead of
// waiting for a dashboard-summary load. Best-effort: never throws.
const RECONCILE_LOOKBACK_MS = 48 * 60 * 60 * 1000;

export async function reconcileCapitalClosedPositions(symbol: string): Promise<number> {
    try {
        const nowMs = Date.now();
        const transactions = await fetchCapitalTradeTransactions({
            fromTsMs: nowMs - RECONCILE_LOOKBACK_MS,
            toTsMs: nowMs,
        });
        const symbolKey = normalizeCapitalSymbolKey(symbol);
        const windows = transactions
            .map(capitalTransactionToWindow)
            .filter((row): row is PositionWindow => row !== null)
            .filter((row) => normalizeCapitalSymbolKey(row.symbol) === symbolKey);
        if (!windows.length) return 0;
        const history = await loadDecisionHistory(symbol, 60, 'capital').catch(() => [] as any[]);
        let persistedCount = 0;
        await Promise.all(
            windows
                .map((window) => enrichCapitalWindowFromHistory(window, history))
                .map((window) =>
                    upsertSwingPosition('capital', {
                        ...window,
                        status: 'closed',
                        leverageSource: window.leverage ? 'captured' : null,
                    })
                        .then(() => {
                            persistedCount += 1;
                        })
                        .catch((err) => {
                            console.warn(`Could not persist reconciled Capital close ${window.id}:`, err);
                        }),
                ),
        );
        return persistedCount;
    } catch (err) {
        console.warn(`Capital close reconcile failed for ${symbol}:`, err);
        return 0;
    }
}
