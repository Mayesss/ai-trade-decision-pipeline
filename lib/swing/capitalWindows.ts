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
import { maybeEnqueueSwingPostmortem } from './postmortem';

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

// One realized cash chunk of a position (a trim or the final close), kept on
// the folded window so consumers can place per-day cash or per-trim markers.
export type CapitalWindowChunk = {
    exitTimestamp: number;
    pnlNet: number | null;
    pnlPct: number | null;
};

export type FoldedCapitalWindow = PositionWindow & { chunks?: CapitalWindowChunk[] };

// Two windows belong to the same position when their (enriched) entries agree
// within this tolerance…
const SAME_POSITION_ENTRY_TOLERANCE_MS = 5 * 60 * 1000;
// …OR when their spans overlap by more than this. Capital holds ONE position
// per symbol at a time, so same-symbol windows that genuinely coexist can only
// be realized chunks of the same position — sequential positions overlap by at
// most seconds of venue skew (a close and a REVERSE re-entry book back to
// back). The overlap test is what catches pullback-limit entries: a trim tx
// row is enriched with the BUY/SELL DECISION timestamp while captured rows and
// the open position carry the venue FILL time, which can trail the decision by
// hours — same position, entries far apart, spans overlapping.
const SAME_POSITION_MIN_OVERLAP_MS = 5 * 60 * 1000;

function spanOverlapMs(aEntry: number, aExit: number, bEntry: number, bExit: number): number {
    return Math.min(aExit, bExit) - Math.max(aEntry, bEntry);
}

function toChunk(window: PositionWindow): CapitalWindowChunk {
    return {
        exitTimestamp: Number(window.exitTimestamp),
        pnlNet: finiteNumber(window.pnlNet),
        pnlPct: finiteNumber(window.pnlPct),
    };
}

// Sum of the non-null values; null when every value is null (so a group of
// percent-less rows doesn't fabricate a 0).
function sumOrNull(values: Array<number | null>): number | null {
    const present = values.filter((v): v is number => v !== null);
    return present.length ? present.reduce((a, b) => a + b, 0) : null;
}

function firstNonNull<T>(values: Array<T | null | undefined>): T | null {
    for (const value of values) if (value !== null && value !== undefined) return value;
    return null;
}

function foldChunkGroup(members: PositionWindow[]): FoldedCapitalWindow {
    if (members.length === 1) return members[0];
    const final = members[members.length - 1];
    // Every chunk was enriched with the FULL position notional (from the entry
    // decision), so chunk percents already share one basis: the position's
    // percent is the SUM of chunk percents, and the folded notional is the max
    // (summing would double-count the same exposure).
    const notionals = members.map((m) => positiveNumber(m.notional)).filter((n): n is number => n !== null);
    const folded: FoldedCapitalWindow = {
        ...final,
        entryTimestamp: Math.min(...members.map((m) => Number(m.entryTimestamp))),
        entryPrice: firstNonNull(members.map((m) => finiteNumber(m.entryPrice))),
        side: firstNonNull(members.map((m) => m.side ?? null)),
        leverage: firstNonNull(members.map((m) => positiveNumber(m.leverage))),
        pnlNet: sumOrNull(members.map((m) => finiteNumber(m.pnlNet))),
        pnlGross: sumOrNull(members.map((m) => finiteNumber(m.pnlGross))),
        pnlPct: sumOrNull(members.map((m) => finiteNumber(m.pnlPct))),
        pnlGrossPct: sumOrNull(members.map((m) => finiteNumber(m.pnlGrossPct))),
        notional: notionals.length ? Math.max(...notionals) : null,
        chunks: members.map(toChunk),
    };
    // Backstop for groups where no chunk carried a percent: derive one from the
    // summed cash over the shared margin basis.
    return withDerivedPnlPct(folded);
}

// A Capital trim (partial close) realizes cash, so it lands as its OWN closed
// window (entry → trim time) next to the remainder's window (entry → final
// exit). That is correct for cash accounting — every chunk is real money — but
// consumed naively it draws one position as overlapping boxes and counts it as
// several trades. Fold same-position chunks (same symbol, entries within
// tolerance OR spans overlapping — see the constants above) into ONE window:
// the last-exiting chunk frames it, cash sums,
// percents sum over their shared full-notional basis, and the constituent
// chunks stay on `chunks` (exit-sorted). Windows without entry AND exit
// timestamps can't be attributed and pass through untouched.
// `openEntryTimestampMs`: chunks belonging to the STILL-OPEN position are
// excluded from the folded output and returned as `openChunks` — the chart
// draws that position from the live broker overlay instead of a phantom
// closed box underneath it.
export function foldCapitalTrimChunks(
    windows: PositionWindow[],
    opts?: { openEntryTimestampMs?: number | null },
): { windows: FoldedCapitalWindow[]; openChunks: CapitalWindowChunk[] } {
    const openEntry = finiteNumber(opts?.openEntryTimestampMs);
    const byExit = (a: PositionWindow, b: PositionWindow) =>
        Number(a.exitTimestamp ?? a.entryTimestamp ?? 0) - Number(b.exitTimestamp ?? b.entryTimestamp ?? 0);
    const passthrough: FoldedCapitalWindow[] = [];
    const openChunks: CapitalWindowChunk[] = [];
    const groups: Array<{ minEntry: number; maxExit: number; symbolKey: string; members: PositionWindow[] }> = [];
    for (const window of windows.slice().sort(byExit)) {
        const entry = finiteNumber(window.entryTimestamp);
        const exit = finiteNumber(window.exitTimestamp);
        if (entry === null || exit === null) {
            passthrough.push(window);
            continue;
        }
        // Realized while the current position is open (exit meaningfully after
        // its fill) or sharing its entry ⇒ a trim of the OPEN position. A
        // previous position always closed before the current one filled, so
        // its exits can't trail openEntry by more than venue skew.
        if (
            openEntry !== null &&
            (Math.abs(entry - openEntry) <= SAME_POSITION_ENTRY_TOLERANCE_MS ||
                exit - openEntry >= SAME_POSITION_MIN_OVERLAP_MS)
        ) {
            openChunks.push(toChunk(window));
            continue;
        }
        const symbolKey = normalizeCapitalSymbolKey(window.symbol);
        const group = groups.find(
            (g) =>
                g.symbolKey === symbolKey &&
                (Math.abs(g.minEntry - entry) <= SAME_POSITION_ENTRY_TOLERANCE_MS ||
                    spanOverlapMs(entry, exit, g.minEntry, g.maxExit) >= SAME_POSITION_MIN_OVERLAP_MS),
        );
        if (group) {
            group.members.push(window);
            group.minEntry = Math.min(group.minEntry, entry);
            group.maxExit = Math.max(group.maxExit, exit);
        } else {
            groups.push({ minEntry: entry, maxExit: exit, symbolKey, members: [window] });
        }
    }
    return {
        windows: [...passthrough, ...groups.map((g) => foldChunkGroup(g.members))].sort(byExit),
        openChunks,
    };
}

// Attach the realized cash of each trim chunk to its partial-close AI-decision
// brief (matched by time: decision fires within minutes of the venue fill) so
// the chart tooltip can show what the trim actually banked.
const TRIM_CHUNK_MATCH_MS = 30 * 60 * 1000;

export function attachTrimChunkPnl<T extends { timestamp?: number | null }>(
    partials: T[],
    chunks: CapitalWindowChunk[] | null | undefined,
): Array<T & { pnlNet?: number | null }> {
    if (!partials.length || !chunks?.length) return partials;
    return partials.map((partial) => {
        const ts = finiteNumber(partial.timestamp);
        if (ts === null) return partial;
        let best: CapitalWindowChunk | null = null;
        let bestDiff = TRIM_CHUNK_MATCH_MS;
        for (const chunk of chunks) {
            const diff = Math.abs(chunk.exitTimestamp - ts);
            if (diff <= bestDiff) {
                bestDiff = diff;
                best = chunk;
            }
        }
        return best && best.pnlNet !== null ? { ...partial, pnlNet: best.pnlNet } : partial;
    });
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
                        .then(async () => {
                            persistedCount += 1;
                            // 48h-lookback reconcile revisits the same closes on
                            // every venue-close detection — the enqueue's unique
                            // key makes the repeats free no-ops.
                            await maybeEnqueueSwingPostmortem('capital', window);
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
