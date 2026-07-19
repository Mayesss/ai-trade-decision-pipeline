// Recent-actions enrichment for the swing prompt: the raw decision history
// rows say what the model ASKED for (BUY/SELL/CLOSE), not what happened. A
// resting pullback limit that never filled still reads "SELL", and the same
// limit re-issued five consecutive ticks reads as five SELLs — the model
// believes it recently traded when it didn't. These helpers collapse re-issue
// chains and attach measured outcomes (never_filled / still_open / closed pnl)
// from swing.positions. The raw `action` string is left untouched: the
// anti-flip guard in postprocessDecision matches on it.

export type RecentActionOutcome =
    | 'never_filled'
    | 'still_open'
    | { closedPnlPctOnMargin: number | null; heldMin: number | null };

export type RecentActionEntry = {
    action: string;
    timestamp: number;
    closePct?: number | null;
    // Pullback limit the entry rested at (null/absent = market entry).
    entryLimitPrice?: number | null;
    // >1 when consecutive identical limit re-issues were collapsed into this row.
    reissueCount?: number;
    // Timestamp of the first re-issue in a collapsed chain.
    firstTimestamp?: number;
    outcome?: RecentActionOutcome | null;
};

export type PositionForOutcome = {
    side?: string | null;
    entryTimestamp?: number | null;
    exitTimestamp?: number | null;
    pnlPct?: number | null;
};

const ENTRY_SIDE: Record<string, string> = { BUY: 'long', SELL: 'short' };
// A resting limit is cancelled/superseded at the next evaluation; when no later
// action exists, consider the fill window closed after this long.
const FILL_WINDOW_FALLBACK_MS = 90 * 60_000;
// Slack around timestamps: order placement/fill logging is seconds-to-minutes
// off the decision persist time.
const MATCH_SLACK_MS = 2 * 60_000;
const CLOSE_MATCH_MS = 10 * 60_000;

const num = (v: unknown): number | null => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
};

// Merge consecutive identical limit re-issues (same action, same limit price,
// adjacent in the ascending list) into one row carrying the LAST timestamp and
// the chain length — five re-issued SELLs otherwise eat the whole prompt
// window and read as five separate trades. Expects (and returns) oldest-first.
export function collapseLimitReissues(actions: RecentActionEntry[]): RecentActionEntry[] {
    const out: RecentActionEntry[] = [];
    for (const a of actions) {
        const prev = out[out.length - 1];
        const isReissue =
            prev &&
            (a.action === 'BUY' || a.action === 'SELL') &&
            prev.action === a.action &&
            prev.entryLimitPrice != null &&
            a.entryLimitPrice != null &&
            prev.entryLimitPrice === a.entryLimitPrice;
        if (isReissue) {
            prev.reissueCount = (prev.reissueCount ?? 1) + 1;
            prev.firstTimestamp = Math.min(prev.firstTimestamp ?? prev.timestamp, a.timestamp);
            prev.timestamp = Math.max(prev.timestamp, a.timestamp);
        } else {
            out.push({ ...a });
        }
    }
    return out;
}

// Attach measured outcomes by joining actions to positions. Entry actions
// match a position whose entryTimestamp falls inside the action's live window
// (from the first (re-)issue until the next action superseded it); CLOSE
// actions match a position exiting near the action time. Expects oldest-first.
export function attachRecentActionOutcomes(
    actions: RecentActionEntry[],
    opts: {
        positions: PositionForOutcome[];
        openPosition?: { side?: string | null; entryTimestamp?: number | null } | null;
        nowMs: number;
    },
): RecentActionEntry[] {
    const { positions, openPosition, nowMs } = opts;
    return actions.map((a, i) => {
        const entry = { ...a };
        const isEntry = a.action === 'BUY' || a.action === 'SELL';
        if (isEntry) {
            const windowStart = (a.firstTimestamp ?? a.timestamp) - MATCH_SLACK_MS;
            const nextTs = num(actions[i + 1]?.timestamp);
            const windowEnd = nextTs !== null ? nextTs + MATCH_SLACK_MS : a.timestamp + FILL_WINDOW_FALLBACK_MS;
            const wantSide = ENTRY_SIDE[a.action];
            const sideOk = (side: unknown) => !side || String(side).toLowerCase() === wantSide;
            const filledClosed = positions.find((p) => {
                const ts = num(p.entryTimestamp);
                return ts !== null && ts >= windowStart && ts <= windowEnd && sideOk(p.side);
            });
            if (filledClosed) {
                const entryTs = num(filledClosed.entryTimestamp);
                const exitTs = num(filledClosed.exitTimestamp);
                entry.outcome = {
                    closedPnlPctOnMargin: num(filledClosed.pnlPct),
                    heldMin: entryTs !== null && exitTs !== null ? Math.max(0, Math.round((exitTs - entryTs) / 60_000)) : null,
                };
            } else {
                const openTs = num(openPosition?.entryTimestamp);
                const filledOpen =
                    openPosition && openTs !== null && openTs >= windowStart && openTs <= windowEnd && sideOk(openPosition.side);
                if (filledOpen) {
                    entry.outcome = 'still_open';
                } else if (a.entryLimitPrice != null && (nextTs !== null || nowMs - a.timestamp > FILL_WINDOW_FALLBACK_MS)) {
                    // Only claim never_filled once the order is certainly gone
                    // (a later evaluation superseded it, or the window expired);
                    // a still-resting limit gets no outcome claim.
                    entry.outcome = 'never_filled';
                } else {
                    entry.outcome = null;
                }
            }
        } else if (a.action === 'CLOSE' && (a.closePct == null || a.closePct >= 100)) {
            // Full closes only: a trim's realized chunk pnl isn't the position
            // row's pnl, so partial closes keep their "CLOSE 30%" label alone.
            const match = positions.find((p) => {
                const exitTs = num(p.exitTimestamp);
                return exitTs !== null && Math.abs(exitTs - a.timestamp) <= CLOSE_MATCH_MS;
            });
            if (match) {
                const entryTs = num(match.entryTimestamp);
                const exitTs = num(match.exitTimestamp);
                entry.outcome = {
                    closedPnlPctOnMargin: num(match.pnlPct),
                    heldMin: entryTs !== null && exitTs !== null ? Math.max(0, Math.round((exitTs - entryTs) / 60_000)) : null,
                };
            } else {
                entry.outcome = null;
            }
        } else {
            entry.outcome = null;
        }
        return entry;
    });
}
