// Two pre-AI skips for FLAT ticks, both pure "spend nothing on a call that
// cannot change anything" filters — they only ever skip work, never override a
// decision (the ai-bouncer rule). Measured on the week of 2026-09-07
// (docs/week-one-review-2026-09-10.md §11):
//
// 1. Spendable-margin gate. 66 of 169 flat Bitget AI calls answered BUY/SELL
//    and were then dropped in code with insufficient_available_margin: the
//    risk-sized entry needed a median 23 USDT of margin against a median 8.4
//    spendable (four positions open on a ~60 USDT account). The existing
//    pre-AI margin gate asks only whether the venue's MINIMUM size is
//    affordable (need≈1), so it never fired. This one asks whether the entry
//    the sizing will actually produce can be posted at the most permissive
//    leverage the model may pick.
//
// 2. Boundary same-setup dedupe. 45 bar-close flat calls repeated a setup the
//    previous bar-close call had already answered HOLD on — same admitting
//    door, price within 0.5 ATR, no plan left behind, nothing resting. 39 held
//    again, 6 entered. Skipping the repeat costs those 6 entries one bar, and
//    the one-bar limit (never dedupe two bars in a row) bounds the deferral.
import { EXPOSURE_CAP_EQUITY_MULT, RISK_EQUITY_PCT } from './riskSizing';

// ---------------------------------------------------------------------------
// 1. Spendable margin
// ---------------------------------------------------------------------------

// The margin a risk-sized entry needs at the most permissive leverage. The
// exposure cap (EXPOSURE_CAP_EQUITY_MULT × equity) binds for every stop tighter
// than RISK_EQUITY_PCT / EXPOSURE_CAP_EQUITY_MULT of price (0.5% at the defaults) —
// i.e. every swing stop — so the entry's notional IS the cap and its margin is
// cap / leverage. Taking the maximum leverage the model may request makes this
// the SMALLEST margin any compliant entry could post: if even that does not fit,
// no answer the model gives can be executed.
export function estimateEntryMarginNeedUsd(params: {
    equityUsd: number;
    maxLeverage: number;
    exposureCapMult?: number;
}): number | null {
    const equity = Number(params.equityUsd);
    const lev = Number(params.maxLeverage);
    const cap = params.exposureCapMult ?? EXPOSURE_CAP_EQUITY_MULT;
    if (!(equity > 0) || !(lev > 0) || !(cap > 0)) return null;
    return (cap * equity) / lev;
}

export type SpendableMarginVerdict = {
    blocked: boolean;
    needUsd: number | null;
    haveUsd: number | null;
};

// Same 2% headroom the post-AI check applies (fees/price drift between check
// and fill). Fails OPEN on any missing reading — the venue still backstops.
export function evaluateSpendableMarginGate(params: {
    equityUsd: number | null | undefined;
    availableUsd: number | null | undefined;
    maxLeverage: number;
    headroom?: number;
}): SpendableMarginVerdict {
    // null/undefined readings fail OPEN — Number(null) is 0, which would read
    // as "nothing spendable" and block every call on a failed account read.
    if (params.availableUsd == null || params.equityUsd == null) {
        return { blocked: false, needUsd: null, haveUsd: null };
    }
    const have = Number(params.availableUsd);
    const equity = Number(params.equityUsd);
    if (!Number.isFinite(have) || !Number.isFinite(equity) || equity <= 0) {
        return { blocked: false, needUsd: null, haveUsd: Number.isFinite(have) ? have : null };
    }
    const need = estimateEntryMarginNeedUsd({ equityUsd: equity, maxLeverage: params.maxLeverage });
    if (need === null) return { blocked: false, needUsd: null, haveUsd: have };
    const headroom = params.headroom ?? 0.98;
    return { blocked: need > have * headroom, needUsd: need, haveUsd: have };
}

// Documented for the reader of the gate's tick_log rows: at RISK_EQUITY_PCT=10
// and cap 2×, the cap binds below a 5% stop.
export const SPENDABLE_GATE_CAP_BINDS_BELOW_STOP_PCT = RISK_EQUITY_PCT / EXPOSURE_CAP_EQUITY_MULT;

// ---------------------------------------------------------------------------
// 2. Boundary same-setup dedupe
// ---------------------------------------------------------------------------

// Read at call time so a scenario can flip them. 0 ATR disables the gate.
export function resolveBoundaryDedupeConfig(): { maxMoveAtr: number; maxAgeMin: number } {
    const atr = Number(process.env.SWING_FLAT_BOUNDARY_DEDUPE_ATR);
    const age = Number(process.env.SWING_FLAT_BOUNDARY_DEDUPE_MAX_AGE_MIN);
    return {
        maxMoveAtr: Number.isFinite(atr) && atr >= 0 ? atr : 0.5,
        // One primary bar plus cron slack: the previous bar-close call, nothing older.
        maxAgeMin: Number.isFinite(age) && age > 0 ? age : 260,
    };
}

export type BoundaryDedupeInput = {
    // The most recent flat AI call for this symbol (not a skip row).
    lastFlat: {
        action: string | null | undefined;
        ageMin: number;
        priceMoveAtr: number | null;
        actionabilityReason: string | null | undefined;
        // Did that call leave a plan (wake band or cooldown)? A plan means the
        // model wants the band, not the bar, to bring it back — never dedupe
        // over it; the band fires on its own.
        leftPlan: boolean;
    } | null;
    currentActionabilityReason: string | null | undefined;
    // A resting order stands: the bar-close re-look does real work 42% of the
    // time (withdraw/supersede) — never dedupe it.
    restingEntryStanding: boolean;
    // The previous bar was itself deduped: the one-bar limit says evaluate now.
    dedupedLastBar: boolean;
    maxMoveAtr: number;
    maxAgeMin: number;
};

export function shouldDedupeBoundaryLook(input: BoundaryDedupeInput): boolean {
    if (!(input.maxMoveAtr > 0)) return false;
    if (input.restingEntryStanding || input.dedupedLastBar) return false;
    const last = input.lastFlat;
    if (!last) return false;
    if (String(last.action || '').toUpperCase() !== 'HOLD') return false;
    if (last.leftPlan) return false;
    if (!(last.ageMin >= 0 && last.ageMin <= input.maxAgeMin)) return false;
    if (last.priceMoveAtr === null || !Number.isFinite(last.priceMoveAtr)) return false;
    if (last.priceMoveAtr > input.maxMoveAtr) return false;
    const a = String(last.actionabilityReason || '');
    const b = String(input.currentActionabilityReason || '');
    return a.length > 0 && a === b;
}
