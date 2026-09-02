// Fixed-fractional position sizing for swing entries: one full stop-out costs
// a fixed percentage of account equity, so the STOP DISTANCE decides the
// position size — a wide structural stop automatically means a small position.
// This replaces the stop-blind fixed notional (DEFAULT_NOTIONAL_USDT × lev),
// under which realized risk per trade ranged −$0.30 to −$12 depending only on
// how close the model happened to place its stop.

export type RiskBasedSizing = {
    // What a full stop-out should cost, in account currency.
    riskUsd: number;
    // Position size (exposure) that makes the stop cost exactly riskUsd.
    notionalUsd: number;
    // Margin to post for that notional at the given leverage — this is the
    // "sideSize" figure both execution paths take as input.
    marginUsd: number;
    // |entry − stop| / entry.
    stopDistancePct: number;
    equityUsd: number | null;
    source: 'equity_pct' | 'fallback_fixed';
};

// Risk per trade as % of account equity.
export const RISK_EQUITY_PCT = (() => {
    const n = Number(process.env.SWING_RISK_EQUITY_PCT);
    return Number.isFinite(n) && n > 0 && n <= 20 ? n : 10;
})();

// Absolute risk used when no equity reading is available (fetch failed and no
// recent snapshot): deliberately small — fail toward under-sizing.
export const RISK_FALLBACK_USD = (() => {
    const n = Number(process.env.SWING_RISK_FALLBACK_USD);
    return Number.isFinite(n) && n > 0 ? n : 5;
})();

// Smallest stop, as a fraction of entry, this function will size against. Below
// it the notional explodes, so sizing refuses and the caller falls back to the
// legacy fixed notional — the stop-blind sizing this module exists to replace.
//
// EXPORTED because sanitizeExchangeTpSl has to keep stops OUT of this band: its
// own floor is BRACKET_MIN_GAP_ATR (0.1 ATR), and on a low-ATR instrument 0.1
// ATR is SMALLER than this (EURUSD 4H ATR ≈ 0.355% of price → 0.1 ATR ≈
// 0.036%), so a stop could pass sanitation and still be unsizeable. The two
// floors have to be compared in one unit or the gap is silent.
export const MIN_SIZEABLE_STOP_PCT = 0.0005;

// Exposure ceiling as a multiple of equity. Named (not inlined) because the
// prompt quotes it: the model owns stop distance now, and this is what actually
// converts a tight stop into a smaller position rather than a same-cost one.
export const EXPOSURE_CAP_EQUITY_MULT = 2;

export function resolveRiskBasedSizing(params: {
    entryPrice: number;
    stopPrice: number;
    equityUsd: number | null;
    leverage: number | null;
    riskEquityPct?: number;
    fallbackRiskUsd?: number;
}): RiskBasedSizing | null {
    const entry = Number(params.entryPrice);
    const stop = Number(params.stopPrice);
    if (!Number.isFinite(entry) || entry <= 0 || !Number.isFinite(stop) || stop <= 0) return null;
    const stopDistancePct = Math.abs(entry - stop) / entry;
    // Degenerate-stop backstop. The sanitizer's ≥1-ATR entry floor was removed
    // 2026-09-02 (the model owns stop distance now), so this is no longer a
    // redundant check — it is the only guard against a stop sitting essentially
    // on top of entry. Refuse to size rather than emit an unbounded notional;
    // the 2× equity ceiling below caps everything above it.
    if (!(stopDistancePct > MIN_SIZEABLE_STOP_PCT)) return null;

    const equityUsd = Number.isFinite(params.equityUsd as number) && (params.equityUsd as number) > 0
        ? Number(params.equityUsd)
        : null;
    const riskEquityPct = params.riskEquityPct ?? RISK_EQUITY_PCT;
    const fallbackRiskUsd = params.fallbackRiskUsd ?? RISK_FALLBACK_USD;
    const riskUsd = equityUsd !== null ? (equityUsd * riskEquityPct) / 100 : fallbackRiskUsd;

    let notionalUsd = riskUsd / stopDistancePct;
    // Exposure ceiling: never let a tight stop turn the fixed risk fraction into
    // more than EXPOSURE_CAP_EQUITY_MULT× the account in notional — beyond that,
    // gap/slippage risk dominates the modeled stop-out cost. Since the ≥1-ATR
    // entry-stop floor was removed this is the load-bearing guard on tight stops:
    // it already bound at that floor (a 4H crypto ATR is ~1.4% of price), so
    // admitting tighter stops lowers realized risk here rather than raising it.
    //
    // It is NOT a rare backstop: at RISK_EQUITY_PCT=10 it binds for every stop
    // tighter than 5% of entry, i.e. essentially every swing stop. Whenever it
    // binds, "fixed dollar risk" stops being true — realized risk becomes
    // EXPOSURE_CAP_EQUITY_MULT × stopDistancePct of equity, so a tighter stop is
    // a genuinely smaller loss AND a genuinely smaller position. The prompt says
    // so out loud (see DECISION OWNERSHIP in prompt.ts): a model told its stop
    // width is risk-neutral would shrink stops for nominal R and quietly trade a
    // fraction of the intended book.
    if (equityUsd !== null && notionalUsd > equityUsd * EXPOSURE_CAP_EQUITY_MULT) {
        notionalUsd = equityUsd * EXPOSURE_CAP_EQUITY_MULT;
    }

    const lev = Number.isFinite(params.leverage as number) && (params.leverage as number) > 0
        ? Number(params.leverage)
        : 1;
    const marginUsd = notionalUsd / lev;

    return {
        riskUsd,
        notionalUsd,
        marginUsd,
        stopDistancePct,
        equityUsd,
        source: equityUsd !== null ? 'equity_pct' : 'fallback_fixed',
    };
}
