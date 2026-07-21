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
    return Number.isFinite(n) && n > 0 && n <= 5 ? n : 1;
})();

// Absolute risk used when no equity reading is available (fetch failed and no
// recent snapshot): deliberately small — fail toward under-sizing.
export const RISK_FALLBACK_USD = (() => {
    const n = Number(process.env.SWING_RISK_FALLBACK_USD);
    return Number.isFinite(n) && n > 0 ? n : 5;
})();

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
    // A stop this close to entry is upstream-invalid (the sanitizer enforces a
    // ≥1-ATR floor); refuse to size against it rather than emit a huge notional.
    if (!(stopDistancePct > 0.0005)) return null;

    const equityUsd = Number.isFinite(params.equityUsd as number) && (params.equityUsd as number) > 0
        ? Number(params.equityUsd)
        : null;
    const riskEquityPct = params.riskEquityPct ?? RISK_EQUITY_PCT;
    const fallbackRiskUsd = params.fallbackRiskUsd ?? RISK_FALLBACK_USD;
    const riskUsd = equityUsd !== null ? (equityUsd * riskEquityPct) / 100 : fallbackRiskUsd;

    let notionalUsd = riskUsd / stopDistancePct;
    // Exposure ceiling: never let a tight (1-ATR minimum) stop turn 1% risk
    // into more than 2× the account in notional — beyond that, gap/slippage
    // risk dominates the modeled stop-out cost.
    if (equityUsd !== null && notionalUsd > equityUsd * 2) notionalUsd = equityUsd * 2;

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
