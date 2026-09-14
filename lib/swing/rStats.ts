// R-multiple statistics — the one number set the measurement window is read
// in. R = pnl_net / the risk ACTUALLY taken at entry, so a clean stop-out is
// −1R and a −2R says the stop slipped or gapped. Cash PnL across mixed
// position sizes hides expectancy; R does not. Pure — the rows come from
// loadClosedPositionRiskRows (lib/swing/pg.ts).
//
// The denominator is not the budget. Sizing budgets equity × RISK_EQUITY_PCT
// and then clamps the notional at EXPOSURE_CAP_EQUITY_MULT × equity, which
// binds for any stop tighter than ~5% of entry — i.e. essentially every swing
// stop (riskSizing.ts says so out loud). Dividing by the budget therefore
// divides by a number 5–16× too large and compresses every R toward zero.
// Measured on 122 closes, 2026-09-11: stop-implied risk $1.23 (bitget) / $0.56
// (capital) against budgets of $6.01 / $9.05; median loser −0.05R and nothing
// in 125 trades worse than −0.75R, in a design where a clean stop is −1R. Both
// the "avg R ≤ 0 at 200 closes" read and the "worse than −1.3R means slippage"
// tripwire were unfireable against that unit.
//
// A row whose real denominator cannot be reconstructed is counted as CLOSED
// BUT NOT MEASURED rather than falling back to the budget: mixing units across
// trades makes the mean meaningless, which is the failure being repaired.

import { R_SAMPLE_TARGET } from './decisionConfig';
import { MIN_SIZEABLE_STOP_PCT } from './riskSizing';

import type { SwingClosedRiskRow } from './pg';

// Where a measured row's denominator came from. 'position_stop' is preferred:
// it uses the fill's OWN notional and entry against the stop actually shipped,
// so it survives a resting entry filling away from the decision price.
export type RiskSource = 'position_stop' | 'effective';

// Largest stop distance treated as real. Beyond a full 100% of entry the row is
// a data error (inverted or unit-mismatched stop), not a wide stop.
const MAX_PLAUSIBLE_STOP_PCT = 1;

export function resolveRealizedRiskUsd(
    row: SwingClosedRiskRow,
): { riskUsd: number; source: RiskSource } | null {
    const { entryPrice, stopPrice, notionalUsd } = row;
    if (entryPrice != null && stopPrice != null && notionalUsd != null) {
        const stopDistancePct = Math.abs(entryPrice - stopPrice) / entryPrice;
        if (stopDistancePct > MIN_SIZEABLE_STOP_PCT && stopDistancePct < MAX_PLAUSIBLE_STOP_PCT) {
            return { riskUsd: notionalUsd * stopDistancePct, source: 'position_stop' };
        }
    }
    if (row.effectiveRiskUsd != null && row.effectiveRiskUsd > 0) {
        return { riskUsd: row.effectiveRiskUsd, source: 'effective' };
    }
    return null;
}

export type RStats = {
    // Closed positions in the window, measured or not.
    closed: number;
    // Of those, the ones whose real risk denominator could be resolved.
    measured: number;
    // Provenance of the measured rows. A window carried mostly by 'effective'
    // is reading planned risk; 'position_stop' is the fill's own.
    sources: Record<RiskSource, number>;
    target: number;
    // Mean R over measured trades — the expectancy per unit risked.
    avgR: number | null;
    sumR: number | null;
    winRate: number | null; // 0..1 over measured trades
    avgWinR: number | null;
    avgLossR: number | null; // negative
    // avgWinR / |avgLossR|: how many losers one winner pays for.
    payoff: number | null;
    // Worst single trade in R — a stop that slipped shows up here first.
    minR: number | null;
    // Cumulative R, one point per measured trade, for a sparkline.
    curve: number[];
};

const round = (n: number, dp: number) => Number(n.toFixed(dp));

export function summarizeRStats(rows: SwingClosedRiskRow[], target: number = R_SAMPLE_TARGET): RStats {
    const rs: number[] = [];
    const sources: Record<RiskSource, number> = { position_stop: 0, effective: 0 };
    for (const row of rows) {
        if (row.pnlNet == null) continue;
        const risk = resolveRealizedRiskUsd(row);
        if (!risk) continue;
        sources[risk.source] += 1;
        rs.push(row.pnlNet / risk.riskUsd);
    }
    const wins = rs.filter((r) => r > 0);
    const losses = rs.filter((r) => r <= 0);
    const sum = (xs: number[]) => xs.reduce((a, b) => a + b, 0);
    const mean = (xs: number[]) => (xs.length ? sum(xs) / xs.length : null);
    const avgWinR = mean(wins);
    const avgLossR = mean(losses);
    let running = 0;
    const curve = rs.map((r) => round((running += r), 3));
    return {
        closed: rows.length,
        measured: rs.length,
        sources,
        target,
        avgR: rs.length ? round(sum(rs) / rs.length, 3) : null,
        sumR: rs.length ? round(sum(rs), 2) : null,
        winRate: rs.length ? round(wins.length / rs.length, 4) : null,
        avgWinR: avgWinR == null ? null : round(avgWinR, 3),
        avgLossR: avgLossR == null ? null : round(avgLossR, 3),
        payoff: avgWinR != null && avgLossR != null && avgLossR < 0 ? round(avgWinR / -avgLossR, 2) : null,
        minR: rs.length ? round(Math.min(...rs), 2) : null,
        curve,
    };
}
