// R-multiple statistics — the one number set the measurement window is read
// in. R = pnl_net / risk budgeted at entry, so a clean stop-out is −1R and a
// −2R says the stop slipped or gapped. Cash PnL across mixed position sizes
// hides expectancy; R does not. Pure — the rows come from
// loadClosedPositionRiskRows (lib/swing/pg.ts).

import { R_SAMPLE_TARGET } from './decisionConfig';

import type { SwingClosedRiskRow } from './pg';

export type RStats = {
    // Closed positions in the window, measured or not.
    closed: number;
    // Of those, the ones with a risk budget on their placing decision.
    measured: number;
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
    for (const row of rows) {
        if (row.pnlNet == null || !(row.riskUsd != null && row.riskUsd > 0)) continue;
        rs.push(row.pnlNet / row.riskUsd);
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
