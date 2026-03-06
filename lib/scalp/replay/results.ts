import type { ScalpBacktestLeaderboardEntry, ScalpReplayResult } from './types';

export function toScalpBacktestLeaderboardEntry(
    result: Pick<ScalpReplayResult, 'config' | 'summary'>,
): ScalpBacktestLeaderboardEntry {
    return {
        symbol: result.summary.symbol,
        strategyId: result.summary.strategyId || result.config.strategyId,
        tuneId: result.summary.tuneId || result.config.tuneId,
        deploymentId: result.summary.deploymentId || result.config.deploymentId,
        tuneLabel: result.summary.tuneLabel || result.config.tuneLabel,
        netR: result.summary.netR,
        profitFactor: result.summary.profitFactor,
        maxDrawdownR: result.summary.maxDrawdownR,
        trades: result.summary.trades,
        winRatePct: result.summary.winRatePct,
        avgHoldMinutes: result.summary.avgHoldMinutes,
        expectancyR: result.summary.expectancyR,
    };
}
