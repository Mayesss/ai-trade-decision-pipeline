import { computeSelectorScore, loadForexPairMarketState, toPairMetrics } from './marketData';
import { getForexStrategyConfig, getForexUniversePairs } from './config';
import { evaluateForexEventGate } from './events/gate';
import type {
    ForexPairEligibility,
    ForexScanSnapshot,
    NormalizedForexEconomicEvent,
} from './types';

function isSessionTradeable(sessionTag: string): boolean {
    return sessionTag !== 'DEAD_HOURS';
}

export function selectorTopRankCutoff(totalRows: number, topPercent: number): number {
    const safeTotal = Math.max(0, Math.floor(Number(totalRows) || 0));
    if (safeTotal <= 0) return 0;
    const pct = Math.max(1, Math.min(100, Number(topPercent) || 100));
    return Math.max(1, Math.ceil((safeTotal * pct) / 100));
}

export function isWithinSelectorTopPercentile(params: {
    rank: number;
    totalRows: number;
    topPercent: number;
}): boolean {
    const rank = Math.floor(Number(params.rank) || 0);
    if (rank <= 0) return false;
    const cutoff = selectorTopRankCutoff(params.totalRows, params.topPercent);
    return cutoff > 0 && rank <= cutoff;
}

export function evaluatePairEligibility(params: {
    pair: string;
    metrics: ReturnType<typeof toPairMetrics>;
    staleEvents: boolean;
    events: NormalizedForexEconomicEvent[];
}): ForexPairEligibility {
    const cfg = getForexStrategyConfig();
    const reasons: string[] = [];
    let eligible = true;

    const { pair, metrics, staleEvents, events } = params;

    if (metrics.spreadToAtr1h >= cfg.selector.maxSpreadToAtr1h) {
        eligible = false;
        reasons.push('SPREAD_TO_ATR_TOO_HIGH');
    }

    if (metrics.atr1hPercent < cfg.selector.minAtr1hPercent) {
        eligible = false;
        reasons.push('ATR_TOO_LOW');
    }

    if (!isSessionTradeable(metrics.sessionTag)) {
        eligible = false;
        reasons.push('DEAD_SESSION');
    }

    if (metrics.shockFlag) {
        eligible = false;
        reasons.push('POST_SHOCK_COOLDOWN');
    }

    const gate = evaluateForexEventGate({
        pair,
        events,
        staleData: staleEvents,
        riskState: 'normal',
    });
    if (gate.blockNewEntries) {
        eligible = false;
        reasons.push(...gate.reasonCodes);
    }

    const score = computeSelectorScore(metrics);
    if (score < cfg.selector.minScore) {
        eligible = false;
        reasons.push('SCORE_BELOW_MIN');
    }

    if (!reasons.length) reasons.push('ELIGIBLE');

    return {
        pair,
        eligible,
        rank: 0,
        score,
        reasons,
        metrics,
    };
}

export async function runForexUniverseScan(params: {
    nowMs?: number;
    events: NormalizedForexEconomicEvent[];
    staleEvents: boolean;
}): Promise<ForexScanSnapshot> {
    const nowMs = Number.isFinite(params.nowMs as number) ? Number(params.nowMs) : Date.now();
    const pairs = getForexUniversePairs();

    const rows = await Promise.all(
        pairs.map(async (pair) => {
            const state = await loadForexPairMarketState(pair, nowMs);
            const metrics = toPairMetrics(state);
            return evaluatePairEligibility({
                pair,
                metrics,
                staleEvents: params.staleEvents,
                events: params.events,
            });
        }),
    );

    const sorted = rows
        .slice()
        .sort((a, b) => b.score - a.score)
        .map((row, index) => ({ ...row, rank: index + 1 }));

    return {
        generatedAtMs: nowMs,
        staleEvents: params.staleEvents,
        pairs: sorted,
    };
}
