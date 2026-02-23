import { isWithinSessionTransitionBuffer } from '../config';
import type { ReplayEventRisk, ReplayQuote, ReplaySlippageConfig, ReplaySpreadStressConfig } from './types';
import type { SeededRng } from './random';

export interface StressedQuote {
    ts: number;
    bid: number;
    ask: number;
    mid: number;
    spreadAbs: number;
    spreadMultiplier: number;
    spreadReasons: string[];
    eventRisk: ReplayEventRisk;
    shock: boolean;
    rollover: boolean;
}

function safeEventRisk(value: ReplayQuote['eventRisk']): ReplayEventRisk {
    if (value === 'medium' || value === 'high') return value;
    return 'none';
}

export function applySpreadStress(quote: ReplayQuote, cfg: ReplaySpreadStressConfig): StressedQuote {
    const bid = Number(quote.bid);
    const ask = Number(quote.ask);
    if (!(Number.isFinite(bid) && Number.isFinite(ask) && ask > bid && bid > 0)) {
        throw new Error(`Invalid quote at ts=${quote.ts}: bid=${quote.bid}, ask=${quote.ask}`);
    }

    const eventRisk = safeEventRisk(quote.eventRisk);
    const shock = Boolean(quote.shock);
    const rollover = Boolean(quote.rollover);
    const mid = (bid + ask) / 2;
    const baseSpreadAbs = ask - bid;

    let spreadMultiplier = 1;
    const spreadReasons: string[] = [];

    if (isWithinSessionTransitionBuffer(quote.ts, cfg.transitionBufferMinutes)) {
        spreadMultiplier *= Math.max(1, Number(cfg.transitionMultiplier) || 1);
        spreadReasons.push('SESSION_TRANSITION_SPREAD_STRESS');
    }
    if (rollover) {
        spreadMultiplier *= Math.max(1, Number(cfg.rolloverMultiplier) || 1);
        spreadReasons.push('ROLLOVER_SPREAD_STRESS');
    }
    if (eventRisk === 'medium') {
        spreadMultiplier *= Math.max(1, Number(cfg.mediumEventMultiplier) || 1);
        spreadReasons.push('EVENT_MEDIUM_SPREAD_STRESS');
    }
    if (eventRisk === 'high') {
        spreadMultiplier *= Math.max(1, Number(cfg.highEventMultiplier) || 1);
        spreadReasons.push('EVENT_HIGH_SPREAD_STRESS');
    }
    if (Number.isFinite(Number(quote.spreadMultiplier)) && Number(quote.spreadMultiplier) > 1) {
        spreadMultiplier *= Number(quote.spreadMultiplier);
        spreadReasons.push('CUSTOM_SPREAD_MULTIPLIER');
    }

    const stressedSpread = Math.max(1e-9, baseSpreadAbs * spreadMultiplier);
    const stressedBid = mid - stressedSpread / 2;
    const stressedAsk = mid + stressedSpread / 2;

    return {
        ts: quote.ts,
        bid: stressedBid,
        ask: stressedAsk,
        mid,
        spreadAbs: stressedSpread,
        spreadMultiplier,
        spreadReasons,
        eventRisk,
        shock,
        rollover,
    };
}

export function executionSlippageBps(params: {
    quote: StressedQuote;
    cfg: ReplaySlippageConfig;
    rng: SeededRng;
    isEntry: boolean;
}): number {
    const { quote, cfg, rng, isEntry } = params;
    let bps = isEntry ? cfg.entryBaseBps : cfg.exitBaseBps;

    if (quote.eventRisk === 'medium') bps += cfg.mediumEventBps;
    if (quote.eventRisk === 'high') bps += cfg.highEventBps;
    if (quote.shock) bps += cfg.shockBps;

    const randomBps = (Number(cfg.randomBps) || 0) * rng.nextSigned();
    bps += randomBps;
    return Math.max(0, bps);
}

export function applyExecutionPrice(params: {
    side: 'BUY' | 'SELL';
    referencePrice: number;
    slippageBps: number;
}): number {
    const ref = Number(params.referencePrice);
    if (!(Number.isFinite(ref) && ref > 0)) return NaN;
    const bps = Math.max(0, Number(params.slippageBps) || 0);
    const mult = 1 + (bps / 10_000) * (params.side === 'BUY' ? 1 : -1);
    return ref * mult;
}
