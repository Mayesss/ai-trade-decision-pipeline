import type { CapitalOpenPositionSnapshot } from '../capital';
import { fetchCapitalPositionInfo } from '../capital';
import {
    getForexStrategyConfig,
    isWithinSessionTransitionBuffer,
    spreadPipsCapForPair,
    tightenSpreadToAtrCap,
} from './config';
import { isWithinPreRolloverWindow } from './rollover';
import { getForexPairCooldownUntil, setForexPairCooldown } from './store';
import type { ForexEventGateDecision, ForexPairMetrics, ForexPositionContext, ForexRiskCheck } from './types';
import { pairCurrencies } from './events/gate';

function nowMsOr(value: number | undefined): number {
    return Number.isFinite(value as number) ? Number(value) : Date.now();
}

export type CurrencyExposureMap = Record<string, number>;
export type CurrencyRiskPctMap = Record<string, number>;
export type PairRiskPctMap = Record<string, number>;

export interface ForexOpenRiskUsage {
    portfolioOpenRiskPct: number;
    currencyOpenRiskPct: CurrencyRiskPctMap;
    pairOpenRiskPct: PairRiskPctMap;
    unknownRiskPairs: string[];
}

export interface ForexHybridSizeDecision {
    sideSizeUsd: number;
    leverage: number;
    effectiveNotionalUsd: number;
    riskUsd: number | null;
    riskPctUsed: number | null;
    stopDistance: number | null;
    usedFallback: boolean;
    reasonCodes: string[];
}

function safePositive(value: unknown): number {
    const n = Number(value);
    return Number.isFinite(n) && n > 0 ? n : NaN;
}

export function confidenceToLeverageCapped(confidence: number, maxLeverage: number): number {
    const cap = Math.max(1, Math.floor(safePositive(maxLeverage) || 1));
    let suggested = 1;
    if (confidence >= 0.85) suggested = 3;
    else if (confidence >= 0.68) suggested = 2;
    return Math.min(cap, suggested);
}

export function computeOpenRiskUsage(params: {
    openByPair: Map<string, CapitalOpenPositionSnapshot>;
    contextsByPair: Map<string, ForexPositionContext>;
    equityUsd: number;
    fallbackRiskPctForUnknown?: number;
}): ForexOpenRiskUsage {
    const equityUsd = safePositive(params.equityUsd);
    const fallbackRiskPct = Number.isFinite(Number(params.fallbackRiskPctForUnknown))
        ? Math.max(0, Number(params.fallbackRiskPctForUnknown))
        : 0;

    const usage: ForexOpenRiskUsage = {
        portfolioOpenRiskPct: 0,
        currencyOpenRiskPct: {},
        pairOpenRiskPct: {},
        unknownRiskPairs: [],
    };

    for (const [pair, snapshot] of params.openByPair.entries()) {
        const context = params.contextsByPair.get(pair);
        const size = safePositive(snapshot?.size);
        const entryPrice = safePositive(snapshot?.entryPrice) || safePositive(context?.entryPrice);
        const stopPrice = safePositive(context?.currentStopPrice) || safePositive(context?.initialStopPrice);

        let riskPct = NaN;
        if (Number.isFinite(equityUsd) && Number.isFinite(size) && Number.isFinite(entryPrice) && Number.isFinite(stopPrice)) {
            const riskUsd = Math.abs(entryPrice - stopPrice) * size;
            riskPct = (riskUsd / equityUsd) * 100;
        }

        if (!(Number.isFinite(riskPct) && riskPct > 0)) {
            if (fallbackRiskPct > 0) {
                riskPct = fallbackRiskPct;
            } else {
                usage.unknownRiskPairs.push(pair);
                continue;
            }
        }

        usage.pairOpenRiskPct[pair] = (usage.pairOpenRiskPct[pair] || 0) + riskPct;
        usage.portfolioOpenRiskPct += riskPct;
        for (const currency of pairCurrencies(pair)) {
            usage.currencyOpenRiskPct[currency] = (usage.currencyOpenRiskPct[currency] || 0) + riskPct;
        }
    }

    return usage;
}

export function computeHybridRiskSize(params: {
    entryPrice: number;
    stopPrice: number;
    confidence: number;
    fallbackNotionalUsd: number;
    maxLeverage: number;
    riskPerTradePct: number;
    referenceEquityUsd: number;
}): ForexHybridSizeDecision {
    const reasonCodes: string[] = [];
    const fallbackNotionalUsd = safePositive(params.fallbackNotionalUsd);
    const leverage = confidenceToLeverageCapped(params.confidence, params.maxLeverage);
    const entryPrice = safePositive(params.entryPrice);
    const stopPrice = safePositive(params.stopPrice);
    const referenceEquityUsd = safePositive(params.referenceEquityUsd);
    const riskPerTradePct = safePositive(params.riskPerTradePct);

    const stopDistance = Number.isFinite(entryPrice) && Number.isFinite(stopPrice) ? Math.abs(entryPrice - stopPrice) : NaN;
    if (
        !(Number.isFinite(referenceEquityUsd) && referenceEquityUsd > 0) ||
        !(Number.isFinite(riskPerTradePct) && riskPerTradePct > 0) ||
        !(Number.isFinite(stopDistance) && stopDistance > 0) ||
        !(Number.isFinite(entryPrice) && entryPrice > 0) ||
        !(Number.isFinite(fallbackNotionalUsd) && fallbackNotionalUsd > 0)
    ) {
        reasonCodes.push('SIZE_FALLBACK_NOTIONAL');
        return {
            sideSizeUsd: Number.isFinite(fallbackNotionalUsd) && fallbackNotionalUsd > 0 ? fallbackNotionalUsd : 100,
            leverage,
            effectiveNotionalUsd:
                (Number.isFinite(fallbackNotionalUsd) && fallbackNotionalUsd > 0 ? fallbackNotionalUsd : 100) * leverage,
            riskUsd: null,
            riskPctUsed: null,
            stopDistance: Number.isFinite(stopDistance) ? stopDistance : null,
            usedFallback: true,
            reasonCodes,
        };
    }

    const riskUsd = referenceEquityUsd * (riskPerTradePct / 100);
    const units = riskUsd / stopDistance;
    const effectiveNotionalUsd = units * entryPrice;
    const sideSizeUsd = effectiveNotionalUsd / leverage;

    if (!(Number.isFinite(sideSizeUsd) && sideSizeUsd > 0)) {
        reasonCodes.push('SIZE_FALLBACK_NOTIONAL');
        const fallback = Number.isFinite(fallbackNotionalUsd) && fallbackNotionalUsd > 0 ? fallbackNotionalUsd : 100;
        return {
            sideSizeUsd: fallback,
            leverage,
            effectiveNotionalUsd: fallback * leverage,
            riskUsd,
            riskPctUsed: riskPerTradePct,
            stopDistance,
            usedFallback: true,
            reasonCodes,
        };
    }

    reasonCodes.push('SIZE_HYBRID_RISK');
    return {
        sideSizeUsd: sideSizeUsd,
        leverage,
        effectiveNotionalUsd,
        riskUsd,
        riskPctUsed: riskPerTradePct,
        stopDistance,
        usedFallback: false,
        reasonCodes,
    };
}

export function evaluateRiskCapBudget(params: {
    pair: string;
    candidateRiskPct: number;
    usage: ForexOpenRiskUsage;
    maxPortfolioOpenPct: number;
    maxCurrencyOpenPct: number;
}): { allow: boolean; reasonCodes: string[] } {
    const reasons: string[] = [];
    const candidateRiskPct = Math.max(0, Number(params.candidateRiskPct) || 0);
    const maxPortfolioOpenPct = Math.max(0, Number(params.maxPortfolioOpenPct) || 0);
    const maxCurrencyOpenPct = Math.max(0, Number(params.maxCurrencyOpenPct) || 0);

    if (maxPortfolioOpenPct > 0 && params.usage.portfolioOpenRiskPct + candidateRiskPct > maxPortfolioOpenPct) {
        reasons.push('NO_TRADE_RISK_CAP_PORTFOLIO');
    }

    if (maxCurrencyOpenPct > 0) {
        for (const currency of pairCurrencies(params.pair)) {
            const current = params.usage.currencyOpenRiskPct[currency] || 0;
            if (current + candidateRiskPct > maxCurrencyOpenPct) {
                reasons.push('NO_TRADE_RISK_CAP_CURRENCY');
                break;
            }
        }
    }

    return {
        allow: !reasons.length,
        reasonCodes: reasons,
    };
}

export async function buildOpenCurrencyExposure(pairs: string[]): Promise<CurrencyExposureMap> {
    const exposure: CurrencyExposureMap = {};

    await Promise.all(
        pairs.map(async (pair) => {
            try {
                const pos = await fetchCapitalPositionInfo(pair);
                if (pos.status !== 'open') return;
                const currencies = pairCurrencies(pair);
                for (const ccy of currencies) {
                    exposure[ccy] = (exposure[ccy] || 0) + 1;
                }
            } catch {
                // ignore per-pair exposure fetch failure; risk check remains conservative elsewhere
            }
        }),
    );

    return exposure;
}

export async function evaluateForexRiskCheck(params: {
    pair: string;
    metrics: ForexPairMetrics;
    eventGate: ForexEventGateDecision;
    nowMs?: number;
    exposure?: CurrencyExposureMap;
}): Promise<ForexRiskCheck> {
    const cfg = getForexStrategyConfig();
    const nowMs = nowMsOr(params.nowMs);
    const reasons: string[] = [];
    let allowEntry = true;

    if (!params.eventGate.allowNewEntries || params.eventGate.blockNewEntries) {
        allowEntry = false;
        reasons.push(...params.eventGate.reasonCodes);
    }

    const pairSpreadCap = spreadPipsCapForPair(params.pair);
    if (params.metrics.spreadPips > pairSpreadCap) {
        allowEntry = false;
        reasons.push('SPREAD_PIPS_CAP_EXCEEDED');
        reasons.push('NO_TRADE_SPREAD_TOO_HIGH');
    }

    if (params.metrics.spreadToAtr1h > cfg.risk.maxSpreadToAtr1h) {
        allowEntry = false;
        reasons.push('SPREAD_TO_ATR_RISK_CAP_EXCEEDED');
        reasons.push('NO_TRADE_SPREAD_TOO_HIGH');
    }

    const transitionStress =
        isWithinSessionTransitionBuffer(nowMs, cfg.risk.sessionTransitionBufferMinutes) &&
        params.metrics.spreadToAtr1h >
            tightenSpreadToAtrCap(cfg.risk.maxSpreadToAtr1h, cfg.risk.transitionSpreadToAtrMultiplier);
    if (transitionStress) {
        allowEntry = false;
        reasons.push('SESSION_TRANSITION_SPREAD_STRESS');
        reasons.push('SPREAD_TO_ATR_TRANSITION_RISK_CAP_EXCEEDED');
        reasons.push('NO_TRADE_SPREAD_TOO_HIGH');
    }

    if (isWithinPreRolloverWindow(nowMs, cfg.risk.rolloverEntryBlockMinutes, cfg.risk.rolloverHourUtc)) {
        allowEntry = false;
        reasons.push('ROLLOVER_ENTRY_BLOCK_WINDOW');
        reasons.push('NO_TRADE_ROLLOVER_WINDOW');
    }

    if (params.metrics.shockFlag) {
        allowEntry = false;
        const cooldownUntilMs = nowMs + cfg.risk.shockCooldownMinutes * 60_000;
        await setForexPairCooldown(params.pair, cooldownUntilMs);
        reasons.push('VOLATILITY_SHOCK_COOLDOWN_SET');
    }

    const cooldownUntil = await getForexPairCooldownUntil(params.pair);
    if (Number.isFinite(cooldownUntil as number) && Number(cooldownUntil) > nowMs) {
        allowEntry = false;
        reasons.push('PAIR_COOLDOWN_ACTIVE');
    }

    const currencies = pairCurrencies(params.pair);
    if (params.exposure && currencies.length === 2) {
        const maxExp = cfg.risk.maxCurrencyExposure;
        const baseExposure = params.exposure[currencies[0]!] || 0;
        const quoteExposure = params.exposure[currencies[1]!] || 0;
        if (baseExposure >= maxExp || quoteExposure >= maxExp) {
            allowEntry = false;
            reasons.push('CURRENCY_EXPOSURE_LIMIT');
        }
    }

    if (!reasons.length) reasons.push('RISK_GREEN');

    return {
        pair: params.pair,
        allowEntry,
        allowRiskReduction: true,
        reasonCodes: Array.from(new Set(reasons.map((reason) => String(reason).trim()).filter((reason) => reason.length > 0))),
        cooldownUntilMs: Number.isFinite(cooldownUntil as number) ? Number(cooldownUntil) : null,
    };
}
