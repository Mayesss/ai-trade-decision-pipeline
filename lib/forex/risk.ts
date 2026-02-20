import { fetchCapitalPositionInfo } from '../capital';
import { getForexStrategyConfig, spreadPipsCapForPair } from './config';
import { getForexPairCooldownUntil, setForexPairCooldown } from './store';
import type { ForexEventGateDecision, ForexPairMetrics, ForexRiskCheck } from './types';
import { pairCurrencies } from './events/gate';

function nowMsOr(value: number | undefined): number {
    return Number.isFinite(value as number) ? Number(value) : Date.now();
}

export type CurrencyExposureMap = Record<string, number>;

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
    }

    if (params.metrics.spreadToAtr1h > cfg.risk.maxSpreadToAtr1h) {
        allowEntry = false;
        reasons.push('SPREAD_TO_ATR_RISK_CAP_EXCEEDED');
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
        reasonCodes: Array.from(new Set(reasons)),
        cooldownUntilMs: Number.isFinite(cooldownUntil as number) ? Number(cooldownUntil) : null,
    };
}
