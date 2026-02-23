import assert from 'node:assert/strict';
import test from 'node:test';

import { createSeededRng } from './replay/random';
import type { ForexOpenRiskUsage } from './risk';
import { evaluateRiskCapBudget } from './risk';

const PAIRS = ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDCAD', 'NZDCHF', 'EURJPY', 'USDCAD'] as const;

function pairCurrencies(pair: string): [string, string] {
    const normalized = String(pair).trim().toUpperCase();
    return [normalized.slice(0, 3), normalized.slice(3, 6)];
}

function emptyUsage(): ForexOpenRiskUsage {
    return {
        portfolioOpenRiskPct: 0,
        currencyOpenRiskPct: {},
        pairOpenRiskPct: {},
        unknownRiskPairs: [],
    };
}

test('property: evaluateRiskCapBudget matches explicit cap math over randomized inputs', () => {
    const rng = createSeededRng(20260223);
    for (let i = 0; i < 600; i += 1) {
        const pair = PAIRS[Math.floor(rng.next() * PAIRS.length)]!;
        const [base, quote] = pairCurrencies(pair);
        const usage = emptyUsage();
        usage.portfolioOpenRiskPct = rng.next() * 4;
        usage.currencyOpenRiskPct[base] = rng.next() * 2.8;
        usage.currencyOpenRiskPct[quote] = rng.next() * 2.8;

        const candidateRiskPct = 0.01 + rng.next() * 1.2;
        const maxPortfolioOpenPct = rng.next() < 0.1 ? 0 : 0.4 + rng.next() * 3.2;
        const maxCurrencyOpenPct = rng.next() < 0.1 ? 0 : 0.3 + rng.next() * 2.2;

        const expectedPortfolioBreach =
            maxPortfolioOpenPct > 0 && usage.portfolioOpenRiskPct + candidateRiskPct > maxPortfolioOpenPct;
        const expectedCurrencyBreach =
            maxCurrencyOpenPct > 0 &&
            ((usage.currencyOpenRiskPct[base] || 0) + candidateRiskPct > maxCurrencyOpenPct ||
                (usage.currencyOpenRiskPct[quote] || 0) + candidateRiskPct > maxCurrencyOpenPct);

        const out = evaluateRiskCapBudget({
            pair,
            candidateRiskPct,
            usage,
            maxPortfolioOpenPct,
            maxCurrencyOpenPct,
        });

        const expectedAllow = !(expectedPortfolioBreach || expectedCurrencyBreach);
        assert.equal(out.allow, expectedAllow, `allow mismatch at iteration ${i}`);
        assert.equal(
            out.reasonCodes.includes('NO_TRADE_RISK_CAP_PORTFOLIO'),
            expectedPortfolioBreach,
            `portfolio reason mismatch at iteration ${i}`,
        );
        assert.equal(
            out.reasonCodes.includes('NO_TRADE_RISK_CAP_CURRENCY'),
            expectedCurrencyBreach,
            `currency reason mismatch at iteration ${i}`,
        );
    }
});

test('property: accepted candidate risk actions never breach caps across random action sequences', () => {
    const rng = createSeededRng(772314);
    const maxPortfolioOpenPct = 2.0;
    const maxCurrencyOpenPct = 1.0;
    const eps = 1e-9;

    for (let trial = 0; trial < 120; trial += 1) {
        const usage = emptyUsage();
        for (let step = 0; step < 45; step += 1) {
            const pair = PAIRS[Math.floor(rng.next() * PAIRS.length)]!;
            const [base, quote] = pairCurrencies(pair);
            const candidateRiskPct = 0.05 + rng.next() * 0.35;
            const decision = evaluateRiskCapBudget({
                pair,
                candidateRiskPct,
                usage,
                maxPortfolioOpenPct,
                maxCurrencyOpenPct,
            });

            if (!decision.allow) continue;
            usage.portfolioOpenRiskPct += candidateRiskPct;
            usage.currencyOpenRiskPct[base] = (usage.currencyOpenRiskPct[base] || 0) + candidateRiskPct;
            usage.currencyOpenRiskPct[quote] = (usage.currencyOpenRiskPct[quote] || 0) + candidateRiskPct;

            assert.ok(
                usage.portfolioOpenRiskPct <= maxPortfolioOpenPct + eps,
                `portfolio cap breached at trial=${trial}, step=${step}`,
            );
            assert.ok(
                usage.currencyOpenRiskPct[base]! <= maxCurrencyOpenPct + eps,
                `currency cap breached (${base}) at trial=${trial}, step=${step}`,
            );
            assert.ok(
                usage.currencyOpenRiskPct[quote]! <= maxCurrencyOpenPct + eps,
                `currency cap breached (${quote}) at trial=${trial}, step=${step}`,
            );
        }
    }
});
