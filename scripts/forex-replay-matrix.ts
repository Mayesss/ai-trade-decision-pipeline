#!/usr/bin/env node
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { defaultReplayConfig, normalizeReplayInput, runReplay } from '../lib/forex/replay/harness';
import { applySpreadStress } from '../lib/forex/replay/models';
import type { ReplayInputFile, ReplayQuote, ReplayResult, ReplayRuntimeConfig } from '../lib/forex/replay/types';

type ShockProfile = 'none' | 'occasional' | 'clustered' | 'frequent';

type MatrixScenario = {
    id: string;
    spreadFactor: number;
    slippageFactor: number;
    shockProfile: ShockProfile;
    seed: number;
};

type FixtureManifestEntry = {
    id: string;
    file: string;
    tier?: 'core' | 'extended' | string;
    tags?: string[];
    description?: string;
    purpose?: string;
};

type FixtureManifest = {
    fixtures: FixtureManifestEntry[];
};

type LoadedFixture = {
    id: string;
    filePath: string;
    tier: string;
    tags: string[];
    description: string;
    purpose: string;
    input: ReturnType<typeof normalizeReplayInput>;
};

type FillStats = {
    avgSpreadBps: number;
    avgSlippageBps: number;
    fills: number;
};

type TradeStats = {
    entryTs: number;
    exitTs: number;
    entryPrice: number;
    exitPrice: number;
    riskUsd: number;
    grossPnlUsd: number;
    rolloverUsd: number;
    netPnlUsd: number;
    rMultiple: number;
    holdMinutes: number;
    partials: number;
    exitReason: string;
    breakevenExit: boolean;
};

type MatrixRunSummary = {
    fixtureId: string;
    fixtureTier: string;
    fixtureTags: string[];
    scenario: MatrixScenario;
    pair: string;
    trades: number;
    closedLegs: number;
    wins: number;
    winRatePct: number;
    avgR: number;
    avgSpreadBps: number;
    avgSlippageBps: number;
    avgHoldMinutes: number;
    shortHoldTradePct: number;
    partialTradePct: number;
    breakevenExitPct: number;
    totalRolloverUsd: number;
    returnPct: number;
    maxDrawdownPct: number;
    exitsByReason: Record<string, number>;
    exitsByReasonPct: Record<string, number>;
};

type FixtureSummary = {
    fixtureId: string;
    tier: string;
    tags: string[];
    scenarioRuns: number;
    scenariosWithTrades: number;
    scenariosWithoutTrades: number;
    totalTrades: number;
    avgReturnPct: number;
    avgAvgR: number;
    avgSpreadBps: number;
    avgSlippageBps: number;
    bestScenarioId: string | null;
    worstScenarioId: string | null;
    bestScenarioWithTradesId: string | null;
    worstScenarioWithTradesId: string | null;
};

type ScenarioSummary = {
    scenarioId: string;
    spreadFactor: number;
    slippageFactor: number;
    shockProfile: ShockProfile;
    fixtureRuns: number;
    fixturesWithTrades: number;
    fixturesWithoutTrades: number;
    avgReturnPct: number;
    avgAvgR: number;
    tradeCoveragePct: number;
    medianAvgRTraded: number | null;
    worstTradedAvgR: number | null;
    tailGap: number | null;
    costDragBps: number;
    churnPenaltyPct: number;
    robustnessScore: number;
    bestFixtureId: string | null;
    worstFixtureId: string | null;
    worstTradedFixtureId: string | null;
};

type RobustScenarioPreview = {
    scenarioId: string;
    robustnessScore: number;
    tradeCoveragePct: number;
    medianAvgRTraded: number | null;
    worstTradedAvgR: number | null;
    tailGap: number | null;
};

type RobustnessFrontierRow = {
    scenarioId: string;
    spreadFactor: number;
    slippageFactor: number;
    shockProfile: ShockProfile;
    worstFixtureId: string | null;
    robustnessScore: number;
    tradeCoveragePct: number;
    churnPenaltyPct: number;
    tailGap: number | null;
    medianAvgRTraded: number | null;
    worstTradedAvgR: number | null;
};

type RobustnessConstraints = {
    minTradeCoveragePct: number;
    maxChurnPenaltyPct: number;
    maxTailGap: number;
};

const DEFAULT_ROBUSTNESS_CONSTRAINTS: RobustnessConstraints = {
    minTradeCoveragePct: 50,
    maxChurnPenaltyPct: 75,
    maxTailGap: 2.8,
};

type MatrixReport = {
    generatedAtIso: string;
    outDir: string;
    inputPath: string | null;
    fixturesIndexPath: string | null;
    fixtureSelection: string;
    fixtureCount: number;
    scenarios: MatrixScenario[];
    fixtures: Array<{
        id: string;
        tier: string;
        tags: string[];
        description: string;
        purpose: string;
        filePath: string;
        pair: string;
        quoteCount: number;
        entryCount: number;
    }>;
    runs: MatrixRunSummary[];
    fixtureSummaries: FixtureSummary[];
    scenarioSummaries: ScenarioSummary[];
    overview: {
        totalRuns: number;
        scenariosWithTrades: number;
        scenariosWithoutTrades: number;
        bestReturnRun: { fixtureId: string; scenarioId: string; returnPct: number } | null;
        worstReturnRun: { fixtureId: string; scenarioId: string; returnPct: number } | null;
        bestReturnTradedRun: { fixtureId: string; scenarioId: string; returnPct: number } | null;
        worstReturnTradedRun: { fixtureId: string; scenarioId: string; returnPct: number } | null;
        topRobustScenarios: RobustScenarioPreview[];
        topRobustScenariosConstrained: {
            constraints: RobustnessConstraints;
            consideredScenarioCount: number;
            skippedNoTradeScenarioCount: number;
            eligibleScenarioCount: number;
            rejectedScenarioCount: number;
            rejectedByCoverage: number;
            rejectedByChurn: number;
            rejectedByTailGap: number;
            rejectedByMultiple: number;
            scenarios: RobustScenarioPreview[];
        };
        robustnessFrontier: RobustnessFrontierRow[];
        topRobustByShockProfile: Array<{
            shockProfile: ShockProfile;
            scenarioId: string;
            robustnessScore: number;
            tradeCoveragePct: number;
            medianAvgRTraded: number | null;
            worstTradedAvgR: number | null;
            tailGap: number | null;
        }>;
        worstFixtureHeatmap: Array<{
            fixtureId: string;
            worstScenarioCount: number;
            avgWorstTradedAvgR: number | null;
        }>;
        scoreAudit: {
            sampleSize: number;
            topK: number;
            spearman: Array<{
                metric: string;
                direction: 'higher_is_better' | 'lower_is_better';
                rho: number | null;
            }>;
            topKOverlap: Array<{
                metric: string;
                direction: 'higher_is_better' | 'lower_is_better';
                overlapCount: number;
                overlapPct: number;
            }>;
            dominanceFlags: string[];
        };
    };
};

function usage() {
    return [
        'Usage:',
        '  node --import tsx scripts/forex-replay-matrix.ts --fixtures core [options]',
        '  node --import tsx scripts/forex-replay-matrix.ts --input <quotes.json> [options]',
        '',
        'Options:',
        '  --outDir <path>                Output folder (default: /tmp/forex-replay-matrix)',
        '  --fixtures <selector>          all | core | comma-separated fixture ids',
        '  --fixturesIndex <path>         Fixture index path (default: data/replay/fixtures/index.json)',
        '  --input <path>                 Single replay input file (fallback mode)',
        '  --pair <symbol>                Override pair',
        '  --seed <int>                   Base RNG seed (default: 17)',
        '  --spreadFactors <csv>          Spread stress factors (default: 1,1.5,2,3)',
        '  --slippageFactors <csv>        Slippage factors (default: 1,1.5,2)',
        '  --shockProfiles <csv>          Shock profiles: none,occasional,clustered,frequent',
        '  --minCoverage <pct>            Constrained shortlist min trade coverage % (default: 50)',
        '  --maxChurn <pct>               Constrained shortlist max churn penalty % (default: 75)',
        '  --maxTailGap <R>               Constrained shortlist max tail gap in R (default: 2.8)',
        '  --topKConstrained <int>        Constrained shortlist size (default: 5)',
        '  --startingEquity <usd>         Starting equity override',
        '  --notional <usd>               Default notional override',
        '  --atr1h <abs>                  ATR1h absolute override',
        '  --transitionBufferMin <int>    Session transition buffer override',
        '  --rolloverFeeBps <float>       Daily rollover fee override',
        '  --help                         Show help',
    ].join('\n');
}

function parseArgs(argv: string[]): Record<string, string | boolean> {
    const out: Record<string, string | boolean> = {};
    for (let i = 0; i < argv.length; i += 1) {
        const token = argv[i]!;
        if (!token.startsWith('--')) continue;
        const key = token.slice(2);
        const next = argv[i + 1];
        if (!next || next.startsWith('--')) {
            out[key] = true;
            continue;
        }
        out[key] = next;
        i += 1;
    }
    return out;
}

function toNum(value: unknown): number | undefined {
    const n = Number(value);
    return Number.isFinite(n) ? n : undefined;
}

function parseNumberCsv(value: unknown, fallback: number[]): number[] {
    if (typeof value !== 'string' || !value.trim()) return fallback.slice();
    const parsed = value
        .split(',')
        .map((v) => Number(v.trim()))
        .filter((v) => Number.isFinite(v) && v > 0);
    return parsed.length ? parsed : fallback.slice();
}

function parseShockProfiles(value: unknown): ShockProfile[] {
    if (typeof value !== 'string' || !value.trim()) return ['none', 'occasional', 'clustered', 'frequent'];
    const parsed = value
        .split(',')
        .map((v) => v.trim().toLowerCase())
        .filter((v): v is ShockProfile => v === 'none' || v === 'occasional' || v === 'clustered' || v === 'frequent');
    return parsed.length ? parsed : ['none', 'occasional', 'clustered', 'frequent'];
}

function resolveRobustnessConstraints(args: Record<string, string | boolean>): RobustnessConstraints {
    const minCoverage = toNum(args.minCoverage);
    const maxChurn = toNum(args.maxChurn);
    const maxTailGap = toNum(args.maxTailGap);

    return {
        minTradeCoveragePct:
            minCoverage === undefined
                ? DEFAULT_ROBUSTNESS_CONSTRAINTS.minTradeCoveragePct
                : clamp(minCoverage, 0, 100),
        maxChurnPenaltyPct:
            maxChurn === undefined
                ? DEFAULT_ROBUSTNESS_CONSTRAINTS.maxChurnPenaltyPct
                : clamp(maxChurn, 0, 100),
        maxTailGap:
            maxTailGap === undefined
                ? DEFAULT_ROBUSTNESS_CONSTRAINTS.maxTailGap
                : Math.max(0, maxTailGap),
    };
}

function resolveTopKConstrained(args: Record<string, string | boolean>): number {
    const topK = toNum(args.topKConstrained);
    if (topK === undefined) return 5;
    return Math.max(1, Math.floor(topK));
}

function cloneConfig(config: ReplayRuntimeConfig): ReplayRuntimeConfig {
    return {
        ...config,
        reentry: { ...config.reentry },
        spreadStress: { ...config.spreadStress },
        slippage: { ...config.slippage },
        management: { ...config.management },
        rollover: { ...config.rollover },
    };
}

function applyBaseOverrides(config: ReplayRuntimeConfig, args: Record<string, string | boolean>): ReplayRuntimeConfig {
    const next = cloneConfig(config);
    const pair = typeof args.pair === 'string' ? String(args.pair).trim().toUpperCase() : '';
    if (pair) next.pair = pair;

    const startingEquity = toNum(args.startingEquity);
    if (startingEquity !== undefined && startingEquity > 0) next.startingEquityUsd = startingEquity;

    const notional = toNum(args.notional);
    if (notional !== undefined && notional > 0) next.defaultNotionalUsd = notional;

    const atr1h = toNum(args.atr1h);
    if (atr1h !== undefined && atr1h > 0) next.atr1hAbs = atr1h;

    const transitionBufferMin = toNum(args.transitionBufferMin);
    if (transitionBufferMin !== undefined && transitionBufferMin >= 0) {
        next.spreadStress.transitionBufferMinutes = Math.floor(transitionBufferMin);
    }

    const rolloverFeeBps = toNum(args.rolloverFeeBps);
    if (rolloverFeeBps !== undefined && rolloverFeeBps >= 0) {
        next.rollover.dailyFeeBps = rolloverFeeBps;
    }

    return next;
}

function withShockProfile(quotes: ReplayQuote[], profile: ShockProfile): ReplayQuote[] {
    if (profile === 'none') return quotes.map((q) => ({ ...q }));
    if (profile === 'frequent') {
        return quotes.map((quote) => ({
            ...quote,
            shock: true,
        }));
    }
    if (profile === 'clustered') {
        const burstEvery = 10;
        const burstLength = 3;
        const burstOffset = 2;
        return quotes.map((quote, idx) => {
            const rel = idx - burstOffset;
            const inBurst = rel >= 0 && rel % burstEvery < burstLength;
            return {
                ...quote,
                shock: Boolean(quote.shock) || inBurst,
                spreadMultiplier: inBurst
                    ? Math.max(1, Number(quote.spreadMultiplier) || 1) * 1.35
                    : quote.spreadMultiplier,
            };
        });
    }
    const divisor = 4;
    return quotes.map((quote, idx) => ({
        ...quote,
        shock: Boolean(quote.shock) || idx % divisor === 0,
    }));
}

function withScenarioConfig(base: ReplayRuntimeConfig, scenario: MatrixScenario): ReplayRuntimeConfig {
    const next = cloneConfig(base);
    const sf = scenario.spreadFactor;
    const lf = scenario.slippageFactor;

    next.spreadStress.transitionMultiplier *= sf;
    next.spreadStress.rolloverMultiplier *= sf;
    next.spreadStress.mediumEventMultiplier *= sf;
    next.spreadStress.highEventMultiplier *= sf;

    next.slippage.seed = scenario.seed;
    next.slippage.entryBaseBps *= lf;
    next.slippage.exitBaseBps *= lf;
    next.slippage.randomBps *= lf;
    next.slippage.shockBps *= lf;
    next.slippage.mediumEventBps *= lf;
    next.slippage.highEventBps *= lf;

    return next;
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

function mean(values: number[]): number {
    if (!values.length) return 0;
    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function median(values: number[]): number {
    if (!values.length) return 0;
    const sorted = values.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 1) return sorted[mid]!;
    return (sorted[mid - 1]! + sorted[mid]!) / 2;
}

function toPctMap(counts: Record<string, number>, total: number): Record<string, number> {
    const out: Record<string, number> = {};
    for (const [key, count] of Object.entries(counts)) {
        out[key] = total > 0 ? (count / total) * 100 : 0;
    }
    return out;
}

function summarizeFills(result: ReplayResult, quotes: ReplayQuote[], config: ReplayRuntimeConfig): FillStats {
    const stressedByTs = new Map<number, ReturnType<typeof applySpreadStress>>();
    for (const quote of quotes) {
        stressedByTs.set(quote.ts, applySpreadStress(quote, config.spreadStress));
    }

    const spreadBps: number[] = [];
    const slippageBps: number[] = [];

    for (const row of result.ledger) {
        if (row.kind !== 'ENTRY' && row.kind !== 'PARTIAL_EXIT' && row.kind !== 'EXIT') continue;
        if (!(row.side && Number.isFinite(row.price as number) && (row.price as number) > 0)) continue;
        const stressed = stressedByTs.get(row.ts);
        if (!stressed) continue;
        const reference = row.side === 'BUY' ? stressed.ask : stressed.bid;
        if (!(Number.isFinite(reference) && reference > 0)) continue;
        spreadBps.push(stressed.mid > 0 ? (stressed.spreadAbs / stressed.mid) * 10_000 : 0);
        slippageBps.push((Math.abs(Number(row.price) - reference) / reference) * 10_000);
    }

    return {
        avgSpreadBps: mean(spreadBps),
        avgSlippageBps: mean(slippageBps),
        fills: spreadBps.length,
    };
}

function summarizeTrades(result: ReplayResult): {
    trades: TradeStats[];
    exitsByReason: Record<string, number>;
} {
    const entryStopByTs = new Map<number, number>();
    for (const event of result.timeline) {
        if (event.type !== 'ENTRY_OPENED') continue;
        const stop = Number(event.details?.stopPrice);
        if (Number.isFinite(stop) && stop > 0) {
            entryStopByTs.set(event.ts, stop);
        }
    }

    const trades: TradeStats[] = [];
    const exitsByReason: Record<string, number> = {};
    let active: {
        entryTs: number;
        entryPrice: number;
        riskUsd: number;
        grossPnlUsd: number;
        rolloverUsd: number;
        partials: number;
    } | null = null;

    for (const row of result.ledger) {
        if (row.kind === 'ENTRY') {
            const entryPrice = Number(row.price);
            const units = Number(row.units);
            const stop = Number(entryStopByTs.get(row.ts));
            const riskAbs = Number.isFinite(stop) && stop > 0 ? Math.abs(entryPrice - stop) : NaN;
            const riskUsd = Number.isFinite(riskAbs) && Number.isFinite(units) ? riskAbs * units : NaN;
            active = {
                entryTs: row.ts,
                entryPrice,
                riskUsd: Number.isFinite(riskUsd) && riskUsd > 0 ? riskUsd : NaN,
                grossPnlUsd: 0,
                rolloverUsd: 0,
                partials: 0,
            };
            continue;
        }

        if (!active) continue;

        if (row.kind === 'ROLLOVER_FEE') {
            active.rolloverUsd += Number(row.feeUsd) || 0;
            continue;
        }

        if (row.kind === 'PARTIAL_EXIT') {
            active.grossPnlUsd += Number(row.pnlUsd) || 0;
            active.partials += 1;
            continue;
        }

        if (row.kind === 'EXIT') {
            active.grossPnlUsd += Number(row.pnlUsd) || 0;
            const netPnlUsd = active.grossPnlUsd - active.rolloverUsd;
            const rMultiple = Number.isFinite(active.riskUsd) && active.riskUsd > 0 ? netPnlUsd / active.riskUsd : 0;
            const holdMinutes = Math.max(0, (row.ts - active.entryTs) / 60_000);
            const reason = String(row.reasonCodes?.[0] || 'UNKNOWN').trim().toUpperCase() || 'UNKNOWN';
            exitsByReason[reason] = (exitsByReason[reason] || 0) + 1;

            const trade: TradeStats = {
                entryTs: active.entryTs,
                exitTs: row.ts,
                entryPrice: active.entryPrice,
                exitPrice: Number(row.price) || 0,
                riskUsd: Number.isFinite(active.riskUsd) && active.riskUsd > 0 ? active.riskUsd : 0,
                grossPnlUsd: active.grossPnlUsd,
                rolloverUsd: active.rolloverUsd,
                netPnlUsd,
                rMultiple,
                holdMinutes,
                partials: active.partials,
                exitReason: reason,
                breakevenExit:
                    Number.isFinite(active.riskUsd) && active.riskUsd > 0
                        ? Math.abs(netPnlUsd) <= active.riskUsd * 0.05
                        : Math.abs(netPnlUsd) <= 0.01,
            };
            trades.push(trade);
            active = null;
        }
    }

    return { trades, exitsByReason };
}

function summarizeRun(params: {
    fixture: LoadedFixture;
    result: ReplayResult;
    scenario: MatrixScenario;
    quotes: ReplayQuote[];
    config: ReplayRuntimeConfig;
}): MatrixRunSummary {
    const { fixture, result, scenario, quotes, config } = params;
    const fillStats = summarizeFills(result, quotes, config);
    const { trades, exitsByReason } = summarizeTrades(result);

    const wins = trades.filter((trade) => trade.netPnlUsd > 0).length;
    const avgR = mean(trades.map((trade) => trade.rMultiple));
    const avgHoldMinutes = mean(trades.map((trade) => trade.holdMinutes));
    const shortHoldTradePct = trades.length ? (trades.filter((trade) => trade.holdMinutes <= 60).length / trades.length) * 100 : 0;
    const partialTradePct = trades.length ? (trades.filter((trade) => trade.partials > 0).length / trades.length) * 100 : 0;
    const breakevenExitPct = trades.length ? (trades.filter((trade) => trade.breakevenExit).length / trades.length) * 100 : 0;

    return {
        fixtureId: fixture.id,
        fixtureTier: fixture.tier,
        fixtureTags: fixture.tags,
        scenario,
        pair: result.summary.pair,
        trades: trades.length,
        closedLegs: result.summary.closedLegs,
        wins,
        winRatePct: trades.length ? (wins / trades.length) * 100 : 0,
        avgR,
        avgSpreadBps: fillStats.avgSpreadBps,
        avgSlippageBps: fillStats.avgSlippageBps,
        avgHoldMinutes,
        shortHoldTradePct,
        partialTradePct,
        breakevenExitPct,
        totalRolloverUsd: result.summary.rolloverFeesUsd,
        returnPct: result.summary.returnPct,
        maxDrawdownPct: result.summary.maxDrawdownPct,
        exitsByReason,
        exitsByReasonPct: toPctMap(exitsByReason, trades.length),
    };
}

function toCsv(rows: MatrixRunSummary[]): string {
    const header = [
        'fixtureId',
        'fixtureTier',
        'scenarioId',
        'spreadFactor',
        'slippageFactor',
        'shockProfile',
        'seed',
        'pair',
        'trades',
        'winRatePct',
        'avgR',
        'avgSpreadBps',
        'avgSlippageBps',
        'avgHoldMinutes',
        'shortHoldTradePct',
        'partialTradePct',
        'breakevenExitPct',
        'totalRolloverUsd',
        'returnPct',
        'maxDrawdownPct',
        'closedLegs',
    ].join(',');

    const lines = rows.map((row) =>
        [
            row.fixtureId,
            row.fixtureTier,
            row.scenario.id,
            row.scenario.spreadFactor,
            row.scenario.slippageFactor,
            row.scenario.shockProfile,
            row.scenario.seed,
            row.pair,
            row.trades,
            row.winRatePct.toFixed(6),
            row.avgR.toFixed(6),
            row.avgSpreadBps.toFixed(6),
            row.avgSlippageBps.toFixed(6),
            row.avgHoldMinutes.toFixed(6),
            row.shortHoldTradePct.toFixed(6),
            row.partialTradePct.toFixed(6),
            row.breakevenExitPct.toFixed(6),
            row.totalRolloverUsd.toFixed(6),
            row.returnPct.toFixed(6),
            row.maxDrawdownPct.toFixed(6),
            row.closedLegs,
        ].join(','),
    );
    return [header, ...lines].join('\n');
}

function toFrontierCsv(rows: RobustnessFrontierRow[]): string {
    const header = [
        'scenarioId',
        'spreadFactor',
        'slippageFactor',
        'shockProfile',
        'worstFixtureId',
        'robustnessScore',
        'tradeCoveragePct',
        'churnPenaltyPct',
        'tailGap',
        'medianAvgRTraded',
        'worstTradedAvgR',
    ].join(',');
    const lines = rows.map((row) =>
        [
            row.scenarioId,
            row.spreadFactor,
            row.slippageFactor,
            row.shockProfile,
            row.worstFixtureId || '',
            row.robustnessScore.toFixed(6),
            row.tradeCoveragePct.toFixed(6),
            row.churnPenaltyPct.toFixed(6),
            row.tailGap === null ? '' : row.tailGap.toFixed(6),
            row.medianAvgRTraded === null ? '' : row.medianAvgRTraded.toFixed(6),
            row.worstTradedAvgR === null ? '' : row.worstTradedAvgR.toFixed(6),
        ].join(','),
    );
    return [header, ...lines].join('\n');
}

function scenarioId(spreadFactor: number, slippageFactor: number, shockProfile: ShockProfile): string {
    return `spread_${spreadFactor.toFixed(2)}__slip_${slippageFactor.toFixed(2)}__shock_${shockProfile}`;
}

function normalizeStringArray(value: unknown): string[] {
    if (!Array.isArray(value)) return [];
    return value.map((v) => String(v || '').trim()).filter((v) => v.length > 0);
}

async function loadFixtureManifest(indexPath: string): Promise<FixtureManifest> {
    const raw = await readFile(indexPath, 'utf8');
    const parsed = JSON.parse(raw) as FixtureManifest;
    if (!parsed || !Array.isArray(parsed.fixtures) || parsed.fixtures.length === 0) {
        throw new Error(`Fixture index is empty: ${indexPath}`);
    }
    return parsed;
}

function resolveFixtureIds(params: {
    manifest: FixtureManifest;
    selector: string;
}): string[] {
    const entries = params.manifest.fixtures;
    const selector = params.selector.trim().toLowerCase();

    if (!selector || selector === 'core') {
        const core = entries.filter((entry) => String(entry.tier || '').toLowerCase() === 'core').map((entry) => entry.id);
        return core.length ? core : entries.map((entry) => entry.id);
    }

    if (selector === 'all') {
        return entries.map((entry) => entry.id);
    }

    const wanted = selector
        .split(',')
        .map((id) => id.trim())
        .filter((id) => id.length > 0);
    return Array.from(new Set(wanted));
}

async function loadFixtures(params: {
    indexPath: string;
    selector: string;
}): Promise<LoadedFixture[]> {
    const indexPath = path.resolve(params.indexPath);
    const indexDir = path.dirname(indexPath);
    const manifest = await loadFixtureManifest(indexPath);
    const wantedIds = resolveFixtureIds({ manifest, selector: params.selector });
    const byId = new Map(manifest.fixtures.map((entry) => [entry.id, entry]));

    const fixtures: LoadedFixture[] = [];
    for (const id of wantedIds) {
        const entry = byId.get(id);
        if (!entry) {
            const known = manifest.fixtures.map((row) => row.id).join(', ');
            throw new Error(`Unknown fixture id "${id}". Known fixtures: ${known}`);
        }
        const filePath = path.resolve(indexDir, entry.file);
        const raw = await readFile(filePath, 'utf8');
        const input = normalizeReplayInput(JSON.parse(raw) as ReplayInputFile);
        fixtures.push({
            id: entry.id,
            filePath,
            tier: String(entry.tier || 'core').toLowerCase(),
            tags: normalizeStringArray(entry.tags).map((tag) => tag.toLowerCase()),
            description: String(entry.description || '').trim(),
            purpose: String(entry.purpose || '').trim(),
            input,
        });
    }

    return fixtures;
}

async function loadSingleInputFixture(inputPath: string): Promise<LoadedFixture> {
    const resolved = path.resolve(inputPath);
    const raw = await readFile(resolved, 'utf8');
    const input = normalizeReplayInput(JSON.parse(raw) as ReplayInputFile);
    return {
        id: path.basename(resolved).replace(/\.[^.]+$/, ''),
        filePath: resolved,
        tier: 'ad_hoc',
        tags: ['ad-hoc'],
        description: 'Single input replay file',
        purpose: 'ad-hoc',
        input,
    };
}

function summarizeFixtures(fixtures: LoadedFixture[], runs: MatrixRunSummary[]): FixtureSummary[] {
    return fixtures.map((fixture) => {
        const rows = runs.filter((run) => run.fixtureId === fixture.id);
        const withTrades = rows.filter((run) => run.trades > 0);
        const byReturn = rows.slice().sort((a, b) => b.returnPct - a.returnPct);
        const byReturnTraded = withTrades.slice().sort((a, b) => b.returnPct - a.returnPct);

        return {
            fixtureId: fixture.id,
            tier: fixture.tier,
            tags: fixture.tags,
            scenarioRuns: rows.length,
            scenariosWithTrades: withTrades.length,
            scenariosWithoutTrades: rows.length - withTrades.length,
            totalTrades: rows.reduce((sum, row) => sum + row.trades, 0),
            avgReturnPct: mean(rows.map((row) => row.returnPct)),
            avgAvgR: mean(rows.map((row) => row.avgR)),
            avgSpreadBps: mean(rows.map((row) => row.avgSpreadBps)),
            avgSlippageBps: mean(rows.map((row) => row.avgSlippageBps)),
            bestScenarioId: byReturn[0]?.scenario.id || null,
            worstScenarioId: byReturn[byReturn.length - 1]?.scenario.id || null,
            bestScenarioWithTradesId: byReturnTraded[0]?.scenario.id || null,
            worstScenarioWithTradesId: byReturnTraded[byReturnTraded.length - 1]?.scenario.id || null,
        };
    });
}

function percentileScores(
    values: Array<number | null>,
    direction: 'higher_is_better' | 'lower_is_better',
): number[] {
    const scores = new Array<number>(values.length).fill(0);
    const finite = values
        .map((value, idx) => ({ idx, value }))
        .filter((row): row is { idx: number; value: number } => Number.isFinite(row.value as number));

    const n = finite.length;
    if (n === 0) return scores;
    if (n === 1) {
        scores[finite[0]!.idx] = 100;
        return scores;
    }

    const ranks = rankWithTies(finite.map((row) => row.value));
    for (let i = 0; i < finite.length; i += 1) {
        const { idx } = finite[i]!;
        const rank = ranks[i]!;
        const higherPct = ((rank - 1) / (n - 1)) * 100;
        const lowerPct = ((n - rank) / (n - 1)) * 100;
        scores[idx] = direction === 'higher_is_better' ? higherPct : lowerPct;
    }
    return scores;
}

function summarizeScenarios(scenarios: MatrixScenario[], runs: MatrixRunSummary[]): ScenarioSummary[] {
    const baseSummaries = scenarios.map((scenario) => {
        const rows = runs.filter((run) => run.scenario.id === scenario.id);
        const withTrades = rows.filter((run) => run.trades > 0);
        const byReturn = rows.slice().sort((a, b) => b.returnPct - a.returnPct);
        const byReturnTraded = withTrades.slice().sort((a, b) => a.returnPct - b.returnPct);

        const tradeCoveragePct = rows.length > 0 ? (withTrades.length / rows.length) * 100 : 0;
        const tradedAvgRs = withTrades.map((row) => row.avgR);
        const medianAvgRTraded = withTrades.length ? median(tradedAvgRs) : null;
        const worstTradedAvgR = withTrades.length ? Math.min(...tradedAvgRs) : null;
        const tailGap =
            withTrades.length && medianAvgRTraded !== null && worstTradedAvgR !== null
                ? medianAvgRTraded - worstTradedAvgR
                : null;
        const costDragBps = withTrades.length ? mean(withTrades.map((row) => row.avgSpreadBps + row.avgSlippageBps)) : 0;
        const churnPenaltyPct = withTrades.length
            ? mean(withTrades.map((row) => row.breakevenExitPct + row.partialTradePct + row.shortHoldTradePct))
            : 0;

        return {
            scenarioId: scenario.id,
            spreadFactor: scenario.spreadFactor,
            slippageFactor: scenario.slippageFactor,
            shockProfile: scenario.shockProfile,
            fixtureRuns: rows.length,
            fixturesWithTrades: withTrades.length,
            fixturesWithoutTrades: rows.length - withTrades.length,
            avgReturnPct: mean(rows.map((row) => row.returnPct)),
            avgAvgR: mean(rows.map((row) => row.avgR)),
            tradeCoveragePct,
            medianAvgRTraded,
            worstTradedAvgR,
            tailGap,
            costDragBps,
            churnPenaltyPct,
            robustnessScore: 0,
            bestFixtureId: byReturn[0]?.fixtureId || null,
            worstFixtureId: byReturn[byReturn.length - 1]?.fixtureId || null,
            worstTradedFixtureId: byReturnTraded[0]?.fixtureId || null,
        };
    });

    const coverageScores = percentileScores(
        baseSummaries.map((row) => row.tradeCoveragePct),
        'higher_is_better',
    );
    const medianScores = percentileScores(
        baseSummaries.map((row) => row.medianAvgRTraded),
        'higher_is_better',
    );
    const worstScores = percentileScores(
        baseSummaries.map((row) => row.worstTradedAvgR),
        'higher_is_better',
    );
    const tailScores = percentileScores(
        baseSummaries.map((row) => row.tailGap),
        'lower_is_better',
    );
    const costScores = percentileScores(
        baseSummaries.map((row) => row.costDragBps),
        'lower_is_better',
    );
    const churnScores = percentileScores(
        baseSummaries.map((row) => row.churnPenaltyPct),
        'lower_is_better',
    );

    const weights = {
        coverage: 0.22,
        median: 0.12,
        worst: 0.22,
        tail: 0.18,
        churn: 0.16,
        cost: 0.10,
    };

    return baseSummaries.map((row, idx) => {
        if (row.fixturesWithTrades <= 0) {
            return {
                ...row,
                robustnessScore: 0,
            };
        }
        const robustnessScore =
            coverageScores[idx]! * weights.coverage +
            medianScores[idx]! * weights.median +
            worstScores[idx]! * weights.worst +
            tailScores[idx]! * weights.tail +
            churnScores[idx]! * weights.churn +
            costScores[idx]! * weights.cost;

        return {
            ...row,
            robustnessScore,
        };
    });
}

function toRobustScenarioPreview(row: ScenarioSummary): RobustScenarioPreview {
    return {
        scenarioId: row.scenarioId,
        robustnessScore: row.robustnessScore,
        tradeCoveragePct: row.tradeCoveragePct,
        medianAvgRTraded: row.medianAvgRTraded,
        worstTradedAvgR: row.worstTradedAvgR,
        tailGap: row.tailGap,
    };
}

function buildTopRobustScenarios(summaries: ScenarioSummary[], limit = 5): RobustScenarioPreview[] {
    return summaries
        .slice()
        .sort((a, b) => b.robustnessScore - a.robustnessScore)
        .slice(0, Math.max(1, limit))
        .map((row) => toRobustScenarioPreview(row));
}

function buildRobustnessFrontier(summaries: ScenarioSummary[]): RobustnessFrontierRow[] {
    return summaries
        .slice()
        .sort((a, b) => b.robustnessScore - a.robustnessScore)
        .map((row) => ({
            scenarioId: row.scenarioId,
            spreadFactor: row.spreadFactor,
            slippageFactor: row.slippageFactor,
            shockProfile: row.shockProfile,
            worstFixtureId: row.worstFixtureId,
            robustnessScore: row.robustnessScore,
            tradeCoveragePct: row.tradeCoveragePct,
            churnPenaltyPct: row.churnPenaltyPct,
            tailGap: row.tailGap,
            medianAvgRTraded: row.medianAvgRTraded,
            worstTradedAvgR: row.worstTradedAvgR,
        }));
}

function topRobustScenariosConstrained(params: {
    summaries: ScenarioSummary[];
    constraints: RobustnessConstraints;
    limit?: number;
}): MatrixReport['overview']['topRobustScenariosConstrained'] {
    const limit = Math.max(1, params.limit ?? 5);
    const constraints = params.constraints;
    const eligible: ScenarioSummary[] = [];
    let consideredScenarioCount = 0;
    let skippedNoTradeScenarioCount = 0;
    let rejectedByCoverage = 0;
    let rejectedByChurn = 0;
    let rejectedByTailGap = 0;
    let rejectedByMultiple = 0;

    for (const row of params.summaries) {
        if (row.fixturesWithTrades <= 0) {
            skippedNoTradeScenarioCount += 1;
            continue;
        }
        consideredScenarioCount += 1;

        const failCoverage = row.tradeCoveragePct < constraints.minTradeCoveragePct;
        const failChurn = row.churnPenaltyPct > constraints.maxChurnPenaltyPct;
        const failTailGap = row.tailGap === null || row.tailGap > constraints.maxTailGap;
        const failCount = Number(failCoverage) + Number(failChurn) + Number(failTailGap);

        if (failCount === 0) {
            eligible.push(row);
            continue;
        }
        if (failCoverage) rejectedByCoverage += 1;
        if (failChurn) rejectedByChurn += 1;
        if (failTailGap) rejectedByTailGap += 1;
        if (failCount > 1) rejectedByMultiple += 1;
    }

    return {
        constraints,
        consideredScenarioCount,
        skippedNoTradeScenarioCount,
        eligibleScenarioCount: eligible.length,
        rejectedScenarioCount: Math.max(0, consideredScenarioCount - eligible.length),
        rejectedByCoverage,
        rejectedByChurn,
        rejectedByTailGap,
        rejectedByMultiple,
        scenarios: eligible
            .slice()
            .sort((a, b) => b.robustnessScore - a.robustnessScore)
            .slice(0, limit)
            .map((row) => toRobustScenarioPreview(row)),
    };
}

function topRobustByShockProfile(summaries: ScenarioSummary[]): Array<{
    shockProfile: ShockProfile;
    scenarioId: string;
    robustnessScore: number;
    tradeCoveragePct: number;
    medianAvgRTraded: number | null;
    worstTradedAvgR: number | null;
    tailGap: number | null;
}> {
    const profiles: ShockProfile[] = ['none', 'occasional', 'clustered', 'frequent'];
    const out: Array<{
        shockProfile: ShockProfile;
        scenarioId: string;
        robustnessScore: number;
        tradeCoveragePct: number;
        medianAvgRTraded: number | null;
        worstTradedAvgR: number | null;
        tailGap: number | null;
    }> = [];

    for (const profile of profiles) {
        const rows = summaries.filter((row) => row.shockProfile === profile);
        if (!rows.length) continue;
        const best = rows.slice().sort((a, b) => b.robustnessScore - a.robustnessScore)[0]!;
        out.push({
            shockProfile: profile,
            scenarioId: best.scenarioId,
            robustnessScore: best.robustnessScore,
            tradeCoveragePct: best.tradeCoveragePct,
            medianAvgRTraded: best.medianAvgRTraded,
            worstTradedAvgR: best.worstTradedAvgR,
            tailGap: best.tailGap,
        });
    }
    return out;
}

function worstFixtureHeatmap(scenarioSummaries: ScenarioSummary[]): Array<{
    fixtureId: string;
    worstScenarioCount: number;
    avgWorstTradedAvgR: number | null;
}> {
    const counts = new Map<string, number>();
    const tradedTailValues = new Map<string, number[]>();

    for (const row of scenarioSummaries) {
        const fixtureId = row.worstFixtureId;
        if (!fixtureId) continue;
        counts.set(fixtureId, (counts.get(fixtureId) || 0) + 1);
        if (row.worstTradedAvgR !== null) {
            const bucket = tradedTailValues.get(fixtureId) || [];
            bucket.push(row.worstTradedAvgR);
            tradedTailValues.set(fixtureId, bucket);
        }
    }

    return Array.from(counts.entries())
        .map(([fixtureId, worstScenarioCount]) => {
            const values = tradedTailValues.get(fixtureId) || [];
            return {
                fixtureId,
                worstScenarioCount,
                avgWorstTradedAvgR: values.length ? mean(values) : null,
            };
        })
        .sort((a, b) => b.worstScenarioCount - a.worstScenarioCount);
}

function rankWithTies(values: number[]): number[] {
    const indexed = values.map((value, idx) => ({ idx, value }));
    indexed.sort((a, b) => a.value - b.value);

    const ranks = new Array<number>(values.length);
    let i = 0;
    while (i < indexed.length) {
        let j = i + 1;
        while (j < indexed.length && indexed[j]!.value === indexed[i]!.value) {
            j += 1;
        }
        const rankStart = i + 1;
        const rankEnd = j;
        const avgRank = (rankStart + rankEnd) / 2;
        for (let k = i; k < j; k += 1) {
            ranks[indexed[k]!.idx] = avgRank;
        }
        i = j;
    }
    return ranks;
}

function pearsonCorrelation(x: number[], y: number[]): number | null {
    if (x.length !== y.length || x.length < 2) return null;
    const meanX = mean(x);
    const meanY = mean(y);
    let num = 0;
    let denX = 0;
    let denY = 0;
    for (let i = 0; i < x.length; i += 1) {
        const dx = x[i]! - meanX;
        const dy = y[i]! - meanY;
        num += dx * dy;
        denX += dx * dx;
        denY += dy * dy;
    }
    if (!(denX > 0 && denY > 0)) return null;
    return num / Math.sqrt(denX * denY);
}

function spearmanRankCorrelation(x: number[], y: number[]): number | null {
    if (x.length !== y.length || x.length < 3) return null;
    const rankX = rankWithTies(x);
    const rankY = rankWithTies(y);
    return pearsonCorrelation(rankX, rankY);
}

function topKScenarioIds(params: {
    scenarios: ScenarioSummary[];
    k: number;
    direction: 'higher_is_better' | 'lower_is_better';
    getValue: (row: ScenarioSummary) => number | null;
}): string[] {
    const scored = params.scenarios
        .map((row) => ({ id: row.scenarioId, value: params.getValue(row) }))
        .filter((row) => Number.isFinite(row.value as number))
        .sort((a, b) =>
            params.direction === 'higher_is_better'
                ? Number(b.value) - Number(a.value)
                : Number(a.value) - Number(b.value),
        );

    return scored.slice(0, Math.max(1, params.k)).map((row) => row.id);
}

function computeScoreAudit(scenarioSummaries: ScenarioSummary[]): MatrixReport['overview']['scoreAudit'] {
    const topK = Math.min(10, Math.max(1, scenarioSummaries.length));
    const dominantRhoThreshold = 0.92;
    const dominantOverlapPctThreshold = 80;
    const robustTop = topKScenarioIds({
        scenarios: scenarioSummaries,
        k: topK,
        direction: 'higher_is_better',
        getValue: (row) => row.robustnessScore,
    });
    const robustTopSet = new Set(robustTop);

    const metricSpecs: Array<{
        metric: string;
        direction: 'higher_is_better' | 'lower_is_better';
        getValue: (row: ScenarioSummary) => number | null;
    }> = [
        {
            metric: 'tradeCoveragePct',
            direction: 'higher_is_better',
            getValue: (row) => row.tradeCoveragePct,
        },
        {
            metric: 'medianAvgRTraded',
            direction: 'higher_is_better',
            getValue: (row) => row.medianAvgRTraded,
        },
        {
            metric: 'worstTradedAvgR',
            direction: 'higher_is_better',
            getValue: (row) => row.worstTradedAvgR,
        },
        {
            metric: 'tailGap',
            direction: 'lower_is_better',
            getValue: (row) => row.tailGap,
        },
        {
            metric: 'costDragBps',
            direction: 'lower_is_better',
            getValue: (row) => row.costDragBps,
        },
        {
            metric: 'churnPenaltyPct',
            direction: 'lower_is_better',
            getValue: (row) => row.churnPenaltyPct,
        },
    ];

    const spearman: MatrixReport['overview']['scoreAudit']['spearman'] = [];
    const topKOverlap: MatrixReport['overview']['scoreAudit']['topKOverlap'] = [];
    const dominanceFlags: string[] = [];

    for (const spec of metricSpecs) {
        const rows = scenarioSummaries
            .map((row) => ({
                robust: row.robustnessScore,
                metric: spec.getValue(row),
            }))
            .filter((row) => Number.isFinite(row.robust) && Number.isFinite(row.metric as number));

        let rho: number | null = null;
        if (rows.length >= 3) {
            const robustness = rows.map((row) => Number(row.robust));
            const orientedMetric = rows.map((row) =>
                spec.direction === 'higher_is_better' ? Number(row.metric) : -Number(row.metric),
            );
            rho = spearmanRankCorrelation(robustness, orientedMetric);
        }
        spearman.push({
            metric: spec.metric,
            direction: spec.direction,
            rho,
        });

        const metricTop = topKScenarioIds({
            scenarios: scenarioSummaries,
            k: topK,
            direction: spec.direction,
            getValue: spec.getValue,
        });
        const overlapCount = metricTop.reduce((sum, id) => sum + (robustTopSet.has(id) ? 1 : 0), 0);
        const overlapPct = topK > 0 ? (overlapCount / topK) * 100 : 0;
        topKOverlap.push({
            metric: spec.metric,
            direction: spec.direction,
            overlapCount,
            overlapPct,
        });

        if (
            rho !== null &&
            Math.abs(rho) > dominantRhoThreshold &&
            overlapPct > dominantOverlapPctThreshold
        ) {
            dominanceFlags.push(
                `DOMINANT_COMBINED_${spec.metric.toUpperCase()}_RHO_${rho.toFixed(3)}_TOPK_${overlapPct.toFixed(1)}PCT`,
            );
        }
    }

    return {
        sampleSize: scenarioSummaries.length,
        topK,
        spearman,
        topKOverlap,
        dominanceFlags: Array.from(new Set(dominanceFlags)),
    };
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    if (args.help) {
        console.log(usage());
        return;
    }

    const outDir = typeof args.outDir === 'string' ? String(args.outDir) : '/tmp/forex-replay-matrix';
    const fixtureSelector = typeof args.fixtures === 'string' ? String(args.fixtures) : '';
    const fixturesIndexPath =
        typeof args.fixturesIndex === 'string' ? String(args.fixturesIndex) : 'data/replay/fixtures/index.json';
    const inputPath = typeof args.input === 'string' ? String(args.input) : '';

    const useFixtures = fixtureSelector.trim().length > 0 || !inputPath.trim();
    let fixtureSelectionUsed = fixtureSelector.trim() || 'core';
    let fixtures: LoadedFixture[] = [];

    if (useFixtures) {
        fixtures = await loadFixtures({
            indexPath: fixturesIndexPath,
            selector: fixtureSelectionUsed,
        });
    } else {
        fixtureSelectionUsed = 'input';
        fixtures = [await loadSingleInputFixture(inputPath)];
    }

    const baseSeed = Math.floor(toNum(args.seed) ?? 17);
    const spreadFactors = parseNumberCsv(args.spreadFactors, [1, 1.5, 2, 3]);
    const slippageFactors = parseNumberCsv(args.slippageFactors, [1, 1.5, 2]);
    const shockProfiles = parseShockProfiles(args.shockProfiles);
    const constrainedConstraints = resolveRobustnessConstraints(args);
    const topKConstrained = resolveTopKConstrained(args);

    const scenarios: MatrixScenario[] = [];
    let scenarioIndex = 0;
    for (const spreadFactor of spreadFactors) {
        for (const slippageFactor of slippageFactors) {
            for (const shockProfile of shockProfiles) {
                scenarioIndex += 1;
                scenarios.push({
                    id: scenarioId(spreadFactor, slippageFactor, shockProfile),
                    spreadFactor,
                    slippageFactor,
                    shockProfile,
                    seed: baseSeed + scenarioIndex,
                });
            }
        }
    }

    const runs: MatrixRunSummary[] = [];
    for (const fixture of fixtures) {
        const baseConfig = applyBaseOverrides(defaultReplayConfig(fixture.input.pair), args);
        for (const scenario of scenarios) {
            const quotes = withShockProfile(fixture.input.quotes, scenario.shockProfile);
            const config = withScenarioConfig(baseConfig, scenario);
            const result = runReplay({
                quotes,
                entries: fixture.input.entries,
                config,
            });
            runs.push(
                summarizeRun({
                    fixture,
                    result,
                    scenario,
                    quotes,
                    config,
                }),
            );
        }
    }

    const fixtureSummaries = summarizeFixtures(fixtures, runs);
    const scenarioSummaries = summarizeScenarios(scenarios, runs);
    const byReturn = runs.slice().sort((a, b) => b.returnPct - a.returnPct);
    const withTrades = runs.filter((run) => run.trades > 0);
    const byReturnTraded = withTrades.slice().sort((a, b) => b.returnPct - a.returnPct);
    const topRobustScenarios = buildTopRobustScenarios(scenarioSummaries, 5);
    const constrainedRobustScenarios = topRobustScenariosConstrained({
        summaries: scenarioSummaries,
        constraints: constrainedConstraints,
        limit: topKConstrained,
    });
    const robustnessFrontier = buildRobustnessFrontier(scenarioSummaries);
    const robustByProfile = topRobustByShockProfile(scenarioSummaries);
    const fixtureHeatmap = worstFixtureHeatmap(scenarioSummaries);
    const scoreAudit = computeScoreAudit(scenarioSummaries);

    const report: MatrixReport = {
        generatedAtIso: new Date().toISOString(),
        outDir: path.resolve(outDir),
        inputPath: useFixtures ? null : path.resolve(inputPath),
        fixturesIndexPath: useFixtures ? path.resolve(fixturesIndexPath) : null,
        fixtureSelection: fixtureSelectionUsed,
        fixtureCount: fixtures.length,
        scenarios,
        fixtures: fixtures.map((fixture) => ({
            id: fixture.id,
            tier: fixture.tier,
            tags: fixture.tags,
            description: fixture.description,
            purpose: fixture.purpose,
            filePath: fixture.filePath,
            pair: fixture.input.pair,
            quoteCount: fixture.input.quotes.length,
            entryCount: fixture.input.entries.length,
        })),
        runs,
        fixtureSummaries,
        scenarioSummaries,
        overview: {
            totalRuns: runs.length,
            scenariosWithTrades: withTrades.length,
            scenariosWithoutTrades: runs.length - withTrades.length,
            bestReturnRun: byReturn[0]
                ? {
                    fixtureId: byReturn[0].fixtureId,
                    scenarioId: byReturn[0].scenario.id,
                    returnPct: byReturn[0].returnPct,
                }
                : null,
            worstReturnRun: byReturn[byReturn.length - 1]
                ? {
                    fixtureId: byReturn[byReturn.length - 1]!.fixtureId,
                    scenarioId: byReturn[byReturn.length - 1]!.scenario.id,
                    returnPct: byReturn[byReturn.length - 1]!.returnPct,
                }
                : null,
            bestReturnTradedRun: byReturnTraded[0]
                ? {
                    fixtureId: byReturnTraded[0].fixtureId,
                    scenarioId: byReturnTraded[0].scenario.id,
                    returnPct: byReturnTraded[0].returnPct,
                }
                : null,
            worstReturnTradedRun: byReturnTraded[byReturnTraded.length - 1]
                ? {
                    fixtureId: byReturnTraded[byReturnTraded.length - 1]!.fixtureId,
                    scenarioId: byReturnTraded[byReturnTraded.length - 1]!.scenario.id,
                    returnPct: byReturnTraded[byReturnTraded.length - 1]!.returnPct,
                }
                : null,
            topRobustScenarios,
            topRobustScenariosConstrained: constrainedRobustScenarios,
            robustnessFrontier,
            topRobustByShockProfile: robustByProfile,
            worstFixtureHeatmap: fixtureHeatmap,
            scoreAudit,
        },
    };

    await mkdir(outDir, { recursive: true });
    await Promise.all([
        writeFile(path.join(outDir, 'matrix.summary.json'), JSON.stringify(report, null, 2), 'utf8'),
        writeFile(path.join(outDir, 'matrix.summary.csv'), toCsv(runs), 'utf8'),
        writeFile(path.join(outDir, 'matrix.frontier.csv'), toFrontierCsv(robustnessFrontier), 'utf8'),
    ]);

    console.log(`Matrix complete`);
    console.log(`Fixtures: ${fixtures.length} | Scenarios: ${scenarios.length} | Runs: ${runs.length}`);
    console.log(`Runs with trades: ${withTrades.length}`);
    const noTradeFixtures = fixtureSummaries.filter((row) => row.scenariosWithoutTrades > 0).length;
    console.log(`Fixtures with at least one no-trade scenario: ${noTradeFixtures}/${fixtureSummaries.length}`);
    if (report.overview.bestReturnRun) {
        const best = report.overview.bestReturnRun;
        console.log(`Best return run: ${best.returnPct.toFixed(3)}% (${best.fixtureId} / ${best.scenarioId})`);
    }
    if (report.overview.worstReturnRun) {
        const worst = report.overview.worstReturnRun;
        console.log(`Worst return run: ${worst.returnPct.toFixed(3)}% (${worst.fixtureId} / ${worst.scenarioId})`);
    }
    if (report.overview.bestReturnTradedRun) {
        const best = report.overview.bestReturnTradedRun;
        console.log(`Best traded run: ${best.returnPct.toFixed(3)}% (${best.fixtureId} / ${best.scenarioId})`);
    }
    if (report.overview.worstReturnTradedRun) {
        const worst = report.overview.worstReturnTradedRun;
        console.log(`Worst traded run: ${worst.returnPct.toFixed(3)}% (${worst.fixtureId} / ${worst.scenarioId})`);
    }
    if (report.overview.topRobustScenarios.length) {
        const preview = report.overview.topRobustScenarios
            .slice(0, 3)
            .map((row) => `${row.scenarioId}:${row.robustnessScore.toFixed(1)}`)
            .join(' | ');
        console.log(`Top robustness scenarios: ${preview}`);
    }
    if (report.overview.topRobustScenariosConstrained.scenarios.length) {
        const constrained = report.overview.topRobustScenariosConstrained.scenarios
            .slice(0, 3)
            .map((row) => `${row.scenarioId}:${row.robustnessScore.toFixed(1)}`)
            .join(' | ');
        console.log(`Top constrained robustness scenarios: ${constrained}`);
    } else {
        const c = report.overview.topRobustScenariosConstrained.constraints;
        console.log(
            `Top constrained robustness scenarios: none (coverage>=${c.minTradeCoveragePct}, churn<=${c.maxChurnPenaltyPct}, tailGap<=${c.maxTailGap})`,
        );
    }
    const constrainedStats = report.overview.topRobustScenariosConstrained;
    console.log(
        `Constrained filter stats: eligible ${constrainedStats.eligibleScenarioCount}/${constrainedStats.consideredScenarioCount} | ` +
            `rejected total ${constrainedStats.rejectedScenarioCount} ` +
            `(coverage ${constrainedStats.rejectedByCoverage}, churn ${constrainedStats.rejectedByChurn}, ` +
            `tail ${constrainedStats.rejectedByTailGap}, multi ${constrainedStats.rejectedByMultiple}) | ` +
            `skipped no-trade ${constrainedStats.skippedNoTradeScenarioCount}`,
    );
    if (report.overview.topRobustByShockProfile.length) {
        const byProfile = report.overview.topRobustByShockProfile
            .map((row) => `${row.shockProfile}=>${row.scenarioId}:${row.robustnessScore.toFixed(1)}`)
            .join(' | ');
        console.log(`Top robustness by shock profile: ${byProfile}`);
    }
    if (report.overview.worstFixtureHeatmap.length) {
        const heatmap = report.overview.worstFixtureHeatmap
            .slice(0, 3)
            .map((row) => `${row.fixtureId}:${row.worstScenarioCount}`)
            .join(' | ');
        console.log(`Worst fixture heatmap (top): ${heatmap}`);
    }
    const audit = report.overview.scoreAudit;
    if (audit.spearman.length) {
        const spearmanPreview = audit.spearman
            .map((row) => `${row.metric}:${row.rho === null ? 'n/a' : row.rho.toFixed(2)}`)
            .join(' | ');
        console.log(`Score audit spearman: ${spearmanPreview}`);
    }
    if (audit.dominanceFlags.length) {
        console.log(`Score audit dominance flags: ${audit.dominanceFlags.join(' | ')}`);
    }
    console.log(`Artifacts written to: ${path.resolve(outDir)}`);
}

main().catch((err) => {
    console.error('Matrix replay failed:', err);
    process.exitCode = 1;
});
