#!/usr/bin/env node
import { mkdir, readFile, readdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { loadScalpCandleHistory, type CandleHistoryBackend } from '../lib/scalp/candleHistory';
import { resolveScalpDeployment, normalizeScalpTuneId } from '../lib/scalp/deployments';
import { upsertScalpDeploymentRegistryEntry } from '../lib/scalp/deploymentRegistry';
import { pipSizeForScalpSymbol } from '../lib/scalp/marketData';
import { defaultScalpReplayConfig, runScalpReplay } from '../lib/scalp/replay/harness';
import { toScalpBacktestLeaderboardEntry } from '../lib/scalp/replay/results';
import type { ScalpReplayCandle, ScalpReplayRuntimeConfig } from '../lib/scalp/replay/types';
import { getScalpStrategyById, listScalpStrategies, normalizeScalpStrategyId } from '../lib/scalp/strategies/registry';
import { applySymbolGuardRiskDefaultsToReplayRuntime } from '../lib/scalp/strategies/guardDefaults';
import { buildScalpConfigOverrideFromEffectiveConfig } from '../lib/scalp/tuning';

type Scenario = {
    id: string;
    tuneId: string;
    strategyId: string;
    executeMinutes: number;
    spreadFactor: number;
    slippagePips: number;
    strategy: Partial<ScalpReplayRuntimeConfig['strategy']>;
};

type ScenarioFileEntry = Partial<Omit<Scenario, 'id' | 'tuneId'>> & { id?: string; tuneId?: string };
type ScenarioFile = { scenarios: ScenarioFileEntry[] };

type DeploymentRunRow = {
    symbol: string;
    scenarioId: string;
    strategyId: string;
    tuneId: string;
    deploymentId: string;
    tuneLabel: string;
    executeMinutes: number;
    spreadFactor: number;
    slippagePips: number;
    trades: number;
    winRatePct: number;
    netR: number;
    expectancyR: number;
    profitFactor: number | null;
    maxDrawdownR: number;
    avgHoldMinutes: number;
    eligible: boolean;
    configOverride: ReturnType<typeof buildScalpConfigOverrideFromEffectiveConfig>;
};

type SymbolLeaderboard = {
    symbol: string;
    runs: DeploymentRunRow[];
    winners: DeploymentRunRow[];
};

const DAY_MS = 24 * 60 * 60 * 1000;
const MIN_CANDLES = 180;

function usage() {
    return [
        'Usage:',
        '  node --import tsx scripts/scalp-deployment-matrix.ts [options]',
        '',
        'Options:',
        '  --symbols <csv>                Symbols to test. Default: symbols available in data/candles-history',
        '  --days <int>                   Lookback window in days (default: 30)',
        '  --historyBackend <file|kv>     Candle history backend (default: file)',
        `  --strategyIds <csv>            Strategy ids (default: ${listScalpStrategies().map((row) => row.id).join(',')})`,
        '  --scenarioFile <path>          Optional scenario JSON with explicit tune definitions',
        '  --executeMinutes <int>         Execute cadence for generated scenarios (default: runtime default)',
        '  --spreadFactors <csv>          Spread factors (default: 1)',
        '  --slippagePips <csv>           Slippage pips (default: runtime default)',
        '  --tpRs <csv>                   Take-profit R values (default: runtime default)',
        '  --riskPcts <csv>               Risk % values (default: runtime default)',
        '  --sweepBufferPips <csv>        Sweep buffer pips (default: runtime default)',
        '  --mssLookbackBars <csv>        MSS lookback bars (default: runtime default)',
        '  --ifvgEntryModes <csv>         first_touch|midline_touch|full_fill',
        '  --minTrades <int>              Eligibility floor for winner persistence (default: 1)',
        '  --maxDrawdownR <num>           Optional max drawdown gate for winners',
        '  --topK <int>                   Top winners per symbol to persist/output (default: 3)',
        '  --persistRegistry              Persist winners into the deployment registry file',
        '  --outDir <path>                Output folder (default: /tmp/scalp-deployment-matrix)',
        '  --help',
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

function toPositiveInt(value: unknown, fallback: number): number {
    const n = toNum(value);
    if (n === undefined || n <= 0) return fallback;
    return Math.max(1, Math.floor(n));
}

function parseNonNegativeCsv(value: unknown, fallback: number[]): number[] {
    if (typeof value !== 'string' || !value.trim()) return fallback.slice();
    const parsed = value
        .split(',')
        .map((row) => Number(row.trim()))
        .filter((row) => Number.isFinite(row) && row >= 0);
    return parsed.length ? parsed : fallback.slice();
}

function parsePositiveCsv(value: unknown, fallback: number[]): number[] {
    if (typeof value !== 'string' || !value.trim()) return fallback.slice();
    const parsed = value
        .split(',')
        .map((row) => Number(row.trim()))
        .filter((row) => Number.isFinite(row) && row > 0);
    return parsed.length ? parsed : fallback.slice();
}

function parsePositiveIntCsv(value: unknown, fallback: number[]): number[] {
    return parsePositiveCsv(value, fallback).map((row) => Math.floor(row));
}

function parseIfvgEntryMode(
    value: unknown,
): ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'] | null {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'first_touch' || normalized === 'midline_touch' || normalized === 'full_fill') return normalized;
    return null;
}

function parseIfvgEntryModesCsv(
    value: unknown,
    fallback: ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'][],
): ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'][] {
    if (typeof value !== 'string' || !value.trim()) return fallback.slice();
    const parsed = value
        .split(',')
        .map((row) => parseIfvgEntryMode(row))
        .filter((row): row is ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'] => Boolean(row));
    return parsed.length ? parsed : fallback.slice();
}

function parseStrategyIdsCsv(value: unknown, fallback: string[]): string[] {
    if (typeof value !== 'string' || !value.trim()) return fallback.slice();
    const parsed = value
        .split(',')
        .map((row) => normalizeScalpStrategyId(row))
        .filter((row): row is string => Boolean(row))
        .map((row) => getScalpStrategyById(row)?.id || null)
        .filter((row): row is string => Boolean(row));
    return parsed.length ? Array.from(new Set(parsed)) : fallback.slice();
}

function parseHistoryBackend(value: unknown): CandleHistoryBackend {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    return normalized === 'kv' ? 'kv' : 'file';
}

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

async function resolveSymbols(args: Record<string, string | boolean>): Promise<string[]> {
    if (typeof args.symbols === 'string' && args.symbols.trim()) {
        return Array.from(
            new Set(
                args.symbols
                    .split(',')
                    .map((row) => normalizeSymbol(row))
                    .filter((row) => row.length > 0),
            ),
        );
    }
    const root = path.resolve(process.cwd(), 'data/candles-history');
    try {
        const entries = await readdir(root, { withFileTypes: true });
        return entries
            .filter((entry) => entry.isDirectory())
            .map((entry) => normalizeSymbol(entry.name))
            .filter((entry) => entry.length > 0)
            .sort();
    } catch {
        return [];
    }
}

function toReplayCandles(
    rows: Array<[number, number, number, number, number, number]>,
    spreadPips: number,
): ScalpReplayCandle[] {
    return rows.map((row) => ({
        ts: Number(row[0]),
        open: Number(row[1]),
        high: Number(row[2]),
        low: Number(row[3]),
        close: Number(row[4]),
        volume: Number(row[5] ?? 0),
        spreadPips,
    }));
}

function buildScenarioId(params: {
    strategyId: string;
    spreadFactor: number;
    slippagePips: number;
    tpR: number;
    riskPct: number;
    sweepBufferPips: number;
    mssLookbackBars: number;
    ifvgEntryMode: ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'];
    executeMinutes: number;
}): string {
    return normalizeScalpTuneId(
        [
            params.strategyId,
            `e${params.executeMinutes}`,
            `spr${params.spreadFactor.toFixed(2)}`,
            `slp${params.slippagePips.toFixed(2)}`,
            `tp${params.tpR.toFixed(2)}`,
            `risk${params.riskPct.toFixed(2)}`,
            `swp${params.sweepBufferPips.toFixed(2)}`,
            `mss${params.mssLookbackBars}`,
            params.ifvgEntryMode,
        ].join('_'),
    );
}

function buildScenarios(params: {
    strategyIds: string[];
    executeMinutes: number;
    spreadFactors: number[];
    slippagePips: number[];
    tpRs: number[];
    riskPcts: number[];
    sweepBufferPips: number[];
    mssLookbackBars: number[];
    ifvgEntryModes: ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'][];
}): Scenario[] {
    const out: Scenario[] = [];
    for (const strategyId of params.strategyIds) {
        for (const spreadFactor of params.spreadFactors) {
            for (const slippagePips of params.slippagePips) {
                for (const tpR of params.tpRs) {
                    for (const riskPct of params.riskPcts) {
                        for (const sweepBufferPips of params.sweepBufferPips) {
                            for (const mssLookbackBars of params.mssLookbackBars) {
                                for (const ifvgEntryMode of params.ifvgEntryModes) {
                                    const tuneId = buildScenarioId({
                                        strategyId,
                                        executeMinutes: params.executeMinutes,
                                        spreadFactor,
                                        slippagePips,
                                        tpR,
                                        riskPct,
                                        sweepBufferPips,
                                        mssLookbackBars,
                                        ifvgEntryMode,
                                    });
                                    out.push({
                                        id: tuneId,
                                        tuneId,
                                        strategyId,
                                        executeMinutes: params.executeMinutes,
                                        spreadFactor,
                                        slippagePips,
                                        strategy: {
                                            takeProfitR: tpR,
                                            riskPerTradePct: riskPct,
                                            sweepBufferPips,
                                            mssLookbackBars,
                                            ifvgEntryMode,
                                        },
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    return out;
}

async function loadScenariosFromFile(
    filePath: string,
    fallback: { executeMinutes: number; strategyId: string },
): Promise<Scenario[]> {
    const raw = JSON.parse(await readFile(filePath, 'utf8')) as ScenarioFile;
    const entries = Array.isArray(raw.scenarios) ? raw.scenarios : [];
    if (!entries.length) throw new Error('scenarioFile must include at least one scenario');
    return entries.map((entry, idx) => {
        const strategyId = getScalpStrategyById(normalizeScalpStrategyId(entry.strategyId) || '')?.id || fallback.strategyId;
        const tuneId = normalizeScalpTuneId(entry.tuneId || entry.id || `scenario_${idx + 1}`);
        return {
            id: String(entry.id || tuneId),
            tuneId,
            strategyId,
            executeMinutes: toPositiveInt(entry.executeMinutes, fallback.executeMinutes),
            spreadFactor: Number.isFinite(Number(entry.spreadFactor)) && Number(entry.spreadFactor) > 0 ? Number(entry.spreadFactor) : 1,
            slippagePips: Number.isFinite(Number(entry.slippagePips)) && Number(entry.slippagePips) >= 0 ? Number(entry.slippagePips) : 0,
            strategy: { ...(entry.strategy || {}) },
        };
    });
}

function sortRuns(rows: DeploymentRunRow[]): DeploymentRunRow[] {
    return rows.slice().sort((a, b) => {
        if (b.netR !== a.netR) return b.netR - a.netR;
        const pfA = a.profitFactor ?? -1;
        const pfB = b.profitFactor ?? -1;
        if (pfB !== pfA) return pfB - pfA;
        if (a.maxDrawdownR !== b.maxDrawdownR) return a.maxDrawdownR - b.maxDrawdownR;
        if (b.trades !== a.trades) return b.trades - a.trades;
        return b.winRatePct - a.winRatePct;
    });
}

function toRunsCsv(rows: DeploymentRunRow[]): string {
    const headers = [
        'symbol',
        'scenarioId',
        'strategyId',
        'tuneId',
        'deploymentId',
        'executeMinutes',
        'spreadFactor',
        'slippagePips',
        'trades',
        'winRatePct',
        'netR',
        'expectancyR',
        'profitFactor',
        'maxDrawdownR',
        'avgHoldMinutes',
        'eligible',
    ];
    const lines = [headers.join(',')];
    for (const row of rows) {
        lines.push(
            [
                row.symbol,
                row.scenarioId,
                row.strategyId,
                row.tuneId,
                row.deploymentId,
                row.executeMinutes,
                row.spreadFactor.toFixed(4),
                row.slippagePips.toFixed(4),
                row.trades,
                row.winRatePct.toFixed(4),
                row.netR.toFixed(6),
                row.expectancyR.toFixed(6),
                row.profitFactor === null ? '' : row.profitFactor.toFixed(6),
                row.maxDrawdownR.toFixed(6),
                row.avgHoldMinutes.toFixed(4),
                row.eligible ? '1' : '0',
            ].join(','),
        );
    }
    return `${lines.join('\n')}\n`;
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    if (args.help) {
        console.log(usage());
        return;
    }

    const symbols = await resolveSymbols(args);
    if (!symbols.length) throw new Error('No symbols available for deployment matrix');

    const outDir = path.resolve(typeof args.outDir === 'string' ? String(args.outDir) : '/tmp/scalp-deployment-matrix');
    const days = toPositiveInt(args.days, 30);
    const executeMinutesArg = toPositiveInt(args.executeMinutes, 3);
    const historyBackend = parseHistoryBackend(args.historyBackend);
    const topK = toPositiveInt(args.topK, 3);
    const minTrades = toPositiveInt(args.minTrades, 1);
    const maxDrawdownRGate = toNum(args.maxDrawdownR);
    const persistRegistry = Boolean(args.persistRegistry);

    const seedConfig = defaultScalpReplayConfig(symbols[0]!);
    const strategyIds = parseStrategyIdsCsv(args.strategyIds, listScalpStrategies().map((row) => row.id));
    const scenarios =
        typeof args.scenarioFile === 'string' && args.scenarioFile.trim()
            ? await loadScenariosFromFile(path.resolve(String(args.scenarioFile)), {
                  executeMinutes: executeMinutesArg,
                  strategyId: strategyIds[0] || seedConfig.strategyId,
              })
            : buildScenarios({
                  strategyIds,
                  executeMinutes: executeMinutesArg,
                  spreadFactors: parsePositiveCsv(args.spreadFactors, [1]),
                  slippagePips: parseNonNegativeCsv(args.slippagePips, [seedConfig.slippagePips]),
                  tpRs: parsePositiveCsv(args.tpRs, [seedConfig.strategy.takeProfitR]),
                  riskPcts: parsePositiveCsv(args.riskPcts, [seedConfig.strategy.riskPerTradePct]),
                  sweepBufferPips: parseNonNegativeCsv(args.sweepBufferPips, [seedConfig.strategy.sweepBufferPips]),
                  mssLookbackBars: parsePositiveIntCsv(args.mssLookbackBars, [seedConfig.strategy.mssLookbackBars]),
                  ifvgEntryModes: parseIfvgEntryModesCsv(args.ifvgEntryModes, [seedConfig.strategy.ifvgEntryMode]),
              });
    if (!scenarios.length) throw new Error('No scenarios generated');

    const nowMs = Date.now();
    const fromMs = nowMs - days * DAY_MS;
    const allRuns: DeploymentRunRow[] = [];
    const leaderboards: SymbolLeaderboard[] = [];
    const totalPlannedRuns = symbols.length * scenarios.length;
    let completedRuns = 0;

    console.log(
        `Scalp deployment matrix started | symbols=${symbols.length} scenarios=${scenarios.length} plannedRuns=${totalPlannedRuns} persistRegistry=${persistRegistry}`,
    );

    for (const symbol of symbols) {
        const history = await loadScalpCandleHistory(symbol, '1m', { backend: historyBackend });
        const rows = (history.record?.candles || []).filter((row) => row[0] >= fromMs && row[0] <= nowMs) as Array<
            [number, number, number, number, number, number]
        >;
        if (rows.length < MIN_CANDLES) {
            console.log(`[skip] ${symbol} insufficient_history candles=${rows.length}`);
            continue;
        }
        const baseRuntime = defaultScalpReplayConfig(symbol);
        const candles = toReplayCandles(rows, baseRuntime.defaultSpreadPips);
        const pipSize = pipSizeForScalpSymbol(symbol);
        const symbolRuns: DeploymentRunRow[] = [];

        for (const scenario of scenarios) {
            const deployment = resolveScalpDeployment({
                symbol,
                strategyId: scenario.strategyId,
                tuneId: scenario.tuneId,
            });
            let runtime: ScalpReplayRuntimeConfig = {
                ...baseRuntime,
                symbol,
                strategyId: deployment.strategyId,
                tuneId: deployment.tuneId,
                deploymentId: deployment.deploymentId,
                tuneLabel: deployment.tuneLabel,
                executeMinutes: scenario.executeMinutes,
                spreadFactor: scenario.spreadFactor,
                slippagePips: scenario.slippagePips,
                strategy: {
                    ...baseRuntime.strategy,
                    ...scenario.strategy,
                },
            };
            runtime = applySymbolGuardRiskDefaultsToReplayRuntime(runtime);
            const replay = await runScalpReplay({
                candles,
                pipSize,
                config: runtime,
            });
            const leaderboardEntry = toScalpBacktestLeaderboardEntry(replay);
            const configOverride = buildScalpConfigOverrideFromEffectiveConfig(replay.config, { includeTimeframes: true });
            const eligible =
                leaderboardEntry.trades >= minTrades &&
                (maxDrawdownRGate === undefined || leaderboardEntry.maxDrawdownR <= maxDrawdownRGate);
            symbolRuns.push({
                symbol,
                scenarioId: scenario.id,
                strategyId: leaderboardEntry.strategyId,
                tuneId: leaderboardEntry.tuneId,
                deploymentId: leaderboardEntry.deploymentId,
                tuneLabel: leaderboardEntry.tuneLabel,
                executeMinutes: runtime.executeMinutes,
                spreadFactor: runtime.spreadFactor,
                slippagePips: runtime.slippagePips,
                trades: leaderboardEntry.trades,
                winRatePct: leaderboardEntry.winRatePct,
                netR: leaderboardEntry.netR,
                expectancyR: leaderboardEntry.expectancyR,
                profitFactor: leaderboardEntry.profitFactor,
                maxDrawdownR: leaderboardEntry.maxDrawdownR,
                avgHoldMinutes: leaderboardEntry.avgHoldMinutes,
                eligible,
                configOverride,
            });
            completedRuns += 1;
            const pct = totalPlannedRuns > 0 ? (completedRuns / totalPlannedRuns) * 100 : 100;
            console.log(
                `[run] ${symbol} strategy=${leaderboardEntry.strategyId} tune=${leaderboardEntry.tuneId} netR=${leaderboardEntry.netR.toFixed(3)} trades=${leaderboardEntry.trades} progress=${completedRuns}/${totalPlannedRuns} (${pct.toFixed(1)}%)`,
            );
        }

        const sorted = sortRuns(symbolRuns);
        const winners = sorted.filter((row) => row.eligible).slice(0, topK);
        leaderboards.push({
            symbol,
            runs: sorted,
            winners,
        });
        allRuns.push(...sorted);

        if (persistRegistry) {
            for (const winner of winners) {
                await upsertScalpDeploymentRegistryEntry({
                    symbol: winner.symbol,
                    strategyId: winner.strategyId,
                    tuneId: winner.tuneId,
                    deploymentId: winner.deploymentId,
                    enabled: false,
                    source: 'matrix',
                    updatedBy: 'scripts/scalp-deployment-matrix',
                    notes: `days=${days} scenario=${winner.scenarioId} pending_forward_gate`,
                    leaderboardEntry: {
                        symbol: winner.symbol,
                        strategyId: winner.strategyId,
                        tuneId: winner.tuneId,
                        deploymentId: winner.deploymentId,
                        tuneLabel: winner.tuneLabel,
                        netR: winner.netR,
                        profitFactor: winner.profitFactor,
                        maxDrawdownR: winner.maxDrawdownR,
                        trades: winner.trades,
                        winRatePct: winner.winRatePct,
                        avgHoldMinutes: winner.avgHoldMinutes,
                        expectancyR: winner.expectancyR,
                    },
                    configOverride: winner.configOverride,
                });
            }
        }
    }

    await mkdir(outDir, { recursive: true });
    const sortedAllRuns = sortRuns(allRuns);
    const overview = {
        generatedAtIso: new Date().toISOString(),
        days,
        historyBackend,
        symbolsTested: leaderboards.length,
        scenarioCount: scenarios.length,
        runCount: allRuns.length,
        topK,
        minTrades,
        maxDrawdownRGate: maxDrawdownRGate ?? null,
        persistRegistry,
        bestRun: sortedAllRuns[0] || null,
    };
    await writeFile(
        path.join(outDir, 'leaderboard.summary.json'),
        `${JSON.stringify({ overview, scenarios, leaderboards }, null, 2)}\n`,
        'utf8',
    );
    await writeFile(path.join(outDir, 'leaderboard.runs.csv'), toRunsCsv(sortedAllRuns), 'utf8');
    await writeFile(
        path.join(outDir, 'leaderboard.winners.json'),
        `${JSON.stringify(
            leaderboards.map((row) => ({
                symbol: row.symbol,
                winners: row.winners,
            })),
            null,
            2,
        )}\n`,
        'utf8',
    );

    console.log(
        `Scalp deployment matrix complete | symbols=${leaderboards.length} runs=${allRuns.length} persisted=${persistRegistry ? leaderboards.reduce((acc, row) => acc + row.winners.length, 0) : 0}`,
    );
    console.log(`Artifacts: ${outDir}`);
}

main().catch((err) => {
    console.error('Scalp deployment matrix failed:', err);
    process.exitCode = 1;
});
