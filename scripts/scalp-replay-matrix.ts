#!/usr/bin/env node
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { defaultScalpReplayConfig, normalizeScalpReplayInput, runScalpReplay } from '../lib/scalp/replay/harness';
import type { ScalpReplayInputFile, ScalpReplayRuntimeConfig } from '../lib/scalp/replay/types';

type Scenario = {
    id: string;
    spreadFactor: number;
    slippagePips: number;
    tpR: number;
    riskPct: number;
    sweepBufferPips: number;
    mssLookbackBars: number;
    ifvgEntryMode: ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'];
};

type ScenarioFileEntry = Partial<Omit<Scenario, 'id'>> & { id?: string };
type ScenarioFile = {
    scenarios: ScenarioFileEntry[];
};

type FixtureManifestEntry = {
    id: string;
    file: string;
    tier?: string;
    tags?: string[];
    description?: string;
};

type FixtureManifest = {
    fixtures: FixtureManifestEntry[];
};

type RunSummary = {
    fixtureId: string;
    scenarioId: string;
    spreadFactor: number;
    slippagePips: number;
    tpR: number;
    riskPct: number;
    sweepBufferPips: number;
    mssLookbackBars: number;
    ifvgEntryMode: ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'];
    trades: number;
    winRatePct: number;
    avgR: number;
    expectancyR: number;
    netR: number;
    netPnlUsd: number;
    maxDrawdownR: number;
    avgHoldMinutes: number;
};

type ScenarioAggregate = {
    scenarioId: string;
    spreadFactor: number;
    slippagePips: number;
    tpR: number;
    riskPct: number;
    sweepBufferPips: number;
    mssLookbackBars: number;
    ifvgEntryMode: ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'];
    runs: number;
    fixturesWithTrades: number;
    tradeCoveragePct: number;
    totalTrades: number;
    avgWinRatePct: number;
    avgR: number;
    avgExpectancyR: number;
    avgNetR: number;
    avgNetPnlUsd: number;
    worstNetR: number;
    worstMaxDrawdownR: number;
    robustnessScore: number;
};

function usage() {
    return [
        'Usage:',
        '  node --import tsx scripts/scalp-replay-matrix.ts --fixtures core [options]',
        '  node --import tsx scripts/scalp-replay-matrix.ts --input <candles.json> [options]',
        '',
        'Options:',
        '  --outDir <path>                Output folder (default: /tmp/scalp-replay-matrix)',
        '  --fixtures <selector>          all | core | comma-separated fixture ids',
        '  --fixturesIndex <path>         Fixture index path (default: data/scalp-replay/fixtures/index.json)',
        '  --input <path>                 Single replay input file',
        '  --scenarioFile <path>          Optional JSON scenario list for custom sweeps',
        '  --spreadFactors <csv>          Spread factors (default: 1,1.5,2)',
        '  --slippagePips <csv>           Slippage pips (default: 0,0.15,0.3)',
        '  --tpRs <csv>                   Take-profit R values (default: config value)',
        '  --riskPcts <csv>               Risk % values (default: config value)',
        '  --sweepBufferPips <csv>        Sweep buffer in pips (default: config value)',
        '  --mssLookbackBars <csv>        MSS lookback bars (default: config value)',
        '  --ifvgEntryModes <csv>         first_touch|midline_touch|full_fill',
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

function parseNonNegativeCsv(value: unknown, fallback: number[]): number[] {
    if (typeof value !== 'string' || !value.trim()) return fallback.slice();
    const parsed = value
        .split(',')
        .map((v) => Number(v.trim()))
        .filter((v) => Number.isFinite(v) && v >= 0);
    return parsed.length ? parsed : fallback.slice();
}

function parsePositiveCsv(value: unknown, fallback: number[]): number[] {
    if (typeof value !== 'string' || !value.trim()) return fallback.slice();
    const parsed = value
        .split(',')
        .map((v) => Number(v.trim()))
        .filter((v) => Number.isFinite(v) && v > 0);
    return parsed.length ? parsed : fallback.slice();
}

function parsePositiveIntCsv(value: unknown, fallback: number[]): number[] {
    return parsePositiveCsv(value, fallback).map((v) => Math.floor(v));
}

function parseIfvgEntryMode(value: unknown): ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'] | null {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'first_touch' || normalized === 'midline_touch' || normalized === 'full_fill') {
        return normalized;
    }
    return null;
}

function parseIfvgEntryModeCsv(
    value: unknown,
    fallback: ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'][],
): ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'][] {
    if (typeof value !== 'string' || !value.trim()) return fallback.slice();
    const parsed = value
        .split(',')
        .map((v) => parseIfvgEntryMode(v))
        .filter((v): v is ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'] => Boolean(v));
    return parsed.length ? parsed : fallback.slice();
}

async function loadFixtureInput(filePath: string): Promise<ReturnType<typeof normalizeScalpReplayInput>> {
    const raw = await readFile(filePath, 'utf8');
    return normalizeScalpReplayInput(JSON.parse(raw) as ScalpReplayInputFile);
}

function selectFixtures(manifest: FixtureManifest, selector: string): FixtureManifestEntry[] {
    const fixtures = Array.isArray(manifest.fixtures) ? manifest.fixtures : [];
    if (!fixtures.length) throw new Error('Fixture index has no fixtures');
    const normalized = String(selector || 'core').trim().toLowerCase();
    if (normalized === 'all') return fixtures;
    if (normalized === 'core') {
        return fixtures.filter((f) => String(f.tier || '').toLowerCase() === 'core');
    }
    const selectedIds = new Set(
        normalized
            .split(',')
            .map((v) => v.trim())
            .filter((v) => v.length > 0),
    );
    return fixtures.filter((f) => selectedIds.has(String(f.id || '').trim().toLowerCase()));
}

function buildScenarios(params: {
    spreadFactors: number[];
    slippagePips: number[];
    tpRs: number[];
    riskPcts: number[];
    sweepBufferPips: number[];
    mssLookbackBars: number[];
    ifvgEntryModes: ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'][];
}): Scenario[] {
    const out: Scenario[] = [];
    let idx = 0;
    for (const spread of params.spreadFactors) {
        for (const slip of params.slippagePips) {
            for (const tpR of params.tpRs) {
                for (const riskPct of params.riskPcts) {
                    for (const sweepBufferPips of params.sweepBufferPips) {
                        for (const mssLookbackBars of params.mssLookbackBars) {
                            for (const ifvgEntryMode of params.ifvgEntryModes) {
                                out.push({
                                    id: [
                                        `S${idx.toString().padStart(2, '0')}`,
                                        `SPR${spread.toFixed(2)}`,
                                        `SLP${slip.toFixed(2)}`,
                                        `TP${tpR.toFixed(2)}`,
                                        `RISK${riskPct.toFixed(2)}`,
                                        `SWP${sweepBufferPips.toFixed(2)}`,
                                        `MSS${mssLookbackBars}`,
                                        `IFVG${ifvgEntryMode.toUpperCase()}`,
                                    ].join('_'),
                                    spreadFactor: spread,
                                    slippagePips: slip,
                                    tpR,
                                    riskPct,
                                    sweepBufferPips,
                                    mssLookbackBars,
                                    ifvgEntryMode,
                                });
                                idx += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    if (!out.length) throw new Error('No scenarios generated from provided grid');
    return out;
}

function toCsv(rows: RunSummary[]): string {
    const headers = [
        'fixtureId',
        'scenarioId',
        'spreadFactor',
        'slippagePips',
        'tpR',
        'riskPct',
        'sweepBufferPips',
        'mssLookbackBars',
        'ifvgEntryMode',
        'trades',
        'winRatePct',
        'avgR',
        'expectancyR',
        'netR',
        'netPnlUsd',
        'maxDrawdownR',
        'avgHoldMinutes',
    ];
    const lines = [headers.join(',')];
    for (const row of rows) {
        lines.push(
            [
                row.fixtureId,
                row.scenarioId,
                row.spreadFactor.toFixed(4),
                row.slippagePips.toFixed(4),
                row.tpR.toFixed(4),
                row.riskPct.toFixed(4),
                row.sweepBufferPips.toFixed(4),
                row.mssLookbackBars,
                row.ifvgEntryMode,
                row.trades,
                row.winRatePct.toFixed(4),
                row.avgR.toFixed(6),
                row.expectancyR.toFixed(6),
                row.netR.toFixed(6),
                row.netPnlUsd.toFixed(4),
                row.maxDrawdownR.toFixed(6),
                row.avgHoldMinutes.toFixed(4),
            ].join(','),
        );
    }
    return `${lines.join('\n')}\n`;
}

function toAggregateCsv(rows: ScenarioAggregate[]): string {
    const headers = [
        'scenarioId',
        'spreadFactor',
        'slippagePips',
        'tpR',
        'riskPct',
        'sweepBufferPips',
        'mssLookbackBars',
        'ifvgEntryMode',
        'runs',
        'fixturesWithTrades',
        'tradeCoveragePct',
        'totalTrades',
        'avgWinRatePct',
        'avgR',
        'avgExpectancyR',
        'avgNetR',
        'avgNetPnlUsd',
        'worstNetR',
        'worstMaxDrawdownR',
        'robustnessScore',
    ];
    const lines = [headers.join(',')];
    for (const row of rows) {
        lines.push(
            [
                row.scenarioId,
                row.spreadFactor.toFixed(4),
                row.slippagePips.toFixed(4),
                row.tpR.toFixed(4),
                row.riskPct.toFixed(4),
                row.sweepBufferPips.toFixed(4),
                row.mssLookbackBars,
                row.ifvgEntryMode,
                row.runs,
                row.fixturesWithTrades,
                row.tradeCoveragePct.toFixed(4),
                row.totalTrades,
                row.avgWinRatePct.toFixed(4),
                row.avgR.toFixed(6),
                row.avgExpectancyR.toFixed(6),
                row.avgNetR.toFixed(6),
                row.avgNetPnlUsd.toFixed(4),
                row.worstNetR.toFixed(6),
                row.worstMaxDrawdownR.toFixed(6),
                row.robustnessScore.toFixed(6),
            ].join(','),
        );
    }
    return `${lines.join('\n')}\n`;
}

function mean(values: number[]): number {
    if (!values.length) return 0;
    return values.reduce((acc, v) => acc + v, 0) / values.length;
}

function aggregateScenarios(runs: RunSummary[], fixtureCount: number): ScenarioAggregate[] {
    const byScenario = new Map<string, RunSummary[]>();
    for (const run of runs) {
        if (!byScenario.has(run.scenarioId)) byScenario.set(run.scenarioId, []);
        byScenario.get(run.scenarioId)!.push(run);
    }

    const aggregates: ScenarioAggregate[] = [];
    for (const [scenarioId, rows] of byScenario.entries()) {
        if (!rows.length) continue;
        const sample = rows[0]!;
        const fixturesWithTrades = new Set(rows.filter((r) => r.trades > 0).map((r) => r.fixtureId)).size;
        const tradeCoveragePct = fixtureCount > 0 ? (fixturesWithTrades / fixtureCount) * 100 : 0;
        const totalTrades = rows.reduce((acc, row) => acc + row.trades, 0);
        const avgNetR = mean(rows.map((row) => row.netR));
        const worstNetR = Math.min(...rows.map((row) => row.netR));
        const worstMaxDrawdownR = Math.max(...rows.map((row) => row.maxDrawdownR));
        const coveragePenalty = Math.max(0, 1 - tradeCoveragePct / 100);
        const robustnessScore = avgNetR - 0.35 * worstMaxDrawdownR - 0.5 * coveragePenalty;

        aggregates.push({
            scenarioId,
            spreadFactor: sample.spreadFactor,
            slippagePips: sample.slippagePips,
            tpR: sample.tpR,
            riskPct: sample.riskPct,
            sweepBufferPips: sample.sweepBufferPips,
            mssLookbackBars: sample.mssLookbackBars,
            ifvgEntryMode: sample.ifvgEntryMode,
            runs: rows.length,
            fixturesWithTrades,
            tradeCoveragePct,
            totalTrades,
            avgWinRatePct: mean(rows.map((row) => row.winRatePct)),
            avgR: mean(rows.map((row) => row.avgR)),
            avgExpectancyR: mean(rows.map((row) => row.expectancyR)),
            avgNetR,
            avgNetPnlUsd: mean(rows.map((row) => row.netPnlUsd)),
            worstNetR,
            worstMaxDrawdownR,
            robustnessScore,
        });
    }

    return aggregates.sort((a, b) => b.robustnessScore - a.robustnessScore);
}

function sanitizeScenarioFromFile(params: {
    entry: ScenarioFileEntry;
    idx: number;
    fallback: Omit<Scenario, 'id'>;
}): Scenario {
    const spreadFactor = toNum(params.entry.spreadFactor);
    const slippagePips = toNum(params.entry.slippagePips);
    const tpR = toNum(params.entry.tpR);
    const riskPct = toNum(params.entry.riskPct);
    const sweepBufferPips = toNum(params.entry.sweepBufferPips);
    const mssLookbackBars = toNum(params.entry.mssLookbackBars);
    const ifvgEntryMode = parseIfvgEntryMode(params.entry.ifvgEntryMode);
    const id = String(params.entry.id || '').trim() || `scenario_${params.idx.toString().padStart(2, '0')}`;

    return {
        id,
        spreadFactor: spreadFactor !== undefined && spreadFactor > 0 ? spreadFactor : params.fallback.spreadFactor,
        slippagePips: slippagePips !== undefined && slippagePips >= 0 ? slippagePips : params.fallback.slippagePips,
        tpR: tpR !== undefined && tpR > 0 ? tpR : params.fallback.tpR,
        riskPct: riskPct !== undefined && riskPct > 0 ? riskPct : params.fallback.riskPct,
        sweepBufferPips:
            sweepBufferPips !== undefined && sweepBufferPips >= 0 ? sweepBufferPips : params.fallback.sweepBufferPips,
        mssLookbackBars:
            mssLookbackBars !== undefined && mssLookbackBars > 0 ? Math.floor(mssLookbackBars) : params.fallback.mssLookbackBars,
        ifvgEntryMode: ifvgEntryMode || params.fallback.ifvgEntryMode,
    };
}

async function loadScenariosFromFile(params: {
    scenarioFilePath: string;
    fallback: Omit<Scenario, 'id'>;
}): Promise<Scenario[]> {
    const raw = await readFile(params.scenarioFilePath, 'utf8');
    const parsed = JSON.parse(raw) as ScenarioFile;
    const entries = Array.isArray(parsed.scenarios) ? parsed.scenarios : [];
    if (!entries.length) throw new Error('scenarioFile must include at least one scenario entry');
    return entries.map((entry, idx) => sanitizeScenarioFromFile({ entry, idx, fallback: params.fallback }));
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    if (args.help) {
        console.log(usage());
        return;
    }

    const outDir = path.resolve(typeof args.outDir === 'string' ? String(args.outDir) : '/tmp/scalp-replay-matrix');

    const fixtureInputs: Array<{ id: string; input: ReturnType<typeof normalizeScalpReplayInput> }> = [];
    let fixtureSelection = 'core';
    let fixturesIndexPath: string | null = null;

    if (typeof args.input === 'string' && args.input.trim()) {
        const inputPath = path.resolve(String(args.input));
        const input = await loadFixtureInput(inputPath);
        fixtureInputs.push({ id: path.basename(inputPath), input });
        fixtureSelection = 'single-input';
    } else {
        fixturesIndexPath = path.resolve(
            typeof args.fixturesIndex === 'string' ? String(args.fixturesIndex) : 'data/scalp-replay/fixtures/index.json',
        );
        fixtureSelection = typeof args.fixtures === 'string' ? String(args.fixtures) : 'core';
        const manifest = JSON.parse(await readFile(fixturesIndexPath, 'utf8')) as FixtureManifest;
        const selected = selectFixtures(manifest, fixtureSelection);
        if (!selected.length) throw new Error('No fixtures matched selection');
        for (const fixture of selected) {
            const filePath = path.resolve(path.dirname(fixturesIndexPath), fixture.file);
            fixtureInputs.push({ id: fixture.id, input: await loadFixtureInput(filePath) });
        }
    }
    if (!fixtureInputs.length) throw new Error('No fixture inputs available');

    const seedConfig = defaultScalpReplayConfig(fixtureInputs[0]!.input.symbol);
    const fallbackScenario: Omit<Scenario, 'id'> = {
        spreadFactor: 1,
        slippagePips: seedConfig.slippagePips,
        tpR: seedConfig.strategy.takeProfitR,
        riskPct: seedConfig.strategy.riskPerTradePct,
        sweepBufferPips: seedConfig.strategy.sweepBufferPips,
        mssLookbackBars: seedConfig.strategy.mssLookbackBars,
        ifvgEntryMode: seedConfig.strategy.ifvgEntryMode,
    };

    const scenarios =
        typeof args.scenarioFile === 'string' && args.scenarioFile.trim()
            ? await loadScenariosFromFile({
                  scenarioFilePath: path.resolve(String(args.scenarioFile)),
                  fallback: fallbackScenario,
              })
            : buildScenarios({
                  spreadFactors: parsePositiveCsv(args.spreadFactors, [1, 1.5, 2]),
                  slippagePips: parseNonNegativeCsv(args.slippagePips, [0, 0.15, 0.3]),
                  tpRs: parsePositiveCsv(args.tpRs, [fallbackScenario.tpR]),
                  riskPcts: parsePositiveCsv(args.riskPcts, [fallbackScenario.riskPct]),
                  sweepBufferPips: parseNonNegativeCsv(args.sweepBufferPips, [fallbackScenario.sweepBufferPips]),
                  mssLookbackBars: parsePositiveIntCsv(args.mssLookbackBars, [fallbackScenario.mssLookbackBars]),
                  ifvgEntryModes: parseIfvgEntryModeCsv(args.ifvgEntryModes, [fallbackScenario.ifvgEntryMode]),
              });

    const runs: RunSummary[] = [];
    for (const fixture of fixtureInputs) {
        for (const scenario of scenarios) {
            const baseConfig = defaultScalpReplayConfig(fixture.input.symbol);
            const config: ScalpReplayRuntimeConfig = {
                ...baseConfig,
                strategy: {
                    ...baseConfig.strategy,
                    takeProfitR: scenario.tpR,
                    riskPerTradePct: scenario.riskPct,
                    sweepBufferPips: scenario.sweepBufferPips,
                    mssLookbackBars: scenario.mssLookbackBars,
                    ifvgEntryMode: scenario.ifvgEntryMode,
                },
                symbol: fixture.input.symbol,
                spreadFactor: scenario.spreadFactor,
                slippagePips: scenario.slippagePips,
            };

            const replay = runScalpReplay({
                candles: fixture.input.candles,
                pipSize: fixture.input.pipSize,
                config,
            });

            runs.push({
                fixtureId: fixture.id,
                scenarioId: scenario.id,
                spreadFactor: scenario.spreadFactor,
                slippagePips: scenario.slippagePips,
                tpR: scenario.tpR,
                riskPct: scenario.riskPct,
                sweepBufferPips: scenario.sweepBufferPips,
                mssLookbackBars: scenario.mssLookbackBars,
                ifvgEntryMode: scenario.ifvgEntryMode,
                trades: replay.summary.trades,
                winRatePct: replay.summary.winRatePct,
                avgR: replay.summary.avgR,
                expectancyR: replay.summary.expectancyR,
                netR: replay.summary.netR,
                netPnlUsd: replay.summary.netPnlUsd,
                maxDrawdownR: replay.summary.maxDrawdownR,
                avgHoldMinutes: replay.summary.avgHoldMinutes,
            });
        }
    }

    const byNetR = runs.slice().sort((a, b) => b.netR - a.netR);
    const aggregates = aggregateScenarios(runs, fixtureInputs.length);
    const bestAggregate = aggregates[0] || null;
    const worstAggregate = aggregates[aggregates.length - 1] || null;

    const overview = {
        generatedAtIso: new Date().toISOString(),
        fixtureSelection,
        fixturesIndexPath,
        fixtureCount: fixtureInputs.length,
        scenarioCount: scenarios.length,
        runCount: runs.length,
        bestRun: byNetR[0] || null,
        worstRun: byNetR[byNetR.length - 1] || null,
        bestScenario: bestAggregate,
        worstScenario: worstAggregate,
        topRobustScenarios: aggregates.slice(0, 5).map((row) => ({
            scenarioId: row.scenarioId,
            robustnessScore: row.robustnessScore,
            tradeCoveragePct: row.tradeCoveragePct,
            avgNetR: row.avgNetR,
            worstMaxDrawdownR: row.worstMaxDrawdownR,
        })),
    };

    await mkdir(outDir, { recursive: true });
    await writeFile(path.join(outDir, 'matrix.summary.json'), JSON.stringify({ overview, runs, scenarioSummaries: aggregates }, null, 2), 'utf8');
    await writeFile(path.join(outDir, 'matrix.summary.csv'), toCsv(runs), 'utf8');
    await writeFile(path.join(outDir, 'matrix.scenarios.csv'), toAggregateCsv(aggregates), 'utf8');

    console.log(`Scalp replay matrix complete | fixtures=${fixtureInputs.length} scenarios=${scenarios.length} runs=${runs.length}`);
    if (overview.bestRun) {
        console.log(`Best run: fixture=${overview.bestRun.fixtureId} scenario=${overview.bestRun.scenarioId} netR=${overview.bestRun.netR.toFixed(3)}`);
    }
    if (overview.worstRun) {
        console.log(`Worst run: fixture=${overview.worstRun.fixtureId} scenario=${overview.worstRun.scenarioId} netR=${overview.worstRun.netR.toFixed(3)}`);
    }
    if (overview.bestScenario) {
        console.log(
            `Top robust scenario: ${overview.bestScenario.scenarioId} score=${overview.bestScenario.robustnessScore.toFixed(3)} coverage=${overview.bestScenario.tradeCoveragePct.toFixed(1)}%`,
        );
    }
    console.log(`Artifacts: ${outDir}`);
}

main().catch((err) => {
    console.error('Scalp replay matrix failed:', err);
    process.exitCode = 1;
});
