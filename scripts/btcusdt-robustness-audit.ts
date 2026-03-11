#!/usr/bin/env node
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { loadScalpCandleHistory } from '../lib/scalp/candleHistory';
import { pipSizeForScalpSymbol } from '../lib/scalp/marketData';
import { defaultScalpReplayConfig, runScalpReplay } from '../lib/scalp/replay/harness';
import type {
    ScalpReplayCandle,
    ScalpReplayResult,
    ScalpReplayRuntimeConfig,
    ScalpReplaySummary,
    ScalpReplayTrade,
} from '../lib/scalp/replay/types';
import { resolveScalpDeployment } from '../lib/scalp/deployments';
import { applySymbolGuardRiskDefaultsToReplayRuntime } from '../lib/scalp/strategies/guardDefaults';
import { COMPRESSION_BREAKOUT_PULLBACK_M15_M3_STRATEGY_ID } from '../lib/scalp/strategies/compressionBreakoutPullbackM15M3';
import { REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID } from '../lib/scalp/strategies/regimePullbackM15M3BtcusdtGuarded';

type CandleRow = [number, number, number, number, number, number];

type ScenarioFamily = 'cbp' | 'guarded';

type Scenario = {
    id: string;
    family: ScenarioFamily;
    strategyId: string;
    executeMinutes: number;
    spreadFactor: number;
    slippagePips: number;
    blockedHoursVariant?: string | null;
    blockedHoursBerlin?: number[] | null;
    strategyOverrides: Partial<ScalpReplayRuntimeConfig['strategy']>;
    metadata?: Record<string, unknown>;
};

type WindowRange = {
    fromTs: number;
    toTs: number;
};

type StressCase = {
    id: string;
    spreadMult: number;
    slippageMult: number;
};

type Roll = {
    rollIndex: number;
    selection: WindowRange;
    forward2w: WindowRange;
    forward4w: WindowRange;
};

type ScenarioEval = {
    scenarioId: string;
    family: ScenarioFamily;
    strategyId: string;
    summary: ScalpReplaySummary;
};

type SweepRow = {
    scenarioId: string;
    value: string;
    valueNum: number | null;
    trades: number;
    netPnlUsd: number;
    expectancyR: number;
    expectancyUsdPerTrade: number;
    profitFactor: number | null;
    maxDrawdownR: number;
    winRatePct: number;
    avgHoldMinutes: number;
};

type SweepAssessment = {
    judgment: 'stable plateau' | 'moderately sensitive' | 'narrow peak / likely overfit';
    deployedLocation: 'near center of good region' | 'at an edge' | 'single sharp optimum' | 'categorical / n-a';
    deployedValue: string;
    bestValue: string;
};

type WalkForwardSelectionRow = {
    rollIndex: number;
    gate: number;
    selectedScenarioId: string;
    selectedFamily: ScenarioFamily;
    selectedStrategyId: string;
    eligibleCandidates: number;
    candidateCount: number;
    selectionTrades: number;
    selectionNetPnlUsd: number;
    selectionExpectancyR: number;
    selectionProfitFactor: number | null;
    selectionMaxDrawdownR: number;
    fwd2wNetPnlUsd: number;
    fwd2wExpectancyR: number;
    fwd2wProfitFactor: number | null;
    fwd2wMaxDrawdownR: number;
    fwd4wNetPnlUsd: number;
    fwd4wExpectancyR: number;
    fwd4wProfitFactor: number | null;
    fwd4wMaxDrawdownR: number;
};

type MonthlyStats = {
    month: string;
    trades: number;
    netPnlUsd: number;
    expectancyR: number;
};

type ScriptOutput = {
    generatedAtIso: string;
    symbol: string;
    dataset: {
        yearFromTs: number;
        yearToTs: number;
        candles15m: number;
        candles1m: number;
    };
    assumptions: {
        walkForwardSelectionDays: number;
        walkForwardForward2wDays: number;
        walkForwardForward4wDays: number;
        walkForwardStepDays: number;
        baselineDdGate: number;
        selectionRanking: string;
    };
    taskA: unknown;
    taskB: unknown;
    taskC: unknown;
    taskD: unknown;
    taskE: unknown;
    taskF: unknown;
    finalConclusions: unknown;
};

const BTC_SYMBOL = 'BTCUSDT';
const DAY_MS = 24 * 60 * 60 * 1000;
const SELECTION_DAYS = 90;
const FORWARD_2W_DAYS = 14;
const FORWARD_4W_DAYS = 28;
const ROLL_STEP_DAYS = 14;
const BASELINE_DD_GATE = 8;
const DD_GATES = [8, 10, 12];

const STRESS_CASES: StressCase[] = [
    { id: 'baseline', slippageMult: 1, spreadMult: 1 },
    { id: 'slippage_x2', slippageMult: 2, spreadMult: 1 },
    { id: 'spread_1.25x', slippageMult: 1, spreadMult: 1.25 },
    { id: 'spread_1.50x', slippageMult: 1, spreadMult: 1.5 },
    { id: 'slippage_x2_spread_1.50x', slippageMult: 2, spreadMult: 1.5 },
];

const CBP_DEPLOYED = {
    executeMinutes: 2,
    spreadFactor: 1,
    slippagePips: 0.15,
    strategyOverrides: {
        tp1ClosePct: 8,
        trailAtrMult: 1.5,
        timeStopBars: 18,
        sweepBufferPips: 0.2,
        takeProfitR: 1,
        riskPerTradePct: 0.25,
    } satisfies Partial<ScalpReplayRuntimeConfig['strategy']>,
};

const GUARDED_DEPLOYED = {
    executeMinutes: 3,
    spreadFactor: 1,
    slippagePips: 0.15,
    blockedHoursVariant: 'btcusdt_high_pf',
    blockedHoursBerlin: null as number[] | null,
    strategyOverrides: {} as Partial<ScalpReplayRuntimeConfig['strategy']>,
};

function toReplayCandles(rows: CandleRow[], spreadPips: number): ScalpReplayCandle[] {
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

function cloneRuntime(runtime: ScalpReplayRuntimeConfig): ScalpReplayRuntimeConfig {
    return JSON.parse(JSON.stringify(runtime)) as ScalpReplayRuntimeConfig;
}

function withEnv<T>(env: Record<string, string | undefined>, fn: () => Promise<T>): Promise<T> {
    const previous = new Map<string, string | undefined>();
    for (const [key, value] of Object.entries(env)) {
        previous.set(key, process.env[key]);
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
    }
    return fn().finally(() => {
        for (const [key, value] of previous.entries()) {
            if (value === undefined) delete process.env[key];
            else process.env[key] = value;
        }
    });
}

function percentile(values: number[], p: number): number {
    if (!values.length) return NaN;
    const sorted = values.slice().sort((a, b) => a - b);
    const rank = Math.max(0, Math.min(1, p)) * (sorted.length - 1);
    const lo = Math.floor(rank);
    const hi = Math.ceil(rank);
    if (lo === hi) return sorted[lo]!;
    const w = rank - lo;
    return sorted[lo]! * (1 - w) + sorted[hi]! * w;
}

function mean(values: number[]): number {
    if (!values.length) return NaN;
    return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function median(values: number[]): number {
    return percentile(values, 0.5);
}

function toMonthKey(ts: number): string {
    const d = new Date(ts);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    return `${y}-${m}`;
}

function pearsonCorrelation(xs: number[], ys: number[]): number | null {
    if (xs.length !== ys.length || xs.length < 2) return null;
    const mx = mean(xs);
    const my = mean(ys);
    if (!Number.isFinite(mx) || !Number.isFinite(my)) return null;
    let num = 0;
    let vx = 0;
    let vy = 0;
    for (let i = 0; i < xs.length; i += 1) {
        const dx = xs[i]! - mx;
        const dy = ys[i]! - my;
        num += dx * dy;
        vx += dx * dx;
        vy += dy * dy;
    }
    if (!(vx > 0) || !(vy > 0)) return null;
    return num / Math.sqrt(vx * vy);
}

function maxDrawdownFromPnl(pnls: number[]): number {
    let equity = 0;
    let peak = 0;
    let maxDd = 0;
    for (const pnl of pnls) {
        equity += pnl;
        peak = Math.max(peak, equity);
        maxDd = Math.max(maxDd, peak - equity);
    }
    return maxDd;
}

function binarySearchFirst(rows: ScalpReplayCandle[], targetTs: number): number {
    let lo = 0;
    let hi = rows.length;
    while (lo < hi) {
        const mid = Math.floor((lo + hi) / 2);
        if (rows[mid]!.ts < targetTs) lo = mid + 1;
        else hi = mid;
    }
    return lo;
}

function sliceByTs(rows: ScalpReplayCandle[], fromTs: number, toTs: number): ScalpReplayCandle[] {
    const start = binarySearchFirst(rows, fromTs);
    const end = binarySearchFirst(rows, toTs);
    return rows.slice(start, end);
}

function formatBlockedHours(hours: number[] | null | undefined): string {
    if (!hours || !hours.length) return 'none';
    return `[${hours.join(',')}]`;
}

function buildCbpScenario(params: {
    id: string;
    executeMinutes: number;
    tp1ClosePct: number;
    trailAtrMult: number;
    timeStopBars: number;
    sweepBufferPips: number;
    spreadFactor?: number;
    slippagePips?: number;
}): Scenario {
    return {
        id: params.id,
        family: 'cbp',
        strategyId: COMPRESSION_BREAKOUT_PULLBACK_M15_M3_STRATEGY_ID,
        executeMinutes: params.executeMinutes,
        spreadFactor: params.spreadFactor ?? 1,
        slippagePips: params.slippagePips ?? 0.15,
        strategyOverrides: {
            tp1ClosePct: params.tp1ClosePct,
            trailAtrMult: params.trailAtrMult,
            timeStopBars: params.timeStopBars,
            sweepBufferPips: params.sweepBufferPips,
            takeProfitR: 1,
            riskPerTradePct: 0.25,
        },
        metadata: {
            tp1ClosePct: params.tp1ClosePct,
            trailAtrMult: params.trailAtrMult,
            timeStopBars: params.timeStopBars,
            sweepBufferPips: params.sweepBufferPips,
        },
    };
}

function buildGuardedScenario(params: {
    id: string;
    blockedHours: number[] | null;
    blockedVariant?: string | null;
    trailAtrMult?: number;
    timeStopBars?: number;
    tp1ClosePct?: number;
    executeMinutes?: number;
    spreadFactor?: number;
    slippagePips?: number;
}): Scenario {
    const overrides: Partial<ScalpReplayRuntimeConfig['strategy']> = {};
    if (Number.isFinite(Number(params.trailAtrMult))) overrides.trailAtrMult = Number(params.trailAtrMult);
    if (Number.isFinite(Number(params.timeStopBars))) overrides.timeStopBars = Number(params.timeStopBars);
    if (Number.isFinite(Number(params.tp1ClosePct))) overrides.tp1ClosePct = Number(params.tp1ClosePct);
    return {
        id: params.id,
        family: 'guarded',
        strategyId: REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID,
        executeMinutes: params.executeMinutes ?? 3,
        spreadFactor: params.spreadFactor ?? 1,
        slippagePips: params.slippagePips ?? 0.15,
        blockedHoursVariant: params.blockedVariant ?? null,
        blockedHoursBerlin: params.blockedHours,
        strategyOverrides: overrides,
        metadata: {
            blockedHours: formatBlockedHours(params.blockedHours),
            blockedVariant: params.blockedVariant ?? null,
            trailAtrMult: params.trailAtrMult ?? null,
            timeStopBars: params.timeStopBars ?? null,
            tp1ClosePct: params.tp1ClosePct ?? null,
        },
    };
}

function buildCbpSelectionUniverse(): Scenario[] {
    const trail = [1.2, 1.3, 1.4, 1.5];
    const tp1 = [8, 10, 12, 15, 18, 20];
    const sweep = [0.2, 0.25];
    const out: Scenario[] = [];
    for (const p of tp1) {
        for (const tr of trail) {
            for (const sw of sweep) {
                const id = `cbp_btc_dd8_e2_p${p}_tr${tr.toFixed(1)}_ts18_sw${sw.toFixed(2)}`;
                out.push(
                    buildCbpScenario({
                        id,
                        executeMinutes: 2,
                        tp1ClosePct: p,
                        trailAtrMult: tr,
                        timeStopBars: 18,
                        sweepBufferPips: sw,
                    }),
                );
            }
        }
    }
    return out;
}

function buildGuardedSelectionUniverse(): Scenario[] {
    const variants: Array<{ name: string; hours: number[] | null }> = [
        { name: 'none', hours: [] },
        { name: 'h10_11', hours: [10, 11] },
        { name: 'h9_10', hours: [9, 10] },
        { name: 'h11_12', hours: [11, 12] },
        { name: 'h10', hours: [10] },
        { name: 'h11', hours: [11] },
    ];

    const out: Scenario[] = [];
    for (const variant of variants) {
        const id = `guarded_${variant.name}_default`;
        out.push(
            buildGuardedScenario({
                id,
                blockedHours: variant.hours,
                blockedVariant: 'off',
                trailAtrMult: 1.4,
                timeStopBars: 15,
                tp1ClosePct: 20,
            }),
        );
    }
    for (const value of [1.2, 1.3, 1.4, 1.5, 1.6]) {
        out.push(
            buildGuardedScenario({
                id: `guarded_h10_11_tr${value.toFixed(1)}`,
                blockedHours: [10, 11],
                blockedVariant: 'off',
                trailAtrMult: value,
                timeStopBars: 15,
                tp1ClosePct: 20,
            }),
        );
    }
    for (const value of [12, 15, 18]) {
        out.push(
            buildGuardedScenario({
                id: `guarded_h10_11_ts${value}`,
                blockedHours: [10, 11],
                blockedVariant: 'off',
                trailAtrMult: 1.4,
                timeStopBars: value,
                tp1ClosePct: 20,
            }),
        );
    }
    for (const value of [10, 20, 30]) {
        out.push(
            buildGuardedScenario({
                id: `guarded_h10_11_tp1_${value}`,
                blockedHours: [10, 11],
                blockedVariant: 'off',
                trailAtrMult: 1.4,
                timeStopBars: 15,
                tp1ClosePct: value,
            }),
        );
    }

    const deduped = new Map<string, Scenario>();
    for (const scenario of out) deduped.set(scenario.id, scenario);
    return Array.from(deduped.values());
}

function compareScenarioRows(a: ScenarioEval, b: ScenarioEval): number {
    if (b.summary.netR !== a.summary.netR) return b.summary.netR - a.summary.netR;
    const pfA = a.summary.profitFactor ?? -1;
    const pfB = b.summary.profitFactor ?? -1;
    if (pfB !== pfA) return pfB - pfA;
    if (a.summary.maxDrawdownR !== b.summary.maxDrawdownR) return a.summary.maxDrawdownR - b.summary.maxDrawdownR;
    if (b.summary.trades !== a.summary.trades) return b.summary.trades - a.summary.trades;
    return b.summary.winRatePct - a.summary.winRatePct;
}

function assessSweep(rows: SweepRow[], deployedValue: string, numeric: boolean): SweepAssessment {
    const sorted = rows.slice().sort((a, b) => {
        if (numeric && a.valueNum !== null && b.valueNum !== null) return a.valueNum - b.valueNum;
        return a.value.localeCompare(b.value);
    });
    const bestExpectancy = Math.max(...sorted.map((row) => row.expectancyR));
    const goodThreshold = Number.isFinite(bestExpectancy) ? bestExpectancy * 0.9 : -Infinity;
    const good = sorted.filter((row) => row.expectancyR >= goodThreshold);

    let judgment: SweepAssessment['judgment'];
    if (good.length >= Math.ceil(sorted.length * 0.6)) judgment = 'stable plateau';
    else if (good.length >= 2) judgment = 'moderately sensitive';
    else judgment = 'narrow peak / likely overfit';

    const deployedIdx = sorted.findIndex((row) => row.value === deployedValue);
    const best = sorted[0] ? sorted.slice().sort((a, b) => b.expectancyR - a.expectancyR)[0]! : null;

    let deployedLocation: SweepAssessment['deployedLocation'] = 'categorical / n-a';
    if (numeric && deployedIdx >= 0) {
        const bestIdx = sorted.findIndex((row) => row.value === (best?.value ?? ''));
        const atEdge = deployedIdx === 0 || deployedIdx === sorted.length - 1;
        const leftGood = deployedIdx > 0 && sorted[deployedIdx - 1]!.expectancyR >= goodThreshold;
        const rightGood = deployedIdx < sorted.length - 1 && sorted[deployedIdx + 1]!.expectancyR >= goodThreshold;
        const uniqueBest = sorted.filter((row) => row.expectancyR === (best?.expectancyR ?? NaN)).length === 1;
        const sharpDropLeft = deployedIdx > 0 ? sorted[deployedIdx]!.expectancyR - sorted[deployedIdx - 1]!.expectancyR : 0;
        const sharpDropRight = deployedIdx < sorted.length - 1 ? sorted[deployedIdx]!.expectancyR - sorted[deployedIdx + 1]!.expectancyR : 0;

        if (uniqueBest && deployedIdx === bestIdx && sharpDropLeft > 0.05 && sharpDropRight > 0.05) {
            deployedLocation = 'single sharp optimum';
        } else if (atEdge) {
            deployedLocation = 'at an edge';
        } else if (leftGood && rightGood) {
            deployedLocation = 'near center of good region';
        } else {
            deployedLocation = 'at an edge';
        }
    }

    return {
        judgment,
        deployedLocation,
        deployedValue,
        bestValue: best?.value ?? 'n/a',
    };
}

function buildRolls(yearFromTs: number, yearToTs: number): Roll[] {
    const rolls: Roll[] = [];
    const selectionMs = SELECTION_DAYS * DAY_MS;
    const fwd2wMs = FORWARD_2W_DAYS * DAY_MS;
    const fwd4wMs = FORWARD_4W_DAYS * DAY_MS;
    const stepMs = ROLL_STEP_DAYS * DAY_MS;
    let selectionEnd = yearFromTs + selectionMs;
    let rollIndex = 1;
    while (selectionEnd + fwd4wMs <= yearToTs) {
        const selectionStart = selectionEnd - selectionMs;
        rolls.push({
            rollIndex,
            selection: { fromTs: selectionStart, toTs: selectionEnd },
            forward2w: { fromTs: selectionEnd, toTs: selectionEnd + fwd2wMs },
            forward4w: { fromTs: selectionEnd, toTs: selectionEnd + fwd4wMs },
        });
        selectionEnd += stepMs;
        rollIndex += 1;
    }
    return rolls;
}

function createCsv(headers: string[], rows: Array<Array<string | number | null>>): string {
    const lines = [headers.join(',')];
    for (const row of rows) {
        lines.push(
            row
                .map((value) => {
                    if (value === null || value === undefined) return '';
                    const text = String(value);
                    if (text.includes(',') || text.includes('"') || text.includes('\n')) {
                        return `"${text.replace(/"/g, '""')}"`;
                    }
                    return text;
                })
                .join(','),
        );
    }
    return `${lines.join('\n')}\n`;
}

async function loadCbpSelectionScenariosFromArtifact(): Promise<Scenario[] | null> {
    const artifactPath = '/tmp/compression-strict-dd8-scenarios-focused.json';
    try {
        const parsed = JSON.parse(await readFile(artifactPath, 'utf8')) as {
            scenarios?: Array<{
                id?: string;
                tuneId?: string;
                executeMinutes?: number;
                spreadFactor?: number;
                slippagePips?: number;
                strategy?: Record<string, unknown>;
            }>;
        };
        if (!Array.isArray(parsed.scenarios) || parsed.scenarios.length === 0) return null;
        const out: Scenario[] = [];
        for (const row of parsed.scenarios) {
            const id = String(row.tuneId || row.id || '').trim();
            if (!id) continue;
            const strategy = row.strategy || {};
            out.push(
                buildCbpScenario({
                    id,
                    executeMinutes: Number(row.executeMinutes || 2),
                    tp1ClosePct: Number(strategy.tp1ClosePct ?? 8),
                    trailAtrMult: Number(strategy.trailAtrMult ?? 1.5),
                    timeStopBars: Number(strategy.timeStopBars ?? 18),
                    sweepBufferPips: Number(strategy.sweepBufferPips ?? 0.2),
                    spreadFactor: Number(row.spreadFactor ?? 1),
                    slippagePips: Number(row.slippagePips ?? 0.15),
                }),
            );
        }
        return out.length ? out : null;
    } catch {
        return null;
    }
}

async function main() {
    const outputRoot = path.resolve('/tmp', `btcusdt-robustness-audit-${new Date().toISOString().replace(/[:.]/g, '-')}`);
    await mkdir(outputRoot, { recursive: true });

    const [hist15m, hist1m] = await Promise.all([
        loadScalpCandleHistory(BTC_SYMBOL, '15m', { backend: 'pg' }),
        loadScalpCandleHistory(BTC_SYMBOL, '1m', { backend: 'pg' }),
    ]);

    const rows15m = (hist15m.record?.candles || []) as CandleRow[];
    const rows1m = (hist1m.record?.candles || []) as CandleRow[];
    if (!rows15m.length || !rows1m.length) {
        throw new Error('Missing BTCUSDT candle history (1m or 15m).');
    }

    const lastTs = Math.min(rows15m.at(-1)?.[0] || 0, rows1m.at(-1)?.[0] || 0);
    const firstTs = Math.max(rows15m[0]![0], rows1m[0]![0]);
    const yearFromTs = Math.max(firstTs, lastTs - 365 * DAY_MS);
    const yearToTs = lastTs;

    const defaultRuntime = defaultScalpReplayConfig(BTC_SYMBOL);
    const defaultSpread = defaultRuntime.defaultSpreadPips;
    const pipSize = pipSizeForScalpSymbol(BTC_SYMBOL);

    const replay15mAll = toReplayCandles(rows15m, defaultSpread);
    const replay1mAll = toReplayCandles(rows1m, defaultSpread);

    const replayCache = new Map<string, ScalpReplayResult>();

    function compactReplayResult(result: ScalpReplayResult, includeTrades: boolean): ScalpReplayResult {
        return {
            config: result.config,
            summary: result.summary,
            trades: includeTrades ? result.trades : [],
            timeline: [],
        };
    }

    async function runScenario(params: {
        scenario: Scenario;
        window: WindowRange;
        stress?: StressCase;
        includeTrades?: boolean;
    }): Promise<ScalpReplayResult> {
        const includeTrades = params.includeTrades ?? false;
        const stressId = params.stress?.id || 'baseline';
        const detailId = includeTrades ? 'withTrades' : 'summaryOnly';
        const key = `${params.scenario.id}|${params.window.fromTs}|${params.window.toTs}|${stressId}|${detailId}`;
        const cached = replayCache.get(key);
        if (cached) return cached;

        const env: Record<string, string | undefined> = {
            SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_VARIANT: params.scenario.blockedHoursVariant || undefined,
            SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_BERLIN:
                params.scenario.blockedHoursBerlin === null || params.scenario.blockedHoursBerlin === undefined
                    ? undefined
                    : params.scenario.blockedHoursBerlin.join(','),
        };

        const result = await withEnv(env, async () => {
            let runtime = cloneRuntime(defaultRuntime);
            runtime.symbol = BTC_SYMBOL;
            runtime.strategyId = params.scenario.strategyId;
            runtime.executeMinutes = params.scenario.executeMinutes;
            runtime.spreadFactor = params.scenario.spreadFactor;
            runtime.slippagePips = params.scenario.slippagePips;
            runtime.strategy = {
                ...runtime.strategy,
                ...params.scenario.strategyOverrides,
            };

            runtime = applySymbolGuardRiskDefaultsToReplayRuntime(runtime);
            runtime.strategy = {
                ...runtime.strategy,
                ...params.scenario.strategyOverrides,
            };

            if (params.stress) {
                runtime.spreadFactor = runtime.spreadFactor * params.stress.spreadMult;
                runtime.slippagePips = runtime.slippagePips * params.stress.slippageMult;
            }

            const deployment = resolveScalpDeployment({
                symbol: BTC_SYMBOL,
                strategyId: runtime.strategyId,
                tuneId: params.scenario.id,
            });
            runtime.tuneId = deployment.tuneId;
            runtime.tuneLabel = deployment.tuneLabel;
            runtime.deploymentId = deployment.deploymentId;

            const baseCandles = sliceByTs(replay15mAll, params.window.fromTs, params.window.toTs);
            const confirmCandles = sliceByTs(replay1mAll, params.window.fromTs, params.window.toTs);
            if (baseCandles.length < 20 || confirmCandles.length < 200) {
                const emptyResult: ScalpReplayResult = {
                    config: runtime,
                    summary: {
                        symbol: BTC_SYMBOL,
                        strategyId: runtime.strategyId,
                        tuneId: runtime.tuneId,
                        deploymentId: runtime.deploymentId,
                        tuneLabel: runtime.tuneLabel,
                        startTs: params.window.fromTs,
                        endTs: params.window.toTs,
                        runs: 0,
                        trades: 0,
                        wins: 0,
                        losses: 0,
                        winRatePct: 0,
                        avgR: 0,
                        expectancyR: 0,
                        netR: 0,
                        grossProfitR: 0,
                        grossLossR: 0,
                        profitFactor: null,
                        netPnlUsd: 0,
                        maxDrawdownR: 0,
                        avgHoldMinutes: 0,
                        exitsByReason: {},
                    },
                    trades: [],
                    timeline: [],
                };
                return emptyResult;
            }

            const replay = await runScalpReplay({
                candles: confirmCandles,
                pipSize,
                config: runtime,
                captureTimeline: false,
                marketData: {
                    baseCandles,
                    confirmCandles,
                    priceCandles: confirmCandles,
                },
            });
            return compactReplayResult(replay, includeTrades);
        });

        replayCache.set(key, result);
        return result;
    }

    const cbpSelectionFromArtifact = await loadCbpSelectionScenariosFromArtifact();
    const cbpSelectionUniverse = cbpSelectionFromArtifact || buildCbpSelectionUniverse();
    const guardedSelectionUniverse = buildGuardedSelectionUniverse();
    const selectionUniverse = [...cbpSelectionUniverse, ...guardedSelectionUniverse];

    const cbpDeployedScenario = buildCbpScenario({
        id: 'cbp_btc_dd8_e2_p8_tr1.5_ts18_sw0.20',
        executeMinutes: CBP_DEPLOYED.executeMinutes,
        tp1ClosePct: 8,
        trailAtrMult: 1.5,
        timeStopBars: 18,
        sweepBufferPips: 0.2,
        spreadFactor: CBP_DEPLOYED.spreadFactor,
        slippagePips: CBP_DEPLOYED.slippagePips,
    });

    const guardedDeployedScenario = buildGuardedScenario({
        id: 'guarded_high_pf_default',
        blockedHours: [10, 11],
        blockedVariant: 'off',
        trailAtrMult: 1.4,
        timeStopBars: 15,
        tp1ClosePct: 20,
        executeMinutes: GUARDED_DEPLOYED.executeMinutes,
        spreadFactor: GUARDED_DEPLOYED.spreadFactor,
        slippagePips: GUARDED_DEPLOYED.slippagePips,
    });

    const fullYearWindow: WindowRange = {
        fromTs: yearFromTs,
        toTs: yearToTs,
    };

    const cbpSweepScenarios = {
        trailAtrMult: [1.3, 1.4, 1.5, 1.6, 1.7].map((value) =>
            buildCbpScenario({
                id: `cbp_sweep_tr${value.toFixed(1)}`,
                executeMinutes: 2,
                tp1ClosePct: 8,
                trailAtrMult: value,
                timeStopBars: 18,
                sweepBufferPips: 0.2,
            }),
        ),
        timeStopBars: [12, 15, 18, 21].map((value) =>
            buildCbpScenario({
                id: `cbp_sweep_ts${value}`,
                executeMinutes: 2,
                tp1ClosePct: 8,
                trailAtrMult: 1.5,
                timeStopBars: value,
                sweepBufferPips: 0.2,
            }),
        ),
        sweepBufferPips: [0.1, 0.15, 0.2, 0.25, 0.3].map((value) =>
            buildCbpScenario({
                id: `cbp_sweep_sw${value.toFixed(2)}`,
                executeMinutes: 2,
                tp1ClosePct: 8,
                trailAtrMult: 1.5,
                timeStopBars: 18,
                sweepBufferPips: value,
            }),
        ),
        tp1ClosePct: [0, 8, 15, 20].map((value) =>
            buildCbpScenario({
                id: `cbp_sweep_tp${value}`,
                executeMinutes: 2,
                tp1ClosePct: value,
                trailAtrMult: 1.5,
                timeStopBars: 18,
                sweepBufferPips: 0.2,
            }),
        ),
    };

    const guardedBlockedVariants: Array<{ key: string; hours: number[] }> = [
        { key: 'none', hours: [] },
        { key: 'h10_11', hours: [10, 11] },
        { key: 'h9_10', hours: [9, 10] },
        { key: 'h11_12', hours: [11, 12] },
        { key: 'h10', hours: [10] },
        { key: 'h11', hours: [11] },
    ];

    const guardedSweepScenarios = {
        blockedHours: guardedBlockedVariants.map((variant) =>
            buildGuardedScenario({
                id: `guarded_blocked_${variant.key}`,
                blockedHours: variant.hours,
                blockedVariant: 'off',
                trailAtrMult: 1.4,
                timeStopBars: 15,
                tp1ClosePct: 20,
            }),
        ),
        trailAtrMult: [1.2, 1.3, 1.4, 1.5, 1.6].map((value) =>
            buildGuardedScenario({
                id: `guarded_tr${value.toFixed(1)}`,
                blockedHours: [10, 11],
                blockedVariant: 'off',
                trailAtrMult: value,
                timeStopBars: 15,
                tp1ClosePct: 20,
            }),
        ),
        timeStopBars: [12, 15, 18].map((value) =>
            buildGuardedScenario({
                id: `guarded_ts${value}`,
                blockedHours: [10, 11],
                blockedVariant: 'off',
                trailAtrMult: 1.4,
                timeStopBars: value,
                tp1ClosePct: 20,
            }),
        ),
        tp1ClosePct: [10, 20, 30].map((value) =>
            buildGuardedScenario({
                id: `guarded_tp${value}`,
                blockedHours: [10, 11],
                blockedVariant: 'off',
                trailAtrMult: 1.4,
                timeStopBars: 15,
                tp1ClosePct: value,
            }),
        ),
    };

    async function evaluateSweep(
        scenarios: Scenario[],
        valueExtractor: (scenario: Scenario) => { value: string; valueNum: number | null },
    ): Promise<SweepRow[]> {
        const rows: SweepRow[] = [];
        for (const scenario of scenarios) {
            const replay = await runScenario({ scenario, window: fullYearWindow, includeTrades: false });
            const { value, valueNum } = valueExtractor(scenario);
            rows.push({
                scenarioId: scenario.id,
                value,
                valueNum,
                trades: replay.summary.trades,
                netPnlUsd: replay.summary.netPnlUsd,
                expectancyR: replay.summary.expectancyR,
                expectancyUsdPerTrade: replay.summary.trades > 0 ? replay.summary.netPnlUsd / replay.summary.trades : 0,
                profitFactor: replay.summary.profitFactor,
                maxDrawdownR: replay.summary.maxDrawdownR,
                winRatePct: replay.summary.winRatePct,
                avgHoldMinutes: replay.summary.avgHoldMinutes,
            });
        }
        return rows;
    }

    const cbpTaskARaw = {
        trailAtrMult: await evaluateSweep(cbpSweepScenarios.trailAtrMult, (scenario) => ({
            value: String(scenario.strategyOverrides.trailAtrMult),
            valueNum: Number(scenario.strategyOverrides.trailAtrMult),
        })),
        timeStopBars: await evaluateSweep(cbpSweepScenarios.timeStopBars, (scenario) => ({
            value: String(scenario.strategyOverrides.timeStopBars),
            valueNum: Number(scenario.strategyOverrides.timeStopBars),
        })),
        sweepBufferPips: await evaluateSweep(cbpSweepScenarios.sweepBufferPips, (scenario) => ({
            value: String(scenario.strategyOverrides.sweepBufferPips),
            valueNum: Number(scenario.strategyOverrides.sweepBufferPips),
        })),
        tp1ClosePct: await evaluateSweep(cbpSweepScenarios.tp1ClosePct, (scenario) => ({
            value: String(scenario.strategyOverrides.tp1ClosePct),
            valueNum: Number(scenario.strategyOverrides.tp1ClosePct),
        })),
    };

    const guardedTaskARaw = {
        blockedHours: await evaluateSweep(guardedSweepScenarios.blockedHours, (scenario) => ({
            value: formatBlockedHours(scenario.blockedHoursBerlin),
            valueNum: null,
        })),
        trailAtrMult: await evaluateSweep(guardedSweepScenarios.trailAtrMult, (scenario) => ({
            value: String(scenario.strategyOverrides.trailAtrMult),
            valueNum: Number(scenario.strategyOverrides.trailAtrMult),
        })),
        timeStopBars: await evaluateSweep(guardedSweepScenarios.timeStopBars, (scenario) => ({
            value: String(scenario.strategyOverrides.timeStopBars),
            valueNum: Number(scenario.strategyOverrides.timeStopBars),
        })),
        tp1ClosePct: await evaluateSweep(guardedSweepScenarios.tp1ClosePct, (scenario) => ({
            value: String(scenario.strategyOverrides.tp1ClosePct),
            valueNum: Number(scenario.strategyOverrides.tp1ClosePct),
        })),
    };

    const taskA = {
        cbp: {
            sweeps: cbpTaskARaw,
            assessments: {
                trailAtrMult: assessSweep(cbpTaskARaw.trailAtrMult, '1.5', true),
                timeStopBars: assessSweep(cbpTaskARaw.timeStopBars, '18', true),
                sweepBufferPips: assessSweep(cbpTaskARaw.sweepBufferPips, '0.2', true),
                tp1ClosePct: assessSweep(cbpTaskARaw.tp1ClosePct, '8', true),
            },
        },
        guarded: {
            sweeps: guardedTaskARaw,
            assessments: {
                blockedHours: assessSweep(guardedTaskARaw.blockedHours, '[10,11]', false),
                trailAtrMult: assessSweep(guardedTaskARaw.trailAtrMult, '1.4', true),
                timeStopBars: assessSweep(guardedTaskARaw.timeStopBars, '15', true),
                tp1ClosePct: assessSweep(guardedTaskARaw.tp1ClosePct, '20', true),
            },
        },
    };

    const rolls = buildRolls(yearFromTs, yearToTs);

    const trainByRoll = new Map<number, ScenarioEval[]>();

    for (const roll of rolls) {
        const rows: ScenarioEval[] = [];
        for (const scenario of selectionUniverse) {
            const replay = await runScenario({ scenario, window: roll.selection });
            rows.push({
                scenarioId: scenario.id,
                family: scenario.family,
                strategyId: scenario.strategyId,
                summary: replay.summary,
            });
        }
        rows.sort(compareScenarioRows);
        trainByRoll.set(roll.rollIndex, rows);
    }

    async function runWalkForwardForGate(gate: number): Promise<WalkForwardSelectionRow[]> {
        const out: WalkForwardSelectionRow[] = [];
        for (const roll of rolls) {
            const trainRows = trainByRoll.get(roll.rollIndex) || [];
            const eligible = trainRows.filter((row) => row.summary.maxDrawdownR <= gate);
            const selected = (eligible.length ? eligible : trainRows)[0];
            if (!selected) continue;
            const scenario = selectionUniverse.find((row) => row.id === selected.scenarioId);
            if (!scenario) continue;
            const fwd2w = await runScenario({ scenario, window: roll.forward2w });
            const fwd4w = await runScenario({ scenario, window: roll.forward4w });

            out.push({
                rollIndex: roll.rollIndex,
                gate,
                selectedScenarioId: selected.scenarioId,
                selectedFamily: selected.family,
                selectedStrategyId: selected.strategyId,
                eligibleCandidates: eligible.length,
                candidateCount: trainRows.length,
                selectionTrades: selected.summary.trades,
                selectionNetPnlUsd: selected.summary.netPnlUsd,
                selectionExpectancyR: selected.summary.expectancyR,
                selectionProfitFactor: selected.summary.profitFactor,
                selectionMaxDrawdownR: selected.summary.maxDrawdownR,
                fwd2wNetPnlUsd: fwd2w.summary.netPnlUsd,
                fwd2wExpectancyR: fwd2w.summary.expectancyR,
                fwd2wProfitFactor: fwd2w.summary.profitFactor,
                fwd2wMaxDrawdownR: fwd2w.summary.maxDrawdownR,
                fwd4wNetPnlUsd: fwd4w.summary.netPnlUsd,
                fwd4wExpectancyR: fwd4w.summary.expectancyR,
                fwd4wProfitFactor: fwd4w.summary.profitFactor,
                fwd4wMaxDrawdownR: fwd4w.summary.maxDrawdownR,
            });
        }
        return out;
    }

    const walkForwardBaseline = await runWalkForwardForGate(BASELINE_DD_GATE);

    function summarizeWalkForward(rows: WalkForwardSelectionRow[]) {
        const profitable2w = rows.filter((row) => row.fwd2wNetPnlUsd > 0).length;
        const profitable4w = rows.filter((row) => row.fwd4wNetPnlUsd > 0).length;
        const selectionExpectancy = rows.map((row) => row.selectionExpectancyR);
        const fwd2wExpectancy = rows.map((row) => row.fwd2wExpectancyR);
        const fwd4wExpectancy = rows.map((row) => row.fwd4wExpectancyR);

        const scenarioCounts = new Map<string, number>();
        for (const row of rows) {
            scenarioCounts.set(row.selectedScenarioId, (scenarioCounts.get(row.selectedScenarioId) || 0) + 1);
        }
        const churn = rows.length > 1
            ? rows.slice(1).filter((row, idx) => row.selectedScenarioId !== rows[idx]!.selectedScenarioId).length
            : 0;

        return {
            rolls: rows.length,
            profitable2wPct: rows.length ? (profitable2w / rows.length) * 100 : 0,
            profitable4wPct: rows.length ? (profitable4w / rows.length) * 100 : 0,
            meanSelectionExpectancyR: mean(selectionExpectancy),
            medianSelectionExpectancyR: median(selectionExpectancy),
            meanForward2wExpectancyR: mean(fwd2wExpectancy),
            medianForward2wExpectancyR: median(fwd2wExpectancy),
            meanForward4wExpectancyR: mean(fwd4wExpectancy),
            medianForward4wExpectancyR: median(fwd4wExpectancy),
            meanDegradation2w: mean(rows.map((row) => row.fwd2wExpectancyR - row.selectionExpectancyR)),
            meanDegradation4w: mean(rows.map((row) => row.fwd4wExpectancyR - row.selectionExpectancyR)),
            uniqueSelectedScenarios: scenarioCounts.size,
            selectedScenarioFrequency: Array.from(scenarioCounts.entries())
                .map(([scenarioId, count]) => ({ scenarioId, count }))
                .sort((a, b) => b.count - a.count),
            churnTransitions: churn,
            churnPct: rows.length > 1 ? (churn / (rows.length - 1)) * 100 : 0,
        };
    }

    const taskB = {
        rolls,
        rows: walkForwardBaseline,
        summary: summarizeWalkForward(walkForwardBaseline),
    };

    const guardedVariantScenarios = guardedBlockedVariants.map((variant) =>
        buildGuardedScenario({
            id: `c_variant_${variant.key}`,
            blockedHours: variant.hours,
            blockedVariant: 'off',
            trailAtrMult: 1.4,
            timeStopBars: 15,
            tp1ClosePct: 20,
        }),
    );

    const cAggregateRows: Array<{
        variant: string;
        trades: number;
        netPnlUsd: number;
        expectancyR: number;
        profitFactor: number | null;
        maxDrawdownR: number;
        winRatePct: number;
        avgHoldMinutes: number;
    }> = [];

    type HourStats = { trades: number; netPnlUsd: number; sumR: number };
    const cByHour: Record<string, Record<string, { trades: number; netPnlUsd: number; expectancyR: number }>> = {};

    for (const scenario of guardedVariantScenarios) {
        const replay = await runScenario({ scenario, window: fullYearWindow, includeTrades: true });
        cAggregateRows.push({
            variant: formatBlockedHours(scenario.blockedHoursBerlin),
            trades: replay.summary.trades,
            netPnlUsd: replay.summary.netPnlUsd,
            expectancyR: replay.summary.expectancyR,
            profitFactor: replay.summary.profitFactor,
            maxDrawdownR: replay.summary.maxDrawdownR,
            winRatePct: replay.summary.winRatePct,
            avgHoldMinutes: replay.summary.avgHoldMinutes,
        });

        const byHour = new Map<number, HourStats>();
        for (const trade of replay.trades) {
            const parts = new Intl.DateTimeFormat('en-GB', {
                timeZone: 'Europe/Berlin',
                hour: '2-digit',
                hour12: false,
            }).formatToParts(new Date(trade.entryTs));
            const hour = Number(parts.find((row) => row.type === 'hour')?.value || '0');
            const prev = byHour.get(hour) || { trades: 0, netPnlUsd: 0, sumR: 0 };
            prev.trades += 1;
            prev.netPnlUsd += trade.pnlUsd;
            prev.sumR += trade.rMultiple;
            byHour.set(hour, prev);
        }

        const hourTable: Record<string, { trades: number; netPnlUsd: number; expectancyR: number }> = {};
        for (const [hour, stats] of Array.from(byHour.entries()).sort((a, b) => a[0] - b[0])) {
            hourTable[String(hour)] = {
                trades: stats.trades,
                netPnlUsd: stats.netPnlUsd,
                expectancyR: stats.trades > 0 ? stats.sumR / stats.trades : 0,
            };
        }
        cByHour[formatBlockedHours(scenario.blockedHoursBerlin)] = hourTable;
    }

    const cWalkForwardRows: Array<{
        rollIndex: number;
        variant: string;
        netPnlUsd: number;
        expectancyR: number;
        profitFactor: number | null;
        maxDrawdownR: number;
    }> = [];

    for (const roll of rolls) {
        for (const scenario of guardedVariantScenarios) {
            const replay = await runScenario({ scenario, window: roll.forward4w });
            cWalkForwardRows.push({
                rollIndex: roll.rollIndex,
                variant: formatBlockedHours(scenario.blockedHoursBerlin),
                netPnlUsd: replay.summary.netPnlUsd,
                expectancyR: replay.summary.expectancyR,
                profitFactor: replay.summary.profitFactor,
                maxDrawdownR: replay.summary.maxDrawdownR,
            });
        }
    }

    const cBestByRoll = new Map<number, { byNet: string; byExpectancy: string }>();
    for (const roll of rolls) {
        const rows = cWalkForwardRows.filter((row) => row.rollIndex === roll.rollIndex);
        const byNet = rows.slice().sort((a, b) => b.netPnlUsd - a.netPnlUsd)[0];
        const byExp = rows.slice().sort((a, b) => b.expectancyR - a.expectancyR)[0];
        cBestByRoll.set(roll.rollIndex, {
            byNet: byNet?.variant || 'n/a',
            byExpectancy: byExp?.variant || 'n/a',
        });
    }

    const cWinsByVariantNet: Record<string, number> = {};
    const cWinsByVariantExp: Record<string, number> = {};
    for (const row of cBestByRoll.values()) {
        cWinsByVariantNet[row.byNet] = (cWinsByVariantNet[row.byNet] || 0) + 1;
        cWinsByVariantExp[row.byExpectancy] = (cWinsByVariantExp[row.byExpectancy] || 0) + 1;
    }

    const taskC = {
        aggregate: cAggregateRows,
        byBerlinHour: cByHour,
        walkForward4wRows: cWalkForwardRows,
        bestVariantWins: {
            byNetPnl: cWinsByVariantNet,
            byExpectancy: cWinsByVariantExp,
        },
    };

    const allCbpSweepRows = [
        ...cbpTaskARaw.trailAtrMult,
        ...cbpTaskARaw.timeStopBars,
        ...cbpTaskARaw.sweepBufferPips,
        ...cbpTaskARaw.tp1ClosePct,
    ];
    const allGuardedSweepRows = [
        ...guardedTaskARaw.blockedHours,
        ...guardedTaskARaw.trailAtrMult,
        ...guardedTaskARaw.timeStopBars,
        ...guardedTaskARaw.tp1ClosePct,
    ];

    const cbpBestNearby = allCbpSweepRows
        .filter((row) => row.scenarioId !== cbpDeployedScenario.id)
        .sort((a, b) => b.expectancyR - a.expectancyR)[0];
    const guardedBestNearby = allGuardedSweepRows
        .filter((row) => row.scenarioId !== guardedDeployedScenario.id)
        .sort((a, b) => b.expectancyR - a.expectancyR)[0];

    const cbpBestNearbyScenario = cbpBestNearby
        ? [...cbpSweepScenarios.trailAtrMult, ...cbpSweepScenarios.timeStopBars, ...cbpSweepScenarios.sweepBufferPips, ...cbpSweepScenarios.tp1ClosePct]
              .find((row) => row.id === cbpBestNearby.scenarioId) || cbpDeployedScenario
        : cbpDeployedScenario;

    const guardedBestNearbyScenario = guardedBestNearby
        ? [...guardedSweepScenarios.blockedHours, ...guardedSweepScenarios.trailAtrMult, ...guardedSweepScenarios.timeStopBars, ...guardedSweepScenarios.tp1ClosePct]
              .find((row) => row.id === guardedBestNearby.scenarioId) || guardedDeployedScenario
        : guardedDeployedScenario;

    const stressTargets = [
        { label: 'cbp_deployed', scenario: cbpDeployedScenario },
        { label: 'guarded_deployed', scenario: guardedDeployedScenario },
        { label: 'cbp_best_nearby', scenario: cbpBestNearbyScenario },
        { label: 'guarded_best_nearby', scenario: guardedBestNearbyScenario },
    ];

    const taskDRows: Array<{
        strategyLabel: string;
        stressId: string;
        netPnlUsd: number;
        expectancyR: number;
        profitFactor: number | null;
        maxDrawdownR: number;
        netPnlDeltaPctVsBaseline: number;
        expectancyDeltaPctVsBaseline: number;
    }> = [];

    for (const target of stressTargets) {
        const baselineReplay = await runScenario({ scenario: target.scenario, window: fullYearWindow, stress: STRESS_CASES[0] });
        for (const stress of STRESS_CASES) {
            const replay = await runScenario({ scenario: target.scenario, window: fullYearWindow, stress });
            taskDRows.push({
                strategyLabel: target.label,
                stressId: stress.id,
                netPnlUsd: replay.summary.netPnlUsd,
                expectancyR: replay.summary.expectancyR,
                profitFactor: replay.summary.profitFactor,
                maxDrawdownR: replay.summary.maxDrawdownR,
                netPnlDeltaPctVsBaseline:
                    Math.abs(baselineReplay.summary.netPnlUsd) > 1e-9
                        ? ((replay.summary.netPnlUsd - baselineReplay.summary.netPnlUsd) / Math.abs(baselineReplay.summary.netPnlUsd)) * 100
                        : 0,
                expectancyDeltaPctVsBaseline:
                    Math.abs(baselineReplay.summary.expectancyR) > 1e-9
                        ? ((replay.summary.expectancyR - baselineReplay.summary.expectancyR) / Math.abs(baselineReplay.summary.expectancyR)) * 100
                        : 0,
            });
        }
    }

    const taskD = {
        rows: taskDRows,
        unsupportedStressTests: ['delayed entry / missed best-case fills are not modeled directly in current replay harness'],
    };

    const cbpDeployedReplay = await runScenario({ scenario: cbpDeployedScenario, window: fullYearWindow, includeTrades: true });
    const guardedDeployedReplay = await runScenario({ scenario: guardedDeployedScenario, window: fullYearWindow, includeTrades: true });

    function monthlyFromTrades(trades: ScalpReplayTrade[]): MonthlyStats[] {
        const byMonth = new Map<string, { trades: number; netPnlUsd: number; sumR: number }>();
        for (const trade of trades) {
            const key = toMonthKey(trade.exitTs);
            const prev = byMonth.get(key) || { trades: 0, netPnlUsd: 0, sumR: 0 };
            prev.trades += 1;
            prev.netPnlUsd += trade.pnlUsd;
            prev.sumR += trade.rMultiple;
            byMonth.set(key, prev);
        }
        return Array.from(byMonth.entries())
            .map(([month, row]) => ({
                month,
                trades: row.trades,
                netPnlUsd: row.netPnlUsd,
                expectancyR: row.trades > 0 ? row.sumR / row.trades : 0,
            }))
            .sort((a, b) => a.month.localeCompare(b.month));
    }

    const monthlyCbp = monthlyFromTrades(cbpDeployedReplay.trades);
    const monthlyGuarded = monthlyFromTrades(guardedDeployedReplay.trades);
    const monthSet = new Set([...monthlyCbp.map((row) => row.month), ...monthlyGuarded.map((row) => row.month)]);
    const monthKeys = Array.from(monthSet).sort();

    const cbpMonthMap = new Map(monthlyCbp.map((row) => [row.month, row]));
    const guardedMonthMap = new Map(monthlyGuarded.map((row) => [row.month, row]));

    const alignedCbp = monthKeys.map((month) => cbpMonthMap.get(month)?.netPnlUsd ?? 0);
    const alignedGuarded = monthKeys.map((month) => guardedMonthMap.get(month)?.netPnlUsd ?? 0);
    const monthlyCorr = pearsonCorrelation(alignedCbp, alignedGuarded);

    const combinedByMonth: MonthlyStats[] = monthKeys.map((month) => {
        const cbp = cbpMonthMap.get(month);
        const guarded = guardedMonthMap.get(month);
        const trades = (cbp?.trades || 0) + (guarded?.trades || 0);
        const netPnlUsd = (cbp?.netPnlUsd || 0) + (guarded?.netPnlUsd || 0);
        const sumR = (cbp?.expectancyR || 0) * (cbp?.trades || 0) + (guarded?.expectancyR || 0) * (guarded?.trades || 0);
        return {
            month,
            trades,
            netPnlUsd,
            expectancyR: trades > 0 ? sumR / trades : 0,
        };
    });

    function overlapMinutes(a: ScalpReplayTrade[], b: ScalpReplayTrade[]): number {
        const aIntervals = a.map((row) => [row.entryTs, row.exitTs] as const).sort((x, y) => x[0] - y[0]);
        const bIntervals = b.map((row) => [row.entryTs, row.exitTs] as const).sort((x, y) => x[0] - y[0]);
        let i = 0;
        let j = 0;
        let overlapMs = 0;
        while (i < aIntervals.length && j < bIntervals.length) {
            const [aStart, aEnd] = aIntervals[i]!;
            const [bStart, bEnd] = bIntervals[j]!;
            const start = Math.max(aStart, bStart);
            const end = Math.min(aEnd, bEnd);
            if (end > start) overlapMs += end - start;
            if (aEnd < bEnd) i += 1;
            else j += 1;
        }
        return overlapMs / 60_000;
    }

    const cbpMinutes = cbpDeployedReplay.trades.reduce((acc, row) => acc + (row.exitTs - row.entryTs) / 60_000, 0);
    const guardedMinutes = guardedDeployedReplay.trades.reduce((acc, row) => acc + (row.exitTs - row.entryTs) / 60_000, 0);
    const overlappedMinutes = overlapMinutes(cbpDeployedReplay.trades, guardedDeployedReplay.trades);
    const unionMinutes = cbpMinutes + guardedMinutes - overlappedMinutes;

    const combinedTradesChrono = [...cbpDeployedReplay.trades, ...guardedDeployedReplay.trades].sort((a, b) => a.exitTs - b.exitTs);
    const taskE = {
        monthly: {
            cbp: monthlyCbp,
            guarded: monthlyGuarded,
            combined: combinedByMonth,
        },
        returnCorrelationMonthlyPnl: monthlyCorr,
        overlap: {
            cbpActiveMinutes: cbpMinutes,
            guardedActiveMinutes: guardedMinutes,
            overlappedMinutes,
            overlapPctOfUnion: unionMinutes > 0 ? (overlappedMinutes / unionMinutes) * 100 : 0,
        },
        portfolioComparison: {
            cbp: {
                netPnlUsd: cbpDeployedReplay.summary.netPnlUsd,
                maxDrawdownUsd: maxDrawdownFromPnl(cbpDeployedReplay.trades.map((row) => row.pnlUsd)),
            },
            guarded: {
                netPnlUsd: guardedDeployedReplay.summary.netPnlUsd,
                maxDrawdownUsd: maxDrawdownFromPnl(guardedDeployedReplay.trades.map((row) => row.pnlUsd)),
            },
            combined: {
                netPnlUsd: cbpDeployedReplay.summary.netPnlUsd + guardedDeployedReplay.summary.netPnlUsd,
                maxDrawdownUsd: maxDrawdownFromPnl(combinedTradesChrono.map((row) => row.pnlUsd)),
            },
        },
    };

    const gateRowsByGate: Record<string, WalkForwardSelectionRow[]> = {};
    const gateSummaryByGate: Record<string, unknown> = {};
    for (const gate of DD_GATES) {
        const rows = gate === BASELINE_DD_GATE ? walkForwardBaseline : await runWalkForwardForGate(gate);
        gateRowsByGate[String(gate)] = rows;
        const survivors = rows.map((row) => row.eligibleCandidates);
        const selectedCounts = new Map<string, number>();
        for (const row of rows) {
            selectedCounts.set(row.selectedScenarioId, (selectedCounts.get(row.selectedScenarioId) || 0) + 1);
        }
        gateSummaryByGate[String(gate)] = {
            rolls: rows.length,
            survivorCount: {
                mean: mean(survivors),
                median: median(survivors),
                min: Math.min(...survivors),
                max: Math.max(...survivors),
            },
            selectedScenarioFrequency: Array.from(selectedCounts.entries())
                .map(([scenarioId, count]) => ({ scenarioId, count }))
                .sort((a, b) => b.count - a.count),
            forward2w: {
                meanNetPnlUsd: mean(rows.map((row) => row.fwd2wNetPnlUsd)),
                meanExpectancyR: mean(rows.map((row) => row.fwd2wExpectancyR)),
                profitablePct: rows.length ? (rows.filter((row) => row.fwd2wNetPnlUsd > 0).length / rows.length) * 100 : 0,
            },
            forward4w: {
                meanNetPnlUsd: mean(rows.map((row) => row.fwd4wNetPnlUsd)),
                meanExpectancyR: mean(rows.map((row) => row.fwd4wExpectancyR)),
                profitablePct: rows.length ? (rows.filter((row) => row.fwd4wNetPnlUsd > 0).length / rows.length) * 100 : 0,
            },
        };
    }

    const taskF = {
        rowsByGate: gateRowsByGate,
        summaryByGate: gateSummaryByGate,
    };

    const cbpAssessments = Object.values(taskA.cbp.assessments);
    const guardedAssessments = Object.values(taskA.guarded.assessments);

    function verdictFromAssessments(
        assessments: SweepAssessment[],
        forwardSummary: ReturnType<typeof summarizeWalkForward>,
        stressRows: Array<{ strategyLabel: string; stressId: string; expectancyR: number }>,
        stressLabel: string,
    ): 'likely structural' | 'possibly structural but parameter-fragile' | 'likely overfit' {
        const stable = assessments.filter((row) => row.judgment === 'stable plateau').length;
        const narrow = assessments.filter((row) => row.judgment === 'narrow peak / likely overfit').length;
        const worstStress = stressRows
            .filter((row) => row.strategyLabel === stressLabel)
            .reduce((acc, row) => Math.min(acc, row.expectancyR), Number.POSITIVE_INFINITY);
        if (
            stable >= Math.ceil(assessments.length / 2) &&
            forwardSummary.profitable4wPct >= 55 &&
            forwardSummary.meanForward4wExpectancyR > 0 &&
            worstStress > 0
        ) {
            return 'likely structural';
        }
        if (narrow >= 2 || forwardSummary.meanForward4wExpectancyR <= 0) {
            return 'likely overfit';
        }
        return 'possibly structural but parameter-fragile';
    }

    const blocked1011Aggregate = cAggregateRows.find((row) => row.variant === '[10,11]');
    const blockedNoneAggregate = cAggregateRows.find((row) => row.variant === 'none');
    const blockedWinsByExp = taskC.bestVariantWins.byExpectancy['[10,11]'] || 0;
    const blockedVerdict: 'supported' | 'unclear' | 'likely curve-fit' =
        blocked1011Aggregate &&
        blockedNoneAggregate &&
        blocked1011Aggregate.expectancyR > blockedNoneAggregate.expectancyR &&
        blockedWinsByExp >= Math.ceil(rolls.length * 0.4)
            ? blockedWinsByExp >= Math.ceil(rolls.length * 0.6)
                ? 'supported'
                : 'unclear'
            : blocked1011Aggregate && blockedNoneAggregate && blocked1011Aggregate.expectancyR <= blockedNoneAggregate.expectancyR
              ? 'likely curve-fit'
              : 'unclear';

    const selectionVerdict: 'generalizes forward' | 'weakly generalizes' | 'mostly buys yesterday\'s luck' =
        taskB.summary.meanForward4wExpectancyR > 0 && taskB.summary.profitable4wPct >= 60
            ? 'generalizes forward'
            : taskB.summary.meanForward4wExpectancyR > -0.02 && taskB.summary.profitable4wPct >= 45
              ? 'weakly generalizes'
              : 'mostly buys yesterday\'s luck';

    const deployedStressRows = taskDRows.filter(
        (row) => row.strategyLabel === 'cbp_deployed' || row.strategyLabel === 'guarded_deployed',
    );
    const harshStressRows = deployedStressRows.filter((row) => row.stressId === 'slippage_x2_spread_1.50x');
    const harshPositive = harshStressRows.filter((row) => row.expectancyR > 0).length;
    const executionVerdict: 'survives stress' | 'borderline' | 'too thin to trust live' =
        harshPositive === harshStressRows.length
            ? 'survives stress'
            : harshPositive >= 1
              ? 'borderline'
              : 'too thin to trust live';

    const diversificationVerdict: 'distinct edges' | 'partial overlap' | 'mostly same exposure' =
        taskE.returnCorrelationMonthlyPnl !== null && taskE.returnCorrelationMonthlyPnl < 0.3 && taskE.overlap.overlapPctOfUnion < 40
            ? 'distinct edges'
            : taskE.returnCorrelationMonthlyPnl !== null && taskE.returnCorrelationMonthlyPnl < 0.7
              ? 'partial overlap'
              : 'mostly same exposure';

    const finalConclusions = {
        cbpVerdict: verdictFromAssessments(cbpAssessments, taskB.summary, taskDRows, 'cbp_deployed'),
        guardedVerdict: verdictFromAssessments(guardedAssessments, taskB.summary, taskDRows, 'guarded_deployed'),
        blockedHours1011Verdict: blockedVerdict,
        selectionLogicVerdict: selectionVerdict,
        executionRobustnessVerdict: executionVerdict,
        diversificationVerdict,
    };

    const output: ScriptOutput = {
        generatedAtIso: new Date().toISOString(),
        symbol: BTC_SYMBOL,
        dataset: {
            yearFromTs,
            yearToTs,
            candles15m: sliceByTs(replay15mAll, yearFromTs, yearToTs).length,
            candles1m: sliceByTs(replay1mAll, yearFromTs, yearToTs).length,
        },
        assumptions: {
            walkForwardSelectionDays: SELECTION_DAYS,
            walkForwardForward2wDays: FORWARD_2W_DAYS,
            walkForwardForward4wDays: FORWARD_4W_DAYS,
            walkForwardStepDays: ROLL_STEP_DAYS,
            baselineDdGate: BASELINE_DD_GATE,
            selectionRanking:
                'sort by netR desc, then profitFactor desc, then maxDrawdownR asc, then trades desc, then winRate desc; choose top eligible by DD gate, fallback to top overall',
        },
        taskA,
        taskB,
        taskC,
        taskD,
        taskE,
        taskF,
        finalConclusions,
    };

    const jsonPath = path.join(outputRoot, 'report.json');
    await writeFile(jsonPath, `${JSON.stringify(output, null, 2)}\n`, 'utf8');

    const aCsvRows: Array<Array<string | number | null>> = [];
    for (const [sweepName, rows] of Object.entries(taskA.cbp.sweeps)) {
        for (const row of rows as SweepRow[]) {
            aCsvRows.push([
                'cbp',
                sweepName,
                row.value,
                row.trades,
                row.netPnlUsd,
                row.expectancyR,
                row.expectancyUsdPerTrade,
                row.profitFactor,
                row.maxDrawdownR,
                row.winRatePct,
                row.avgHoldMinutes,
            ]);
        }
    }
    for (const [sweepName, rows] of Object.entries(taskA.guarded.sweeps)) {
        for (const row of rows as SweepRow[]) {
            aCsvRows.push([
                'guarded',
                sweepName,
                row.value,
                row.trades,
                row.netPnlUsd,
                row.expectancyR,
                row.expectancyUsdPerTrade,
                row.profitFactor,
                row.maxDrawdownR,
                row.winRatePct,
                row.avgHoldMinutes,
            ]);
        }
    }
    await writeFile(
        path.join(outputRoot, 'taskA_sweeps.csv'),
        createCsv(
            [
                'family',
                'sweep',
                'value',
                'trades',
                'netPnlUsd',
                'expectancyR',
                'expectancyUsdPerTrade',
                'profitFactor',
                'maxDrawdownR',
                'winRatePct',
                'avgHoldMinutes',
            ],
            aCsvRows,
        ),
        'utf8',
    );

    await writeFile(
        path.join(outputRoot, 'taskB_walkforward.csv'),
        createCsv(
            [
                'rollIndex',
                'gate',
                'selectedScenarioId',
                'selectedFamily',
                'selectedStrategyId',
                'eligibleCandidates',
                'candidateCount',
                'selectionTrades',
                'selectionNetPnlUsd',
                'selectionExpectancyR',
                'selectionProfitFactor',
                'selectionMaxDrawdownR',
                'fwd2wNetPnlUsd',
                'fwd2wExpectancyR',
                'fwd2wProfitFactor',
                'fwd2wMaxDrawdownR',
                'fwd4wNetPnlUsd',
                'fwd4wExpectancyR',
                'fwd4wProfitFactor',
                'fwd4wMaxDrawdownR',
            ],
            taskB.rows.map((row: WalkForwardSelectionRow) => [
                row.rollIndex,
                row.gate,
                row.selectedScenarioId,
                row.selectedFamily,
                row.selectedStrategyId,
                row.eligibleCandidates,
                row.candidateCount,
                row.selectionTrades,
                row.selectionNetPnlUsd,
                row.selectionExpectancyR,
                row.selectionProfitFactor,
                row.selectionMaxDrawdownR,
                row.fwd2wNetPnlUsd,
                row.fwd2wExpectancyR,
                row.fwd2wProfitFactor,
                row.fwd2wMaxDrawdownR,
                row.fwd4wNetPnlUsd,
                row.fwd4wExpectancyR,
                row.fwd4wProfitFactor,
                row.fwd4wMaxDrawdownR,
            ]),
        ),
        'utf8',
    );

    await writeFile(
        path.join(outputRoot, 'taskC_falsification_walkforward4w.csv'),
        createCsv(
            ['rollIndex', 'variant', 'netPnlUsd', 'expectancyR', 'profitFactor', 'maxDrawdownR'],
            taskC.walkForward4wRows.map((row) => [
                row.rollIndex,
                row.variant,
                row.netPnlUsd,
                row.expectancyR,
                row.profitFactor,
                row.maxDrawdownR,
            ]),
        ),
        'utf8',
    );

    await writeFile(
        path.join(outputRoot, 'taskD_stress.csv'),
        createCsv(
            [
                'strategyLabel',
                'stressId',
                'netPnlUsd',
                'expectancyR',
                'profitFactor',
                'maxDrawdownR',
                'netPnlDeltaPctVsBaseline',
                'expectancyDeltaPctVsBaseline',
            ],
            taskD.rows.map((row) => [
                row.strategyLabel,
                row.stressId,
                row.netPnlUsd,
                row.expectancyR,
                row.profitFactor,
                row.maxDrawdownR,
                row.netPnlDeltaPctVsBaseline,
                row.expectancyDeltaPctVsBaseline,
            ]),
        ),
        'utf8',
    );

    const monthlyRows: Array<Array<string | number | null>> = [];
    for (const row of taskE.monthly.cbp) monthlyRows.push(['cbp', row.month, row.trades, row.netPnlUsd, row.expectancyR]);
    for (const row of taskE.monthly.guarded) monthlyRows.push(['guarded', row.month, row.trades, row.netPnlUsd, row.expectancyR]);
    for (const row of taskE.monthly.combined) monthlyRows.push(['combined', row.month, row.trades, row.netPnlUsd, row.expectancyR]);
    await writeFile(
        path.join(outputRoot, 'taskE_monthly.csv'),
        createCsv(['series', 'month', 'trades', 'netPnlUsd', 'expectancyR'], monthlyRows),
        'utf8',
    );

    for (const gate of DD_GATES) {
        const rows = gateRowsByGate[String(gate)] || [];
        await writeFile(
            path.join(outputRoot, `taskF_gate_${gate}.csv`),
            createCsv(
                [
                    'rollIndex',
                    'selectedScenarioId',
                    'selectedFamily',
                    'eligibleCandidates',
                    'selectionExpectancyR',
                    'fwd2wExpectancyR',
                    'fwd4wExpectancyR',
                    'fwd2wNetPnlUsd',
                    'fwd4wNetPnlUsd',
                ],
                rows.map((row) => [
                    row.rollIndex,
                    row.selectedScenarioId,
                    row.selectedFamily,
                    row.eligibleCandidates,
                    row.selectionExpectancyR,
                    row.fwd2wExpectancyR,
                    row.fwd4wExpectancyR,
                    row.fwd2wNetPnlUsd,
                    row.fwd4wNetPnlUsd,
                ]),
            ),
            'utf8',
        );
    }

    console.log(JSON.stringify({
        ok: true,
        outputRoot,
        reportJson: jsonPath,
        files: [
            'report.json',
            'taskA_sweeps.csv',
            'taskB_walkforward.csv',
            'taskC_falsification_walkforward4w.csv',
            'taskD_stress.csv',
            'taskE_monthly.csv',
            ...DD_GATES.map((gate) => `taskF_gate_${gate}.csv`),
        ],
        replayCacheSize: replayCache.size,
        rolls: rolls.length,
        selectionUniverseSize: selectionUniverse.length,
        conclusions: finalConclusions,
    }, null, 2));
}

main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
});
