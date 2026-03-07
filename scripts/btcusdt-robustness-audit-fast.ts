#!/usr/bin/env node
import { access, mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { spawn } from 'node:child_process';

import type { ScalpReplayRuntimeConfig } from '../lib/scalp/replay/types';
import { COMPRESSION_BREAKOUT_PULLBACK_M15_M3_STRATEGY_ID } from '../lib/scalp/strategies/compressionBreakoutPullbackM15M3';
import { REGIME_PULLBACK_M15_M3_BTCUSDT_STRATEGY_ID } from '../lib/scalp/strategies/regimePullbackM15M3BtcusdtGuarded';

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

type StressId = 'baseline' | 'slippage_x2' | 'spread_1.25x' | 'spread_1.50x' | 'slippage_x2_spread_1.50x';

type ReplayTrade = {
    id: string;
    side: 'BUY' | 'SELL';
    entryTs: number;
    exitTs: number;
    holdMinutes: number;
    entryPrice: number;
    exitPrice: number;
    exitReason: string;
    rMultiple: number;
    pnlUsd: number;
};

type ReplaySummary = {
    trades: number;
    wins: number;
    losses: number;
    winRatePct: number;
    netR: number;
    expectancyR: number;
    profitFactor: number | null;
    maxDrawdownR: number;
    avgHoldMinutes: number;
    netPnlUsd: number;
};

type ReplayFile = {
    scenarioId: string;
    family: ScenarioFamily;
    strategyId: string;
    stressId: StressId;
    yearFromTs: number;
    yearToTs: number;
    elapsedMs: number;
    summary: ReplaySummary;
    trades: ReplayTrade[];
};

type SliceSummary = {
    trades: number;
    wins: number;
    losses: number;
    winRatePct: number;
    netR: number;
    expectancyR: number;
    expectancyUsdPerTrade: number;
    grossProfitR: number;
    grossLossAbsR: number;
    profitFactor: number | null;
    maxDrawdownR: number;
    avgHoldMinutes: number;
    netPnlUsd: number;
};

type WindowRange = {
    fromTs: number;
    toTs: number;
};

type Roll = {
    rollIndex: number;
    selection: WindowRange;
    forward2w: WindowRange;
    forward4w: WindowRange;
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

const DAY_MS = 24 * 60 * 60 * 1000;
const SELECTION_DAYS = 90;
const FORWARD_2W_DAYS = 14;
const FORWARD_4W_DAYS = 28;
const ROLL_STEP_DAYS = 14;
const BASELINE_DD_GATE = 8;
const DD_GATES = [8, 10, 12];

const STRESS_CASES: Array<{ id: StressId; spreadMult: number; slippageMult: number }> = [
    { id: 'baseline', spreadMult: 1, slippageMult: 1 },
    { id: 'slippage_x2', spreadMult: 1, slippageMult: 2 },
    { id: 'spread_1.25x', spreadMult: 1.25, slippageMult: 1 },
    { id: 'spread_1.50x', spreadMult: 1.5, slippageMult: 1 },
    { id: 'slippage_x2_spread_1.50x', spreadMult: 1.5, slippageMult: 2 },
];

const CBP_DEPLOYED_ID = 'cbp_btc_dd8_e2_p8_tr1.5_ts18_sw0.20';
const GUARDED_DEPLOYED_ID = 'guarded_high_pf_default';

function toMonthKey(ts: number): string {
    const d = new Date(ts);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    return `${y}-${m}`;
}

function mean(values: number[]): number {
    if (!values.length) return NaN;
    return values.reduce((acc, value) => acc + value, 0) / values.length;
}

function median(values: number[]): number {
    if (!values.length) return NaN;
    const sorted = values.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2) return sorted[mid]!;
    return (sorted[mid - 1]! + sorted[mid]!) / 2;
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

function maxDrawdownFromR(rs: number[]): number {
    let equity = 0;
    let peak = 0;
    let maxDd = 0;
    for (const r of rs) {
        equity += r;
        peak = Math.max(peak, equity);
        maxDd = Math.max(maxDd, peak - equity);
    }
    return maxDd;
}

function formatBlockedHours(hours: number[] | null | undefined): string {
    if (!hours || !hours.length) return 'none';
    return `[${hours.join(',')}]`;
}

function buildCbpScenario(params: {
    id: string;
    executeMinutes?: number;
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
        executeMinutes: params.executeMinutes ?? 2,
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

function dedupeScenarios(rows: Scenario[]): Scenario[] {
    const map = new Map<string, Scenario>();
    for (const row of rows) map.set(row.id, row);
    return Array.from(map.values());
}

function toCsvCell(value: string | number | boolean | null | undefined): string {
    if (value === null || value === undefined) return '';
    const raw = String(value);
    if (!/[",\n]/.test(raw)) return raw;
    return `"${raw.replace(/"/g, '""')}"`;
}

function createCsv(headers: string[], rows: Array<Array<string | number | boolean | null | undefined>>): string {
    const lines = [headers.map(toCsvCell).join(',')];
    for (const row of rows) lines.push(row.map(toCsvCell).join(','));
    return `${lines.join('\n')}\n`;
}

async function exists(filePath: string): Promise<boolean> {
    try {
        await access(filePath);
        return true;
    } catch {
        return false;
    }
}

async function runWorkerJob(params: {
    manifestPath: string;
    scenarioId: string;
    stressId: StressId;
    outPath: string;
}): Promise<void> {
    if (await exists(params.outPath)) return;

    await new Promise<void>((resolve, reject) => {
        const child = spawn(
            'node',
            [
                '--import',
                'tsx',
                'scripts/btcusdt-scenario-year-run.ts',
                '--manifest',
                params.manifestPath,
                '--scenarioId',
                params.scenarioId,
                '--stressId',
                params.stressId,
                '--out',
                params.outPath,
            ],
            { cwd: process.cwd(), stdio: ['ignore', 'pipe', 'pipe'] },
        );
        let stderr = '';
        child.stderr.on('data', (buf) => {
            stderr += String(buf);
        });
        child.on('error', reject);
        child.on('close', (code) => {
            if (code === 0) resolve();
            else reject(new Error(`Worker failed for ${params.scenarioId}/${params.stressId}: ${stderr.trim()}`));
        });
    });
}

async function runWithConcurrency<T>(
    items: T[],
    concurrency: number,
    fn: (item: T, index: number) => Promise<void>,
): Promise<void> {
    if (!items.length) return;
    const limit = Math.max(1, Math.floor(concurrency));
    let cursor = 0;
    async function worker() {
        while (true) {
            const index = cursor;
            cursor += 1;
            if (index >= items.length) return;
            await fn(items[index]!, index);
        }
    }
    await Promise.all(Array.from({ length: Math.min(limit, items.length) }, () => worker()));
}

function summarizeTrades(trades: ReplayTrade[], fromTs: number, toTs: number): SliceSummary {
    const filtered = trades
        .filter((row) => row.entryTs >= fromTs && row.entryTs < toTs)
        .sort((a, b) => a.exitTs - b.exitTs);
    const tradeCount = filtered.length;
    const wins = filtered.filter((row) => row.rMultiple > 0).length;
    const losses = filtered.filter((row) => row.rMultiple < 0).length;
    const netR = filtered.reduce((acc, row) => acc + row.rMultiple, 0);
    const grossProfitR = filtered.reduce((acc, row) => acc + Math.max(0, row.rMultiple), 0);
    const grossLossAbsR = filtered.reduce((acc, row) => acc + Math.max(0, -row.rMultiple), 0);
    const netPnlUsd = filtered.reduce((acc, row) => acc + row.pnlUsd, 0);
    const avgHoldMinutes = tradeCount ? filtered.reduce((acc, row) => acc + row.holdMinutes, 0) / tradeCount : 0;
    const expectancyR = tradeCount ? netR / tradeCount : 0;
    const expectancyUsdPerTrade = tradeCount ? netPnlUsd / tradeCount : 0;
    const profitFactor = grossLossAbsR > 0 ? grossProfitR / grossLossAbsR : null;
    const maxDrawdownR = maxDrawdownFromR(filtered.map((row) => row.rMultiple));
    return {
        trades: tradeCount,
        wins,
        losses,
        winRatePct: tradeCount ? (wins / tradeCount) * 100 : 0,
        netR,
        expectancyR,
        expectancyUsdPerTrade,
        grossProfitR,
        grossLossAbsR,
        profitFactor,
        maxDrawdownR,
        avgHoldMinutes,
        netPnlUsd,
    };
}

function compareSelectionRows(a: { summary: SliceSummary }, b: { summary: SliceSummary }): number {
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

    const deployedIndex = sorted.findIndex((row) => row.value === deployedValue);
    const bestIndex = sorted.findIndex((row) => row.expectancyR === bestExpectancy);

    let deployedLocation: SweepAssessment['deployedLocation'] = 'categorical / n-a';
    if (deployedIndex >= 0) {
        if (good.length === 1 && deployedIndex === bestIndex) {
            deployedLocation = 'single sharp optimum';
        } else if (good.some((row) => row.value === deployedValue)) {
            const idxs = good
                .map((row) => sorted.findIndex((r) => r.value === row.value))
                .filter((idx) => idx >= 0)
                .sort((x, y) => x - y);
            const minIdx = idxs[0]!;
            const maxIdx = idxs[idxs.length - 1]!;
            deployedLocation = deployedIndex > minIdx && deployedIndex < maxIdx ? 'near center of good region' : 'at an edge';
        } else if (deployedIndex === 0 || deployedIndex === sorted.length - 1) {
            deployedLocation = 'at an edge';
        } else {
            deployedLocation = 'near center of good region';
        }
    }

    return {
        judgment,
        deployedLocation,
        deployedValue,
        bestValue: sorted.find((row) => row.expectancyR === bestExpectancy)?.value || deployedValue,
    };
}

function buildRolls(yearFromTs: number, yearToTs: number): Roll[] {
    const selMs = SELECTION_DAYS * DAY_MS;
    const fwd2wMs = FORWARD_2W_DAYS * DAY_MS;
    const fwd4wMs = FORWARD_4W_DAYS * DAY_MS;
    const stepMs = ROLL_STEP_DAYS * DAY_MS;
    const rolls: Roll[] = [];
    let selectionEnd = yearFromTs + selMs;
    let rollIndex = 1;
    while (selectionEnd + fwd4wMs <= yearToTs) {
        rolls.push({
            rollIndex,
            selection: { fromTs: selectionEnd - selMs, toTs: selectionEnd },
            forward2w: { fromTs: selectionEnd, toTs: selectionEnd + fwd2wMs },
            forward4w: { fromTs: selectionEnd, toTs: selectionEnd + fwd4wMs },
        });
        selectionEnd += stepMs;
        rollIndex += 1;
    }
    return rolls;
}

function summarizeWalkForward(rows: WalkForwardSelectionRow[]) {
    if (!rows.length) {
        return {
            rolls: 0,
            profitable2wPct: 0,
            profitable4wPct: 0,
            meanForward2wExpectancyR: 0,
            meanForward4wExpectancyR: 0,
            medianForward2wExpectancyR: 0,
            medianForward4wExpectancyR: 0,
            meanDegradation2w: 0,
            meanDegradation4w: 0,
            churnTransitions: 0,
            repeatTransitions: 0,
            selectedFrequency: [] as Array<{ scenarioId: string; count: number }>,
        };
    }
    const selectedFrequency = new Map<string, number>();
    let repeats = 0;
    for (let i = 0; i < rows.length; i += 1) {
        const row = rows[i]!;
        selectedFrequency.set(row.selectedScenarioId, (selectedFrequency.get(row.selectedScenarioId) || 0) + 1);
        if (i > 0 && rows[i - 1]!.selectedScenarioId === row.selectedScenarioId) repeats += 1;
    }
    return {
        rolls: rows.length,
        profitable2wPct: (rows.filter((row) => row.fwd2wNetPnlUsd > 0).length / rows.length) * 100,
        profitable4wPct: (rows.filter((row) => row.fwd4wNetPnlUsd > 0).length / rows.length) * 100,
        meanForward2wExpectancyR: mean(rows.map((row) => row.fwd2wExpectancyR)),
        meanForward4wExpectancyR: mean(rows.map((row) => row.fwd4wExpectancyR)),
        medianForward2wExpectancyR: median(rows.map((row) => row.fwd2wExpectancyR)),
        medianForward4wExpectancyR: median(rows.map((row) => row.fwd4wExpectancyR)),
        meanDegradation2w: mean(rows.map((row) => row.fwd2wExpectancyR - row.selectionExpectancyR)),
        meanDegradation4w: mean(rows.map((row) => row.fwd4wExpectancyR - row.selectionExpectancyR)),
        churnTransitions: rows.length > 1 ? rows.length - 1 - repeats : 0,
        repeatTransitions: repeats,
        selectedFrequency: Array.from(selectedFrequency.entries())
            .map(([scenarioId, count]) => ({ scenarioId, count }))
            .sort((a, b) => b.count - a.count),
    };
}

function toBerlinHour(ts: number): number {
    const fmt = new Intl.DateTimeFormat('en-GB', {
        timeZone: 'Europe/Berlin',
        hour: '2-digit',
        hour12: false,
    });
    return Number(fmt.format(new Date(ts)));
}

async function main() {
    const outputRoot = process.env.BTCUSDT_AUDIT_OUTPUT_ROOT
        ? path.resolve(process.env.BTCUSDT_AUDIT_OUTPUT_ROOT)
        : path.resolve('/tmp', `btcusdt-robustness-audit-fast-${new Date().toISOString().replace(/[:.]/g, '-')}`);
    const runsDir = path.join(outputRoot, 'runs');
    await mkdir(runsDir, { recursive: true });

    const cbpDeployed = buildCbpScenario({
        id: CBP_DEPLOYED_ID,
        executeMinutes: 2,
        tp1ClosePct: 8,
        trailAtrMult: 1.5,
        timeStopBars: 18,
        sweepBufferPips: 0.2,
    });
    const guardedDeployed = buildGuardedScenario({
        id: GUARDED_DEPLOYED_ID,
        blockedHours: [10, 11],
        blockedVariant: 'off',
        trailAtrMult: 1.4,
        timeStopBars: 15,
        tp1ClosePct: 20,
        executeMinutes: 3,
    });

    const guardedBlockedVariants: Array<{ key: string; hours: number[] | null }> = [
        { key: 'none', hours: [] },
        { key: 'h10_11', hours: [10, 11] },
        { key: 'h9_10', hours: [9, 10] },
        { key: 'h11_12', hours: [11, 12] },
        { key: 'h10', hours: [10] },
        { key: 'h11', hours: [11] },
    ];

    const cbpSweeps = {
        trailAtrMult: [1.3, 1.4, 1.5, 1.6, 1.7].map((value) =>
            buildCbpScenario({
                id: `cbp_tr_${value.toFixed(1)}`,
                executeMinutes: 2,
                tp1ClosePct: 8,
                trailAtrMult: value,
                timeStopBars: 18,
                sweepBufferPips: 0.2,
            }),
        ),
        timeStopBars: [12, 15, 18, 21].map((value) =>
            buildCbpScenario({
                id: `cbp_ts_${value}`,
                executeMinutes: 2,
                tp1ClosePct: 8,
                trailAtrMult: 1.5,
                timeStopBars: value,
                sweepBufferPips: 0.2,
            }),
        ),
        sweepBufferPips: [0.1, 0.15, 0.2, 0.25, 0.3].map((value) =>
            buildCbpScenario({
                id: `cbp_sw_${value.toFixed(2)}`,
                executeMinutes: 2,
                tp1ClosePct: 8,
                trailAtrMult: 1.5,
                timeStopBars: 18,
                sweepBufferPips: value,
            }),
        ),
        tp1ClosePct: [0, 8, 15, 20].map((value) =>
            buildCbpScenario({
                id: `cbp_tp1_${value}`,
                executeMinutes: 2,
                tp1ClosePct: value,
                trailAtrMult: 1.5,
                timeStopBars: 18,
                sweepBufferPips: 0.2,
            }),
        ),
    };

    // Keep deployed ids stable for easier reporting.
    cbpSweeps.trailAtrMult = cbpSweeps.trailAtrMult.map((row) => (row.strategyOverrides.trailAtrMult === 1.5 ? { ...row, id: CBP_DEPLOYED_ID } : row));
    cbpSweeps.timeStopBars = cbpSweeps.timeStopBars.map((row) => (row.strategyOverrides.timeStopBars === 18 ? { ...row, id: CBP_DEPLOYED_ID } : row));
    cbpSweeps.sweepBufferPips = cbpSweeps.sweepBufferPips.map((row) => (row.strategyOverrides.sweepBufferPips === 0.2 ? { ...row, id: CBP_DEPLOYED_ID } : row));
    cbpSweeps.tp1ClosePct = cbpSweeps.tp1ClosePct.map((row) => (row.strategyOverrides.tp1ClosePct === 8 ? { ...row, id: CBP_DEPLOYED_ID } : row));

    const guardedSweeps = {
        blockedHours: guardedBlockedVariants.map((variant) =>
            buildGuardedScenario({
                id: variant.hours?.length === 2 && variant.hours[0] === 10 && variant.hours[1] === 11 ? GUARDED_DEPLOYED_ID : `guarded_blocked_${variant.key}`,
                blockedHours: variant.hours,
                blockedVariant: 'off',
                trailAtrMult: 1.4,
                timeStopBars: 15,
                tp1ClosePct: 20,
                executeMinutes: 3,
            }),
        ),
        trailAtrMult: [1.2, 1.3, 1.4, 1.5, 1.6].map((value) =>
            buildGuardedScenario({
                id: value === 1.4 ? GUARDED_DEPLOYED_ID : `guarded_tr_${value.toFixed(1)}`,
                blockedHours: [10, 11],
                blockedVariant: 'off',
                trailAtrMult: value,
                timeStopBars: 15,
                tp1ClosePct: 20,
                executeMinutes: 3,
            }),
        ),
        timeStopBars: [12, 15, 18].map((value) =>
            buildGuardedScenario({
                id: value === 15 ? GUARDED_DEPLOYED_ID : `guarded_ts_${value}`,
                blockedHours: [10, 11],
                blockedVariant: 'off',
                trailAtrMult: 1.4,
                timeStopBars: value,
                tp1ClosePct: 20,
                executeMinutes: 3,
            }),
        ),
        tp1ClosePct: [10, 20, 30].map((value) =>
            buildGuardedScenario({
                id: value === 20 ? GUARDED_DEPLOYED_ID : `guarded_tp1_${value}`,
                blockedHours: [10, 11],
                blockedVariant: 'off',
                trailAtrMult: 1.4,
                timeStopBars: 15,
                tp1ClosePct: value,
                executeMinutes: 3,
            }),
        ),
    };

    const allBaselineScenarios = dedupeScenarios([
        ...cbpSweeps.trailAtrMult,
        ...cbpSweeps.timeStopBars,
        ...cbpSweeps.sweepBufferPips,
        ...cbpSweeps.tp1ClosePct,
        ...guardedSweeps.blockedHours,
        ...guardedSweeps.trailAtrMult,
        ...guardedSweeps.timeStopBars,
        ...guardedSweeps.tp1ClosePct,
    ]);

    const manifestPath = path.join(outputRoot, 'scenario-manifest.json');
    await writeFile(manifestPath, `${JSON.stringify(allBaselineScenarios, null, 2)}\n`, 'utf8');

    const concurrency = Math.max(1, Number(process.env.BTCUSDT_AUDIT_CONCURRENCY || 6));
    const baselineJobs = allBaselineScenarios.map((scenario) => ({
        scenarioId: scenario.id,
        stressId: 'baseline' as StressId,
        outPath: path.join(runsDir, `${encodeURIComponent(scenario.id)}__baseline.json`),
    }));

    let completed = 0;
    process.stdout.write(
        JSON.stringify({
            phase: 'baseline_replays',
            scenarios: allBaselineScenarios.length,
            concurrency,
        }) + '\n',
    );
    await runWithConcurrency(baselineJobs, concurrency, async (job, idx) => {
        await runWorkerJob({
            manifestPath,
            scenarioId: job.scenarioId,
            stressId: job.stressId,
            outPath: job.outPath,
        });
        completed += 1;
        if (completed % 5 === 0 || completed === baselineJobs.length) {
            process.stdout.write(JSON.stringify({ phase: 'baseline_replays_progress', completed, total: baselineJobs.length, idx }) + '\n');
        }
    });

    const baselineResults = new Map<string, ReplayFile>();
    for (const scenario of allBaselineScenarios) {
        const filePath = path.join(runsDir, `${encodeURIComponent(scenario.id)}__baseline.json`);
        const parsed = JSON.parse(await readFile(filePath, 'utf8')) as ReplayFile;
        baselineResults.set(scenario.id, parsed);
    }

    const one = baselineResults.values().next().value as ReplayFile;
    const yearFromTs = one.yearFromTs;
    const yearToTs = one.yearToTs;
    const fullYearWindow: WindowRange = { fromTs: yearFromTs, toTs: yearToTs };
    const rolls = buildRolls(yearFromTs, yearToTs);

    const scenarioById = new Map(allBaselineScenarios.map((row) => [row.id, row] as const));
    const sliceCache = new Map<string, SliceSummary>();
    function getSliceSummary(scenarioId: string, fromTs: number, toTs: number): SliceSummary {
        const key = `${scenarioId}|${fromTs}|${toTs}`;
        const cached = sliceCache.get(key);
        if (cached) return cached;
        const replay = baselineResults.get(scenarioId);
        if (!replay) {
            const empty: SliceSummary = {
                trades: 0,
                wins: 0,
                losses: 0,
                winRatePct: 0,
                netR: 0,
                expectancyR: 0,
                expectancyUsdPerTrade: 0,
                grossProfitR: 0,
                grossLossAbsR: 0,
                profitFactor: null,
                maxDrawdownR: 0,
                avgHoldMinutes: 0,
                netPnlUsd: 0,
            };
            sliceCache.set(key, empty);
            return empty;
        }
        const summary = summarizeTrades(replay.trades, fromTs, toTs);
        sliceCache.set(key, summary);
        return summary;
    }

    function summarizeSweep(
        scenarios: Scenario[],
        valueOf: (scenario: Scenario) => { value: string; valueNum: number | null },
    ): SweepRow[] {
        return scenarios.map((scenario) => {
            const summary = getSliceSummary(scenario.id, fullYearWindow.fromTs, fullYearWindow.toTs);
            const value = valueOf(scenario);
            return {
                scenarioId: scenario.id,
                value: value.value,
                valueNum: value.valueNum,
                trades: summary.trades,
                netPnlUsd: summary.netPnlUsd,
                expectancyR: summary.expectancyR,
                expectancyUsdPerTrade: summary.expectancyUsdPerTrade,
                profitFactor: summary.profitFactor,
                maxDrawdownR: summary.maxDrawdownR,
                winRatePct: summary.winRatePct,
                avgHoldMinutes: summary.avgHoldMinutes,
            };
        });
    }

    const cbpSweepRows = {
        trailAtrMult: summarizeSweep(cbpSweeps.trailAtrMult, (scenario) => ({
            value: String(scenario.strategyOverrides.trailAtrMult),
            valueNum: Number(scenario.strategyOverrides.trailAtrMult),
        })),
        timeStopBars: summarizeSweep(cbpSweeps.timeStopBars, (scenario) => ({
            value: String(scenario.strategyOverrides.timeStopBars),
            valueNum: Number(scenario.strategyOverrides.timeStopBars),
        })),
        sweepBufferPips: summarizeSweep(cbpSweeps.sweepBufferPips, (scenario) => ({
            value: Number(scenario.strategyOverrides.sweepBufferPips).toFixed(2),
            valueNum: Number(scenario.strategyOverrides.sweepBufferPips),
        })),
        tp1ClosePct: summarizeSweep(cbpSweeps.tp1ClosePct, (scenario) => ({
            value: String(scenario.strategyOverrides.tp1ClosePct),
            valueNum: Number(scenario.strategyOverrides.tp1ClosePct),
        })),
    };
    const guardedSweepRows = {
        blockedHours: summarizeSweep(guardedSweeps.blockedHours, (scenario) => ({
            value: formatBlockedHours(scenario.blockedHoursBerlin),
            valueNum: null,
        })),
        trailAtrMult: summarizeSweep(guardedSweeps.trailAtrMult, (scenario) => ({
            value: String(scenario.strategyOverrides.trailAtrMult),
            valueNum: Number(scenario.strategyOverrides.trailAtrMult),
        })),
        timeStopBars: summarizeSweep(guardedSweeps.timeStopBars, (scenario) => ({
            value: String(scenario.strategyOverrides.timeStopBars),
            valueNum: Number(scenario.strategyOverrides.timeStopBars),
        })),
        tp1ClosePct: summarizeSweep(guardedSweeps.tp1ClosePct, (scenario) => ({
            value: String(scenario.strategyOverrides.tp1ClosePct),
            valueNum: Number(scenario.strategyOverrides.tp1ClosePct),
        })),
    };
    const taskA = {
        cbp: {
            sweeps: cbpSweepRows,
            assessments: {
                trailAtrMult: assessSweep(cbpSweepRows.trailAtrMult, '1.5', true),
                timeStopBars: assessSweep(cbpSweepRows.timeStopBars, '18', true),
                sweepBufferPips: assessSweep(cbpSweepRows.sweepBufferPips, '0.20', true),
                tp1ClosePct: assessSweep(cbpSweepRows.tp1ClosePct, '8', true),
            },
        },
        guarded: {
            sweeps: guardedSweepRows,
            assessments: {
                blockedHours: assessSweep(guardedSweepRows.blockedHours, '[10,11]', false),
                trailAtrMult: assessSweep(guardedSweepRows.trailAtrMult, '1.4', true),
                timeStopBars: assessSweep(guardedSweepRows.timeStopBars, '15', true),
                tp1ClosePct: assessSweep(guardedSweepRows.tp1ClosePct, '20', true),
            },
        },
    };

    const selectionUniverse = dedupeScenarios([
        ...cbpSweeps.trailAtrMult,
        ...cbpSweeps.timeStopBars,
        ...cbpSweeps.sweepBufferPips,
        ...cbpSweeps.tp1ClosePct,
        ...guardedSweeps.blockedHours,
        ...guardedSweeps.trailAtrMult,
        ...guardedSweeps.timeStopBars,
        ...guardedSweeps.tp1ClosePct,
    ]);

    function runWalkForward(gate: number): WalkForwardSelectionRow[] {
        const rows: WalkForwardSelectionRow[] = [];
        for (const roll of rolls) {
            const trainRows = selectionUniverse.map((scenario) => ({
                scenario,
                summary: getSliceSummary(scenario.id, roll.selection.fromTs, roll.selection.toTs),
            }));
            const sorted = trainRows.slice().sort(compareSelectionRows);
            const eligible = sorted.filter((row) => row.summary.maxDrawdownR <= gate);
            const picked = (eligible[0] || sorted[0])!;
            const fwd2 = getSliceSummary(picked.scenario.id, roll.forward2w.fromTs, roll.forward2w.toTs);
            const fwd4 = getSliceSummary(picked.scenario.id, roll.forward4w.fromTs, roll.forward4w.toTs);
            rows.push({
                rollIndex: roll.rollIndex,
                gate,
                selectedScenarioId: picked.scenario.id,
                selectedFamily: picked.scenario.family,
                selectedStrategyId: picked.scenario.strategyId,
                eligibleCandidates: eligible.length,
                candidateCount: sorted.length,
                selectionTrades: picked.summary.trades,
                selectionNetPnlUsd: picked.summary.netPnlUsd,
                selectionExpectancyR: picked.summary.expectancyR,
                selectionProfitFactor: picked.summary.profitFactor,
                selectionMaxDrawdownR: picked.summary.maxDrawdownR,
                fwd2wNetPnlUsd: fwd2.netPnlUsd,
                fwd2wExpectancyR: fwd2.expectancyR,
                fwd2wProfitFactor: fwd2.profitFactor,
                fwd2wMaxDrawdownR: fwd2.maxDrawdownR,
                fwd4wNetPnlUsd: fwd4.netPnlUsd,
                fwd4wExpectancyR: fwd4.expectancyR,
                fwd4wProfitFactor: fwd4.profitFactor,
                fwd4wMaxDrawdownR: fwd4.maxDrawdownR,
            });
        }
        return rows;
    }

    const walkForwardBaseline = runWalkForward(BASELINE_DD_GATE);
    const taskB = {
        rolls,
        rows: walkForwardBaseline,
        summary: summarizeWalkForward(walkForwardBaseline),
    };

    const guardedVariantScenarios = guardedSweeps.blockedHours;
    const cAggregateRows = guardedVariantScenarios.map((scenario) => {
        const summary = getSliceSummary(scenario.id, fullYearWindow.fromTs, fullYearWindow.toTs);
        return {
            scenarioId: scenario.id,
            variant: formatBlockedHours(scenario.blockedHoursBerlin),
            trades: summary.trades,
            netPnlUsd: summary.netPnlUsd,
            expectancyR: summary.expectancyR,
            expectancyUsdPerTrade: summary.expectancyUsdPerTrade,
            profitFactor: summary.profitFactor,
            maxDrawdownR: summary.maxDrawdownR,
            winRatePct: summary.winRatePct,
            avgHoldMinutes: summary.avgHoldMinutes,
        };
    });

    const cByHour: Record<
        string,
        Array<{
            berlinHour: number;
            trades: number;
            netPnlUsd: number;
            expectancyR: number;
            expectancyUsdPerTrade: number;
        }>
    > = {};
    for (const scenario of guardedVariantScenarios) {
        const replay = baselineResults.get(scenario.id)!;
        const byHour = new Map<number, { trades: number; netPnlUsd: number; netR: number }>();
        for (const trade of replay.trades) {
            const hour = toBerlinHour(trade.entryTs);
            const row = byHour.get(hour) || { trades: 0, netPnlUsd: 0, netR: 0 };
            row.trades += 1;
            row.netPnlUsd += trade.pnlUsd;
            row.netR += trade.rMultiple;
            byHour.set(hour, row);
        }
        cByHour[formatBlockedHours(scenario.blockedHoursBerlin)] = Array.from(byHour.entries())
            .sort((a, b) => a[0] - b[0])
            .map(([hour, row]) => ({
                berlinHour: hour,
                trades: row.trades,
                netPnlUsd: row.netPnlUsd,
                expectancyR: row.trades ? row.netR / row.trades : 0,
                expectancyUsdPerTrade: row.trades ? row.netPnlUsd / row.trades : 0,
            }));
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
            const summary = getSliceSummary(scenario.id, roll.forward4w.fromTs, roll.forward4w.toTs);
            cWalkForwardRows.push({
                rollIndex: roll.rollIndex,
                variant: formatBlockedHours(scenario.blockedHoursBerlin),
                netPnlUsd: summary.netPnlUsd,
                expectancyR: summary.expectancyR,
                profitFactor: summary.profitFactor,
                maxDrawdownR: summary.maxDrawdownR,
            });
        }
    }

    const bestByExpectancy = new Map<number, string>();
    const bestByPnl = new Map<number, string>();
    for (const roll of rolls) {
        const rows = cWalkForwardRows.filter((row) => row.rollIndex === roll.rollIndex);
        rows.sort((a, b) => b.expectancyR - a.expectancyR);
        if (rows.length) bestByExpectancy.set(roll.rollIndex, rows[0]!.variant);
        rows.sort((a, b) => b.netPnlUsd - a.netPnlUsd);
        if (rows.length) bestByPnl.set(roll.rollIndex, rows[0]!.variant);
    }
    const winsExpectancy: Record<string, number> = {};
    const winsPnl: Record<string, number> = {};
    for (const v of bestByExpectancy.values()) winsExpectancy[v] = (winsExpectancy[v] || 0) + 1;
    for (const v of bestByPnl.values()) winsPnl[v] = (winsPnl[v] || 0) + 1;

    const taskC = {
        aggregateRows: cAggregateRows,
        byHour: cByHour,
        walkForward4wRows: cWalkForwardRows,
        bestVariantWins: {
            byExpectancy: winsExpectancy,
            byPnl: winsPnl,
        },
    };

    const bestCbpSweep = dedupeScenarios([
        ...cbpSweeps.trailAtrMult,
        ...cbpSweeps.timeStopBars,
        ...cbpSweeps.sweepBufferPips,
        ...cbpSweeps.tp1ClosePct,
    ])
        .map((scenario) => ({
            scenario,
            summary: getSliceSummary(scenario.id, fullYearWindow.fromTs, fullYearWindow.toTs),
        }))
        .sort((a, b) => b.summary.expectancyR - a.summary.expectancyR)[0]!;

    const bestGuardedSweep = dedupeScenarios([
        ...guardedSweeps.blockedHours,
        ...guardedSweeps.trailAtrMult,
        ...guardedSweeps.timeStopBars,
        ...guardedSweeps.tp1ClosePct,
    ])
        .map((scenario) => ({
            scenario,
            summary: getSliceSummary(scenario.id, fullYearWindow.fromTs, fullYearWindow.toTs),
        }))
        .sort((a, b) => b.summary.expectancyR - a.summary.expectancyR)[0]!;

    const stressTargets = dedupeScenarios([
        { ...cbpDeployed, metadata: { label: 'cbp_deployed' } },
        { ...guardedDeployed, metadata: { label: 'guarded_deployed' } },
        { ...bestCbpSweep.scenario, metadata: { label: 'cbp_best_nearby' } },
        { ...bestGuardedSweep.scenario, metadata: { label: 'guarded_best_nearby' } },
    ]);

    const stressJobs = [];
    for (const scenario of stressTargets) {
        for (const stress of STRESS_CASES) {
            if (stress.id === 'baseline') continue;
            stressJobs.push({
                scenarioId: scenario.id,
                stressId: stress.id,
                outPath: path.join(runsDir, `${encodeURIComponent(scenario.id)}__${encodeURIComponent(stress.id)}.json`),
            });
        }
    }
    process.stdout.write(JSON.stringify({ phase: 'stress_replays', jobs: stressJobs.length, concurrency }) + '\n');
    await runWithConcurrency(stressJobs, concurrency, async (job, idx) => {
        await runWorkerJob({
            manifestPath,
            scenarioId: job.scenarioId,
            stressId: job.stressId as StressId,
            outPath: job.outPath,
        });
        if ((idx + 1) % 5 === 0 || idx + 1 === stressJobs.length) {
            process.stdout.write(JSON.stringify({ phase: 'stress_replays_progress', completed: idx + 1, total: stressJobs.length }) + '\n');
        }
    });

    const stressResults = new Map<string, ReplayFile>();
    for (const scenario of stressTargets) {
        for (const stress of STRESS_CASES) {
            const filePath = path.join(runsDir, `${encodeURIComponent(scenario.id)}__${encodeURIComponent(stress.id)}.json`);
            if (stress.id === 'baseline') {
                stressResults.set(`${scenario.id}|baseline`, baselineResults.get(scenario.id)!);
                continue;
            }
            const parsed = JSON.parse(await readFile(filePath, 'utf8')) as ReplayFile;
            stressResults.set(`${scenario.id}|${stress.id}`, parsed);
        }
    }

    const taskDRows: Array<{
        strategyLabel: string;
        scenarioId: string;
        stressId: StressId;
        netPnlUsd: number;
        expectancyR: number;
        profitFactor: number | null;
        maxDrawdownR: number;
        netPnlDeltaPctVsBaseline: number;
        expectancyDeltaPctVsBaseline: number;
    }> = [];
    for (const scenario of stressTargets) {
        const label = String(scenario.metadata?.label || scenario.id);
        const baseline = stressResults.get(`${scenario.id}|baseline`)!;
        for (const stress of STRESS_CASES) {
            const replay = stressResults.get(`${scenario.id}|${stress.id}`)!;
            const bNet = baseline.summary.netPnlUsd;
            const bExp = baseline.summary.expectancyR;
            taskDRows.push({
                strategyLabel: label,
                scenarioId: scenario.id,
                stressId: stress.id,
                netPnlUsd: replay.summary.netPnlUsd,
                expectancyR: replay.summary.expectancyR,
                profitFactor: replay.summary.profitFactor,
                maxDrawdownR: replay.summary.maxDrawdownR,
                netPnlDeltaPctVsBaseline: bNet === 0 ? NaN : ((replay.summary.netPnlUsd - bNet) / Math.abs(bNet)) * 100,
                expectancyDeltaPctVsBaseline: bExp === 0 ? NaN : ((replay.summary.expectancyR - bExp) / Math.abs(bExp)) * 100,
            });
        }
    }
    const taskD = {
        stressCases: STRESS_CASES,
        targets: stressTargets.map((row) => ({ scenarioId: row.id, label: String(row.metadata?.label || row.id) })),
        rows: taskDRows,
    };

    const cbpDeployedReplay = baselineResults.get(CBP_DEPLOYED_ID)!;
    const guardedDeployedReplay = baselineResults.get(GUARDED_DEPLOYED_ID)!;

    function monthlyStats(trades: ReplayTrade[]): MonthlyStats[] {
        const byMonth = new Map<string, { trades: number; netPnlUsd: number; netR: number }>();
        for (const trade of trades) {
            const month = toMonthKey(trade.exitTs);
            const row = byMonth.get(month) || { trades: 0, netPnlUsd: 0, netR: 0 };
            row.trades += 1;
            row.netPnlUsd += trade.pnlUsd;
            row.netR += trade.rMultiple;
            byMonth.set(month, row);
        }
        return Array.from(byMonth.entries())
            .sort((a, b) => a[0].localeCompare(b[0]))
            .map(([month, row]) => ({
                month,
                trades: row.trades,
                netPnlUsd: row.netPnlUsd,
                expectancyR: row.trades ? row.netR / row.trades : 0,
            }));
    }

    const monthlyCbp = monthlyStats(cbpDeployedReplay.trades);
    const monthlyGuarded = monthlyStats(guardedDeployedReplay.trades);
    const monthSet = new Set([...monthlyCbp.map((row) => row.month), ...monthlyGuarded.map((row) => row.month)]);
    const cbpMonthMap = new Map(monthlyCbp.map((row) => [row.month, row] as const));
    const guardedMonthMap = new Map(monthlyGuarded.map((row) => [row.month, row] as const));
    const months = Array.from(monthSet).sort();
    const cbpReturns = months.map((month) => cbpMonthMap.get(month)?.netPnlUsd ?? 0);
    const guardedReturns = months.map((month) => guardedMonthMap.get(month)?.netPnlUsd ?? 0);
    const monthlyCorr = pearsonCorrelation(cbpReturns, guardedReturns);
    const combinedByMonth = months.map((month) => {
        const cbp = cbpMonthMap.get(month);
        const guarded = guardedMonthMap.get(month);
        const trades = (cbp?.trades || 0) + (guarded?.trades || 0);
        const netPnlUsd = (cbp?.netPnlUsd || 0) + (guarded?.netPnlUsd || 0);
        const sumR = (cbp?.expectancyR || 0) * (cbp?.trades || 0) + (guarded?.expectancyR || 0) * (guarded?.trades || 0);
        return {
            month,
            trades,
            netPnlUsd,
            expectancyR: trades ? sumR / trades : 0,
        };
    });

    function overlapMinutes(a: ReplayTrade[], b: ReplayTrade[]): number {
        const ai = a.map((row) => [row.entryTs, row.exitTs] as const).sort((x, y) => x[0] - y[0]);
        const bi = b.map((row) => [row.entryTs, row.exitTs] as const).sort((x, y) => x[0] - y[0]);
        let i = 0;
        let j = 0;
        let overlap = 0;
        while (i < ai.length && j < bi.length) {
            const [aStart, aEnd] = ai[i]!;
            const [bStart, bEnd] = bi[j]!;
            const start = Math.max(aStart, bStart);
            const end = Math.min(aEnd, bEnd);
            if (end > start) overlap += end - start;
            if (aEnd < bEnd) i += 1;
            else j += 1;
        }
        return overlap / 60_000;
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
        const rows = gate === BASELINE_DD_GATE ? walkForwardBaseline : runWalkForward(gate);
        gateRowsByGate[String(gate)] = rows;
        const survivors = rows.map((row) => row.eligibleCandidates);
        const selectedCounts = new Map<string, number>();
        for (const row of rows) selectedCounts.set(row.selectedScenarioId, (selectedCounts.get(row.selectedScenarioId) || 0) + 1);
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

    const cbpAssessments = Object.values((taskA.cbp as any).assessments) as SweepAssessment[];
    const guardedAssessments = Object.values((taskA.guarded as any).assessments) as SweepAssessment[];

    function verdictFromAssessments(
        assessments: SweepAssessment[],
        forwardSummary: ReturnType<typeof summarizeWalkForward>,
        stressRows: Array<{ strategyLabel: string; stressId: StressId; expectancyR: number }>,
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

    const selectionVerdict: 'generalizes forward' | 'weakly generalizes' | "mostly buys yesterday's luck" =
        taskB.summary.meanForward4wExpectancyR > 0 && taskB.summary.profitable4wPct >= 60
            ? 'generalizes forward'
            : taskB.summary.meanForward4wExpectancyR > -0.02 && taskB.summary.profitable4wPct >= 45
              ? 'weakly generalizes'
              : "mostly buys yesterday's luck";

    const deployedStressRows = taskDRows.filter(
        (row) => row.strategyLabel === 'cbp_deployed' || row.strategyLabel === 'guarded_deployed',
    );
    const harshRows = deployedStressRows.filter((row) => row.stressId === 'slippage_x2_spread_1.50x');
    const harshPositive = harshRows.filter((row) => row.expectancyR > 0).length;
    const executionVerdict: 'survives stress' | 'borderline' | 'too thin to trust live' =
        harshRows.length && harshPositive === harshRows.length
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

    const output = {
        generatedAtIso: new Date().toISOString(),
        symbol: 'BTCUSDT',
        dataset: {
            yearFromTs,
            yearToTs,
            note: 'Computed from full-year replays per scenario; rolling windows are sliced by entry timestamp.',
        },
        assumptions: {
            walkForwardSelectionDays: SELECTION_DAYS,
            walkForwardForward2wDays: FORWARD_2W_DAYS,
            walkForwardForward4wDays: FORWARD_4W_DAYS,
            walkForwardStepDays: ROLL_STEP_DAYS,
            baselineDdGate: BASELINE_DD_GATE,
            selectionUniverseSize: selectionUniverse.length,
            selectionUniverseNote: 'In-scope deployed-neighborhood scenarios from Task A sweeps (CBP + guarded).',
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

    const aRows: Array<Array<string | number | null>> = [];
    for (const [sweepName, rows] of Object.entries(taskA.cbp.sweeps)) {
        for (const row of rows as SweepRow[]) {
            aRows.push([
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
            aRows.push([
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
            aRows,
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
            taskB.rows.map((row) => [
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
            taskC.walkForward4wRows.map((row) => [row.rollIndex, row.variant, row.netPnlUsd, row.expectancyR, row.profitFactor, row.maxDrawdownR]),
        ),
        'utf8',
    );

    await writeFile(
        path.join(outputRoot, 'taskD_stress.csv'),
        createCsv(
            [
                'strategyLabel',
                'scenarioId',
                'stressId',
                'netPnlUsd',
                'expectancyR',
                'profitFactor',
                'maxDrawdownR',
                'netPnlDeltaPctVsBaseline',
                'expectancyDeltaPctVsBaseline',
            ],
            taskDRows.map((row) => [
                row.strategyLabel,
                row.scenarioId,
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

    const monthlyRows: Array<Array<string | number>> = [];
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

    process.stdout.write(
        JSON.stringify(
            {
                ok: true,
                outputRoot,
                reportJson: jsonPath,
                scenarioCount: allBaselineScenarios.length,
                selectionUniverseSize: selectionUniverse.length,
                rolls: rolls.length,
                conclusions: finalConclusions,
            },
            null,
            2,
        ) + '\n',
    );
}

main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
});
