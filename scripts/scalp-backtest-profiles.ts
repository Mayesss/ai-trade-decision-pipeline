#!/usr/bin/env node
import { writeFile } from 'node:fs/promises';

import capitalTickerMap from '../data/capitalTickerMap.json';
import { loadScalpCandleHistory } from '../lib/scalp/candleHistory';
import { pipSizeForScalpSymbol } from '../lib/scalp/marketData';
import { defaultScalpReplayConfig, runScalpReplay } from '../lib/scalp/replay/harness';
import { applySymbolGuardRiskDefaultsToReplayRuntime } from '../lib/scalp/strategies/guardDefaults';
import type { ScalpReplayCandle, ScalpReplayRuntimeConfig } from '../lib/scalp/replay/types';

type ProfileId = 'strict' | 'baseline' | 'loose';

type ProfileConfig = {
    id: ProfileId;
    strategy: Partial<ScalpReplayRuntimeConfig['strategy']>;
};

type ProfileRun = {
    profile: ProfileId;
    trades: number;
    wins: number;
    losses: number;
    winRatePct: number;
    netR: number;
    netPnlUsd: number;
    maxDrawdownR: number;
    avgHoldMinutes: number;
    replayDurationSec: number;
};

type SymbolResult = {
    symbol: string;
    ok: boolean;
    error?: string;
    baseCandles15m: number;
    confirmCandles1m: number;
    fromTsMs: number | null;
    toTsMs: number | null;
    profiles: ProfileRun[];
    bestProfile: ProfileId | null;
};

type ProfileAggregate = {
    profile: ProfileId;
    symbolsTested: number;
    symbolsWithTrades: number;
    symbolsNetPositive: number;
    totalTrades: number;
    totalWins: number;
    totalLosses: number;
    winRatePct: number;
    totalNetR: number;
    totalNetPnlUsd: number;
    avgNetRPerSymbol: number;
    bestProfileCount: number;
};

const MIN_CANDLES = 180;
const DAY_MS = 24 * 60 * 60 * 1000;

const PROFILES: ProfileConfig[] = [
    {
        id: 'strict',
        strategy: {
            maxTradesPerDay: 1,
            takeProfitR: 1.6,
            sweepBufferPips: 0.55,
            sweepRejectInsidePips: 0.2,
            sweepRejectMaxBars: 6,
            sweepMinWickBodyRatio: 1.4,
            displacementBodyAtrMult: 0.2,
            displacementRangeAtrMult: 0.35,
            mssLookbackBars: 5,
            mssBreakBufferPips: 0.2,
            ifvgMinAtrMult: 0.15,
            ifvgMaxAtrMult: 1.1,
            ifvgEntryMode: 'full_fill',
        },
    },
    {
        id: 'baseline',
        strategy: {},
    },
    {
        id: 'loose',
        strategy: {
            maxTradesPerDay: 4,
            takeProfitR: 1.0,
            sweepBufferPips: 0.05,
            sweepRejectInsidePips: 0,
            sweepRejectMaxBars: 24,
            sweepMinWickBodyRatio: 0.55,
            displacementBodyAtrMult: 0.02,
            displacementRangeAtrMult: 0.05,
            mssLookbackBars: 1,
            mssBreakBufferPips: 0,
            ifvgMinAtrMult: 0,
            ifvgMaxAtrMult: 3,
            ifvgEntryMode: 'first_touch',
        },
    },
];

function toPositiveInt(raw: string | undefined, fallback: number): number {
    const n = Number(raw);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return Math.floor(n);
}

function toReplayRows(rows: Array<[number, number, number, number, number, number]>, spreadPips: number): ScalpReplayCandle[] {
    return rows.map((row) => ({
        ts: Number(row[0]),
        open: Number(row[1]),
        high: Number(row[2]),
        low: Number(row[3]),
        close: Number(row[4]),
        volume: Number.isFinite(Number(row[5])) ? Number(row[5]) : 0,
        spreadPips,
    }));
}

function pickBestProfile(runs: ProfileRun[]): ProfileId | null {
    if (!runs.length) return null;
    const sorted = runs
        .slice()
        .sort((a, b) => b.netR - a.netR || b.trades - a.trades || b.winRatePct - a.winRatePct);
    return sorted[0]?.profile ?? null;
}

function buildProfileAggregates(rows: SymbolResult[]): ProfileAggregate[] {
    const okRows = rows.filter((r) => r.ok);
    const counts = new Map<ProfileId, number>([
        ['strict', 0],
        ['baseline', 0],
        ['loose', 0],
    ]);
    for (const row of okRows) {
        if (row.bestProfile) {
            counts.set(row.bestProfile, (counts.get(row.bestProfile) || 0) + 1);
        }
    }

    return PROFILES.map((profile) => {
        const runs = okRows
            .map((row) => row.profiles.find((p) => p.profile === profile.id))
            .filter((run): run is ProfileRun => Boolean(run));
        const totalTrades = runs.reduce((acc, run) => acc + run.trades, 0);
        const totalWins = runs.reduce((acc, run) => acc + run.wins, 0);
        const totalLosses = runs.reduce((acc, run) => acc + run.losses, 0);
        const totalNetR = runs.reduce((acc, run) => acc + run.netR, 0);
        const totalNetPnlUsd = runs.reduce((acc, run) => acc + run.netPnlUsd, 0);
        const symbolsWithTrades = runs.filter((run) => run.trades > 0).length;
        const symbolsNetPositive = runs.filter((run) => run.netR > 0).length;

        return {
            profile: profile.id,
            symbolsTested: runs.length,
            symbolsWithTrades,
            symbolsNetPositive,
            totalTrades,
            totalWins,
            totalLosses,
            winRatePct: totalTrades > 0 ? (totalWins / totalTrades) * 100 : 0,
            totalNetR,
            totalNetPnlUsd,
            avgNetRPerSymbol: runs.length ? totalNetR / runs.length : 0,
            bestProfileCount: counts.get(profile.id) || 0,
        };
    }).sort((a, b) => b.totalNetR - a.totalNetR);
}

async function main() {
    const days = toPositiveInt(process.env.DAYS, 30);
    const executeMinutes = toPositiveInt(process.env.EXECUTE_MINUTES, 3);
    const nowMs = Date.now();
    const fromMs = nowMs - days * DAY_MS;
    const symbols = Object.keys(capitalTickerMap as Record<string, string>)
        .map((s) => String(s).toUpperCase())
        .sort();

    const rows: SymbolResult[] = [];
    const totalRunsPlanned = symbols.length * PROFILES.length;
    let completedRuns = 0;
    const startedAtMs = Date.now();

    console.log(
        `Starting scalp profile matrix | symbols=${symbols.length} profiles=${PROFILES.length} days=${days} executeMinutes=${executeMinutes}`,
    );

    for (const symbol of symbols) {
        const startedSymbolMs = Date.now();
        try {
            const [hist15m, hist1m] = await Promise.all([
                loadScalpCandleHistory(symbol, '15m', { backend: 'file' }),
                loadScalpCandleHistory(symbol, '1m', { backend: 'file' }),
            ]);
            const rows15m = (hist15m.record?.candles || [])
                .filter((row) => row[0] >= fromMs && row[0] <= nowMs) as Array<[number, number, number, number, number, number]>;
            const rows1m = (hist1m.record?.candles || [])
                .filter((row) => row[0] >= fromMs && row[0] <= nowMs) as Array<[number, number, number, number, number, number]>;

            if (rows15m.length < MIN_CANDLES || rows1m.length < MIN_CANDLES) {
                throw new Error(`insufficient_history base15m=${rows15m.length} confirm1m=${rows1m.length}`);
            }

            const baseRuntime = defaultScalpReplayConfig(symbol);
            baseRuntime.symbol = symbol;
            baseRuntime.executeMinutes = executeMinutes;
            baseRuntime.strategy.asiaBaseTf = 'M15';
            baseRuntime.strategy.confirmTf = 'M3';

            const baseCandles = toReplayRows(rows15m, baseRuntime.defaultSpreadPips);
            const confirmCandles = toReplayRows(rows1m, baseRuntime.defaultSpreadPips);
            const pipSize = pipSizeForScalpSymbol(symbol);

            const profileRuns: ProfileRun[] = [];
            for (const profile of PROFILES) {
                let runtime = JSON.parse(JSON.stringify(baseRuntime)) as ScalpReplayRuntimeConfig;
                runtime.strategy = { ...runtime.strategy, ...profile.strategy };
                runtime = applySymbolGuardRiskDefaultsToReplayRuntime(runtime);
                const replayStartedAtMs = Date.now();
                const replay = await runScalpReplay({
                    candles: baseCandles,
                    pipSize,
                    config: runtime,
                    marketData: {
                        baseCandles,
                        confirmCandles,
                        priceCandles: confirmCandles,
                    },
                });
                const replayDurationSec = Number(((Date.now() - replayStartedAtMs) / 1000).toFixed(2));
                const summary = replay.summary;
                profileRuns.push({
                    profile: profile.id,
                    trades: summary.trades,
                    wins: summary.wins,
                    losses: summary.losses,
                    winRatePct: summary.winRatePct,
                    netR: summary.netR,
                    netPnlUsd: summary.netPnlUsd,
                    maxDrawdownR: summary.maxDrawdownR,
                    avgHoldMinutes: summary.avgHoldMinutes,
                    replayDurationSec,
                });
                completedRuns += 1;
                const pct = totalRunsPlanned > 0 ? (completedRuns / totalRunsPlanned) * 100 : 100;
                console.log(
                    `[run] ${symbol} profile=${profile.id} trades=${summary.trades} netR=${summary.netR.toFixed(3)} progress=${completedRuns}/${totalRunsPlanned} (${pct.toFixed(1)}%)`,
                );
            }

            const bestProfile = pickBestProfile(profileRuns);
            const symbolDurationSec = Number(((Date.now() - startedSymbolMs) / 1000).toFixed(2));
            console.log(
                `[symbol] ${symbol} best=${bestProfile || 'n/a'} base15m=${rows15m.length} confirm1m=${rows1m.length} took=${symbolDurationSec}s`,
            );
            rows.push({
                symbol,
                ok: true,
                baseCandles15m: rows15m.length,
                confirmCandles1m: rows1m.length,
                fromTsMs: rows1m[0]?.[0] ?? null,
                toTsMs: rows1m[rows1m.length - 1]?.[0] ?? null,
                profiles: profileRuns,
                bestProfile,
            });
        } catch (err: any) {
            const message = err?.message || String(err);
            console.log(`[error] ${symbol} ${message}`);
            rows.push({
                symbol,
                ok: false,
                error: message,
                baseCandles15m: 0,
                confirmCandles1m: 0,
                fromTsMs: null,
                toTsMs: null,
                profiles: [],
                bestProfile: null,
            });
        }
    }

    const profileAggregates = buildProfileAggregates(rows);
    const okCount = rows.filter((row) => row.ok).length;
    const failCount = rows.length - okCount;
    const elapsedSec = Number(((Date.now() - startedAtMs) / 1000).toFixed(2));
    const report = {
        generatedAtIso: new Date().toISOString(),
        days,
        executeMinutes,
        strategyTimeframes: { base: 'M15', confirm: 'M3' },
        dataSources: { base: '15m history', confirmAndPrice: '1m history' },
        symbolsTotal: rows.length,
        symbolsOk: okCount,
        symbolsFailed: failCount,
        profiles: PROFILES.map((p) => p.id),
        elapsedSec,
        profileAggregates,
        symbolResults: rows,
    };

    const outPath = `/tmp/scalp_backtest_profiles_${days}d_report.json`;
    await writeFile(outPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

    console.log(`Done | ok=${okCount} fail=${failCount} elapsedSec=${elapsedSec}`);
    console.log(`Report: ${outPath}`);
}

main().catch((err) => {
    console.error('Scalp profile matrix failed:', err);
    process.exitCode = 1;
});
