#!/usr/bin/env node
import { readFile, writeFile } from 'node:fs/promises';

import type { ScalpReplayRuntimeConfig } from '../lib/scalp/replay/types';

type CandleRow = [number, number, number, number, number, number];

type Scenario = {
    id: string;
    family: 'cbp' | 'guarded';
    strategyId: string;
    executeMinutes: number;
    spreadFactor: number;
    slippagePips: number;
    blockedHoursVariant?: string | null;
    blockedHoursBerlin?: number[] | null;
    strategyOverrides: Partial<ScalpReplayRuntimeConfig['strategy']>;
};

type StressId = 'baseline' | 'slippage_x2' | 'spread_1.25x' | 'spread_1.50x' | 'slippage_x2_spread_1.50x';

const STRESS_MAP: Record<StressId, { spreadMult: number; slippageMult: number }> = {
    baseline: { spreadMult: 1, slippageMult: 1 },
    slippage_x2: { spreadMult: 1, slippageMult: 2 },
    'spread_1.25x': { spreadMult: 1.25, slippageMult: 1 },
    'spread_1.50x': { spreadMult: 1.5, slippageMult: 1 },
    'slippage_x2_spread_1.50x': { spreadMult: 1.5, slippageMult: 2 },
};

function parseArgs(argv: string[]): Record<string, string> {
    const out: Record<string, string> = {};
    for (let i = 0; i < argv.length; i += 1) {
        const token = argv[i]!;
        if (!token.startsWith('--')) continue;
        const key = token.slice(2);
        const next = argv[i + 1];
        if (!next || next.startsWith('--')) continue;
        out[key] = next;
        i += 1;
    }
    return out;
}

function toReplay(rows: CandleRow[], spreadPips: number) {
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

async function main() {
    const args = parseArgs(process.argv.slice(2));
    const manifestPath = args.manifest;
    const scenarioId = args.scenarioId;
    const outPath = args.out;
    const stressId = (args.stressId || 'baseline') as StressId;
    if (!manifestPath || !scenarioId || !outPath) {
        throw new Error('Usage: --manifest <path> --scenarioId <id> --out <path> [--stressId <id>]');
    }
    if (!STRESS_MAP[stressId]) throw new Error(`Unknown stressId: ${stressId}`);

    const scenarios = JSON.parse(await readFile(manifestPath, 'utf8')) as Scenario[];
    const scenario = scenarios.find((row) => row.id === scenarioId);
    if (!scenario) throw new Error(`Scenario not found: ${scenarioId}`);

    const symbol = 'BTCUSDT';
    process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_VARIANT = scenario.blockedHoursVariant || '';
    if (scenario.blockedHoursBerlin === null || scenario.blockedHoursBerlin === undefined) {
        delete process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_BERLIN;
    } else {
        process.env.SCALP_BTCUSDT_GUARD_BLOCKED_HOURS_BERLIN = scenario.blockedHoursBerlin.join(',');
    }

    // Important: import replay/strategy modules after env is set. Blocked-hour variants are resolved at module load.
    const [{ loadScalpCandleHistory }, { resolveScalpDeployment }, { pipSizeForScalpSymbol }, replayHarness, guardDefaults] =
        await Promise.all([
            import('../lib/scalp/candleHistory'),
            import('../lib/scalp/deployments'),
            import('../lib/scalp/marketData'),
            import('../lib/scalp/replay/harness'),
            import('../lib/scalp/strategies/guardDefaults'),
        ]);
    const { defaultScalpReplayConfig, runScalpReplay } = replayHarness;
    const { applySymbolGuardRiskDefaultsToReplayRuntime } = guardDefaults;

    const [h15, h1] = await Promise.all([
        loadScalpCandleHistory(symbol, '15m', { backend: 'pg' }),
        loadScalpCandleHistory(symbol, '1m', { backend: 'pg' }),
    ]);
    const rows15 = (h15.record?.candles || []) as CandleRow[];
    const rows1 = (h1.record?.candles || []) as CandleRow[];
    if (!rows15.length || !rows1.length) throw new Error('Missing BTCUSDT candle history');

    const firstTs = Math.max(rows15[0]![0], rows1[0]![0]);
    const lastTs = Math.min(rows15.at(-1)?.[0] || 0, rows1.at(-1)?.[0] || 0);
    const yearFromTs = Math.max(firstTs, lastTs - 365 * 24 * 60 * 60 * 1000);
    const yearToTs = lastTs;

    const rows15Win = rows15.filter((row) => row[0] >= yearFromTs && row[0] <= yearToTs);
    const rows1Win = rows1.filter((row) => row[0] >= yearFromTs && row[0] <= yearToTs);

    const baseRuntime = defaultScalpReplayConfig(symbol);
    const pipSize = pipSizeForScalpSymbol(symbol);
    const replay15 = toReplay(rows15Win, baseRuntime.defaultSpreadPips);
    const replay1 = toReplay(rows1Win, baseRuntime.defaultSpreadPips);

    const stress = STRESS_MAP[stressId];

    const startedAtMs = Date.now();
    let runtime = JSON.parse(JSON.stringify(baseRuntime)) as ScalpReplayRuntimeConfig;
    runtime.symbol = symbol;
    runtime.strategyId = scenario.strategyId;
    runtime.executeMinutes = scenario.executeMinutes;
    runtime.spreadFactor = scenario.spreadFactor * stress.spreadMult;
    runtime.slippagePips = scenario.slippagePips * stress.slippageMult;
    runtime.strategy = {
        ...runtime.strategy,
        ...scenario.strategyOverrides,
    };
    runtime = applySymbolGuardRiskDefaultsToReplayRuntime(runtime);
    runtime.strategy = {
        ...runtime.strategy,
        ...scenario.strategyOverrides,
    };
    const deployment = resolveScalpDeployment({
        symbol,
        strategyId: runtime.strategyId,
        tuneId: scenario.id,
    });
    runtime.tuneId = deployment.tuneId;
    runtime.tuneLabel = deployment.tuneLabel;
    runtime.deploymentId = deployment.deploymentId;

    const replay = await runScalpReplay({
        candles: replay1,
        pipSize,
        config: runtime,
        captureTimeline: false,
        marketData: {
            baseCandles: replay15,
            confirmCandles: replay1,
            priceCandles: replay1,
        },
    });
    const elapsedMs = Date.now() - startedAtMs;

    const payload = {
        scenarioId: scenario.id,
        family: scenario.family,
        strategyId: scenario.strategyId,
        stressId,
        yearFromTs,
        yearToTs,
        elapsedMs,
        summary: replay.summary,
        trades: replay.trades.map((row) => ({
            id: row.id,
            side: row.side,
            entryTs: row.entryTs,
            exitTs: row.exitTs,
            holdMinutes: row.holdMinutes,
            entryPrice: row.entryPrice,
            exitPrice: row.exitPrice,
            exitReason: row.exitReason,
            rMultiple: row.rMultiple,
            pnlUsd: row.pnlUsd,
        })),
    };

    await writeFile(outPath, `${JSON.stringify(payload)}\n`, 'utf8');
    process.stdout.write(
        JSON.stringify({
            ok: true,
            scenarioId,
            stressId,
            trades: replay.summary.trades,
            netR: replay.summary.netR,
            netPnlUsd: replay.summary.netPnlUsd,
            elapsedMs,
            outPath,
        }) + '\n',
    );
}

main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
});
