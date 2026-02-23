#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import path from 'node:path';

import { defaultReplayConfig, normalizeReplayInput, runReplay } from '../lib/forex/replay/harness';
import { writeReplayArtifacts } from '../lib/forex/replay/io';
import type { ReplayInputFile, ReplayRuntimeConfig } from '../lib/forex/replay/types';

function usage() {
    return [
        'Usage:',
        '  node --import tsx scripts/forex-replay.ts --input <quotes.json> [options]',
        '',
        'Options:',
        '  --outDir <path>                Output folder (default: /tmp/forex-replay)',
        '  --pair <symbol>                Override pair (default from input or EURUSD)',
        '  --seed <int>                   Slippage RNG seed',
        '  --startingEquity <usd>         Starting equity',
        '  --notional <usd>               Default entry notional',
        '  --atr1h <abs>                  ATR1h absolute value for spread-to-ATR gates',
        '  --transitionBufferMin <int>    Session transition buffer in minutes',
        '  --transitionSpreadMult <float> Transition spread multiplier',
        '  --rolloverFeeBps <float>       Daily rollover fee in bps',
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

function applyOverrides(config: ReplayRuntimeConfig, args: Record<string, string | boolean>): ReplayRuntimeConfig {
    const next = { ...config };
    const pair = typeof args.pair === 'string' ? String(args.pair).trim().toUpperCase() : '';
    if (pair) next.pair = pair;

    const seed = toNum(args.seed);
    if (seed !== undefined) next.slippage.seed = Math.floor(seed);
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
    const transitionSpreadMult = toNum(args.transitionSpreadMult);
    if (transitionSpreadMult !== undefined && transitionSpreadMult >= 1) {
        next.spreadStress.transitionMultiplier = transitionSpreadMult;
    }
    const rolloverFeeBps = toNum(args.rolloverFeeBps);
    if (rolloverFeeBps !== undefined && rolloverFeeBps >= 0) {
        next.rollover.dailyFeeBps = rolloverFeeBps;
    }
    return next;
}

async function main() {
    const args = parseArgs(process.argv.slice(2));
    if (args.help) {
        console.log(usage());
        return;
    }
    if (typeof args.input !== 'string' || !args.input.trim()) {
        console.error('Missing required --input argument.\n');
        console.error(usage());
        process.exitCode = 1;
        return;
    }

    const inputPath = path.resolve(String(args.input));
    const outDir = typeof args.outDir === 'string' ? String(args.outDir) : '/tmp/forex-replay';
    const raw = await readFile(inputPath, 'utf8');
    const input = JSON.parse(raw) as ReplayInputFile;
    const normalized = normalizeReplayInput(input);

    const baseConfig = defaultReplayConfig(normalized.pair);
    const config = applyOverrides(baseConfig, args);
    const result = runReplay({
        quotes: normalized.quotes,
        entries: normalized.entries,
        config,
    });
    await writeReplayArtifacts(outDir, result);

    console.log(`Replay complete for ${result.summary.pair}`);
    console.log(`Window: ${result.summary.startTs ?? 'n/a'} -> ${result.summary.endTs ?? 'n/a'}`);
    console.log(
        `P&L: ${result.summary.realizedPnlUsd.toFixed(2)} USD | Return: ${result.summary.returnPct.toFixed(2)}% | MaxDD: ${result.summary.maxDrawdownPct.toFixed(2)}%`,
    );
    console.log(`Closed legs: ${result.summary.closedLegs} | WinRate: ${result.summary.winRatePct.toFixed(2)}%`);
    console.log(`Rollover fees: ${result.summary.rolloverFeesUsd.toFixed(2)} USD`);
    console.log(`Artifacts written to: ${path.resolve(outDir)}`);
}

main().catch((err) => {
    console.error('Replay failed:', err);
    process.exitCode = 1;
});
