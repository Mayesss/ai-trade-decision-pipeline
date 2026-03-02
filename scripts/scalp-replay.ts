#!/usr/bin/env node
import { readFile } from 'node:fs/promises';
import path from 'node:path';

import { defaultScalpReplayConfig, normalizeScalpReplayInput, runScalpReplay } from '../lib/scalp/replay/harness';
import { writeScalpReplayArtifacts } from '../lib/scalp/replay/io';
import type { ScalpReplayInputFile, ScalpReplayRuntimeConfig } from '../lib/scalp/replay/types';

function usage() {
    return [
        'Usage:',
        '  node --import tsx scripts/scalp-replay.ts --input <candles.json> [options]',
        '',
        'Options:',
        '  --outDir <path>             Output folder (default: /tmp/scalp-replay)',
        '  --symbol <ticker>           Override symbol',
        '  --executeMinutes <int>      Replay cron cadence in minutes',
        '  --spreadFactor <float>      Multiplier for input spread',
        '  --slippagePips <float>      Per-fill adverse slippage in pips',
        '  --defaultSpreadPips <float> Fallback spread when missing in candles',
        '  --asiaBaseTf <M1|M3|M5|M15>',
        '  --confirmTf <M1|M3>',
        '  --maxTradesPerDay <int>',
        '  --riskPct <float>           Risk per trade percent',
        '  --referenceEquity <float>',
        '  --tpR <float>               Take-profit in R',
        '  --sweepBufferPips <float>',
        '  --sweepRejectMaxBars <int>',
        '  --displacementBodyAtrMult <float>',
        '  --displacementRangeAtrMult <float>',
        '  --mssLookbackBars <int>',
        '  --ifvgMinAtrMult <float>',
        '  --ifvgMaxAtrMult <float>',
        '  --ifvgEntryMode <first_touch|midline_touch|full_fill>',
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

function parseIfvgEntryMode(value: unknown): ScalpReplayRuntimeConfig['strategy']['ifvgEntryMode'] | undefined {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'first_touch' || normalized === 'midline_touch' || normalized === 'full_fill') {
        return normalized;
    }
    return undefined;
}

function applyOverrides(config: ScalpReplayRuntimeConfig, args: Record<string, string | boolean>): ScalpReplayRuntimeConfig {
    const next: ScalpReplayRuntimeConfig = JSON.parse(JSON.stringify(config));
    const symbol = typeof args.symbol === 'string' ? String(args.symbol).trim().toUpperCase() : '';
    if (symbol) next.symbol = symbol;

    const executeMinutes = toNum(args.executeMinutes);
    if (executeMinutes !== undefined && executeMinutes > 0) next.executeMinutes = Math.floor(executeMinutes);
    const spreadFactor = toNum(args.spreadFactor);
    if (spreadFactor !== undefined && spreadFactor > 0) next.spreadFactor = spreadFactor;
    const slippagePips = toNum(args.slippagePips);
    if (slippagePips !== undefined && slippagePips >= 0) next.slippagePips = slippagePips;
    const defaultSpreadPips = toNum(args.defaultSpreadPips);
    if (defaultSpreadPips !== undefined && defaultSpreadPips >= 0) next.defaultSpreadPips = defaultSpreadPips;

    const asiaBaseTf = typeof args.asiaBaseTf === 'string' ? String(args.asiaBaseTf).trim().toUpperCase() : '';
    if (asiaBaseTf === 'M1' || asiaBaseTf === 'M3' || asiaBaseTf === 'M5' || asiaBaseTf === 'M15') {
        next.strategy.asiaBaseTf = asiaBaseTf;
    }
    const confirmTf = typeof args.confirmTf === 'string' ? String(args.confirmTf).trim().toUpperCase() : '';
    if (confirmTf === 'M1' || confirmTf === 'M3') next.strategy.confirmTf = confirmTf;

    const maxTradesPerDay = toNum(args.maxTradesPerDay);
    if (maxTradesPerDay !== undefined && maxTradesPerDay > 0) next.strategy.maxTradesPerDay = Math.floor(maxTradesPerDay);
    const riskPct = toNum(args.riskPct);
    if (riskPct !== undefined && riskPct > 0) next.strategy.riskPerTradePct = riskPct;
    const referenceEquity = toNum(args.referenceEquity);
    if (referenceEquity !== undefined && referenceEquity > 0) next.strategy.referenceEquityUsd = referenceEquity;
    const tpR = toNum(args.tpR);
    if (tpR !== undefined && tpR > 0) next.strategy.takeProfitR = tpR;
    const sweepBufferPips = toNum(args.sweepBufferPips);
    if (sweepBufferPips !== undefined && sweepBufferPips >= 0) next.strategy.sweepBufferPips = sweepBufferPips;
    const sweepRejectMaxBars = toNum(args.sweepRejectMaxBars);
    if (sweepRejectMaxBars !== undefined && sweepRejectMaxBars > 0) next.strategy.sweepRejectMaxBars = Math.floor(sweepRejectMaxBars);
    const displacementBodyAtrMult = toNum(args.displacementBodyAtrMult);
    if (displacementBodyAtrMult !== undefined && displacementBodyAtrMult >= 0) {
        next.strategy.displacementBodyAtrMult = displacementBodyAtrMult;
    }
    const displacementRangeAtrMult = toNum(args.displacementRangeAtrMult);
    if (displacementRangeAtrMult !== undefined && displacementRangeAtrMult >= 0) {
        next.strategy.displacementRangeAtrMult = displacementRangeAtrMult;
    }
    const mssLookbackBars = toNum(args.mssLookbackBars);
    if (mssLookbackBars !== undefined && mssLookbackBars > 0) next.strategy.mssLookbackBars = Math.floor(mssLookbackBars);
    const ifvgMinAtrMult = toNum(args.ifvgMinAtrMult);
    if (ifvgMinAtrMult !== undefined && ifvgMinAtrMult >= 0) next.strategy.ifvgMinAtrMult = ifvgMinAtrMult;
    const ifvgMaxAtrMult = toNum(args.ifvgMaxAtrMult);
    if (ifvgMaxAtrMult !== undefined && ifvgMaxAtrMult > 0) next.strategy.ifvgMaxAtrMult = ifvgMaxAtrMult;
    const ifvgEntryMode = parseIfvgEntryMode(args.ifvgEntryMode);
    if (ifvgEntryMode) next.strategy.ifvgEntryMode = ifvgEntryMode;

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
    const outDir = typeof args.outDir === 'string' ? String(args.outDir) : '/tmp/scalp-replay';
    const raw = await readFile(inputPath, 'utf8');
    const input = JSON.parse(raw) as ScalpReplayInputFile;
    const normalized = normalizeScalpReplayInput(input);
    const baseConfig = defaultScalpReplayConfig(normalized.symbol);
    const config = applyOverrides(baseConfig, args);
    config.symbol = normalized.symbol;
    const result = runScalpReplay({
        candles: normalized.candles,
        pipSize: normalized.pipSize,
        config,
    });
    await writeScalpReplayArtifacts(outDir, result);

    console.log(`Scalp replay complete for ${result.summary.symbol}`);
    console.log(`Window: ${result.summary.startTs ?? 'n/a'} -> ${result.summary.endTs ?? 'n/a'}`);
    console.log(
        `Trades: ${result.summary.trades} | WinRate: ${result.summary.winRatePct.toFixed(2)}% | AvgR: ${result.summary.avgR.toFixed(3)} | NetR: ${result.summary.netR.toFixed(3)}`,
    );
    console.log(`NetPnL: ${result.summary.netPnlUsd.toFixed(2)} USD | MaxDD(R): ${result.summary.maxDrawdownR.toFixed(3)}`);
    console.log(`Artifacts: ${path.resolve(outDir)}`);
}

main().catch((err) => {
    console.error('Scalp replay failed:', err);
    process.exitCode = 1;
});
