import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';

import type { ScalpReplayResult } from './types';

function csvEscape(value: unknown): string {
    const raw = String(value ?? '');
    if (/[",\n]/.test(raw)) {
        return `"${raw.replace(/"/g, '""')}"`;
    }
    return raw;
}

function toTradesCsv(result: ScalpReplayResult): string {
    const headers = [
        'id',
        'dayKey',
        'side',
        'entryTs',
        'exitTs',
        'holdMinutes',
        'entryPrice',
        'stopPrice',
        'takeProfitPrice',
        'exitPrice',
        'exitReason',
        'riskAbs',
        'riskUsd',
        'notionalUsd',
        'rMultiple',
        'pnlUsd',
    ];
    const lines = [headers.join(',')];
    for (const trade of result.trades) {
        const row = [
            trade.id,
            trade.dayKey,
            trade.side,
            trade.entryTs,
            trade.exitTs,
            trade.holdMinutes.toFixed(4),
            trade.entryPrice.toFixed(8),
            trade.stopPrice.toFixed(8),
            trade.takeProfitPrice.toFixed(8),
            trade.exitPrice.toFixed(8),
            trade.exitReason,
            trade.riskAbs.toFixed(8),
            trade.riskUsd.toFixed(4),
            trade.notionalUsd.toFixed(4),
            trade.rMultiple.toFixed(6),
            trade.pnlUsd.toFixed(4),
        ];
        lines.push(row.map(csvEscape).join(','));
    }
    return `${lines.join('\n')}\n`;
}

function toTimelineCsv(result: ScalpReplayResult): string {
    const headers = ['ts', 'type', 'state', 'reasonCodes'];
    const lines = [headers.join(',')];
    for (const event of result.timeline) {
        const row = [event.ts, event.type, event.state || '', event.reasonCodes.join('|')];
        lines.push(row.map(csvEscape).join(','));
    }
    return `${lines.join('\n')}\n`;
}

export async function writeScalpReplayArtifacts(outDir: string, result: ScalpReplayResult): Promise<void> {
    const dir = path.resolve(outDir);
    await mkdir(dir, { recursive: true });
    await writeFile(path.join(dir, 'summary.json'), JSON.stringify(result.summary, null, 2), 'utf8');
    await writeFile(path.join(dir, 'config.json'), JSON.stringify(result.config, null, 2), 'utf8');
    await writeFile(path.join(dir, 'trades.json'), JSON.stringify(result.trades, null, 2), 'utf8');
    await writeFile(path.join(dir, 'timeline.json'), JSON.stringify(result.timeline, null, 2), 'utf8');
    await writeFile(path.join(dir, 'trades.csv'), toTradesCsv(result), 'utf8');
    await writeFile(path.join(dir, 'timeline.csv'), toTimelineCsv(result), 'utf8');
}
