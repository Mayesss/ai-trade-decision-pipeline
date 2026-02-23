import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import type { ReplayResult } from './types';

function csvEscape(value: unknown): string {
    if (value === null || value === undefined) return '';
    const str = String(value);
    if (!/[,"\n]/.test(str)) return str;
    return `"${str.replace(/"/g, '""')}"`;
}

function ledgerToCsv(result: ReplayResult): string {
    const header = [
        'id',
        'ts',
        'kind',
        'side',
        'price',
        'units',
        'notionalUsd',
        'pnlUsd',
        'feeUsd',
        'reasonCodes',
        'positionUnitsAfter',
        'equityUsdAfter',
    ].join(',');
    const rows = result.ledger.map((row) =>
        [
            row.id,
            row.ts,
            row.kind,
            row.side ?? '',
            row.price ?? '',
            row.units ?? '',
            row.notionalUsd ?? '',
            row.pnlUsd,
            row.feeUsd,
            row.reasonCodes.join('|'),
            row.positionUnitsAfter,
            row.equityUsdAfter,
        ]
            .map(csvEscape)
            .join(','),
    );
    return [header, ...rows].join('\n');
}

export async function writeReplayArtifacts(outDir: string, result: ReplayResult): Promise<void> {
    const resolved = path.resolve(outDir);
    await mkdir(resolved, { recursive: true });
    await Promise.all([
        writeFile(path.join(resolved, 'summary.json'), JSON.stringify(result.summary, null, 2), 'utf8'),
        writeFile(path.join(resolved, 'equity.json'), JSON.stringify(result.equityCurve, null, 2), 'utf8'),
        writeFile(path.join(resolved, 'timeline.json'), JSON.stringify(result.timeline, null, 2), 'utf8'),
        writeFile(path.join(resolved, 'ledger.csv'), ledgerToCsv(result), 'utf8'),
    ]);
}
