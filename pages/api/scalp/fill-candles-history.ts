export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { fetchCapitalCandlesByEpicDateRange, resolveCapitalEpicRuntime } from '../../../lib/capital';
import { requireAdminAccess } from '../../../lib/admin';
import {
    type CandleHistoryBackend,
    loadScalpCandleHistory,
    mergeScalpCandleHistory,
    normalizeHistoryTimeframe,
    saveScalpCandleHistory,
    timeframeToMs,
} from '../../../lib/scalp/candleHistory';
import type { ScalpCandle } from '../../../lib/scalp/types';

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

function parseBool(value: string | string[] | undefined, fallback: boolean): boolean {
    if (value === undefined) return fallback;
    const first = Array.isArray(value) ? value[0] : value;
    if (first === undefined) return fallback;
    const normalized = String(first).trim().toLowerCase();
    if (['false', '0', 'no', 'off'].includes(normalized)) return false;
    if (['true', '1', 'yes', 'on'].includes(normalized)) return true;
    return fallback;
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
    if (typeof value === 'string') return value.trim() || undefined;
    if (Array.isArray(value) && value.length > 0) {
        return String(value[0] || '').trim() || undefined;
    }
    return undefined;
}

function parseNowMs(value: string | undefined): number | undefined {
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return Math.floor(n);
    return undefined;
}

function toPositiveInt(value: string | undefined, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return Math.max(1, Math.floor(n));
}

function parseDirection(value: string | undefined): 'backfill' | 'forward' {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'forward') return 'forward';
    return 'backfill';
}

function parseBackend(value: string | undefined): CandleHistoryBackend | undefined {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'file') return 'file';
    if (normalized === 'kv') return 'kv';
    return undefined;
}

function normalizeSymbol(value: string | undefined): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function normalizeFetchedCandles(rows: any[]): ScalpCandle[] {
    return rows
        .map((row) => {
            const ts = Number(row?.[0]);
            const open = Number(row?.[1]);
            const high = Number(row?.[2]);
            const low = Number(row?.[3]);
            const close = Number(row?.[4]);
            const volume = Number(row?.[5] ?? 0);
            if (![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) return null;
            return [Math.floor(ts), open, high, low, close, Number.isFinite(volume) ? volume : 0] as ScalpCandle;
        })
        .filter((row): row is ScalpCandle => Boolean(row))
        .sort((a, b) => a[0] - b[0]);
}

function setNoStoreHeaders(res: NextApiResponse): void {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    try {
        const symbol = normalizeSymbol(firstQueryValue(req.query.symbol));
        if (!symbol) {
            return res.status(400).json({ error: 'symbol_required', message: 'Provide symbol (e.g. EURUSD).' });
        }

        const timeframe = normalizeHistoryTimeframe(firstQueryValue(req.query.timeframe) || '15m');
        const direction = parseDirection(firstQueryValue(req.query.direction));
        const days = Math.max(1, Math.min(120, toPositiveInt(firstQueryValue(req.query.days), 30)));
        const dryRun = parseBool(req.query.dryRun, true);
        const nowMs = parseNowMs(firstQueryValue(req.query.nowMs)) ?? Date.now();
        const debug = parseBool(req.query.debug, false);
        const backend = parseBackend(firstQueryValue(req.query.backend));

        const history = await loadScalpCandleHistory(symbol, timeframe, { backend });
        const existing = history.record?.candles ?? [];
        const tfMs = timeframeToMs(timeframe);
        const maxRequests = Math.max(20, Math.min(240, toPositiveInt(firstQueryValue(req.query.maxRequests), 120)));

        const baseAnchorMs =
            direction === 'backfill'
                ? existing[0]?.[0] ?? nowMs
                : existing[existing.length - 1]?.[0] ?? nowMs - days * ONE_DAY_MS;

        let fetchFromMs = direction === 'backfill' ? baseAnchorMs - days * ONE_DAY_MS : baseAnchorMs + tfMs;
        let fetchToMs = direction === 'backfill' ? baseAnchorMs - tfMs : baseAnchorMs + days * ONE_DAY_MS;
        const clampedToNow = direction === 'forward' && fetchToMs > nowMs;
        if (direction === 'forward') fetchToMs = Math.min(fetchToMs, nowMs);
        fetchFromMs = Math.max(0, Math.floor(fetchFromMs));
        fetchToMs = Math.max(0, Math.floor(fetchToMs));

        if (!(fetchToMs > fetchFromMs)) {
            return res.status(200).json({
                ok: true,
                symbol,
                timeframe,
                direction,
                dryRun,
                days,
                backend: history.backend,
                storageRef: history.storageRef,
                existingCount: existing.length,
                fetchedCount: 0,
                mergedCount: existing.length,
                addedCount: 0,
                clampedToNow,
                message: 'No fetch needed for requested window.',
            });
        }

        const epicResolved = await resolveCapitalEpicRuntime(symbol);
        const fetchedRaw = await fetchCapitalCandlesByEpicDateRange(epicResolved.epic, timeframe, fetchFromMs, fetchToMs, {
            maxPerRequest: 1000,
            maxRequests,
            debug,
            debugLabel: `${symbol}:${timeframe}:fill`,
        });
        const fetched = normalizeFetchedCandles(fetchedRaw);
        const merged = mergeScalpCandleHistory(existing, fetched);
        const addedCount = Math.max(0, merged.length - existing.length);

        let saveResult: { backend: string; storageRef: string; saved: boolean } | null = null;
        if (!dryRun) {
            saveResult = await saveScalpCandleHistory({
                symbol,
                timeframe,
                epic: epicResolved.epic,
                source: 'capital',
                candles: merged,
            }, { backend });
        }

        return res.status(200).json({
            ok: true,
            symbol,
            epic: epicResolved.epic,
            timeframe,
            direction,
            dryRun,
            backendRequested: backend || 'auto',
            days,
            fetchFromMs,
            fetchToMs,
            clampedToNow,
            backend: saveResult?.backend || history.backend,
            storageRef: saveResult?.storageRef || history.storageRef,
            existingCount: existing.length,
            fetchedCount: fetched.length,
            mergedCount: merged.length,
            addedCount,
            saved: saveResult?.saved ?? false,
            coverage: {
                before: {
                    fromTsMs: existing[0]?.[0] ?? null,
                    toTsMs: existing[existing.length - 1]?.[0] ?? null,
                },
                after: {
                    fromTsMs: merged[0]?.[0] ?? null,
                    toTsMs: merged[merged.length - 1]?.[0] ?? null,
                },
            },
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'fill_candles_history_failed',
            message: err?.message || String(err),
        });
    }
}
