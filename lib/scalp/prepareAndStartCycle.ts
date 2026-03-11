import { fetchCapitalCandlesByEpicDateRange, resolveCapitalEpicRuntime } from '../capital';
import {
    loadScalpCandleHistoryBulk,
    mergeScalpCandleHistory,
    normalizeHistoryTimeframe,
    saveScalpCandleHistoryBulk,
} from './candleHistory';
import { evaluateResearchCyclePreflight, startScalpResearchCycle, type StartResearchCycleParams } from './researchCycle';
import { refreshScalpResearchPortfolioReport } from './researchReporting';
import { loadScalpSymbolUniverseSnapshot, runScalpSymbolDiscoveryCycle } from './symbolDiscovery';
import type { ScalpCandle } from './types';

const ONE_DAY_MS = 24 * 60 * 60 * 1000;

export interface PrepareAndStartCycleParams extends StartResearchCycleParams {
    includeLiveQuotes?: boolean;
    seedTimeframe?: string;
    maxRequestsPerSymbol?: number;
    maxSymbolsPerRun?: number;
    batchCursor?: number;
    runDiscovery?: boolean;
    finalizeBatch?: boolean;
    maxDurationMs?: number;
    nowMs?: number;
}

export interface PrepareAndStartCycleResult {
    ok: boolean;
    started: boolean;
    dryRun: boolean;
    nowMs: number;
    nowIso: string;
    symbols: string[];
    batch: {
        totalSymbols: number;
        processedSymbols: string[];
        batchCursor: number;
        maxSymbolsPerRun: number;
        nextCursor: number | null;
        hasMore: boolean;
        finalized: boolean;
    };
    lookbackDays: number;
    maxRequestsPerSymbol: number;
    steps: {
        discovery: {
            generatedAtIso: string | null;
            selectedSymbols: string[];
            candidatesEvaluated: number;
        };
        fill: Array<{
            symbol: string;
            timeframe: string;
            epic: string | null;
            existingCount: number;
            fetchedCount: number;
            mergedCount: number;
            addedCount: number;
            saved: boolean;
            fetchFromMs: number;
            fetchToMs: number;
            error: string | null;
        }>;
        report: {
            generatedAtIso: string | null;
            cycleId: string | null;
        };
        preflight: Awaited<ReturnType<typeof evaluateResearchCyclePreflight>> | null;
    };
    cycle: Awaited<ReturnType<typeof startScalpResearchCycle>>['cycle'] | null;
}

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function parsePositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function dedupe<T>(rows: T[]): T[] {
    return Array.from(new Set(rows));
}

function normalizeFetchedCandles(rows: unknown[]): ScalpCandle[] {
    return rows
        .map((row) => {
            const value = Array.isArray(row) ? row : [];
            const ts = Number(value[0]);
            const open = Number(value[1]);
            const high = Number(value[2]);
            const low = Number(value[3]);
            const close = Number(value[4]);
            const volume = Number(value[5] ?? 0);
            if (![ts, open, high, low, close].every((v) => Number.isFinite(v) && v > 0)) return null;
            return [Math.floor(ts), open, high, low, close, Number.isFinite(volume) ? volume : 0] as ScalpCandle;
        })
        .filter((row): row is ScalpCandle => Boolean(row))
        .sort((a, b) => a[0] - b[0]);
}

export async function prepareAndStartScalpResearchCycle(
    params: PrepareAndStartCycleParams = {},
): Promise<PrepareAndStartCycleResult> {
    const invokeStartedAtMs = Date.now();
    const nowMs = Number.isFinite(Number(params.nowMs)) ? Math.floor(Number(params.nowMs)) : invokeStartedAtMs;
    const dryRun = Boolean(params.dryRun);
    const lookbackDays = parsePositiveInt(params.lookbackDays, 90);
    const minCandlesPerTask = parsePositiveInt(params.minCandlesPerTask, 180);
    const includeLiveQuotes = params.includeLiveQuotes === true;
    const seedTimeframe = normalizeHistoryTimeframe(params.seedTimeframe || '1m');
    const defaultMaxRequests = Math.max(20, Math.min(800, Math.ceil((lookbackDays * 24 * 60) / 900) + 10));
    const maxRequestsPerSymbol = parsePositiveInt(params.maxRequestsPerSymbol, defaultMaxRequests);
    const maxSymbolsPerRun = Math.max(1, parsePositiveInt(params.maxSymbolsPerRun, 1000));
    const maxDurationMs = Math.max(30_000, Math.min(10 * 60_000, parsePositiveInt(params.maxDurationMs, 4 * 60_000)));
    const batchCursor = Math.max(0, parsePositiveInt(params.batchCursor, 0) - 1 + 1);
    const runDiscovery = params.runDiscovery !== false;

    const discovery = runDiscovery
        ? await runScalpSymbolDiscoveryCycle({
              dryRun,
              includeLiveQuotes,
              nowMs,
              seedTimeframe,
              seedTopSymbols: params.symbols?.length ? params.symbols.length : undefined,
              seedTargetHistoryDays: Math.max(lookbackDays, 7),
              seedMaxHistoryDays: Math.max(lookbackDays + 7, 14),
              seedChunkDays: Math.min(14, Math.max(1, params.chunkDays || 3)),
              seedMaxRequestsPerSymbol: maxRequestsPerSymbol,
              seedMaxSymbolsPerRun: params.symbols?.length ? params.symbols.length : undefined,
              seedAllowBootstrapSymbols: true,
          })
        : await loadScalpSymbolUniverseSnapshot();

    const symbols = dedupe(
        (params.symbols?.length ? params.symbols : discovery?.selectedSymbols || [])
            .map((row) => normalizeSymbol(row))
            .filter((row) => Boolean(row)),
    );
    const batchStart = Math.min(symbols.length, Math.max(0, batchCursor));
    const batchEnd = Math.min(symbols.length, batchStart + maxSymbolsPerRun);
    const batchSymbols = symbols.slice(batchStart, batchEnd);
    let effectiveBatchEnd = batchEnd;
    const hasMore = batchEnd < symbols.length;
    let nextCursor = hasMore ? batchEnd : null;
    let finalized = params.finalizeBatch === true || !hasMore;

    const fillRows: PrepareAndStartCycleResult['steps']['fill'] = [];
    const fetchFromMs = nowMs - lookbackDays * ONE_DAY_MS;
    const fetchToMs = nowMs;
    const processedSymbols: string[] = [];
    const existingBySymbol = new Map<string, ScalpCandle[]>();
    if (batchSymbols.length > 0) {
        const existingBatch = await loadScalpCandleHistoryBulk(batchSymbols, seedTimeframe);
        for (let i = 0; i < batchSymbols.length; i += 1) {
            const symbol = batchSymbols[i]!;
            existingBySymbol.set(symbol, existingBatch[i]?.record?.candles || []);
        }
    }
    const pendingSaves: Array<{
        symbol: string;
        timeframe: string;
        epic: string | null;
        source: 'capital';
        candles: ScalpCandle[];
    }> = [];
    for (const symbol of batchSymbols) {
        if (Date.now() - invokeStartedAtMs >= maxDurationMs) {
            break;
        }
        try {
            const existing = existingBySymbol.get(symbol) || [];
            const epicResolved = await resolveCapitalEpicRuntime(symbol);
            const fetchedRaw = await fetchCapitalCandlesByEpicDateRange(epicResolved.epic, seedTimeframe, fetchFromMs, fetchToMs, {
                maxPerRequest: 1000,
                maxRequests: maxRequestsPerSymbol,
                debug: false,
                debugLabel: `prepare-cycle:${symbol}:${seedTimeframe}`,
            });
            const fetched = normalizeFetchedCandles(fetchedRaw);
            const merged = mergeScalpCandleHistory(existing, fetched);
            const addedCount = Math.max(0, merged.length - existing.length);
            let saved = false;
            if (!dryRun) {
                pendingSaves.push({
                    symbol,
                    timeframe: seedTimeframe,
                    epic: epicResolved.epic,
                    source: 'capital',
                    candles: merged,
                });
                saved = true;
            }
            fillRows.push({
                symbol,
                timeframe: seedTimeframe,
                epic: epicResolved.epic,
                existingCount: existing.length,
                fetchedCount: fetched.length,
                mergedCount: merged.length,
                addedCount,
                saved,
                fetchFromMs,
                fetchToMs,
                error: null,
            });
            processedSymbols.push(symbol);
        } catch (err: any) {
            fillRows.push({
                symbol,
                timeframe: seedTimeframe,
                epic: null,
                existingCount: 0,
                fetchedCount: 0,
                mergedCount: 0,
                addedCount: 0,
                saved: false,
                fetchFromMs,
                fetchToMs,
                error: String(err?.message || err || 'fill_failed'),
            });
            processedSymbols.push(symbol);
        }
    }
    if (!dryRun && pendingSaves.length > 0) {
        await saveScalpCandleHistoryBulk(pendingSaves, {});
    }

    effectiveBatchEnd = batchStart + processedSymbols.length;
    nextCursor = effectiveBatchEnd < symbols.length ? effectiveBatchEnd : null;
    finalized = params.finalizeBatch === true || nextCursor === null;

    const report = finalized ? await refreshScalpResearchPortfolioReport({ nowMs, persist: false }) : null;
    const preflight = finalized
        ? await evaluateResearchCyclePreflight({
              symbols,
              lookbackDays,
              minCandlesPerTask,
              requireUniverseSnapshot: false,
              requireReportSnapshot: false,
              maxCandleChecks: Math.max(1, symbols.length),
          })
        : null;

    if (!finalized) {
        return {
            ok: true,
            started: false,
            dryRun,
            nowMs,
            nowIso: new Date(nowMs).toISOString(),
            symbols,
            batch: {
                totalSymbols: symbols.length,
                processedSymbols,
                batchCursor: batchStart,
                maxSymbolsPerRun,
                nextCursor,
                hasMore: nextCursor !== null,
                finalized: false,
            },
            lookbackDays,
            maxRequestsPerSymbol,
            steps: {
                discovery: {
                    generatedAtIso: discovery?.generatedAtIso || null,
                    selectedSymbols: discovery?.selectedSymbols || symbols,
                    candidatesEvaluated: Number(discovery?.candidatesEvaluated || 0),
                },
                fill: fillRows,
                report: {
                    generatedAtIso: null,
                    cycleId: null,
                },
                preflight: null,
            },
            cycle: null,
        };
    }

    if (!preflight?.ready) {
        return {
            ok: false,
            started: false,
            dryRun,
            nowMs,
            nowIso: new Date(nowMs).toISOString(),
            symbols,
            batch: {
                totalSymbols: symbols.length,
                processedSymbols,
                batchCursor: batchStart,
                maxSymbolsPerRun,
                nextCursor,
                hasMore: nextCursor !== null,
                finalized: true,
            },
            lookbackDays,
            maxRequestsPerSymbol,
            steps: {
                discovery: {
                    generatedAtIso: discovery?.generatedAtIso || null,
                    selectedSymbols: discovery?.selectedSymbols || symbols,
                    candidatesEvaluated: Number(discovery?.candidatesEvaluated || 0),
                },
                fill: fillRows,
                report: {
                    generatedAtIso: report?.generatedAtIso || null,
                    cycleId: report?.cycle?.cycleId || null,
                },
                preflight: preflight || null,
            },
            cycle: null,
        };
    }

    const started = await startScalpResearchCycle({
        ...params,
        dryRun,
        symbols,
        lookbackDays,
        minCandlesPerTask,
        requireUniverseSnapshot: false,
        requireReportSnapshot: false,
        startedBy: params.startedBy || 'cron:prepare-and-start-cycle',
    });

    return {
        ok: true,
        started: started.started,
        dryRun,
        nowMs,
        nowIso: new Date(nowMs).toISOString(),
        symbols,
        batch: {
            totalSymbols: symbols.length,
            processedSymbols,
            batchCursor: batchStart,
            maxSymbolsPerRun,
            nextCursor,
            hasMore: nextCursor !== null,
            finalized: true,
        },
        lookbackDays,
        maxRequestsPerSymbol,
        steps: {
            discovery: {
                generatedAtIso: discovery?.generatedAtIso || null,
                selectedSymbols: discovery?.selectedSymbols || symbols,
                candidatesEvaluated: Number(discovery?.candidatesEvaluated || 0),
            },
            fill: fillRows,
            report: {
                generatedAtIso: report?.generatedAtIso || null,
                cycleId: report?.cycle?.cycleId || null,
            },
            preflight: preflight || null,
        },
        cycle: started.cycle,
    };
}
