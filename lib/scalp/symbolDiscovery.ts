import { mkdir, readdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import defaultTickerEpicMap from '../../data/capitalTickerMap.json';
import { fetchCapitalLivePrice } from '../capital';
import { kvGetJson, kvSetJson } from '../kv';
import { loadScalpCandleHistory } from './candleHistory';
import { loadScalpDeploymentRegistry } from './deploymentRegistry';
import { pipSizeForScalpSymbol } from './marketData';
import { listScalpStrategies } from './strategies/registry';

export interface ScalpSymbolDiscoveryPolicy {
    version: 1;
    updatedAt: string | null;
    notes: string | null;
    limits: {
        maxUniverseSymbols: number;
        minUniverseSymbols: number;
        maxWeeklyAdds: number;
        maxWeeklyRemoves: number;
        maxCandidates: number;
    };
    criteria: {
        minHistoryDays: number;
        minHistoryCoveragePct: number;
        minAvgBarsPerDay: number;
        minRecentBars7d: number;
        minMedianRangePct: number;
        maxSpreadPips: number | null;
        requireTradableQuote: boolean;
    };
    sources: {
        includeCapitalTickerMap: boolean;
        includeDeploymentSymbols: boolean;
        includeHistorySymbols: boolean;
        explicitSymbols: string[];
        excludedSymbols: string[];
    };
    pinnedSymbols: string[];
    strategyAllowlist: string[];
}

export interface ScalpSymbolCandidateMetrics {
    historyBars1m: number;
    historyFromTs: number | null;
    historyToTs: number | null;
    historySpanDays: number;
    historyCoveragePct: number;
    avgBarsPerDay: number;
    recentBars7d: number;
    medianRangePct: number;
    livePrice: number | null;
    liveSpreadPips: number | null;
}

export interface ScalpSymbolCandidateRow {
    symbol: string;
    eligible: boolean;
    score: number;
    reasons: string[];
    recommendedStrategyIds: string[];
    metrics: ScalpSymbolCandidateMetrics;
}

export interface ScalpSymbolUniverseSnapshot {
    version: 1;
    generatedAtIso: string;
    policy: ScalpSymbolDiscoveryPolicy;
    source: 'weekly_discovery_v1';
    dryRun: boolean;
    previousSymbols: string[];
    selectedSymbols: string[];
    addedSymbols: string[];
    removedSymbols: string[];
    candidatesEvaluated: number;
    selectedRows: ScalpSymbolCandidateRow[];
    topRejectedRows: ScalpSymbolCandidateRow[];
}

export interface ScalpSymbolDiscoveryRunParams {
    dryRun?: boolean;
    includeLiveQuotes?: boolean;
    nowMs?: number;
    maxCandidatesOverride?: number;
}

const DEFAULT_POLICY_PATH = path.resolve(process.cwd(), 'data/scalp-symbol-discovery-policy.json');
const DEFAULT_UNIVERSE_FILE_PATH = path.resolve(process.cwd(), 'data/scalp-symbol-universe.json');
const UNIVERSE_KV_KEY = 'scalp:symbol-universe:v1';

const DEFAULT_POLICY: ScalpSymbolDiscoveryPolicy = {
    version: 1,
    updatedAt: null,
    notes: 'Default scalp symbol discovery policy.',
    limits: {
        maxUniverseSymbols: 16,
        minUniverseSymbols: 4,
        maxWeeklyAdds: 2,
        maxWeeklyRemoves: 2,
        maxCandidates: 40,
    },
    criteria: {
        minHistoryDays: 45,
        minHistoryCoveragePct: 80,
        minAvgBarsPerDay: 900,
        minRecentBars7d: 4000,
        minMedianRangePct: 0.025,
        maxSpreadPips: 35,
        requireTradableQuote: true,
    },
    sources: {
        includeCapitalTickerMap: true,
        includeDeploymentSymbols: true,
        includeHistorySymbols: true,
        explicitSymbols: [],
        excludedSymbols: [],
    },
    pinnedSymbols: ['BTCUSDT', 'XAUUSDT'],
    strategyAllowlist: [
        'compression_breakout_pullback_m15_m3',
        'regime_pullback_m15_m3',
        'regime_pullback_m15_m3_btcusdt',
        'regime_pullback_m15_m3_xauusd',
        'trend_day_reacceleration_m15_m3',
        'pdh_pdl_reclaim_m15_m3',
    ],
};

function normalizeSymbol(value: unknown): string {
    return String(value || '')
        .trim()
        .toUpperCase()
        .replace(/[^A-Z0-9._-]/g, '');
}

function toBool(value: unknown, fallback: boolean): boolean {
    if (typeof value === 'boolean') return value;
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (!normalized) return fallback;
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
}

function toNumber(value: unknown, fallback: number): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return n;
}

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(toNumber(value, fallback));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toNonNegativeInt(value: unknown, fallback: number): number {
    const n = Math.floor(toNumber(value, fallback));
    if (!Number.isFinite(n) || n < 0) return fallback;
    return n;
}

function toNonNegativeNumber(value: unknown, fallback: number): number {
    const n = toNumber(value, fallback);
    if (!Number.isFinite(n) || n < 0) return fallback;
    return n;
}

function normalizeStringArray(value: unknown): string[] {
    const rows = Array.isArray(value) ? value : [];
    const normalized = rows.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row));
    return Array.from(new Set(normalized));
}

function normalizeStrategyIdArray(value: unknown): string[] {
    const rows = Array.isArray(value) ? value : [];
    const normalized = rows
        .map((row) =>
            String(row || '')
                .trim()
                .toLowerCase()
                .replace(/[^a-z0-9._-]/g, ''),
        )
        .filter((row) => Boolean(row));
    return Array.from(new Set(normalized));
}

function resolvePolicyPath(): string {
    const configured = String(process.env.SCALP_SYMBOL_DISCOVERY_POLICY_PATH || '').trim();
    if (!configured) return DEFAULT_POLICY_PATH;
    return path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
}

function resolveUniverseFilePath(): string {
    const configured = String(process.env.SCALP_SYMBOL_UNIVERSE_PATH || '').trim();
    if (!configured) return DEFAULT_UNIVERSE_FILE_PATH;
    return path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
}

function resolveUniverseStoreMode(): 'kv' | 'file' {
    const mode = String(process.env.SCALP_SYMBOL_UNIVERSE_STORE || 'auto')
        .trim()
        .toLowerCase();
    if (mode === 'kv') return 'kv';
    if (mode === 'file') return 'file';
    const hasKv = Boolean(process.env.KV_REST_API_URL && process.env.KV_REST_API_TOKEN);
    return hasKv ? 'kv' : 'file';
}

function normalizePolicy(raw: unknown): ScalpSymbolDiscoveryPolicy {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return { ...DEFAULT_POLICY };
    const row = raw as Record<string, unknown>;

    const maxUniverseSymbols = Math.max(1, toPositiveInt((row.limits as any)?.maxUniverseSymbols, DEFAULT_POLICY.limits.maxUniverseSymbols));
    const minUniverseSymbols = Math.max(
        1,
        Math.min(maxUniverseSymbols, toPositiveInt((row.limits as any)?.minUniverseSymbols, DEFAULT_POLICY.limits.minUniverseSymbols)),
    );

    const maxSpreadPipsRaw = toNumber((row.criteria as any)?.maxSpreadPips, NaN);
    const maxSpreadPips = Number.isFinite(maxSpreadPipsRaw) && maxSpreadPipsRaw >= 0 ? maxSpreadPipsRaw : null;

    return {
        version: 1,
        updatedAt: typeof row.updatedAt === 'string' ? row.updatedAt : null,
        notes: typeof row.notes === 'string' ? row.notes : null,
        limits: {
            maxUniverseSymbols,
            minUniverseSymbols,
            maxWeeklyAdds: toNonNegativeInt((row.limits as any)?.maxWeeklyAdds, DEFAULT_POLICY.limits.maxWeeklyAdds),
            maxWeeklyRemoves: toNonNegativeInt((row.limits as any)?.maxWeeklyRemoves, DEFAULT_POLICY.limits.maxWeeklyRemoves),
            maxCandidates: Math.max(1, toPositiveInt((row.limits as any)?.maxCandidates, DEFAULT_POLICY.limits.maxCandidates)),
        },
        criteria: {
            minHistoryDays: Math.max(1, toNonNegativeNumber((row.criteria as any)?.minHistoryDays, DEFAULT_POLICY.criteria.minHistoryDays)),
            minHistoryCoveragePct: Math.max(
                1,
                Math.min(100, toNonNegativeNumber((row.criteria as any)?.minHistoryCoveragePct, DEFAULT_POLICY.criteria.minHistoryCoveragePct)),
            ),
            minAvgBarsPerDay: Math.max(
                1,
                toNonNegativeNumber((row.criteria as any)?.minAvgBarsPerDay, DEFAULT_POLICY.criteria.minAvgBarsPerDay),
            ),
            minRecentBars7d: Math.max(
                0,
                toNonNegativeNumber((row.criteria as any)?.minRecentBars7d, DEFAULT_POLICY.criteria.minRecentBars7d),
            ),
            minMedianRangePct: Math.max(
                0,
                toNonNegativeNumber((row.criteria as any)?.minMedianRangePct, DEFAULT_POLICY.criteria.minMedianRangePct),
            ),
            maxSpreadPips,
            requireTradableQuote: toBool((row.criteria as any)?.requireTradableQuote, DEFAULT_POLICY.criteria.requireTradableQuote),
        },
        sources: {
            includeCapitalTickerMap: toBool(
                (row.sources as any)?.includeCapitalTickerMap,
                DEFAULT_POLICY.sources.includeCapitalTickerMap,
            ),
            includeDeploymentSymbols: toBool(
                (row.sources as any)?.includeDeploymentSymbols,
                DEFAULT_POLICY.sources.includeDeploymentSymbols,
            ),
            includeHistorySymbols: toBool((row.sources as any)?.includeHistorySymbols, DEFAULT_POLICY.sources.includeHistorySymbols),
            explicitSymbols: normalizeStringArray((row.sources as any)?.explicitSymbols),
            excludedSymbols: normalizeStringArray((row.sources as any)?.excludedSymbols),
        },
        pinnedSymbols: normalizeStringArray(row.pinnedSymbols),
        strategyAllowlist: normalizeStrategyIdArray(row.strategyAllowlist),
    };
}

export async function loadScalpSymbolDiscoveryPolicy(): Promise<ScalpSymbolDiscoveryPolicy> {
    const filePath = resolvePolicyPath();
    try {
        const raw = await readFile(filePath, 'utf8');
        return normalizePolicy(JSON.parse(raw));
    } catch {
        return normalizePolicy(DEFAULT_POLICY);
    }
}

export async function loadScalpSymbolUniverseSnapshot(): Promise<ScalpSymbolUniverseSnapshot | null> {
    const storeMode = resolveUniverseStoreMode();
    if (storeMode === 'kv') {
        return kvGetJson<ScalpSymbolUniverseSnapshot>(UNIVERSE_KV_KEY);
    }
    try {
        const raw = await readFile(resolveUniverseFilePath(), 'utf8');
        return JSON.parse(raw) as ScalpSymbolUniverseSnapshot;
    } catch {
        return null;
    }
}

async function saveScalpSymbolUniverseSnapshot(snapshot: ScalpSymbolUniverseSnapshot): Promise<void> {
    const storeMode = resolveUniverseStoreMode();
    if (storeMode === 'kv') {
        await kvSetJson(UNIVERSE_KV_KEY, snapshot);
        return;
    }
    const filePath = resolveUniverseFilePath();
    await mkdir(path.dirname(filePath), { recursive: true });
    await writeFile(filePath, `${JSON.stringify(snapshot, null, 2)}\n`, 'utf8');
}

async function listHistorySymbols(): Promise<string[]> {
    const root = path.resolve(process.cwd(), String(process.env.CANDLE_HISTORY_DIR || 'data/candles-history'));
    try {
        const entries = await readdir(root, { withFileTypes: true });
        const symbols = entries
            .filter((entry) => entry.isDirectory())
            .map((entry) => normalizeSymbol(entry.name))
            .filter((row) => Boolean(row));
        return Array.from(new Set(symbols));
    } catch {
        return [];
    }
}

function median(values: number[]): number {
    if (!values.length) return 0;
    const sorted = values.slice().sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    if (sorted.length % 2 === 1) return sorted[mid] || 0;
    return ((sorted[mid - 1] || 0) + (sorted[mid] || 0)) / 2;
}

function clamp(value: number, min: number, max: number): number {
    return Math.max(min, Math.min(max, value));
}

export function resolveRecommendedStrategiesForSymbol(symbolRaw: string, allowlist: string[]): string[] {
    const symbol = normalizeSymbol(symbolRaw);
    const has = (id: string) => allowlist.length === 0 || allowlist.includes(id);
    const out: string[] = [];

    const push = (id: string) => {
        if (!has(id)) return;
        out.push(id);
    };

    const isBtc = symbol === 'BTCUSDT' || symbol === 'BTCUSD';
    const isXau = symbol === 'XAUUSDT' || symbol === 'XAUUSD' || symbol === 'GOLD';
    const isFx = /^[A-Z]{6}$/.test(symbol) && !symbol.endsWith('USDT');

    if (isBtc) {
        push('regime_pullback_m15_m3_btcusdt');
        push('compression_breakout_pullback_m15_m3');
    } else if (isXau) {
        push('regime_pullback_m15_m3_xauusd');
        push('trend_day_reacceleration_m15_m3');
    } else if (isFx) {
        push('regime_pullback_m15_m3');
        push('pdh_pdl_reclaim_m15_m3');
    } else {
        push('regime_pullback_m15_m3');
        push('trend_day_reacceleration_m15_m3');
    }

    return Array.from(new Set(out));
}

async function buildCandidatePool(policy: ScalpSymbolDiscoveryPolicy): Promise<string[]> {
    const pool = new Set<string>();

    if (policy.sources.includeCapitalTickerMap) {
        for (const symbol of Object.keys(defaultTickerEpicMap as Record<string, string>)) {
            const normalized = normalizeSymbol(symbol);
            if (normalized) pool.add(normalized);
        }
    }

    if (policy.sources.includeDeploymentSymbols) {
        const deployments = await loadScalpDeploymentRegistry();
        for (const row of deployments.deployments) {
            const normalized = normalizeSymbol(row.symbol);
            if (normalized) pool.add(normalized);
        }
    }

    if (policy.sources.includeHistorySymbols) {
        for (const symbol of await listHistorySymbols()) {
            const normalized = normalizeSymbol(symbol);
            if (normalized) pool.add(normalized);
        }
    }

    for (const symbol of policy.sources.explicitSymbols) {
        const normalized = normalizeSymbol(symbol);
        if (normalized) pool.add(normalized);
    }

    for (const symbol of policy.sources.excludedSymbols) {
        pool.delete(normalizeSymbol(symbol));
    }

    return Array.from(pool)
        .filter((row) => Boolean(row))
        .sort();
}

export async function evaluateScalpSymbolCandidate(params: {
    symbol: string;
    policy: ScalpSymbolDiscoveryPolicy;
    nowMs: number;
    includeLiveQuotes: boolean;
    knownStrategyIds: Set<string>;
}): Promise<ScalpSymbolCandidateRow> {
    const symbol = normalizeSymbol(params.symbol);
    const history = await loadScalpCandleHistory(symbol, '1m');
    const candles = history.record?.candles || [];

    const fromTs = candles.length ? Number(candles[0]?.[0]) : null;
    const toTs = candles.length ? Number(candles[candles.length - 1]?.[0]) : null;
    const spanMs = fromTs !== null && toTs !== null && toTs >= fromTs ? toTs - fromTs : 0;
    const spanDays = spanMs > 0 ? spanMs / (24 * 60 * 60_000) : 0;
    const expectedBars = spanMs > 0 ? Math.max(1, Math.floor(spanMs / 60_000) + 1) : 0;
    const coveragePct = expectedBars > 0 ? clamp((candles.length / expectedBars) * 100, 0, 100) : 0;
    const avgBarsPerDay = spanDays > 0 ? candles.length / spanDays : 0;
    const recentStartTs = params.nowMs - 7 * 24 * 60 * 60_000;
    const recentBars7d = candles.filter((row) => Number(row[0]) >= recentStartTs).length;
    const medianRangePct = median(
        candles
            .slice(-Math.min(candles.length, 2000))
            .map((row) => {
                const high = Number(row[2]);
                const low = Number(row[3]);
                const close = Number(row[4]);
                if (!Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close) || close <= 0) return 0;
                return ((high - low) / close) * 100;
            })
            .filter((row) => Number.isFinite(row) && row >= 0),
    );

    let livePrice: number | null = null;
    let liveSpreadPips: number | null = null;
    if (params.includeLiveQuotes) {
        try {
            const quote = await fetchCapitalLivePrice(symbol);
            const bid = Number(quote?.bid);
            const offer = Number(quote?.offer);
            const mid = Number(quote?.price);
            if (Number.isFinite(mid) && mid > 0) livePrice = mid;
            if (Number.isFinite(bid) && Number.isFinite(offer) && offer >= bid) {
                const pipSize = pipSizeForScalpSymbol(symbol);
                if (pipSize > 0) {
                    liveSpreadPips = (offer - bid) / pipSize;
                }
                if (livePrice === null && offer > 0 && bid > 0) {
                    livePrice = (offer + bid) / 2;
                }
            }
        } catch {
            // Keep quote metrics null if Capital query fails.
        }
    }

    const reasons: string[] = [];
    if (spanDays < params.policy.criteria.minHistoryDays) reasons.push('history_days_below_min');
    if (coveragePct < params.policy.criteria.minHistoryCoveragePct) reasons.push('history_coverage_below_min');
    if (avgBarsPerDay < params.policy.criteria.minAvgBarsPerDay) reasons.push('avg_bars_per_day_below_min');
    if (recentBars7d < params.policy.criteria.minRecentBars7d) reasons.push('recent_bars_7d_below_min');
    if (medianRangePct < params.policy.criteria.minMedianRangePct) reasons.push('median_range_pct_below_min');

    if (params.policy.criteria.requireTradableQuote) {
        if (livePrice === null || livePrice <= 0) reasons.push('live_quote_missing_or_invalid');
    }
    if (params.policy.criteria.maxSpreadPips !== null && liveSpreadPips !== null && liveSpreadPips > params.policy.criteria.maxSpreadPips) {
        reasons.push('live_spread_above_max');
    }

    let score = 0;
    score += clamp((coveragePct / 100) * 25, 0, 25);
    score += clamp((spanDays / Math.max(params.policy.criteria.minHistoryDays, 1)) * 20, 0, 20);
    score += clamp((avgBarsPerDay / Math.max(params.policy.criteria.minAvgBarsPerDay, 1)) * 20, 0, 20);
    score += clamp((recentBars7d / Math.max(params.policy.criteria.minRecentBars7d, 1)) * 15, 0, 15);
    score += clamp((medianRangePct / Math.max(params.policy.criteria.minMedianRangePct, 0.0001)) * 10, 0, 10);
    if (liveSpreadPips !== null && params.policy.criteria.maxSpreadPips !== null) {
        const spreadScore = liveSpreadPips <= 0 ? 10 : clamp((params.policy.criteria.maxSpreadPips / liveSpreadPips) * 10, 0, 10);
        score += spreadScore;
    } else if (liveSpreadPips !== null) {
        score += 5;
    }

    const strategies = resolveRecommendedStrategiesForSymbol(symbol, params.policy.strategyAllowlist).filter((id) =>
        params.knownStrategyIds.has(id),
    );
    if (strategies.length === 0) {
        reasons.push('no_strategy_fit');
    }

    return {
        symbol,
        eligible: reasons.length === 0,
        score: Number(score.toFixed(4)),
        reasons,
        recommendedStrategyIds: strategies,
        metrics: {
            historyBars1m: candles.length,
            historyFromTs: fromTs,
            historyToTs: toTs,
            historySpanDays: Number(spanDays.toFixed(4)),
            historyCoveragePct: Number(coveragePct.toFixed(4)),
            avgBarsPerDay: Number(avgBarsPerDay.toFixed(4)),
            recentBars7d,
            medianRangePct: Number(medianRangePct.toFixed(6)),
            livePrice: livePrice !== null ? Number(livePrice.toFixed(10)) : null,
            liveSpreadPips: liveSpreadPips !== null ? Number(liveSpreadPips.toFixed(4)) : null,
        },
    };
}

function byScoreDesc(a: ScalpSymbolCandidateRow, b: ScalpSymbolCandidateRow): number {
    if (b.score !== a.score) return b.score - a.score;
    return a.symbol.localeCompare(b.symbol);
}

export function buildNextUniverseWithChurnCaps(params: {
    previousSymbols: string[];
    candidateRows: ScalpSymbolCandidateRow[];
    policy: ScalpSymbolDiscoveryPolicy;
    pinnedSymbols: string[];
}): { selectedSymbols: string[]; addedSymbols: string[]; removedSymbols: string[] } {
    const previous = Array.from(new Set(params.previousSymbols.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row))));
    const pinned = new Set(params.pinnedSymbols.map((row) => normalizeSymbol(row)).filter((row) => Boolean(row)));

    const eligibleSorted = params.candidateRows.filter((row) => row.eligible).slice().sort(byScoreDesc);
    const targetTop = eligibleSorted.slice(0, params.policy.limits.maxUniverseSymbols).map((row) => row.symbol);
    const targetSet = new Set(targetTop);

    if (previous.length === 0) {
        const first = Array.from(new Set([...Array.from(pinned), ...targetTop])).slice(0, params.policy.limits.maxUniverseSymbols);
        return {
            selectedSymbols: first,
            addedSymbols: first.slice(),
            removedSymbols: [],
        };
    }

    const next = new Set(previous);

    const removable = previous
        .filter((symbol) => !pinned.has(symbol))
        .filter((symbol) => !targetSet.has(symbol));
    const removeCount = Math.min(params.policy.limits.maxWeeklyRemoves, removable.length);
    for (const symbol of removable.slice(0, removeCount)) {
        next.delete(symbol);
    }

    const addable = targetTop.filter((symbol) => !next.has(symbol));
    const addCount = Math.min(params.policy.limits.maxWeeklyAdds, addable.length);
    for (const symbol of addable.slice(0, addCount)) {
        if (next.size >= params.policy.limits.maxUniverseSymbols) break;
        next.add(symbol);
    }

    for (const symbol of pinned) {
        next.add(symbol);
    }

    if (next.size < params.policy.limits.minUniverseSymbols) {
        for (const symbol of targetTop) {
            next.add(symbol);
            if (next.size >= params.policy.limits.minUniverseSymbols) break;
        }
    }

    if (next.size > params.policy.limits.maxUniverseSymbols) {
        const scoreBySymbol = new Map(params.candidateRows.map((row) => [row.symbol, row.score]));
        const sorted = Array.from(next).sort((a, b) => {
            const aPinned = pinned.has(a) ? 1 : 0;
            const bPinned = pinned.has(b) ? 1 : 0;
            if (aPinned !== bPinned) return bPinned - aPinned;
            const aScore = scoreBySymbol.get(a) ?? -1;
            const bScore = scoreBySymbol.get(b) ?? -1;
            if (bScore !== aScore) return bScore - aScore;
            return a.localeCompare(b);
        });
        next.clear();
        for (const symbol of sorted.slice(0, params.policy.limits.maxUniverseSymbols)) {
            next.add(symbol);
        }
    }

    const selectedSymbols = Array.from(next).sort();
    const previousSet = new Set(previous);
    const selectedSet = new Set(selectedSymbols);
    const addedSymbols = selectedSymbols.filter((row) => !previousSet.has(row));
    const removedSymbols = previous.filter((row) => !selectedSet.has(row));

    return {
        selectedSymbols,
        addedSymbols,
        removedSymbols,
    };
}

export async function runScalpSymbolDiscoveryCycle(
    params: ScalpSymbolDiscoveryRunParams = {},
): Promise<ScalpSymbolUniverseSnapshot> {
    const nowMs = Number.isFinite(Number(params.nowMs)) ? Math.floor(Number(params.nowMs)) : Date.now();
    const dryRun = Boolean(params.dryRun);
    const includeLiveQuotes = params.includeLiveQuotes ?? true;

    const policy = await loadScalpSymbolDiscoveryPolicy();
    const previous = await loadScalpSymbolUniverseSnapshot();

    const knownStrategies = new Set(listScalpStrategies().map((row) => row.id));
    const candidatePool = await buildCandidatePool(policy);
    const cappedPool = candidatePool.slice(0, Math.max(1, params.maxCandidatesOverride || policy.limits.maxCandidates));

    const rows: ScalpSymbolCandidateRow[] = [];
    for (const symbol of cappedPool) {
        rows.push(
            await evaluateScalpSymbolCandidate({
                symbol,
                policy,
                nowMs,
                includeLiveQuotes,
                knownStrategyIds: knownStrategies,
            }),
        );
    }

    const sorted = rows.slice().sort(byScoreDesc);
    const churn = buildNextUniverseWithChurnCaps({
        previousSymbols: previous?.selectedSymbols || [],
        candidateRows: sorted,
        policy,
        pinnedSymbols: policy.pinnedSymbols,
    });

    const selectedSet = new Set(churn.selectedSymbols);
    const selectedRows = sorted.filter((row) => selectedSet.has(row.symbol));
    const rejectedRows = sorted.filter((row) => !selectedSet.has(row.symbol));

    const snapshot: ScalpSymbolUniverseSnapshot = {
        version: 1,
        generatedAtIso: new Date(nowMs).toISOString(),
        policy,
        source: 'weekly_discovery_v1',
        dryRun,
        previousSymbols: previous?.selectedSymbols || [],
        selectedSymbols: churn.selectedSymbols,
        addedSymbols: churn.addedSymbols,
        removedSymbols: churn.removedSymbols,
        candidatesEvaluated: sorted.length,
        selectedRows,
        topRejectedRows: rejectedRows.slice(0, Math.max(10, policy.limits.maxUniverseSymbols)),
    };

    if (!dryRun) {
        await saveScalpSymbolUniverseSnapshot(snapshot);
    }

    return snapshot;
}
