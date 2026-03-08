import { mkdir, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';

import { kvGetJson, kvSetJson } from '../kv';
import {
    listScalpDeploymentRegistryEntries,
    type ScalpDeploymentRegistryEntry,
    type ScalpForwardValidationMetrics,
} from './deploymentRegistry';
import {
    loadActiveResearchCycleId,
    loadLatestCompletedResearchCycleId,
    loadResearchCycle,
    loadResearchCycleSummary,
    type ScalpResearchCycleStatus,
} from './researchCycle';
import { loadScalpTradeLedger } from './store';
import type { ScalpTradeLedgerEntry } from './types';

const REPORT_KV_KEY = 'scalp:research:portfolio-report:v1';
const DEFAULT_REPORT_FILE_PATH = path.resolve(process.cwd(), 'data/scalp-research-report.json');

export interface ScalpTradeWindowPerformance {
    trades: number;
    wins: number;
    losses: number;
    netR: number;
    expectancyR: number;
    winRatePct: number;
    maxDrawdownR: number;
    lastTradeAtMs: number | null;
}

export interface ScalpMonthlyDeploymentPerformance {
    month: string;
    trades: number;
    netR: number;
    expectancyR: number;
}

export interface ScalpResearchReportDeploymentRow {
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    source: string;
    enabled: boolean;
    promotionEligible: boolean;
    promotionReason: string | null;
    forwardValidation: ScalpForwardValidationMetrics | null;
    perf30d: ScalpTradeWindowPerformance;
    perf90d: ScalpTradeWindowPerformance;
    monthly: ScalpMonthlyDeploymentPerformance[];
}

export interface ScalpResearchReportCorrelationRow {
    deploymentIdA: string;
    symbolA: string;
    strategyIdA: string;
    deploymentIdB: string;
    symbolB: string;
    strategyIdB: string;
    monthsOverlap: number;
    correlation: number;
}

export interface ScalpResearchPortfolioReportSnapshot {
    version: 1;
    generatedAtMs: number;
    generatedAtIso: string;
    params: {
        tradeLimit: number;
        monthlyMonths: number;
        cycleId: string | null;
    };
    cycle: {
        cycleId: string | null;
        status: ScalpResearchCycleStatus | null;
        progressPct: number | null;
        tasks: number | null;
        completed: number | null;
        failed: number | null;
        candidateCount: number | null;
    };
    summary: {
        deploymentsTotal: number;
        deploymentsEnabled: number;
        enabledPromotionEligible: number;
        enabledPromotionIneligible: number;
        enabledWithoutGate: number;
        enabledSymbols: number;
        symbolConcentration: Array<{ symbol: string; deployments: number }>;
        recent30d: ScalpTradeWindowPerformance;
        recent90d: ScalpTradeWindowPerformance;
        avgAbsPairCorrelation: number | null;
        pairCorrelationCount: number;
    };
    monthlyPortfolio: ScalpMonthlyDeploymentPerformance[];
    deployments: ScalpResearchReportDeploymentRow[];
    correlationPairs: ScalpResearchReportCorrelationRow[];
}

export interface BuildScalpResearchReportParams {
    nowMs?: number;
    tradeLimit?: number;
    monthlyMonths?: number;
    cycleId?: string;
    persist?: boolean;
}

function toFinite(value: unknown, fallback = 0): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return n;
}

function resolveReportStoreMode(): 'kv' | 'file' {
    const configured = String(process.env.SCALP_RESEARCH_REPORT_STORE || 'auto')
        .trim()
        .toLowerCase();
    if (configured === 'kv') return 'kv';
    if (configured === 'file') return 'file';
    return process.env.KV_REST_API_URL && process.env.KV_REST_API_TOKEN ? 'kv' : 'file';
}

function resolveReportFilePath(): string {
    const configured = String(process.env.SCALP_RESEARCH_REPORT_PATH || '').trim();
    if (!configured) return DEFAULT_REPORT_FILE_PATH;
    return path.isAbsolute(configured) ? configured : path.resolve(process.cwd(), configured);
}

function monthKeyUtc(ts: number): string {
    const d = new Date(ts);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    return `${y}-${m}`;
}

function monthStartUtc(year: number, monthZeroBased: number): number {
    return Date.UTC(year, monthZeroBased, 1, 0, 0, 0, 0);
}

export function buildRecentMonthKeys(nowMs: number, months: number): string[] {
    const d = new Date(nowMs);
    let year = d.getUTCFullYear();
    let month = d.getUTCMonth();
    const out: string[] = [];
    const count = Math.max(1, Math.min(36, Math.floor(months)));

    for (let i = 0; i < count; i += 1) {
        out.push(`${year}-${String(month + 1).padStart(2, '0')}`);
        month -= 1;
        if (month < 0) {
            month = 11;
            year -= 1;
        }
    }

    return out.reverse();
}

export function computeTradeWindowPerformance(
    trades: ScalpTradeLedgerEntry[],
    startMs: number,
    endMs: number,
): ScalpTradeWindowPerformance {
    const scoped = trades
        .filter((row) => !row.dryRun)
        .filter((row) => Number.isFinite(Number(row.exitAtMs)))
        .filter((row) => Number(row.exitAtMs) >= startMs && Number(row.exitAtMs) <= endMs)
        .slice()
        .sort((a, b) => Number(a.exitAtMs) - Number(b.exitAtMs));

    let wins = 0;
    let losses = 0;
    let netR = 0;
    let equityR = 0;
    let peakR = 0;
    let maxDrawdownR = 0;
    let lastTradeAtMs: number | null = null;

    for (const trade of scoped) {
        const r = toFinite(trade.rMultiple, 0);
        if (r > 0) wins += 1;
        else if (r < 0) losses += 1;
        netR += r;
        equityR += r;
        peakR = Math.max(peakR, equityR);
        maxDrawdownR = Math.max(maxDrawdownR, peakR - equityR);
        const ts = Number(trade.exitAtMs);
        if (Number.isFinite(ts)) {
            lastTradeAtMs = lastTradeAtMs === null ? ts : Math.max(lastTradeAtMs, ts);
        }
    }

    const tradesCount = scoped.length;
    return {
        trades: tradesCount,
        wins,
        losses,
        netR,
        expectancyR: tradesCount > 0 ? netR / tradesCount : 0,
        winRatePct: tradesCount > 0 ? (wins / tradesCount) * 100 : 0,
        maxDrawdownR,
        lastTradeAtMs,
    };
}

function monthlyFromTrades(
    trades: ScalpTradeLedgerEntry[],
    monthKeys: string[],
): ScalpMonthlyDeploymentPerformance[] {
    const monthSet = new Set(monthKeys);
    const buckets = new Map<string, { trades: number; netR: number }>();

    for (const trade of trades) {
        if (trade.dryRun) continue;
        const ts = Number(trade.exitAtMs);
        if (!Number.isFinite(ts)) continue;
        const mk = monthKeyUtc(ts);
        if (!monthSet.has(mk)) continue;
        const existing = buckets.get(mk) || { trades: 0, netR: 0 };
        existing.trades += 1;
        existing.netR += toFinite(trade.rMultiple, 0);
        buckets.set(mk, existing);
    }

    return monthKeys.map((month) => {
        const row = buckets.get(month) || { trades: 0, netR: 0 };
        return {
            month,
            trades: row.trades,
            netR: row.netR,
            expectancyR: row.trades > 0 ? row.netR / row.trades : 0,
        };
    });
}

export function pearsonCorrelation(xs: number[], ys: number[]): number | null {
    if (xs.length !== ys.length || xs.length < 2) return null;
    const n = xs.length;
    const meanX = xs.reduce((acc, row) => acc + row, 0) / n;
    const meanY = ys.reduce((acc, row) => acc + row, 0) / n;

    let num = 0;
    let denX = 0;
    let denY = 0;

    for (let i = 0; i < n; i += 1) {
        const dx = xs[i]! - meanX;
        const dy = ys[i]! - meanY;
        num += dx * dy;
        denX += dx * dx;
        denY += dy * dy;
    }

    if (denX <= 0 || denY <= 0) return null;
    return num / Math.sqrt(denX * denY);
}

function correlationRowsForDeployments(
    rows: ScalpResearchReportDeploymentRow[],
    monthKeys: string[],
): ScalpResearchReportCorrelationRow[] {
    const out: ScalpResearchReportCorrelationRow[] = [];

    for (let i = 0; i < rows.length; i += 1) {
        const a = rows[i]!;
        for (let j = i + 1; j < rows.length; j += 1) {
            const b = rows[j]!;
            const valuesA: number[] = [];
            const valuesB: number[] = [];
            for (const mk of monthKeys) {
                const ma = a.monthly.find((row) => row.month === mk);
                const mb = b.monthly.find((row) => row.month === mk);
                if (!ma || !mb) continue;
                if (ma.trades <= 0 || mb.trades <= 0) continue;
                valuesA.push(ma.netR);
                valuesB.push(mb.netR);
            }
            const corr = pearsonCorrelation(valuesA, valuesB);
            if (corr === null) continue;
            out.push({
                deploymentIdA: a.deploymentId,
                symbolA: a.symbol,
                strategyIdA: a.strategyId,
                deploymentIdB: b.deploymentId,
                symbolB: b.symbol,
                strategyIdB: b.strategyId,
                monthsOverlap: valuesA.length,
                correlation: corr,
            });
        }
    }

    return out.sort((a, b) => {
        const absA = Math.abs(a.correlation);
        const absB = Math.abs(b.correlation);
        if (absB !== absA) return absB - absA;
        if (a.symbolA !== b.symbolA) return a.symbolA.localeCompare(b.symbolA);
        return a.symbolB.localeCompare(b.symbolB);
    });
}

function scopedTradesByDeployment(
    trades: ScalpTradeLedgerEntry[],
): Map<string, ScalpTradeLedgerEntry[]> {
    const map = new Map<string, ScalpTradeLedgerEntry[]>();
    for (const trade of trades) {
        const deploymentId = String(trade.deploymentId || '').trim();
        if (!deploymentId) continue;
        const bucket = map.get(deploymentId) || [];
        bucket.push(trade);
        map.set(deploymentId, bucket);
    }
    for (const [deploymentId, rows] of map.entries()) {
        map.set(
            deploymentId,
            rows.slice().sort((a, b) => Number(a.exitAtMs) - Number(b.exitAtMs)),
        );
    }
    return map;
}

function toDeploymentReportRow(
    entry: ScalpDeploymentRegistryEntry,
    trades: ScalpTradeLedgerEntry[],
    nowMs: number,
    monthKeys: string[],
): ScalpResearchReportDeploymentRow {
    const perf30d = computeTradeWindowPerformance(trades, nowMs - 30 * 24 * 60 * 60_000, nowMs);
    const perf90d = computeTradeWindowPerformance(trades, nowMs - 90 * 24 * 60 * 60_000, nowMs);
    return {
        deploymentId: entry.deploymentId,
        symbol: entry.symbol,
        strategyId: entry.strategyId,
        tuneId: entry.tuneId,
        source: entry.source,
        enabled: entry.enabled,
        promotionEligible: Boolean(entry.promotionGate?.eligible),
        promotionReason: entry.promotionGate?.reason || null,
        forwardValidation: entry.promotionGate?.forwardValidation || null,
        perf30d,
        perf90d,
        monthly: monthlyFromTrades(trades, monthKeys),
    };
}

function summarizeSymbolConcentration(rows: ScalpResearchReportDeploymentRow[]): Array<{ symbol: string; deployments: number }> {
    const buckets = new Map<string, number>();
    for (const row of rows) {
        if (!row.enabled) continue;
        buckets.set(row.symbol, (buckets.get(row.symbol) || 0) + 1);
    }
    return Array.from(buckets.entries())
        .map(([symbol, deployments]) => ({ symbol, deployments }))
        .sort((a, b) => {
            if (b.deployments !== a.deployments) return b.deployments - a.deployments;
            return a.symbol.localeCompare(b.symbol);
        });
}

async function buildCycleContext(cycleId: string | null): Promise<ScalpResearchPortfolioReportSnapshot['cycle']> {
    if (!cycleId) {
        return {
            cycleId: null,
            status: null,
            progressPct: null,
            tasks: null,
            completed: null,
            failed: null,
            candidateCount: null,
        };
    }
    const cycle = await loadResearchCycle(cycleId);
    if (!cycle) {
        return {
            cycleId,
            status: null,
            progressPct: null,
            tasks: null,
            completed: null,
            failed: null,
            candidateCount: null,
        };
    }

    const summary = cycle.latestSummary || (await loadResearchCycleSummary(cycle.cycleId));
    return {
        cycleId,
        status: cycle.status,
        progressPct: summary?.progressPct ?? null,
        tasks: summary?.totals.tasks ?? cycle.taskIds.length,
        completed: summary?.totals.completed ?? null,
        failed: summary?.totals.failed ?? null,
        candidateCount: summary?.candidateAggregates.length ?? null,
    };
}

export async function buildScalpResearchPortfolioReport(
    params: BuildScalpResearchReportParams = {},
): Promise<ScalpResearchPortfolioReportSnapshot> {
    const nowMs = Number.isFinite(Number(params.nowMs)) ? Math.floor(Number(params.nowMs)) : Date.now();
    const tradeLimit = Math.max(500, Math.min(50_000, Math.floor(Number(params.tradeLimit) || 20_000)));
    const monthlyMonths = Math.max(3, Math.min(24, Math.floor(Number(params.monthlyMonths) || 12)));
    const cycleId =
        String(params.cycleId || '').trim() ||
        (await loadActiveResearchCycleId()) ||
        (await loadLatestCompletedResearchCycleId());

    const [deployments, tradeLedger] = await Promise.all([
        listScalpDeploymentRegistryEntries({}),
        loadScalpTradeLedger(tradeLimit),
    ]);

    const monthKeys = buildRecentMonthKeys(nowMs, monthlyMonths);
    const tradesByDeployment = scopedTradesByDeployment(tradeLedger);

    const deploymentRows = deployments
        .map((entry) => toDeploymentReportRow(entry, tradesByDeployment.get(entry.deploymentId) || [], nowMs, monthKeys))
        .sort((a, b) => {
            if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
            if (a.strategyId !== b.strategyId) return a.strategyId.localeCompare(b.strategyId);
            return a.tuneId.localeCompare(b.tuneId);
        });

    const enabledRows = deploymentRows.filter((row) => row.enabled);
    const eligibleEnabled = enabledRows.filter((row) => row.promotionEligible).length;
    const ineligibleEnabled = enabledRows.filter((row) => !row.promotionEligible && row.forwardValidation !== null).length;
    const withoutGateEnabled = enabledRows.filter((row) => row.forwardValidation === null).length;

    const enabledPairs = correlationRowsForDeployments(enabledRows, monthKeys);
    const avgAbsPairCorrelation =
        enabledPairs.length > 0
            ? enabledPairs.reduce((acc, row) => acc + Math.abs(row.correlation), 0) / enabledPairs.length
            : null;

    const start30d = nowMs - 30 * 24 * 60 * 60_000;
    const start90d = nowMs - 90 * 24 * 60 * 60_000;
    const portfolioTrades = tradeLedger.filter((row) => !row.dryRun);

    const monthPortfolio = monthKeys.map((month) => {
        const rows = portfolioTrades.filter((trade) => monthKeyUtc(Number(trade.exitAtMs)) === month);
        const trades = rows.length;
        const netR = rows.reduce((acc, trade) => acc + toFinite(trade.rMultiple, 0), 0);
        return {
            month,
            trades,
            netR,
            expectancyR: trades > 0 ? netR / trades : 0,
        };
    });

    const cycleContext = await buildCycleContext(cycleId || null);

    return {
        version: 1,
        generatedAtMs: nowMs,
        generatedAtIso: new Date(nowMs).toISOString(),
        params: {
            tradeLimit,
            monthlyMonths,
            cycleId: cycleId || null,
        },
        cycle: cycleContext,
        summary: {
            deploymentsTotal: deploymentRows.length,
            deploymentsEnabled: enabledRows.length,
            enabledPromotionEligible: eligibleEnabled,
            enabledPromotionIneligible: ineligibleEnabled,
            enabledWithoutGate: withoutGateEnabled,
            enabledSymbols: new Set(enabledRows.map((row) => row.symbol)).size,
            symbolConcentration: summarizeSymbolConcentration(deploymentRows),
            recent30d: computeTradeWindowPerformance(portfolioTrades, start30d, nowMs),
            recent90d: computeTradeWindowPerformance(portfolioTrades, start90d, nowMs),
            avgAbsPairCorrelation,
            pairCorrelationCount: enabledPairs.length,
        },
        monthlyPortfolio: monthPortfolio,
        deployments: deploymentRows,
        correlationPairs: enabledPairs.slice(0, 30),
    };
}

export async function loadScalpResearchPortfolioReportSnapshot(): Promise<ScalpResearchPortfolioReportSnapshot | null> {
    const mode = resolveReportStoreMode();
    if (mode === 'kv') {
        return kvGetJson<ScalpResearchPortfolioReportSnapshot>(REPORT_KV_KEY);
    }
    try {
        const raw = await readFile(resolveReportFilePath(), 'utf8');
        return JSON.parse(raw) as ScalpResearchPortfolioReportSnapshot;
    } catch {
        return null;
    }
}

export async function saveScalpResearchPortfolioReportSnapshot(snapshot: ScalpResearchPortfolioReportSnapshot): Promise<void> {
    const mode = resolveReportStoreMode();
    if (mode === 'kv') {
        await kvSetJson(REPORT_KV_KEY, snapshot);
        return;
    }
    const filePath = resolveReportFilePath();
    await mkdir(path.dirname(filePath), { recursive: true });
    await writeFile(filePath, `${JSON.stringify(snapshot, null, 2)}\n`, 'utf8');
}

export async function refreshScalpResearchPortfolioReport(
    params: BuildScalpResearchReportParams = {},
): Promise<ScalpResearchPortfolioReportSnapshot> {
    const snapshot = await buildScalpResearchPortfolioReport(params);
    if (params.persist !== false) {
        await saveScalpResearchPortfolioReportSnapshot(snapshot);
    }
    return snapshot;
}

export function monthKeyWindowBounds(month: string): { fromTs: number; toTs: number } | null {
    const match = String(month || '').match(/^(\d{4})-(\d{2})$/);
    if (!match) return null;
    const year = Number(match[1]);
    const monthNum = Number(match[2]);
    if (!Number.isFinite(year) || !Number.isFinite(monthNum) || monthNum < 1 || monthNum > 12) return null;
    const fromTs = monthStartUtc(year, monthNum - 1);
    const toTs = monthNum === 12 ? monthStartUtc(year + 1, 0) - 1 : monthStartUtc(year, monthNum) - 1;
    return { fromTs, toTs };
}
