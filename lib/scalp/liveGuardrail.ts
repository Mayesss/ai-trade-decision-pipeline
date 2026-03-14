import { getScalpStrategyConfig } from './config';
import {
    listScalpDeploymentRegistryEntries,
    upsertScalpDeploymentRegistryEntry,
    type ScalpDeploymentRegistryEntry,
} from './deploymentRegistry';
import { refreshScalpResearchPortfolioReport, type ScalpResearchReportDeploymentRow } from './researchReporting';
import { appendScalpJournal } from './store';
import type { ScalpJournalEntry } from './types';

function toFinite(value: unknown, fallback = 0): number {
    const n = Number(value);
    if (!Number.isFinite(n)) return fallback;
    return n;
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

function toPositiveInt(value: unknown, fallback: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return fallback;
    return n;
}

function toNullableFinite(value: unknown): number | null {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    return n;
}

function normalizeText(value: unknown, maxLen = 400): string {
    return String(value || '')
        .trim()
        .slice(0, maxLen);
}

export interface ScalpLiveGuardrailThresholds {
    minTrades30d: number;
    minExpectancyR30d: number;
    maxDrawdownR30d: number | null;
    maxExpectancyDriftFromForward: number | null;
    maxTradesPerDay30d: number | null;
    minForwardProfitableWindowPct: number | null;
    autoPause: boolean;
}

export function resolveScalpLiveGuardrailThresholds(overrides: Partial<ScalpLiveGuardrailThresholds> = {}): ScalpLiveGuardrailThresholds {
    const thresholds: ScalpLiveGuardrailThresholds = {
        minTrades30d: toPositiveInt(process.env.SCALP_GUARDRAIL_MIN_TRADES_30D, 8),
        minExpectancyR30d: toFinite(process.env.SCALP_GUARDRAIL_MIN_EXPECTANCY_R_30D, -0.15),
        maxDrawdownR30d: toNullableFinite(process.env.SCALP_GUARDRAIL_MAX_DRAWDOWN_R_30D),
        maxExpectancyDriftFromForward: toNullableFinite(process.env.SCALP_GUARDRAIL_MAX_EXPECTANCY_DRIFT_R_30D),
        maxTradesPerDay30d: toNullableFinite(process.env.SCALP_GUARDRAIL_MAX_TRADES_PER_DAY_30D),
        minForwardProfitableWindowPct: toNullableFinite(process.env.SCALP_GUARDRAIL_MIN_FORWARD_PROFITABLE_PCT),
        autoPause: toBool(process.env.SCALP_GUARDRAIL_AUTO_PAUSE, true),
    };
    return {
        ...thresholds,
        ...overrides,
    };
}

export interface ScalpLiveGuardrailBreach {
    code: string;
    severity: 'soft' | 'hard';
    message: string;
}

export interface ScalpLiveGuardrailWarmup {
    code: string;
    message: string;
}

export interface ScalpLiveGuardrailEvaluation {
    warmups: ScalpLiveGuardrailWarmup[];
    breaches: ScalpLiveGuardrailBreach[];
    warmupCount: number;
    hardBreachCount: number;
    softBreachCount: number;
}

export function evaluateScalpDeploymentGuardrail(
    row: Pick<ScalpResearchReportDeploymentRow, 'deploymentId' | 'perf30d' | 'forwardValidation'>,
    thresholds: ScalpLiveGuardrailThresholds,
): ScalpLiveGuardrailEvaluation {
    const warmups: ScalpLiveGuardrailWarmup[] = [];
    const breaches: ScalpLiveGuardrailBreach[] = [];
    const trades30d = Math.max(0, Math.floor(toFinite(row.perf30d.trades, 0)));
    const expectancy30d = toFinite(row.perf30d.expectancyR, 0);
    const maxDrawdown30d = Math.max(0, toFinite(row.perf30d.maxDrawdownR, 0));
    const tradesPerDay30d = trades30d / 30;

    if (trades30d < thresholds.minTrades30d) {
        warmups.push({
            code: 'GUARDRAIL_LOW_SAMPLE_30D',
            message: `30D trades ${trades30d} < min ${thresholds.minTrades30d}`,
        });
    } else {
        if (expectancy30d < thresholds.minExpectancyR30d) {
            breaches.push({
                code: 'GUARDRAIL_EXPECTANCY_BELOW_FLOOR_30D',
                severity: 'hard',
                message: `30D expectancy ${expectancy30d.toFixed(4)} < floor ${thresholds.minExpectancyR30d.toFixed(4)}`,
            });
        }
        if (thresholds.maxDrawdownR30d !== null && maxDrawdown30d > thresholds.maxDrawdownR30d) {
            breaches.push({
                code: 'GUARDRAIL_DRAWDOWN_ABOVE_CAP_30D',
                severity: 'hard',
                message: `30D maxDD ${maxDrawdown30d.toFixed(4)} > cap ${thresholds.maxDrawdownR30d.toFixed(4)}`,
            });
        }
        if (thresholds.maxTradesPerDay30d !== null && tradesPerDay30d > thresholds.maxTradesPerDay30d) {
            breaches.push({
                code: 'GUARDRAIL_CHURN_TRADES_PER_DAY_ABOVE_CAP_30D',
                severity: 'hard',
                message: `30D trades/day ${tradesPerDay30d.toFixed(4)} > cap ${thresholds.maxTradesPerDay30d.toFixed(4)}`,
            });
        }
    }

    const forward = row.forwardValidation;
    if (forward) {
        if (
            thresholds.maxExpectancyDriftFromForward !== null &&
            expectancy30d < forward.meanExpectancyR - thresholds.maxExpectancyDriftFromForward
        ) {
            breaches.push({
                code: 'GUARDRAIL_EXPECTANCY_DRIFT_BELOW_FORWARD_BAND',
                severity: 'hard',
                message: `30D expectancy ${expectancy30d.toFixed(4)} < forward ${forward.meanExpectancyR.toFixed(4)} - ${thresholds.maxExpectancyDriftFromForward.toFixed(4)}`,
            });
        }
        if (
            thresholds.minForwardProfitableWindowPct !== null &&
            toFinite(forward.profitableWindowPct, 0) < thresholds.minForwardProfitableWindowPct
        ) {
            breaches.push({
                code: 'GUARDRAIL_FORWARD_PROFITABLE_PCT_BELOW_MIN',
                severity: 'soft',
                message: `Forward profitable % ${toFinite(forward.profitableWindowPct, 0).toFixed(2)} < min ${thresholds.minForwardProfitableWindowPct.toFixed(2)}`,
            });
        }
    }

    const hardBreachCount = breaches.filter((row) => row.severity === 'hard').length;
    const softBreachCount = breaches.length - hardBreachCount;
    return {
        warmups,
        breaches,
        warmupCount: warmups.length,
        hardBreachCount,
        softBreachCount,
    };
}

export interface ScalpLiveGuardrailMonitorRow {
    deploymentId: string;
    symbol: string;
    strategyId: string;
    tuneId: string;
    enabled: boolean;
    warmups: ScalpLiveGuardrailWarmup[];
    breaches: ScalpLiveGuardrailBreach[];
    warmupCount: number;
    hardBreachCount: number;
    softBreachCount: number;
    action: 'none' | 'pause';
    paused: boolean;
}

export interface RunScalpLiveGuardrailMonitorParams {
    dryRun?: boolean;
    nowMs?: number;
    tradeLimit?: number;
    monthlyMonths?: number;
    autoPause?: boolean;
}

export interface RunScalpLiveGuardrailMonitorResult {
    ok: boolean;
    dryRun: boolean;
    generatedAtMs: number;
    generatedAtIso: string;
    thresholds: ScalpLiveGuardrailThresholds;
    summary: {
        enabledDeployments: number;
        checkedDeployments: number;
        warmupDeployments: number;
        breachedDeployments: number;
        hardBreachedDeployments: number;
        pausedDeployments: number;
    };
    rows: ScalpLiveGuardrailMonitorRow[];
}

function appendGuardrailNote(existing: string | null, nowIso: string, breaches: ScalpLiveGuardrailBreach[]): string {
    const marker = `[guardrail ${nowIso}] ${breaches.map((row) => row.code).join(',')}`;
    const base = normalizeText(existing, 330);
    return base ? `${base} | ${marker}`.slice(0, 400) : marker.slice(0, 400);
}

function buildGuardrailJournalEntry(params: {
    nowMs: number;
    symbol: string;
    deploymentId: string;
    strategyId: string;
    tuneId: string;
    warmups: ScalpLiveGuardrailWarmup[];
    breaches: ScalpLiveGuardrailBreach[];
    action: 'none' | 'pause';
    dryRun: boolean;
    thresholds: ScalpLiveGuardrailThresholds;
    metrics: Pick<ScalpResearchReportDeploymentRow, 'perf30d' | 'forwardValidation'>;
}): ScalpJournalEntry {
    const hasBreaches = params.breaches.length > 0;
    const level: ScalpJournalEntry['level'] = hasBreaches
        ? params.action === 'pause'
            ? 'error'
            : 'warn'
        : 'info';
    const type: ScalpJournalEntry['type'] = hasBreaches ? 'risk' : 'state';
    const categoryCode = hasBreaches ? 'SCALP_GUARDRAIL_BREACH' : 'SCALP_GUARDRAIL_WARMUP';
    return {
        id: `guardrail:${params.nowMs}:${params.deploymentId}`,
        timestampMs: params.nowMs,
        type,
        symbol: params.symbol,
        dayKey: null,
        level,
        reasonCodes: [categoryCode]
            .concat(params.warmups.map((row) => row.code))
            .concat(params.breaches.map((row) => row.code))
            .slice(0, 12),
        payload: {
            deploymentId: params.deploymentId,
            strategyId: params.strategyId,
            tuneId: params.tuneId,
            action: params.action,
            dryRun: params.dryRun,
            warmups: params.warmups,
            breaches: params.breaches,
            thresholds: params.thresholds,
            perf30d: params.metrics.perf30d,
            forwardValidation: params.metrics.forwardValidation,
        },
    };
}

export async function runScalpLiveGuardrailMonitor(
    params: RunScalpLiveGuardrailMonitorParams = {},
): Promise<RunScalpLiveGuardrailMonitorResult> {
    const nowMs = Number.isFinite(Number(params.nowMs)) ? Math.floor(Number(params.nowMs)) : Date.now();
    const dryRun = Boolean(params.dryRun);
    const thresholds = resolveScalpLiveGuardrailThresholds({
        autoPause: params.autoPause ?? resolveScalpLiveGuardrailThresholds().autoPause,
    });

    const [report, registryRows] = await Promise.all([
        refreshScalpResearchPortfolioReport({
            nowMs,
            tradeLimit: params.tradeLimit,
            monthlyMonths: params.monthlyMonths,
            persist: false,
        }),
        listScalpDeploymentRegistryEntries({}),
    ]);

    const registryByDeploymentId = new Map<string, ScalpDeploymentRegistryEntry>(
        registryRows.map((row) => [row.deploymentId, row]),
    );

    const rows: ScalpLiveGuardrailMonitorRow[] = [];
    let checkedDeployments = 0;
    let warmupDeployments = 0;
    let breachedDeployments = 0;
    let hardBreachedDeployments = 0;
    let pausedDeployments = 0;

    const cfg = getScalpStrategyConfig();
    const nowIso = new Date(nowMs).toISOString();

    for (const row of report.deployments) {
        if (!row.enabled) continue;
        checkedDeployments += 1;

        const evaluation = evaluateScalpDeploymentGuardrail(row, thresholds);
        const hasWarmups = evaluation.warmups.length > 0;
        const hasBreaches = evaluation.breaches.length > 0;
        const hasHardBreaches = evaluation.hardBreachCount > 0;

        if (hasWarmups) warmupDeployments += 1;
        if (hasBreaches) breachedDeployments += 1;
        if (hasHardBreaches) hardBreachedDeployments += 1;

        const shouldPause = thresholds.autoPause && hasHardBreaches;
        let paused = false;

        if (hasWarmups || hasBreaches) {
            const journalEntry = buildGuardrailJournalEntry({
                nowMs,
                symbol: row.symbol,
                deploymentId: row.deploymentId,
                strategyId: row.strategyId,
                tuneId: row.tuneId,
                warmups: evaluation.warmups,
                breaches: evaluation.breaches,
                action: shouldPause ? 'pause' : 'none',
                dryRun,
                thresholds,
                metrics: {
                    perf30d: row.perf30d,
                    forwardValidation: row.forwardValidation,
                },
            });
            if (!dryRun) {
                await appendScalpJournal(journalEntry, cfg.storage.journalMax);
            }
        }

        if (shouldPause) {
            const existing = registryByDeploymentId.get(row.deploymentId);
            if (existing && !dryRun) {
                await upsertScalpDeploymentRegistryEntry({
                    deploymentId: existing.deploymentId,
                    source: existing.source,
                    enabled: false,
                    notes: appendGuardrailNote(existing.notes, nowIso, evaluation.breaches),
                    updatedBy: 'cron:live-guardrail-monitor',
                });
                paused = true;
                pausedDeployments += 1;
            } else if (dryRun) {
                paused = true;
                pausedDeployments += 1;
            }
        }

        rows.push({
            deploymentId: row.deploymentId,
            symbol: row.symbol,
            strategyId: row.strategyId,
            tuneId: row.tuneId,
            enabled: row.enabled,
            warmups: evaluation.warmups,
            breaches: evaluation.breaches,
            warmupCount: evaluation.warmupCount,
            hardBreachCount: evaluation.hardBreachCount,
            softBreachCount: evaluation.softBreachCount,
            action: shouldPause ? 'pause' : 'none',
            paused,
        });
    }

    rows.sort((a, b) => {
        if (b.hardBreachCount !== a.hardBreachCount) return b.hardBreachCount - a.hardBreachCount;
        if (b.softBreachCount !== a.softBreachCount) return b.softBreachCount - a.softBreachCount;
        if (b.warmupCount !== a.warmupCount) return b.warmupCount - a.warmupCount;
        if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
        return a.strategyId.localeCompare(b.strategyId);
    });

    return {
        ok: true,
        dryRun,
        generatedAtMs: nowMs,
        generatedAtIso: nowIso,
        thresholds,
        summary: {
            enabledDeployments: report.deployments.filter((row) => row.enabled).length,
            checkedDeployments,
            warmupDeployments,
            breachedDeployments,
            hardBreachedDeployments,
            pausedDeployments,
        },
        rows,
    };
}
