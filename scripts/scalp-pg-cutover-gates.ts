import { readFile } from 'node:fs/promises';

import { Prisma } from '@prisma/client';

import {
    loadScalpDeploymentRegistry,
    type ScalpDeploymentPromotionGateThresholds,
    type ScalpForwardValidationMetrics,
} from '../lib/scalp/deploymentRegistry';
import { resolveScalpBackend } from '../lib/scalp/backend';
import { isScalpPgConfigured, scalpPrisma } from '../lib/scalp/pg/client';
import { syncResearchCyclePromotionGates, type SyncResearchPromotionResult } from '../lib/scalp/researchPromotion';

type GateStatus = 'pass' | 'fail' | 'unknown';

type GateResult = {
    id: string;
    title: string;
    status: GateStatus;
    pass: boolean | null;
    details: Record<string, unknown>;
};

type CutoverGateOptions = {
    execWindowHours: number;
    lagWindowHours: number;
    cooldownWindowHours: number;
    cooldownFailureSinceIso: string | null;
    maxResearchQueueLagMs: number;
    requiredConsecutiveCycles: number;
    sampleLimit: number;
    strict: boolean;
    failOnUnknown: boolean;
};

const DEFAULT_FORWARD_GATE_THRESHOLDS: ScalpDeploymentPromotionGateThresholds = {
    minRollCount: 6,
    minProfitableWindowPct: 55,
    minMeanExpectancyR: 0,
    minTradesPerWindow: 2,
    maxDrawdownR: null,
    minWeeklySlices: null,
    minWeeklyProfitablePct: null,
    minWeeklyMedianExpectancyR: null,
    maxWeeklyTopWeekPnlConcentrationPct: null,
};

function parseCronPath(pathRaw: unknown): { pathname: string; query: URLSearchParams } | null {
    const raw = String(pathRaw || '').trim();
    if (!raw) return null;
    try {
        const parsed = new URL(raw, 'http://localhost');
        return {
            pathname: String(parsed.pathname || '').trim(),
            query: parsed.searchParams,
        };
    } catch {
        return null;
    }
}

function parseBool(value: unknown, fallback: boolean): boolean {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (!normalized) return fallback;
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
}

function parsePositiveInt(value: unknown, fallback: number, min: number, max: number): number {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n < min) return fallback;
    return Math.max(min, Math.min(max, n));
}

function parsePositiveNumber(value: unknown, fallback: number, min: number, max: number): number {
    const n = Number(value);
    if (!Number.isFinite(n) || n < min) return fallback;
    return Math.max(min, Math.min(max, n));
}

function parseIsoDate(value: unknown): Date | null {
    const raw = String(value || '').trim();
    if (!raw) return null;
    const dt = new Date(raw);
    if (!Number.isFinite(dt.getTime())) return null;
    return dt;
}

function parseArgs(argv: string[]): CutoverGateOptions {
    const get = (name: string): string | null => {
        const hit = argv.find((arg) => arg.startsWith(`${name}=`));
        return hit ? hit.slice(name.length + 1) : null;
    };
    const cooldownFailureSinceArg = get('--cooldownFailureSinceIso');
    const cooldownFailureSinceEnv = String(process.env.SCALP_CUTOVER_GATE3_COOLDOWN_SINCE_ISO || '').trim();
    const cooldownFailureSinceIso = toIso(parseIsoDate(cooldownFailureSinceArg || cooldownFailureSinceEnv));

    return {
        execWindowHours: parsePositiveInt(get('--execWindowHours'), 48, 1, 24 * 30),
        lagWindowHours: parsePositiveInt(get('--lagWindowHours'), 24, 1, 24 * 30),
        cooldownWindowHours: parsePositiveInt(get('--cooldownWindowHours'), 48, 1, 24 * 30),
        cooldownFailureSinceIso,
        maxResearchQueueLagMs: parsePositiveNumber(get('--maxResearchQueueLagMs'), 120_000, 1_000, 60 * 60_000),
        requiredConsecutiveCycles: parsePositiveInt(get('--requiredConsecutiveCycles'), 2, 2, 10),
        sampleLimit: parsePositiveInt(get('--sampleLimit'), 20, 1, 200),
        strict: parseBool(get('--strict'), false),
        failOnUnknown: parseBool(get('--failOnUnknown'), false),
    };
}

function toNumber(value: unknown): number {
    if (typeof value === 'bigint') return Number(value);
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
}

function normalizeFiniteNumber(value: unknown): number | null {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function toIso(value: unknown): string | null {
    if (value instanceof Date && Number.isFinite(value.getTime())) return value.toISOString();
    const raw = String(value || '').trim();
    if (!raw) return null;
    const dt = new Date(raw);
    if (!Number.isFinite(dt.getTime())) return null;
    return dt.toISOString();
}

function keyOf(symbol: string, strategyId: string, tuneId: string): string {
    return `${String(symbol || '').trim().toUpperCase()}::${String(strategyId || '').trim().toLowerCase()}::${String(tuneId || '').trim().toLowerCase()}`;
}

function setDiff(source: Set<string>, remove: Set<string>): string[] {
    const out: string[] = [];
    for (const item of source) {
        if (!remove.has(item)) out.push(item);
    }
    return out.sort();
}

function resolveForwardGateThresholds(): ScalpDeploymentPromotionGateThresholds {
    const minRollCount = Math.floor(Number(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_ROLLS));
    const minProfitableWindowPct = normalizeFiniteNumber(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_PROFITABLE_PCT);
    const minMeanExpectancyR = normalizeFiniteNumber(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_MEAN_EXPECTANCY_R);
    const minTradesPerWindow = Math.floor(Number(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_TRADES_PER_WINDOW));
    const maxDrawdownR = normalizeFiniteNumber(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MAX_DRAWDOWN_R);
    const minWeeklySlices = normalizeFiniteNumber(process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_WEEKLY_SLICES);
    const minWeeklyProfitablePct = normalizeFiniteNumber(
        process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_WEEKLY_PROFITABLE_PCT,
    );
    const minWeeklyMedianExpectancyR = normalizeFiniteNumber(
        process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MIN_WEEKLY_MEDIAN_EXPECTANCY_R,
    );
    const maxWeeklyTopWeekPnlConcentrationPct = normalizeFiniteNumber(
        process.env.SCALP_DEPLOYMENT_FORWARD_GATE_MAX_WEEKLY_TOP_WEEK_PNL_CONCENTRATION_PCT,
    );

    return {
        minRollCount:
            Number.isFinite(minRollCount) && minRollCount > 0 ? minRollCount : DEFAULT_FORWARD_GATE_THRESHOLDS.minRollCount,
        minProfitableWindowPct:
            minProfitableWindowPct !== null
                ? Math.max(0, Math.min(100, minProfitableWindowPct))
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minProfitableWindowPct,
        minMeanExpectancyR:
            minMeanExpectancyR !== null ? minMeanExpectancyR : DEFAULT_FORWARD_GATE_THRESHOLDS.minMeanExpectancyR,
        minTradesPerWindow:
            Number.isFinite(minTradesPerWindow) && minTradesPerWindow >= 0
                ? minTradesPerWindow
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minTradesPerWindow,
        maxDrawdownR: maxDrawdownR !== null && maxDrawdownR >= 0 ? maxDrawdownR : null,
        minWeeklySlices:
            minWeeklySlices !== null && minWeeklySlices > 0
                ? Math.floor(minWeeklySlices)
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minWeeklySlices,
        minWeeklyProfitablePct:
            minWeeklyProfitablePct !== null
                ? Math.max(0, Math.min(100, minWeeklyProfitablePct))
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minWeeklyProfitablePct,
        minWeeklyMedianExpectancyR:
            minWeeklyMedianExpectancyR !== null
                ? minWeeklyMedianExpectancyR
                : DEFAULT_FORWARD_GATE_THRESHOLDS.minWeeklyMedianExpectancyR,
        maxWeeklyTopWeekPnlConcentrationPct:
            maxWeeklyTopWeekPnlConcentrationPct !== null
                ? Math.max(0, Math.min(100, maxWeeklyTopWeekPnlConcentrationPct))
                : DEFAULT_FORWARD_GATE_THRESHOLDS.maxWeeklyTopWeekPnlConcentrationPct,
    };
}

function evaluateForwardValidationAgainstThresholds(
    validation: ScalpForwardValidationMetrics,
    thresholds: ScalpDeploymentPromotionGateThresholds,
): { eligible: boolean; reason: string | null } {
    if (validation.rollCount < thresholds.minRollCount) {
        return { eligible: false, reason: 'forward_roll_count_below_threshold' };
    }
    if (validation.profitableWindowPct < thresholds.minProfitableWindowPct) {
        return { eligible: false, reason: 'forward_profitable_window_pct_below_threshold' };
    }
    if (validation.meanExpectancyR < thresholds.minMeanExpectancyR) {
        return { eligible: false, reason: 'forward_mean_expectancy_below_threshold' };
    }
    if (thresholds.minTradesPerWindow > 0) {
        const minTrades = validation.minTradesPerWindow;
        if (minTrades === null || minTrades < thresholds.minTradesPerWindow) {
            return { eligible: false, reason: 'forward_min_trades_per_window_below_threshold' };
        }
    }
    if (thresholds.maxDrawdownR !== null) {
        const maxDrawdownR = validation.maxDrawdownR;
        if (maxDrawdownR === null || maxDrawdownR > thresholds.maxDrawdownR) {
            return { eligible: false, reason: 'forward_max_drawdown_above_threshold' };
        }
    }
    if (typeof thresholds.minWeeklySlices === 'number') {
        const weeklySlices = validation.weeklySlices;
        if (weeklySlices === null || weeklySlices === undefined || weeklySlices < thresholds.minWeeklySlices) {
            return { eligible: false, reason: 'weekly_slice_count_below_threshold' };
        }
    }
    if (typeof thresholds.minWeeklyProfitablePct === 'number') {
        const weeklyProfitablePct = validation.weeklyProfitablePct;
        if (
            weeklyProfitablePct === null ||
            weeklyProfitablePct === undefined ||
            weeklyProfitablePct < thresholds.minWeeklyProfitablePct
        ) {
            return { eligible: false, reason: 'weekly_profitable_pct_below_threshold' };
        }
    }
    if (typeof thresholds.minWeeklyMedianExpectancyR === 'number') {
        const weeklyMedianExpectancyR = validation.weeklyMedianExpectancyR;
        if (
            weeklyMedianExpectancyR === null ||
            weeklyMedianExpectancyR === undefined ||
            weeklyMedianExpectancyR < thresholds.minWeeklyMedianExpectancyR
        ) {
            return { eligible: false, reason: 'weekly_median_expectancy_below_threshold' };
        }
    }
    if (typeof thresholds.maxWeeklyTopWeekPnlConcentrationPct === 'number') {
        const concentration = validation.weeklyTopWeekPnlConcentrationPct;
        if (
            concentration === null ||
            concentration === undefined ||
            concentration > thresholds.maxWeeklyTopWeekPnlConcentrationPct
        ) {
            return { eligible: false, reason: 'weekly_top_week_concentration_above_threshold' };
        }
    }
    return { eligible: true, reason: null };
}

function computeExpectedEligibilityFromSync(
    sync: SyncResearchPromotionResult,
    thresholds: ScalpDeploymentPromotionGateThresholds,
): {
    expectedEligibleIds: Set<string>;
    expectedByDeploymentId: Record<string, { eligible: boolean; reason: string | null }>;
} {
    const expectedEligibleIds = new Set<string>();
    const expectedByDeploymentId: Record<string, { eligible: boolean; reason: string | null }> = {};
    const byDeploymentId = new Map(sync.candidates.map((row) => [row.deploymentId, row] as const));
    const byKey = new Map(sync.candidates.map((row) => [keyOf(row.symbol, row.strategyId, row.tuneId), row] as const));

    for (const row of sync.rows) {
        if (row.source !== 'matrix' && row.source !== 'backtest') continue;
        if (!row.matchedCandidate) {
            expectedByDeploymentId[row.deploymentId] = { eligible: false, reason: 'missing_cycle_candidate' };
            continue;
        }
        if (row.weeklyGateReason) {
            expectedByDeploymentId[row.deploymentId] = { eligible: false, reason: row.weeklyGateReason };
            continue;
        }
        const candidate = byDeploymentId.get(row.deploymentId) || byKey.get(keyOf(row.symbol, row.strategyId, row.tuneId));
        if (!candidate) {
            expectedByDeploymentId[row.deploymentId] = { eligible: false, reason: 'missing_forward_validation_candidate' };
            continue;
        }

        const validation: ScalpForwardValidationMetrics = {
            ...candidate.forwardValidation,
            weeklySlices: row.weeklyRobustness?.slices ?? candidate.forwardValidation.weeklySlices ?? null,
            weeklyProfitablePct: row.weeklyRobustness?.profitablePct ?? candidate.forwardValidation.weeklyProfitablePct ?? null,
            weeklyMeanExpectancyR: row.weeklyRobustness?.meanExpectancyR ?? candidate.forwardValidation.weeklyMeanExpectancyR ?? null,
            weeklyMedianExpectancyR:
                row.weeklyRobustness?.medianExpectancyR ?? candidate.forwardValidation.weeklyMedianExpectancyR ?? null,
            weeklyWorstNetR: row.weeklyRobustness?.worstNetR ?? candidate.forwardValidation.weeklyWorstNetR ?? null,
            weeklyTopWeekPnlConcentrationPct:
                row.weeklyRobustness?.topWeekPnlConcentrationPct ??
                candidate.forwardValidation.weeklyTopWeekPnlConcentrationPct ??
                null,
            weeklyEvaluatedAtMs: row.weeklyRobustness?.evaluatedAtMs ?? candidate.forwardValidation.weeklyEvaluatedAtMs ?? null,
        };

        const evaluated = evaluateForwardValidationAgainstThresholds(validation, thresholds);
        expectedByDeploymentId[row.deploymentId] = evaluated;
        if (evaluated.eligible) expectedEligibleIds.add(row.deploymentId);
    }

    return {
        expectedEligibleIds,
        expectedByDeploymentId,
    };
}

async function gate1NoDuplicateExecutionRuns(
    nowMs: number,
    options: CutoverGateOptions,
): Promise<GateResult> {
    const since = new Date(nowMs - options.execWindowHours * 60 * 60_000);
    const db = scalpPrisma();

    const [stats] = await db.$queryRaw<
        Array<{
            duplicateKeys: bigint | number | string;
            duplicateRows: bigint | number | string;
            totalRows: bigint | number | string;
            distinctDeployments: bigint | number | string;
        }>
    >(Prisma.sql`
        WITH recent AS (
            SELECT deployment_id, scheduled_minute
            FROM scalp_execution_runs
            WHERE scheduled_minute >= ${since}
        ),
        grouped AS (
            SELECT deployment_id, scheduled_minute, COUNT(*)::int AS c
            FROM recent
            GROUP BY deployment_id, scheduled_minute
        )
        SELECT
            COALESCE(COUNT(*) FILTER (WHERE c > 1), 0)::bigint AS "duplicateKeys",
            COALESCE(SUM(CASE WHEN c > 1 THEN c - 1 ELSE 0 END), 0)::bigint AS "duplicateRows",
            COALESCE(SUM(c), 0)::bigint AS "totalRows",
            COALESCE(COUNT(DISTINCT deployment_id), 0)::bigint AS "distinctDeployments"
        FROM grouped;
    `);

    const duplicateKeys = toNumber(stats?.duplicateKeys);
    const duplicateRows = toNumber(stats?.duplicateRows);
    const totalRows = toNumber(stats?.totalRows);
    const distinctDeployments = toNumber(stats?.distinctDeployments);
    const samples =
        duplicateKeys > 0
            ? await db.$queryRaw<
                  Array<{
                      deploymentId: string;
                      scheduledMinute: Date;
                      duplicateCount: number;
                  }>
              >(Prisma.sql`
                SELECT
                    deployment_id AS "deploymentId",
                    scheduled_minute AS "scheduledMinute",
                    COUNT(*)::int AS "duplicateCount"
                FROM scalp_execution_runs
                WHERE scheduled_minute >= ${since}
                GROUP BY deployment_id, scheduled_minute
                HAVING COUNT(*) > 1
                ORDER BY scheduled_minute DESC
                LIMIT ${options.sampleLimit};
            `)
            : [];

    if (totalRows <= 0) {
        return {
            id: 'gate1_no_duplicate_execution_runs',
            title: 'No duplicate execution runs for same (deployment_id, scheduled_minute) over window',
            status: 'unknown',
            pass: null,
            details: {
                sinceIso: since.toISOString(),
                windowHours: options.execWindowHours,
                totalRows,
                duplicateKeys,
                duplicateRows,
                distinctDeployments,
                reason: 'insufficient_execution_rows_in_window',
            },
        };
    }

    return {
        id: 'gate1_no_duplicate_execution_runs',
        title: 'No duplicate execution runs for same (deployment_id, scheduled_minute) over window',
        status: duplicateRows === 0 ? 'pass' : 'fail',
        pass: duplicateRows === 0,
        details: {
            sinceIso: since.toISOString(),
            windowHours: options.execWindowHours,
            totalRows,
            duplicateKeys,
            duplicateRows,
            distinctDeployments,
            duplicateSamples: samples.map((row) => ({
                deploymentId: row.deploymentId,
                scheduledMinuteIso: row.scheduledMinute instanceof Date ? row.scheduledMinute.toISOString() : null,
                duplicateCount: row.duplicateCount,
            })),
        },
    };
}

async function gate0ControlPlaneCutoverConfig(): Promise<GateResult> {
    const scalpBackend = resolveScalpBackend();
    const backendIsPg = scalpBackend === 'pg';

    let mainExecutionCronCount = 0;
    let pgExecutionCronCount = 0;
    let mainCronHasIncludeCanaryParam = false;
    let parseError: string | null = null;

    try {
        const raw = await readFile(new URL('../vercel.json', import.meta.url), 'utf8');
        const parsed = JSON.parse(raw) as { crons?: Array<{ path?: unknown }> };
        const crons = Array.isArray(parsed?.crons) ? parsed.crons : [];
        for (const row of crons) {
            const cron = parseCronPath(row?.path);
            if (!cron) continue;
            if (cron.pathname === '/api/scalp/cron/execute-deployments') {
                mainExecutionCronCount += 1;
                if (cron.query.has('includeCanary')) {
                    mainCronHasIncludeCanaryParam = true;
                }
            } else if (cron.pathname === '/api/scalp/cron/execute-deployments-pg') {
                pgExecutionCronCount += 1;
            }
        }
    } catch (err: any) {
        parseError = String(err?.message || err || 'vercel_json_read_failed');
    }

    if (parseError) {
        return {
            id: 'gate0_control_plane_cutover_config',
            title: 'Control-plane is configured for full PG cutover',
            status: 'unknown',
            pass: null,
            details: {
                scalpBackend,
                backendIsPg,
                parseError,
                reason: 'vercel_cron_config_unavailable',
            },
        };
    }

    const pass =
        backendIsPg &&
        mainExecutionCronCount === 1 &&
        pgExecutionCronCount === 0 &&
        !mainCronHasIncludeCanaryParam;

    return {
        id: 'gate0_control_plane_cutover_config',
        title: 'Control-plane is configured for full PG cutover',
        status: pass ? 'pass' : 'fail',
        pass,
        details: {
            scalpBackend,
            backendIsPg,
            expectedBackend: 'pg',
            mainExecutionCronCount,
            expectedMainExecutionCronCount: 1,
            pgExecutionCronCount,
            expectedPgExecutionCronCount: 0,
            mainCronHasIncludeCanaryParam,
            expectedMainCronHasIncludeCanaryParam: false,
        },
    };
}

async function gate2ResearchQueueLagP95(
    nowMs: number,
    options: CutoverGateOptions,
): Promise<GateResult> {
    const since = new Date(nowMs - options.lagWindowHours * 60 * 60_000);
    const db = scalpPrisma();

    const [lagStats] = await db.$queryRaw<
        Array<{
            sampleCount: bigint | number | string;
            p95LagMs: number | null;
            avgLagMs: number | null;
            minLagMs: number | null;
            maxLagMs: number | null;
        }>
    >(Prisma.sql`
        SELECT
            COUNT(*)::bigint AS "sampleCount",
            PERCENTILE_CONT(0.95) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (r.claimed_at - j.scheduled_for)) * 1000.0
            ) AS "p95LagMs",
            AVG(EXTRACT(EPOCH FROM (r.claimed_at - j.scheduled_for)) * 1000.0) AS "avgLagMs",
            MIN(EXTRACT(EPOCH FROM (r.claimed_at - j.scheduled_for)) * 1000.0) AS "minLagMs",
            MAX(EXTRACT(EPOCH FROM (r.claimed_at - j.scheduled_for)) * 1000.0) AS "maxLagMs"
        FROM scalp_shadow_job_runs r
        INNER JOIN scalp_jobs j ON j.id = r.job_id
        WHERE r.kind = 'research_task'
          AND r.success = TRUE
          AND r.created_at >= ${since};
    `);

    const [queueDepth] = await db.$queryRaw<Array<{ dueCount: bigint | number | string }>>(Prisma.sql`
        SELECT COUNT(*)::bigint AS "dueCount"
        FROM scalp_jobs
        WHERE kind = 'research_task'
          AND status IN ('pending', 'retry_wait')
          AND next_run_at <= NOW();
    `);

    const sampleCount = toNumber(lagStats?.sampleCount);
    const p95LagMs = normalizeFiniteNumber(lagStats?.p95LagMs);
    const avgLagMs = normalizeFiniteNumber(lagStats?.avgLagMs);
    const minLagMs = normalizeFiniteNumber(lagStats?.minLagMs);
    const maxLagMs = normalizeFiniteNumber(lagStats?.maxLagMs);
    const dueCount = toNumber(queueDepth?.dueCount);

    if (sampleCount <= 0 || p95LagMs === null) {
        return {
            id: 'gate2_research_queue_lag_p95',
            title: 'Research queue lag p95 below threshold over window',
            status: 'unknown',
            pass: null,
            details: {
                sinceIso: since.toISOString(),
                windowHours: options.lagWindowHours,
                sampleCount,
                p95LagMs,
                maxAllowedLagMs: options.maxResearchQueueLagMs,
                dueCount,
                reason: 'insufficient_research_queue_samples_in_window',
            },
        };
    }

    return {
        id: 'gate2_research_queue_lag_p95',
        title: 'Research queue lag p95 below threshold over window',
        status: p95LagMs <= options.maxResearchQueueLagMs ? 'pass' : 'fail',
        pass: p95LagMs <= options.maxResearchQueueLagMs,
        details: {
            sinceIso: since.toISOString(),
            windowHours: options.lagWindowHours,
            sampleCount,
            p95LagMs,
            avgLagMs,
            minLagMs,
            maxLagMs,
            maxAllowedLagMs: options.maxResearchQueueLagMs,
            dueCount,
        },
    };
}

async function gate3CooldownDerivedTerminalFailures(
    nowMs: number,
    options: CutoverGateOptions,
): Promise<GateResult> {
    const windowSince = new Date(nowMs - options.cooldownWindowHours * 60 * 60_000);
    const configuredSince = parseIsoDate(options.cooldownFailureSinceIso);
    const effectiveSince =
        configuredSince && configuredSince.getTime() > windowSince.getTime() ? configuredSince : windowSince;
    const db = scalpPrisma();

    const [stats] = await db.$queryRaw<
        Array<{
            terminalFailures: bigint | number | string;
            cooldownDerivedTerminalFailures: bigint | number | string;
        }>
    >(Prisma.sql`
        SELECT
            COUNT(*) FILTER (WHERE status = 'failed_permanent')::bigint AS "terminalFailures",
            COUNT(*) FILTER (
                WHERE status = 'failed_permanent'
                  AND (
                      COALESCE(error_code, '') ILIKE '%cooldown%'
                      OR COALESCE(error_message, '') ILIKE '%cooldown%'
                  )
            )::bigint AS "cooldownDerivedTerminalFailures"
        FROM scalp_research_tasks
        WHERE updated_at >= ${effectiveSince};
    `);

    const cooldownDerivedTerminalFailures = toNumber(stats?.cooldownDerivedTerminalFailures);
    const terminalFailures = toNumber(stats?.terminalFailures);

    const samples =
        cooldownDerivedTerminalFailures > 0
            ? await db.$queryRaw<
                  Array<{
                      taskId: string;
                      cycleId: string;
                      symbol: string;
                      errorCode: string | null;
                      errorMessage: string | null;
                      updatedAt: Date;
                  }>
              >(Prisma.sql`
                SELECT
                    task_id AS "taskId",
                    cycle_id AS "cycleId",
                    symbol,
                    error_code AS "errorCode",
                    error_message AS "errorMessage",
                    updated_at AS "updatedAt"
                FROM scalp_research_tasks
                WHERE status = 'failed_permanent'
                  AND updated_at >= ${effectiveSince}
                  AND (
                      COALESCE(error_code, '') ILIKE '%cooldown%'
                      OR COALESCE(error_message, '') ILIKE '%cooldown%'
                  )
                ORDER BY updated_at DESC
                LIMIT ${options.sampleLimit};
            `)
            : [];

    return {
        id: 'gate3_cooldown_terminal_failures_zero',
        title: 'Cooldown-derived terminal failures are zero over window',
        status: cooldownDerivedTerminalFailures === 0 ? 'pass' : 'fail',
        pass: cooldownDerivedTerminalFailures === 0,
        details: {
            windowSinceIso: windowSince.toISOString(),
            configuredSinceIso: options.cooldownFailureSinceIso,
            effectiveSinceIso: effectiveSince.toISOString(),
            windowHours: options.cooldownWindowHours,
            terminalFailures,
            cooldownDerivedTerminalFailures,
            cooldownFailureSamples: samples.map((row) => ({
                taskId: row.taskId,
                cycleId: row.cycleId,
                symbol: row.symbol,
                errorCode: row.errorCode,
                errorMessage: row.errorMessage,
                updatedAtIso: row.updatedAt instanceof Date ? row.updatedAt.toISOString() : null,
            })),
        },
    };
}

async function gate4PromotionParityConsecutiveCycles(
    thresholds: ScalpDeploymentPromotionGateThresholds,
    options: CutoverGateOptions,
): Promise<GateResult> {
    const db = scalpPrisma();
    const completedCycles = await db.$queryRaw<
        Array<{
            cycleId: string;
            completedAt: Date | null;
            updatedAt: Date | null;
        }>
    >(Prisma.sql`
        SELECT
            cycle_id AS "cycleId",
            completed_at AS "completedAt",
            updated_at AS "updatedAt"
        FROM scalp_research_cycles
        WHERE status = 'completed'
        ORDER BY COALESCE(completed_at, updated_at, created_at) DESC
        LIMIT ${options.requiredConsecutiveCycles};
    `);

    if (completedCycles.length < options.requiredConsecutiveCycles) {
        return {
            id: 'gate4_promotion_outputs_match_expected_two_cycles',
            title: 'Promotion outputs match expected candidates across consecutive completed cycles',
            status: 'unknown',
            pass: null,
            details: {
                requiredConsecutiveCycles: options.requiredConsecutiveCycles,
                completedCyclesFound: completedCycles.length,
                reason: 'insufficient_completed_cycles',
                inference: true,
            },
        };
    }

    const registry = await loadScalpDeploymentRegistry();
    const actualEligibleIds = new Set(
        registry.deployments
            .filter(
                (row) =>
                    (row.source === 'matrix' || row.source === 'backtest') &&
                    Boolean(row.promotionGate?.eligible),
            )
            .map((row) => row.deploymentId),
    );

    const cycleEvaluations: Array<{
        cycleId: string;
        completedAtIso: string | null;
        syncOk: boolean;
        syncReason: string | null;
        expectedEligibleIds: Set<string>;
        missingActualIds: string[];
        unexpectedActualIds: string[];
        parityPass: boolean;
    }> = [];

    for (const cycle of completedCycles) {
        const sync = await syncResearchCyclePromotionGates({
            cycleId: cycle.cycleId,
            dryRun: true,
            requireCompletedCycle: true,
            materializeMissingCandidates: false,
            materializeEnabled: false,
            sources: ['matrix', 'backtest'],
            updatedBy: 'phase_e_cutover_gate_check',
        });

        if (!sync.ok) {
            cycleEvaluations.push({
                cycleId: cycle.cycleId,
                completedAtIso: toIso(cycle.completedAt || cycle.updatedAt),
                syncOk: false,
                syncReason: sync.reason || 'sync_failed',
                expectedEligibleIds: new Set<string>(),
                missingActualIds: [],
                unexpectedActualIds: [],
                parityPass: false,
            });
            continue;
        }

        const expected = computeExpectedEligibilityFromSync(sync, thresholds);
        const missingActualIds = setDiff(expected.expectedEligibleIds, actualEligibleIds);
        const unexpectedActualIds = setDiff(actualEligibleIds, expected.expectedEligibleIds);
        cycleEvaluations.push({
            cycleId: cycle.cycleId,
            completedAtIso: toIso(cycle.completedAt || cycle.updatedAt),
            syncOk: true,
            syncReason: sync.reason,
            expectedEligibleIds: expected.expectedEligibleIds,
            missingActualIds,
            unexpectedActualIds,
            parityPass: missingActualIds.length === 0 && unexpectedActualIds.length === 0,
        });
    }

    const latest = cycleEvaluations[0];
    const previous = cycleEvaluations[1];
    const syncFailures = cycleEvaluations.filter((row) => !row.syncOk);
    if (!latest || !previous) {
        return {
            id: 'gate4_promotion_outputs_match_expected_two_cycles',
            title: 'Promotion outputs match expected candidates across consecutive completed cycles',
            status: 'unknown',
            pass: null,
            details: {
                requiredConsecutiveCycles: options.requiredConsecutiveCycles,
                completedCyclesFound: completedCycles.length,
                reason: 'insufficient_cycle_evaluations',
                inference: true,
            },
        };
    }
    if (syncFailures.length > 0) {
        return {
            id: 'gate4_promotion_outputs_match_expected_two_cycles',
            title: 'Promotion outputs match expected candidates across consecutive completed cycles',
            status: 'unknown',
            pass: null,
            details: {
                reason: 'dry_run_sync_failed_for_cycle',
                syncFailures: syncFailures.map((row) => ({
                    cycleId: row.cycleId,
                    syncReason: row.syncReason,
                })),
                cycleEvaluations: cycleEvaluations.map((row) => ({
                    cycleId: row.cycleId,
                    completedAtIso: row.completedAtIso,
                    syncOk: row.syncOk,
                    parityPass: row.parityPass,
                })),
                inference: true,
            },
        };
    }

    const latestVsActualParityPass = latest.parityPass;
    const consecutiveExpectedStabilityPass =
        setDiff(latest.expectedEligibleIds, previous.expectedEligibleIds).length === 0 &&
        setDiff(previous.expectedEligibleIds, latest.expectedEligibleIds).length === 0;
    const pass = latestVsActualParityPass && consecutiveExpectedStabilityPass;

    return {
        id: 'gate4_promotion_outputs_match_expected_two_cycles',
        title: 'Promotion outputs match expected candidates across consecutive completed cycles',
        status: pass ? 'pass' : 'fail',
        pass,
        details: {
            inference: true,
            methodology:
                'pass requires (1) latest completed cycle expected-eligible set matches current promotion-eligible set, and (2) expected-eligible sets are stable across latest two completed cycles',
            latestVsActualParityPass,
            consecutiveExpectedStabilityPass,
            actualEligibleCount: actualEligibleIds.size,
            latestCycleId: latest.cycleId,
            previousCycleId: previous.cycleId,
            latestMissingActualIds: latest.missingActualIds.slice(0, options.sampleLimit),
            latestUnexpectedActualIds: latest.unexpectedActualIds.slice(0, options.sampleLimit),
            cycleEvaluations: cycleEvaluations.map((row) => ({
                cycleId: row.cycleId,
                completedAtIso: row.completedAtIso,
                syncOk: row.syncOk,
                syncReason: row.syncReason,
                expectedEligibleCount: row.expectedEligibleIds.size,
                missingActualCount: row.missingActualIds.length,
                unexpectedActualCount: row.unexpectedActualIds.length,
                parityPass: row.parityPass,
            })),
        },
    };
}

async function main() {
    const options = parseArgs(process.argv);
    if (!isScalpPgConfigured()) {
        throw new Error('Missing PRISMA_CONNECTION_STRING for cutover gate checks');
    }

    const nowMs = Date.now();
    const thresholds = resolveForwardGateThresholds();

    const [gate0, gate1, gate2, gate3, gate4] = await Promise.all([
        gate0ControlPlaneCutoverConfig(),
        gate1NoDuplicateExecutionRuns(nowMs, options),
        gate2ResearchQueueLagP95(nowMs, options),
        gate3CooldownDerivedTerminalFailures(nowMs, options),
        gate4PromotionParityConsecutiveCycles(thresholds, options),
    ]);

    const gates = [gate0, gate1, gate2, gate3, gate4];
    const failed = gates.filter((row) => row.status === 'fail').map((row) => row.id);
    const unknown = gates.filter((row) => row.status === 'unknown').map((row) => row.id);
    const overallPass = failed.length === 0 && (!options.failOnUnknown || unknown.length === 0);

    const report = {
        ok: overallPass,
        generatedAtIso: new Date(nowMs).toISOString(),
        options,
        environment: {
            scalpBackend: resolveScalpBackend(),
            pgConfigured: true,
        },
        thresholds,
        gates,
        summary: {
            failedGateIds: failed,
            unknownGateIds: unknown,
        },
    };

    console.log(JSON.stringify(report, null, 2));

    if (options.strict && !overallPass) {
        process.exitCode = 1;
    }
}

main()
    .catch((err: any) => {
        console.error('scalp-pg-cutover-gates failed:', err?.message || String(err));
        process.exitCode = 1;
    })
    .finally(async () => {
        try {
            await scalpPrisma().$disconnect();
        } catch {
            // best effort
        }
    });
