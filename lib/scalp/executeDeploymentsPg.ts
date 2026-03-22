import { Prisma } from '@prisma/client';
import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../admin';
import {
    getScalpVenueAdapter,
    isScalpVenueAdapterSupported,
    supportedScalpVenues,
    type ScalpBrokerPositionSnapshot,
} from './adapters';
import { getScalpStrategyConfig } from './config';
import { runScalpExecuteCycle } from './engine';
import { isScalpPgConfigured, scalpPrisma } from './pg/client';
import { listExecutableDeploymentsFromPg, type PgExecutableDeploymentRow } from './pg/deployments';
import {
    claimScalpExecutionRunSlotsBulk,
    finalizeScalpExecutionRunsBulk,
    type FinalizeScalpExecutionRunInput,
} from './pg/executionRuns';
import { listScalpEntrySessionProfiles, parseScalpEntrySessionProfileStrict } from './sessions';
import { loadScalpStrategyRuntimeSnapshot } from './store';
import type { ScalpEntrySessionProfile, ScalpMarketSnapshot } from './types';
import { type ScalpVenue } from './venue';

const EXECUTE_DEPLOYMENTS_MUTEX_KIND = 'execute_cycle';
const EXECUTE_DEPLOYMENTS_MUTEX_PREFIX = 'scalp_execute_deployments_mutex_v2';

function parseBoolParam(value: string | string[] | undefined, fallback: boolean): boolean {
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
    if (Array.isArray(value) && value.length > 0) return String(value[0] || '').trim() || undefined;
    return undefined;
}

function parseNowMs(value: string | undefined): number | undefined {
    const num = Number(value);
    if (Number.isFinite(num) && num > 0) return Math.floor(num);
    return undefined;
}

function parsePositiveInt(value: string | undefined): number | undefined {
    const num = Number(value);
    if (!Number.isFinite(num) || num <= 0) return undefined;
    return Math.floor(num);
}

function parseVenue(value: string | undefined): ScalpVenue | null | undefined {
    if (!value) return undefined;
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'bitget') {
        return normalized;
    }
    return null;
}

function parseEntrySessionProfile(value: string | undefined): ScalpEntrySessionProfile | null {
    return parseScalpEntrySessionProfileStrict(value);
}

function setNoStoreHeaders(res: NextApiResponse): void {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

function floorToMinuteMs(valueMs: number): number {
    return Math.floor(valueMs / 60_000) * 60_000;
}

function extractErrorCode(message: string): string {
    const normalized = message
        .toLowerCase()
        .replace(/[^a-z0-9_]+/g, '_')
        .replace(/_+/g, '_')
        .replace(/^_+|_+$/g, '');
    return (normalized || 'execution_failed').slice(0, 80);
}

function toPositiveInt(value: unknown, fallback: number): number {
    const num = Math.floor(Number(value));
    if (!Number.isFinite(num) || num <= 0) return fallback;
    return num;
}

function resolveVenueDefaultConcurrency(venue: ScalpVenue): number {
    const perVenueEnvKey = 'SCALP_EXECUTE_DEPLOYMENTS_CONCURRENCY_BITGET';
    const configuredPerVenue = Number(process.env[perVenueEnvKey]);
    if (Number.isFinite(configuredPerVenue) && configuredPerVenue > 0) {
        return Math.max(1, Math.min(32, Math.floor(configuredPerVenue)));
    }
    const configuredGlobal = Number(process.env.SCALP_EXECUTE_DEPLOYMENTS_CONCURRENCY);
    if (Number.isFinite(configuredGlobal) && configuredGlobal > 0) {
        return Math.max(1, Math.min(32, Math.floor(configuredGlobal)));
    }
    return 4;
}

function resolveVenueRequestedConcurrency(venue: ScalpVenue, concurrencyQuery: number | undefined): number {
    if (concurrencyQuery && concurrencyQuery > 0) {
        return Math.max(1, Math.min(32, Math.floor(concurrencyQuery)));
    }
    return resolveVenueDefaultConcurrency(venue);
}

function classifyVenueLockError(error: unknown): string {
    const message = String((error as any)?.message || error || '')
        .trim()
        .toLowerCase();
    if (!message) return 'venue_lock_error';
    if (message.includes('timed out fetching a new connection from the connection pool')) {
        return 'venue_lock_db_pool_timeout';
    }
    if (message.includes("can't reach database server")) {
        return 'venue_lock_db_unreachable';
    }
    if (message.includes('connection pool')) {
        return 'venue_lock_db_pool_error';
    }
    if (message.includes('prisma')) {
        return 'venue_lock_db_prisma_error';
    }
    return 'venue_lock_error';
}

function buildVenueMutexDedupeKey(venue: ScalpVenue, entrySessionProfile: ScalpEntrySessionProfile): string {
    return `${EXECUTE_DEPLOYMENTS_MUTEX_PREFIX}:${venue}:${entrySessionProfile}`;
}

async function acquireExecuteDeploymentsVenueLock(params: {
    venue: ScalpVenue;
    entrySessionProfile: ScalpEntrySessionProfile;
    lockOwner: string;
    lockTtlMs: number;
}): Promise<boolean> {
    if (!isScalpPgConfigured()) return false;
    const db = scalpPrisma();
    const dedupeKey = buildVenueMutexDedupeKey(params.venue, params.entrySessionProfile);
    await db.$executeRaw(
        Prisma.sql`
            INSERT INTO scalp_jobs(
                kind,
                dedupe_key,
                payload,
                status,
                attempts,
                max_attempts,
                scheduled_for,
                next_run_at,
                locked_by,
                locked_at,
                last_error
            )
            VALUES(
                ${EXECUTE_DEPLOYMENTS_MUTEX_KIND}::scalp_job_kind,
                ${dedupeKey},
                ${JSON.stringify({ venue: params.venue, entrySessionProfile: params.entrySessionProfile })}::jsonb,
                'succeeded'::scalp_job_status,
                1,
                1,
                NOW(),
                NOW(),
                NULL,
                NULL,
                NULL
            )
            ON CONFLICT(kind, dedupe_key)
            DO NOTHING;
        `,
    );
    const rows = await db.$queryRaw<Array<{ id: bigint }>>(Prisma.sql`
        UPDATE scalp_jobs
        SET
            locked_by = ${params.lockOwner},
            locked_at = NOW(),
            updated_at = NOW(),
            payload = COALESCE(payload, '{}'::jsonb) || ${JSON.stringify({ venue: params.venue, entrySessionProfile: params.entrySessionProfile })}::jsonb
        WHERE kind = ${EXECUTE_DEPLOYMENTS_MUTEX_KIND}::scalp_job_kind
          AND dedupe_key = ${dedupeKey}
          AND (
              locked_by IS NULL
              OR locked_at IS NULL
              OR locked_at <= NOW() - (${Math.max(60_000, Math.floor(params.lockTtlMs))} * INTERVAL '1 millisecond')
              OR locked_by = ${params.lockOwner}
          )
        RETURNING id;
    `);
    return rows.length > 0;
}

async function releaseExecuteDeploymentsVenueLock(params: {
    venue: ScalpVenue;
    entrySessionProfile: ScalpEntrySessionProfile;
    lockOwner: string;
}): Promise<void> {
    if (!isScalpPgConfigured()) return;
    const db = scalpPrisma();
    const dedupeKey = buildVenueMutexDedupeKey(params.venue, params.entrySessionProfile);
    await db.$executeRaw(Prisma.sql`
        UPDATE scalp_jobs
        SET
            locked_by = NULL,
            locked_at = NULL,
            updated_at = NOW()
        WHERE kind = ${EXECUTE_DEPLOYMENTS_MUTEX_KIND}::scalp_job_kind
          AND dedupe_key = ${dedupeKey}
          AND locked_by = ${params.lockOwner};
    `);
}

interface VenueBatchOutput {
    venue: ScalpVenue;
    requestedConcurrency: number;
    effectiveConcurrency: number;
    lockOwner: string;
    lockAcquired: boolean;
    claimedCount: number;
    skippedAlreadyClaimedCount: number;
    skippedVenueLockBusyCount: number;
    skippedVenueLockErrorCount: number;
    brokerSnapshotsPrefetched: boolean;
    skipBrokerSnapshotFetch: boolean;
    results: Array<Record<string, unknown>>;
    errors: Array<Record<string, unknown>>;
    finalizeRows: FinalizeScalpExecutionRunInput[];
}

async function runVenueDeploymentsBatch(params: {
    venue: ScalpVenue;
    entrySessionProfile: ScalpEntrySessionProfile;
    deployments: PgExecutableDeploymentRow[];
    dryRun: boolean;
    debug: boolean;
    effectiveNowMs: number;
    scheduledMinuteMs: number;
    requestedConcurrency: number;
    runtimeSnapshot: Awaited<ReturnType<typeof loadScalpStrategyRuntimeSnapshot>>;
    marketSnapshotCache: Map<string, ScalpMarketSnapshot>;
    lockTtlMs: number;
}): Promise<VenueBatchOutput> {
    const {
        venue,
        entrySessionProfile,
        deployments,
        dryRun,
        debug,
        effectiveNowMs,
        scheduledMinuteMs,
        requestedConcurrency,
        runtimeSnapshot,
        marketSnapshotCache,
        lockTtlMs,
    } = params;

    const output: VenueBatchOutput = {
        venue,
        requestedConcurrency,
        effectiveConcurrency: deployments.length > 0 ? Math.max(1, Math.min(requestedConcurrency, deployments.length)) : 0,
        lockOwner: `scalp_exec_lock_${venue}_${entrySessionProfile}_${effectiveNowMs}_${Math.floor(Math.random() * 1_000_000)}`,
        lockAcquired: false,
        claimedCount: 0,
        skippedAlreadyClaimedCount: 0,
        skippedVenueLockBusyCount: 0,
        skippedVenueLockErrorCount: 0,
        brokerSnapshotsPrefetched: false,
        skipBrokerSnapshotFetch: false,
        results: [],
        errors: [],
        finalizeRows: [],
    };

    if (!deployments.length) {
        output.effectiveConcurrency = 0;
        return output;
    }

    try {
        output.lockAcquired = await acquireExecuteDeploymentsVenueLock({
            venue,
            entrySessionProfile,
            lockOwner: output.lockOwner,
            lockTtlMs,
        });
    } catch (err: any) {
        const lockErrorMessage = String(err?.message || err || 'venue_lock_acquire_failed');
        const lockErrorCode = classifyVenueLockError(err);
        output.errors.push({
            venue,
            error: lockErrorMessage,
            errorCode: lockErrorCode,
            stage: 'venue_lock_acquire',
        });
        output.skippedVenueLockErrorCount = deployments.length;
        for (const deployment of deployments) {
            output.results.push({
                generatedAtMs: effectiveNowMs,
                venue: deployment.venue,
                symbol: deployment.symbol,
                strategyId: deployment.strategyId,
                tuneId: deployment.tuneId,
                deploymentId: deployment.deploymentId,
                dayKey: null,
                dryRun,
                runLockAcquired: false,
                state: 'IDLE',
                reasonCodes: ['SCALP_EXECUTION_VENUE_LOCK_ERROR'],
                enabled: deployment.enabled,
                source: deployment.source,
                promotionEligible: deployment.promotionEligible,
                promotionReason: deployment.promotionReason,
                backend: 'pg',
                executionRunStatus: 'skipped',
                executionRunSkippedReason: 'venue_lock_acquire_failed',
                executionRunSkippedErrorCode: lockErrorCode,
            });
        }
        return output;
    }

    if (!output.lockAcquired) {
        output.skippedVenueLockBusyCount = deployments.length;
        for (const deployment of deployments) {
            output.results.push({
                generatedAtMs: effectiveNowMs,
                venue: deployment.venue,
                symbol: deployment.symbol,
                strategyId: deployment.strategyId,
                tuneId: deployment.tuneId,
                deploymentId: deployment.deploymentId,
                dayKey: null,
                dryRun,
                runLockAcquired: false,
                state: 'IDLE',
                reasonCodes: ['SCALP_EXECUTION_VENUE_LOCK_BUSY'],
                enabled: deployment.enabled,
                source: deployment.source,
                promotionEligible: deployment.promotionEligible,
                promotionReason: deployment.promotionReason,
                backend: 'pg',
                executionRunStatus: 'skipped',
                executionRunSkippedReason: 'venue_lock_busy',
            });
        }
        return output;
    }

    try {
        let brokerPositionSnapshots: ScalpBrokerPositionSnapshot[] | undefined = undefined;
        if (!dryRun) {
            try {
                const adapter = getScalpVenueAdapter(venue);
                brokerPositionSnapshots = await adapter.broker.fetchOpenPositionSnapshots();
                output.brokerSnapshotsPrefetched = true;
            } catch {
                output.skipBrokerSnapshotFetch = true;
            }
        }

        const claimedRuns = await claimScalpExecutionRunSlotsBulk(
            deployments.map((row) => ({
                deploymentId: row.deploymentId,
                scheduledMinuteMs,
                startedAtMs: effectiveNowMs,
            })),
        );
        const claimedDeploymentIds = new Set(claimedRuns.map((row) => row.deploymentId));
        const alreadyClaimedDeployments = deployments.filter((row) => !claimedDeploymentIds.has(row.deploymentId));
        const deploymentsToRun = deployments.filter((row) => claimedDeploymentIds.has(row.deploymentId));
        output.claimedCount = deploymentsToRun.length;
        output.skippedAlreadyClaimedCount = alreadyClaimedDeployments.length;
        output.effectiveConcurrency =
            deploymentsToRun.length > 0 ? Math.max(1, Math.min(requestedConcurrency, deploymentsToRun.length)) : 0;

        for (const deployment of alreadyClaimedDeployments) {
            output.results.push({
                generatedAtMs: effectiveNowMs,
                venue: deployment.venue,
                symbol: deployment.symbol,
                strategyId: deployment.strategyId,
                tuneId: deployment.tuneId,
                deploymentId: deployment.deploymentId,
                dayKey: null,
                dryRun,
                runLockAcquired: false,
                state: 'IDLE',
                reasonCodes: ['SCALP_EXECUTION_RUN_ALREADY_CLAIMED'],
                enabled: deployment.enabled,
                source: deployment.source,
                promotionEligible: deployment.promotionEligible,
                promotionReason: deployment.promotionReason,
                backend: 'pg',
                executionRunStatus: 'skipped',
            });
        }

        const runOutcomes: Array<{
            result?: Record<string, unknown>;
            error?: Record<string, unknown>;
            finalize: FinalizeScalpExecutionRunInput;
        }> = new Array(deploymentsToRun.length);

        let nextIndex = 0;
        const runWorker = async () => {
            while (true) {
                const idx = nextIndex;
                nextIndex += 1;
                if (idx >= deploymentsToRun.length) return;
                const deployment = deploymentsToRun[idx]!;
                try {
                    const cycle = await runScalpExecuteCycle({
                        venue: deployment.venue,
                        symbol: deployment.symbol,
                        dryRun,
                        debug,
                        nowMs: effectiveNowMs,
                        strategyId: deployment.strategyId,
                        tuneId: deployment.tuneId,
                        deploymentId: deployment.deploymentId,
                        configOverride: deployment.configOverride || undefined,
                        marketSnapshotCache,
                        runtimeSnapshot,
                        brokerPositionSnapshots,
                        skipBrokerSnapshotFetch: output.skipBrokerSnapshotFetch,
                    });
                    const finishedAtMs = Date.now();
                    const executionRunStatus = cycle.runLockAcquired ? 'succeeded' : 'skipped';
                    runOutcomes[idx] = {
                        result: {
                            ...cycle,
                            enabled: deployment.enabled,
                            source: deployment.source,
                            promotionEligible: deployment.promotionEligible,
                            promotionReason: deployment.promotionReason,
                            backend: 'pg',
                            executionRunStatus,
                        },
                        finalize: {
                            deploymentId: deployment.deploymentId,
                            scheduledMinuteMs,
                            status: executionRunStatus,
                            reasonCodes: cycle.reasonCodes,
                            finishedAtMs,
                        },
                    };
                } catch (err: any) {
                    const finishedAtMs = Date.now();
                    const message = String(err?.message || err || 'execution_failed');
                    const errorCode = extractErrorCode(message);
                    runOutcomes[idx] = {
                        error: {
                            venue,
                            symbol: deployment.symbol,
                            strategyId: deployment.strategyId,
                            tuneId: deployment.tuneId,
                            deploymentId: deployment.deploymentId,
                            error: message,
                            errorCode,
                        },
                        finalize: {
                            deploymentId: deployment.deploymentId,
                            scheduledMinuteMs,
                            status: 'failed',
                            errorCode,
                            errorMessage: message,
                            finishedAtMs,
                        },
                    };
                }
            }
        };

        if (output.effectiveConcurrency > 0) {
            await Promise.all(Array.from({ length: output.effectiveConcurrency }, () => runWorker()));
        }
        for (const outcome of runOutcomes) {
            if (!outcome) continue;
            if (outcome.result) output.results.push(outcome.result);
            if (outcome.error) output.errors.push(outcome.error);
            output.finalizeRows.push(outcome.finalize);
        }

        return output;
    } finally {
        try {
            await releaseExecuteDeploymentsVenueLock({
                venue,
                entrySessionProfile,
                lockOwner: output.lockOwner,
            });
        } catch (releaseErr: any) {
            const releaseError = {
                venue,
                stage: 'venue_lock_release',
                errorCode: classifyVenueLockError(releaseErr),
                error: String(releaseErr?.message || releaseErr || 'venue_lock_release_failed'),
            };
            output.errors.push(releaseError);
            if (debug) {
                console.warn(
                    JSON.stringify({
                        scope: 'scalp_execute_deployments_pg',
                        event: 'venue_lock_release_failed',
                        ...releaseError,
                    }),
                );
            }
        }
    }
}

export async function runExecuteDeploymentsPg(
    req: NextApiRequest,
    res: NextApiResponse,
    options: {
        strictPgRequired?: boolean;
    } = {},
) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    const dryRun = parseBoolParam(req.query.dryRun, true);
    const debug = parseBoolParam(req.query.debug, false);
    const nowMs = parseNowMs(firstQueryValue(req.query.nowMs));
    const concurrencyQuery = parsePositiveInt(firstQueryValue(req.query.concurrency));
    const symbol = firstQueryValue(req.query.symbol);
    const venue = parseVenue(firstQueryValue(req.query.venue));
    const entrySessionProfile = parseEntrySessionProfile(firstQueryValue(req.query.session));
    const all = parseBoolParam(req.query.all, false);
    const requirePromotionEligible = parseBoolParam(
        req.query.requirePromotionEligible,
        parseBoolParam(process.env.SCALP_REQUIRE_PROMOTION_ELIGIBLE, false),
    );
    const strictPgRequired = options.strictPgRequired !== false;
    const lockTtlMs = Math.max(
        60_000,
        Math.min(
            30 * 60_000,
            toPositiveInt(
                parsePositiveInt(firstQueryValue(req.query.lockTtlMs)),
                toPositiveInt(process.env.SCALP_EXECUTE_DEPLOYMENTS_LOCK_TTL_MS, 5 * 60_000),
            ),
        ),
    );

    try {
        if (venue === null) {
            return res.status(400).json({
                error: 'invalid_venue',
                message: 'Unsupported venue. Use ?venue=bitget.',
            });
        }
        if (!entrySessionProfile) {
            return res.status(400).json({
                error: 'invalid_session',
                message: `Use session=${listScalpEntrySessionProfiles().join('|')}.`,
            });
        }
        if (venue && !isScalpVenueAdapterSupported(venue)) {
            return res.status(501).json({
                error: 'venue_adapter_not_implemented',
                venue,
                message: `Scalp venue adapter is not implemented for "${venue}" yet.`,
            });
        }
        if (!isScalpPgConfigured()) {
            if (strictPgRequired) {
                return res.status(503).json({
                    error: 'execute_deployments_pg_not_configured',
                    message: 'Scalp PG is not configured; set DATABASE_URL (Neon) or SCALP_PG_CONNECTION_STRING.',
                    backend: 'pg',
                    strictPgRequired,
                });
            }
            return res.status(200).json({
                ok: true,
                dryRun,
                backend: 'pg',
                strictPgRequired,
                skipped: true,
                reason: 'pg_not_configured',
                message: 'Scalp PG is not configured; skipping PG execution.',
            });
        }
        if (!symbol && !all) {
            return res.status(400).json({
                error: 'symbol_required',
                message: 'Provide ?symbol=... or ?all=true.',
            });
        }

        const deploymentsRaw = await listExecutableDeploymentsFromPg({
            symbol,
            venue: venue ?? undefined,
            sessionProfile: entrySessionProfile,
            requirePromotionEligible,
            limit: 2000,
        });
        const deployments = deploymentsRaw.filter((row) => isScalpVenueAdapterSupported(row.venue));

        if (!deployments.length) {
            return res.status(200).json({
                ok: true,
                dryRun,
                backend: 'pg',
                strictPgRequired,
                requestedSymbol: symbol || null,
                requestedVenue: venue || null,
                requestedSession: entrySessionProfile,
                requestedAll: all,
                requirePromotionEligible,
                count: 0,
                results: [],
                message: requirePromotionEligible
                    ? 'No PG deployments matched enabled + promotion-eligible filters.'
                    : 'No PG deployments matched the request.',
            });
        }

        const marketSnapshotCacheByVenue = new Map<ScalpVenue, Map<string, ScalpMarketSnapshot>>();
        for (const deploymentVenue of supportedScalpVenues()) {
            marketSnapshotCacheByVenue.set(deploymentVenue, new Map<string, ScalpMarketSnapshot>());
        }
        const cfg = getScalpStrategyConfig();
        const runtimeSnapshot = await loadScalpStrategyRuntimeSnapshot(cfg.enabled);

        const deploymentsByVenue = new Map<ScalpVenue, PgExecutableDeploymentRow[]>();
        for (const row of deployments) {
            const bucket = deploymentsByVenue.get(row.venue) || [];
            bucket.push(row);
            deploymentsByVenue.set(row.venue, bucket);
        }

        const effectiveNowMs = nowMs ?? Date.now();
        const scheduledMinuteMs = floorToMinuteMs(effectiveNowMs);

        const venueOrder = venue
            ? [venue]
            : supportedScalpVenues().filter((deploymentVenue) => (deploymentsByVenue.get(deploymentVenue) || []).length);
        const venueBatchOutputs = await Promise.all(
            venueOrder.map(async (deploymentVenue) => {
                const rows = deploymentsByVenue.get(deploymentVenue) || [];
                const requestedConcurrency = resolveVenueRequestedConcurrency(deploymentVenue, concurrencyQuery);
                const cache = marketSnapshotCacheByVenue.get(deploymentVenue) || new Map<string, ScalpMarketSnapshot>();
                return runVenueDeploymentsBatch({
                    venue: deploymentVenue,
                    entrySessionProfile,
                    deployments: rows,
                    dryRun,
                    debug,
                    effectiveNowMs,
                    scheduledMinuteMs,
                    requestedConcurrency,
                    runtimeSnapshot,
                    marketSnapshotCache: cache,
                    lockTtlMs,
                });
            }),
        );

        const results: Array<Record<string, unknown>> = [];
        const errors: Array<Record<string, unknown>> = [];
        const finalizeRows: FinalizeScalpExecutionRunInput[] = [];
        let executionRunClaimedCount = 0;
        let executionRunSkippedAlreadyClaimedCount = 0;
        let executionRunSkippedVenueLockBusyCount = 0;
        let executionRunSkippedVenueLockErrorCount = 0;
        let brokerSnapshotsPrefetched = false;
        const skipBrokerSnapshotFetchByVenue = new Set<ScalpVenue>();

        for (const batch of venueBatchOutputs) {
            results.push(...batch.results);
            errors.push(...batch.errors);
            finalizeRows.push(...batch.finalizeRows);
            executionRunClaimedCount += batch.claimedCount;
            executionRunSkippedAlreadyClaimedCount += batch.skippedAlreadyClaimedCount;
            executionRunSkippedVenueLockBusyCount += batch.skippedVenueLockBusyCount;
            executionRunSkippedVenueLockErrorCount += batch.skippedVenueLockErrorCount;
            if (batch.brokerSnapshotsPrefetched) brokerSnapshotsPrefetched = true;
            if (batch.skipBrokerSnapshotFetch) skipBrokerSnapshotFetchByVenue.add(batch.venue);
        }

        let executionRunFinalizeUpdated = 0;
        if (finalizeRows.length > 0) {
            executionRunFinalizeUpdated = await finalizeScalpExecutionRunsBulk(finalizeRows);
            if (executionRunFinalizeUpdated !== finalizeRows.length) {
                errors.push({
                    error: 'execution_run_finalize_mismatch',
                    scheduledMinuteMs,
                    expected: finalizeRows.length,
                    updated: executionRunFinalizeUpdated,
                });
            }
        }

        const executionConcurrencyRequested =
            concurrencyQuery && concurrencyQuery > 0
                ? Math.max(1, Math.min(32, Math.floor(concurrencyQuery)))
                : Math.max(1, Math.min(32, Math.floor(Number(process.env.SCALP_EXECUTE_DEPLOYMENTS_CONCURRENCY) || 4)));
        const executionConcurrencyEffective = venueBatchOutputs.reduce((sum, batch) => sum + batch.effectiveConcurrency, 0);

        return res.status(errors.length ? 207 : 200).json({
            ok: errors.length === 0,
            dryRun,
            debug,
            backend: 'pg',
            strictPgRequired,
            requestedSymbol: symbol || null,
            requestedVenue: venue || null,
            requestedSession: entrySessionProfile,
            requestedAll: all,
            requirePromotionEligible,
            executionConcurrencyRequested,
            executionConcurrencyEffective,
            executionConcurrencyByVenue: Object.fromEntries(
                venueBatchOutputs.map((batch) => [
                    batch.venue,
                    {
                        requested: batch.requestedConcurrency,
                        effective: batch.effectiveConcurrency,
                        lockAcquired: batch.lockAcquired,
                        deployments: (deploymentsByVenue.get(batch.venue) || []).length,
                    },
                ]),
            ),
            lockTtlMs,
            scheduledMinuteMs,
            executionRunClaimedCount,
            executionRunSkippedAlreadyClaimedCount,
            executionRunSkippedVenueLockBusyCount,
            executionRunSkippedVenueLockErrorCount,
            executionRunFinalizeExpected: finalizeRows.length,
            executionRunFinalizeUpdated,
            count: deployments.length,
            brokerSnapshotsPrefetched,
            skipBrokerSnapshotFetch: Array.from(skipBrokerSnapshotFetchByVenue),
            results,
            errors,
        });
    } catch (err: any) {
        return res.status(500).json({
            error: 'execute_deployments_pg_failed',
            message: err?.message || String(err),
        });
    }
}
