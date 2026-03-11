import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../admin';
import { fetchCapitalOpenPositionSnapshots } from '../capital';
import type { CapitalOpenPositionSnapshot } from '../capital';
import { getScalpStrategyConfig } from './config';
import { runScalpExecuteCycle } from './engine';
import { isScalpPgConfigured } from './pg/client';
import { listExecutableDeploymentsFromPg } from './pg/deployments';
import {
    claimScalpExecutionRunSlotsBulk,
    finalizeScalpExecutionRunsBulk,
    type FinalizeScalpExecutionRunInput,
} from './pg/executionRuns';
import { loadScalpStrategyRuntimeSnapshot } from './store';
import type { ScalpMarketSnapshot } from './types';

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
    const symbol = firstQueryValue(req.query.symbol);
    const all = parseBoolParam(req.query.all, false);
    const requirePromotionEligible = parseBoolParam(
        req.query.requirePromotionEligible,
        parseBoolParam(process.env.SCALP_REQUIRE_PROMOTION_ELIGIBLE, false),
    );
    const strictPgRequired = options.strictPgRequired !== false;

    try {
        if (!isScalpPgConfigured()) {
            if (strictPgRequired) {
                return res.status(503).json({
                    error: 'execute_deployments_pg_not_configured',
                    message: 'PRISMA_CONNECTION_STRING is required for scalp PG execution.',
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
                message: 'PRISMA_CONNECTION_STRING is not configured; skipping PG execution.',
            });
        }
        if (!symbol && !all) {
            return res.status(400).json({
                error: 'symbol_required',
                message: 'Provide ?symbol=... or ?all=true.',
            });
        }

        const deployments = await listExecutableDeploymentsFromPg({
            symbol,
            requirePromotionEligible,
            limit: 2000,
        });

        if (!deployments.length) {
            return res.status(200).json({
                ok: true,
                dryRun,
                backend: 'pg',
                strictPgRequired,
                count: 0,
                results: [],
                message: requirePromotionEligible
                    ? 'No PG deployments matched enabled + promotion-eligible filters.'
                    : 'No PG deployments matched the request.',
            });
        }

        const results: Array<Record<string, unknown>> = [];
        const errors: Array<Record<string, unknown>> = [];
        const marketSnapshotCache = new Map<string, ScalpMarketSnapshot>();
        const cfg = getScalpStrategyConfig();
        const runtimeSnapshot = await loadScalpStrategyRuntimeSnapshot(cfg.enabled);
        let brokerPositionSnapshots: CapitalOpenPositionSnapshot[] | undefined;
        let skipBrokerSnapshotFetch = false;
        let brokerSnapshotsPrefetched = false;
        if (!dryRun) {
            try {
                brokerPositionSnapshots = await fetchCapitalOpenPositionSnapshots();
                brokerSnapshotsPrefetched = true;
            } catch {
                skipBrokerSnapshotFetch = true;
            }
        }

        const effectiveNowMs = nowMs ?? Date.now();
        const scheduledMinuteMs = floorToMinuteMs(effectiveNowMs);
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
        const finalizeRows: FinalizeScalpExecutionRunInput[] = [];

        for (const deployment of alreadyClaimedDeployments) {
            results.push({
                generatedAtMs: effectiveNowMs,
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

        for (const deployment of deploymentsToRun) {
            try {
                const cycle = await runScalpExecuteCycle({
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
                    skipBrokerSnapshotFetch,
                });
                const finishedAtMs = Date.now();
                const executionRunStatus = cycle.runLockAcquired ? 'succeeded' : 'skipped';
                results.push({
                    ...cycle,
                    enabled: deployment.enabled,
                    source: deployment.source,
                    promotionEligible: deployment.promotionEligible,
                    promotionReason: deployment.promotionReason,
                    backend: 'pg',
                    executionRunStatus,
                });
                finalizeRows.push({
                    deploymentId: deployment.deploymentId,
                    scheduledMinuteMs,
                    status: executionRunStatus,
                    reasonCodes: cycle.reasonCodes,
                    finishedAtMs,
                });
            } catch (err: any) {
                const finishedAtMs = Date.now();
                const message = String(err?.message || err || 'execution_failed');
                const errorCode = extractErrorCode(message);
                errors.push({
                    symbol: deployment.symbol,
                    strategyId: deployment.strategyId,
                    tuneId: deployment.tuneId,
                    deploymentId: deployment.deploymentId,
                    error: message,
                    errorCode,
                });
                finalizeRows.push({
                    deploymentId: deployment.deploymentId,
                    scheduledMinuteMs,
                    status: 'failed',
                    errorCode,
                    errorMessage: message,
                    finishedAtMs,
                });
            }
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

        return res.status(errors.length ? 207 : 200).json({
            ok: errors.length === 0,
            dryRun,
            debug,
            backend: 'pg',
            strictPgRequired,
            requestedSymbol: symbol || null,
            requestedAll: all,
            requirePromotionEligible,
            scheduledMinuteMs,
            executionRunClaimedCount: deploymentsToRun.length,
            executionRunSkippedAlreadyClaimedCount: alreadyClaimedDeployments.length,
            executionRunFinalizeExpected: finalizeRows.length,
            executionRunFinalizeUpdated,
            count: deployments.length,
            brokerSnapshotsPrefetched,
            skipBrokerSnapshotFetch,
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
