export const config = { runtime: 'nodejs' };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { invokeCronEndpoint } from '../../../../lib/scalp/cronChaining';
import { loadScalpPanicStopState } from '../../../../lib/scalp/panicStop';
import { aggregateScalpResearchCycle, runResearchWorker } from '../../../../lib/scalp/researchCycle';
import { syncResearchCyclePromotionGates } from '../../../../lib/scalp/researchPromotion';

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

function parsePositiveInt(value: string | undefined): number | undefined {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return undefined;
    return Math.floor(n);
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

    const cycleId = firstQueryValue(req.query.cycleId);
    const workerId = firstQueryValue(req.query.workerId);
    const maxRuns = parsePositiveInt(firstQueryValue(req.query.maxRuns));
    const concurrency = parsePositiveInt(firstQueryValue(req.query.concurrency));
    const maxDurationMs = parsePositiveInt(firstQueryValue(req.query.maxDurationMs));
    const debug = parseBoolParam(req.query.debug, false);
    const autoContinue = parseBoolParam(req.query.autoContinue, true);
    const continueHop = Math.max(0, Math.floor(Number(firstQueryValue(req.query.continueHop)) || 0));
    const autoContinueMaxHops = Math.max(
        0,
        Math.min(
            10,
            Math.floor(
                Number(firstQueryValue(req.query.autoContinueMaxHops)) ||
                    Number(process.env.SCALP_RESEARCH_WORKER_AUTO_CONTINUE_MAX_HOPS) ||
                    2,
            ),
        ),
    );
    const aggregateAfter = parseBoolParam(req.query.aggregateAfter, true);
    const finalizeWhenDone = parseBoolParam(req.query.finalizeWhenDone, true);
    const syncPromotionGates = parseBoolParam(req.query.syncPromotionGates, true);
    const requireCompletedCycleForSync = parseBoolParam(req.query.requireCompletedCycleForSync, true);
    const autoSuccessor = parseBoolParam(req.query.autoSuccessor, true);
    const successorPath = firstQueryValue(req.query.successorPath) || '/api/scalp/cron/research-cycle-sync-gates';

    try {
        const panicStop = await loadScalpPanicStopState();
        if (panicStop.enabled) {
            return res.status(409).json({
                ok: false,
                error: 'panic_stop_enabled',
                message: `panic_stop_enabled${panicStop.reason ? `:${panicStop.reason}` : ''}`,
                panicStop,
            });
        }
        const worker = await runResearchWorker({ cycleId, workerId, maxRuns, concurrency, maxDurationMs, debug });
        const noClaim = worker.noClaimScanSummary;
        const preflightBlocked = worker.orchestration.gate === 'blocked';
        const shouldWarnNoProgress =
            worker.attemptedRuns === 0 &&
            !!noClaim &&
            (noClaim.pending > 0 ||
                noClaim.runningFresh > 0 ||
                noClaim.runningStale > 0 ||
                noClaim.runningMissingStartedAt > 0 ||
                noClaim.failedRetryable > 0 ||
                noClaim.lockMisses > 0);
        const workerMessage =
            preflightBlocked
                ? `worker blocked by preflight (${worker.orchestration.reasonCodes.join(',') || 'unknown'})`
                : worker.attemptedRuns > 0
                  ? `worker processed ${worker.attemptedRuns} tasks (completed=${worker.completedRuns}, failed=${worker.failedRuns}, concurrency=${worker.concurrency}${worker.stoppedByDurationBudget ? ', stoppedByDurationBudget=true' : ''})`
                  : noClaim
                    ? `no claimable tasks (pending=${noClaim.pending}, runningFresh=${noClaim.runningFresh}, runningStale=${noClaim.runningStale}, runningMissingStartedAt=${noClaim.runningMissingStartedAt}, failedPendingManualRetry=${noClaim.failedRetryable}, failedMaxed=${noClaim.failedMaxed}, symbolCooldownBlocked=${noClaim.symbolCooldownBlocked}, lockMisses=${noClaim.lockMisses})`
                    : 'worker did not claim any tasks';
        const aggregate =
            !preflightBlocked && aggregateAfter && worker.cycleId
                ? await aggregateScalpResearchCycle({
                      cycleId: worker.cycleId,
                      finalizeWhenDone,
                  })
                : null;
        const promotionSync =
            !preflightBlocked &&
            syncPromotionGates &&
            worker.cycleId &&
            aggregate &&
            (!requireCompletedCycleForSync || aggregate.summary.status === 'completed')
                ? await syncResearchCyclePromotionGates({
                      cycleId: worker.cycleId,
                      dryRun: false,
                      requireCompletedCycle: requireCompletedCycleForSync,
                      updatedBy: 'cron:research-cycle-worker',
                  })
                : null;
        const shouldAutoContinue =
            autoContinue &&
            !preflightBlocked &&
            continueHop < autoContinueMaxHops &&
            Boolean(worker.cycleId) &&
            Boolean(aggregate) &&
            aggregate!.summary.status === 'running' &&
            (aggregate!.summary.totals.pending > 0 || aggregate!.summary.totals.running > 0) &&
            (worker.stoppedByDurationBudget || worker.attemptedRuns >= worker.maxRuns);
        const continuation = shouldAutoContinue
            ? await invokeCronEndpoint(req, '/api/scalp/cron/research-cycle-worker', {
                  cycleId: worker.cycleId!,
                  maxRuns: worker.maxRuns,
                  concurrency: worker.concurrency,
                  maxDurationMs: worker.maxDurationMs,
                  debug,
                  autoContinue: 1,
                  continueHop: continueHop + 1,
                  autoContinueMaxHops,
                  aggregateAfter,
                  finalizeWhenDone,
                  syncPromotionGates,
                  requireCompletedCycleForSync,
                  autoSuccessor,
                  successorPath,
              })
            : null;
        const shouldCallSuccessor =
            autoSuccessor &&
            !shouldAutoContinue &&
            Boolean(worker.cycleId) &&
            Boolean(aggregate) &&
            aggregate!.summary.status === 'completed' &&
            !promotionSync;
        const successor = shouldCallSuccessor
            ? await invokeCronEndpoint(req, successorPath, {
                  cycleId: worker.cycleId!,
                  dryRun: 0,
                  requireCompletedCycle: requireCompletedCycleForSync,
                  updatedBy: 'cron:research-cycle-worker:successor',
              })
            : null;
        if (worker.failedRuns > 0 || shouldWarnNoProgress || preflightBlocked) {
            console.warn(
                JSON.stringify({
                    scope: 'scalp_research_worker_api',
                    event: 'worker_attention',
                    cycleId: worker.cycleId,
                    workerId: worker.workerId,
                    maxRuns: worker.maxRuns,
                    maxDurationMs: worker.maxDurationMs,
                    attemptedRuns: worker.attemptedRuns,
                    completedRuns: worker.completedRuns,
                    failedRuns: worker.failedRuns,
                    stoppedByDurationBudget: worker.stoppedByDurationBudget,
                    noClaimScanSummary: worker.noClaimScanSummary,
                    orchestration: worker.orchestration,
                    aggregateStatus: aggregate?.summary?.status || null,
                }),
            );
        } else if (debug) {
            console.info(
                JSON.stringify({
                    scope: 'scalp_research_worker_api',
                    event: 'worker_ok',
                    cycleId: worker.cycleId,
                    workerId: worker.workerId,
                    maxRuns: worker.maxRuns,
                    maxDurationMs: worker.maxDurationMs,
                    attemptedRuns: worker.attemptedRuns,
                    completedRuns: worker.completedRuns,
                    failedRuns: worker.failedRuns,
                    stoppedByDurationBudget: worker.stoppedByDurationBudget,
                }),
            );
        }

        return res.status(200).json({
            ok: true,
            message: workerMessage,
            autoContinue: {
                enabled: autoContinue,
                continueHop,
                maxHops: autoContinueMaxHops,
                requested: shouldAutoContinue,
                continuation,
            },
            autoSuccessor: {
                enabled: autoSuccessor,
                successorPath,
                requested: shouldCallSuccessor,
                successor,
            },
            requestedCycleId: cycleId || null,
            worker,
            orchestration: worker.orchestration,
            aggregate: aggregate
                ? {
                      cycleId: aggregate.cycle.cycleId,
                      status: aggregate.cycle.status,
                      progressPct: aggregate.summary.progressPct,
                      totals: aggregate.summary.totals,
                      topCandidates: aggregate.summary.candidateAggregates.slice(0, 10),
                      generatedAtMs: aggregate.summary.generatedAtMs,
                  }
                : null,
            promotionSync,
        });
    } catch (err: any) {
        console.error(
            JSON.stringify({
                scope: 'scalp_research_worker_api',
                event: 'handler_failed',
                cycleId: cycleId || null,
                workerId: workerId || null,
                maxRuns: maxRuns ?? null,
                maxDurationMs: maxDurationMs ?? null,
                debug,
                error: err?.message || String(err),
                stack: err?.stack || null,
            }),
        );
        return res.status(500).json({
            error: 'research_cycle_worker_failed',
            message: err?.message || String(err),
        });
    }
}
