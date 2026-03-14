export const config = { runtime: 'nodejs', maxDuration: 600 };

import type { NextApiRequest, NextApiResponse } from 'next';

import { requireAdminAccess } from '../../../../lib/admin';
import { invokeCronEndpoint, invokeCronEndpointDetached } from '../../../../lib/scalp/cronChaining';
import {
    savePromotionSyncProgressSnapshot,
    syncResearchCyclePromotionGates,
} from '../../../../lib/scalp/researchPromotion';

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

function parseSourceList(value: string | undefined): Array<'manual' | 'backtest' | 'matrix'> | undefined {
    if (!value) return undefined;
    const rows = value
        .split(',')
        .map((row) => String(row || '').trim().toLowerCase())
        .filter((row): row is 'manual' | 'backtest' | 'matrix' => row === 'manual' || row === 'backtest' || row === 'matrix');
    return rows.length ? Array.from(new Set(rows)) : undefined;
}

function parseMaterializeSource(value: string | undefined): 'matrix' | 'backtest' | undefined {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();
    if (normalized === 'matrix' || normalized === 'backtest') return normalized;
    return undefined;
}

function parsePositiveInt(value: string | undefined): number | undefined {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n <= 0) return undefined;
    return n;
}

function parseNonNegativeInt(value: string | undefined): number | undefined {
    const n = Math.floor(Number(value));
    if (!Number.isFinite(n) || n < 0) return undefined;
    return n;
}

function parseFiniteNumber(value: string | undefined): number | undefined {
    const n = Number(value);
    if (!Number.isFinite(n)) return undefined;
    return n;
}

function setNoStoreHeaders(res: NextApiResponse): void {
    res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
    res.setHeader('Pragma', 'no-cache');
    res.setHeader('Expires', '0');
}

function logSyncGates(
    event: string,
    payload: Record<string, unknown>,
    level: 'info' | 'warn' | 'error' = 'info',
    force = false,
    debug = false,
): void {
    if (!force && !debug) return;
    const line = JSON.stringify({
        scope: 'scalp_research_cycle_sync_gates_api',
        event,
        ...payload,
    });
    if (level === 'error') console.error(line);
    else if (level === 'warn') console.warn(line);
    else console.info(line);
}

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
    if (req.method !== 'GET') {
        return res.status(405).json({ error: 'Method Not Allowed', message: 'Use GET' });
    }
    if (!requireAdminAccess(req, res)) return;
    setNoStoreHeaders(res);

    const cycleId = firstQueryValue(req.query.cycleId);
    const dryRun = parseBoolParam(req.query.dryRun, false);
    const requireCompletedCycle = parseBoolParam(req.query.requireCompletedCycle, true);
    const sources = parseSourceList(firstQueryValue(req.query.sources));
    const weeklyRobustnessEnabled = parseBoolParam(req.query.weeklyRobustnessEnabled, true);
    const weeklyRobustnessRequireWinnerShortlist = parseBoolParam(req.query.weeklyRobustnessRequireWinnerShortlist, true);
    const weeklyRobustnessTopKPerSymbol = parsePositiveInt(firstQueryValue(req.query.weeklyRobustnessTopKPerSymbol));
    const weeklyRobustnessLookbackDays = parsePositiveInt(firstQueryValue(req.query.weeklyRobustnessLookbackDays));
    const weeklyRobustnessMinCandlesPerSlice = parsePositiveInt(firstQueryValue(req.query.weeklyRobustnessMinCandlesPerSlice));
    const weeklyRobustnessMinSlices = parsePositiveInt(firstQueryValue(req.query.weeklyRobustnessMinSlices));
    const weeklyRobustnessMinProfitablePct = parseFiniteNumber(firstQueryValue(req.query.weeklyRobustnessMinProfitablePct));
    const weeklyRobustnessMinMedianExpectancyR = parseFiniteNumber(firstQueryValue(req.query.weeklyRobustnessMinMedianExpectancyR));
    const weeklyRobustnessMaxTopWeekPnlConcentrationPct = parseFiniteNumber(
        firstQueryValue(req.query.weeklyRobustnessMaxTopWeekPnlConcentrationPct),
    );
    const materializeMissingCandidates = parseBoolParam(req.query.materializeMissingCandidates, true);
    const materializeTopKPerSymbol = parsePositiveInt(firstQueryValue(req.query.materializeTopKPerSymbol));
    const materializeSource = parseMaterializeSource(firstQueryValue(req.query.materializeSource));
    const materializeEnabled = parseBoolParam(req.query.materializeEnabled, true);
    const materializeMinTradesPerWindow = parseNonNegativeInt(firstQueryValue(req.query.materializeMinTradesPerWindow));
    const materializeMinMeanExpectancyR = parseFiniteNumber(firstQueryValue(req.query.materializeMinMeanExpectancyR));
    const launchDetached = parseBoolParam(req.query.async ?? req.query.detach, false);
    const debug = parseBoolParam(req.query.debug ?? req.query.dubg, false);
    const autoSuccessor = parseBoolParam(req.query.autoSuccessor, true);
    const successorPath = firstQueryValue(req.query.successorPath) || '/api/scalp/cron/orchestrate-pipeline';
    const startedAtMs = Date.now();

    if (launchDetached) {
        if (!dryRun) {
            await savePromotionSyncProgressSnapshot({
                version: 1,
                status: 'queued',
                cycleId: cycleId || null,
                dryRun: false,
                requireCompletedCycle,
                phase: 'queued',
                startedAtMs,
                updatedAtMs: startedAtMs,
                finishedAtMs: null,
                totalDeployments: null,
                processedDeployments: 0,
                matchedDeployments: 0,
                updatedDeployments: 0,
                currentSymbol: null,
                currentStrategyId: null,
                currentTuneId: null,
                reason: 'launch_requested',
                lastError: null,
            });
        }
        const launch = await invokeCronEndpointDetached(
            req,
            '/api/scalp/cron/research-cycle-sync-gates',
            {
                cycleId,
                dryRun,
                requireCompletedCycle,
                sources: firstQueryValue(req.query.sources),
                weeklyRobustnessEnabled,
                weeklyRobustnessRequireWinnerShortlist,
                weeklyRobustnessTopKPerSymbol,
                weeklyRobustnessLookbackDays,
                weeklyRobustnessMinCandlesPerSlice,
                weeklyRobustnessMinSlices,
                weeklyRobustnessMinProfitablePct,
                weeklyRobustnessMinMedianExpectancyR,
                weeklyRobustnessMaxTopWeekPnlConcentrationPct,
                materializeMissingCandidates,
                materializeTopKPerSymbol,
                materializeSource,
                materializeEnabled,
                materializeMinTradesPerWindow,
                materializeMinMeanExpectancyR,
                updatedBy:
                    firstQueryValue(req.query.updatedBy) ||
                    'cron:research-cycle-sync-gates:detached-launch',
                debug,
                autoSuccessor,
                successorPath,
            },
            750,
        );
        const launchOk = Boolean(launch.invoked) && !launch.error;
        if (!launchOk && !dryRun) {
            await savePromotionSyncProgressSnapshot({
                version: 1,
                status: 'failed',
                cycleId: cycleId || null,
                dryRun: false,
                requireCompletedCycle,
                phase: 'launch_failed',
                startedAtMs,
                updatedAtMs: Date.now(),
                finishedAtMs: Date.now(),
                totalDeployments: null,
                processedDeployments: 0,
                matchedDeployments: 0,
                updatedDeployments: 0,
                currentSymbol: null,
                currentStrategyId: null,
                currentTuneId: null,
                reason: 'launch_failed',
                lastError: launch.error || (launch.status ? `http_${launch.status}` : 'launch_failed'),
            });
        }
        return res.status(launchOk ? 202 : 500).json({
            ok: launchOk,
            message: launchOk ? 'promotion gate sync launched' : 'promotion gate sync launch failed',
            cycleId: cycleId || null,
            dryRun,
            requireCompletedCycle,
            detached: launch.detached === true,
            launch,
        });
    }

    try {
        const out = await syncResearchCyclePromotionGates({
            cycleId,
            dryRun,
            requireCompletedCycle,
            sources,
            updatedBy: firstQueryValue(req.query.updatedBy) || 'cron:research-cycle-sync-gates',
            weeklyRobustnessEnabled,
            weeklyRobustnessRequireWinnerShortlist,
            weeklyRobustnessTopKPerSymbol,
            weeklyRobustnessLookbackDays,
            weeklyRobustnessMinCandlesPerSlice,
            weeklyRobustnessMinSlices,
            weeklyRobustnessMinProfitablePct,
            weeklyRobustnessMinMedianExpectancyR,
            weeklyRobustnessMaxTopWeekPnlConcentrationPct,
            materializeMissingCandidates,
            materializeTopKPerSymbol,
            materializeSource,
            materializeEnabled,
            materializeMinTradesPerWindow,
            materializeMinMeanExpectancyR,
            debug,
        });
        const message =
            out.ok === false
                ? `promotion gate sync skipped (${out.reason || 'unknown_reason'})`
                : out.reason
                  ? `promotion gate sync no-op (${out.reason})`
                  : `promotion gate sync completed (updated=${out.deploymentsUpdated}, matched=${out.deploymentsMatched}, considered=${out.deploymentsConsidered})`;
        const shouldWarn = out.ok === false && out.reason !== 'cycle_not_completed' && out.reason !== 'sync_already_current';
        logSyncGates(
            'sync_completed',
            {
                requestedCycleId: cycleId || null,
                cycleId: out.cycleId,
                cycleStatus: out.cycleStatus,
                dryRun,
                requireCompletedCycle,
                reason: out.reason,
                ok: out.ok,
                deploymentsConsidered: out.deploymentsConsidered,
                deploymentsMatched: out.deploymentsMatched,
                deploymentsUpdated: out.deploymentsUpdated,
                candidatesCount: out.candidates.length,
                rowsCount: out.rows.length,
                materialization: out.materialization,
                durationMs: Date.now() - startedAtMs,
            },
            shouldWarn ? 'warn' : 'info',
            shouldWarn,
            debug,
        );
        const shouldCallSuccessor = autoSuccessor && !dryRun && out.ok !== false && out.reason !== 'cycle_not_completed';
        const successor = shouldCallSuccessor
            ? await invokeCronEndpoint(req, successorPath, {
                  continue: 1,
                  debug,
              })
            : null;

        return res.status(200).json({
            ok: out.ok,
            message,
            cycleId: out.cycleId,
            cycleStatus: out.cycleStatus,
            reason: out.reason,
            dryRun: out.dryRun,
            requireCompletedCycle: out.requireCompletedCycle,
            weeklyPolicy: out.weeklyPolicy,
            confirmationPolicy: out.confirmationPolicy,
            deploymentsConsidered: out.deploymentsConsidered,
            deploymentsMatched: out.deploymentsMatched,
            deploymentsUpdated: out.deploymentsUpdated,
            materialization: out.materialization,
            candidates: out.candidates,
            rows: out.rows,
            chaining: {
                autoSuccessor,
                successorPath,
                requested: shouldCallSuccessor,
                successor,
            },
        });
    } catch (err: any) {
        if (!dryRun) {
            await savePromotionSyncProgressSnapshot({
                version: 1,
                status: 'failed',
                cycleId: cycleId || null,
                dryRun: false,
                requireCompletedCycle,
                phase: 'failed',
                startedAtMs: startedAtMs,
                updatedAtMs: Date.now(),
                finishedAtMs: Date.now(),
                totalDeployments: null,
                processedDeployments: 0,
                matchedDeployments: 0,
                updatedDeployments: 0,
                currentSymbol: null,
                currentStrategyId: null,
                currentTuneId: null,
                reason: 'sync_failed',
                lastError: String(err?.message || err || 'research_cycle_sync_gates_failed').slice(0, 600),
            }).catch(() => null);
        }
        logSyncGates(
            'sync_failed',
            {
                requestedCycleId: cycleId || null,
                dryRun,
                requireCompletedCycle,
                error: err?.message || String(err),
                stack: err?.stack || null,
                durationMs: Date.now() - startedAtMs,
            },
            'error',
            true,
            debug,
        );
        return res.status(500).json({
            error: 'research_cycle_sync_gates_failed',
            message: err?.message || String(err),
        });
    }
}
