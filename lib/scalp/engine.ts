import crypto from 'crypto';

import { getScalpVenueAdapter, type ScalpBrokerPositionSnapshot } from './adapters';
import { applyScalpStrategyConfigOverride, getScalpStrategyConfig, normalizeScalpSymbol } from './config';
import { ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID } from './adaptive/types';
import type { ScalpStrategyConfigOverride } from './config';
import { resolveScalpDeployment } from './deployments';
import {
    buildScalpEntryPlan,
    executeScalpEntryPlan,
    manageScalpOpenTrade,
    reconcileScalpBrokerPosition,
    resolveLegacyIfvgEntryIntent,
} from './execution';
import { loadScalpMarketSnapshot } from './marketData';
import { buildScalpSessionWindows, isScalpSundayEntryBlocked } from './sessions';
import { getDefaultScalpStrategy, getScalpStrategyById, getScalpStrategyPreferredTimeframes } from './strategies/registry';
import { applySymbolGuardRiskDefaultsToStrategyConfig } from './strategies/guardDefaults';
import { advanceScalpStateMachine, createInitialScalpSessionState, deriveScalpDayKey } from './stateMachine';
import { isModelGuidedComposerStrategyId, resolveScalpExecutionStrategyId } from '../scalp-v2/composerExecution';
import {
    appendScalpTradeLedgerEntry,
    appendScalpJournal,
    loadScalpSessionState,
    loadScalpStrategyRuntimeSnapshot,
    releaseScalpRunLock,
    saveScalpSessionState,
    tryAcquireScalpRunLock,
} from './store';
import { appendScalpAdaptiveSelectorDecisions, getScalpAdaptiveActiveSnapshot } from './pg/adaptive';
import type { ScalpStrategyRuntimeSnapshot } from './store';
import type {
    ScalpExecuteCycleResult,
    ScalpJournalEntry,
    ScalpMarketSnapshot,
    ScalpSessionState,
    ScalpTradeLedgerEntry,
} from './types';
import type { ScalpVenue } from './venue';

function journalEntry(params: {
    type: ScalpJournalEntry['type'];
    symbol?: string | null;
    dayKey?: string | null;
    level?: ScalpJournalEntry['level'];
    reasonCodes?: string[];
    payload?: Record<string, unknown>;
}): ScalpJournalEntry {
    return {
        id: crypto.randomUUID(),
        timestampMs: Date.now(),
        type: params.type,
        symbol: params.symbol ?? null,
        dayKey: params.dayKey ?? null,
        level: params.level ?? 'info',
        reasonCodes: params.reasonCodes ?? [],
        payload: params.payload ?? {},
    };
}

async function safeAppendJournal(entry: ScalpJournalEntry, maxRows: number): Promise<void> {
    try {
        await appendScalpJournal(entry, maxRows);
    } catch (err) {
        console.warn('Failed to append scalp journal:', err);
    }
}

async function safeAppendTradeLedgerEntry(entry: ScalpTradeLedgerEntry): Promise<void> {
    try {
        await appendScalpTradeLedgerEntry(entry);
    } catch (err) {
        console.warn('Failed to append scalp trade ledger entry:', err);
    }
}

async function safeAppendAdaptiveDecision(params: {
    deploymentId: string;
    symbol: string;
    strategyId: string;
    entrySessionProfile: 'tokyo' | 'berlin' | 'newyork' | 'sydney';
    nowMs: number;
    selectedArmType: 'pattern' | 'incumbent' | 'none';
    selectedArmId?: string | null;
    confidence?: number | null;
    skipReason?: string | null;
    reasonCodes?: string[];
    featuresHash?: string | null;
    snapshotId?: string | null;
    details?: Record<string, unknown> | null;
}): Promise<void> {
    try {
        await appendScalpAdaptiveSelectorDecisions([
            {
                tsMs: params.nowMs,
                deploymentId: params.deploymentId,
                symbol: params.symbol,
                strategyId: params.strategyId,
                entrySessionProfile: params.entrySessionProfile,
                snapshotId: params.snapshotId || null,
                selectedArmId: params.selectedArmId || null,
                selectedArmType: params.selectedArmType,
                confidence:
                    params.confidence === null || params.confidence === undefined
                        ? null
                        : Number(params.confidence),
                skipReason: params.skipReason || null,
                reasonCodes: params.reasonCodes || [],
                featuresHash: params.featuresHash || null,
                details: params.details || null,
            },
        ]);
    } catch (err) {
        console.warn('Failed to append scalp adaptive selector decision:', err);
    }
}

function withRunContext(
    state: ScalpSessionState,
    params: { nowMs: number; runId: string; dryRun: boolean; reasonCodes: string[]; killSwitch: boolean },
): ScalpSessionState {
    return {
        ...state,
        killSwitchActive: params.killSwitch,
        updatedAtMs: params.nowMs,
        run: {
            lastRunAtMs: params.nowMs,
            lastRunId: params.runId,
            dryRunLast: params.dryRun,
            lastReasonCodes: params.reasonCodes.slice(0, 16),
        },
    };
}

function dedupeReasonCodes(codes: string[]): string[] {
    return Array.from(new Set(codes.map((code) => String(code || '').trim().toUpperCase()).filter((code) => code.length > 0)));
}

function asRecord(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
    return value as Record<string, unknown>;
}

const SCALP_ENFORCED_RISK_PCT_OF_EQUITY = 5;
const SCALP_IDLE_HEARTBEAT_PERSIST_MS = (() => {
    const n = Number(process.env.SCALP_IDLE_HEARTBEAT_PERSIST_MS ?? 5 * 60_000);
    if (!Number.isFinite(n) || n <= 0) return 5 * 60_000;
    return Math.max(60_000, Math.min(30 * 60_000, Math.floor(n)));
})();

export async function runScalpExecuteCycle(opts: {
    venue?: ScalpVenue;
    symbol?: string;
    dryRun?: boolean;
    nowMs?: number;
    configOverride?: ScalpStrategyConfigOverride;
    strategyId?: string;
    tuneId?: string;
    deploymentId?: string;
    debug?: boolean;
    marketSnapshotCache?: Map<string, ScalpMarketSnapshot>;
    runtimeSnapshot?: ScalpStrategyRuntimeSnapshot;
    brokerPositionSnapshots?: ScalpBrokerPositionSnapshot[];
    skipBrokerSnapshotFetch?: boolean;
} = {}): Promise<ScalpExecuteCycleResult> {
    const baseCfg = getScalpStrategyConfig();
    let cfg = applyScalpStrategyConfigOverride(baseCfg, opts.configOverride);
    const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
    const dryRun = opts.dryRun ?? cfg.dryRunDefault;
    const symbol = normalizeScalpSymbol(opts.symbol || cfg.defaultSymbol);
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const runId = crypto.randomUUID();
    const debug = Boolean(opts.debug);
    const runtime = opts.runtimeSnapshot || (await loadScalpStrategyRuntimeSnapshot(cfg.enabled, opts.strategyId));
    const requestedStrategyId = String(opts.strategyId || '')
        .trim()
        .toLowerCase();
    const strategyControl =
        runtime.strategies.find((row) => row.strategyId === requestedStrategyId) ||
        runtime.strategy ||
        runtime.strategies.find((row) => row.strategyId === runtime.defaultStrategyId) ||
        runtime.strategies[0] ||
        {
            strategyId: runtime.defaultStrategyId,
            shortName: runtime.defaultStrategyId,
            longName: runtime.defaultStrategyId,
            enabled: cfg.enabled,
            envEnabled: cfg.enabled,
            kvEnabled: null,
            updatedAtMs: null,
            updatedBy: null,
        };
    const strategyId = strategyControl.strategyId;
    const deployment = resolveScalpDeployment({
        venue: opts.venue,
        symbol,
        strategyId,
        tuneId: opts.tuneId,
        deploymentId: opts.deploymentId,
    });
    const venueAdapter = getScalpVenueAdapter(deployment.venue);
    const executionStrategyId =
        resolveScalpExecutionStrategyId({
            strategyId: deployment.strategyId,
            tuneId: deployment.tuneId,
        }) || deployment.strategyId;
    const isComposer = isModelGuidedComposerStrategyId(deployment.strategyId);
    const strategyDef = isComposer
        ? (getScalpStrategyById(deployment.strategyId) || getDefaultScalpStrategy())
        : (getScalpStrategyById(executionStrategyId) ||
           getScalpStrategyById(deployment.strategyId) ||
           getScalpStrategyById(runtime.defaultStrategyId) ||
           getDefaultScalpStrategy());
    const preferredTimeframes = getScalpStrategyPreferredTimeframes(
        executionStrategyId,
    );
    cfg = applySymbolGuardRiskDefaultsToStrategyConfig({
        cfg,
        symbol: deployment.symbol,
        strategyId: executionStrategyId,
    });
    if (preferredTimeframes) {
        cfg = {
            ...cfg,
            timeframes: {
                ...cfg.timeframes,
                asiaBase: preferredTimeframes.asiaBaseTf,
                confirm: preferredTimeframes.confirmTf,
            },
        };
    }
    if (opts.configOverride) {
        cfg = applyScalpStrategyConfigOverride(cfg, opts.configOverride);
    }
    if (executionStrategyId === ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID) {
        const adaptiveSnapshot = await getScalpAdaptiveActiveSnapshot({
            symbol: deployment.symbol,
            entrySessionProfile: cfg.sessions.entrySessionProfile,
            strategyId: executionStrategyId,
        });
        if (adaptiveSnapshot) {
            cfg = applyScalpStrategyConfigOverride(cfg, {
                adaptive: {
                    snapshotId: adaptiveSnapshot.snapshotId,
                    incumbentArm: adaptiveSnapshot.catalog?.incumbentArm || null,
                    thresholds: {
                        minConfidence: Number.isFinite(Number(adaptiveSnapshot.catalog?.minConfidence))
                            ? Number(adaptiveSnapshot.catalog?.minConfidence)
                            : null,
                    },
                    catalog: adaptiveSnapshot.catalog || null,
                },
            });
        }
    }

    if (!strategyControl.enabled) {
        const reasonCodes = strategyControl.envEnabled
            ? ['SCALP_ENGINE_DISABLED', 'SCALP_STRATEGY_DISABLED_BY_KV']
            : ['SCALP_ENGINE_DISABLED', 'SCALP_ENGINE_DISABLED_BY_ENV'];
        await safeAppendJournal(
            journalEntry({
                type: 'execution',
                symbol,
                dayKey,
                level: 'warn',
                reasonCodes,
                payload: {
                    dryRun,
                    nowMs,
                    runId,
                    strategyId: deployment.strategyId,
                    tuneId: deployment.tuneId,
                    deploymentId: deployment.deploymentId,
                    strategyEnabled: strategyControl.enabled,
                    strategyEnvEnabled: strategyControl.envEnabled,
                    strategyKvEnabled: strategyControl.kvEnabled,
                },
            }),
            cfg.storage.journalMax,
        );
        return {
                generatedAtMs: nowMs,
                venue: deployment.venue,
                symbol: deployment.symbol,
                strategyId: deployment.strategyId,
                tuneId: deployment.tuneId,
                deploymentId: deployment.deploymentId,
                dayKey,
                dryRun,
                runLockAcquired: false,
            state: 'IDLE',
            reasonCodes,
        };
    }

    const runLockAcquired = await tryAcquireScalpRunLock(symbol, runId, cfg.idempotency.runLockSeconds, strategyId, {
        venue: deployment.venue,
        tuneId: deployment.tuneId,
        deploymentId: deployment.deploymentId,
    });

    if (!runLockAcquired) {
        const reasonCodes = ['SCALP_RUN_LOCK_ACTIVE'];
        await safeAppendJournal(
            journalEntry({
                type: 'execution',
                symbol,
                dayKey,
                level: 'warn',
                reasonCodes,
                payload: {
                    dryRun,
                    nowMs,
                    runId,
                    strategyId: deployment.strategyId,
                    tuneId: deployment.tuneId,
                    deploymentId: deployment.deploymentId,
                },
            }),
            cfg.storage.journalMax,
        );
        return {
            generatedAtMs: nowMs,
            venue: deployment.venue,
            symbol: deployment.symbol,
            strategyId: deployment.strategyId,
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
            dayKey,
            dryRun,
            runLockAcquired: false,
            state: 'IDLE',
            reasonCodes,
        };
    }

    try {
        const loadedState = await loadScalpSessionState(symbol, dayKey, strategyId, {
            venue: deployment.venue,
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
        });
        const currentState =
            loadedState ||
            createInitialScalpSessionState({
                venue: deployment.venue,
                symbol: deployment.symbol,
                strategyId: deployment.strategyId,
                tuneId: deployment.tuneId,
                deploymentId: deployment.deploymentId,
                dayKey,
                nowMs,
                killSwitchActive: cfg.risk.killSwitch,
            });
        currentState.killSwitchActive = cfg.risk.killSwitch;

        const transition = advanceScalpStateMachine(currentState, { nowMs, dayKey });
        const windows = buildScalpSessionWindows({
            dayKey,
            clockMode: cfg.sessions.clockMode,
            asiaWindowLocal: cfg.sessions.asiaWindowLocal,
            raidWindowLocal: cfg.sessions.raidWindowLocal,
        });

        const startedWithTrade = Boolean(currentState.trade);
        let tradeEventOccurred = false;
        let nextState = transition.nextState;
        let market: ScalpMarketSnapshot | null = null;
        const phaseReasonCodes: string[] = [];

        if (!cfg.risk.killSwitch) {
            try {
                const snapshotCacheKey = [
                    deployment.venue,
                    deployment.symbol,
                    cfg.timeframes.asiaBase,
                    cfg.timeframes.confirm,
                    nowMs,
                    cfg.data.minBaseCandles,
                    cfg.data.minConfirmCandles,
                    cfg.data.maxCandlesPerRequest,
                ].join('|');
                if (opts.marketSnapshotCache?.has(snapshotCacheKey)) {
                    market = opts.marketSnapshotCache.get(snapshotCacheKey)!;
                    if (debug) {
                        console.info(
                            JSON.stringify({
                                scope: 'scalp_debug',
                                event: 'market_snapshot_reused',
                                symbol: deployment.symbol,
                                strategyId: deployment.strategyId,
                                tuneId: deployment.tuneId,
                                deploymentId: deployment.deploymentId,
                                nowMs,
                                dryRun,
                                baseTf: cfg.timeframes.asiaBase,
                                confirmTf: cfg.timeframes.confirm,
                                baseCandles: market.baseCandles.length,
                                confirmCandles: market.confirmCandles.length,
                            }),
                        );
                    }
                } else {
                    market = await loadScalpMarketSnapshot({
                        adapter: venueAdapter,
                        symbol,
                        nowMs,
                        windows,
                        baseTf: cfg.timeframes.asiaBase,
                        confirmTf: cfg.timeframes.confirm,
                        minBaseCandles: cfg.data.minBaseCandles,
                        minConfirmCandles: cfg.data.minConfirmCandles,
                        maxCandlesPerRequest: cfg.data.maxCandlesPerRequest,
                    });
                    opts.marketSnapshotCache?.set(snapshotCacheKey, market);
                }
                if (debug) {
                    console.info(
                        JSON.stringify({
                            scope: 'scalp_debug',
                            event: 'market_snapshot_loaded',
                            symbol: deployment.symbol,
                            strategyId: deployment.strategyId,
                            tuneId: deployment.tuneId,
                            deploymentId: deployment.deploymentId,
                            nowMs,
                            dryRun,
                            baseTf: cfg.timeframes.asiaBase,
                            confirmTf: cfg.timeframes.confirm,
                            baseCandles: market.baseCandles.length,
                            confirmCandles: market.confirmCandles.length,
                            minBaseCandles: cfg.data.minBaseCandles,
                            minConfirmCandles: cfg.data.minConfirmCandles,
                            maxCandlesPerRequest: cfg.data.maxCandlesPerRequest,
                        }),
                    );
                }
                const phase = strategyDef.applyPhaseDetectors({
                    state: nextState,
                    market,
                    windows,
                    nowMs,
                    cfg,
                });
                nextState = phase.state;
                phaseReasonCodes.push(...phase.reasonCodes);
                if (deployment.strategyId === ADAPTIVE_META_SELECTOR_M15_M3_STRATEGY_ID) {
                    const adaptiveDecision = asRecord(asRecord(phase.meta)?.adaptiveDecision);
                    const selectedArmTypeRaw = String(adaptiveDecision?.selectedArmType || '').trim().toLowerCase();
                    const selectedArmType =
                        selectedArmTypeRaw === 'pattern' || selectedArmTypeRaw === 'incumbent'
                            ? selectedArmTypeRaw
                            : 'none';
                    const selectedArmId = String(adaptiveDecision?.selectedArmId || '').trim() || null;
                    const confidenceRaw = Number(adaptiveDecision?.confidence);
                    const confidence = Number.isFinite(confidenceRaw) ? confidenceRaw : null;
                    const skipReason = String(adaptiveDecision?.skipReason || '').trim() || null;
                    const snapshotId =
                        String(adaptiveDecision?.snapshotId || '').trim() ||
                        String(cfg.adaptive?.snapshotId || '').trim() ||
                        null;
                    const featureHash = String(adaptiveDecision?.featureHash || '').trim() || null;
                    await safeAppendAdaptiveDecision({
                        deploymentId: deployment.deploymentId,
                        symbol: deployment.symbol,
                        strategyId: deployment.strategyId,
                        entrySessionProfile: cfg.sessions.entrySessionProfile,
                        nowMs,
                        selectedArmType,
                        selectedArmId,
                        confidence,
                        skipReason,
                        reasonCodes: phase.reasonCodes,
                        featuresHash: featureHash,
                        snapshotId,
                        details: adaptiveDecision,
                    });
                }
                if (debug) {
                    const volCodes = phase.reasonCodes.filter((code) => String(code || '').includes('VOL_FILTER'));
                    console.info(
                        JSON.stringify({
                            scope: 'scalp_debug',
                            event: 'phase_detectors_applied',
                            symbol: deployment.symbol,
                            strategyId: deployment.strategyId,
                            tuneId: deployment.tuneId,
                            deploymentId: deployment.deploymentId,
                            nowMs,
                            state: nextState.state,
                            reasonCodes: phase.reasonCodes,
                            volFilterCodes: volCodes,
                            baseCandles: market.baseCandles.length,
                            confirmCandles: market.confirmCandles.length,
                        }),
                    );
                }

                const reconciled = await reconcileScalpBrokerPosition({
                    adapter: venueAdapter,
                    state: nextState,
                    market,
                    dryRun,
                    maxOpenPositionsPerSymbol: cfg.risk.maxOpenPositionsPerSymbol,
                    snapshots: opts.brokerPositionSnapshots,
                    skipSnapshotFetch: opts.skipBrokerSnapshotFetch,
                });
                nextState = reconciled.state;
                phaseReasonCodes.push(...reconciled.reasonCodes);
                const brokerPositionGuardBlocked =
                    reconciled.reasonCodes.includes('BROKER_RECONCILE_UNAVAILABLE') ||
                    reconciled.reasonCodes.includes('BROKER_SYMBOL_POSITION_LIMIT_REACHED');
                const hadOpenTradeAtStartOfManage = Boolean(nextState.trade);
                const tradeBeforeManage = hadOpenTradeAtStartOfManage && nextState.trade ? { ...nextState.trade } : null;
                const priorRealizedR = Number.isFinite(Number(nextState.stats.realizedR)) ? Number(nextState.stats.realizedR) : 0;

                const managed = await manageScalpOpenTrade({
                    adapter: venueAdapter,
                    state: nextState,
                    market,
                    cfg,
                    dryRun,
                    nowMs,
                });
                nextState = managed.state;
                phaseReasonCodes.push(...managed.reasonCodes);
                if (hadOpenTradeAtStartOfManage && tradeBeforeManage && !nextState.trade) {
                    tradeEventOccurred = true;
                    const nextRealizedR = Number.isFinite(Number(nextState.stats.realizedR)) ? Number(nextState.stats.realizedR) : priorRealizedR;
                    const totalTradeR = nextRealizedR - priorRealizedR;
                    await safeAppendTradeLedgerEntry({
                        id: crypto.randomUUID(),
                        timestampMs: nowMs,
                        exitAtMs: Number.isFinite(Number(nextState.stats.lastExitAtMs))
                            ? Number(nextState.stats.lastExitAtMs)
                            : nowMs,
                        symbol: deployment.symbol,
                        strategyId: deployment.strategyId,
                        tuneId: deployment.tuneId,
                        deploymentId: deployment.deploymentId,
                        side: tradeBeforeManage.side,
                        dryRun: Boolean(tradeBeforeManage.dryRun),
                        rMultiple: Number.isFinite(totalTradeR) ? totalTradeR : 0,
                        reasonCodes: dedupeReasonCodes(managed.reasonCodes),
                    });
                }

                const strategyEntryIntent = phase.entryIntent ?? null;
                const legacyEntryIntent = strategyEntryIntent ? null : resolveLegacyIfvgEntryIntent(nextState);
                if (!strategyEntryIntent && legacyEntryIntent) {
                    phaseReasonCodes.push('ENTRY_INTENT_LEGACY_FALLBACK');
                }
                const entryIntent = strategyEntryIntent || legacyEntryIntent;

                const realizedR = Number.isFinite(Number(nextState.stats.realizedR)) ? Number(nextState.stats.realizedR) : 0;
                if (realizedR <= cfg.risk.dailyLossLimitR) {
                    nextState.state = 'DONE';
                    phaseReasonCodes.push('DAILY_LOSS_LIMIT_BLOCKED_NEW_ENTRY');
                }
                const sundayEntryBlocked = isScalpSundayEntryBlocked({
                    nowMs,
                    clockMode: cfg.sessions.clockMode,
                });
                if (sundayEntryBlocked) {
                    phaseReasonCodes.push('ENTRY_BLOCKED_SUNDAY_GLOBAL');
                }

                if (
                    !hadOpenTradeAtStartOfManage &&
                    !nextState.trade &&
                    !brokerPositionGuardBlocked &&
                    entryIntent &&
                    nextState.state !== 'DONE' &&
                    nextState.state !== 'COOLDOWN' &&
                    !sundayEntryBlocked
                ) {
                    let entryCfg = {
                        ...cfg,
                        risk: {
                            ...cfg.risk,
                            riskPerTradePct: SCALP_ENFORCED_RISK_PCT_OF_EQUITY,
                        },
                    };
                    let canPlanEntry = dryRun;

                    if (!dryRun) {
                        try {
                            const liveEquityUsd = await venueAdapter.broker.fetchAccountEquityUsd();
                            if (Number.isFinite(liveEquityUsd as number) && Number(liveEquityUsd) > 0) {
                                entryCfg = {
                                    ...entryCfg,
                                    risk: {
                                        ...entryCfg.risk,
                                        referenceEquityUsd: Number(liveEquityUsd),
                                    },
                                };
                                phaseReasonCodes.push(`ENTRY_RISK_${SCALP_ENFORCED_RISK_PCT_OF_EQUITY}PCT_LIVE_EQUITY`);
                                canPlanEntry = true;
                            } else {
                                phaseReasonCodes.push('ENTRY_BLOCKED_LIVE_EQUITY_UNAVAILABLE');
                            }
                        } catch {
                            phaseReasonCodes.push('ENTRY_BLOCKED_LIVE_EQUITY_UNAVAILABLE');
                        }
                    } else {
                            phaseReasonCodes.push(`ENTRY_RISK_${SCALP_ENFORCED_RISK_PCT_OF_EQUITY}PCT_REFERENCE_EQUITY`);
                    }

                    if (!canPlanEntry) {
                        phaseReasonCodes.push('ENTRY_SKIPPED_RISK_SIZING_GUARD');
                    } else {
                        const planRes = buildScalpEntryPlan({
                            state: nextState,
                            market,
                            cfg: entryCfg,
                            entryIntent,
                        });
                        phaseReasonCodes.push(...planRes.reasonCodes);
                        if (planRes.plan) {
                            const entryRes = await executeScalpEntryPlan({
                                adapter: venueAdapter,
                                state: nextState,
                                plan: planRes.plan,
                                cfg: entryCfg,
                                dryRun,
                                nowMs,
                            });
                            nextState = entryRes.state;
                            phaseReasonCodes.push(...entryRes.reasonCodes);
                        }
                    }
                }
                if (startedWithTrade !== Boolean(nextState.trade)) {
                    tradeEventOccurred = true;
                }
            } catch (err: any) {
                phaseReasonCodes.push('MARKET_DATA_UNAVAILABLE');
                await safeAppendJournal(
                    journalEntry({
                        type: 'risk',
                        symbol,
                        dayKey,
                        level: 'warn',
                        reasonCodes: ['MARKET_DATA_UNAVAILABLE'],
                        payload: {
                            dryRun,
                            nowMs,
                            runId,
                            strategyId: deployment.strategyId,
                            tuneId: deployment.tuneId,
                            deploymentId: deployment.deploymentId,
                            message: err?.message || String(err),
                        },
                    }),
                    cfg.storage.journalMax,
                );
            }
        } else {
            phaseReasonCodes.push('GLOBAL_KILL_SWITCH_ACTIVE');
        }

        const reasonCodes = dedupeReasonCodes(['SCALP_PHASE3_EXECUTION', ...transition.reasonCodes, ...phaseReasonCodes]);
        const previousReasonCodes = dedupeReasonCodes(currentState.run?.lastReasonCodes || []);
        const sameReasonSignature =
            previousReasonCodes.length === reasonCodes.length &&
            previousReasonCodes.every((code, idx) => code === reasonCodes[idx]);
        const stateChanged = transition.transitioned || currentState.state !== nextState.state;
        nextState = withRunContext(nextState, {
            nowMs,
            runId,
            dryRun,
            reasonCodes,
            killSwitch: cfg.risk.killSwitch,
        });

        const persistableNoop = !stateChanged && !tradeEventOccurred && sameReasonSignature;
        const lastPersistAtMs = Number.isFinite(Number(currentState.run?.lastRunAtMs))
            ? Number(currentState.run?.lastRunAtMs)
            : Number.isFinite(Number(currentState.updatedAtMs))
            ? Number(currentState.updatedAtMs)
            : 0;
        const heartbeatPersistDue = lastPersistAtMs <= 0 || nowMs - lastPersistAtMs >= SCALP_IDLE_HEARTBEAT_PERSIST_MS;
        const shouldPersistState = !persistableNoop || heartbeatPersistDue;
        const shouldPersistJournal = !persistableNoop;

        if (shouldPersistState) {
            await saveScalpSessionState(nextState, cfg.storage.sessionTtlSeconds, strategyId, {
                venue: deployment.venue,
                tuneId: deployment.tuneId,
                deploymentId: deployment.deploymentId,
            });
        }

        if (shouldPersistJournal) {
            await safeAppendJournal(
                journalEntry({
                    type: transition.transitioned ? 'state' : 'execution',
                    symbol: deployment.symbol,
                    dayKey,
                    reasonCodes,
                    payload: {
                        dryRun,
                        nowMs,
                        runId,
                        strategyId: deployment.strategyId,
                        tuneId: deployment.tuneId,
                        deploymentId: deployment.deploymentId,
                        state: nextState.state,
                        transitioned: transition.transitioned,
                        stateChanged,
                        tradeEventOccurred,
                        maxTradesPerDay: cfg.risk.maxTradesPerSymbolPerDay,
                        cooldownAfterLossMinutes: cfg.risk.cooldownAfterLossMinutes,
                        sessionClockMode: cfg.sessions.clockMode,
                        entrySessionProfile: cfg.sessions.entrySessionProfile,
                        asiaWindowLocal: cfg.sessions.asiaWindowLocal,
                        raidWindowLocal: cfg.sessions.raidWindowLocal,
                        asiaTf: cfg.timeframes.asiaBase,
                        confirmTf: cfg.timeframes.confirm,
                        windows,
                        marketSummary: market
                            ? {
                                  epic: market.epic,
                                  baseCandleCount: market.baseCandles.length,
                                  confirmCandleCount: market.confirmCandles.length,
                                  spreadPips: market.quote.spreadPips,
                              }
                            : null,
                        hasAsiaRange: Boolean(nextState.asiaRange),
                        hasSweep: Boolean(nextState.sweep),
                        hasConfirmation: Boolean(nextState.confirmation),
                        hasIfvg: Boolean(nextState.ifvg),
                        hasTrade: Boolean(nextState.trade),
                        tradeDryRun: nextState.trade?.dryRun ?? null,
                    },
                }),
                cfg.storage.journalMax,
            );
        }

        return {
            generatedAtMs: nowMs,
            venue: deployment.venue,
            symbol: deployment.symbol,
            strategyId: deployment.strategyId,
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
            dayKey,
            dryRun,
            runLockAcquired: true,
            state: nextState.state,
            reasonCodes,
        };
    } catch (err: any) {
        await safeAppendJournal(
            journalEntry({
                type: 'error',
                symbol,
                dayKey,
                level: 'error',
                reasonCodes: ['SCALP_EXECUTE_CYCLE_ERROR'],
                payload: {
                    dryRun,
                    nowMs,
                    runId,
                    strategyId: deployment.strategyId,
                    tuneId: deployment.tuneId,
                    deploymentId: deployment.deploymentId,
                    message: err?.message || String(err),
                },
            }),
            cfg.storage.journalMax,
        );
        throw err;
    } finally {
        await releaseScalpRunLock(symbol, runId, strategyId, {
            venue: deployment.venue,
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
        });
    }
}
