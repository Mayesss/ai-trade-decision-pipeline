import crypto from 'crypto';

import { fetchCapitalAccountEquityUsd } from '../capital';
import { applyScalpStrategyConfigOverride, getScalpStrategyConfig, normalizeScalpSymbol } from './config';
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
import { buildScalpSessionWindows } from './sessions';
import { getDefaultScalpStrategy, getScalpStrategyById, getScalpStrategyPreferredTimeframes } from './strategies/registry';
import { applySymbolGuardRiskDefaultsToStrategyConfig } from './strategies/guardDefaults';
import { advanceScalpStateMachine, createInitialScalpSessionState, deriveScalpDayKey } from './stateMachine';
import {
    appendScalpTradeLedgerEntry,
    appendScalpJournal,
    loadScalpSessionState,
    loadScalpStrategyRuntimeSnapshot,
    releaseScalpRunLock,
    saveScalpSessionState,
    tryAcquireScalpRunLock,
} from './store';
import type {
    ScalpExecuteCycleResult,
    ScalpJournalEntry,
    ScalpMarketSnapshot,
    ScalpSessionState,
    ScalpTradeLedgerEntry,
} from './types';

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

const SCALP_ENFORCED_RISK_PCT_OF_EQUITY = 5;

export async function runScalpExecuteCycle(opts: {
    symbol?: string;
    dryRun?: boolean;
    nowMs?: number;
    configOverride?: ScalpStrategyConfigOverride;
    strategyId?: string;
    tuneId?: string;
    deploymentId?: string;
    debug?: boolean;
    marketSnapshotCache?: Map<string, ScalpMarketSnapshot>;
} = {}): Promise<ScalpExecuteCycleResult> {
    const baseCfg = getScalpStrategyConfig();
    let cfg = applyScalpStrategyConfigOverride(baseCfg, opts.configOverride);
    const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
    const dryRun = opts.dryRun ?? cfg.dryRunDefault;
    const symbol = normalizeScalpSymbol(opts.symbol || cfg.defaultSymbol);
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const runId = crypto.randomUUID();
    const debug = Boolean(opts.debug);
    const runtime = await loadScalpStrategyRuntimeSnapshot(cfg.enabled, opts.strategyId);
    const strategyControl = runtime.strategy;
    const strategyId = strategyControl.strategyId;
    const deployment = resolveScalpDeployment({
        symbol,
        strategyId,
        tuneId: opts.tuneId,
        deploymentId: opts.deploymentId,
    });
    const strategyDef =
        getScalpStrategyById(deployment.strategyId) ||
        getScalpStrategyById(runtime.defaultStrategyId) ||
        getDefaultScalpStrategy();
    const preferredTimeframes = getScalpStrategyPreferredTimeframes(strategyDef.id);
    cfg = applySymbolGuardRiskDefaultsToStrategyConfig({ cfg, symbol: deployment.symbol, strategyId: deployment.strategyId });
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
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
        });
        const currentState =
            loadedState ||
            createInitialScalpSessionState({
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

        let nextState = transition.nextState;
        let market: ScalpMarketSnapshot | null = null;
        const phaseReasonCodes: string[] = [];

        if (!cfg.risk.killSwitch) {
            try {
                const snapshotCacheKey = [
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
                    state: nextState,
                    market,
                    dryRun,
                    maxOpenPositionsPerSymbol: cfg.risk.maxOpenPositionsPerSymbol,
                });
                nextState = reconciled.state;
                phaseReasonCodes.push(...reconciled.reasonCodes);
                const hadOpenTradeAtStartOfManage = Boolean(nextState.trade);
                const tradeBeforeManage = hadOpenTradeAtStartOfManage && nextState.trade ? { ...nextState.trade } : null;
                const priorRealizedR = Number.isFinite(Number(nextState.stats.realizedR)) ? Number(nextState.stats.realizedR) : 0;

                const managed = await manageScalpOpenTrade({
                    state: nextState,
                    market,
                    cfg,
                    dryRun,
                    nowMs,
                });
                nextState = managed.state;
                phaseReasonCodes.push(...managed.reasonCodes);
                if (hadOpenTradeAtStartOfManage && tradeBeforeManage && !nextState.trade) {
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

                if (!hadOpenTradeAtStartOfManage && !nextState.trade && entryIntent && nextState.state !== 'DONE' && nextState.state !== 'COOLDOWN') {
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
                            const liveEquityUsd = await fetchCapitalAccountEquityUsd();
                            if (Number.isFinite(liveEquityUsd as number) && Number(liveEquityUsd) > 0) {
                                entryCfg = {
                                    ...entryCfg,
                                    risk: {
                                        ...entryCfg.risk,
                                        referenceEquityUsd: Number(liveEquityUsd),
                                    },
                                };
                                phaseReasonCodes.push('ENTRY_RISK_5PCT_LIVE_EQUITY');
                                canPlanEntry = true;
                            } else {
                                phaseReasonCodes.push('ENTRY_BLOCKED_LIVE_EQUITY_UNAVAILABLE');
                            }
                        } catch {
                            phaseReasonCodes.push('ENTRY_BLOCKED_LIVE_EQUITY_UNAVAILABLE');
                        }
                    } else {
                        phaseReasonCodes.push('ENTRY_RISK_5PCT_REFERENCE_EQUITY');
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
        nextState = withRunContext(nextState, {
            nowMs,
            runId,
            dryRun,
            reasonCodes,
            killSwitch: cfg.risk.killSwitch,
        });

        await saveScalpSessionState(nextState, cfg.storage.sessionTtlSeconds, strategyId, {
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
        });
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
                    maxTradesPerDay: cfg.risk.maxTradesPerSymbolPerDay,
                    cooldownAfterLossMinutes: cfg.risk.cooldownAfterLossMinutes,
                    sessionClockMode: cfg.sessions.clockMode,
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

        return {
            generatedAtMs: nowMs,
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
            tuneId: deployment.tuneId,
            deploymentId: deployment.deploymentId,
        });
    }
}
