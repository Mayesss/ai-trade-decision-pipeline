import crypto from 'crypto';

import { applyScalpStrategyConfigOverride, getScalpStrategyConfig, normalizeScalpSymbol } from './config';
import type { ScalpStrategyConfigOverride } from './config';
import {
    buildScalpEntryPlan,
    executeScalpEntryPlan,
    manageScalpOpenTrade,
    reconcileScalpBrokerPosition,
    resolveLegacyIfvgEntryIntent,
} from './execution';
import { loadScalpMarketSnapshot } from './marketData';
import { buildScalpSessionWindows } from './sessions';
import { getDefaultScalpStrategy, getScalpStrategyById } from './strategies/registry';
import { applyXauusdGuardRiskDefaultsToStrategyConfig } from './strategies/regimePullbackM15M3XauusdGuarded';
import { advanceScalpStateMachine, createInitialScalpSessionState, deriveScalpDayKey } from './stateMachine';
import {
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

export async function runScalpExecuteCycle(opts: {
    symbol?: string;
    dryRun?: boolean;
    nowMs?: number;
    configOverride?: ScalpStrategyConfigOverride;
    strategyId?: string;
} = {}): Promise<ScalpExecuteCycleResult> {
    const baseCfg = getScalpStrategyConfig();
    let cfg = applyScalpStrategyConfigOverride(baseCfg, opts.configOverride);
    const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
    const dryRun = opts.dryRun ?? cfg.dryRunDefault;
    const symbol = normalizeScalpSymbol(opts.symbol || cfg.defaultSymbol);
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const runId = crypto.randomUUID();
    const runtime = await loadScalpStrategyRuntimeSnapshot(cfg.enabled, opts.strategyId);
    const strategyControl = runtime.strategy;
    const strategyId = strategyControl.strategyId;
    const strategyDef =
        getScalpStrategyById(strategyId) ||
        getScalpStrategyById(runtime.defaultStrategyId) ||
        getDefaultScalpStrategy();
    cfg = applyXauusdGuardRiskDefaultsToStrategyConfig({ cfg, symbol, strategyId });
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
                    strategyId,
                    strategyEnabled: strategyControl.enabled,
                    strategyEnvEnabled: strategyControl.envEnabled,
                    strategyKvEnabled: strategyControl.kvEnabled,
                },
            }),
            cfg.storage.journalMax,
        );
        return {
            generatedAtMs: nowMs,
            symbol,
            strategyId,
            dayKey,
            dryRun,
            runLockAcquired: false,
            state: 'IDLE',
            reasonCodes,
        };
    }

    const runLockAcquired = await tryAcquireScalpRunLock(symbol, runId, cfg.idempotency.runLockSeconds, strategyId);

    if (!runLockAcquired) {
        const reasonCodes = ['SCALP_RUN_LOCK_ACTIVE'];
        await safeAppendJournal(
            journalEntry({
                type: 'execution',
                symbol,
                dayKey,
                level: 'warn',
                reasonCodes,
                payload: { dryRun, nowMs, runId, strategyId },
            }),
            cfg.storage.journalMax,
        );
        return {
            generatedAtMs: nowMs,
            symbol,
            strategyId,
            dayKey,
            dryRun,
            runLockAcquired: false,
            state: 'IDLE',
            reasonCodes,
        };
    }

    try {
        const loadedState = await loadScalpSessionState(symbol, dayKey, strategyId);
        const currentState =
            loadedState ||
            createInitialScalpSessionState({
                symbol,
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
                const phase = strategyDef.applyPhaseDetectors({
                    state: nextState,
                    market,
                    windows,
                    nowMs,
                    cfg,
                });
                nextState = phase.state;
                phaseReasonCodes.push(...phase.reasonCodes);

                const reconciled = await reconcileScalpBrokerPosition({
                    state: nextState,
                    market,
                    dryRun,
                    maxOpenPositionsPerSymbol: cfg.risk.maxOpenPositionsPerSymbol,
                });
                nextState = reconciled.state;
                phaseReasonCodes.push(...reconciled.reasonCodes);
                const hadOpenTradeAtStartOfManage = Boolean(nextState.trade);

                const managed = await manageScalpOpenTrade({
                    state: nextState,
                    market,
                    cfg,
                    dryRun,
                    nowMs,
                });
                nextState = managed.state;
                phaseReasonCodes.push(...managed.reasonCodes);

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
                    const planRes = buildScalpEntryPlan({
                        state: nextState,
                        market,
                        cfg,
                        entryIntent,
                    });
                    phaseReasonCodes.push(...planRes.reasonCodes);
                    if (planRes.plan) {
                        const entryRes = await executeScalpEntryPlan({
                            state: nextState,
                            plan: planRes.plan,
                            cfg,
                            dryRun,
                            nowMs,
                        });
                        nextState = entryRes.state;
                        phaseReasonCodes.push(...entryRes.reasonCodes);
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
                            strategyId,
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

        await saveScalpSessionState(nextState, cfg.storage.sessionTtlSeconds, strategyId);
        await safeAppendJournal(
            journalEntry({
                type: transition.transitioned ? 'state' : 'execution',
                symbol,
                dayKey,
                reasonCodes,
                payload: {
                    dryRun,
                    nowMs,
                    runId,
                    strategyId,
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
            symbol,
            strategyId,
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
                    strategyId,
                    message: err?.message || String(err),
                },
            }),
            cfg.storage.journalMax,
        );
        throw err;
    } finally {
        await releaseScalpRunLock(symbol, runId, strategyId);
    }
}
