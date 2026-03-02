import crypto from 'crypto';

import { applyScalpStrategyConfigOverride, getScalpStrategyConfig, normalizeScalpSymbol } from './config';
import type { ScalpStrategyConfigOverride } from './config';
import {
    buildAsiaRangeSnapshot,
    computeAtr,
    detectConfirmation,
    detectIfvg,
    detectIfvgTouch,
    detectSweepLifecycle,
} from './detectors';
import { buildScalpEntryPlan, executeScalpEntryPlan, reconcileScalpBrokerPosition } from './execution';
import { loadScalpMarketSnapshot, pipSizeForScalpSymbol } from './marketData';
import { buildScalpSessionWindows } from './sessions';
import { advanceScalpStateMachine, createInitialScalpSessionState, deriveScalpDayKey } from './stateMachine';
import { appendScalpJournal, loadScalpSessionState, releaseScalpRunLock, saveScalpSessionState, tryAcquireScalpRunLock } from './store';
import type {
    ScalpDirectionalBias,
    ScalpExecuteCycleResult,
    ScalpJournalEntry,
    ScalpMarketSnapshot,
    ScalpSessionState,
    ScalpSessionWindows,
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

function latestTs(candles: Array<[number, number, number, number, number, number]>): number | null {
    const ts = candles.at(-1)?.[0];
    return Number.isFinite(Number(ts)) ? Number(ts) : null;
}

function dedupeReasonCodes(codes: string[]): string[] {
    return Array.from(new Set(codes.map((code) => String(code || '').trim().toUpperCase()).filter((code) => code.length > 0)));
}

function withLastProcessed(state: ScalpSessionState, market: ScalpMarketSnapshot): ScalpSessionState {
    const next = {
        ...state,
        lastProcessed: {
            ...state.lastProcessed,
        },
    };
    const baseTs = latestTs(market.baseCandles);
    const confirmTs = latestTs(market.confirmCandles);
    if (market.baseTf === 'M1') next.lastProcessed.m1ClosedTsMs = baseTs;
    if (market.baseTf === 'M3') next.lastProcessed.m3ClosedTsMs = baseTs;
    if (market.baseTf === 'M5') next.lastProcessed.m5ClosedTsMs = baseTs;
    if (market.baseTf === 'M15') next.lastProcessed.m15ClosedTsMs = baseTs;

    if (market.confirmTf === 'M1') next.lastProcessed.m1ClosedTsMs = confirmTs;
    if (market.confirmTf === 'M3') next.lastProcessed.m3ClosedTsMs = confirmTs;
    return next;
}

function expectedDirectionFromSweep(state: ScalpSessionState): ScalpDirectionalBias | null {
    if (!state.sweep) return null;
    return state.sweep.side === 'BUY_SIDE' ? 'BEARISH' : 'BULLISH';
}

function applyPhase2Detectors(params: {
    state: ScalpSessionState;
    market: ScalpMarketSnapshot;
    windows: ScalpSessionWindows;
    nowMs: number;
    cfg: ReturnType<typeof getScalpStrategyConfig>;
}): { state: ScalpSessionState; reasonCodes: string[] } {
    const reasonCodes: string[] = [];
    let next = withLastProcessed(params.state, params.market);

    if (next.state === 'IN_TRADE' || next.state === 'COOLDOWN') {
        return { state: next, reasonCodes: ['STATE_SKIPPED_MANAGED_EXTERNALLY'] };
    }
    if (next.state === 'DONE') {
        return { state: next, reasonCodes: ['DAY_ALREADY_DONE'] };
    }

    if (!next.asiaRange) {
        const asia = buildAsiaRangeSnapshot({
            nowMs: params.nowMs,
            windows: params.windows,
            candles: params.market.baseCandles,
            minCandles: params.cfg.data.minAsiaCandles,
            sourceTf: params.market.baseTf,
        });
        reasonCodes.push(...asia.reasonCodes);
        if (asia.snapshot) {
            next.asiaRange = asia.snapshot;
            if (next.state === 'IDLE' || next.state === 'ASIA_RANGE_READY') {
                next.state = 'ASIA_RANGE_READY';
            }
        } else {
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
    }

    if (!next.sweep && params.nowMs > params.windows.raidEndMs && next.state === 'ASIA_RANGE_READY') {
        next.state = 'DONE';
        reasonCodes.push('RAID_WINDOW_CLOSED_NO_SWEEP');
        return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
    }

    if (next.state === 'IDLE') {
        next.state = 'ASIA_RANGE_READY';
    }

    if (next.state === 'ASIA_RANGE_READY' || next.state === 'SWEEP_DETECTED') {
        const atrBase = computeAtr(params.market.baseCandles, params.cfg.data.atrPeriod);
        const sweep = detectSweepLifecycle({
            existingSweep: next.sweep,
            candles: params.market.baseCandles,
            windows: params.windows,
            nowMs: params.nowMs,
            asiaHigh: next.asiaRange.high,
            asiaLow: next.asiaRange.low,
            atrAbs: atrBase,
            spreadAbs: params.market.quote.spreadAbs,
            pipSize: pipSizeForScalpSymbol(next.symbol),
            cfg: params.cfg.sweep,
        });
        reasonCodes.push(...sweep.reasonCodes);
        if (sweep.sweep) next.sweep = sweep.sweep;
        if (sweep.status === 'rejected') {
            next.state = 'CONFIRMING';
        } else if (sweep.status === 'pending') {
            next.state = 'SWEEP_DETECTED';
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        } else if (sweep.status === 'expired') {
            next.state = 'DONE';
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        } else if (sweep.status === 'none') {
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
    }

    if (next.state === 'CONFIRMING') {
        const rejectionTsMs = Number(next.sweep?.rejectedTsMs);
        const direction = expectedDirectionFromSweep(next);
        if (!(Number.isFinite(rejectionTsMs) && rejectionTsMs > 0 && direction)) {
            reasonCodes.push('CONFIRM_REQUIRES_REJECTED_SWEEP');
            next.state = 'DONE';
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }

        const confirmation = detectConfirmation({
            candles: params.market.confirmCandles,
            nowMs: params.nowMs,
            rejectionTsMs,
            pipSize: pipSizeForScalpSymbol(next.symbol),
            atrPeriod: params.cfg.data.atrPeriod,
            direction,
            cfg: params.cfg.confirm,
        });
        next.confirmation = confirmation.snapshot;
        reasonCodes.push(...confirmation.reasonCodes);
        if (confirmation.status === 'pending') {
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
        if (confirmation.status === 'expired') {
            next.state = 'DONE';
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }

        if (confirmation.status === 'confirmed' && confirmation.displacementTsMs && confirmation.structureShiftTsMs) {
            const ifvg = detectIfvg({
                candles: params.market.confirmCandles,
                direction,
                displacementTsMs: confirmation.displacementTsMs,
                structureShiftTsMs: confirmation.structureShiftTsMs,
                nowMs: params.nowMs,
                atrPeriod: params.cfg.data.atrPeriod,
                cfg: params.cfg.ifvg,
            });
            reasonCodes.push(...ifvg.reasonCodes);
            if (!ifvg.zone) {
                if (params.nowMs > rejectionTsMs + params.cfg.confirm.ttlMinutes * 60_000) {
                    next.state = 'DONE';
                    reasonCodes.push('IFVG_NOT_FOUND_BEFORE_CONFIRM_TTL');
                }
                return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
            }
            next.ifvg = ifvg.zone;
            next.state = 'WAITING_RETRACE';
        }
    }

    if (next.state === 'WAITING_RETRACE' && next.ifvg) {
        const touch = detectIfvgTouch({
            candles: params.market.confirmCandles,
            ifvg: next.ifvg,
            nowMs: params.nowMs,
        });
        reasonCodes.push(...touch.reasonCodes);
        if (touch.touched) {
            next.ifvg = {
                ...next.ifvg,
                touched: true,
            };
            next.state = 'WAITING_RETRACE';
            reasonCodes.push('ENTRY_SIGNAL_READY');
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
        if (touch.expired) {
            next.state = 'DONE';
            return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
        }
    }

    return { state: next, reasonCodes: dedupeReasonCodes(reasonCodes) };
}

export async function runScalpExecuteCycle(opts: {
    symbol?: string;
    dryRun?: boolean;
    nowMs?: number;
    configOverride?: ScalpStrategyConfigOverride;
} = {}): Promise<ScalpExecuteCycleResult> {
    const cfg = applyScalpStrategyConfigOverride(getScalpStrategyConfig(), opts.configOverride);
    const nowMs = Number.isFinite(opts.nowMs as number) ? Number(opts.nowMs) : Date.now();
    const dryRun = opts.dryRun ?? cfg.dryRunDefault;
    const symbol = normalizeScalpSymbol(opts.symbol || cfg.defaultSymbol);
    const dayKey = deriveScalpDayKey(nowMs, cfg.sessions.clockMode);
    const runId = crypto.randomUUID();
    const runLockAcquired = await tryAcquireScalpRunLock(symbol, runId, cfg.idempotency.runLockSeconds);

    if (!runLockAcquired) {
        const reasonCodes = ['SCALP_RUN_LOCK_ACTIVE'];
        await safeAppendJournal(
            journalEntry({
                type: 'execution',
                symbol,
                dayKey,
                level: 'warn',
                reasonCodes,
                payload: { dryRun, nowMs, runId },
            }),
            cfg.storage.journalMax,
        );
        return {
            generatedAtMs: nowMs,
            symbol,
            dayKey,
            dryRun,
            runLockAcquired: false,
            state: 'IDLE',
            reasonCodes,
        };
    }

    try {
        if (!cfg.enabled) {
            const disabledState = withRunContext(
                createInitialScalpSessionState({
                    symbol,
                    dayKey,
                    nowMs,
                    killSwitchActive: cfg.risk.killSwitch,
                }),
                {
                    nowMs,
                    runId,
                    dryRun,
                    reasonCodes: ['SCALP_ENGINE_DISABLED'],
                    killSwitch: cfg.risk.killSwitch,
                },
            );

            await saveScalpSessionState(disabledState, cfg.storage.sessionTtlSeconds);
            await safeAppendJournal(
                journalEntry({
                    type: 'execution',
                    symbol,
                    dayKey,
                    level: 'warn',
                    reasonCodes: ['SCALP_ENGINE_DISABLED'],
                    payload: { dryRun, nowMs, runId },
                }),
                cfg.storage.journalMax,
            );
            return {
                generatedAtMs: nowMs,
                symbol,
                dayKey,
                dryRun,
                runLockAcquired: true,
                state: disabledState.state,
                reasonCodes: ['SCALP_ENGINE_DISABLED'],
            };
        }

        const loadedState = await loadScalpSessionState(symbol, dayKey);
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
                const phase = applyPhase2Detectors({
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

                if (nextState.state === 'WAITING_RETRACE' && nextState.ifvg?.touched && !nextState.trade) {
                    const planRes = buildScalpEntryPlan({
                        state: nextState,
                        market,
                        cfg,
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

        await saveScalpSessionState(nextState, cfg.storage.sessionTtlSeconds);
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
                    message: err?.message || String(err),
                },
            }),
            cfg.storage.journalMax,
        );
        throw err;
    } finally {
        await releaseScalpRunLock(symbol, runId);
    }
}
