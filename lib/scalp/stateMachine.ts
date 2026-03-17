import type { ScalpClockMode, ScalpSessionState, ScalpStateMachineInput, ScalpStateMachineResult } from './types';
import { DEFAULT_SCALP_TUNE_ID, buildScalpDeploymentId } from './deployments';
import { getDefaultScalpStrategy } from './strategies/registry';
import { DEFAULT_SCALP_VENUE, normalizeScalpVenue } from './venue';

function dayKeyInTimeZone(tsMs: number, timeZone: string): string {
    const fmt = new Intl.DateTimeFormat('en-CA', {
        timeZone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
    });
    const parts = fmt.formatToParts(new Date(tsMs));
    const year = parts.find((p) => p.type === 'year')?.value || '1970';
    const month = parts.find((p) => p.type === 'month')?.value || '01';
    const day = parts.find((p) => p.type === 'day')?.value || '01';
    return `${year}-${month}-${day}`;
}

export function deriveScalpDayKey(nowMs: number, clockMode: ScalpClockMode): string {
    if (clockMode === 'UTC_FIXED') {
        return new Date(nowMs).toISOString().slice(0, 10);
    }
    return dayKeyInTimeZone(nowMs, 'Europe/London');
}

export function createInitialScalpSessionState(params: {
    venue?: string;
    symbol: string;
    strategyId?: string;
    tuneId?: string;
    deploymentId?: string;
    dayKey: string;
    nowMs: number;
    killSwitchActive?: boolean;
}): ScalpSessionState {
    const strategyId = String(params.strategyId || getDefaultScalpStrategy().id)
        .trim()
        .toLowerCase();
    const tuneId = String(params.tuneId || DEFAULT_SCALP_TUNE_ID)
        .trim()
        .toLowerCase();
    const venue = normalizeScalpVenue(params.venue, DEFAULT_SCALP_VENUE);
    return {
        version: 2,
        venue,
        symbol: params.symbol.toUpperCase(),
        strategyId,
        tuneId,
        deploymentId: String(params.deploymentId || buildScalpDeploymentId({ venue, symbol: params.symbol, strategyId, tuneId })),
        dayKey: params.dayKey,
        state: 'IDLE',
        createdAtMs: params.nowMs,
        updatedAtMs: params.nowMs,
        cooldownUntilMs: null,
        killSwitchActive: Boolean(params.killSwitchActive),
        asiaRange: null,
        sweep: null,
        confirmation: null,
        ifvg: null,
        trade: null,
        lastProcessed: {
            m1ClosedTsMs: null,
            m3ClosedTsMs: null,
            m5ClosedTsMs: null,
            m15ClosedTsMs: null,
        },
        stats: {
            tradesPlaced: 0,
            wins: 0,
            losses: 0,
            realizedR: 0,
            consecutiveLosses: 0,
            lastExitAtMs: null,
            lastTradeAtMs: null,
        },
        run: {
            lastRunAtMs: null,
            lastRunId: null,
            dryRunLast: true,
            lastReasonCodes: [],
        },
    };
}

function cloneState(state: ScalpSessionState): ScalpSessionState {
    return {
        ...state,
        lastProcessed: { ...state.lastProcessed },
        stats: { ...state.stats },
        run: { ...state.run, lastReasonCodes: state.run.lastReasonCodes.slice() },
        asiaRange: state.asiaRange ? { ...state.asiaRange } : null,
        sweep: state.sweep ? { ...state.sweep, reasonCodes: state.sweep.reasonCodes.slice() } : null,
        confirmation: state.confirmation ? { ...state.confirmation, reasonCodes: state.confirmation.reasonCodes.slice() } : null,
        ifvg: state.ifvg ? { ...state.ifvg } : null,
        trade: state.trade ? { ...state.trade } : null,
    };
}

export function advanceScalpStateMachine(
    state: ScalpSessionState,
    input: ScalpStateMachineInput,
): ScalpStateMachineResult {
    const next = cloneState(state);
    const reasonCodes: string[] = [];
    let transitioned = false;

    if (next.dayKey !== input.dayKey) {
        const reset = createInitialScalpSessionState({
            venue: next.venue,
            symbol: next.symbol,
            strategyId: next.strategyId,
            tuneId: next.tuneId,
            deploymentId: next.deploymentId,
            dayKey: input.dayKey,
            nowMs: input.nowMs,
            killSwitchActive: next.killSwitchActive,
        });
        return {
            nextState: reset,
            transitioned: true,
            reasonCodes: ['NEW_DAY_SESSION_RESET'],
        };
    }

    if (next.cooldownUntilMs && input.nowMs < next.cooldownUntilMs) {
        if (next.state !== 'COOLDOWN') {
            next.state = 'COOLDOWN';
            transitioned = true;
        }
        reasonCodes.push('COOLDOWN_ACTIVE');
    } else if (next.state === 'COOLDOWN') {
        next.cooldownUntilMs = null;
        next.state = 'IDLE';
        transitioned = true;
        reasonCodes.push('COOLDOWN_EXPIRED');
    }

    if (next.killSwitchActive) {
        reasonCodes.push('GLOBAL_KILL_SWITCH_ACTIVE');
    }

    if (reasonCodes.length === 0) {
        reasonCodes.push('NO_STATE_CHANGE');
    }

    next.updatedAtMs = input.nowMs;
    return {
        nextState: next,
        transitioned,
        reasonCodes,
    };
}
