import {
    appendScalpJournal,
    appendScalpTradeLedgerEntry,
    loadScalpSessionState,
    loadScalpStrategyRuntimeSnapshot,
    releaseScalpRunLock,
    saveScalpSessionState,
    tryAcquireScalpRunLock,
    type ScalpDeploymentKeyOptions,
    type ScalpStrategyRuntimeSnapshot,
} from './store';
import type {
    ScalpJournalEntry,
    ScalpSessionState,
    ScalpTradeLedgerAppendResult,
    ScalpTradeLedgerEntry,
} from './types';

export interface ScalpExecutionPersistenceAdapter {
    loadRuntimeSnapshot: (
        envEnabled: boolean,
        preferredStrategyId?: string,
    ) => Promise<ScalpStrategyRuntimeSnapshot>;
    loadSessionState: (
        symbol: string,
        dayKey: string,
        strategyId?: string,
        opts?: ScalpDeploymentKeyOptions,
    ) => Promise<ScalpSessionState | null>;
    saveSessionState: (
        state: ScalpSessionState,
        ttlSeconds?: number,
        strategyId?: string,
        opts?: ScalpDeploymentKeyOptions,
    ) => Promise<void>;
    appendJournal: (
        entry: ScalpJournalEntry,
        maxRows?: number,
    ) => Promise<void>;
    appendTradeLedgerEntry: (
        entry: ScalpTradeLedgerEntry,
        maxRows?: number,
    ) => Promise<ScalpTradeLedgerAppendResult>;
    tryAcquireRunLock: (
        symbol: string,
        token: string,
        ttlSeconds: number,
        strategyId?: string,
        opts?: ScalpDeploymentKeyOptions,
    ) => Promise<boolean>;
    releaseRunLock: (
        symbol: string,
        token: string,
        strategyId?: string,
        opts?: ScalpDeploymentKeyOptions,
    ) => Promise<void>;
}

export const defaultScalpExecutionPersistenceAdapter: ScalpExecutionPersistenceAdapter =
    {
        loadRuntimeSnapshot: loadScalpStrategyRuntimeSnapshot,
        loadSessionState: loadScalpSessionState,
        saveSessionState: saveScalpSessionState,
        appendJournal: appendScalpJournal,
        appendTradeLedgerEntry: async (entry, maxRows) => {
            await appendScalpTradeLedgerEntry(entry, maxRows);
            return { ok: true, reasonCodes: ['LEDGER_WRITE_CONFIRMED'] };
        },
        tryAcquireRunLock: tryAcquireScalpRunLock,
        releaseRunLock: releaseScalpRunLock,
    };
