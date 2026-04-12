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
    ) => Promise<void>;
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
        appendTradeLedgerEntry: appendScalpTradeLedgerEntry,
        tryAcquireRunLock: tryAcquireScalpRunLock,
        releaseRunLock: releaseScalpRunLock,
    };
