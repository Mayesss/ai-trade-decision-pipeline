// Postgres seam — plumbing only.
//
// The swing data layer (lib/swing/pg.ts) reaches the database through the
// memoized client on global.__pgClient (lib/db/client.ts). The harness plants
// a fake client there BEFORE any query runs, so neither the `pg` socket pool
// nor the Neon HTTP driver is ever constructed in tests.
//
// WHAT a query answers is decided by the world of the test (World.db) — nobody
// simulates a whole database, only the handful of queries the code under test
// issues. A world without a db responder makes every query throw, named.

import { recordEffect } from './recorder';

import type { PgClient, PgSqlObject, PgTxClient } from '../../lib/db/client';

/**
 * Answers one query. Return rows for reads; for writes ($executeRaw*) return
 * the affected-row count (an array counts as its length).
 */
export type PgResponder = (text: string, values: unknown[]) => Record<string, unknown>[] | number;

let responder: PgResponder | null = null;

function interpolate(strings: readonly string[], values: unknown[]): string {
    let text = String(strings[0] ?? '');
    for (let i = 0; i < values.length; i += 1) {
        text += `$${i + 1}${String(strings[i + 1] ?? '')}`;
    }
    return text;
}

function renderQuery(
    query: PgSqlObject | TemplateStringsArray | string,
    values: unknown[],
): { text: string; values: unknown[] } {
    if (typeof query === 'string') return { text: query, values };
    if (Array.isArray(query)) return { text: interpolate(query as readonly string[], values), values };
    const obj = query as PgSqlObject;
    const objValues = Array.isArray(obj.values) ? [...obj.values] : values;
    if (typeof obj.text === 'string') return { text: obj.text, values: objValues };
    if (typeof obj.sql === 'string') return { text: obj.sql, values: objValues };
    if (Array.isArray(obj.strings)) return { text: interpolate(obj.strings, objValues), values: objValues };
    return { text: String(query), values };
}

function run(query: PgSqlObject | TemplateStringsArray | string, values: unknown[]): Record<string, unknown>[] | number {
    const rendered = renderQuery(query, values);
    // Whitespace inside a SQL template is structure, not behavior — collapse it
    // so reformatting a query in lib/ does not churn snapshots.
    const text = rendered.text.replace(/\s+/g, ' ').trim();

    // Every query is an outgoing effect and belongs in the conversation — even
    // one the responder is about to reject.
    recordEffect('SQL', 'neon-postgres', rendered.values.length ? { text, values: rendered.values } : text);

    if (!responder) {
        throw new Error(`Postgres: this world has no db responder, but a query was issued: ${text}`);
    }
    return responder(text, rendered.values);
}

const executor: PgTxClient = {
    async $queryRaw<T = unknown>(query: PgSqlObject | TemplateStringsArray, ...values: unknown[]): Promise<T> {
        const out = run(query, values);
        return (Array.isArray(out) ? out : []) as T;
    },
    async $queryRawUnsafe<T = unknown>(query: string, ...values: unknown[]): Promise<T> {
        const out = run(query, values);
        return (Array.isArray(out) ? out : []) as T;
    },
    async $executeRaw(query: PgSqlObject | TemplateStringsArray, ...values: unknown[]): Promise<number> {
        const out = run(query, values);
        return Array.isArray(out) ? out.length : Number(out) || 0;
    },
    async $executeRawUnsafe(query: string, ...values: unknown[]): Promise<number> {
        const out = run(query, values);
        return Array.isArray(out) ? out.length : Number(out) || 0;
    },
};

const fakePgClient: PgClient = {
    ...executor,
    async $transaction<T>(fn: (tx: PgTxClient) => Promise<T>): Promise<T> {
        recordEffect('SQL', 'neon-postgres', 'BEGIN');
        try {
            const out = await fn(executor);
            recordEffect('SQL', 'neon-postgres', 'COMMIT');
            return out;
        } catch (err) {
            recordEffect('SQL', 'neon-postgres', 'ROLLBACK');
            throw err;
        }
    },
    async $disconnect(): Promise<void> {
        // nothing to close
    },
};

/** Set by the harness per test — worlds provide the responder. */
export function installFakePg(next: PgResponder | null): void {
    responder = next;
    global.__pgClient = fakePgClient;
}

export function uninstallFakePg(): void {
    responder = null;
    if (global.__pgClient === fakePgClient) {
        global.__pgClient = undefined;
    }
}
