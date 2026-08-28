// The boundary harness — the only thing a contract test file must import.
//
// Ground rule of the safety net: tested is ONLY what the code does to the
// outside world. That is
//   - which HTTP requests go out, in which order, with which body
//     (for AI calls that includes the full outgoing PROMPT — the prompt
//     regression net)
//   - which SQL queries hit the swing database
//   - whether the call returns or throws
//
// NOT tested is HOW that comes about internally. That is why the net survives
// refactorings: moving modules, renaming functions, lint autofixes over
// lib/ai.ts — as long as the same conversation goes out, it stays green.

import { setupServer } from 'msw/node';
import { afterAll, afterEach, beforeAll, beforeEach, vi } from 'vitest';

import { installFakePg, uninstallFakePg } from './pg';
import { allEntries, recordPending, resetEntries } from './recorder';

import type { RequestHandler } from 'msw';

import type { PgResponder } from './pg';
import type { RecordedEntry } from './recorder';

// The frozen test clock. This repo computes with Date.now() everywhere
// (cooldowns, cache ages, signing timestamps), so contract tests run on a
// fixed clock: only Date is faked — timers stay real, because the retry and
// pacing sleeps in lib/kv.ts, lib/bitget.ts and lib/capital.ts must still
// elapse.
export const FIXED_NOW_MS = Date.UTC(2026, 7, 12, 10, 0, 0); // 2026-08-12T10:00:00Z — a Wednesday, markets open
export const FIXED_NOW_ISO = new Date(FIXED_NOW_MS).toISOString();

/**
 * The world of a code path — what it may talk to. What is not in here does
 * not exist for the test: any request to an endpoint outside the world is an
 * immediate, named failure (onUnhandledRequest: 'error').
 *
 * Pass a FACTORY when the world holds state (the in-memory KV store does) so
 * every test starts from a fresh copy.
 */
export interface World {
    /** HTTP routes: exactly the endpoints this code path talks to. */
    http?: RequestHandler[];
    /** Answers of the swing Postgres; without it, every query throws. */
    db?: PgResponder;
}

// --- Recording ---------------------------------------------------------------

// Headers that are part of the contract. Everything else (user-agent,
// x-stainless-*, accept-encoding, ...) is transport noise.
const CONTRACT_HEADERS = [
    'authorization',
    'content-type',
    'x-cap-api-key',
    'cst',
    'x-security-token',
    'access-key',
    'access-passphrase',
    'access-timestamp',
    'access-sign',
    'paptrading',
    'locale',
    'x-api-key',
    'anthropic-version',
    'x-admin-access-secret',
    'x-vercel-protection-bypass',
];

async function describeRequest(request: Request): Promise<RecordedEntry> {
    const headers: Record<string, string> = {};
    CONTRACT_HEADERS.forEach((name) => {
        const value = request.headers.get(name);
        if (value === null) return;
        // Tokens and signatures vary per run/body — only the schema is contract.
        if (name === 'authorization') {
            headers[name] = value.replace(/\s.*$/, ' <TOKEN>');
        } else if (name === 'access-sign') {
            headers[name] = '<SIGN>';
        } else {
            headers[name] = value;
        }
    });

    const text = await request.text();
    let body: unknown = text === '' ? undefined : text;
    try {
        body = JSON.parse(text) as unknown;
    } catch {
        // not JSON — keep the raw text
    }

    return {
        method: request.method,
        url: request.url,
        headers,
        body,
    };
}

// --- Normalization -----------------------------------------------------------

const VOLATILE = [
    // ISO timestamps
    [/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z/g, '<TIMESTAMP>'],
    // The frozen clock as epoch-ms — Date.now() leaks into bodies and URLs
    [new RegExp(`\\b${FIXED_NOW_MS}\\b`, 'g'), '<NOW_MS>'],
    // Marketaux authenticates via query param
    [/api_token=[^&"\s]+/g, 'api_token=<TOKEN>'],
] as const;

/**
 * Sort keys recursively so that merely reordering an object literal in
 * production code does not change the snapshot — that is structure, not
 * behavior. Array order stays: that IS behavior.
 */
function sortKeys(value: unknown): unknown {
    if (Array.isArray(value)) return value.map(sortKeys);
    if (value === null || typeof value !== 'object') return value;

    return Object.fromEntries(
        Object.entries(value as Record<string, unknown>)
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([key, entry]) => [key, sortKeys(entry)]),
    );
}

function normalize(text: string): string {
    return VOLATILE.reduce<string>((out, [pattern, replacement]) => out.replace(pattern, replacement), text);
}

function format(request: RecordedEntry): string {
    const lines = [`${request.method} ${request.url}`];

    Object.entries(request.headers)
        .sort(([a], [b]) => a.localeCompare(b))
        .forEach(([name, value]) => lines.push(`  ${name}: ${value}`));

    if (request.body !== undefined) {
        const body = typeof request.body === 'string' ? request.body : JSON.stringify(sortKeys(request.body), null, 2);
        lines.push(body.split('\n').map((line) => `  ${line}`).join('\n'));
    }

    return normalize(lines.join('\n'));
}

/**
 * The complete outgoing conversation as text — exactly what goes into the
 * snapshot. HTTP requests and SQL queries, in order, with full body.
 */
export async function conversation(): Promise<string> {
    const requests = await allEntries();
    return `${requests.map(format).join('\n\n')}\n`;
}

/** Just the head lines — for quick assertions without a snapshot. */
export async function conversationSummary(): Promise<string[]> {
    const requests = await allEntries();
    return requests.map((request) => `${request.method} ${request.url}`);
}

// --- Server lifecycle ----------------------------------------------------------

const server = setupServer();

server.events.on('request:start', ({ request }) => {
    recordPending(describeRequest(request.clone()));
});

/**
 * Call once at the top level of a test file. Registers the lifecycle hooks and
 * returns `use()` to swap responses for a single test.
 *
 *   const boundary = startBoundary(() => ({ http: [...kvWorld(), openAiDecides(HOLD)] }));
 *
 * Per-test env overrides go through vi.stubEnv() — the harness unstubs them
 * after every test. Only env read lazily can be stubbed this way; the
 * import-time-frozen vars live in test/harness/setup-*.ts.
 */
export function startBoundary(world: World | (() => World) = {}): {
    use: (...handlers: RequestHandler[]) => void;
} {
    const buildWorld = typeof world === 'function' ? world : () => world;

    beforeAll(() => {
        server.listen({ onUnhandledRequest: 'error' });
    });

    beforeEach(() => {
        vi.useFakeTimers({ now: FIXED_NOW_MS, toFake: ['Date'] });
        const built = buildWorld();
        // resetHandlers(...) also sets the baseline for this test
        server.resetHandlers(...(built.http ?? []));
        installFakePg(built.db ?? null);
        resetEntries();
    });

    afterEach(() => {
        vi.unstubAllEnvs();
        vi.useRealTimers();
        vi.clearAllMocks();
    });

    afterAll(() => {
        uninstallFakePg();
        server.close();
    });

    return {
        use: (...handlers: RequestHandler[]) => {
            server.use(...handlers);
        },
    };
}
