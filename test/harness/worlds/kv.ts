// WORLD BUILDER: Upstash KV REST — a tiny in-memory Redis.
//
// The repo talks to KV over TWO URL conventions, both POST:
//   lib/kv.ts                       POST <base>            body ["GET","key"]
//   lib/news|history|utils.ts       POST <base>/CMD/arg/…  no body
// Both land on the same in-memory store here.
//
// Response contract (Upstash): { result: ... } on success, { error: "..." }
// on failure. Unsupported commands answer a named 400 so the test fails
// loudly instead of silently pretending.

import { http, HttpResponse } from 'msw';

import type { RequestHandler } from 'msw';

// setup-contract.ts points upstash_payasyougo_KV_REST_API_URL here.
export const KV_TEST_URL = 'https://kv.boundary.test';

type KvValue = string;

interface KvStore {
    strings: Map<string, KvValue>;
    lists: Map<string, KvValue[]>;
    zsets: Map<string, Map<string, number>>;
}

function scoreBound(raw: string): number {
    const text = String(raw).trim().toLowerCase().replace(/^\(/, '');
    if (text === '+inf' || text === 'inf') return Infinity;
    if (text === '-inf') return -Infinity;
    return Number(text);
}

// Redis range indices are INCLUSIVE; negative counts from the end.
function sliceInclusive<T>(rows: T[], start: number, stop: number): T[] {
    const len = rows.length;
    const from = start < 0 ? Math.max(0, len + start) : start;
    const to = stop < 0 ? len + stop : Math.min(stop, len - 1);
    if (to < from) return [];
    return rows.slice(from, to + 1);
}

function globToRegExp(pattern: string): RegExp {
    const escaped = pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.');
    return new RegExp(`^${escaped}$`);
}

function zsetDescending(zset: Map<string, number>): Array<[string, number]> {
    return [...zset.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
}

function exec(store: KvStore, command: string, args: string[]): unknown {
    const cmd = command.toUpperCase();
    switch (cmd) {
        case 'GET':
            return store.strings.get(args[0]) ?? null;
        case 'SET':
            store.strings.set(args[0], args[1]);
            return 'OK';
        case 'SETEX':
            // TTL is irrelevant on the frozen test clock — store the value.
            store.strings.set(args[0], args[2]);
            return 'OK';
        case 'DEL': {
            let removed = 0;
            for (const key of args) {
                if (store.strings.delete(key)) removed += 1;
                if (store.lists.delete(key)) removed += 1;
                if (store.zsets.delete(key)) removed += 1;
            }
            return removed;
        }
        case 'INCR': {
            const next = (Number(store.strings.get(args[0])) || 0) + 1;
            store.strings.set(args[0], String(next));
            return next;
        }
        case 'EXPIRE':
            return 1;
        case 'MGET':
            return args.map((key) => store.strings.get(key) ?? null);
        case 'LPUSH': {
            const rows = store.lists.get(args[0]) ?? [];
            rows.unshift(...args.slice(1));
            store.lists.set(args[0], rows);
            return rows.length;
        }
        case 'LTRIM': {
            const rows = store.lists.get(args[0]) ?? [];
            store.lists.set(args[0], sliceInclusive(rows, Number(args[1]), Number(args[2])));
            return 'OK';
        }
        case 'LRANGE': {
            const rows = store.lists.get(args[0]) ?? [];
            return sliceInclusive(rows, Number(args[1]), Number(args[2]));
        }
        case 'SCAN': {
            const matchIdx = args.findIndex((arg) => arg.toUpperCase() === 'MATCH');
            const pattern = matchIdx >= 0 ? globToRegExp(args[matchIdx + 1]) : /^/;
            const keys = [...store.strings.keys(), ...store.lists.keys(), ...store.zsets.keys()].filter((key) =>
                pattern.test(key),
            );
            return ['0', keys];
        }
        case 'ZADD': {
            const zset = store.zsets.get(args[0]) ?? new Map<string, number>();
            const added = zset.has(args[2]) ? 0 : 1;
            zset.set(args[2], Number(args[1]));
            store.zsets.set(args[0], zset);
            return added;
        }
        case 'ZSCORE': {
            const score = store.zsets.get(args[0])?.get(args[1]);
            return score === undefined ? null : String(score);
        }
        case 'ZREM': {
            const zset = store.zsets.get(args[0]);
            return zset?.delete(args[1]) ? 1 : 0;
        }
        case 'ZREVRANGE': {
            const rows = zsetDescending(store.zsets.get(args[0]) ?? new Map());
            return sliceInclusive(rows, Number(args[1]), Number(args[2])).map(([member]) => member);
        }
        case 'ZREVRANGEBYSCORE': {
            const max = scoreBound(args[1]);
            const min = scoreBound(args[2]);
            return zsetDescending(store.zsets.get(args[0]) ?? new Map())
                .filter(([, score]) => score >= min && score <= max)
                .map(([member]) => member);
        }
        case 'ZREMRANGEBYSCORE': {
            const min = scoreBound(args[1]);
            const max = scoreBound(args[2]);
            const zset = store.zsets.get(args[0]) ?? new Map<string, number>();
            let removed = 0;
            for (const [member, score] of zset) {
                if (score >= min && score <= max) {
                    zset.delete(member);
                    removed += 1;
                }
            }
            return removed;
        }
        default:
            throw new Error(`kv-world: unsupported command ${cmd}`);
    }
}

function respond(store: KvStore, command: string, args: string[]): Response {
    try {
        return HttpResponse.json({ result: exec(store, command, args) });
    } catch (err) {
        return HttpResponse.json({ error: String((err as Error).message || err) }, { status: 400 });
    }
}

/**
 * A fresh in-memory KV per call — hand the result to startBoundary via a
 * world FACTORY so no state leaks between tests. `seed` preloads string keys.
 */
export function kvWorld(seed: Record<string, string> = {}): RequestHandler[] {
    const store: KvStore = {
        strings: new Map(Object.entries(seed)),
        lists: new Map(),
        zsets: new Map(),
    };

    return [
        // lib/kv.ts: JSON command array in the body
        http.post(KV_TEST_URL, async ({ request }) => {
            const body = (await request.json()) as unknown;
            if (!Array.isArray(body) || body.length === 0) {
                return HttpResponse.json({ error: 'kv-world: expected a command array body' }, { status: 400 });
            }
            const [command, ...args] = body.map((item) => String(item));
            return respond(store, command, args);
        }),
        // lib/news.ts / lib/history.ts / lib/utils.ts: /CMD/arg/arg/… in the path
        http.post(`${KV_TEST_URL}/*`, ({ request }) => {
            const segments = new URL(request.url).pathname
                .split('/')
                .filter(Boolean)
                .map((segment) => decodeURIComponent(segment));
            const [command, ...args] = segments;
            return respond(store, command, args);
        }),
    ];
}
