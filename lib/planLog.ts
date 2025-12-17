const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';

const PLAN_LOG_INDEX = 'plan_log:index';
const PLAN_LOG_PREFIX = 'plan_log';
const PLAN_LOG_TTL_SECONDS = 7 * 24 * 60 * 60; // 7 days
const PLAN_LOG_MAX = 60;

export type PlanLogEntry = {
    symbol: string;
    timestamp: number;
    plan: any;
    prompt?: { system: string; user: string } | null;
};

function kvConfigured() {
    return Boolean(KV_REST_API_URL && KV_REST_API_TOKEN);
}

async function kvCommand(command: string, ...args: (string | number)[]) {
    if (!kvConfigured()) throw new Error('Missing KV_REST_API_URL or KV_REST_API_TOKEN');
    const encodedArgs = args
        .map((arg) => encodeURIComponent(typeof arg === 'string' ? arg : String(arg)))
        .join('/');
    const url = `${KV_REST_API_URL}/${command}${encodedArgs ? `/${encodedArgs}` : ''}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: { Authorization: `Bearer ${KV_REST_API_TOKEN}` },
    });
    const data = await res.json();
    if (!res.ok || data.error) throw new Error(data.error || data.message || `KV command failed: ${command}`);
    return data.result;
}

async function kvSetEx(key: string, ttlSeconds: number, value: string) {
    return kvCommand('SETEX', key, ttlSeconds, value);
}

async function kvGet(key: string): Promise<string | null> {
    return kvCommand('GET', key);
}

async function kvZAdd(key: string, score: number, member: string) {
    return kvCommand('ZADD', key, score, member);
}

async function kvZRevRange(key: string, start: number, stop: number): Promise<string[]> {
    const res = await kvCommand('ZREVRANGE', key, start, stop);
    return Array.isArray(res) ? res : [];
}

async function kvMGet(keys: string[]): Promise<(string | null)[]> {
    if (!keys.length) return [];
    const encoded = keys.map((k) => encodeURIComponent(k)).join('/');
    const url = `${KV_REST_API_URL}/MGET/${encoded}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: { Authorization: `Bearer ${KV_REST_API_TOKEN}` },
    });
    const data = await res.json();
    if (!res.ok || data.error) throw new Error(data.error || 'MGET failed');
    return Array.isArray(data.result) ? data.result : [];
}

async function kvZRem(key: string, member: string) {
    return kvCommand('ZREM', key, member);
}

async function kvDel(key: string) {
    return kvCommand('DEL', key);
}

async function kvZRemRangeByRank(key: string, start: number, stop: number) {
    return kvCommand('ZREMRANGEBYRANK', key, start, stop);
}

function entryKey(symbol: string, ts: number) {
    return `${PLAN_LOG_PREFIX}:${ts}:${symbol.toUpperCase()}`;
}

export async function appendPlanLog(entry: PlanLogEntry) {
    if (!kvConfigured()) return;
    const key = entryKey(entry.symbol, entry.timestamp);
    const payload = JSON.stringify(entry);
    await Promise.all([kvSetEx(key, PLAN_LOG_TTL_SECONDS, payload), kvZAdd(PLAN_LOG_INDEX, entry.timestamp, key)]);
    try {
        const countToRemove = -PLAN_LOG_MAX - 1;
        await kvZRemRangeByRank(PLAN_LOG_INDEX, 0, countToRemove);
    } catch {
        // ignore prune errors
    }
}

export async function loadPlanLogs(symbol: string, limit = 60): Promise<PlanLogEntry[]> {
    if (!kvConfigured()) return [];
    const upper = String(symbol || '').toUpperCase();
    const batchSize = 200;
    let start = 0;
    const matchedKeys: string[] = [];
    while (matchedKeys.length < limit) {
        const keys = await kvZRevRange(PLAN_LOG_INDEX, start, start + batchSize - 1);
        if (!keys.length) break;
        start += batchSize;
        const filtered = upper ? keys.filter((k) => k.endsWith(`:${upper}`)) : keys;
        matchedKeys.push(...filtered);
    }
    const slice = matchedKeys.slice(0, limit);
    const values = await kvMGet(slice);
    const parsed = values
        .map((raw) => {
            if (!raw) return null;
            try {
                return JSON.parse(raw) as PlanLogEntry;
            } catch {
                return null;
            }
        })
        .filter(Boolean) as PlanLogEntry[];
    return parsed;
}

export async function listPlanLogSymbols(limit = 200): Promise<string[]> {
    if (!kvConfigured()) return [];
    const stop = limit <= 0 ? -1 : limit - 1;
    const keys = await kvZRevRange(PLAN_LOG_INDEX, 0, stop);
    const symbols = new Set<string>();
    keys.forEach((k) => {
        const parts = String(k).split(':');
        const sym = parts[parts.length - 1];
        if (sym) symbols.add(sym.toUpperCase());
    });
    return Array.from(symbols).sort();
}

export async function clearPlanLogs(symbol?: string) {
    if (!kvConfigured()) return { cleared: false, error: 'kv_not_configured' as const };
    const allKeys = await kvZRevRange(PLAN_LOG_INDEX, 0, -1);
    const filtered = symbol ? allKeys.filter((k) => k.endsWith(`:${symbol.toUpperCase()}`)) : allKeys;
    await Promise.all(
        filtered.flatMap((key) => {
            if (symbol) return [kvDel(key), kvZRem(PLAN_LOG_INDEX, key)];
            return [kvDel(key)];
        }),
    );
    if (!symbol) {
        await kvDel(PLAN_LOG_INDEX);
    }
    return { cleared: true as const };
}
