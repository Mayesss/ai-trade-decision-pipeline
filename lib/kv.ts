const upstash_payasyougo_KV_REST_API_URL = (process.env.upstash_payasyougo_KV_REST_API_URL || '').replace(/\/$/, '');
const upstash_payasyougo_KV_REST_API_TOKEN = process.env.upstash_payasyougo_KV_REST_API_TOKEN || '';

function ensureKvConfig() {
    if (!upstash_payasyougo_KV_REST_API_URL || !upstash_payasyougo_KV_REST_API_TOKEN) {
        throw new Error('Missing upstash_payasyougo_KV_REST_API_URL or upstash_payasyougo_KV_REST_API_TOKEN');
    }
}

async function kvCommand(command: string, ...args: (string | number)[]) {
    ensureKvConfig();
    // Use JSON command bodies to avoid URL-size limits on larger payloads
    // (for example candle-history snapshots stored in KV).
    const url = upstash_payasyougo_KV_REST_API_URL;
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${upstash_payasyougo_KV_REST_API_TOKEN}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify([command, ...args]),
    });

    const rawText = await res.text();
    let data: any = null;
    if (rawText) {
        try {
            data = JSON.parse(rawText);
        } catch (err: any) {
            if (!res.ok) {
                throw new Error(`KV command failed (${command}) HTTP ${res.status}: ${rawText.slice(0, 240)}`);
            }
            console.warn(`KV ${command} response parse warning: ${err?.message || String(err)}`);
            return null;
        }
    }

    if (!res.ok) {
        const message =
            (data && typeof data === 'object' && (data.error || data.message)) ||
            rawText ||
            `KV command failed: ${command}`;
        throw new Error(String(message));
    }

    if (data && typeof data === 'object' && data.error) {
        throw new Error(String(data.error || data.message || `KV command failed: ${command}`));
    }

    if (data && typeof data === 'object' && 'result' in data) {
        return data.result;
    }
    return null;
}

async function kvGet(key: string): Promise<string | null> {
    return kvCommand('GET', key);
}

async function kvSet(key: string, value: string) {
    return kvCommand('SET', key, value);
}

async function kvSetEx(key: string, ttlSeconds: number, value: string) {
    return kvCommand('SETEX', key, ttlSeconds, value);
}

async function kvLPush(key: string, value: string) {
    return kvCommand('LPUSH', key, value);
}

async function kvLTrim(key: string, start: number, stop: number) {
    return kvCommand('LTRIM', key, start, stop);
}

async function kvLRange(key: string, start: number, stop: number) {
    return kvCommand('LRANGE', key, start, stop);
}

async function kvScan(cursor: string, match?: string, count?: number) {
    const args: (string | number)[] = [cursor];
    if (match) {
        args.push('MATCH', match);
    }
    if (Number.isFinite(Number(count)) && Number(count) > 0) {
        args.push('COUNT', Math.floor(Number(count)));
    }
    return kvCommand('SCAN', ...args);
}

export async function kvGetJson<T>(key: string): Promise<T | null> {
    try {
        const raw = await kvGet(key);
        if (raw === null || raw === undefined) return null;
        if (typeof raw !== 'string') return raw as T;
        if (!raw.trim()) return null;
        return JSON.parse(raw) as T;
    } catch {
        return null;
    }
}

export async function kvSetJson<T>(key: string, value: T, ttlSeconds?: number) {
    const payload = JSON.stringify(value);
    if (ttlSeconds && ttlSeconds > 0) {
        return kvSetEx(key, ttlSeconds, payload);
    }
    return kvSet(key, payload);
}

export async function kvListPushJson<T>(key: string, value: T) {
    const payload = JSON.stringify(value);
    return kvLPush(key, payload);
}

export async function kvListTrim(key: string, start: number, stop: number) {
    return kvLTrim(key, start, stop);
}

export async function kvListRangeJson<T>(key: string, start: number, stop: number): Promise<T[]> {
    try {
        const raw = await kvLRange(key, start, stop);
        if (!Array.isArray(raw)) return [];
        const rows: T[] = [];
        for (const item of raw) {
            if (item === null || item === undefined) continue;
            if (typeof item === 'string') {
                const trimmed = item.trim();
                if (!trimmed) continue;
                try {
                    rows.push(JSON.parse(trimmed) as T);
                } catch {
                    continue;
                }
                continue;
            }
            rows.push(item as T);
        }
        return rows;
    } catch {
        return [];
    }
}

function parseKvScanResult(raw: unknown): { cursor: string; keys: string[] } {
    if (Array.isArray(raw) && raw.length >= 2) {
        const cursor = String(raw[0] ?? '0');
        const keyRows = Array.isArray(raw[1]) ? raw[1] : [];
        const keys = keyRows.map((row) => String(row || '').trim()).filter((row) => Boolean(row));
        return { cursor, keys };
    }

    if (raw && typeof raw === 'object') {
        const row = raw as Record<string, unknown>;
        const cursor = String(row.cursor ?? row.nextCursor ?? '0');
        const keyRows = Array.isArray(row.keys) ? row.keys : Array.isArray(row.result) ? row.result : [];
        const keys = keyRows.map((item) => String(item || '').trim()).filter((item) => Boolean(item));
        return { cursor, keys };
    }

    return { cursor: '0', keys: [] };
}

export async function kvCollectKeys(
    match: string,
    opts: { count?: number; maxIterations?: number; maxKeys?: number } = {},
): Promise<string[]> {
    const count = Math.max(10, Math.min(5000, Math.floor(Number(opts.count) || 250)));
    const maxIterations = Math.max(1, Math.min(10_000, Math.floor(Number(opts.maxIterations) || 200)));
    const maxKeys = Math.max(1, Math.min(200_000, Math.floor(Number(opts.maxKeys) || 50_000)));

    const out = new Set<string>();
    let cursor = '0';
    let iteration = 0;

    while (iteration < maxIterations && out.size < maxKeys) {
        iteration += 1;
        const raw = await kvScan(cursor, match, count);
        const parsed = parseKvScanResult(raw);
        for (const key of parsed.keys) {
            out.add(key);
            if (out.size >= maxKeys) break;
        }
        cursor = parsed.cursor || '0';
        if (cursor === '0') break;
    }

    return Array.from(out);
}
