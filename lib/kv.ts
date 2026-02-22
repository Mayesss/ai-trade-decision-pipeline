const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';

function ensureKvConfig() {
    if (!KV_REST_API_URL || !KV_REST_API_TOKEN) {
        throw new Error('Missing KV_REST_API_URL or KV_REST_API_TOKEN');
    }
}

async function kvCommand(command: string, ...args: (string | number)[]) {
    ensureKvConfig();
    const encodedArgs = args
        .map((arg) => encodeURIComponent(typeof arg === 'string' ? arg : String(arg)))
        .join('/');
    const url = `${KV_REST_API_URL}/${command}${encodedArgs ? `/${encodedArgs}` : ''}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${KV_REST_API_TOKEN}`,
        },
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
