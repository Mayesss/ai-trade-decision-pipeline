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
    const data = await res.json();
    if (!res.ok || data.error) {
        throw new Error(data.error || data.message || `KV command failed: ${command}`);
    }
    return data.result;
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

export async function kvGetJson<T>(key: string): Promise<T | null> {
    const raw = await kvGet(key);
    if (!raw) return null;
    try {
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
