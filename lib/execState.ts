const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';

const EXEC_STATE_PREFIX = 'exec_state';
const EXEC_STATE_TTL_SECONDS = 3 * 24 * 60 * 60; // 3 days

type ExecState = {
    last_entry_ts?: number | null;
    last_exit_ts?: number | null;
    last_action?: string | null;
    last_plan_ts?: string | null;
};

function kvConfigured() {
    return Boolean(KV_REST_API_URL && KV_REST_API_TOKEN);
}

function key(symbol: string) {
    return `${EXEC_STATE_PREFIX}:${symbol.toUpperCase()}`;
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

async function kvSetEx(k: string, ttlSeconds: number, value: string) {
    return kvCommand('SETEX', k, ttlSeconds, value);
}

async function kvGet(k: string): Promise<string | null> {
    return kvCommand('GET', k);
}

export async function readExecState(symbol: string): Promise<ExecState> {
    if (!symbol || !kvConfigured()) return {};
    try {
        const raw = await kvGet(key(symbol));
        if (!raw) return {};
        return JSON.parse(raw);
    } catch (err) {
        console.warn('Failed to read exec state:', err);
        return {};
    }
}

export async function saveExecState(symbol: string, state: ExecState) {
    if (!symbol || !kvConfigured()) return { persisted: false, error: 'kv_not_configured' };
    try {
        await kvSetEx(key(symbol), EXEC_STATE_TTL_SECONDS, JSON.stringify(state));
        return { persisted: true };
    } catch (err) {
        console.warn('Failed to save exec state:', err);
        return { persisted: false, error: err instanceof Error ? err.message : String(err) };
    }
}

export type { ExecState };
