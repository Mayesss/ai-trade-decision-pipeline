import { appendPlanLog } from './planLog';

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';

const PLAN_KEY_PREFIX = 'plan';
const PLAN_TTL_SECONDS = 24 * 60 * 60; // 24h

function kvConfigured() {
    return Boolean(KV_REST_API_URL && KV_REST_API_TOKEN);
}

function planKey(symbol: string) {
    return `${PLAN_KEY_PREFIX}:${symbol.toUpperCase()}`;
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
    if (!res.ok || data.error) {
        throw new Error(data.error || data.message || `KV command failed: ${command}`);
    }
    return data.result;
}

async function kvSetEx(key: string, ttlSeconds: number, value: string) {
    return kvCommand('SETEX', key, ttlSeconds, value);
}

async function kvGet(key: string): Promise<string | null> {
    return kvCommand('GET', key);
}

export type StoredPlan = {
    plan: any;
    savedAt: number;
    prompt?: { system: string; user: string };
};

export async function readPlan(symbol: string): Promise<StoredPlan | null> {
    if (!symbol) return null;
    if (!kvConfigured()) return null;
    try {
        const raw = await kvGet(planKey(symbol));
        if (!raw) return null;
        return JSON.parse(raw);
    } catch (err) {
        console.warn('Failed to read plan from KV:', err);
        return null;
    }
}

export async function savePlan(
    symbol: string,
    plan: any,
    prompt?: { system: string; user: string },
): Promise<{ persisted: boolean; error?: string }> {
    if (!symbol) return { persisted: false, error: 'symbol_missing' };
    if (!kvConfigured()) return { persisted: false, error: 'kv_not_configured' };
    try {
        const savedAt = Date.now();
        const payload = JSON.stringify({ plan, savedAt, prompt });
        await kvSetEx(planKey(symbol), PLAN_TTL_SECONDS, payload);
        await appendPlanLog({ symbol, timestamp: savedAt, plan, prompt: prompt ?? null });
        return { persisted: true };
    } catch (err) {
        console.warn('Failed to persist plan to KV:', err);
        return { persisted: false, error: err instanceof Error ? err.message : String(err) };
    }
}
