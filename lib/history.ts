import { PositionContext, MomentumSignals } from './ai';
import { TradeDecision } from './trading';

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';
const HISTORY_INDEX_KEY = 'decision:index';
const HISTORY_KEY_PREFIX = 'decision';

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

async function kvSet(key: string, value: string) {
    return kvCommand('SET', key, value);
}

async function kvGet(key: string): Promise<string | null> {
    return kvCommand('GET', key);
}

async function kvDel(key: string) {
    return kvCommand('DEL', key);
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

export type DecisionSnapshot = {
    price?: number;
    change24h?: number;
    obImb?: number;
    cvd?: number;
    spread?: number;
    gates?: any;
    metrics?: any;
    newsSentiment?: string | null;
    newsHeadlines?: string[];
    positionContext?: PositionContext | null;
    momentumSignals?: MomentumSignals;
};

export type DecisionHistoryEntry = {
    timestamp: number;
    symbol: string;
    timeFrame: string;
    dryRun: boolean;
    prompt: { system: string; user: string };
    aiDecision: TradeDecision & Record<string, any>;
    execResult: Record<string, any>;
    snapshot: DecisionSnapshot;
};

function keyFor(symbol: string, timestamp: number) {
    return `${HISTORY_KEY_PREFIX}:${timestamp}:${symbol.toUpperCase()}`;
}

export async function appendDecisionHistory(entry: DecisionHistoryEntry) {
    try {
        const key = keyFor(entry.symbol, entry.timestamp);
        await kvSet(key, JSON.stringify(entry));
        await kvZAdd(HISTORY_INDEX_KEY, entry.timestamp, key);
    } catch (err) {
        console.error('Failed to append decision history:', err);
    }
}

export async function loadDecisionHistory(symbol?: string, limit = 20): Promise<DecisionHistoryEntry[]> {
    ensureKvConfig();
    const upperSymbol = symbol?.toUpperCase();
    const batchSize = 50;
    let start = 0;
    const results: DecisionHistoryEntry[] = [];
    while (results.length < limit) {
        const keys = await kvZRevRange(HISTORY_INDEX_KEY, start, start + batchSize - 1);
        if (!keys.length) break;
        start += batchSize;
        const filteredKeys = upperSymbol ? keys.filter((k) => k.endsWith(`:${upperSymbol}`)) : keys;
        if (!filteredKeys.length) continue;
        const values = await kvMGet(filteredKeys);
        for (let i = 0; i < filteredKeys.length; i += 1) {
            const raw = values[i];
            if (!raw) continue;
            try {
                const parsed = JSON.parse(raw);
                if (upperSymbol && parsed.symbol?.toUpperCase() !== upperSymbol) continue;
                results.push(parsed);
                if (results.length >= limit) break;
            } catch (err) {
                console.warn('Skipping invalid history entry:', err);
            }
        }
    }
    return results;
}

export async function clearDecisionHistory() {
    ensureKvConfig();
    const allKeys = await kvZRevRange(HISTORY_INDEX_KEY, 0, -1);
    await Promise.all(allKeys.map((key) => kvDel(key)));
    await kvDel(HISTORY_INDEX_KEY);
}
