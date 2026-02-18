import { PositionContext, MomentumSignals } from './ai';
import { TradeDecision } from './trading';

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';
const HISTORY_INDEX_KEY = 'decision:index';
const HISTORY_KEY_PREFIX = 'decision';
const HISTORY_TTL_SECONDS = 7 * 24 * 60 * 60; // 7 days

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

// Set with TTL (seconds)
async function kvSetEx(key: string, ttlSeconds: number, value: string) {
    return kvCommand('SETEX', key, ttlSeconds, value);
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

// Remove members by score range
async function kvZRemRangeByScore(key: string, minScore: number, maxScore: number) {
    return kvCommand('ZREMRANGEBYSCORE', key, minScore, maxScore);
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
    spread?: number; // canonical bps (legacy alias)
    spreadBps?: number;
    spreadAbs?: number;
    bestBid?: number;
    bestAsk?: number;
    gates?: any;
    metrics?: any;
    newsSentiment?: string | null;
    newsHeadlines?: string[];
    positionContext?: PositionContext | null;
    momentumSignals?: MomentumSignals;
    platform?: string;
    newsSource?: string;
    instrumentId?: string;
};

export type DecisionHistoryEntry = {
    timestamp: number;
    symbol: string;
    platform?: string;
    instrumentId?: string;
    newsSource?: string;
    timeFrame: string;
    dryRun: boolean;
    prompt: { system: string; user: string };
    aiDecision: TradeDecision & Record<string, any>;
    execResult: Record<string, any>;
    snapshot: DecisionSnapshot;
    biasTimeframes?: {
        context?: string;
        macro?: string;
        primary?: string;
        micro?: string;
    };
};

function normalizeHistoryPlatform(value?: string) {
    const raw = String(value || '').trim().toLowerCase();
    return raw || 'bitget';
}

function parseHistoryKey(key: string): { symbol: string | null; platform: string | null } {
    const parts = String(key).split(':');
    if (parts.length >= 4) {
        const symbol = parts[parts.length - 1] || null;
        const platform = parts[parts.length - 2] || null;
        return { symbol: symbol ? symbol.toUpperCase() : null, platform: platform ? platform.toLowerCase() : null };
    }
    if (parts.length >= 3) {
        const symbol = parts[parts.length - 1] || null;
        return { symbol: symbol ? symbol.toUpperCase() : null, platform: 'bitget' };
    }
    return { symbol: null, platform: null };
}

function keyFor(symbol: string, timestamp: number, platform?: string) {
    const normalizedPlatform = normalizeHistoryPlatform(platform);
    return `${HISTORY_KEY_PREFIX}:${timestamp}:${normalizedPlatform}:${symbol.toUpperCase()}`;
}

export async function appendDecisionHistory(entry: DecisionHistoryEntry) {
    try {
        const key = keyFor(entry.symbol, entry.timestamp, entry.platform);
        // store entry with TTL
        await kvSetEx(key, HISTORY_TTL_SECONDS, JSON.stringify(entry));
        // add to index
        await kvZAdd(HISTORY_INDEX_KEY, entry.timestamp, key);

        // prune index entries older than TTL (by timestamp score)
        const cutoff = Date.now() - HISTORY_TTL_SECONDS * 1000;
        await kvZRemRangeByScore(HISTORY_INDEX_KEY, 0, cutoff);
    } catch (err) {
        console.error('Failed to append decision history:', err);
    }
}

export async function loadDecisionHistory(symbol?: string, limit = 20, platform?: string): Promise<DecisionHistoryEntry[]> {
    ensureKvConfig();
    const upperSymbol = symbol?.toUpperCase();
    const normalizedPlatform = platform ? normalizeHistoryPlatform(platform) : undefined;
    const batchSize = 50;
    let start = 0;
    const results: DecisionHistoryEntry[] = [];
    while (results.length < limit) {
        const keys = await kvZRevRange(HISTORY_INDEX_KEY, start, start + batchSize - 1);
        if (!keys.length) break;
        start += batchSize;
        const filteredKeys = keys.filter((k) => {
            const parsed = parseHistoryKey(k);
            if (upperSymbol && parsed.symbol !== upperSymbol) return false;
            if (normalizedPlatform && parsed.platform !== normalizedPlatform) return false;
            return true;
        });
        if (!filteredKeys.length) continue;
        const values = await kvMGet(filteredKeys);
        for (let i = 0; i < filteredKeys.length; i += 1) {
            const raw = values[i];
            if (!raw) continue;
            try {
                const parsed = JSON.parse(raw);
                if (upperSymbol && parsed.symbol?.toUpperCase() !== upperSymbol) continue;
                if (normalizedPlatform && normalizeHistoryPlatform(parsed.platform) !== normalizedPlatform) continue;
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

export async function listHistorySymbols(): Promise<string[]> {
    ensureKvConfig();
    const keys = await kvZRevRange(HISTORY_INDEX_KEY, 0, -1);
    const symbols = new Set<string>();
    for (const key of keys) {
        const parsed = parseHistoryKey(key);
        if (parsed.symbol) symbols.add(parsed.symbol.toUpperCase());
    }
    return Array.from(symbols);
}
