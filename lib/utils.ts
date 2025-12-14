// Evaluation persistence (KV only).

export type EvaluationRecord = Record<string, any>;

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';
const EVALUATION_INDEX_KEY = 'evaluation:index';
const EVALUATION_KEY_PREFIX = 'evaluation';

function ensureKvConfig() {
  if (!KV_REST_API_URL || !KV_REST_API_TOKEN) {
    throw new Error('Missing KV_REST_API_URL or KV_REST_API_TOKEN');
  }
}

function evalKey(symbol: string) {
  return `${EVALUATION_KEY_PREFIX}:${symbol.toUpperCase()}`;
}

function symbolFromKey(key: string) {
  const parts = key.split(':');
  return parts.length ? parts[parts.length - 1] : null;
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

async function kvZScore(key: string, member: string): Promise<number | null> {
  const res = await kvCommand('ZSCORE', key, member);
  const num = Number(res);
  return Number.isFinite(num) ? num : null;
}

async function kvZRem(key: string, member: string) {
  return kvCommand('ZREM', key, member);
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

// Save or overwrite the latest evaluation for a symbol.
export async function setEvaluation(symbol: string, evaluation: any) {
  if (!symbol) return;
  const key = evalKey(symbol);
  await Promise.all([
    kvSet(key, JSON.stringify(evaluation ?? {})),
    kvZAdd(EVALUATION_INDEX_KEY, Date.now(), key),
  ]);
}

// Fetch the latest evaluation for a symbol.
export async function getEvaluation(symbol: string) {
  if (!symbol) return null;
  const raw = await kvGet(evalKey(symbol));
  if (!raw) return null;
  return JSON.parse(raw);
}

export async function getEvaluationTimestamp(symbol: string): Promise<number | null> {
  if (!symbol) return null;
  const key = evalKey(symbol);
  try {
    return await kvZScore(EVALUATION_INDEX_KEY, key);
  } catch (err) {
    console.warn('Could not fetch evaluation timestamp from KV:', err);
    return null;
  }
}

// Snapshot of all evaluations (one per symbol).
export async function getAllEvaluations() {
  const keys = await kvZRevRange(EVALUATION_INDEX_KEY, 0, -1);
  const values = await kvMGet(keys);
  const result: EvaluationRecord = {};
  for (let i = 0; i < keys.length; i += 1) {
    const raw = values[i];
    if (!raw) continue;
    const symbol = symbolFromKey(keys[i]);
    if (!symbol) continue;
    try {
      result[symbol] = JSON.parse(raw);
    } catch (err) {
      console.warn('Skipping invalid evaluation entry from KV:', err);
    }
  }
  return result;
}

// Remove a symbol's evaluation.
export async function deleteEvaluation(symbol: string) {
  if (!symbol) return;
  const key = evalKey(symbol);
  await Promise.all([kvDel(key), kvZRem(EVALUATION_INDEX_KEY, key)]);
}
