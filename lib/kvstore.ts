import fs from 'fs/promises';
import path from 'path';
import type { TradeDecision } from './trading';

const MAX_ITEMS = 1000;
const DATA_PATH = path.join(process.cwd(), 'data', 'kvstore.json');

// ------------------------------
// Internal helpers
// ------------------------------

async function loadStore(): Promise<Record<string, any>> {
  try {
    const data = await fs.readFile(DATA_PATH, 'utf-8');
    return JSON.parse(data);
  } catch (err: any) {
    if (err.code === 'ENOENT') return {}; // file not found yet
    throw err;
  }
}

async function saveStore(store: Record<string, any>) {
  await fs.mkdir(path.dirname(DATA_PATH), { recursive: true });
  await fs.writeFile(DATA_PATH, JSON.stringify(store, null, 2), 'utf-8');
}

async function enforceLimit(store: Record<string, any>) {
  const keys = Object.keys(store);
  if (keys.length > MAX_ITEMS) {
    const sorted = keys.sort((a, b) => {
      const ta = Date.parse(a.split(':').slice(1).join(':'));
      const tb = Date.parse(b.split(':').slice(1).join(':'));
      return ta - tb;
    });
    const toDelete = keys.length - MAX_ITEMS;
    for (let i = 0; i < toDelete; i++) {
      delete store[sorted[i]!];
    }
  }
}

// ------------------------------
// Key iteration utilities
// ------------------------------

/** Async iterator over keys matching a glob pattern */
export async function* iterKeys(match: string, _count = 500) {
  const store = await loadStore();
  const regex = new RegExp('^' + match.replace('*', '.*') + '$');
  for (const key of Object.keys(store)) {
    if (regex.test(key)) yield key;
  }
}

/** Collect keys into an array */
export async function listKeysByPrefix(prefix: string): Promise<string[]> {
  const keys: string[] = [];
  for await (const k of iterKeys(`${prefix}*`)) keys.push(k);
  return keys;
}

/** Find the latest decision key for a given symbol */
export async function latestDecisionKey(symbol: string): Promise<string | null> {
  let latest: { key: string; ts: number } | null = null;
  for await (const k of iterKeys(`${symbol}:*`)) {
    const iso = k.split(':').slice(1).join(':');
    const ts = Date.parse(iso);
    if (Number.isFinite(ts)) {
      if (!latest || ts > latest.ts) latest = { key: k, ts };
    }
  }
  return latest?.key ?? null;
}

// ------------------------------
// Persistence helpers
// ------------------------------

/** Save a new trade decision (timestamped key: `${symbol}:<ISO>`) */
export async function saveDecision(_: any, keyBase: string, payload: any): Promise<string> {
  const store = await loadStore();
  const key = `${keyBase}:${new Date().toISOString()}`;
  store[key] = payload;
  await enforceLimit(store);
  await saveStore(store);
  return key;
}

/** Load the most recent saved decision for a symbol */
export async function loadLastDecision(_: any, symbol: string): Promise<TradeDecision | null> {
  const key = await latestDecisionKey(symbol);
  if (!key) return null;
  const store = await loadStore();
  return store[key] ?? null;
}

/** Delete everything in the store */
export async function clearAll(): Promise<void> {
  await fs.mkdir(path.dirname(DATA_PATH), { recursive: true });
  await fs.writeFile(DATA_PATH, '{}', 'utf-8');
}
