import { PositionContext, MomentumSignals } from './ai';
import { TradeDecision } from './trading';
import type { CapturedLeverage } from './analytics';
import { upsertSwingDecision } from './swing/pg';

const upstash_payasyougo_KV_REST_API_URL = (process.env.upstash_payasyougo_KV_REST_API_URL || '').replace(/\/$/, '');
const upstash_payasyougo_KV_REST_API_TOKEN = process.env.upstash_payasyougo_KV_REST_API_TOKEN || '';
const HISTORY_INDEX_KEY = 'decision:index';
const HISTORY_KEY_PREFIX = 'decision';
const HISTORY_TTL_SECONDS = 7 * 24 * 60 * 60; // 7 days

// Per-symbol marker index: a small sorted set (score = timestamp, member = the
// main entry key) holding ONLY entry/exit decisions (BUY/SELL/CLOSE) — the ones
// that draw a chart arrow. The chart endpoint reads this instead of scanning the
// global decision index for one symbol (which pulled up to ~1200 entries across
// dozens of KV round-trips). Stays entirely on KV — no Neon transfer.
const MARKER_INDEX_PREFIX = 'decision:markers';
// One-shot flag marking that a symbol's marker index has been backfilled from the
// legacy global index, so the slow fallback scan runs at most once per symbol.
const MARKER_SEEDED_PREFIX = 'decision:markers:seeded';
// Per-symbol FULL decision index (every row, not just markers): symbol-filtered
// loadDecisionHistory reads used to page the GLOBAL index 50 keys at a time to
// find one symbol's rows — ~120 KV commands per symbol at 25 symbols (~1-in-25
// key density). At 25 symbols × ~96 dashboard-summary rebuilds/day that alone
// was ~290k commands/day (≈ the whole Upstash PAYG budget). This index makes a
// symbol read 1 ZREVRANGE + a few MGETs. Backfilled once per symbol from the
// global index (seeded flag, same pattern as the marker index).
const SYMBOL_INDEX_PREFIX = 'decision:symidx';
const SYMBOL_SEEDED_PREFIX = 'decision:symidx:seeded';
const MARKER_ACTIONS = new Set(['BUY', 'SELL', 'CLOSE']);

function isMarkerAction(action: unknown): boolean {
    return MARKER_ACTIONS.has(String(action || '').trim().toUpperCase());
}

// Flat HOLDs that started a cooldown with at least one wake band also enter the
// per-symbol index: the chart draws the bands as gray horizontal segments over
// the cooldown window. They are NOT marker actions (no chart arrow) — chart-side
// consumers that mean "entry/exit decisions" must filter by action themselves.
export function isCooldownBandDecision(aiDecision: any): boolean {
    if (String(aiDecision?.action || '').trim().toUpperCase() !== 'HOLD') return false;
    const minutes = Number(aiDecision?.cooldown_minutes);
    if (!Number.isFinite(minutes) || minutes <= 0) return false;
    const above = Number(aiDecision?.cooldown_wake_above);
    const below = Number(aiDecision?.cooldown_wake_below);
    return (Number.isFinite(above) && above > 0) || (Number.isFinite(below) && below > 0);
}

// Wake-triggered evaluations enter the index too, whatever their outcome: the
// wake CONSUMED the standing cooldown, and the chart truncates that band's
// overlay at the next indexed decision — without this row a band woken into a
// plain do-nothing HOLD (no marker action, no new band) would keep rendering
// to its scheduled end. Like cooldown-band rows, these are NOT marker actions.
export function isCooldownWakeEntry(entry: { snapshot?: any } | null | undefined): boolean {
    const crossed = (entry as any)?.snapshot?.cooldownWake?.crossed;
    return crossed === 'above' || crossed === 'below';
}

// In-position wake bands (ENABLE_POSITION_WAKE_BANDS) enter the index for the
// same chart overlay. They ride the same cooldown_wake_* decision fields but
// NEVER alongside cooldown_minutes (flat-only, and flat bands cannot survive
// sanitation without minutes) — so "band present, no minutes" is a sufficient
// discriminator; the action check just guards the invariant (HOLD or partial
// CLOSE are the only band-eligible in-position actions).
export function isPositionWakeBandDecision(aiDecision: any): boolean {
    const action = String(aiDecision?.action || '').trim().toUpperCase();
    if (action !== 'HOLD' && action !== 'CLOSE') return false;
    const minutes = Number(aiDecision?.cooldown_minutes);
    if (Number.isFinite(minutes) && minutes > 0) return false;
    const above = Number(aiDecision?.cooldown_wake_above);
    const below = Number(aiDecision?.cooldown_wake_below);
    return (Number.isFinite(above) && above > 0) || (Number.isFinite(below) && below > 0);
}

// Fired in-position wakes mirror isCooldownWakeEntry: the crossing consumed the
// band (replace-on-decision), so the row must exist in the index to truncate
// the band's overlay at the wake instead of at its assumed one-bar end.
export function isPositionWakeEntry(entry: { snapshot?: any } | null | undefined): boolean {
    const crossed = (entry as any)?.snapshot?.positionWake?.crossed;
    return crossed === 'above' || crossed === 'below';
}

function markerIndexKey(symbol: string, platform?: string): string {
    return `${MARKER_INDEX_PREFIX}:${normalizeHistoryPlatform(platform)}:${symbol.toUpperCase()}`;
}

function markerSeededKey(symbol: string, platform?: string): string {
    return `${MARKER_SEEDED_PREFIX}:${normalizeHistoryPlatform(platform)}:${symbol.toUpperCase()}`;
}

function symbolIndexKey(symbol: string, platform?: string): string {
    return `${SYMBOL_INDEX_PREFIX}:${normalizeHistoryPlatform(platform)}:${symbol.toUpperCase()}`;
}

function symbolSeededKey(symbol: string, platform?: string): string {
    return `${SYMBOL_SEEDED_PREFIX}:${normalizeHistoryPlatform(platform)}:${symbol.toUpperCase()}`;
}

function ensureKvConfig() {
    if (!upstash_payasyougo_KV_REST_API_URL || !upstash_payasyougo_KV_REST_API_TOKEN) {
        throw new Error('Missing upstash_payasyougo_KV_REST_API_URL or upstash_payasyougo_KV_REST_API_TOKEN');
    }
}

async function kvCommand(command: string, ...args: (string | number)[]) {
    ensureKvConfig();
    const encodedArgs = args
        .map((arg) => encodeURIComponent(typeof arg === 'string' ? arg : String(arg)))
        .join('/');
    const url = `${upstash_payasyougo_KV_REST_API_URL}/${command}${encodedArgs ? `/${encodedArgs}` : ''}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            Authorization: `Bearer ${upstash_payasyougo_KV_REST_API_TOKEN}`,
        },
    });
    const data = await res.json();
    if (!res.ok || data.error) {
        throw new Error(data.error || data.message || `KV command failed: ${command}`);
    }
    return data.result;
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

// ZREVRANGEBYSCORE key max min [LIMIT 0 count] — members with score in [min,max],
// highest score first. Used to pull just the entry/exit markers inside a chart
// window from the per-symbol marker index (below).
async function kvZRevRangeByScore(
    key: string,
    max: number | string,
    min: number | string,
    limit?: number,
): Promise<string[]> {
    const args: (string | number)[] = [key, max, min];
    if (typeof limit === 'number' && limit > 0) args.push('LIMIT', 0, limit);
    const res = await kvCommand('ZREVRANGEBYSCORE', ...args);
    return Array.isArray(res) ? res : [];
}

async function kvMGet(keys: string[]): Promise<(string | null)[]> {
    if (!keys.length) return [];
    const encoded = keys.map((k) => encodeURIComponent(k)).join('/');
    const url = `${upstash_payasyougo_KV_REST_API_URL}/MGET/${encoded}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: { Authorization: `Bearer ${upstash_payasyougo_KV_REST_API_TOKEN}` },
    });
    const data = await res.json();
    if (!res.ok || data.error) throw new Error(data.error || 'MGET failed');
    return Array.isArray(data.result) ? data.result : [];
}

export type DecisionSnapshot = {
    category?: string;
    promptSkipped?: boolean;
    skipStage?: string;
    skipReason?: string;
    usedTape?: boolean;
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
    // actionability gate verdict for this tick; on called ticks this records which
    // branch admitted the call (skips record it via skipReason already)
    actionability?: { actionable: boolean; reason: string } | null;
    platform?: string;
    newsSource?: string;
    instrumentId?: string;
    forexEventContext?: any;
    forexSessionContext?: any;
};

export type DecisionHistoryEntry = {
    timestamp: number;
    symbol: string;
    category?: string;
    platform?: string;
    instrumentId?: string;
    newsSource?: string;
    timeFrame: string;
    dryRun: boolean;
    prompt: { system: string; user: string } | null;
    aiDecision: TradeDecision & Record<string, any>;
    execResult: Record<string, any>;
    snapshot: DecisionSnapshot;
    biasTimeframes?: {
        context?: string;
        macro?: string;
        primary?: string;
        micro?: string;
        // Present only on decisions where the nano (15m) block was fetched
        // (real AI calls) — drives the Nano bias chip in the dashboard.
        nano?: string;
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
        // Store the entry WITHOUT the prompt: the full prompt (system + the
        // STATE/MARKET user turn) is 50-150KB and rides in prompt_json on the
        // Neon dual-write below — keeping a KV copy made every history read
        // (summary rebuilds, recent-actions loads, timelines) re-download it.
        // The dashboard prompt viewer fetches it from Neon on demand.
        await kvSetEx(key, HISTORY_TTL_SECONDS, JSON.stringify({ ...entry, prompt: null }));
        // add to index
        await kvZAdd(HISTORY_INDEX_KEY, entry.timestamp, key);
        // ...and to the per-symbol full index (fast path for symbol reads).
        await kvZAdd(symbolIndexKey(entry.symbol, entry.platform), entry.timestamp, key);

        // prune index entries older than TTL (by timestamp score)
        const cutoff = Date.now() - HISTORY_TTL_SECONDS * 1000;
        await kvZRemRangeByScore(HISTORY_INDEX_KEY, 0, cutoff);
        await kvZRemRangeByScore(symbolIndexKey(entry.symbol, entry.platform), 0, cutoff);

        // Mirror entry/exit decisions into the per-symbol marker index so the chart
        // can fetch just this symbol's markers without scanning the global index.
        // Cooldown-band HOLDs ride along for the chart's wake-band segments, and
        // wake-triggered evaluations so a consumed band's overlay stops at the wake.
        if (
            isMarkerAction(entry.aiDecision?.action) ||
            isCooldownBandDecision(entry.aiDecision) ||
            isCooldownWakeEntry(entry) ||
            isPositionWakeBandDecision(entry.aiDecision) ||
            isPositionWakeEntry(entry)
        ) {
            const mKey = markerIndexKey(entry.symbol, entry.platform);
            await kvZAdd(mKey, entry.timestamp, key);
            await kvZRemRangeByScore(mKey, 0, cutoff);
        }
    } catch (err) {
        console.error('Failed to append decision history:', err);
    }

    // Durable dual-write to Postgres (source of truth beyond the KV TTL). Kept
    // separate so a PG outage never breaks the KV path, and a no-op when PG is
    // unconfigured.
    try {
        await upsertSwingDecision(entry);
    } catch (err) {
        console.error('Failed to persist decision to Postgres:', err);
    }
}

// Fetch one decision by its exact timestamp — the history key is fully
// reconstructible (decision:{ts}:{platform}:{symbol}), so this is a single KV
// GET with no index scan. Used by the dashboard timeline to load the decision
// behind a clicked tick. Returns null when missing/expired (7d KV TTL).
export async function loadDecisionAt(
    symbol: string,
    timestamp: number,
    platform?: string,
): Promise<DecisionHistoryEntry | null> {
    ensureKvConfig();
    const ts = Math.floor(Number(timestamp));
    if (!Number.isFinite(ts) || ts <= 0) return null;
    try {
        const raw = await kvGet(keyFor(symbol, ts, platform));
        if (!raw) return null;
        const parsed = JSON.parse(raw) as DecisionHistoryEntry;
        return parsed?.symbol?.toUpperCase() === symbol.toUpperCase() ? parsed : null;
    } catch (err) {
        console.warn('loadDecisionAt failed:', err);
        return null;
    }
}

// The original global-index pagination. For a symbol-filtered read this costs
// ~2 commands per 50-key page at 1-in-N-symbols key density — kept only for
// symbol-less/platform-less callers and as the one-time backfill source for
// the per-symbol index below.
async function loadDecisionHistoryGlobalScan(
    upperSymbol: string | undefined,
    normalizedPlatform: string | undefined,
    limit: number,
): Promise<DecisionHistoryEntry[]> {
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

export async function loadDecisionHistory(symbol?: string, limit = 20, platform?: string): Promise<DecisionHistoryEntry[]> {
    ensureKvConfig();
    const upperSymbol = symbol?.toUpperCase();
    const normalizedPlatform = platform ? normalizeHistoryPlatform(platform) : undefined;

    // Fast path — per-symbol index: 1 ZREVRANGE + ceil(limit/50) MGETs instead
    // of paging the global index (which reads ~2 commands per 50 keys of which
    // only ~1/25 belong to this symbol). Rows written before this index
    // existed are pulled in by a one-time global-scan backfill per symbol.
    if (upperSymbol && normalizedPlatform) {
        try {
            const idxKey = symbolIndexKey(upperSymbol, normalizedPlatform);
            const keys = await kvZRevRange(idxKey, 0, limit - 1);
            if (keys.length < limit) {
                const seeded = await kvGet(symbolSeededKey(upperSymbol, normalizedPlatform));
                if (!seeded) {
                    const legacy = await loadDecisionHistoryGlobalScan(upperSymbol, normalizedPlatform, 1200);
                    await Promise.all(
                        legacy.map((h) =>
                            kvZAdd(idxKey, h.timestamp, keyFor(h.symbol, h.timestamp, h.platform)).catch(
                                () => undefined,
                            ),
                        ),
                    );
                    await kvSetEx(symbolSeededKey(upperSymbol, normalizedPlatform), HISTORY_TTL_SECONDS, '1');
                    // The scan already produced the answer, newest-first.
                    return legacy.slice(0, limit);
                }
            }
            const results: DecisionHistoryEntry[] = [];
            for (let i = 0; i < keys.length && results.length < limit; i += 50) {
                const values = await kvMGet(keys.slice(i, i + 50));
                for (const raw of values) {
                    // null = entry expired (7d TTL) under a not-yet-pruned index key.
                    if (!raw) continue;
                    try {
                        results.push(JSON.parse(raw));
                    } catch (err) {
                        console.warn('Skipping invalid history entry:', err);
                    }
                    if (results.length >= limit) break;
                }
            }
            return results;
        } catch (err) {
            console.warn('Symbol decision index read failed, falling back to global scan:', err);
        }
    }

    return loadDecisionHistoryGlobalScan(upperSymbol, normalizedPlatform, limit);
}

// Fetch just the entry/exit marker decisions (BUY/SELL/CLOSE) for one symbol in a
// time window, straight from the per-symbol marker index — a handful of KV
// round-trips instead of scanning the whole global index. The chart endpoint uses
// this for markers + captured-leverage. Entirely KV; never touches Neon.
//
// One-time backfill: the marker index only starts filling from decisions written
// after it shipped, so on the first read for a symbol (no `seeded` flag) we do a
// single legacy scan, populate the index from it, and set the flag — subsequent
// reads are all fast. Returns entries newest-first.
export async function loadSymbolMarkerHistory(
    symbol: string,
    platform?: string,
    opts?: { fromMs?: number; toMs?: number; limit?: number },
): Promise<DecisionHistoryEntry[]> {
    ensureKvConfig();
    const upperSymbol = symbol.toUpperCase();
    const mKey = markerIndexKey(symbol, platform);
    const min = Math.max(0, Math.floor(opts?.fromMs ?? 0));
    const max = typeof opts?.toMs === 'number' ? Math.floor(opts.toMs) : Date.now();
    const limit = typeof opts?.limit === 'number' && opts.limit > 0 ? opts.limit : 500;

    const readFromIndex = async (): Promise<DecisionHistoryEntry[]> => {
        const memberKeys = await kvZRevRangeByScore(mKey, max, min, limit);
        if (!memberKeys.length) return [];
        const values = await kvMGet(memberKeys);
        const out: DecisionHistoryEntry[] = [];
        for (const raw of values) {
            if (!raw) continue;
            try {
                out.push(JSON.parse(raw));
            } catch (err) {
                console.warn('Skipping invalid marker entry:', err);
            }
        }
        return out;
    };

    try {
        const fromIndex = await readFromIndex();
        if (fromIndex.length) return fromIndex;

        // Empty index → backfill once (guarded by the seeded flag so a symbol that
        // genuinely has no markers doesn't re-scan on every load).
        const seededKey = markerSeededKey(symbol, platform);
        const seeded = await kvGet(seededKey);
        if (seeded) return [];

        const legacy = (await loadDecisionHistory(symbol, 1200, platform)).filter(
            (h) =>
                isMarkerAction(h?.aiDecision?.action) ||
                isCooldownBandDecision(h?.aiDecision) ||
                isCooldownWakeEntry(h) ||
                isPositionWakeBandDecision(h?.aiDecision) ||
                isPositionWakeEntry(h),
        );
        // Populate the index so future reads are fast.
        await Promise.all(
            legacy.map((h) =>
                kvZAdd(mKey, h.timestamp, keyFor(h.symbol, h.timestamp, h.platform)).catch(() => undefined),
            ),
        );
        await kvSetEx(seededKey, HISTORY_TTL_SECONDS, '1');
        // Return only the entries inside the requested window, newest-first.
        return legacy
            .filter((h) => {
                const ts = Number(h.timestamp);
                return Number.isFinite(ts) && ts >= min && ts <= max && h.symbol?.toUpperCase() === upperSymbol;
            })
            .sort((a, b) => b.timestamp - a.timestamp)
            .slice(0, limit);
    } catch (err) {
        console.warn('loadSymbolMarkerHistory failed:', err);
        return [];
    }
}

// Extract the leverage we actually set at execution time from decision history,
// as a timestamped list usable to attribute the right leverage to each position.
// Prefers execResult.leverage (what was applied), then the targetLeverage we asked
// for, then the AI's hinted leverage. Used to override Bitget's stale reported
// leverage on closed positions (see pickCapturedLeverage in lib/analytics.ts).
export function extractCapturedLeverages(history: DecisionHistoryEntry[] | null | undefined): CapturedLeverage[] {
    if (!history?.length) return [];
    const out: CapturedLeverage[] = [];
    for (const h of history) {
        const ts = Number(h?.timestamp);
        if (!Number.isFinite(ts) || ts <= 0) continue;
        const lev =
            Number((h.execResult as any)?.leverage) ||
            Number((h.execResult as any)?.targetLeverage) ||
            Number((h.aiDecision as any)?.leverage);
        if (Number.isFinite(lev) && lev > 0) out.push({ timestamp: ts, leverage: lev });
    }
    return out;
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
