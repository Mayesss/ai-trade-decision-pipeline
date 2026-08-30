// Crypto Fear & Greed index (alternative.me) — a free, keyless, market-wide
// daily sentiment gauge (0 = extreme fear … 100 = extreme greed). Fetched by
// /api/analyze ONLY after every gate has passed (crypto ticks only) and
// rendered as market.fear_greed next to news/btc_context: a deterministic
// numeric mood field, complementing the Perplexity prose digest.
//
// The index updates once per day (~00:00 UTC), so one KV-cached value serves
// every symbol for an hour — the cache key is global, not per symbol.
//
// Fail-open by design: any failure (flag off, HTTP error, timeout, malformed
// payload) returns null and the prompt block is simply absent. Like the sonar
// digest, this must never touch lib/swing/aiHealth.ts.

import { kvGetJson, kvSetJson } from '../kv';

export type FearGreedContext = {
    // Today's index value, 0..100.
    value: number;
    // Provider's classification for the value (e.g. "Fear", "Extreme Greed").
    label: string;
    // UTC date (YYYY-MM-DD) of the index timestamp, so the model can see
    // staleness instead of trusting the fetch time.
    updated_utc: string;
    // Last ~10 daily values, oldest first (today last, = value) — the key is
    // self-describing so the prompt needs no extra prose. Direction and
    // persistence live here: a week-long grind from fear into greed reads
    // differently than a one-day blip to the same number.
    daily_oldest_first: number[];
};

// Default ON (mirrors SWING_BTC_CONTEXT_ENABLED): the source is free and the
// loader fails open, so the flag exists only as a kill switch.
export function swingFearGreedEnabled(): boolean {
    const raw = String(process.env.SWING_FEAR_GREED_ENABLED ?? '')
        .trim()
        .toLowerCase();
    if (['false', '0', 'no', 'off'].includes(raw)) return false;
    return true;
}

// limit=10: enough daily history to show sentiment direction/persistence; the
// index is slow-moving and BTC-derived, so a longer window mostly restates
// what STATE's own momentum/volatility already measure.
export const FEAR_GREED_URL = 'https://api.alternative.me/fng/?limit=10';

// The index moves once a day; 60min keeps every crypto tick in an hour on one
// fetch while never serving a value more than an hour past its daily update.
const FEAR_GREED_TTL_SECONDS = 3600;
const FEAR_GREED_TIMEOUT_MS = 5_000;

const CACHE_KEY = 'swing:fearGreed:v1';

type FngRow = { value?: unknown; value_classification?: unknown; timestamp?: unknown };

function parseRow(row: FngRow | undefined): { value: number; label: string; tsMs: number } | null {
    if (!row) return null;
    const value = Number(row.value);
    const label = typeof row.value_classification === 'string' ? row.value_classification.trim() : '';
    const tsSec = Number(row.timestamp);
    if (!Number.isFinite(value) || value < 0 || value > 100 || !label || !Number.isFinite(tsSec)) return null;
    return { value: Math.round(value), label, tsMs: tsSec * 1000 };
}

// Entry point for /api/analyze (crypto ticks). Returns null on flag-off or ANY
// failure so the caller's prompt block is simply absent — never throws.
export async function loadFearGreedContext(): Promise<FearGreedContext | null> {
    if (!swingFearGreedEnabled()) return null;

    try {
        const cached = await kvGetJson<FearGreedContext>(CACHE_KEY);
        if (cached && Number.isFinite(cached.value)) return cached;
    } catch (err) {
        console.warn('Fear/greed cache read failed:', err);
    }

    try {
        const res = await fetch(FEAR_GREED_URL, { signal: AbortSignal.timeout(FEAR_GREED_TIMEOUT_MS) });
        if (!res.ok) {
            console.warn(`Fear/greed fetch failed: HTTP ${res.status}`);
            return null;
        }
        const payload = (await res.json()) as { data?: FngRow[] };
        const rows = Array.isArray(payload?.data) ? payload.data : [];
        // Provider returns newest-first; rows[0] (today) must parse or the
        // whole payload is suspect. Older rows are best-effort.
        const today = parseRow(rows[0]);
        if (!today) return null;
        const daily = rows
            .map(parseRow)
            .filter((r): r is NonNullable<ReturnType<typeof parseRow>> => r !== null)
            .map((r) => r.value)
            .reverse();

        const ctx: FearGreedContext = {
            value: today.value,
            label: today.label,
            updated_utc: new Date(today.tsMs).toISOString().slice(0, 10),
            daily_oldest_first: daily,
        };
        try {
            await kvSetJson(CACHE_KEY, ctx, FEAR_GREED_TTL_SECONDS);
        } catch (err) {
            console.warn('Fear/greed cache write failed:', err);
        }
        return ctx;
    } catch (err) {
        console.warn('Could not load fear/greed index:', err);
        return null;
    }
}
