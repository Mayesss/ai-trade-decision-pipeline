// lib/news.ts

import { COINDESK_API_BASE, COINDESK_NEWS_LIST_PATH } from './constants';

// ------------------------------
// KV cache (1h TTL)
// ------------------------------

const KV_REST_API_URL = (process.env.KV_REST_API_URL || '').replace(/\/$/, '');
const KV_REST_API_TOKEN = process.env.KV_REST_API_TOKEN || '';
const NEWS_CACHE_TTL_SECONDS = 60 * 60; // 1 hour

function ensureKvConfig() {
    return KV_REST_API_URL && KV_REST_API_TOKEN;
}

async function kvGet(key: string): Promise<string | null> {
    if (!ensureKvConfig()) return null;
    const res = await fetch(`${KV_REST_API_URL}/GET/${encodeURIComponent(key)}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${KV_REST_API_TOKEN}` },
    });
    const data = await res.json();
    if (!res.ok || data.error) return null;
    return data.result ?? null;
}

async function kvSetEx(key: string, ttlSeconds: number, value: string) {
    if (!ensureKvConfig()) return;
    await fetch(`${KV_REST_API_URL}/SETEX/${encodeURIComponent(key)}/${ttlSeconds}/${encodeURIComponent(value)}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${KV_REST_API_TOKEN}` },
    });
}

function newsCacheKey(base: string) {
    return `news:${base.toUpperCase()}`;
}

// ------------------------------
// CoinDesk API helpers
// ------------------------------

/** Low-level fetch helper for CoinDesk API */
export async function coindeskFetch(path: string, query: Record<string, string | number | undefined> = {}) {
    if (!process.env.COINDESK_API_KEY) throw new Error('Missing COINDESK_API_KEY');

    const qs = Object.entries(query)
        .filter(([, v]) => v !== undefined && v !== '')
        .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`)
        .join('&');

    const coindeskBase = (COINDESK_API_BASE || 'https://data-api.coindesk.com').replace(/\/+$/, '');
    const url = `${coindeskBase}${path}${qs ? `?${qs}` : ''}`;

    const res = await fetch(url, {
        headers: { Authorization: `Bearer ${process.env.COINDESK_API_KEY}` },
    });

    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`CoinDesk API error ${res.status}: ${res.statusText} ${text}`);
    }

    return res.json();
}

/** Extracts the base asset ticker (e.g., BTC from BTCUSDT or ETH-PERP). */
export function baseFromSymbol(symbol: string): string {
    const s = symbol.toUpperCase();
    const QUOTES = ['USDT', 'USDC', 'USD', 'BTC', 'ETH', 'EUR', 'PERP'];
    for (const q of QUOTES) {
        if (s.endsWith(q)) return s.slice(0, s.length - q.length).replace(/[-_]/g, '');
    }
    return s.replace(/[^A-Z].*$/, '');
}

// ------------------------------
// Sentiment Aggregation
// ------------------------------

export type Sentiment = 'NEGATIVE' | 'NEUTRAL' | 'POSITIVE';

interface Article {
    SENTIMENT: Sentiment;
    PUBLISHED_ON: number;
    TITLE: string;
}

interface Payload {
    Data: Article[];
}

/** Determine the dominant sentiment from a payload of news articles. */
export function getDominantSentiment(payload: Payload): Sentiment {
    const sorted = [...payload.Data].sort((a, b) => b.PUBLISHED_ON - a.PUBLISHED_ON);
    const scores: Record<Sentiment, number> = {
        NEGATIVE: 0,
        NEUTRAL: 0,
        POSITIVE: 0,
    };

    const weightStep = 0.1;

    sorted.forEach((item, index) => {
        const weight = Math.max(1 - index * weightStep, 0.1);
        scores[item.SENTIMENT] += weight;
    });

    return Object.entries(scores).reduce((a, b) => (b[1] > a[1] ? b : a))[0] as Sentiment;
}

/** Fetch latest CoinDesk news and compute overall sentiment. */
export async function fetchNewsSentiment(symbolOrBase: string): Promise<Sentiment | null> {
    const { sentiment } = await fetchNewsWithHeadlines(symbolOrBase);
    return sentiment;
}

/** Fetch latest CoinDesk news, compute sentiment, and return top 3 titles. */
export async function fetchNewsWithHeadlines(
    symbolOrBase: string,
): Promise<{ sentiment: Sentiment | null; headlines: string[] }> {
    const base = baseFromSymbol(symbolOrBase);
    const listPath = COINDESK_NEWS_LIST_PATH || '/news/v1/article/list';
    const cacheKey = newsCacheKey(base);

    try {
        const cached = await kvGet(cacheKey);
        if (cached) {
            const parsed = JSON.parse(cached);
            const ageMs = Date.now() - Number(parsed?.timestamp || 0);
            if (Number.isFinite(ageMs) && ageMs < 59 * 60 * 1000) {
                return {
                    sentiment: parsed?.sentiment ?? null,
                    headlines: Array.isArray(parsed?.headlines) ? parsed.headlines : [],
                };
            }
        }
    } catch (err) {
        console.warn('Failed to read news cache:', err);
    }

    const query = {
        categories: base,
        limit: 25,
        lang: 'EN',
    };

    let payload: any;
    try {
        payload = await coindeskFetch(listPath, query);
    } catch (e) {
         console.warn('Error fetching CoinDesk news:', e);
        return { sentiment: null, headlines: [] };
    }

    const sentiment = getDominantSentiment(payload) || 'NEUTRAL';
    const headlines = Array.isArray(payload?.Data)
        ? payload.Data.sort((a: Article, b: Article) => b.PUBLISHED_ON - a.PUBLISHED_ON)
              .slice(0, 5)
              .map((a: Article) => a.TITLE)
        : [];
    try {
        await kvSetEx(
            cacheKey,
            NEWS_CACHE_TTL_SECONDS,
            JSON.stringify({ timestamp: Date.now(), sentiment, headlines }),
        );
    } catch (err) {
        console.warn('Failed to write news cache:', err);
    }
    return { sentiment, headlines };
}
